//! Sync IO operations
//!
//! The traits to perform positioned io are re-exported from
//! [positioned-io](https://crates.io/crates/positioned-io).
use std::{
    io::{self, Read, Write},
    ops::Range,
    result,
};

use crate::{
    blake3,
    io::error::{AnyDecodeError, EncodeError},
    io::{
        outboard::{parse_hash_pair, PostOrderOutboard, PreOrderOutboard},
        Leaf, Parent,
    },
    iter::BaoChunk,
    rec::{encode_selected_rec, truncate_ranges},
    BaoTree, ByteNum, ChunkRanges, ChunkRangesRef, TreeNode,
};
use blake3::guts::parent_cv;
use bytes::BytesMut;
pub use positioned_io::{ReadAt, Size, WriteAt};
use smallvec::SmallVec;

use super::{combine_hash_pair, BaoContentItem, DecodeError};
use crate::{hash_subtree, iter::ResponseIterRef};

/// A binary merkle tree for blake3 hashes of a blob.
///
/// This trait contains information about the geometry of the tree, the root hash,
/// and a method to load the hashes at a given node.
///
/// It is up to the implementor to decide how to store the hashes.
///
/// In the original bao crate, the hashes are stored in a file in pre order.
/// This is implemented for a generic io object in [super::outboard::PreOrderOutboard]
/// and for a memory region in [super::outboard::PreOrderMemOutboard].
///
/// For files that grow over time, it is more efficient to store the hashes in post order.
/// This is implemented for a generic io object in [super::outboard::PostOrderOutboard]
/// and for a memory region in [super::outboard::PostOrderMemOutboard].
///
/// If you use a different storage engine, you can implement this trait for it. E.g.
/// you could store the hashes in a database and use the node number as the key.
pub trait Outboard {
    /// The root hash
    fn root(&self) -> blake3::Hash;
    /// The tree. This contains the information about the size of the file and the block size.
    fn tree(&self) -> BaoTree;
    /// load the hash pair for a node
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>>;
}

/// A mutable outboard.
///
/// This trait provides a way to save a hash pair for a node and to set the
/// length of the data file.
///
/// This trait can be used to incrementally save an outboard when receiving data.
/// If you want to just ignore outboard data, there is a special placeholder outboard
/// implementation [super::outboard::EmptyOutboard].
pub trait OutboardMut: Sized {
    /// Save a hash pair for a node
    fn save(&mut self, node: TreeNode, hash_pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()>;
}

impl<O: OutboardMut> OutboardMut for &mut O {
    fn save(&mut self, node: TreeNode, hash_pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        (**self).save(node, hash_pair)
    }
}

impl<O: Outboard> Outboard for &O {
    fn root(&self) -> blake3::Hash {
        (**self).root()
    }
    fn tree(&self) -> BaoTree {
        (**self).tree()
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        (**self).load(node)
    }
}

impl<O: Outboard> Outboard for &mut O {
    fn root(&self) -> blake3::Hash {
        (**self).root()
    }
    fn tree(&self) -> BaoTree {
        (**self).tree()
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        (**self).load(node)
    }
}

impl<R: ReadAt + Size> PreOrderOutboard<R> {
    /// Create a new outboard from a reader, root hash, and block size.
    pub fn new(root: blake3::Hash, tree: BaoTree, data: R) -> Self {
        Self { root, tree, data }
    }
}

impl<R: ReadAt> Outboard for PreOrderOutboard<R> {
    fn root(&self) -> blake3::Hash {
        self.root
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return Ok(None);
        };
        let offset = offset * 64;
        let mut content = [0u8; 64];
        self.data.read_exact_at(offset, &mut content)?;
        Ok(Some(parse_hash_pair(content)))
    }
}

impl<W: ReadAt + WriteAt> OutboardMut for PreOrderOutboard<W> {
    fn save(&mut self, node: TreeNode, hash_pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        let Some(offset) = self.tree.pre_order_offset(node) else {
            return Ok(());
        };
        let offset = offset * 64;
        let mut content = [0u8; 64];
        content[0..32].copy_from_slice(hash_pair.0.as_bytes());
        content[32..64].copy_from_slice(hash_pair.1.as_bytes());
        self.data.write_all_at(offset, &content)?;
        Ok(())
    }
}

impl<R: ReadAt + Size> PostOrderOutboard<R> {
    /// Create a new outboard from a reader, root hash, and block size.
    pub fn new(root: blake3::Hash, tree: BaoTree, data: R) -> Self {
        Self { root, tree, data }
    }
}

impl<R: ReadAt> Outboard for PostOrderOutboard<R> {
    fn root(&self) -> blake3::Hash {
        self.root
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let Some(offset) = self.tree.post_order_offset(node) else {
            return Ok(None);
        };
        let offset = offset.value() * 64;
        let mut content = [0u8; 64];
        self.data.read_exact_at(offset, &mut content)?;
        Ok(Some(parse_hash_pair(content)))
    }
}

/// Given an outboard, return a range set of all valid ranges
pub fn valid_outboard_ranges<O>(outboard: &O) -> io::Result<ChunkRanges>
where
    O: Outboard,
{
    struct RecursiveValidator<'a, O: Outboard> {
        tree: BaoTree,
        shifted_filled_size: TreeNode,
        res: ChunkRanges,
        outboard: &'a O,
    }

    impl<'a, O: Outboard> RecursiveValidator<'a, O> {
        fn validate_rec(
            &mut self,
            parent_hash: &blake3::Hash,
            shifted: TreeNode,
            is_root: bool,
        ) -> io::Result<()> {
            let node = shifted.subtract_block_size(self.tree.block_size.0);
            let (l_hash, r_hash) = if let Some((l_hash, r_hash)) = self.outboard.load(node)? {
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                if &actual != parent_hash {
                    // we got a validation error. Simply continue without adding the range
                    return Ok(());
                }
                (l_hash, r_hash)
            } else {
                (*parent_hash, blake3::Hash::from([0; 32]))
            };
            if shifted.is_leaf() {
                let start = node.chunk_range().start;
                let end = (start + self.tree.chunk_group_chunks() * 2).min(self.tree.chunks());
                self.res |= ChunkRanges::from(start..end);
            } else {
                // recurse
                let left = shifted.left_child().unwrap();
                self.validate_rec(&l_hash, left, false)?;
                let right = shifted.right_descendant(self.shifted_filled_size).unwrap();
                self.validate_rec(&r_hash, right, false)?;
            }
            Ok(())
        }
    }
    let tree = outboard.tree();
    let root_hash = outboard.root();
    let (shifted_root, shifted_filled_size) = tree.shifted();
    let mut validator = RecursiveValidator {
        tree,
        shifted_filled_size,
        res: ChunkRanges::empty(),
        outboard,
    };
    validator.validate_rec(&root_hash, shifted_root, true)?;
    Ok(validator.res)
}

/// Iterator that can be used to decode a response to a range request
#[derive(Debug)]
pub struct DecodeResponseIter<'a, R> {
    inner: ResponseIterRef<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: BytesMut,
}

impl<'a, R: Read> DecodeResponseIter<'a, R> {
    /// Create a new iterator to decode a response.
    ///
    /// For decoding you need to know the root hash, block size, and the ranges that were requested.
    /// Additionally you need to provide a reader that can be used to read the encoded data.
    pub fn new(root: blake3::Hash, tree: BaoTree, encoded: R, ranges: &'a ChunkRangesRef) -> Self {
        let buf = BytesMut::with_capacity(tree.block_size().bytes());
        Self::new_with_buffer(root, tree, encoded, ranges, buf)
    }

    /// Create a new iterator to decode a response.
    ///
    /// This is the same as [Self::new], but allows you to provide a buffer to use for decoding.
    /// The buffer will be resized as needed, but it's capacity should be the [crate::BlockSize::bytes].
    pub fn new_with_buffer(
        root: blake3::Hash,
        tree: BaoTree,
        encoded: R,
        ranges: &'a ChunkRangesRef,
        buf: BytesMut,
    ) -> Self {
        let ranges = truncate_ranges(ranges, tree.size());
        let mut stack = SmallVec::new();
        stack.push(root);
        Self {
            stack,
            inner: ResponseIterRef::new(tree, ranges),
            encoded,
            buf,
        }
    }

    /// Get a reference to the buffer used for decoding.
    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    /// Get a reference to the tree used for decoding.
    ///
    /// This is only available after the first chunk has been decoded.
    pub fn tree(&self) -> BaoTree {
        self.inner.tree()
    }

    fn next0(&mut self) -> result::Result<Option<BaoContentItem>, AnyDecodeError> {
        match self.inner.next() {
            Some(BaoChunk::Parent {
                is_root,
                left,
                right,
                node,
                ..
            }) => {
                let pair @ (l_hash, r_hash) = read_parent(&mut self.encoded)
                    .map_err(|e| DecodeError::maybe_parent_not_found(e, node))?;
                let parent_hash = self.stack.pop().unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                if parent_hash != actual {
                    return Err(AnyDecodeError::ParentHashMismatch(node));
                }
                if right {
                    self.stack.push(r_hash);
                }
                if left {
                    self.stack.push(l_hash);
                }
                Ok(Some(Parent { node, pair }.into()))
            }
            Some(BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
                ..
            }) => {
                self.buf.resize(size, 0);
                self.encoded
                    .read_exact(&mut self.buf)
                    .map_err(|e| DecodeError::maybe_leaf_not_found(e, start_chunk))?;
                let actual = hash_subtree(start_chunk.0, &self.buf, is_root);
                let leaf_hash = self.stack.pop().unwrap();
                if leaf_hash != actual {
                    return Err(AnyDecodeError::LeafHashMismatch(start_chunk));
                }
                Ok(Some(
                    Leaf {
                        offset: start_chunk.to_bytes(),
                        data: self.buf.split().freeze(),
                    }
                    .into(),
                ))
            }
            None => Ok(None),
        }
    }
}

impl<'a, R: Read> Iterator for DecodeResponseIter<'a, R> {
    type Item = result::Result<BaoContentItem, AnyDecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This will not validate on writing, so data corruption will be detected on reading
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub fn encode_ranges<D: ReadAt + Size, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &ChunkRangesRef,
    encoded: W,
) -> result::Result<(), EncodeError> {
    let data = data;
    let mut encoded = encoded;
    let tree = outboard.tree();
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    // write header
    encoded.write_all(tree.size.0.to_le_bytes().as_slice())?;
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent { node, .. } => {
                let (l_hash, r_hash) = outboard.load(node)?.unwrap();
                let pair = combine_hash_pair(&l_hash, &r_hash);
                encoded.write_all(&pair)?;
            }
            BaoChunk::Leaf {
                start_chunk, size, ..
            } => {
                let start = start_chunk.to_bytes();
                let buf = &mut buffer[..size];
                data.read_exact_at(start.0, buf)?;
                encoded.write_all(buf)?;
            }
        }
    }
    Ok(())
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This function validates the data before writing.
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub fn encode_ranges_validated<D: ReadAt + Size, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &ChunkRangesRef,
    encoded: W,
) -> result::Result<(), EncodeError> {
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let data = data;
    let mut encoded = encoded;
    let tree = outboard.tree();
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let mut out_buf = Vec::new();
    // canonicalize ranges
    let ranges = truncate_ranges(ranges, tree.size());
    // write header
    encoded.write_all(tree.size.0.to_le_bytes().as_slice())?;
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent {
                is_root,
                left,
                right,
                node,
                ..
            } => {
                let (l_hash, r_hash) = outboard.load(node)?.unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                let expected = stack.pop().unwrap();
                if actual != expected {
                    return Err(EncodeError::ParentHashMismatch(node));
                }
                if right {
                    stack.push(r_hash);
                }
                if left {
                    stack.push(l_hash);
                }
                let pair = combine_hash_pair(&l_hash, &r_hash);
                encoded.write_all(&pair)?;
            }
            BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
                ranges,
                ..
            } => {
                let expected = stack.pop().unwrap();
                let start = start_chunk.to_bytes();
                let buf = &mut buffer[..size];
                data.read_exact_at(start.0, buf)?;
                let (actual, to_write) = if !ranges.is_all() {
                    // we need to encode just a part of the data
                    //
                    // write into an out buffer to ensure we detect mismatches
                    // before writing to the output.
                    out_buf.clear();
                    let actual = encode_selected_rec(
                        start_chunk,
                        buf,
                        is_root,
                        ranges,
                        tree.block_size.to_u32(),
                        true,
                        &mut out_buf,
                    );
                    (actual, &out_buf[..])
                } else {
                    let actual = hash_subtree(start_chunk.0, buf, is_root);
                    #[allow(clippy::redundant_slicing)]
                    (actual, &buf[..])
                };
                if actual != expected {
                    return Err(EncodeError::LeafHashMismatch(start_chunk));
                }
                encoded.write_all(to_write)?;
            }
        }
    }
    Ok(())
}

/// Decode a response into a file while updating an outboard.
///
/// If you do not want to update an outboard, use [super::outboard::EmptyOutboard] as
/// the outboard.
pub fn decode_ranges<R, O, W>(
    ranges: &ChunkRangesRef,
    encoded: R,
    mut target: W,
    mut outboard: O,
) -> io::Result<()>
where
    O: OutboardMut + Outboard,
    R: Read,
    W: WriteAt,
{
    let iter = DecodeResponseIter::new(outboard.root(), outboard.tree(), encoded, ranges);
    for item in iter {
        match item? {
            BaoContentItem::Parent(Parent { node, pair }) => {
                outboard.save(node, &pair)?;
            }
            BaoContentItem::Leaf(Leaf { offset, data }) => {
                target.write_all_at(offset.0, &data)?;
            }
        }
    }
    Ok(())
}

/// Compute the outboard for the given data.
///
/// Unlike [outboard_post_order], this will work with any outboard
/// implementation, but it is not guaranteed that writes are sequential.
pub fn outboard(
    data: impl Read,
    tree: BaoTree,
    mut outboard: impl OutboardMut,
) -> io::Result<blake3::Hash> {
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let hash = outboard_impl(tree, data, &mut outboard, &mut buffer)?;
    Ok(hash)
}

/// Internal helper for [outboard_post_order]. This takes a buffer of the chunk group size.
fn outboard_impl(
    tree: BaoTree,
    mut data: impl Read,
    mut outboard: impl OutboardMut,
    buffer: &mut [u8],
) -> io::Result<blake3::Hash> {
    // do not allocate for small trees
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    debug_assert!(buffer.len() == tree.chunk_group_bytes().to_usize());
    for item in tree.post_order_chunks_iter() {
        match item {
            BaoChunk::Parent { is_root, node, .. } => {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                outboard.save(node, &(left_hash, right_hash))?;
                let parent = parent_cv(&left_hash, &right_hash, is_root);
                stack.push(parent);
            }
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
                ..
            } => {
                let buf = &mut buffer[..size];
                data.read_exact(buf)?;
                let hash = hash_subtree(start_chunk.0, buf, is_root);
                stack.push(hash);
            }
        }
    }
    debug_assert_eq!(stack.len(), 1);
    let hash = stack.pop().unwrap();
    Ok(hash)
}

/// Compute the post order outboard for the given data, writing into a io::Write
///
/// For the post order outboard, writes to the target are sequential.
///
/// This will not add the size to the output. You need to store it somewhere else
/// or append it yourself.
pub fn outboard_post_order(
    data: impl Read,
    tree: BaoTree,
    mut outboard: impl Write,
) -> io::Result<blake3::Hash> {
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let hash = outboard_post_order_impl(tree, data, &mut outboard, &mut buffer)?;
    Ok(hash)
}

/// Internal helper for [outboard_post_order]. This takes a buffer of the chunk group size.
fn outboard_post_order_impl(
    tree: BaoTree,
    mut data: impl Read,
    mut outboard: impl Write,
    buffer: &mut [u8],
) -> io::Result<blake3::Hash> {
    // do not allocate for small trees
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    debug_assert!(buffer.len() == tree.chunk_group_bytes().to_usize());
    for item in tree.post_order_chunks_iter() {
        match item {
            BaoChunk::Parent { is_root, .. } => {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                outboard.write_all(left_hash.as_bytes())?;
                outboard.write_all(right_hash.as_bytes())?;
                let parent = parent_cv(&left_hash, &right_hash, is_root);
                stack.push(parent);
            }
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
                ..
            } => {
                let buf = &mut buffer[..size];
                data.read_exact(buf)?;
                let hash = hash_subtree(start_chunk.0, buf, is_root);
                stack.push(hash);
            }
        }
    }
    debug_assert_eq!(stack.len(), 1);
    let hash = stack.pop().unwrap();
    Ok(hash)
}

fn read_parent(mut from: impl Read) -> std::io::Result<(blake3::Hash, blake3::Hash)> {
    let mut buf = [0; 64];
    from.read_exact(&mut buf)?;
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..]).unwrap());
    Ok((l_hash, r_hash))
}

/// seeks read the bytes for the range from the source
fn read_range(from: impl ReadAt, range: Range<ByteNum>, buf: &mut [u8]) -> std::io::Result<&[u8]> {
    let len = (range.end - range.start).to_usize();
    let buf = &mut buf[..len];
    from.read_exact_at(range.start.0, buf)?;
    Ok(buf)
}

/// Given an outboard and a file, return all valid ranges
pub fn valid_file_ranges<O, R>(outboard: &O, reader: R) -> io::Result<ChunkRanges>
where
    O: Outboard,
    R: ReadAt,
{
    struct RecursiveValidator<'a, O: Outboard, R: ReadAt> {
        tree: BaoTree,
        valid_nodes: TreeNode,
        res: ChunkRanges,
        outboard: &'a O,
        reader: R,
        buffer: Vec<u8>,
    }

    impl<'a, O: Outboard, R: ReadAt> RecursiveValidator<'a, O, R> {
        fn validate_rec(
            &mut self,
            parent_hash: &blake3::Hash,
            node: TreeNode,
            is_root: bool,
        ) -> io::Result<()> {
            if let Some((l_hash, r_hash)) = self.outboard.load(node)? {
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                if &actual != parent_hash {
                    // we got a validation error. Simply continue without adding the range
                    return Ok(());
                }
                if node.is_leaf() {
                    let (s, m, e) = self.tree.leaf_byte_ranges3(node);
                    let l_data = read_range(&mut self.reader, s..m, &mut self.buffer)?;
                    let actual = hash_subtree(s.chunks().0, l_data, false);
                    if actual == l_hash {
                        self.res |= ChunkRanges::from(s.chunks()..m.chunks());
                    }

                    let r_data = read_range(&mut self.reader, m..e, &mut self.buffer)?;
                    let actual = hash_subtree(m.chunks().0, r_data, false);
                    if actual == r_hash {
                        self.res |= ChunkRanges::from(m.chunks()..e.chunks());
                    }
                } else {
                    // recurse
                    let left = node.left_child().unwrap();
                    self.validate_rec(&l_hash, left, false)?;
                    let right = node.right_descendant(self.valid_nodes).unwrap();
                    self.validate_rec(&r_hash, right, false)?;
                }
            } else if node.is_leaf() {
                let (s, m, _) = self.tree.leaf_byte_ranges3(node);
                let l_data = read_range(&mut self.reader, s..m, &mut self.buffer)?;
                let actual = hash_subtree(s.chunks().0, l_data, is_root);
                if actual == *parent_hash {
                    self.res |= ChunkRanges::from(s.chunks()..m.chunks());
                }
            };
            Ok(())
        }
    }
    let tree = outboard.tree();
    let root_hash = outboard.root();
    let mut validator = RecursiveValidator {
        tree,
        valid_nodes: tree.filled_size(),
        res: ChunkRanges::empty(),
        outboard,
        reader,
        buffer: vec![0; tree.block_size.bytes()],
    };
    validator.validate_rec(&root_hash, tree.root(), true)?;
    Ok(validator.res)
}
