//! Syncronous IO
use std::{
    io::{self, Read, Write},
    ops::Range,
    result,
};

use crate::{
    blake3,
    io::error::{AnyDecodeError, EncodeError},
    io::{
        outboard::{parse_hash_pair, PostOrderMemOutboard, PostOrderOutboard, PreOrderOutboard},
        Header, Leaf, Parent,
    },
    iter::{BaoChunk, ResponseChunk},
    BaoTree, BlockSize, ByteNum, ChunkNum, TreeNode,
};
use crate::{hash_subtree, iter::ResponseIterRef};
use blake3::guts::parent_cv;
use bytes::BytesMut;
pub use positioned_io::{ReadAt, Size, WriteAt};
use range_collections::{range_set::RangeSetRange, RangeSet2, RangeSetRef};
use smallvec::SmallVec;

use super::{DecodeError, StartDecodeError};

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}

/// An item of a decode response
#[derive(Debug)]
pub enum DecodeResponseItem {
    /// We got the header and now know how big the overall size is
    ///
    /// Actually this is just how big the remote side *claims* the overall size is.
    /// In an adversarial setting, this could be wrong.
    Header(Header),
    /// a parent node, to update the outboard
    Parent(Parent),
    /// a leaf node, to write to the file
    Leaf(Leaf),
}

impl From<Header> for DecodeResponseItem {
    fn from(h: Header) -> Self {
        Self::Header(h)
    }
}

impl From<Parent> for DecodeResponseItem {
    fn from(p: Parent) -> Self {
        Self::Parent(p)
    }
}

impl From<Leaf> for DecodeResponseItem {
    fn from(l: Leaf) -> Self {
        Self::Leaf(l)
    }
}

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
/// This trait extends [Outboard] with methods to save a hash pair for a node and to set the
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
    pub fn new(root: blake3::Hash, block_size: BlockSize, data: R) -> io::Result<Self> {
        let mut content = [0u8; 8];
        data.read_exact_at(0, &mut content)?;
        let len = ByteNum(u64::from_le_bytes(content[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, block_size);
        let expected_outboard_size = super::outboard_size(len.0, block_size);
        let size = data.size()?;
        if size != Some(expected_outboard_size) {
            io_error!(
                "Expected outboard size of {} bytes, but got {} bytes",
                expected_outboard_size,
                size.map(|s| s.to_string()).unwrap_or("unknown".to_string())
            );
        }
        // zero pad the rest, if needed.
        Ok(Self { root, tree, data })
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
        let offset = offset * 64 + 8;
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
        let offset = offset * 64 + 8;
        let mut content = [0u8; 64];
        content[0..32].copy_from_slice(hash_pair.0.as_bytes());
        content[32..64].copy_from_slice(hash_pair.1.as_bytes());
        self.data.write_all_at(offset, &content)?;
        Ok(())
    }
}

impl<R: ReadAt + Size> PostOrderOutboard<R> {
    /// Create a new outboard from a reader, root hash, and block size.
    pub fn new(root: blake3::Hash, block_size: BlockSize, data: R) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        let Some(outboard_size) = data.size()? else {
            io_error!("outboard must have a known size");
        };
        if outboard_size < 8 {
            io_error!("outboard is too short");
        };
        let mut suffix = [0u8; 8];
        data.read_exact_at(outboard_size - 8, &mut suffix)?;
        let len = u64::from_le_bytes(suffix);
        let expected_outboard_size = super::outboard_size(len, block_size);
        if outboard_size != expected_outboard_size {
            io_error!(
                "Expected outboard size of {} bytes, but got {} bytes",
                expected_outboard_size,
                outboard_size
            );
        }
        let tree = BaoTree::new(ByteNum(len), block_size);
        Ok(Self { root, tree, data })
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
        let offset = offset.value() * 64 + 8;
        let mut content = [0u8; 64];
        self.data.read_exact_at(offset, &mut content)?;
        Ok(Some(parse_hash_pair(content)))
    }
}

impl PostOrderMemOutboard {
    /// Load a post-order outboard from a reader, root hash, and block size.
    pub fn load(
        root: blake3::Hash,
        data: impl ReadAt + Size,
        block_size: BlockSize,
    ) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        let Some(size) = data.size()? else {
            io_error!("outboard must have a known size");
        };
        let Ok(size) = usize::try_from(size) else {
            io_error!("outboard size must be less than usize::MAX");
        };
        let mut outboard = vec![0; size];
        data.read_exact_at(0, &mut outboard)?;
        if outboard.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let suffix = &outboard[outboard.len() - 8..];
        let len = u64::from_le_bytes(suffix.try_into().unwrap());
        let expected_outboard_size = super::outboard_size(len, block_size);
        let outboard_size = outboard.len() as u64;
        if outboard_size != expected_outboard_size {
            io_error!(
                "outboard length does not match expected outboard length: {outboard_size} != {expected_outboard_size}"                
            );
        }
        let tree = BaoTree::new(ByteNum(len), block_size);
        outboard.truncate(outboard.len() - 8);
        Ok(Self::new(root, tree, outboard))
    }
}

/// Given an outboard, return a range set of all valid ranges
pub fn valid_ranges<O>(outboard: &O) -> io::Result<RangeSet2<ChunkNum>>
where
    O: Outboard,
{
    struct RecursiveValidator<'a, O: Outboard> {
        tree: BaoTree,
        valid_nodes: TreeNode,
        res: RangeSet2<ChunkNum>,
        outboard: &'a O,
    }

    impl<'a, O: Outboard> RecursiveValidator<'a, O> {
        fn validate_rec(
            &mut self,
            parent_hash: &blake3::Hash,
            node: TreeNode,
            is_root: bool,
        ) -> io::Result<()> {
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
            if let Some(leaf) = node.as_leaf() {
                let start = self.tree.chunk_num(leaf);
                let end = (start + self.tree.chunk_group_chunks() * 2).min(self.tree.chunks());
                self.res |= RangeSet2::from(start..end);
            } else {
                // recurse
                let left = node.left_child().unwrap();
                self.validate_rec(&l_hash, left, false)?;
                let right = node.right_descendant(self.valid_nodes).unwrap();
                self.validate_rec(&r_hash, right, false)?;
            }
            Ok(())
        }
    }
    let tree = outboard.tree();
    let root_hash = outboard.root();
    let mut validator = RecursiveValidator {
        tree,
        valid_nodes: tree.filled_size(),
        res: RangeSet2::empty(),
        outboard,
    };
    validator.validate_rec(&root_hash, tree.root(), true)?;
    Ok(validator.res)
}

// When this enum is used it is in the Header variant for the first 8 bytes, then stays in
// the Content state for the remainder.  Since the Content is the largest part that this
// size inbalance is fine, hence allow clippy::large_enum_variant.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum Position<'a> {
    /// currently reading the header, so don't know how big the tree is
    /// so we need to store the ranges and the chunk group log
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    },
    /// currently reading the tree, all the info we need is in the iter
    Content { iter: ResponseIterRef<'a> },
}

/// Iterator that can be used to decode a response to a range request
#[derive(Debug)]
pub struct DecodeResponseIter<'a, R> {
    inner: Position<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: BytesMut,
}

impl<'a, R: Read> DecodeResponseIter<'a, R> {
    /// Create a new iterator to decode a response.
    ///
    /// For decoding you need to know the root hash, block size, and the ranges that were requested.
    /// Additionally you need to provide a reader that can be used to read the encoded data, and
    /// a buffer to use for decoding.
    pub fn new(
        root: blake3::Hash,
        block_size: BlockSize,
        encoded: R,
        ranges: &'a RangeSetRef<ChunkNum>,
        buf: BytesMut,
    ) -> Self {
        let mut stack = SmallVec::new();
        stack.push(root);
        Self {
            stack,
            inner: Position::Header { ranges, block_size },
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
    pub fn tree(&self) -> Option<&BaoTree> {
        match &self.inner {
            Position::Content { iter } => Some(iter.tree()),
            Position::Header { .. } => None,
        }
    }

    fn next0(&mut self) -> result::Result<Option<DecodeResponseItem>, AnyDecodeError> {
        let inner = match &mut self.inner {
            Position::Content { ref mut iter } => iter,
            Position::Header { block_size, ranges } => {
                let size =
                    read_len(&mut self.encoded).map_err(StartDecodeError::maybe_not_found)?;
                let tree = BaoTree::new(size, *block_size);
                for elem in ResponseIterRef::new(tree, ranges) {
                    println!("{:?}", elem);
                    match elem {
                        ResponseChunk::Leaf {
                            start_chunk, size, ..
                        } => {
                            println!(
                                "{:?}",
                                start_chunk.to_bytes().0..start_chunk.to_bytes().0 + (size as u64)
                            )
                        }
                        ResponseChunk::Parent { node, .. } => {
                            println!("{:?}", tree.byte_range(node))
                        }
                    }
                }
                self.inner = Position::Content {
                    iter: ResponseIterRef::new(tree, ranges),
                };
                return Ok(Some(Header { size }.into()));
            }
        };
        match inner.next() {
            Some(ResponseChunk::Parent {
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
            Some(ResponseChunk::Leaf {
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
                    println!("{}", hex::encode(&self.buf));
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
    type Item = result::Result<DecodeResponseItem, AnyDecodeError>;

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
    ranges: &RangeSetRef<ChunkNum>,
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
                encoded.write_all(l_hash.as_bytes())?;
                encoded.write_all(r_hash.as_bytes())?;
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

/// Encode ranges relevant to a query from a slice and outboard to a buffer
///
/// This will compute the root hash, so it will have to traverse the entire tree.
/// The `ranges` parameter just controls which parts of the data are written.
///
/// Except for writing to a buffer, this is the same as [hash_subtree].
pub(crate) fn bao_encode_selected_recursive(
    start_chunk: ChunkNum,
    data: &[u8],
    is_root: bool,
    query: &RangeSetRef<ChunkNum>,
    max_skip_level: u32,
    res: &mut Vec<u8>,
) -> blake3::Hash {
    use blake3::guts::{ChunkState, CHUNK_LEN};
    if data.len() <= CHUNK_LEN {
        if !query.is_empty() {
            res.extend_from_slice(data);
        }
        let mut hasher = ChunkState::new(start_chunk.0);
        hasher.update(data);
        hasher.finalize(is_root)
    } else {
        let chunks = data.len() / CHUNK_LEN + (data.len() % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let level = chunks.trailing_zeros();
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let mid_chunk = start_chunk + (mid as u64);
        let (l_ranges, r_ranges) = query.split(mid_chunk);
        // for empty ranges, we don't want to emit anything.
        // for full ranges where the level is at or below max_skip_level, we want to emit
        // just the data.
        //
        // todo: maybe call into blake3::guts::hash_subtree directly for this case? it would be faster.
        #[allow(clippy::nonminimal_bool)]
        let emit_parent = !query.is_empty() && !(query.is_all() && level <= max_skip_level);
        let hash_offset = if emit_parent {
            // make some room for the hashes
            res.extend_from_slice(&[0xFFu8; 64]);
            Some(res.len() - 64)
        } else {
            None
        };
        // recurse to the left and right to compute the hashes and emit data
        let left = bao_encode_selected_recursive(
            start_chunk,
            &data[..mid_bytes],
            false,
            l_ranges,
            max_skip_level,
            res,
        );
        let right = bao_encode_selected_recursive(
            mid_chunk,
            &data[mid_bytes..],
            false,
            r_ranges,
            max_skip_level,
            res,
        );
        // backfill the hashes if needed
        if let Some(o) = hash_offset {
            res[o..o + 32].copy_from_slice(left.as_bytes());
            res[o + 32..o + 64].copy_from_slice(right.as_bytes());
        }
        blake3::guts::parent_cv(&left, &right, is_root)
    }
}

/// Select nodes relevant to a query
///
/// This is the receive side equivalent of [bao_encode_selected_recursive].
/// It does not do any hashing, but just emits the nodes that are relevant to a query.
pub(crate) fn select_nodes_recursive(
    start_chunk: ChunkNum,
    size: usize,
    is_root: bool,
    ranges: &RangeSetRef<ChunkNum>,
    max_skip_level: u32,
    emit: &mut impl FnMut(ResponseChunk),
) {
    if ranges.is_empty() {
        return;
    }
    use blake3::guts::CHUNK_LEN;
    if size <= CHUNK_LEN {
        emit(ResponseChunk::Leaf {
            start_chunk,
            size,
            is_root,
        });
    } else {
        let chunks: usize = size / CHUNK_LEN + (size % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let level = chunks.trailing_zeros();
        if ranges.is_all() && level <= max_skip_level {
            // we are allowed to just emit the entire data as a leaf
            emit(ResponseChunk::Leaf {
                start_chunk,
                size,
                is_root,
            });
        } else {
            // split in half and recurse
            let mid = chunks / 2;
            let mid_bytes = mid * CHUNK_LEN;
            let mid_chunk = start_chunk + (mid as u64);
            let (l_ranges, r_ranges) = ranges.split(mid_chunk);
            emit(ResponseChunk::Parent {
                node: TreeNode(0), // todo
                is_root,
                left: !l_ranges.is_empty(),
                right: !r_ranges.is_empty(),
                is_subchunk: true,
            });
            // recurse to the left and right to compute the hashes and emit data
            select_nodes_recursive(
                start_chunk,
                mid_bytes,
                false,
                l_ranges,
                max_skip_level,
                emit,
            );
            select_nodes_recursive(
                mid_chunk,
                size - mid_bytes,
                false,
                r_ranges,
                max_skip_level,
                emit,
            );
        }
    }
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
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError> {
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let data = data;
    let mut encoded = encoded;
    let tree = outboard.tree();
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let mut out_buf = Vec::new();
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
                let mut pair = [0u8; 64];
                pair[..32].copy_from_slice(l_hash.as_bytes());
                pair[32..].copy_from_slice(r_hash.as_bytes());
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
                    let actual = bao_encode_selected_recursive(
                        start_chunk,
                        buf,
                        is_root,
                        ranges,
                        tree.block_size.0 as u32,
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
pub async fn decode_response_into<R, O, W>(
    root: blake3::Hash,
    block_size: BlockSize,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: R,
    create: impl FnOnce(BaoTree, blake3::Hash) -> io::Result<O>,
    mut target: W,
) -> io::Result<Option<O>>
where
    O: OutboardMut,
    R: Read,
    W: WriteAt,
{
    let buffer = BytesMut::with_capacity(block_size.bytes());
    let iter = DecodeResponseIter::new(root, block_size, encoded, ranges, buffer);
    let mut outboard = None;
    let mut tree = None;
    let mut create = Some(create);
    for item in iter {
        match item? {
            DecodeResponseItem::Header(Header { size }) => {
                tree = Some(BaoTree::new(size, block_size));
            }
            DecodeResponseItem::Parent(Parent { node, pair }) => {
                let outboard = if let Some(outboard) = outboard.as_mut() {
                    outboard
                } else {
                    let create = create.take().unwrap();
                    outboard = Some(create(tree.take().unwrap(), root)?);
                    outboard.as_mut().unwrap()
                };
                outboard.save(node, &pair)?;
            }
            DecodeResponseItem::Leaf(Leaf { offset, data }) => {
                target.write_all_at(offset.0, &data)?;
            }
        }
    }
    Ok(outboard)
}

/// Write ranges from memory to disk
///
/// This is useful for writing changes to outboards.
/// Note that it is up to you to call flush.
pub fn write_ranges(
    from: impl AsRef<[u8]>,
    mut to: impl WriteAt,
    ranges: &RangeSetRef<u64>,
) -> io::Result<()> {
    let from = from.as_ref();
    let end = from.len() as u64;
    for range in ranges.iter() {
        let range = match range {
            RangeSetRange::RangeFrom(x) => *x.start..end,
            RangeSetRange::Range(x) => *x.start..*x.end,
        };
        let start = usize::try_from(range.start).unwrap();
        let end = usize::try_from(range.end).unwrap();
        to.write_all_at(range.start, &from[start..end])?;
    }
    Ok(())
}

/// Compute the post order outboard for the given data, writing into a io::Write
pub fn outboard_post_order(
    data: impl Read,
    size: u64,
    block_size: BlockSize,
    mut outboard: impl Write,
) -> io::Result<blake3::Hash> {
    let tree = BaoTree::new(ByteNum(size), block_size);
    let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
    let hash = outboard_post_order_impl(tree, data, &mut outboard, &mut buffer)?;
    outboard.write_all(&size.to_le_bytes())?;
    Ok(hash)
}

/// Compute the post order outboard for the given data
///
/// This is the internal version that takes a start chunk and does not append the size!
pub(crate) fn outboard_post_order_impl(
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

fn read_len(mut from: impl Read) -> std::io::Result<ByteNum> {
    let mut buf = [0; 8];
    from.read_exact(&mut buf)?;
    let len = ByteNum(u64::from_le_bytes(buf));
    Ok(len)
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
pub fn valid_file_ranges<O, R>(outboard: &O, reader: R) -> io::Result<RangeSet2<ChunkNum>>
where
    O: Outboard,
    R: ReadAt,
{
    struct RecursiveValidator<'a, O: Outboard, R: ReadAt> {
        tree: BaoTree,
        valid_nodes: TreeNode,
        res: RangeSet2<ChunkNum>,
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
                if let Some(leaf) = node.as_leaf() {
                    let (s, m, e) = self.tree.leaf_byte_ranges3(leaf);
                    let l_data = read_range(&mut self.reader, s..m, &mut self.buffer)?;
                    let actual = hash_subtree(s.chunks().0, l_data, false);
                    if actual == l_hash {
                        self.res |= RangeSet2::from(s.chunks()..m.chunks());
                    }

                    let r_data = read_range(&mut self.reader, m..e, &mut self.buffer)?;
                    let actual = hash_subtree(m.chunks().0, r_data, false);
                    if actual == r_hash {
                        self.res |= RangeSet2::from(m.chunks()..e.chunks());
                    }
                } else {
                    // recurse
                    let left = node.left_child().unwrap();
                    self.validate_rec(&l_hash, left, false)?;
                    let right = node.right_descendant(self.valid_nodes).unwrap();
                    self.validate_rec(&r_hash, right, false)?;
                }
            } else if let Some(leaf) = node.as_leaf() {
                let (s, m, _) = self.tree.leaf_byte_ranges3(leaf);
                let l_data = read_range(&mut self.reader, s..m, &mut self.buffer)?;
                let actual = hash_subtree(s.chunks().0, l_data, is_root);
                if actual == *parent_hash {
                    self.res |= RangeSet2::from(s.chunks()..m.chunks());
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
        res: RangeSet2::empty(),
        outboard,
        reader,
        buffer: vec![0; tree.block_size.bytes()],
    };
    validator.validate_rec(&root_hash, tree.root(), true)?;
    Ok(validator.res)
}
