//! Async (tokio) io, written in fsm style
//!
//! IO ops are written as async state machines that thread the state through the
//! futures to avoid being encumbered by lifetimes.
//!
//! This makes them occasionally a bit verbose to use, but allows being generic
//! without having to box the futures.
use std::{io, result};

use crate::blake3;
use blake3::guts::{hash_subtree, parent_cv};
use bytes::{Bytes, BytesMut};
use futures::{future::LocalBoxFuture, Future, FutureExt};
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    io::{
        error::{DecodeError, EncodeError},
        outboard::{PostOrderOutboard, PreOrderOutboard},
        Leaf, Parent,
    },
    iter::{BaoChunk, PreOrderChunkIter},
    range_ok, BaoTree, BlockSize, ByteNum, ChunkNum, TreeNode,
};
pub use iroh_io::{AsyncSliceReader, AsyncSliceWriter};

/// An item of bao content
///
/// We know that we are not going to get headers after the first item.
#[derive(Debug)]
pub enum BaoContentItem {
    /// a parent node, to update the outboard
    Parent(Parent),
    /// a leaf node, to write to the file
    Leaf(Leaf),
}

impl From<Parent> for BaoContentItem {
    fn from(p: Parent) -> Self {
        Self::Parent(p)
    }
}

impl From<Leaf> for BaoContentItem {
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
///
/// The async version takes a mutable reference to load, not because it mutates
/// the outboard (it doesn't), but to ensure that there is at most one outstanding
/// load at a time.
///
/// Dropping the load future without polling it to completion is safe, but will
/// possibly render the outboard unusable.
pub trait Outboard {
    /// The root hash
    fn root(&self) -> blake3::Hash;
    /// The tree. This contains the information about the size of the file and the block size.
    fn tree(&self) -> BaoTree;
    /// Future returned by `load`
    type LoadFuture<'a>: Future<Output = io::Result<Option<(blake3::Hash, blake3::Hash)>>>
    where
        Self: 'a;
    /// load the hash pair for a node
    ///
    /// This takes a &mut self not because it mutates the outboard (it doesn't),
    /// but to ensure that there is only one outstanding load at a time.
    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_>;
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
    /// Future returned by `save`
    type SaveFuture<'a>: Future<Output = io::Result<()>>
    where
        Self: 'a;

    /// Save a hash pair for a node
    fn save<'a>(
        &'a mut self,
        node: TreeNode,
        hash_pair: &'a (blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'a>;

    /// Future returned by `sync`
    type SyncFuture<'a>: Future<Output = io::Result<()>>
    where
        Self: 'a;

    /// sync to disk
    fn sync(&mut self) -> Self::SyncFuture<'_>;
}

impl<'b, O: Outboard> Outboard for &'b mut O {
    type LoadFuture<'a> = <O as Outboard>::LoadFuture<'a> where O: 'a, 'b: 'a;

    fn root(&self) -> blake3::Hash {
        (**self).root()
    }

    fn tree(&self) -> BaoTree {
        (**self).tree()
    }

    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        (**self).load(node)
    }
}

impl<R: AsyncSliceReader> Outboard for PreOrderOutboard<R> {
    type LoadFuture<'a> = LocalBoxFuture<'a, io::Result<Option<(blake3::Hash, blake3::Hash)>>>
        where R: 'a;

    fn root(&self) -> blake3::Hash {
        self.root
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        async move {
            let Some(offset) = self.tree.pre_order_offset(node) else {
                return Ok(None);
            };
            let offset = offset * 64 + 8;
            let content = self.data.read_at(offset, 64).await?;
            Ok(Some(if content.len() != 64 {
                (blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32]))
            } else {
                parse_hash_pair(content)?
            }))
        }
        .boxed_local()
    }
}

impl<W: AsyncSliceWriter> OutboardMut for PreOrderOutboard<W> {
    type SaveFuture<'a> = LocalBoxFuture<'a, io::Result<()>>
        where W: 'a;

    fn save<'a>(
        &'a mut self,
        node: TreeNode,
        hash_pair: &'a (blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'_> {
        async move {
            let Some(offset) = self.tree.pre_order_offset(node) else {
                return Ok(());
            };
            let offset = offset * 64 + 8;
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(hash_pair.0.as_bytes());
            buf[32..].copy_from_slice(hash_pair.1.as_bytes());
            self.data.write_at(offset, &buf).await?;
            Ok(())
        }
        .boxed_local()
    }

    type SyncFuture<'a> = W::SyncFuture<'a> where W: 'a;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        self.data.sync()
    }
}

impl<R: AsyncSliceReader> Outboard for PostOrderOutboard<R> {
    type LoadFuture<'a> = LocalBoxFuture<'a, io::Result<Option<(blake3::Hash, blake3::Hash)>>>
        where R: 'a;

    fn root(&self) -> blake3::Hash {
        self.root
    }

    fn tree(&self) -> BaoTree {
        self.tree
    }

    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        async move {
            let Some(offset) = self.tree.post_order_offset(node) else {
                return Ok(None);
            };
            let offset = offset.value() * 64;
            let content = self.data.read_at(offset, 64).await?;
            Ok(Some(if content.len() != 64 {
                (blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32]))
            } else {
                parse_hash_pair(content)?
            }))
        }
        .boxed_local()
    }
}

pub(crate) fn parse_hash_pair(buf: Bytes) -> io::Result<(blake3::Hash, blake3::Hash)> {
    if buf.len() != 64 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "hash pair must be 64 bytes",
        ));
    }
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..]).unwrap());
    Ok((l_hash, r_hash))
}

/// Response decoder state machine, at the start of a stream
#[derive(Debug)]
pub struct ResponseDecoderStart<R> {
    ranges: RangeSet2<ChunkNum>,
    block_size: BlockSize,
    hash: blake3::Hash,
    encoded: R,
}

impl<'a, R: AsyncRead + Unpin> ResponseDecoderStart<R> {
    /// Create a new response decoder state machine, at the start of a stream
    /// where you don't yet know the size.
    pub fn new(
        hash: blake3::Hash,
        ranges: RangeSet2<ChunkNum>,
        block_size: BlockSize,
        encoded: R,
    ) -> Self {
        Self {
            ranges,
            block_size,
            hash,
            encoded,
        }
    }

    /// Immediately finish decoding the stream, returning the underlying reader
    pub fn finish(self) -> R {
        self.encoded
    }

    /// Read the size and go into the next state
    ///
    /// The only thing that can go wrong here is an io error when reading the size.
    pub async fn next(self) -> std::result::Result<(ResponseDecoderReading<R>, u64), io::Error> {
        let Self {
            ranges,
            block_size,
            hash,
            mut encoded,
        } = self;
        let size = encoded.read_u64_le().await?;
        let tree = BaoTree::new(ByteNum(size), block_size);
        let state = ResponseDecoderReading(Box::new(ResponseDecoderReadingInner::new(
            tree, hash, ranges, encoded,
        )));
        Ok((state, size))
    }

    /// Hash of the blob we are currently getting
    pub fn hash(&self) -> &blake3::Hash {
        &self.hash
    }

    /// The ranges we requested
    pub fn ranges(&self) -> &RangeSet2<ChunkNum> {
        &self.ranges
    }
}

#[derive(Debug)]
struct ResponseDecoderReadingInner<R> {
    iter: PreOrderChunkIter,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: BytesMut,
}

impl<R> ResponseDecoderReadingInner<R> {
    fn new(tree: BaoTree, hash: blake3::Hash, ranges: RangeSet2<ChunkNum>, encoded: R) -> Self {
        let mut res = Self {
            iter: PreOrderChunkIter::new(tree, ranges),
            stack: SmallVec::new(),
            encoded,
            buf: BytesMut::with_capacity(tree.chunk_group_bytes().to_usize()),
        };
        res.stack.push(hash);
        res
    }
}

/// Response decoder state machine, after reading the size
#[derive(Debug)]
pub struct ResponseDecoderReading<R>(Box<ResponseDecoderReadingInner<R>>);

/// Next type for ResponseDecoderReading.
#[derive(Debug)]
pub enum ResponseDecoderReadingNext<R> {
    /// One more item, and you get back the state machine in the next state
    More(
        (
            ResponseDecoderReading<R>,
            std::result::Result<BaoContentItem, DecodeError>,
        ),
    ),
    /// The stream is done, you get back the underlying reader
    Done(R),
}

impl<R: AsyncRead + Unpin> ResponseDecoderReading<R> {
    /// Create a new response decoder state machine, when you have already read the size.
    ///
    /// The size as well as the chunk size is given in the `tree` parameter.
    pub fn new(hash: blake3::Hash, ranges: RangeSet2<ChunkNum>, tree: BaoTree, encoded: R) -> Self {
        let mut stack = SmallVec::new();
        stack.push(hash);
        Self(Box::new(ResponseDecoderReadingInner {
            iter: PreOrderChunkIter::new(tree, ranges),
            stack,
            encoded,
            buf: BytesMut::new(),
        }))
    }

    /// Proceed to the next state by reading the next chunk from the stream.
    pub async fn next(mut self) -> ResponseDecoderReadingNext<R> {
        if let Some(chunk) = self.0.iter.next() {
            let item = self.next0(chunk).await;
            ResponseDecoderReadingNext::More((self, item))
        } else {
            ResponseDecoderReadingNext::Done(self.0.encoded)
        }
    }

    /// Immediately return the underlying reader
    pub fn finish(self) -> R {
        self.0.encoded
    }

    /// The tree geometry
    pub fn tree(&self) -> &BaoTree {
        self.0.iter.tree()
    }

    /// Hash of the blob we are currently getting
    pub fn hash(&self) -> &blake3::Hash {
        &self.0.stack[0]
    }

    async fn next0(&mut self, chunk: BaoChunk) -> std::result::Result<BaoContentItem, DecodeError> {
        Ok(match chunk {
            BaoChunk::Parent {
                is_root,
                right,
                left,
                node,
            } => {
                let mut buf = [0u8; 64];
                let this = &mut self.0;
                this.encoded.read_exact(&mut buf).await?;
                let pair @ (l_hash, r_hash) = read_parent(&buf);
                let parent_hash = this.stack.pop().unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                // Push the children in reverse order so they are popped in the correct order
                // only push right if the range intersects with the right child
                if right {
                    this.stack.push(r_hash);
                }
                // only push left if the range intersects with the left child
                if left {
                    this.stack.push(l_hash);
                }
                // Validate after pushing the children so that we could in principle continue
                if parent_hash != actual {
                    return Err(DecodeError::ParentHashMismatch(node));
                }
                Parent { pair, node }.into()
            }
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
            } => {
                // this will resize always to chunk group size, except for the last chunk
                let this = &mut self.0;
                this.buf.resize(size, 0u8);
                this.encoded.read_exact(&mut this.buf).await?;
                let leaf_hash = this.stack.pop().unwrap();
                let actual = hash_subtree(start_chunk.0, &this.buf, is_root);
                if leaf_hash != actual {
                    return Err(DecodeError::LeafHashMismatch(start_chunk));
                }
                Leaf {
                    offset: start_chunk.to_bytes(),
                    data: self.0.buf.split().freeze(),
                }
                .into()
            }
        })
    }
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This will not validate on writing, so data corruption will be detected on reading
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub async fn encode_ranges<D, O, W>(
    mut data: D,
    mut outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError>
where
    D: AsyncSliceReader,
    O: Outboard,
    W: AsyncWrite + Unpin,
{
    let mut encoded = encoded;
    let tree = outboard.tree();
    if !range_ok(ranges, tree.chunks()) {
        return Err(EncodeError::InvalidQueryRange);
    }
    // write header
    encoded
        .write_all(tree.size.0.to_le_bytes().as_slice())
        .await?;
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent { node, .. } => {
                let (l_hash, r_hash) = outboard.load(node).await?.unwrap();
                encoded.write_all(l_hash.as_bytes()).await?;
                encoded.write_all(r_hash.as_bytes()).await?;
            }
            BaoChunk::Leaf {
                start_chunk, size, ..
            } => {
                let start = start_chunk.to_bytes();
                let bytes = data.read_at(start.0, size).await?;
                encoded.write_all(&bytes).await?;
            }
        }
    }
    Ok(())
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This function validates the data before writing
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub async fn encode_ranges_validated<D, O, W>(
    mut data: D,
    mut outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError>
where
    D: AsyncSliceReader,
    O: Outboard,
    W: AsyncWrite + Unpin,
{
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let mut encoded = encoded;
    let tree = outboard.tree();
    if !range_ok(ranges, tree.chunks()) {
        return Err(EncodeError::InvalidQueryRange);
    }
    // write header
    encoded
        .write_all(tree.size.0.to_le_bytes().as_slice())
        .await?;
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent {
                is_root,
                left,
                right,
                node,
            } => {
                let (l_hash, r_hash) = outboard.load(node).await?.unwrap();
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
                encoded.write_all(l_hash.as_bytes()).await?;
                encoded.write_all(r_hash.as_bytes()).await?;
            }
            BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
            } => {
                let expected = stack.pop().unwrap();
                let start = start_chunk.to_bytes();
                let bytes = data.read_at(start.0, size).await?;
                let actual = hash_subtree(start_chunk.0, &bytes, is_root);
                if actual != expected {
                    return Err(EncodeError::LeafHashMismatch(start_chunk));
                }
                encoded.write_all(&bytes).await?;
            }
        }
    }
    Ok(())
}

/// Decode a response into a file while updating an outboard.
///
/// If you do not want to update an outboard, use [super::outboard::EmptyOutboard] as
/// the outboard.
pub async fn decode_response_into<R, O, W, F, Fut>(
    root: blake3::Hash,
    block_size: BlockSize,
    ranges: RangeSet2<ChunkNum>,
    encoded: R,
    create: F,
    mut target: W,
) -> io::Result<Option<O>>
where
    O: OutboardMut,
    R: AsyncRead + Unpin,
    W: AsyncSliceWriter,
    F: FnOnce(blake3::Hash, BaoTree) -> Fut,
    Fut: Future<Output = io::Result<O>>,
{
    let start = ResponseDecoderStart::new(root, ranges, block_size, encoded);
    let (mut reading, _size) = start.next().await?;
    let mut outboard = None;
    let mut create = Some(create);
    loop {
        let item = match reading.next().await {
            ResponseDecoderReadingNext::Done(_reader) => break,
            ResponseDecoderReadingNext::More((next, item)) => {
                reading = next;
                item?
            }
        };
        match item {
            BaoContentItem::Parent(Parent { node, pair }) => {
                let outboard = if let Some(outboard) = outboard.as_mut() {
                    outboard
                } else {
                    let tree = reading.tree();
                    let create = create.take().unwrap();
                    let new = create(root, *tree).await?;
                    outboard = Some(new);
                    outboard.as_mut().unwrap()
                };
                outboard.save(node, &pair).await?;
            }
            BaoContentItem::Leaf(Leaf { offset, data }) => {
                target.write_bytes_at(offset.0, data).await?;
            }
        }
    }
    Ok(outboard)
}
fn read_parent(buf: &[u8]) -> (blake3::Hash, blake3::Hash) {
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..64]).unwrap());
    (l_hash, r_hash)
}

/// Given an outboard, return a range set of all valid ranges
pub async fn valid_ranges<O>(outboard: &mut O) -> io::Result<RangeSet2<ChunkNum>>
where
    O: Outboard,
{
    struct RecursiveValidator<'a, O: Outboard> {
        tree: BaoTree,
        valid_nodes: TreeNode,
        res: RangeSet2<ChunkNum>,
        outboard: &'a mut O,
    }

    impl<'a, O: Outboard> RecursiveValidator<'a, O> {
        fn validate_rec<'b>(
            &'b mut self,
            parent_hash: &'b blake3::Hash,
            node: TreeNode,
            is_root: bool,
        ) -> LocalBoxFuture<'b, io::Result<()>> {
            async move {
                // if there is an IO error reading the hash, we simply continue without adding the range
                let (l_hash, r_hash) =
                    if let Some((l_hash, r_hash)) = self.outboard.load(node).await? {
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
                    self.validate_rec(&l_hash, left, false).await?;
                    let right = node.right_descendant(self.valid_nodes).unwrap();
                    self.validate_rec(&r_hash, right, false).await?;
                }
                Ok(())
            }
            .boxed_local()
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
    validator
        .validate_rec(&root_hash, tree.root(), true)
        .await?;
    Ok(validator.res)
}
