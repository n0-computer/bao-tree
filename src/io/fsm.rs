//! Async (tokio) io, written in fsm style
//!
//! IO ops are written as async state machines that thread the state through the
//! futures to avoid being encumbered by lifetimes.
//!
//! This makes them occasionally a bit verbose to use, but allows being generic
//! without having to box the futures.
use std::{io, result};

use blake3::guts::parent_cv;
use bytes::{Bytes, BytesMut};
use futures::{future::LocalBoxFuture, Future, FutureExt};
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    hash_block,
    io::{
        error::{DecodeError, EncodeError},
        Leaf, Parent,
    },
    iter::{BaoChunk, PreOrderChunkIter},
    outboard::{PostOrderOutboard, PreOrderOutboard},
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
/// This is implemented for a generic io object in [crate::outboard::PreOrderOutboard]
/// and for a memory region in [crate::outboard::PreOrderMemOutboard].
///
/// For files that grow over time, it is more efficient to store the hashes in post order.
/// This is implemented for a generic io object in [crate::outboard::PostOrderOutboard]
/// and for a memory region in [crate::outboard::PostOrderMemOutboard].
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
/// implementation [crate::outboard::EmptyOutboard].
pub trait OutboardMut: Outboard {
    /// Future returned by `save`
    type SaveFuture<'a>: Future<Output = io::Result<()>>
    where
        Self: 'a;

    /// Save a hash pair for a node
    fn save(
        &mut self,
        node: TreeNode,
        hash_pair: &(blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'_>;

    /// Future returned by `set_size`
    type SetSizeFuture<'a>: Future<Output = io::Result<()>>
    where
        Self: 'a;

    /// Set the length of the file for which this outboard is
    fn set_size(&mut self, len: ByteNum) -> Self::SetSizeFuture<'_>;
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
            Ok(Some(parse_hash_pair(content)?))
        }
        .boxed_local()
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
            Ok(Some(parse_hash_pair(content)?))
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
                let actual = hash_block(start_chunk, &this.buf, is_root);
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
pub async fn encode_ranges<D, O, W>(
    data: &mut D,
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
    let file_len = data.len().await?;
    let tree = outboard.tree();
    let ob_len = tree.size;
    if file_len != ob_len {
        return Err(EncodeError::SizeMismatch);
    }
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
pub async fn encode_ranges_validated<D, O, W>(
    data: &mut D,
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
    let file_len = ByteNum(data.len().await?);
    let tree = outboard.tree();
    let ob_len = tree.size;
    if file_len != ob_len {
        return Err(EncodeError::SizeMismatch);
    }
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
                let actual = hash_block(start_chunk, &bytes, is_root);
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
/// If you do not want to update an outboard, use [crate::outboard::EmptyOutboard] as
/// the outboard.
pub async fn decode_response_into<R, O, W>(
    ranges: RangeSet2<ChunkNum>,
    encoded: R,
    mut outboard: O,
    mut target: W,
) -> io::Result<()>
where
    O: OutboardMut,
    R: AsyncRead + Unpin,
    W: AsyncSliceWriter,
{
    let start =
        ResponseDecoderStart::new(outboard.root(), ranges, outboard.tree().block_size, encoded);
    let (mut reading, size) = start.next().await?;
    outboard.set_size(ByteNum(size)).await?;
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
                outboard.save(node, &pair).await?;
            }
            BaoContentItem::Leaf(Leaf { offset, data }) => {
                target.write_bytes_at(offset.0, data).await?;
            }
        }
    }
    Ok(())
}
fn read_parent(buf: &[u8]) -> (blake3::Hash, blake3::Hash) {
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..64]).unwrap());
    (l_hash, r_hash)
}
