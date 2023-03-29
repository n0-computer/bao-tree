//! Async (tokio) IO
use std::{
    fmt,
    io::{self, SeekFrom},
    ops::Range,
    pin::Pin,
    result,
    task::{Context, Poll},
};

use blake3::guts::parent_cv;
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream, StreamExt};
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, ReadBuf,
};

use crate::{
    error::{DecodeError, EncodeError},
    hash_block,
    outboard::Outboard,
    range_ok, BaoTree, ByteNum,
};

use super::{
    iter::{BaoChunk, PreOrderChunkIterRef},
    BlockSize, ChunkNum,
};

use ouroboros::self_referencing;

#[derive(Debug)]
enum DecodeResponseStreamState<'a> {
    /// we are at the header and don't know yet how big the tree is going to be
    ///
    /// the fields of the header is the query and the stuff we need to have to create the tree
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    },
    /// we are at a node, curr is the node we are at, iter is the iterator for rest
    Node {
        iter: Box<PreOrderChunkIterRef<'a>>,
        curr: BaoChunk,
    },
    /// we are at the end of the tree. Still need to store the tree somewhere
    Done,
}

impl DecodeResponseStreamState<'_> {
    fn take(&mut self) -> Self {
        std::mem::replace(self, DecodeResponseStreamState::Done)
    }
}

/// A stream of decoded byte slices, with the byte number of the first byte in the slice
///
/// This is useful if you want to process a query response and place the data in a file.
#[derive(Debug)]
pub struct DecodeResponseStreamRef<'a, R> {
    state: DecodeResponseStreamState<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: BytesMut,
    curr: usize,
}

impl<'a, R: AsyncRead + Unpin> Stream for DecodeResponseStreamRef<'a, R> {
    type Item = std::result::Result<(ByteNum, Bytes), DecodeError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl<'a, R> DecodeResponseStreamRef<'a, R> {
    pub fn new(
        hash: blake3::Hash,
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
        encoded: R,
    ) -> Self {
        let mut stack = SmallVec::new();
        stack.push(hash);
        let mut buf = BytesMut::with_capacity(block_size.size());
        // first item (header) needs 8 bytes.
        buf.resize(8, 0);
        // offset at 0
        let curr = 0;
        Self {
            state: DecodeResponseStreamState::Header { ranges, block_size },
            stack,
            encoded,
            buf,
            curr,
        }
    }
}

impl<'a, R: AsyncRead + Unpin> DecodeResponseStreamRef<'a, R> {
    fn poll_fill_buffer(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let src = &mut self.encoded;
        let mut buf = ReadBuf::new(&mut self.buf);
        buf.set_filled(self.curr);
        while buf.remaining() > 0 {
            ready!(AsyncRead::poll_read(Pin::new(src), cx, &mut buf))?;
            self.curr = buf.filled().len();
        }
        Poll::Ready(Ok(()))
    }

    fn set_state(&mut self, mut iter: Box<PreOrderChunkIterRef<'a>>) {
        self.curr = 0;
        self.state = match iter.next() {
            Some(curr) => {
                let size = match curr {
                    BaoChunk::Parent { .. } => 64,
                    BaoChunk::Leaf { size, .. } => size,
                };
                self.buf.resize(size, 0);
                DecodeResponseStreamState::Node { curr, iter }
            }
            None => {
                self.buf.resize(0, 0);
                DecodeResponseStreamState::Done
            }
        };
    }

    fn poll_next_impl(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(ByteNum, Bytes), DecodeError>>> {
        Poll::Ready(Some(loop {
            // fill the buffer if needed
            ready!(self.poll_fill_buffer(cx))?;
            let (buf, curr) = match self.state.take() {
                DecodeResponseStreamState::Header { ranges, block_size } => {
                    // read header and create the iterator
                    let len = ByteNum(u64::from_le_bytes(self.buf[..8].try_into().unwrap()));
                    let tree = BaoTree::new(len, block_size);
                    let iter = Box::new(tree.ranges_pre_order_chunks_ref(ranges, 0));
                    self.set_state(iter);
                    continue;
                }
                DecodeResponseStreamState::Node { iter, curr } => {
                    // set the state to the next node
                    let buf = self.buf.split().freeze();
                    self.set_state(iter);
                    (buf, curr)
                }
                DecodeResponseStreamState::Done { .. } => return Poll::Ready(None),
            };

            match curr {
                BaoChunk::Parent {
                    is_root,
                    right,
                    left,
                    node,
                } => {
                    assert_eq!(buf.len(), 64);
                    let (l_hash, r_hash) = read_parent(&buf);
                    let parent_hash = self.stack.pop().unwrap();
                    let actual = parent_cv(&l_hash, &r_hash, is_root);
                    // Push the children in reverse order so they are popped in the correct order
                    // only push right if the range intersects with the right child
                    if right {
                        self.stack.push(r_hash);
                    }
                    // only push left if the range intersects with the left child
                    if left {
                        self.stack.push(l_hash);
                    }
                    // Validate after pushing the children so that we could in principle continue
                    if parent_hash != actual {
                        break Err(DecodeError::ParentHashMismatch(node));
                    }
                }
                BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                } => {
                    assert_eq!(buf.len(), size);
                    let leaf_hash = self.stack.pop().unwrap();
                    let actual = hash_block(start_chunk, &buf, is_root);
                    if leaf_hash != actual {
                        break Err(DecodeError::LeafHashMismatch(start_chunk));
                    }
                    break Ok((start_chunk.to_bytes(), buf));
                }
            }
        }))
    }
}

#[self_referencing]
#[derive(Debug)]
struct DecodeResponseStreamInner<R, Q: 'static> {
    ranges: Q,
    #[borrows(ranges)]
    #[not_covariant]
    inner: DecodeResponseStreamRef<'this, R>,
}

/// A DecodeResponseStream that owns the query.
///
/// This just wraps [DecodeResponseStreamRef] in a self-referencing struct.
#[derive(Debug)]
pub struct DecodeResponseStream<R, Q: 'static = RangeSet2<ChunkNum>>(
    DecodeResponseStreamInner<R, Q>,
);

impl<R: AsyncRead + Unpin, Q: AsRef<RangeSetRef<ChunkNum>>> Stream for DecodeResponseStream<R, Q> {
    type Item = Result<(ByteNum, Bytes), DecodeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.with_inner_mut(|x| x.poll_next_unpin(cx))
    }
}

impl<R: AsyncRead, Q: AsRef<RangeSetRef<ChunkNum>> + 'static> DecodeResponseStream<R, Q> {
    /// Create a new DecodeResponseStream.
    ///
    /// ranges has to implement `AsRef<RangeSetRef<ChunkNum>>`, so you can pass e.g. a RangeSet2.
    pub fn new(hash: blake3::Hash, ranges: Q, block_size: BlockSize, encoded: R) -> Self {
        Self(
            DecodeResponseStreamInnerBuilder {
                ranges,
                inner_builder: |ranges| {
                    DecodeResponseStreamRef::new(hash, ranges.as_ref(), block_size, encoded)
                },
            }
            .build(),
        )
    }
}

#[derive(Debug)]
enum AsyncResponseDecoderState<'a> {
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    },
    Reading {
        curr: BaoChunk,
        iter: Box<PreOrderChunkIterRef<'a>>,
    },
    Writing {
        size: usize,
        iter: Box<PreOrderChunkIterRef<'a>>,
    },
    Done {
        tree: BaoTree,
    },
    Taken,
}

impl AsyncResponseDecoderState<'_> {
    fn take(&mut self) -> Self {
        std::mem::replace(self, Self::Taken)
    }

    fn read_size(&self) -> Option<usize> {
        match self {
            Self::Header { .. } => Some(8),
            Self::Reading { curr, .. } => Some(curr.size()),
            _ => None,
        }
    }
}

/// An async decoder that reads from an `AsyncRead` and concatenates the decoded data.
#[derive(Debug)]
pub struct AsyncResponseDecoderRef<'a, R> {
    state: AsyncResponseDecoderState<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: &'a mut [u8],
    start: usize,
}

impl<'a, R: AsyncRead + Unpin> AsyncResponseDecoderRef<'a, R> {
    fn new(
        hash: blake3::Hash,
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
        buf: &'a mut [u8],
        encoded: R,
    ) -> Self {
        let mut stack = SmallVec::new();
        stack.push(hash);
        Self {
            state: AsyncResponseDecoderState::Header { ranges, block_size },
            buf,
            encoded,
            stack,
            start: 0,
        }
    }

    fn poll_read_buffer(
        &mut self,
        size: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let src = &mut self.encoded;
        let mut buf = ReadBuf::new(&mut self.buf[..size]);
        buf.set_filled(self.start);
        while self.start < size {
            ready!(AsyncRead::poll_read(Pin::new(src), cx, &mut buf))?;
            if self.start == buf.filled().len() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                )));
            }
            self.start = buf.filled().len();
        }
        Poll::Ready(Ok(()))
    }

    fn set_state_reading(&mut self, mut iter: Box<PreOrderChunkIterRef<'a>>) {
        self.start = 0;
        self.state = match iter.next() {
            Some(curr) => AsyncResponseDecoderState::Reading { curr, iter },
            None => AsyncResponseDecoderState::Done { tree: *iter.tree() },
        };
    }

    fn set_state_writing(&mut self, size: usize, iter: Box<PreOrderChunkIterRef<'a>>) {
        self.start = 0;
        self.state = AsyncResponseDecoderState::Writing { size, iter };
    }

    pub fn tree(&self) -> Option<&BaoTree> {
        match &self.state {
            AsyncResponseDecoderState::Header { .. } => None,
            AsyncResponseDecoderState::Reading { iter, .. } => Some(iter.tree()),
            AsyncResponseDecoderState::Writing { iter, .. } => Some(iter.tree()),
            AsyncResponseDecoderState::Done { tree } => Some(tree),
            AsyncResponseDecoderState::Taken => None,
        }
    }

    pub async fn read_tree(&mut self) -> io::Result<&BaoTree> {
        self.read(&mut []).await?;
        Ok(self.tree().unwrap())
    }

    pub fn into_inner(self) -> R {
        self.encoded
    }
}

impl<'a, R: AsyncRead + Unpin> AsyncRead for AsyncResponseDecoderRef<'a, R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(loop {
            if let Some(size) = self.state.read_size() {
                ready!(self.poll_read_buffer(size, cx))?;
            }
            let (curr, iter) = match self.state.take() {
                AsyncResponseDecoderState::Header { block_size, ranges } => {
                    let size = ByteNum(u64::from_le_bytes(self.buf[..8].try_into().unwrap()));
                    let tree = BaoTree::new(size, block_size);
                    let iter = Box::new(tree.ranges_pre_order_chunks_ref(ranges, 0));
                    self.set_state_reading(iter);
                    continue;
                }
                AsyncResponseDecoderState::Reading { curr, iter } => (curr, iter),
                AsyncResponseDecoderState::Writing { size, iter } => {
                    let remaining = size - self.start;
                    let n = std::cmp::min(remaining, buf.remaining());
                    buf.put_slice(&self.buf[self.start..self.start + n]);
                    self.start += n;
                    if self.start == size {
                        // become reading
                        self.set_state_reading(iter);
                    } else {
                        // remain writing
                        self.state = AsyncResponseDecoderState::Writing { size, iter };
                    }
                    // break in any case, since we have written something
                    break Ok(());
                }
                AsyncResponseDecoderState::Done { tree } => {
                    self.state = AsyncResponseDecoderState::Done { tree };
                    break Ok(());
                }
                AsyncResponseDecoderState::Taken => {
                    unreachable!()
                }
            };
            match curr {
                BaoChunk::Leaf {
                    is_root,
                    start_chunk,
                    size,
                } => {
                    let node_hash = self.stack.pop().unwrap();
                    let actual = hash_block(start_chunk, &self.buf[..size], is_root);
                    // first state change, then check, so we can continue if we want
                    self.set_state_writing(size, iter);
                    if node_hash != actual {
                        break Err(DecodeError::LeafHashMismatch(start_chunk).into());
                    }
                }
                BaoChunk::Parent {
                    is_root,
                    node,
                    left,
                    right,
                } => {
                    let node_hash = self.stack.pop().unwrap();
                    let (l_hash, r_hash) = read_parent(&self.buf[..64]);
                    let actual = parent_cv(&l_hash, &r_hash, is_root);
                    if right {
                        self.stack.push(r_hash);
                    }
                    if left {
                        self.stack.push(l_hash);
                    }
                    // nothing to write
                    // first state change, then check, so we can continue if we want
                    self.set_state_reading(iter);
                    if node_hash != actual {
                        break Err(DecodeError::ParentHashMismatch(node).into());
                    }
                }
            }
        })
    }
}

#[self_referencing]
struct AsyncResponseDecoderInner<R, Q: 'static> {
    ranges: Q,
    buffer: Vec<u8>,
    #[borrows(ranges, mut buffer)]
    #[not_covariant]
    inner: Option<AsyncResponseDecoderRef<'this, R>>,
}

/// An async decoder that reads from an `AsyncRead` and concatenates the decoded data.
///
/// This just wraps [AsyncResponseDecoderRef] in a self-referencing struct.
pub struct AsyncResponseDecoder<R, Q: 'static = RangeSet2<ChunkNum>>(
    AsyncResponseDecoderInner<R, Q>,
);

impl<R, Q> fmt::Debug for AsyncResponseDecoder<R, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("AsyncResponseDecoder").finish()
    }
}

impl<R: AsyncRead + Unpin, Q: AsRef<RangeSetRef<ChunkNum>> + 'static> AsyncResponseDecoder<R, Q> {
    pub fn new(hash: blake3::Hash, ranges: Q, block_size: BlockSize, encoded: R) -> Self {
        let buffer = vec![0; block_size.size()];
        Self(
            AsyncResponseDecoderInnerBuilder {
                buffer,
                ranges,
                inner_builder: |ranges, buffer| {
                    Some(AsyncResponseDecoderRef::new(
                        hash,
                        ranges.as_ref(),
                        block_size,
                        buffer.as_mut_slice(),
                        encoded,
                    ))
                },
            }
            .build(),
        )
    }

    /// Read the tree geometry from the encoded stream.
    ///
    /// This is useful for determining the size of the decoded stream.
    pub async fn read_tree(&mut self) -> io::Result<BaoTree> {
        self.read(&mut []).await?;
        Ok(self.0.with_inner(|x| *x.as_ref().unwrap().tree().unwrap()))
    }

    /// Read the header containing the size from the encoded stream.
    pub async fn read_size(&mut self) -> io::Result<u64> {
        self.read_tree().await.map(|x| x.size.0)
    }

    pub fn into_inner(self) -> R {
        let mut this = self;
        this.0
            .with_inner_mut(|this| this.take().unwrap().into_inner())
    }
}

impl<R: AsyncRead + Unpin, Q: AsRef<RangeSetRef<ChunkNum>> + 'static> AsyncRead
    for AsyncResponseDecoder<R, Q>
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.with_mut(|this| {
            let inner = this.inner.as_mut().unwrap();
            Pin::new(inner).poll_read(cx, buf)
        })
    }
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This will not validate on writing, so data corruption will be detected on reading
pub async fn encode_ranges<D, O, W>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError>
where
    D: AsyncRead + AsyncSeek + Unpin,
    O: Outboard,
    W: AsyncWrite + Unpin,
{
    let mut data = data;
    let mut encoded = encoded;
    let file_len = ByteNum(data.seek(SeekFrom::End(0)).await?);
    let tree = outboard.tree();
    let ob_len = tree.size;
    if file_len != ob_len {
        return Err(EncodeError::SizeMismatch);
    }
    if !range_ok(ranges, tree.chunks()) {
        return Err(EncodeError::InvalidQueryRange);
    }
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    // write header
    encoded
        .write_all(tree.size.0.to_le_bytes().as_slice())
        .await?;
    for item in tree.ranges_pre_order_chunks_ref(ranges, 0) {
        match item {
            BaoChunk::Parent { node, .. } => {
                let (l_hash, r_hash) = outboard.load(node)?.unwrap();
                encoded.write_all(l_hash.as_bytes()).await?;
                encoded.write_all(r_hash.as_bytes()).await?;
            }
            BaoChunk::Leaf {
                start_chunk, size, ..
            } => {
                let start = start_chunk.to_bytes();
                let data = read_range(&mut data, start..start + (size as u64), &mut buffer).await?;
                encoded.write_all(data).await?;
            }
        }
    }
    Ok(())
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This function validates the data before writing
pub async fn encode_ranges_validated<D, O, W>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError>
where
    D: AsyncRead + AsyncSeek + Unpin,
    O: Outboard,
    W: AsyncWrite + Unpin,
{
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let mut data = data;
    let mut encoded = encoded;
    let file_len = ByteNum(data.seek(SeekFrom::End(0)).await?);
    let tree = outboard.tree();
    let ob_len = tree.size;
    if file_len != ob_len {
        return Err(EncodeError::SizeMismatch);
    }
    if !range_ok(ranges, tree.chunks()) {
        return Err(EncodeError::InvalidQueryRange);
    }
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    // write header
    encoded
        .write_all(tree.size.0.to_le_bytes().as_slice())
        .await?;
    for item in tree.ranges_pre_order_chunks_ref(ranges, 0) {
        match item {
            BaoChunk::Parent {
                is_root,
                left,
                right,
                node,
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
                let data = read_range(&mut data, start..start + (size as u64), &mut buffer).await?;
                let actual = hash_block(start_chunk, data, is_root);
                if actual != expected {
                    return Err(EncodeError::LeafHashMismatch(start_chunk));
                }
                encoded.write_all(data).await?;
            }
        }
    }
    Ok(())
}

/// Compute the post order outboard for the given data, writing into a io::Write
pub async fn outboard_post_order<R, W>(
    data: &mut R,
    size: u64,
    block_size: BlockSize,
    outboard: &mut W,
) -> io::Result<blake3::Hash>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let tree = BaoTree::new_with_start_chunk(ByteNum(size), block_size, ChunkNum(0));
    let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
    let hash = outboard_post_order_impl(tree, data, outboard, &mut buffer).await?;
    outboard.write_all(&size.to_le_bytes()).await?;
    Ok(hash)
}

/// Compute the post order outboard for the given data
///
/// This is the internal version that takes a start chunk and does not append the size!
async fn outboard_post_order_impl<R, W>(
    tree: BaoTree,
    data: &mut R,
    outboard: &mut W,
    buffer: &mut [u8],
) -> io::Result<blake3::Hash>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // do not allocate for small trees
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    debug_assert!(buffer.len() == tree.chunk_group_bytes().to_usize());
    for item in tree.post_order_chunks_iter() {
        match item {
            BaoChunk::Parent { is_root, .. } => {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                outboard.write_all(left_hash.as_bytes()).await?;
                outboard.write_all(right_hash.as_bytes()).await?;
                let parent = parent_cv(&left_hash, &right_hash, is_root);
                stack.push(parent);
            }
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
            } => {
                let buf = &mut buffer[..size];
                data.read_exact(buf).await?;
                let hash = hash_block(start_chunk, buf, is_root);
                stack.push(hash);
            }
        }
    }
    debug_assert_eq!(stack.len(), 1);
    let hash = stack.pop().unwrap();
    Ok(hash)
}

/// seeks read the bytes for the range from the source
async fn read_range<'a>(
    from: &mut (impl AsyncRead + AsyncSeek + Unpin),
    range: Range<ByteNum>,
    buf: &'a mut [u8],
) -> std::io::Result<&'a [u8]> {
    let len = (range.end - range.start).to_usize();
    from.seek(SeekFrom::Start(range.start.0)).await?;
    let mut buf = &mut buf[..len];
    from.read_exact(&mut buf).await?;
    Ok(buf)
}

fn read_parent(buf: &[u8]) -> (blake3::Hash, blake3::Hash) {
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..64]).unwrap());
    (l_hash, r_hash)
}
