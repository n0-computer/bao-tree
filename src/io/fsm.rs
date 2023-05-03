//! Async (tokio) io, written in fsm style
//!
//! IO ops are written as async state machines that thread the state through the
//! futures to avoid being encumbered by lifetimes.
//!
//! This makes them occasionally a bit verbose to use, but allows being generic
//! without having to box the futures.
use std::io::{self, SeekFrom};

use blake3::guts::parent_cv;
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, Future, FutureExt};
use range_collections::RangeSet2;
use smallvec::SmallVec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};

use crate::{
    hash_block,
    iter::{BaoChunk, PreOrderChunkIter},
    BaoTree, BlockSize, ByteNum, ChunkNum,
};

use super::{error::DecodeError, read_parent, DecodeResponseItem, Leaf, Parent};

/// A writer that can write a slice at a specified offset
///
/// Will extend the file if the offset is past the end of the file, just like posix
/// and windows files do.
///
/// For external storage such as S3/R2, this might be implemented in terms of async http requests.
///
/// This is similar to the io interface of sqlite.
/// See xWrite in https://www.sqlite.org/c3ref/io_methods.html
pub trait AsyncSliceWriter: Sized {
    type WriteFuture: Future<Output = (Self, io::Result<()>)> + Send;

    /// Write the entire Bytes at the given position
    fn write_at(self, offset: u64, data: Bytes) -> Self::WriteFuture;

    /// Write an owned byte array at the given position
    fn write_array_at<const N: usize>(self, offset: u64, bytes: [u8; N]) -> Self::WriteFuture;
}

async fn write_at_inner<W: AsyncWrite + AsyncSeek + Unpin>(
    this: &mut W,
    offset: u64,
    buf: &[u8],
) -> io::Result<()> {
    this.seek(SeekFrom::Start(offset)).await?;
    this.write_all(buf).await?;
    Ok(())
}

impl<W: AsyncWrite + AsyncSeek + Unpin + Send + Sync + 'static> AsyncSliceWriter for W {
    type WriteFuture = BoxFuture<'static, (W, io::Result<()>)>;

    fn write_at(mut self, offset: u64, buf: Bytes) -> Self::WriteFuture {
        async move {
            let res = write_at_inner(&mut self, offset, &buf).await;
            (self, res)
        }
        .boxed()
    }

    fn write_array_at<const N: usize>(mut self, offset: u64, buf: [u8; N]) -> Self::WriteFuture {
        async move {
            let res = write_at_inner(&mut self, offset, &buf).await;
            (self, res)
        }
        .boxed()
    }
}

/// A reader that can read a slice at a specified offset
///
/// For a file, this will be implemented by seeking to the offset and then reading the data.
/// For other types of storage, seeking is not necessary. E.g. a Bytes or a memory mapped
/// slice already allows random access.
///
/// For external storage such as S3/R2, this might be implemented in terms of async http requests.
///
/// This is similar to the io interface of sqlite.
/// See xRead, xFileSize in https://www.sqlite.org/c3ref/io_methods.html
#[allow(clippy::len_without_is_empty)]
pub trait AsyncSliceReader: Sized {
    type ReadFuture: Future<Output = (Self, BytesMut, io::Result<()>)> + Send;
    type LenFuture: Future<Output = (Self, io::Result<u64>)> + Send;
    fn read_at(self, offset: u64, buf: BytesMut) -> Self::ReadFuture;
    fn len(self) -> Self::LenFuture;
}

impl<R: AsyncRead + AsyncSeek + Unpin + Send + 'static> AsyncSliceReader for R {
    type ReadFuture = BoxFuture<'static, (R, BytesMut, io::Result<()>)>;
    type LenFuture = BoxFuture<'static, (R, io::Result<u64>)>;
    fn read_at(mut self, offset: u64, mut buf: BytesMut) -> Self::ReadFuture {
        async fn inner<R: AsyncRead + AsyncSeek + Unpin>(
            this: &mut R,
            offset: u64,
            buf: &mut [u8],
        ) -> io::Result<()> {
            this.seek(SeekFrom::Start(offset)).await?;
            this.read_exact(buf).await?;
            Ok(())
        }
        async move {
            let res = inner(&mut self, offset, &mut buf).await;
            (self, buf, res)
        }
        .boxed()
    }

    fn len(mut self) -> Self::LenFuture {
        async move {
            let len = self.seek(SeekFrom::End(0)).await;
            (self, len)
        }
        .boxed()
    }
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
pub enum ResponseDecoderReadingNext<M, D> {
    /// More data is available
    More(M),
    /// The stream is done
    Done(D),
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

    pub async fn next(
        mut self,
    ) -> ResponseDecoderReadingNext<(Self, std::result::Result<DecodeResponseItem, DecodeError>), R>
    {
        if let Some(chunk) = self.0.iter.next() {
            let item = self.next0(chunk).await;
            ResponseDecoderReadingNext::More((self, item))
        } else {
            ResponseDecoderReadingNext::Done(self.0.encoded)
        }
    }

    pub fn finish(self) -> R {
        self.0.encoded
    }

    async fn next0(
        &mut self,
        chunk: BaoChunk,
    ) -> std::result::Result<DecodeResponseItem, DecodeError> {
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
