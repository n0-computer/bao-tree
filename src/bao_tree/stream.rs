use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use blake3::guts::parent_cv;
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream, StreamExt};
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use tokio::io::{AsyncRead, ReadBuf};

use crate::{BaoTree, ByteNum, ChunkNum};

use super::{
    hash_block,
    iter::{DecodeSliceError, NodeInfo, PreOrderPartialIterRef},
    read_parent_mem,
};

use ouroboros::self_referencing;

enum DecodeSliceStreamState<'a> {
    /// we are at the header and don't know yet how big the tree is going to be
    ///
    /// the fields of the header is the query and the stuff we need to have to create the tree
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    },
    /// we are at a node, curr is the node we are at, iter is the iterator for rest
    Node {
        iter: Box<PreOrderPartialIterRef<'a>>,
        curr: NodeInfo<'a>,
    },
    /// we are at the end of the tree. Still need to store the tree somewhere
    Done { tree: BaoTree },
    /// to implement take
    Taken,
}

impl DecodeSliceStreamState<'_> {
    fn take(&mut self) -> Self {
        std::mem::replace(self, DecodeSliceStreamState::Taken)
    }
}

pub struct DecodeSliceStream<'a, R> {
    state: DecodeSliceStreamState<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    buf: BytesMut,
    curr: usize,
}

impl<'a, R: AsyncRead + Unpin> Stream for DecodeSliceStream<'a, R> {
    type Item = std::result::Result<(ByteNum, Bytes), DecodeSliceError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl<'a, R> DecodeSliceStream<'a, R> {
    pub fn new(
        hash: blake3::Hash,
        ranges: &'a RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
        encoded: R,
    ) -> Self {
        let mut stack = SmallVec::new();
        stack.push(hash);
        let mut buf = BytesMut::new();
        buf.resize(8, 0);
        let curr = 0;
        Self {
            state: DecodeSliceStreamState::Header {
                ranges,
                chunk_group_log,
            },
            stack,
            encoded,
            buf,
            curr,
        }
    }
}

impl<'a, R: AsyncRead + Unpin> DecodeSliceStream<'a, R> {
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

    fn set_state(&mut self, mut iter: Box<PreOrderPartialIterRef<'a>>) {
        self.curr = 0;
        self.state = match iter.next() {
            Some(curr) => {
                // calculate how much we need to read
                let mut size = 0;
                if !curr.is_half_leaf {
                    size += 64;
                }
                if let Some(leaf) = curr.node.as_leaf() {
                    let (l, m, r) = iter.tree().leaf_byte_ranges3(leaf);
                    if !curr.l_ranges.is_empty() {
                        size += (m - l).to_usize();
                    };
                    if !curr.r_ranges.is_empty() {
                        size += (r - m).to_usize();
                    };
                };
                self.buf.resize(size, 0);
                DecodeSliceStreamState::Node { curr, iter }
            }
            None => {
                self.buf.resize(0, 0);
                DecodeSliceStreamState::Done { tree: *iter.tree() }
            }
        };
    }

    fn tree(&self) -> &BaoTree {
        match &self.state {
            DecodeSliceStreamState::Node { iter, .. } => iter.tree(),
            DecodeSliceStreamState::Done { tree } => &tree,
            _ => unreachable!(),
        }
    }

    fn poll_next_impl(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(ByteNum, Bytes), DecodeSliceError>>> {
        Poll::Ready(Some(loop {
            // fill the buffer if needed
            ready!(self.poll_fill_buffer(cx))?;
            let (
                mut buf,
                NodeInfo {
                    node,
                    r_ranges,
                    l_ranges,
                    is_half_leaf,
                    is_root,
                    ..
                },
            ) = match self.state.take() {
                DecodeSliceStreamState::Header {
                    ranges,
                    chunk_group_log,
                } => {
                    // read header and create the iterator
                    let len = ByteNum(u64::from_le_bytes(self.buf[..8].try_into().unwrap()));
                    let tree = BaoTree::new(len, chunk_group_log);
                    let iter = Box::new(tree.iterate_part_preorder_ref(ranges, 0));
                    self.set_state(iter);
                    continue;
                }
                DecodeSliceStreamState::Done { .. } => return Poll::Ready(None),
                DecodeSliceStreamState::Node { iter, curr } => {
                    // set the state to the next node
                    let buf = self.buf.split().freeze();
                    self.set_state(iter);
                    (buf, curr)
                }
                DecodeSliceStreamState::Taken => unreachable!(),
            };

            if !is_half_leaf {
                let (l_hash, r_hash) = read_parent_mem(&buf.split_to(64));
                let parent_hash = self.stack.pop().unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                // Push the children in reverse order so they are popped in the correct order
                // only push right if the range intersects with the right child
                if !r_ranges.is_empty() {
                    self.stack.push(r_hash);
                }
                // only push left if the range intersects with the left child
                if !l_ranges.is_empty() {
                    self.stack.push(l_hash);
                }
                // Validate after pushing the children so that we could in principle continue
                if parent_hash != actual {
                    break Err(DecodeSliceError::HashMismatch(node));
                }
            }

            if let Some(leaf) = node.as_leaf() {
                let tree = self.tree();
                let (s, m, e) = tree.leaf_byte_ranges3(leaf);
                let l_start_chunk = tree.chunk_num(leaf);
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                let l_hash = if !l_ranges.is_empty() {
                    Some(self.stack.pop().unwrap())
                } else {
                    None
                };
                let r_hash = if !r_ranges.is_empty() && !is_half_leaf {
                    Some(self.stack.pop().unwrap())
                } else {
                    None
                };
                // validate after popping the hashes so that we could in principle continue
                if let Some(l_hash) = &l_hash {
                    let l_data = &buf[..(m - s).to_usize()];
                    // if is_persisted is true, this is just the left child of a leaf with 2 children
                    // and therefore not the root. Only if it is a half full leaf can it be root
                    let l_is_root = is_root && is_half_leaf;
                    let actual = hash_block(l_start_chunk, &l_data, l_is_root);
                    if l_hash != &actual {
                        break Err(DecodeSliceError::HashMismatch(node));
                    }
                }
                if let Some(r_hash) = &r_hash {
                    let r_data = &buf[buf.len() - (e - m).to_usize()..];
                    // right side can never be root, sorry
                    let r_is_root = false;
                    let actual = hash_block(r_start_chunk, r_data, r_is_root);
                    if r_hash != &actual {
                        break Err(DecodeSliceError::HashMismatch(node));
                    }
                }
                // start offset is start of left range or start of right range (mid)
                let so = if l_hash.is_some() { s } else { m };
                break Ok((so, buf));
            }
        }))
    }
}

#[self_referencing]
struct DecodeSliceStreamOwnedInner<R, Q: 'static> {
    ranges: Q,
    #[borrows(ranges)]
    #[not_covariant]
    inner: DecodeSliceStream<'this, R>,
}
pub struct DecodeSliceStreamOwned<R, Q: 'static = RangeSet2<ChunkNum>>(
    DecodeSliceStreamOwnedInner<R, Q>,
);

impl<R: AsyncRead + Unpin, Q: AsRef<RangeSetRef<ChunkNum>>> Stream
    for DecodeSliceStreamOwned<R, Q>
{
    type Item = Result<(ByteNum, Bytes), DecodeSliceError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.with_inner_mut(|x| x.poll_next_unpin(cx))
    }
}

impl<R: AsyncRead, Q: AsRef<RangeSetRef<ChunkNum>> + 'static> DecodeSliceStreamOwned<R, Q> {
    /// Create a new PreOrderPartialIter.
    ///
    /// ranges has to implement AsRef<RangeSetRef<ChunkNum>>, so you can pass e.g. a RangeSet2.
    pub fn new(hash: blake3::Hash, ranges: Q, encoded: R) -> Self {
        Self(
            DecodeSliceStreamOwnedInnerBuilder {
                ranges,
                inner_builder: |ranges| DecodeSliceStream::new(hash, ranges.as_ref(), 0, encoded),
            }
            .build(),
        )
    }
}

pub struct AsyncSliceDecoder<R, Q: 'static = RangeSet2<ChunkNum>> {
    stream: DecodeSliceStreamOwned<R, Q>,
    current: Bytes,
    offset: usize,
}
