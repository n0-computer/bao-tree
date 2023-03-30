//! Syncronous IO
use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    result,
};

use blake3::guts::parent_cv;
use range_collections::RangeSetRef;
use smallvec::SmallVec;

use crate::{
    error::{DecodeError, EncodeError},
    hash_block, hash_chunk,
    iter::{BaoChunk, PreOrderChunkIterRef},
    outboard::Outboard,
    range_ok, BaoTree, BlockSize, ByteNum, ChunkNum,
};

#[derive(Debug)]
enum Position<'a> {
    /// currently reading the header, so don't know how big the tree is
    /// so we need to store the ranges and the chunk group log
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    },
    /// currently reading the tree, all the info we need is in the iter
    Content { iter: PreOrderChunkIterRef<'a> },
}

#[derive(Debug)]
pub struct DecodeSliceIter<'a, R> {
    inner: Position<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    scratch: &'a mut [u8],
}

impl<'a, R: Read> DecodeSliceIter<'a, R> {
    pub fn new(
        root: blake3::Hash,
        block_size: BlockSize,
        encoded: R,
        ranges: &'a RangeSetRef<ChunkNum>,
        scratch: &'a mut [u8],
    ) -> Self {
        // make sure the buffer is big enough
        assert!(scratch.len() >= block_size.size());
        let mut stack = SmallVec::new();
        stack.push(root);
        Self {
            stack,
            inner: Position::Header { ranges, block_size },
            encoded,
            scratch,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.scratch
    }

    pub fn tree(&self) -> Option<&BaoTree> {
        match &self.inner {
            Position::Content { iter } => Some(iter.tree()),
            Position::Header { .. } => None,
        }
    }

    fn next0(&mut self) -> result::Result<Option<Range<ByteNum>>, DecodeError> {
        loop {
            let inner = match &mut self.inner {
                Position::Content { ref mut iter } => iter,
                Position::Header {
                    block_size,
                    ranges: range,
                } => {
                    let size = read_len(&mut self.encoded)?;
                    // make sure the range is valid and canonical
                    if !range_ok(range, size.chunks()) {
                        break Err(DecodeError::InvalidQueryRange);
                    }
                    let tree = BaoTree::new(size, *block_size);
                    self.inner = Position::Content {
                        iter: tree.ranges_pre_order_chunks_ref(range, 0),
                    };
                    continue;
                }
            };
            match inner.next() {
                Some(BaoChunk::Parent {
                    is_root,
                    left,
                    right,
                    node,
                }) => {
                    let (l_hash, r_hash) = read_parent(&mut self.encoded)?;
                    let parent_hash = self.stack.pop().unwrap();
                    let actual = parent_cv(&l_hash, &r_hash, is_root);
                    if parent_hash != actual {
                        break Err(DecodeError::ParentHashMismatch(node));
                    }
                    if right {
                        self.stack.push(r_hash);
                    }
                    if left {
                        self.stack.push(l_hash);
                    }
                }
                Some(BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                }) => {
                    let buf = &mut self.scratch[..size];
                    self.encoded.read_exact(buf)?;
                    let actual = hash_block(start_chunk, buf, is_root);
                    let leaf_hash = self.stack.pop().unwrap();
                    if leaf_hash != actual {
                        break Err(DecodeError::LeafHashMismatch(start_chunk));
                    }
                    let start = start_chunk.to_bytes();
                    let end = start + (size as u64);
                    break Ok(Some(start..end));
                }
                None => break Ok(None),
            }
        }
    }
}

impl<'a, R: Read> Iterator for DecodeSliceIter<'a, R> {
    type Item = result::Result<Range<ByteNum>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This will not validate on writing, so data corruption will be detected on reading
pub fn encode_ranges<D: Read + Seek, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError> {
    let mut data = data;
    let mut encoded = encoded;
    let file_len = ByteNum(data.seek(SeekFrom::End(0))?);
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
    encoded.write_all(tree.size.0.to_le_bytes().as_slice())?;
    for item in tree.ranges_pre_order_chunks_ref(ranges, 0) {
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
                let data = read_range(&mut data, start..start + (size as u64), &mut buffer)?;
                encoded.write_all(data)?;
            }
        }
    }
    Ok(())
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This function validates the data before writing
pub fn encode_ranges_validated<D: Read + Seek, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> result::Result<(), EncodeError> {
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let mut data = data;
    let mut encoded = encoded;
    let file_len = ByteNum(data.seek(SeekFrom::End(0))?);
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
    encoded.write_all(tree.size.0.to_le_bytes().as_slice())?;
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
                encoded.write_all(l_hash.as_bytes())?;
                encoded.write_all(r_hash.as_bytes())?;
            }
            BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
            } => {
                let expected = stack.pop().unwrap();
                let start = start_chunk.to_bytes();
                let data = read_range(&mut data, start..start + (size as u64), &mut buffer)?;
                let actual = hash_block(start_chunk, data, is_root);
                if actual != expected {
                    return Err(EncodeError::LeafHashMismatch(start_chunk));
                }
                encoded.write_all(data)?;
            }
        }
    }
    Ok(())
}

/// Compute the post order outboard for the given data, writing into a io::Write
pub fn outboard_post_order(
    data: &mut impl Read,
    size: u64,
    block_size: BlockSize,
    outboard: &mut impl Write,
) -> io::Result<blake3::Hash> {
    let tree = BaoTree::new_with_start_chunk(ByteNum(size), block_size, ChunkNum(0));
    let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
    let hash = outboard_post_order_impl(tree, data, outboard, &mut buffer)?;
    outboard.write_all(&size.to_le_bytes())?;
    Ok(hash)
}

/// Compute the post order outboard for the given data
///
/// This is the internal version that takes a start chunk and does not append the size!
pub(crate) fn outboard_post_order_impl(
    tree: BaoTree,
    data: &mut impl Read,
    outboard: &mut impl Write,
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
            } => {
                let buf = &mut buffer[..size];
                data.read_exact(buf)?;
                let hash = hash_block(start_chunk, buf, is_root);
                stack.push(hash);
            }
        }
    }
    debug_assert_eq!(stack.len(), 1);
    let hash = stack.pop().unwrap();
    Ok(hash)
}

/// Internal hash computation. This allows to also compute a non root hash, e.g. for a block
///
/// Todo: maybe this should be just done recursively?
pub(crate) fn blake3_hash_inner(
    mut data: impl Read,
    data_len: ByteNum,
    start_chunk: ChunkNum,
    is_root: bool,
    buf: &mut [u8],
) -> std::io::Result<blake3::Hash> {
    let can_be_root = is_root;
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    let tree = BaoTree::new_with_start_chunk(data_len, BlockSize(0), start_chunk);
    for item in tree.post_order_chunks_iter() {
        match item {
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
            } => {
                let buf = &mut buf[..size];
                data.read_exact(buf)?;
                let hash = hash_chunk(start_chunk, buf, can_be_root && is_root);
                stack.push(hash);
            }
            BaoChunk::Parent { is_root, .. } => {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                let hash = parent_cv(&left_hash, &right_hash, can_be_root && is_root);
                stack.push(hash);
            }
        }
    }
    debug_assert_eq!(stack.len(), 1);
    Ok(stack.pop().unwrap())
}

fn read_len(from: &mut impl Read) -> std::io::Result<ByteNum> {
    let mut buf = [0; 8];
    from.read_exact(&mut buf)?;
    let len = ByteNum(u64::from_le_bytes(buf));
    Ok(len)
}

fn read_parent(from: &mut impl Read) -> std::io::Result<(blake3::Hash, blake3::Hash)> {
    let mut buf = [0; 64];
    from.read_exact(&mut buf)?;
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..]).unwrap());
    Ok((l_hash, r_hash))
}

/// seeks read the bytes for the range from the source
fn read_range<'a>(
    from: &mut (impl Read + Seek),
    range: Range<ByteNum>,
    buf: &'a mut [u8],
) -> std::io::Result<&'a [u8]> {
    let len = (range.end - range.start).to_usize();
    from.seek(std::io::SeekFrom::Start(range.start.0))?;
    let mut buf = &mut buf[..len];
    from.read_exact(&mut buf)?;
    Ok(buf)
}
