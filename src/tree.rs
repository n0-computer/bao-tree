//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::ops::{Add, Div, Mul, Range, Sub};

use range_collections::range_set::RangeSetEntry;

index_newtype! {
    /// A number of nodes in the tree
    ///
    /// When used as an index, even numbers correspond to leaf nodes, odd numbers to branch nodes.
    pub struct NodeNum(pub u64);
}

index_newtype! {
    pub struct PONum(pub u64);
}

index_newtype! {
    /// A number of <=1024 byte blake3 chunks
    pub struct ChunkNum(pub u64);
}

impl RangeSetEntry for ChunkNum {
    fn min_value() -> Self {
        ChunkNum(0)
    }

    fn is_min_value(&self) -> bool {
        self.0 == 0
    }
}

pub(crate) const BLAKE3_CHUNK_SIZE: u64 = 1024;

index_newtype! {
    /// a number of leaf blocks with its own hash
    pub struct BlockNum(pub u64);
}

impl BlockNum {
    pub fn to_chunks(self, block_level: u8) -> ChunkNum {
        ChunkNum(self.0 << block_level)
    }

    pub fn to_bytes(self, block_level: BlockLevel) -> ByteNum {
        ByteNum(self.0 << (block_level.0 + 10))
    }
}

index_newtype! {
    /// A number of bytes
    pub struct ByteNum(pub u64);
}

/// A block level. 0 means that a block corresponds to a blake3 chunk,
/// otherwise the block size is 2^block_level * 1024 bytes.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockLevel(pub u32);

/// A tree level. 0 is for leaves, 1 is for the first level of branches, etc.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeLevel(pub u32);

pub(crate) fn block_size(block_level: BlockLevel) -> ByteNum {
    ByteNum(block_size0(block_level.0))
}

fn block_size0(block_level: u32) -> u64 {
    BLAKE3_CHUNK_SIZE << block_level
}

/// Span for an offset. 1 is for leaves, 2 is for the first level of branches, etc.
pub(crate) fn span(offset: NodeNum) -> NodeNum {
    NodeNum(span0(offset.0))
}

fn span0(offset: u64) -> u64 {
    1 << (!offset).trailing_zeros()
}

pub(crate) fn left_child(offset: NodeNum) -> Option<NodeNum> {
    left_child0(offset.0).map(NodeNum)
}

fn left_child0(offset: u64) -> Option<u64> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset - span / 2)
    }
}

pub(crate) fn right_child(offset: NodeNum) -> Option<NodeNum> {
    right_child0(offset.0).map(NodeNum)
}

fn right_child0(offset: u64) -> Option<u64> {
    let span = span0(offset);
    if span == 1 {
        None
    } else {
        Some(offset + span / 2)
    }
}

/// Get a valid right descendant for an offset
pub(crate) fn right_descendant(offset: NodeNum, len: NodeNum) -> Option<NodeNum> {
    let mut offset = right_child(offset)?;
    while offset >= len {
        offset = left_child(offset)?;
    }
    Some(offset)
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub(crate) fn children(offset: NodeNum) -> Option<(NodeNum, NodeNum)> {
    let span = span(offset);
    if span.0 == 1 {
        None
    } else {
        Some((offset - span / 2, offset + span / 2))
    }
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
pub(crate) fn descendants(offset: NodeNum, len: NodeNum) -> Option<(NodeNum, NodeNum)> {
    let lc = left_child(offset);
    let rc = right_descendant(offset, len);
    if let (Some(l), Some(r)) = (lc, rc) {
        Some((l, r))
    } else {
        None
    }
}

pub(crate) fn is_left_sibling(offset: NodeNum) -> bool {
    is_left_sibling0(offset.0)
}

fn is_left_sibling0(offset: u64) -> bool {
    let span = span0(offset) * 2;
    (offset & span) == 0
}

pub(crate) fn parent(offset: NodeNum) -> NodeNum {
    NodeNum(parent0(offset.0))
}

fn parent0(offset: u64) -> u64 {
    let span = span0(offset);
    // if is_left_sibling(offset) {
    if (offset & (span * 2)) == 0 {
        offset + span
    } else {
        offset - span
    }
}

/// Get the chunk index for an offset
pub(crate) fn index(offset: NodeNum) -> BlockNum {
    BlockNum(offset.0 / 2)
}

pub(crate) fn range(offset: NodeNum) -> Range<NodeNum> {
    let r = range0(offset.0);
    NodeNum(r.start)..NodeNum(r.end)
}

fn range0(offset: u64) -> Range<u64> {
    let span = span0(offset);
    offset + 1 - span..offset + span
}

pub fn sibling(offset: NodeNum) -> NodeNum {
    NodeNum(sibling0(offset.0))
}

fn sibling0(offset: u64) -> u64 {
    if is_left_sibling0(offset) {
        offset + span0(offset) * 2
    } else {
        offset - span0(offset) * 2
    }
}

/// Given a range of bytes, returns a range of nodes that cover that range.
pub fn node_range(byte_range: Range<ByteNum>, block_level: BlockLevel) -> Range<NodeNum> {
    let block_size = block_size(block_level).0;
    let start_block = byte_range.start.0 / block_size;
    let end_block = (byte_range.end.0 + block_size - 1) / block_size;
    let start_offset = start_block * 2;
    let end_offset = end_block * 2;
    NodeNum(start_offset)..NodeNum(end_offset)
}

/// byte range for a given block, given a block level and total data len
pub fn leaf_byte_range(
    index: BlockNum,
    block_level: BlockLevel,
    data_len: ByteNum,
) -> Range<ByteNum> {
    let start = index.to_bytes(block_level);
    let end = (index + 1).to_bytes(block_level).min(data_len.max(start));
    start..end
}

/// Size of a leaf, given a block level and total data len
pub fn leaf_size(index: BlockNum, block_level: BlockLevel, data_len: ByteNum) -> ByteNum {
    block_size(block_level).min(data_len - index.to_bytes(block_level))
}
