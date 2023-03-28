//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::ops::{Add, Div, Mul, Range, Sub};

use range_collections::range_set::RangeSetEntry;

index_newtype! {
    pub struct PONum(pub u64);
}

index_newtype! {
    /// A number of <=1024 byte blake3 chunks
    pub struct ChunkNum(pub u64);
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
pub struct BlockLevel(pub u8);

/// A tree level. 0 is for leaves, 1 is for the first level of branches, etc.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TreeLevel(pub u32);

pub(crate) fn block_size(block_level: BlockLevel) -> ByteNum {
    ByteNum(block_size0(block_level.0))
}

fn block_size0(block_level: u8) -> u64 {
    BLAKE3_CHUNK_SIZE << block_level
}
