//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::ops::{Add, Div, Mul, Sub};

use range_collections::range_set::RangeSetEntry;

index_newtype! {
    /// A number of blake3 chunks
    ///
    /// The blake3 chunk size is 1024 bytes.
    pub struct ChunkNum(pub u64);
}

pub(crate) const BLAKE3_CHUNK_SIZE: usize = 1024;

index_newtype! {
    /// a block number
    pub struct BlockNum(pub u64);
}

impl BlockNum {
    pub fn to_chunks(self, block_level: BlockSize) -> ChunkNum {
        ChunkNum(self.0 << block_level.0)
    }

    pub fn to_bytes(self, block_level: BlockSize) -> ByteNum {
        ByteNum(self.0 << (block_level.0 + 10))
    }
}

index_newtype! {
    /// A number of bytes
    pub struct ByteNum(pub u64);
}

/// A block size.
///
/// Block sizes are powers of 2, with the smallest being 1024 bytes.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockSize(pub(crate) u8);

impl BlockSize {
    /// The default block size, 1024 bytes
    ///
    /// This means that blocks and blake3 chunks are the same size.
    pub const DEFAULT: BlockSize = BlockSize(0);

    pub const fn new(n: u8) -> Option<BlockSize> {
        Some(BlockSize(n))
    }

    /// Number of bytes in a block at this level
    pub const fn size(self) -> usize {
        block_size0(self.0)
    }
}

const fn block_size0(block_level: u8) -> usize {
    BLAKE3_CHUNK_SIZE << block_level
}
