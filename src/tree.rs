//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::{
    fmt,
    ops::{Add, Div, Mul, Sub},
};

use range_collections::range_set::RangeSetEntry;

index_newtype! {
    /// A number of blake3 chunks.
    ///
    /// This is a newtype for u64.
    /// The blake3 chunk size is 1024 bytes.
    pub struct ChunkNum(pub u64);
}

pub(crate) const BLAKE3_CHUNK_SIZE: usize = 1024;

index_newtype! {
    /// A block number.
    ///
    /// This is a newtype for u64.
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
    /// A number of bytes.
    ///
    /// This is a newtype for u64. It does not distinguish between an absolute
    /// number of bytes or a difference between two numbers of bytes.
    pub struct ByteNum(pub u64);
}

/// A block size.
///
/// Block sizes are powers of 2, with the smallest being 1024 bytes.
/// They are encoded as the power of 2, minus 10, so 1 is 1024 bytes, 2 is 2048 bytes, etc.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockSize(pub u8);

impl fmt::Display for BlockSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl BlockSize {
    /// The default block size, 1024 bytes
    ///
    /// This means that blocks and blake3 chunks are the same size.
    pub const ZERO: BlockSize = BlockSize(0);

    /// Number of bytes in a block at this level
    pub const fn bytes(self) -> usize {
        byte_size(self.0)
    }

    /// Compute a block size from bytes
    pub const fn from_bytes(bytes: u64) -> Option<Self> {
        if bytes.count_ones() != 1 {
            // must be a power of 2
            return None;
        }
        if bytes < 1024 {
            // must be at least 1024 bytes
            return None;
        }
        Some(Self((bytes.trailing_zeros() - 10) as u8))
    }
}

const fn byte_size(block_level: u8) -> usize {
    BLAKE3_CHUNK_SIZE << block_level
}
