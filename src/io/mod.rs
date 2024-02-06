//! Implementation of bao streaming for std io and tokio io
use crate::{blake3, BaoTree, BlockSize, ByteNum, ChunkNum, ChunkRanges, TreeNode};
use bytes::Bytes;

mod error;
pub use error::*;
use range_collections::{range_set::RangeSetRange, RangeSetRef};

use self::outboard::PostOrderMemOutboard;
#[cfg(feature = "tokio_fsm")]
pub mod fsm;
pub mod outboard;
pub mod sync;

/// A bao header, containing the size of the file.
#[derive(Debug)]
pub struct Header {
    /// The size of the file.
    ///
    /// This is not the size of the data you are being sent, but the oveall size
    /// of the file.
    pub size: ByteNum,
}

/// A parent hash pair.
#[derive(Debug)]
pub struct Parent {
    /// The node in the tree for which the hashes are.
    pub node: TreeNode,
    /// The pair of hashes for the node.
    pub pair: (blake3::Hash, blake3::Hash),
}

/// A leaf node.
#[derive(Debug)]
pub struct Leaf {
    /// The byte offset of the leaf in the file.
    pub offset: ByteNum,
    /// The data of the leaf.
    pub data: Bytes,
}

/// The outboard size of a file of size `size` with a block size of `block_size`
pub fn outboard_size(size: u64, block_size: BlockSize) -> u64 {
    BaoTree::outboard_size(ByteNum(size), block_size).0
}

/// The encoded size of a file of size `size` with a block size of `block_size`
pub fn encoded_size(size: u64, block_size: BlockSize) -> u64 {
    outboard_size(size, block_size) + size
}

/// Computes the pre order outboard of a file in memory.
pub fn outboard(input: impl AsRef<[u8]>, block_size: BlockSize) -> (Vec<u8>, blake3::Hash) {
    let outboard = PostOrderMemOutboard::create(input, block_size).flip();
    let hash = *outboard.hash();
    (outboard.into_inner_with_prefix(), hash)
}

/// Given a range set of byte ranges, round it up to full chunks.
///
/// E.g. a byte range from 1..3 will be converted into the chunk range 0..1 (0..1024 bytes).
pub fn round_up_to_chunks(ranges: &RangeSetRef<u64>) -> ChunkRanges {
    let mut res = ChunkRanges::empty();
    // we don't know if the ranges are overlapping, so we just compute the union
    for item in ranges.iter() {
        // full_chunks() rounds down, chunks() rounds up
        match item {
            RangeSetRange::RangeFrom(range) => {
                res |= ChunkRanges::from(ByteNum(*range.start).full_chunks()..)
            }
            RangeSetRange::Range(range) => {
                res |= ChunkRanges::from(
                    ByteNum(*range.start).full_chunks()..ByteNum(*range.end).chunks(),
                )
            }
        }
    }
    res
}

/// Given a range set of chunk ranges, return the full chunk groups.
///
/// If we store outboard data at a level of granularity of `block_size`, we can only
/// share full chunk groups because we don't have proofs for anything below a chunk group.
pub fn full_chunk_groups(ranges: &ChunkRanges, block_size: BlockSize) -> ChunkRanges {
    fn floor(value: u64, shift: u8) -> u64 {
        value >> shift << shift
    }

    fn ceil(value: u64, shift: u8) -> u64 {
        (value + (1 << shift) - 1) >> shift << shift
    }
    let mut res = ChunkRanges::empty();
    // we don't know if the ranges are overlapping, so we just compute the union
    for item in ranges.iter() {
        // full_chunks() rounds down, chunks() rounds up
        match item {
            RangeSetRange::RangeFrom(range) => {
                let start = ceil(range.start.0, block_size.0);
                res |= ChunkRanges::from(ChunkNum(start)..)
            }
            RangeSetRange::Range(range) => {
                let start = ceil(range.start.0, block_size.0);
                let end = floor(range.end.0, block_size.0);
                if start < end {
                    res |= ChunkRanges::from(ChunkNum(start)..ChunkNum(end))
                }
            }
        }
    }
    res
}
