//! Implementation of bao streaming for std io and tokio io
use std::pin::Pin;

use bytes::Bytes;

use crate::{blake3, BlockSize, ChunkNum, ChunkRanges, TreeNode};

mod error;
use std::future::Future;

pub use error::*;
use range_collections::{range_set::RangeSetRange, RangeSetRef};

#[cfg(feature = "tokio_fsm")]
pub mod fsm;
pub mod mixed;
pub mod outboard;
pub mod sync;

/// A parent hash pair.
#[derive(Debug)]
pub struct Parent {
    /// The node in the tree for which the hashes are.
    pub node: TreeNode,
    /// The pair of hashes for the node.
    pub pair: (blake3::Hash, blake3::Hash),
}

/// A leaf node.
pub struct Leaf {
    /// The byte offset of the leaf in the file.
    pub offset: u64,
    /// The data of the leaf.
    pub data: Bytes,
}

impl std::fmt::Debug for Leaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leaf")
            .field("offset", &self.offset)
            .field("data", &self.data.len())
            .finish()
    }
}

/// A content item for the bao streaming protocol.
///
/// After reading the initial header, the only possible items are `Parent` and
/// `Leaf`.
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

impl BaoContentItem {
    /// True if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, BaoContentItem::Leaf(_))
    }

    /// True if this is a parent node.
    pub fn is_parent(&self) -> bool {
        matches!(self, BaoContentItem::Parent(_))
    }
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
                res |= ChunkRanges::from(ChunkNum::full_chunks(*range.start)..)
            }
            RangeSetRange::Range(range) => {
                res |= ChunkRanges::from(
                    ChunkNum::full_chunks(*range.start)..ChunkNum::chunks(*range.end),
                )
            }
        }
    }
    res
}

/// Given a range set of chunk ranges, round up to chunk groups of the given size.
pub fn round_up_to_chunks_groups(ranges: ChunkRanges, chunk_size: BlockSize) -> ChunkRanges {
    let mut res = ChunkRanges::empty();
    for range in ranges.iter() {
        res |= match range {
            RangeSetRange::RangeFrom(range) => {
                let start = ChunkNum::chunk_group_start(*range.start, chunk_size);
                ChunkRanges::from(start..)
            }
            RangeSetRange::Range(range) => {
                let start = ChunkNum::chunk_group_start(*range.start, chunk_size);
                let end = ChunkNum::chunk_group_end(*range.end, chunk_size);
                ChunkRanges::from(start..end)
            }
        }
    }
    res
}

/// Given a range set of byte ranges, round it up to chunk groups.
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
    for item in ranges.iter() {
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

pub(crate) fn combine_hash_pair(l: &blake3::Hash, r: &blake3::Hash) -> [u8; 64] {
    let mut res = [0u8; 64];
    let lb: &mut [u8; 32] = (&mut res[0..32]).try_into().unwrap();
    *lb = *l.as_bytes();
    let rb: &mut [u8; 32] = (&mut res[32..]).try_into().unwrap();
    *rb = *r.as_bytes();
    res
}

pub(crate) type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[cfg(test)]
mod tests {
    use crate::{BlockSize, ChunkNum};

    #[test]
    fn test_chunk_group_start() {
        let bs = BlockSize(4);
        assert_eq!(ChunkNum::chunk_group_start(ChunkNum(0), bs), ChunkNum(0));
        assert_eq!(ChunkNum::chunk_group_start(ChunkNum(1), bs), ChunkNum(0));
        assert_eq!(ChunkNum::chunk_group_start(ChunkNum(16), bs), ChunkNum(1));
    }
}