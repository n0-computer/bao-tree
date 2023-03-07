//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::{
    ops::{Add, Div, Mul, Range, Sub},
};

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

pub(crate) const BLAKE3_CHUNK_SIZE: u64 = 1024;

impl ChunkNum {
    fn to_bytes(self) -> ByteNum {
        ByteNum(self.0 * BLAKE3_CHUNK_SIZE)
    }
}

index_newtype! {
    /// a number of leaf blocks with its own hash
    pub struct BlockNum(pub u64);
}

impl BlockNum {
    pub fn to_bytes(self, block_level: BlockLevel) -> ByteNum {
        let block_size = block_size(block_level);
        ByteNum(self.0 * block_size.0)
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

pub(crate) fn leafs(blocks: NodeNum) -> BlockNum {
    BlockNum((blocks.0 + 1) / 2)
}

/// Root offset given a number of leaves.
pub(crate) fn root(leafs: BlockNum) -> NodeNum {
    NodeNum(root0(leafs.0))
}

fn root0(leafs: u64) -> u64 {
    leafs.next_power_of_two() - 1
}

/// Level for an offset. 0 is for leaves, 1 is for the first level of branches, etc.
pub(crate) fn level(offset: NodeNum) -> TreeLevel {
    TreeLevel(level0(offset.0))
}

fn level0(offset: u64) -> u32 {
    (!offset).trailing_zeros()
}

pub(crate) fn blocks(len: ByteNum, block_level: BlockLevel) -> BlockNum {
    BlockNum(blocks0(len.0, block_level.0))
}

fn blocks0(len: u64, block_level: u32) -> u64 {
    let block_size = block_size0(block_level);
    len / block_size + if len % block_size == 0 { 0 } else { 1 }
}

pub(crate) fn full_blocks(len: ByteNum, block_level: BlockLevel) -> BlockNum {
    BlockNum(full_blocks0(len.0, block_level.0))
}

fn full_blocks0(len: u64, block_level: u32) -> u64 {
    let block_size = block_size0(block_level);
    len / block_size
}

pub(crate) fn num_hashes(blocks: BlockNum) -> NodeNum {
    NodeNum(num_hashes0(blocks.0))
}

fn num_hashes0(blocks: u64) -> u64 {
    if blocks > 0 {
        blocks * 2 - 1
    } else {
        1
    }
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

/// Hash a blake3 chunk.
///
/// `chunk` is the chunk index, `data` is the chunk data, and `is_root` is true if this is the only chunk.
pub(crate) fn hash_chunk(chunk: ChunkNum, data: &[u8], is_root: bool) -> blake3::Hash {
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN);
    let mut hasher = blake3::guts::ChunkState::new(chunk.0);
    hasher.update(data);
    hasher.finalize(is_root)
}

/// Hash a block that is made of a power of two number of chunks.
///
/// `block` is the block index, `data` is the block data, `is_root` is true if this is the only block,
/// and `block_level` indicates how many chunks make up a block. Chunks = 2^level.
pub(crate) fn hash_block(
    block: BlockNum,
    data: &[u8],
    block_level: BlockLevel,
    is_root: bool,
) -> blake3::Hash {
    // ensure that the data is not too big
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN << block_level.0);
    // compute the cunk number for the first chunk in this block
    let chunk0 = ChunkNum(block.0 << block_level.0);
    // simple recursive hash.
    // Note that this should really call in to blake3 hash_all_at_once, but
    // that is not exposed in the public API and also does not allow providing
    // the chunk.
    hash_block0(chunk0, data, block_level.0, is_root)
}

/// Recursive helper for hash_block.
fn hash_block0(chunk0: ChunkNum, data: &[u8], block_level: u32, is_root: bool) -> blake3::Hash {
    if block_level == 0 {
        // we have just a single chunk
        hash_chunk(chunk0, data, is_root)
    } else {
        // number of chunks at this level
        let chunks = 1 << block_level;
        // size corresponding to this level. Data must not be bigger than this.
        let size = blake3::guts::CHUNK_LEN << block_level;
        // mid point of the data
        let mid = size / 2;
        debug_assert!(data.len() <= size);
        if data.len() <= mid {
            // we don't subdivide, so we need to pass through the is_root flag
            hash_block0(chunk0, data, block_level - 1, is_root)
        } else {
            // block_level is > 0 here, so this is safe
            let child_level = block_level - 1;
            // data is larger than mid, so this is safe
            let l = &data[..mid];
            let r = &data[mid..];
            let chunkl = chunk0;
            let chunkr = chunk0 + chunks / 2;
            let l = hash_block0(chunkl, l, child_level, false);
            let r = hash_block0(chunkr, r, child_level, false);
            blake3::guts::parent_cv(&l, &r, is_root)
        }
    }
}

pub(crate) fn block_hashes_iter(
    data: &[u8],
    block_level: BlockLevel,
) -> impl Iterator<Item = (BlockNum, blake3::Hash)> + '_ {
    let block_size = block_size(block_level);
    let is_root = data.len() <= block_size.to_usize();
    data.chunks(block_size.to_usize())
        .enumerate()
        .map(move |(i, data)| {
            let block = BlockNum(i as u64);
            (block, hash_block(block, data, block_level, is_root))
        })
}

/// placeholder hash for if we don't have a hash yet
///
/// Could be anything, or even uninitialized.
/// But we use all zeros so you can easily recognize it in a hex dump.
pub(crate) fn zero_hash() -> blake3::Hash {
    blake3::Hash::from([0u8; 32])
}

/// root hash for an empty slice, independent of block level
pub(crate) fn empty_root_hash() -> blake3::Hash {
    hash_chunk(ChunkNum(0), &[], true)
}

#[cfg(test)]
mod tests {
    use std::cmp::{max, min};

    use proptest::prelude::*;

    use super::*;

    impl Arbitrary for NodeNum {
        type Parameters = ();
        type Strategy = BoxedStrategy<NodeNum>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<u64>().prop_map(NodeNum).boxed()
        }
    }

    proptest! {

        #[test]
        fn children_parent(i in any::<NodeNum>()) {
            if let Some((l, r)) = children(i) {
                assert_eq!(parent(l), i);
                assert_eq!(parent(r), i);
            }
        }

        /// Checks that left_child/right_child are consistent with children
        #[test]
        fn children_consistent(i in any::<NodeNum>()) {
            let lc = left_child(i);
            let rc = right_child(i);
            let c = children(i);
            let lc1 = c.map(|(l, _)| l);
            let rc1 = c.map(|(_, r)| r);
            assert_eq!(lc, lc1);
            assert_eq!(rc, rc1);
        }

        #[test]
        fn sibling_sibling(i in any::<NodeNum>()) {
            let s = sibling(i);
            let distance = max(s, i) - min(s, i);
            // sibling is at a distance of 2*span
            assert_eq!(distance, span(i) * 2);
            // sibling of sibling is value itself
            assert_eq!(sibling(s), i);
        }

        #[test]
        fn compare_descendants(i in any::<NodeNum>(), len in any::<NodeNum>()) {
            let d = descendants(i, len);
            let lc = left_child(i);
            let rc = right_descendant(i, len);
            if let (Some(lc), Some(rc)) = (lc, rc) {
                assert_eq!(d, Some((lc, rc)));
            } else {
                assert_eq!(d, None);
            }
        }

    }

    #[test]
    fn test_right_descendant() {
        for i in 1..11 {
            println!(
                "valid_right_child({}, 9), {:?}",
                i,
                right_descendant(NodeNum(i), NodeNum(9))
            );
        }
    }

    #[test]
    fn test_left() {
        for i in 0..20 {
            println!(
                "assert_eq!(left_child({}), {:?})",
                i,
                left_child(NodeNum(i))
            );
        }
        for i in 0..20 {
            println!(
                "assert_eq!(is_left({}), {})",
                i,
                is_left_sibling(NodeNum(i))
            );
        }
        for i in 0..20 {
            println!("assert_eq!(parent({}), {:?})", i, parent(NodeNum(i)));
        }
        for i in 0..20 {
            println!("assert_eq!(sibling({}), {:?})", i, sibling(NodeNum(i)));
        }
        assert_eq!(left_child0(3), Some(1));
        assert_eq!(left_child0(1), Some(0));
        assert_eq!(left_child0(0), None);
    }

    #[test]
    fn test_span() {
        for i in 0..10 {
            println!("assert_eq!(span({}), {})", i, span0(i))
        }
    }

    #[test]
    fn test_level() {
        for i in 0..10 {
            println!("assert_eq!(level({}), {})", i, level0(i))
        }
        assert_eq!(level0(0), 0);
        assert_eq!(level0(1), 1);
        assert_eq!(level0(2), 0);
        assert_eq!(level0(3), 2);
    }

    #[test]
    fn test_range() {
        for i in 0..8 {
            println!("{} {:?}", i, range0(i));
        }
    }

    #[test]
    fn test_root() {
        assert_eq!(root0(0), 0);
        assert_eq!(root0(1), 0);
        assert_eq!(root0(2), 1);
        assert_eq!(root0(3), 3);
        assert_eq!(root0(4), 3);
        assert_eq!(root0(5), 7);
        assert_eq!(root0(6), 7);
        assert_eq!(root0(7), 7);
        assert_eq!(root0(8), 7);
        assert_eq!(root0(9), 15);
        assert_eq!(root0(10), 15);
        assert_eq!(root0(11), 15);
        assert_eq!(root0(12), 15);
        assert_eq!(root0(13), 15);
        assert_eq!(root0(14), 15);
        assert_eq!(root0(15), 15);
        assert_eq!(root0(16), 15);
        assert_eq!(root0(17), 31);
        assert_eq!(root0(18), 31);
        assert_eq!(root0(19), 31);
        assert_eq!(root0(20), 31);
        assert_eq!(root0(21), 31);
        assert_eq!(root0(22), 31);
        assert_eq!(root0(23), 31);
        assert_eq!(root0(24), 31);
        assert_eq!(root0(25), 31);
        assert_eq!(root0(26), 31);
        assert_eq!(root0(27), 31);
        assert_eq!(root0(28), 31);
        assert_eq!(root0(29), 31);
        assert_eq!(root0(30), 31);
        assert_eq!(root0(31), 31);
        for i in 1..32 {
            println!("assert_eq!(root0({}),{});", i, root0(i))
        }
    }
}

#[cfg(test)]
mod node_number {
    use std::{ops::{Bound, RangeBounds, Range}, num::NonZeroU64, io::Write};

    use blake3::guts::{parent_cv, ChunkState};
    use futures::io::Cursor;

    use super::{PONum, ChunkNum, ByteNum, BLAKE3_CHUNK_SIZE, hash_chunk, NodeNum};


    fn is_odd(x: usize) -> bool {
        x & 1 == 1
    }

    struct GenericRange {
        min: Option<u64>,
        max: Option<u64>,
    }

    impl RangeBounds<u64> for GenericRange {
        fn start_bound(&self) -> Bound<&u64> {
            match &self.min {
                Some(x) => Bound::Included(x),
                None => Bound::Unbounded,
            }
        }

        fn end_bound(&self) -> Bound<&u64> {
            match &self.max {
                Some(x) => Bound::Excluded(x),
                None => Bound::Unbounded,
            }
        }
    }

    struct RangeSetRef<'a> {
        /// If true, the set contains all values below the first boundary.
        below: bool,
        /// The boundaries of the set. Every boundary is a state change.
        /// The value changes before the boundary.
        boundaries: &'a [u64],
    }

    impl RangeSetRef<'_> {
        fn new(below: bool, boundaries: &[u64]) -> RangeSetRef {
            // debug_assert!(boundaries.is_sorted());
            RangeSetRef { below, boundaries }
        }

        fn serialize(&self) -> (bool, impl Iterator<Item = NonZeroU64> + '_) {
            let below = self.below;
            let iter =
                (0..self.boundaries.len().checked_sub(1).unwrap_or_default()).map(move |i| {
                    let min = self.boundaries[i];
                    let max = self.boundaries[i + 1];
                    NonZeroU64::new(max - min).unwrap()
                });
            (below, iter)
        }

        fn range(&self) -> impl RangeBounds<u64> {
            GenericRange {
                min: self.min(),
                max: self.max(),
            }
        }

        fn contains(&self, x: u64) -> bool {
            match self.boundaries.binary_search(&x) {
                Ok(i) => self.below ^ is_odd(i),
                Err(i) => self.below ^ !is_odd(i),
            }
        }

        fn is_empty(&self) -> bool {
            self.boundaries.is_empty() && !self.below
        }

        fn is_all(&self) -> bool {
            self.boundaries.is_empty() && self.below
        }

        fn limit(&self, bounds: impl RangeBounds<u64>) -> Self {
            let min = match bounds.start_bound() {
                Bound::Included(&x) => Some(x),
                Bound::Excluded(&x) => x.checked_add(1),
                Bound::Unbounded => None,
            };
            let max = match bounds.end_bound() {
                Bound::Included(&x) => x.checked_add(1),
                Bound::Excluded(&x) => Some(x),
                Bound::Unbounded => None,
            };
            self.limit_impl(min, max)
        }

        #[inline(always)]
        fn limit_impl(&self, min: Option<u64>, max: Option<u64>) -> Self {
            let mut below = self.below;
            let mut boundaries = self.boundaries;
            if let (Some(min), Some(max)) = (min, max) {
                debug_assert!(min < max);
            }
            if let Some(min) = min {
                let i = boundaries.binary_search(&min).unwrap_or_else(|i| i);
                boundaries = &boundaries[i..];
                below = below ^ is_odd(i);
            }
            if let Some(max) = max {
                let i = boundaries.binary_search(&max).unwrap_or_else(|i| i);
                boundaries = &boundaries[..i];
            }
            Self::new(below, boundaries)
        }

        fn min(&self) -> Option<u64> {
            if !self.below {
                self.boundaries.first().copied()
            } else {
                None
            }
        }

        fn max(&self) -> Option<u64> {
            let flip = is_odd(self.boundaries.len());
            let above = self.below ^ flip;
            if !above {
                self.boundaries.last().copied()
            } else {
                None
            }
        }
    }

    type Parent = (blake3::Hash, blake3::Hash);

    struct Outboard {
        stable: Vec<Parent>,
        unstable: Vec<Parent>,
    }

    fn root(leafs: ChunkNum) -> NodeNumber {
        NodeNumber(root0(leafs))
    }

    fn root0(leafs: ChunkNum) -> u64 {
        leafs.0.next_power_of_two() - 1
    }

    #[derive(Debug, Clone, Copy)]
    pub struct NodeNumber(u64);

    impl Into<u64> for NodeNumber {
        fn into(self) -> u64 {
            self.0
        }
    }

    impl From<u64> for NodeNumber {
        fn from(x: u64) -> NodeNumber {
            NodeNumber(x)
        }
    }

    impl NodeNumber {

        /// Given a number of chunks, gives the size of the fully filled
        /// tree in nodes. One leaf node is responsible for 2 chunks.
        fn filled_size(chunks: ChunkNum) -> NodeNumber {
            let n = (chunks.0 + 1) / 2;
            NodeNumber(n + n.saturating_sub(1))
        }

        /// Given a number of chunks, gives root node
        fn root(chunks: ChunkNum) -> NodeNumber {
            NodeNumber(((chunks.0 + 1) / 2).next_power_of_two() - 1)
        }

        #[inline]
        fn half_span(nn: Self) -> u64 {
            1 << Self::level(nn)
        }

        #[inline]
        pub fn level(nn: Self) -> u32 {
            (!nn.0).trailing_zeros()
        }

        #[inline]
        pub fn is_leaf(nn: Self) -> bool {
            Self::level(nn) == 0
        }

        pub fn leaf_number(nn: Self) -> Option<ChunkNum> {
            if Self::is_leaf(nn) {
                Some(ChunkNum(nn.0))
            } else {
                None
            }
        }

        #[inline]
        pub fn count_below(nn: Self) -> u64 {
            (1 << (Self::level(nn) + 1)) - 2
        }

        pub fn next_left_ancestor(nn: Self) -> Option<Self> {
            Self::next_left_ancestor0(nn).map(Self)
        }

        pub fn left_child(nn: Self) -> Option<Self> {
            Self::left_child0(nn).map(Self)
        }

        pub fn right_child(nn: Self) -> Option<Self> {
            Self::right_child0(nn).map(Self)
        }

        /// Get a valid right descendant for an offset
        pub(crate) fn right_descendant(offset: Self, len: Self) -> Option<Self> {
            let mut offset = Self::right_child(offset)?;
            while offset.0 >= len.0 {
                offset = Self::left_child(offset)?;
            }
            Some(offset)
        }


        fn left_child0(nn: Self) -> Option<u64> {
            let offset = 1 << Self::level(nn).checked_sub(1)?;
            Some(nn.0 - offset)
        }

        fn right_child0(nn: Self) -> Option<u64> {
            let offset = 1 << Self::level(nn).checked_sub(1)?;
            Some(nn.0 + offset)
        }

        pub fn node_range(nn: Self) -> Range<Self> {
            let half_span = Self::half_span(nn);
            let nn = nn.0;
            let r = nn + half_span;
            let l = nn + 1 - half_span;
            Self(l)..Self(r)
        }

        pub fn chunk_range(nn: Self) -> Range<ChunkNum> {
            let Range { start, end } = Self::chunk_range0(nn);
            ChunkNum(start)..ChunkNum(end)
        }

        pub fn byte_range(nn: Self) -> Range<ByteNum> {
            let Range { start, end } = Self::chunk_range0(nn);
            ByteNum(start * BLAKE3_CHUNK_SIZE) .. ByteNum(end * BLAKE3_CHUNK_SIZE)
        }

        /// Rang of chunks this node covers
        fn chunk_range0(nn: Self) -> Range<u64> {
            let level = Self::level(nn);
            let nn = nn.0;
            match level.checked_sub(1) {
                Some(l) => {
                    let span = 2 << l;
                    let mid = nn + 1;
                    mid - span..mid + span
                }
                None => {
                    let mid = nn;
                    mid..mid + 2
                }
            }
        }

        pub fn post_order_offset(nn: Self) -> PONum {
            PONum(Self::post_order_offset0(nn))
        }

        fn post_order_offset0(nn: Self) -> u64 {
            // compute number of nodes below me
            let below_me = Self::count_below(nn);
            // compute next ancestor that is to the left
            let next_left_ancestor = Self::next_left_ancestor0(nn);
            // compute offset
            let offset = match next_left_ancestor {
                Some(nla) => below_me + nla + 1 - ((nla + 1).count_ones() as u64),
                None => below_me,
            };
            offset
        }

        pub fn post_order_range(nn: Self) -> Range<PONum> {
            let Range { start, end } = Self::post_order_range0(nn);
            PONum(start)..PONum(end)
        }

        fn post_order_range0(nn: Self) -> Range<u64> {
            let offset = Self::post_order_offset0(nn);
            let end = offset + 1;
            let start = offset - Self::count_below(nn);
            start..end
        }

        #[inline]
        fn next_left_ancestor0(nn: Self) -> Option<u64> {
            let level = Self::level(nn);
            let i = nn.0;
            ((i + 1) & !(1 << level)).checked_sub(1)
        }
    }

    impl Outboard {
        fn new() -> Outboard {
            Outboard {
                stable: Vec::new(),
                unstable: Vec::new(),
            }
        }

        // total number of hashes, always chunks * 2 - 1
        fn len(&self) -> u64 {
            self.stable.len() as u64 + self.unstable.len() as u64
        }
    }

    pub fn post_order_outboard(data: &[u8]) -> Vec<(blake3::Hash, blake3::Hash)> {
        let has_incomplete = data.len() % 1024 != 0;
        let chunks = ChunkNum((data.len() / 1024 + has_incomplete as usize).max(1) as u64);
        let nodes = NodeNumber::filled_size(chunks);
        let root = NodeNumber::root(chunks);
        let incomplete = chunks.0.count_ones().saturating_sub(1) as usize;
        let os = (chunks.0 - 1) as usize;
        let mut res = Outboard {
            stable: vec![(blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32])); os - incomplete],
            unstable: vec![(blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32])); incomplete],
        };
        post_order_outboard_rec(data, nodes, root, true, incomplete, &mut res);
        let mut t = res.stable;
        t.extend_from_slice(&res.unstable);
        t
    }

    fn post_order_outboard_rec(data: &[u8], valid_nodes: NodeNumber, nn: NodeNumber, is_root: bool, incomplete: usize, res: &mut Outboard) -> blake3::Hash {
        let (lh, rh) = if NodeNumber::is_leaf(nn) {
            let chunk_range = NodeNumber::chunk_range(nn);
            let range = NodeNumber::byte_range(nn);
            let mut data = &data[range.start.to_usize()..];
            if data.len() <= 1024 {
                return hash_chunk(chunk_range.start, data, is_root);
            } else {
                if data.len() > 2048 {
                    data = &data[..2048];
                }
                let lh = hash_chunk(chunk_range.start,&data[..1024],  false);
                let rh = hash_chunk(chunk_range.start + 1,&data[1024..],  false);
                (lh, rh)
            }
        } else {
            let left = NodeNumber::left_child(nn).unwrap();
            let right = NodeNumber::right_descendant(nn, valid_nodes).unwrap();
            let lh = post_order_outboard_rec(data, valid_nodes, left, false, 0, res);
            let rh = post_order_outboard_rec(data, valid_nodes, right, false, incomplete.saturating_sub(1), res);
            (lh, rh)
        };
        if incomplete > 0 {
            res.unstable[incomplete - 1] = (lh, rh);
        } else {
            let offset = NodeNumber::post_order_offset(nn);
            res.stable[offset.to_usize()] = (lh, rh);
        }
        parent_cv(&lh, &rh, is_root)
    }

    pub fn blake3_hash(data: &[u8]) -> blake3::Hash {
        let chunks = ChunkNum((data.len() / 1024 + if data.len() % 1024 != 0 { 1 } else { 0 }) as u64);
        let nodes = NodeNumber::filled_size(chunks);
        let root = NodeNumber::root(chunks);
        blake3_hash_rec(data, nodes, root, true)
    }

    fn blake3_hash_rec(data: &[u8], valid_nodes: NodeNumber, nn: NodeNumber, is_root: bool) -> blake3::Hash {
        if NodeNumber::is_leaf(nn) {
            let chunk_range = NodeNumber::chunk_range(nn);
            let range = NodeNumber::byte_range(nn);
            let mut data = &data[range.start.to_usize()..];
            if data.len() <= 1024 {
                hash_chunk(chunk_range.start,&data,  is_root)
            } else {
                if data.len() > 2048 {
                    data = &data[..2048];
                }
                let lh = hash_chunk(chunk_range.start,&data[..1024],  false);
                let rh = hash_chunk(chunk_range.start + 1,&data[1024..],  false);
                parent_cv(&lh, &rh, is_root)
            }
        } else {
            let left = NodeNumber::left_child(nn).unwrap();
            let right = NodeNumber::right_descendant(nn, valid_nodes).unwrap();
            let lh = blake3_hash_rec(data, valid_nodes, left, false);
            let rh = blake3_hash_rec(data, valid_nodes, right, false);
            parent_cv(&lh, &rh, is_root)
        }
    }

    fn post_order_offset(n: u64) -> u64 {
        // compute level
        let level = (n + 1).trailing_zeros();
        // compute number of nodes below me
        let below_me = (1 << (level + 1)) - 2;
        // compute next ancestor that is to the left
        let next_left_ancestor = ((n + 1) & !(1 << level)).checked_sub(1);
        // compute offset
        let offset = match next_left_ancestor {
            Some(nla) => below_me + nla + 1 - ((nla + 1).count_ones() as u64),
            None => below_me,
        };
        offset
    }

    use proptest::prelude::*;

    fn compare_blake3_impl(data: Vec<u8>) {
        let h1 = blake3_hash(&data);
        let h2 = blake3::hash(&data);
        assert_eq!(h1, h2);
    }

    fn compare_bao_outboard_impl(data: Vec<u8>) {
        let mut storage = Vec::new();
        let cursor = std::io::Cursor::new(&mut storage);
        let mut encoder = abao::encode::Encoder::new_outboard(cursor);
        encoder.write_all(&data).unwrap();
        encoder.finalize_post_order().unwrap();
        // remove the length suffix
        storage.truncate(storage.len() - 8);

        println!("{}", storage.len() / 64);
        let hashes1 = post_order_outboard(&data);
        let data1 = hashes1.iter().map(|(l, r)| {
            let mut res = [0; 64];
            res[..32].copy_from_slice(l.as_bytes());
            res[32..].copy_from_slice(r.as_bytes());
            res
        }).flatten().collect::<Vec<_>>();

        println!("{} {} {}", data.len(), data1.len(), storage.len());
        println!("{}", hex::encode(&data1));
        println!("{}", hex::encode(&storage));
        assert_eq!(data1, storage);
    }

    #[test]
    fn compare_bao_outboard_0() {
        // compare_bao_outboard_impl(vec![]);
        compare_bao_outboard_impl(vec![0; 1]);
        compare_bao_outboard_impl(vec![0; 1023]);
        compare_bao_outboard_impl(vec![0; 1024]);
        compare_bao_outboard_impl(vec![0; 1025]);
        compare_bao_outboard_impl(vec![0; 2047]);
        compare_bao_outboard_impl(vec![0; 2048]);
        compare_bao_outboard_impl(vec![0; 2049]);
        compare_bao_outboard_impl(vec![0; 10000]);
        compare_bao_outboard_impl(vec![0; 20000]);
    }

    #[test]
    fn compare_blake3_0() {
        compare_blake3_impl(vec![]);
        compare_blake3_impl(vec![0; 1]);
        compare_blake3_impl(vec![0; 1023]);
        compare_blake3_impl(vec![0; 1024]);
        compare_blake3_impl(vec![0; 1025]);
        compare_blake3_impl(vec![0; 2047]);
        compare_blake3_impl(vec![0; 2048]);
        compare_blake3_impl(vec![0; 2049]);
        compare_blake3_impl(vec![0; 10000]);
    }

    proptest! {
        #[test]
        fn compare_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            compare_blake3_impl(data);
        }

        #[test]
        fn compare_bao_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768 * 8)) {
            compare_bao_outboard_impl(data);
        }
    }


    #[test]
    fn test_incomplete() {
        fn incomplete(n: u64) -> u32 {
            let has_incomplete = n & 1023 != 0;
            let complete = n / 1024;
            let chunks = complete + has_incomplete as u64;
            let res = chunks.count_ones().saturating_sub(1);
            res
        }
        for i in 0..17 {
            println!("{}k\t{}", i, incomplete(i * 1024));
        }
        println!();
        for i in 1..17 {
            println!("{}k-1\t{}", i, incomplete(i * 1024 - 1));
        }
        assert_eq!(incomplete(0 * 1024), 0);
        assert_eq!(incomplete(1 * 1024), 0);
        assert_eq!(incomplete(2 * 1024), 0);
        assert_eq!(incomplete(3 * 1024), 1);
        assert_eq!(incomplete(4 * 1024), 0);
        assert_eq!(incomplete(5 * 1024), 1);
        assert_eq!(incomplete(6 * 1024), 1);
        assert_eq!(incomplete(7 * 1024), 2);
        assert_eq!(incomplete(8 * 1024), 0);
        assert_eq!(incomplete(9 * 1024), 1);

        assert_eq!(incomplete(1 * 1024 - 1), 0);
        assert_eq!(incomplete(2 * 1024 - 1), 1);
        assert_eq!(incomplete(3 * 1024 - 1), 1);
        assert_eq!(incomplete(4 * 1024 - 1), 2);
        assert_eq!(incomplete(5 * 1024 - 1), 2);
        assert_eq!(incomplete(6 * 1024 - 1), 2);
        assert_eq!(incomplete(7 * 1024 - 1), 2);
        assert_eq!(incomplete(8 * 1024 - 1), 3);
        // 0 -> 0
        // 1 -> 0
        // 1023 -> 0
        // 1024 -> 0
        // 1025 -> 1
        // 2047 -> 1
        // 2048 -> 0
        // 2049 -> 2
    }

    #[test]
    fn bitmap_query() {
        let mut elems = vec![None; 2000];
        for i in 0..1000 {
            let nn = NodeNumber::from(i);
            let o = NodeNumber::post_order_offset(nn);
            let level = NodeNumber::level(nn);
            let text = if level == 0 {
                format!("c {:?} {:?}", NodeNumber::leaf_number(nn).unwrap(), NodeNumber::chunk_range(nn))
            } else {
                format!(
                    "p ({:?}) ({:?}) ({:?})",
                    NodeNumber::node_range(nn),
                    NodeNumber::post_order_range(nn),
                    NodeNumber::chunk_range(nn)
                )
            };
            elems[o.0 as usize] = Some(text);
        }
        for (i, elem) in elems.into_iter().take(50).enumerate() {
            if let Some(text) = elem {
                println!("{} {}", i, text);
            } else {
                println!();
            }
        }

        for i in 0..100 {
            println!("{} {:?} {:?}", i, NodeNumber::filled_size(ChunkNum(i)), NodeNumber::root(ChunkNum(i)));
        }
    }
}
