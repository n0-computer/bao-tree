//! Define a number of newtypes and operations on these newtypes
//!
//! Most operations are concerned with node indexes in an in order traversal of a binary tree.
use std::ops::{Add, Div, Mul, Range, Sub};

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
