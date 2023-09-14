//! The tree for the bao file format
//!
//! This crate is similar to the [bao crate](https://crates.io/crates/bao), but
//! takes a slightly different approach.
//!
//! The core struct is [BaoTree], which describes the geometry of the tree and
//! various ways to traverse it. An individual node is identified by [TreeNode],
//! which is just a newtype wrapper for an u64.
//!
//! [TreeNode] provides various helpers to e.g. get the offset of a node in
//! different traversal orders.
//!
//! There are various newtypes for the different kinds of integers used in the
//! tree, e.g. [ByteNum] for number of bytes, [ChunkNum] for number of chunks.
//!
//! All this is then used in the [io] module to implement the actual io, both
//! synchronous and asynchronous.
#![deny(missing_docs)]
use range_collections::RangeSetRef;
use std::{
    fmt::{self, Debug},
    ops::Range,
};
#[macro_use]
mod macros;
pub mod iter;
mod tree;
use iter::*;
use tree::BlockNum;
pub use tree::{BlockSize, ByteNum, ChunkNum};
pub mod io;
pub use iroh_blake3 as blake3;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests2;

fn hash_subtree(start_chunk: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    if data.len().is_power_of_two() {
        blake3::guts::hash_subtree(start_chunk, data, is_root)
    } else {
        recursive_hash_subtree(start_chunk, data, is_root)
    }
}

/// This is a recursive version of [`hash_subtree`], for testing.
fn recursive_hash_subtree(start_chunk: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    use blake3::guts::{ChunkState, CHUNK_LEN};
    if data.len() <= CHUNK_LEN {
        let mut hasher = ChunkState::new(start_chunk);
        hasher.update(data);
        hasher.finalize(is_root)
    } else {
        let chunks = data.len() / CHUNK_LEN + (data.len() % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let left = recursive_hash_subtree(start_chunk, &data[..mid_bytes], false);
        let right = recursive_hash_subtree(start_chunk + mid as u64, &data[mid_bytes..], false);
        blake3::guts::parent_cv(&left, &right, is_root)
    }
}

/// Defines a Bao tree.
///
/// This is just the specification of the tree, it does not contain any actual data.
///
/// Usually trees are self-contained. This means that the tree starts at chunk 0,
/// and the hash of the root node is computed with the is_root flag set to true.
///
/// For some internal use, it is also possible to create trees that are just subtrees
/// of a larger tree. In this case, the start_chunk is the chunk number of the first
/// chunk in the tree, and the is_root flag can be false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaoTree {
    /// Total number of bytes in the file
    size: ByteNum,
    /// Log base 2 of the chunk group size
    block_size: BlockSize,
}

/// An offset of a node in a post-order outboard
#[derive(Debug, Clone, Copy)]
pub enum PostOrderOffset {
    /// the node is stable and won't change when appending data
    Stable(u64),
    /// the node is unstable and will change when appending data
    Unstable(u64),
}

impl PostOrderOffset {
    /// Just get the offset value, ignoring whether it's stable or unstable
    pub fn value(self) -> u64 {
        match self {
            Self::Stable(n) => n,
            Self::Unstable(n) => n,
        }
    }
}

impl BaoTree {
    /// Create a new self contained BaoTree
    pub fn new(size: ByteNum, block_size: BlockSize) -> Self {
        Self { size, block_size }
    }

    /// The size of the blob from which this tree was constructed, in bytes
    pub fn size(&self) -> ByteNum {
        self.size
    }

    fn byte_range(&self, node: TreeNode) -> Range<ByteNum> {
        let start = node.chunk_range().start.to_bytes();
        let end = node.chunk_range().end.to_bytes();
        start..end.min(self.size)
    }

    /// Compute the byte ranges for a leaf node
    ///
    /// Returns two ranges, the first is the left range, the second is the right range
    /// If the leaf is partially contained in the tree, the right range will be empty
    fn leaf_byte_ranges3(&self, leaf: TreeNode) -> (ByteNum, ByteNum, ByteNum) {
        let Range { start, end } = leaf.byte_range();
        let mid = leaf.mid().to_bytes();
        if !(start < self.size || (start == 0 && self.size == 0)) {
            debug_assert!(start < self.size || (start == 0 && self.size == 0));
        }
        (start, mid.min(self.size), end.min(self.size))
    }

    /// Traverse the entire tree in post order as [BaoChunk]s
    ///
    /// This iterator is used by both the sync and async io code for computing
    /// an outboard from existing data
    pub fn post_order_chunks_iter(&self) -> PostOrderChunkIter {
        PostOrderChunkIter::new(*self)
    }

    /// Traverse the part of the tree that is relevant for a ranges querys
    /// in pre order as [BaoChunk]s
    ///
    /// This iterator is used by both the sync and async io code for encoding
    /// from an outboard and ranges as well as decoding an encoded stream.
    pub fn ranges_pre_order_chunks_iter_ref<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
        max_skip_level: u8,
    ) -> PreOrderPartialChunkIterRef<'a> {
        PreOrderPartialChunkIterRef::new(*self, ranges, max_skip_level)
    }

    /// Traverse the entire tree in post order as [TreeNode]s,
    /// down to the level given by the block size.
    pub fn post_order_nodes_iter(&self) -> impl Iterator<Item = TreeNode> {
        let (root, len) = shift_tree(self.size, self.block_size);
        let shift = self.block_size.0;
        PostOrderNodeIter::new(root, len).map(move |x| x.subtract_block_size(shift))
    }

    /// Traverse the entire tree in pre order as [TreeNode]s,
    /// down to the level given by the block size.
    pub fn pre_order_nodes_iter(&self) -> impl Iterator<Item = TreeNode> {
        let (root, len) = shift_tree(self.size, self.block_size);
        let shift = self.block_size.0;
        PreOrderNodeIter::new(root, len).map(move |x| x.subtract_block_size(shift))
    }

    /// Traverse the part of the tree that is relevant for a ranges querys
    /// in pre order as [NodeInfo]s
    ///
    /// This is mostly used internally by the [PreOrderChunkIterRef]
    ///
    /// When `min_level` is set to a value greater than 0, the iterator will
    /// skip all branch nodes that are at a level < min_level if they are fully
    /// covered by the ranges.
    pub fn ranges_pre_order_nodes_iter<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
        min_level: u8,
    ) -> PreOrderPartialIterRef<'a> {
        PreOrderPartialIterRef::new(*self, ranges, min_level)
    }

    /// Root of the tree
    ///
    /// Does not consider block size
    pub fn root(&self) -> TreeNode {
        let shift = 10;
        let mask = (1 << shift) - 1;
        let full_blocks = self.size.0 >> shift;
        let open_block = ((self.size.0 & mask) != 0) as u64;
        let blocks = (full_blocks + open_block).max(1);
        let chunks = ChunkNum(blocks);
        TreeNode::root(chunks)
    }

    ///
    pub fn root_for_level(&self) -> TreeNode {
        let mut root = self.root();
        while root.level() < self.block_size.0 as u32 {
            root = root.parent().unwrap();
        }
        root
    }

    /// Number of blocks in the tree
    ///
    /// At chunk group size 1, this is the same as the number of chunks
    /// Even a tree with 0 bytes size has a single block
    pub fn blocks(&self) -> BlockNum {
        // handle the case of an empty tree having 1 block
        self.size.blocks(self.block_size).max(BlockNum(1))
    }

    /// Number of chunks in the tree
    pub fn chunks(&self) -> ChunkNum {
        self.size.chunks()
    }

    /// Number of hash pairs in the outboard
    fn outboard_hash_pairs(&self) -> u64 {
        self.blocks().0 - 1
    }

    pub(crate) fn outboard_size(size: ByteNum, block_size: BlockSize) -> ByteNum {
        let tree = Self::new(size, block_size);
        ByteNum(tree.outboard_hash_pairs() * 64 + 8)
    }

    fn filled_size(&self) -> TreeNode {
        let blocks = self.chunks();
        let n = (blocks.0 + 1) / 2;
        TreeNode(n + n.saturating_sub(1))
    }

    /// true if the node is a leaf for this tree
    ///
    /// If a tree has a non-zero block size, this is different than the node
    /// being a leaf (level=0).
    const fn is_leaf(&self, node: TreeNode) -> bool {
        node.level() == self.block_size.0 as u32
    }

    /// true if the given node is persisted
    ///
    /// the only node that is not persisted is the last leaf node, if it is
    /// less than half full
    #[inline]
    const fn is_persisted(&self, node: TreeNode) -> bool {
        !self.is_leaf(node) || node.mid().to_bytes().0 < self.size.0
    }

    /// true if this is a node that is relevant for the outboard
    #[inline]
    const fn is_relevant_for_outboard(&self, node: TreeNode) -> bool {
        let level = node.level();
        if level < self.block_size.0 as u32 {
            // too small, this outboard does not track it
            false
        } else if level > self.block_size.0 as u32 {
            // a parent node, always relevant
            true
        } else {
            node.mid().to_bytes().0 < self.size.0
        }
    }

    /// The offset of the given node in the pre order traversal
    pub fn pre_order_offset(&self, node: TreeNode) -> Option<u64> {
        // if the node has a level less than block_size, this will return None
        let shifted = node.add_block_size(self.block_size.0)?;
        let is_half_leaf = shifted.is_leaf() && node.mid().to_bytes() >= self.size;
        if !is_half_leaf {
            let (_, tree_filled_size) = shift_tree(self.size, self.block_size);
            Some(pre_order_offset_loop(shifted.0, tree_filled_size.0))
        } else {
            None
        }
    }

    /// The offset of the given node in the post order traversal
    pub fn post_order_offset(&self, node: TreeNode) -> Option<PostOrderOffset> {
        // if the node has a level less than block_size, this will return None
        let shifted = node.add_block_size(self.block_size.0)?;
        if node.byte_range().end <= self.size {
            // stable node, use post_order_offset
            Some(PostOrderOffset::Stable(shifted.post_order_offset()))
        } else {
            // unstable node
            if shifted.is_leaf() && node.mid().to_bytes() >= self.size {
                // half full leaf node, not considered
                None
            } else {
                // compute the offset based on the total size and the height of the node
                self.outboard_hash_pairs()
                    .checked_sub(u64::from(node.right_count()) + 1)
                    .map(PostOrderOffset::Unstable)
            }
        }
    }

    const fn chunk_group_chunks(&self) -> ChunkNum {
        ChunkNum(1 << self.block_size.0)
    }

    const fn chunk_group_bytes(&self) -> ByteNum {
        self.chunk_group_chunks().to_bytes()
    }
}

impl ByteNum {
    /// number of chunks that this number of bytes covers
    pub const fn chunks(&self) -> ChunkNum {
        let mask = (1 << 10) - 1;
        let part = ((self.0 & mask) != 0) as u64;
        let whole = self.0 >> 10;
        ChunkNum(whole + part)
    }

    /// number of chunks that this number of bytes covers
    pub const fn full_chunks(&self) -> ChunkNum {
        ChunkNum(self.0 >> 10)
    }

    /// number of blocks that this number of bytes covers,
    /// given a block size
    pub const fn blocks(&self, block_size: BlockSize) -> BlockNum {
        let chunk_group_log = block_size.0;
        let size = self.0;
        let block_bits = chunk_group_log + 10;
        let block_mask = (1 << block_bits) - 1;
        let full_blocks = size >> block_bits;
        let open_block = ((size & block_mask) != 0) as u64;
        BlockNum(full_blocks + open_block)
    }
}

impl ChunkNum {
    /// number of bytes that this number of chunks covers
    pub const fn to_bytes(&self) -> ByteNum {
        ByteNum(self.0 << 10)
    }
}

/// An u64 that defines a node in a bao tree.
///
/// You typically don't have to use this, but it can be useful for debugging
/// and error handling. Hash validation errors contain a `TreeNode` that allows
/// you to find the position where validation failed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeNode(u64);

impl fmt::Debug for TreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !f.alternate() {
            write!(f, "TreeNode({})", self.0)
        } else if self.is_leaf() {
            write!(f, "TreeNode::Leaf({})", self.0)
        } else {
            write!(f, "TreeNode::Branch({}, level={})", self.0, self.level())
        }
    }
}

impl TreeNode {
    /// Create a new tree node from a start chunk and a level
    ///
    /// The start chunk must be the start of a subtree with the given level.
    /// So for level 0, the start chunk must even. For level 1, the start chunk
    /// must be divisible by 4, etc.
    fn from_start_chunk_and_level(start_chunk: ChunkNum, level: BlockSize) -> Self {
        let start_chunk = start_chunk.0;
        let level = level.0;
        // check that the start chunk a start of a subtree with level `level`
        // this ensures that there is a 0 at bit `level`.
        let check_mask = (1 << (level + 1)) - 1;
        debug_assert_eq!(start_chunk & check_mask, 0);
        let level_mask = (1 << level) - 1;
        // set the trailing `level` bits to 1.
        // The level is the number of trailing ones.
        Self(start_chunk | level_mask)
    }

    /// Given a number of blocks, gives root node
    fn root(chunks: ChunkNum) -> TreeNode {
        Self(((chunks.0 + 1) / 2).next_power_of_two() - 1)
    }

    /// the middle of the tree node, in blocks
    pub const fn mid(&self) -> ChunkNum {
        ChunkNum(self.0 + 1)
    }

    #[inline]
    const fn half_span(&self) -> u64 {
        1 << self.level()
    }

    /// The level of the node in the tree, 0 for leafs.
    #[inline]
    pub const fn level(&self) -> u32 {
        self.0.trailing_ones()
    }

    /// True if this is a leaf node.
    #[inline]
    pub const fn is_leaf(&self) -> bool {
        (self.0 & 1) == 0
    }

    /// Convert a node to a node in a tree with a smaller block size
    ///
    /// E.g. a leaf node in a tree with block size 4 will become a node
    /// with level 4 in a tree with block size 0.
    ///
    /// This works by just adding n trailing 1 bits to the node by shifting
    /// to the left.
    #[inline]
    pub const fn subtract_block_size(&self, n: u8) -> Self {
        let shifted = !(!self.0 << n);
        Self(shifted)
    }

    /// Convert a node to a node in a tree with a larger block size
    ///
    /// If the nodes has n trailing 1 bits, they are removed by shifting
    /// the node to the right by n bits.
    ///
    /// If the node has less than n trailing 1 bits, the node is too small
    /// to be represented in the target tree.
    #[inline]
    pub const fn add_block_size(&self, n: u8) -> Option<Self> {
        let mask = (1 << n) - 1;
        // check if the node has a high enough level
        if self.0 & mask == mask {
            Some(Self(self.0 >> n))
        } else {
            None
        }
    }

    /// Range of blocks that this node covers, given a block size
    ///
    /// Note that this will give the untruncated range, which may be larger than
    /// the actual tree. To get the exact byte range for a tree, use
    /// [BaoTree::byte_range];
    fn byte_range(&self) -> Range<ByteNum> {
        let range = self.chunk_range();
        range.start.to_bytes()..range.end.to_bytes()
    }

    /// Number of nodes below this node, excluding this node.
    #[inline]
    pub const fn count_below(&self) -> u64 {
        // go to representation where trailing zeros are the level
        let x = self.0 + 1;
        // isolate the lowest bit
        let lowest_bit = x & (-(x as i64) as u64);
        // number of nodes is n * 2 - 1, subtract 1 for the node itself
        lowest_bit * 2 - 2
    }

    /// Get the next left ancestor of this node, or None if there is none.
    pub fn next_left_ancestor(&self) -> Option<Self> {
        self.next_left_ancestor0().map(Self)
    }

    /// Get the left child of this node, or None if it is a child node.
    pub fn left_child(&self) -> Option<Self> {
        self.left_child0().map(Self)
    }

    /// Get the right child of this node, or None if it is a child node.
    pub fn right_child(&self) -> Option<Self> {
        self.right_child0().map(Self)
    }

    /// Unrestricted parent, can only be None if we are at the top
    pub fn parent(&self) -> Option<Self> {
        self.parent0().map(Self)
    }

    /// Restricted parent, will be None if we call parent on the root
    pub fn restricted_parent(&self, len: Self) -> Option<Self> {
        let mut curr = *self;
        while let Some(parent) = curr.parent() {
            if parent.0 < len.0 {
                return Some(parent);
            }
            curr = parent;
        }
        // we hit the top
        None
    }

    /// Get a valid right descendant for an offset
    pub(crate) fn right_descendant(&self, len: Self) -> Option<Self> {
        let mut node = self.right_child()?;
        while node.0 >= len.0 {
            node = node.left_child()?;
        }
        Some(node)
    }

    fn left_child0(&self) -> Option<u64> {
        let offset = 1 << self.level().checked_sub(1)?;
        Some(self.0 - offset)
    }

    fn right_child0(&self) -> Option<u64> {
        let offset = 1 << self.level().checked_sub(1)?;
        Some(self.0 + offset)
    }

    fn parent0(&self) -> Option<u64> {
        let level = self.level();
        if level == 63 {
            return None;
        }
        let span = 1u64 << level;
        let offset = self.0;
        Some(if (offset & (span * 2)) == 0 {
            offset + span
        } else {
            offset - span
        })
    }

    /// Get the range of nodes this node covers
    pub const fn node_range(&self) -> Range<Self> {
        let half_span = self.half_span();
        let nn = self.0;
        let r = nn + half_span;
        let l = nn + 1 - half_span;
        Self(l)..Self(r)
    }

    /// Get the range of blocks this node covers
    pub fn chunk_range(&self) -> Range<ChunkNum> {
        let Range { start, end } = self.chunk_range0();
        ChunkNum(start)..ChunkNum(end)
    }

    /// Range of blocks this node covers
    const fn chunk_range0(&self) -> Range<u64> {
        let level = self.level();
        let span = 1 << level;
        let mid = self.0 + 1;
        // at level 0 (leaf), range will be nn..nn+2
        // at level >0 (branch), range will be centered on nn+1
        mid - span..mid + span
    }

    /// Get the post order offset of this node
    pub fn post_order_offset(&self) -> u64 {
        self.post_order_offset0()
    }

    /// the number of times you have to go right from the root to get to this node
    ///
    /// 0 for a root node
    pub fn right_count(&self) -> u32 {
        (self.0 + 1).count_ones() - 1
    }

    #[inline]
    const fn post_order_offset0(&self) -> u64 {
        // compute number of nodes below me
        let below_me = self.count_below();
        // compute next ancestor that is to the left
        let next_left_ancestor = self.next_left_ancestor0();
        // compute offset
        match next_left_ancestor {
            Some(nla) => below_me + nla + 1 - ((nla + 1).count_ones() as u64),
            None => below_me,
        }
    }

    /// Get the range of post order offsets this node covers
    pub fn post_order_range(&self) -> Range<u64> {
        self.post_order_range0()
    }

    #[inline]
    const fn post_order_range0(&self) -> Range<u64> {
        let offset = self.post_order_offset0();
        let end = offset + 1;
        let start = offset - self.count_below();
        start..end
    }

    /// Get the next left ancestor, or None if we don't have one
    #[inline]
    const fn next_left_ancestor0(&self) -> Option<u64> {
        // add 1 to go to the representation where trailing zeroes = level
        let x = self.0 + 1;
        // clear the lowest bit
        let without_lowest_bit = x & (x - 1);
        // go back to the normal representation,
        // producing None if without_lowest_bit is 0, which means that there is no next left ancestor
        without_lowest_bit.checked_sub(1)
    }
}

/// Iterative way to find the offset of a node in a pre-order traversal.
///
/// I am sure there is a way that does not require a loop, but this will do for now.
/// It is slower than the direct formula, but it is still in the nanosecond range,
/// so at a block size of 16 KiB it should not be the limiting factor for anything.
fn pre_order_offset_loop(node: u64, len: u64) -> u64 {
    // node level, 0 for leaf nodes
    let level = (!node).trailing_zeros();
    // span of the node, 1 for leaf nodes
    let span = 1u64 << level;
    // nodes to the left of the tree of this node
    let left = node + 1 - span;
    // count the parents with a loop
    let mut parent_count = 0;
    let mut offset = node;
    let mut span = span;
    // loop until we reach the root, adding valid parents
    loop {
        let pspan = span * 2;
        // find parent
        offset = if (offset & pspan) == 0 {
            offset + span
        } else {
            offset - span
        };
        // if parent is inside the tree, increase parent count
        if offset < len {
            parent_count += 1;
        }
        if pspan >= len {
            // we are at the root
            break;
        }
        span = pspan;
    }
    left - (left.count_ones() as u64) + parent_count
}
