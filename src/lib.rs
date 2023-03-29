use range_collections::{range_set::RangeSetEntry, RangeSetRef};
use std::{
    fmt::{self, Debug},
    io::Cursor,
    ops::{Range, RangeFrom},
    result,
};
#[macro_use]
mod macros;
pub mod iter;
mod tree;
use iter::*;
use tree::BlockNum;
pub use tree::{BlockSize, ByteNum, ChunkNum};
#[cfg(feature = "tokio_io")]
pub mod tokio_io;
pub mod error;
pub mod io;
pub mod outboard;
use outboard::*;

#[cfg(test)]
mod tests;

#[cfg(test)]
use range_collections::RangeSet2;

/// Defines a Bao tree.
///
/// This is just the specification of the tree, it does not contain any actual data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaoTree {
    /// Total number of bytes in the file
    size: ByteNum,
    /// Log base 2 of the chunk group size
    block_size: BlockSize,
    /// start chunk of the tree, 0 for self-contained trees
    start_chunk: ChunkNum,
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
    /// Create a new BaoTree
    pub fn new(size: ByteNum, block_size: BlockSize) -> BaoTree {
        Self::new_with_start_chunk(size, block_size, ChunkNum(0))
    }

    /// Compute the post order outboard for the given data, returning a in mem data structure
    pub(crate) fn outboard_post_order_mem(
        data: impl AsRef<[u8]>,
        block_size: BlockSize,
    ) -> PostOrderMemOutboard {
        let data = data.as_ref();
        let tree = Self::new_with_start_chunk(ByteNum(data.len() as u64), block_size, ChunkNum(0));
        let outboard_len: usize = (tree.outboard_hash_pairs() * 64 + 8).try_into().unwrap();
        let mut res = Vec::with_capacity(outboard_len);
        let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
        let hash =
            io::outboard_post_order_impl(tree, &mut Cursor::new(data), &mut res, &mut buffer)
                .unwrap();
        PostOrderMemOutboard::new(hash, tree, res)
    }

    /// Compute the blake3 hash for the given data
    #[cfg(test)]
    pub(crate) fn blake3_hash(data: impl AsRef<[u8]>) -> blake3::Hash {
        let data = data.as_ref();
        let cursor = Cursor::new(data);
        let mut buffer = [0u8; 1024];
        io::blake3_hash_inner(
            cursor,
            ByteNum(data.len() as u64),
            ChunkNum(0),
            true,
            &mut buffer,
        )
        .unwrap()
    }

    /// Given a *post order* outboard, encode a slice of data
    #[cfg(test)]
    pub fn encode_ranges(
        data: &[u8],
        outboard: &[u8],
        ranges: &RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    ) -> Vec<u8> {
        let size = ByteNum(data.len() as u64);
        match canonicalize_range(ranges, size.chunks()) {
            Ok(ranges) => Self::encode_ranges_impl(data, outboard, &ranges, block_size),
            Err(range) => {
                let ranges = RangeSet2::from(range);
                Self::encode_ranges_impl(data, outboard, &ranges, block_size)
            }
        }
    }

    #[cfg(test)]
    fn encode_ranges_impl(
        data: &[u8],
        outboard: &[u8],
        ranges: &RangeSetRef<ChunkNum>,
        block_size: BlockSize,
    ) -> Vec<u8> {
        let mut res = Vec::new();
        let tree = Self::new(ByteNum(data.len() as u64), block_size);
        res.extend_from_slice(&tree.size.0.to_le_bytes());
        for item in tree.ranges_pre_order_chunks_ref(ranges, 0) {
            match item {
                BaoChunk::Parent { node, .. } => {
                    let offset = tree.post_order_offset(node).unwrap();
                    let hash_offset = usize::try_from(offset.value() * 64).unwrap();
                    res.extend_from_slice(&outboard[hash_offset..hash_offset + 64]);
                }
                BaoChunk::Leaf {
                    size, start_chunk, ..
                } => {
                    let start = start_chunk.to_bytes().to_usize();
                    res.extend_from_slice(&data[start..start + size]);
                }
            }
        }
        res
    }

    /// Compute the byte ranges for a leaf node
    ///
    /// Returns two ranges, the first is the left range, the second is the right range
    /// If the leaf is partially contained in the tree, the right range will be empty
    fn leaf_byte_ranges3(&self, leaf: LeafNode) -> (ByteNum, ByteNum, ByteNum) {
        let chunk_group_bytes = self.chunk_group_bytes();
        let start = chunk_group_bytes * leaf.0;
        let mid = start + chunk_group_bytes;
        let end = start + chunk_group_bytes * 2;
        debug_assert!(start < self.size || (start == 0 && self.size == 0));
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
    pub fn ranges_pre_order_chunks_ref<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
        min_level: u8,
    ) -> PreOrderChunkIterRef<'a> {
        PreOrderChunkIterRef::new(*self, ranges, min_level)
    }

    /// Traverse the entire tree in post order as [TreeNode]s
    ///
    /// This is mostly used internally by the [PostOrderChunkIter]
    pub fn post_order_nodes_iter(&self) -> PostOrderNodeIter {
        PostOrderNodeIter::new(*self)
    }

    /// Traverse the part of the tree that is relevant for a ranges querys
    /// in pre order as [NodeInfo]s
    ///
    /// This is mostly used internally by the [PreOrderChunkIterRef]
    pub fn ranges_pre_order_nodes_iter<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
        min_level: u8,
    ) -> PreOrderPartialIterRef<'a> {
        PreOrderPartialIterRef::new(*self, ranges, min_level)
    }

    /// Create a new BaoTree with a start chunk
    ///
    /// This is used for trees that are part of a larger file.
    /// The start chunk is the chunk number of the first chunk in the tree.
    ///
    /// This is mostly used internally.
    pub fn new_with_start_chunk(
        size: ByteNum,
        block_size: BlockSize,
        start_chunk: ChunkNum,
    ) -> BaoTree {
        BaoTree {
            size,
            block_size,
            start_chunk,
        }
    }

    /// Root of the tree
    pub fn root(&self) -> TreeNode {
        TreeNode::root(self.blocks())
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
        let blocks = self.blocks();
        let n = (blocks.0 + 1) / 2;
        TreeNode(n + n.saturating_sub(1))
    }

    /// Given a leaf node of this tree, return its start chunk number
    pub const fn chunk_num(&self, node: LeafNode) -> ChunkNum {
        // block number of a leaf node is just the node number
        // multiply by chunk_group_size to get the chunk number
        ChunkNum((node.0 << self.block_size.0) + self.start_chunk.0)
    }
    /// Total number of nodes in the tree
    ///
    /// Each leaf node contains up to 2 blocks, and for n leaf nodes there will
    /// be n-1 branch nodes
    ///
    /// Note that this is not the same as the number of hashes in the outboard.
    #[cfg(test)]
    fn node_count(&self) -> u64 {
        let blocks = self.blocks().0 - 1;
        blocks.saturating_sub(1).max(1)
    }

    /// iterate over all nodes in the tree in depth first, left to right, post order
    ///
    /// Recursive reference implementation, just used in tests
    #[cfg(test)]
    fn iterate_reference(&self) -> Vec<TreeNode> {
        fn iterate_rec(valid_nodes: TreeNode, nn: TreeNode, res: &mut Vec<TreeNode>) {
            if !nn.is_leaf() {
                let l = nn.left_child().unwrap();
                let r = nn.right_descendant(valid_nodes).unwrap();
                iterate_rec(valid_nodes, l, res);
                iterate_rec(valid_nodes, r, res);
            }
            res.push(nn);
        }
        // todo: make this a proper iterator
        let nodes = self.node_count();
        let mut res = Vec::with_capacity(nodes.try_into().unwrap());
        iterate_rec(self.filled_size(), self.root(), &mut res);
        res
    }

    /// iterate over all nodes in the tree in depth first, left to right, pre order
    /// that are required to validate the given ranges
    ///
    /// Recursive reference implementation, just used in tests
    #[cfg(test)]
    fn iterate_part_preorder_reference<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
        min_level: u8,
    ) -> Vec<NodeInfo<'a>> {
        fn iterate_part_rec<'a>(
            tree: &BaoTree,
            node: TreeNode,
            ranges: &'a RangeSetRef<ChunkNum>,
            min_level: u8,
            is_root: bool,
            res: &mut Vec<NodeInfo<'a>>,
        ) {
            if ranges.is_empty() {
                return;
            }
            // the middle chunk of the node
            let mid = node.mid().to_chunks(tree.block_size);
            // the start chunk of the node
            let start = node.block_range().start.to_chunks(tree.block_size);
            // check if the node is fully included
            let full = ranges.boundaries().len() == 1 && ranges.boundaries()[0] <= start;
            // split the ranges into left and right
            let (l_ranges, r_ranges) = ranges.split(mid);

            let query_leaf = node.is_leaf() || (full && node.level() < min_level as u32);
            let is_half_leaf = !tree.is_persisted(node);
            // push no matter if leaf or not
            res.push(NodeInfo {
                node,
                l_ranges,
                r_ranges,
                full,
                query_leaf,
                is_root,
                is_half_leaf,
            });
            // if not leaf, recurse
            if !query_leaf {
                let valid_nodes = tree.filled_size();
                let l = node.left_child().unwrap();
                let r = node.right_descendant(valid_nodes).unwrap();
                iterate_part_rec(tree, l, l_ranges, min_level, false, res);
                iterate_part_rec(tree, r, r_ranges, min_level, false, res);
            }
        }
        let mut res = Vec::new();
        let can_be_root = self.start_chunk == 0;
        iterate_part_rec(self, self.root(), ranges, min_level, can_be_root, &mut res);
        res
    }

    /// true if the given node is complete/sealed
    fn is_sealed(&self, node: TreeNode) -> bool {
        node.byte_range(self.block_size).end <= self.size
    }

    /// true if the given node is persisted
    ///
    /// the only node that is not persisted is the last leaf node, if it is
    /// less than half full
    fn is_persisted(&self, node: TreeNode) -> bool {
        !node.is_leaf() || self.bytes(node.mid()) < self.size.0
    }

    fn bytes(&self, blocks: BlockNum) -> ByteNum {
        ByteNum(blocks.0 << (10 + self.block_size.0))
    }

    fn pre_order_offset(&self, node: TreeNode) -> Option<u64> {
        if self.is_persisted(node) {
            Some(pre_order_offset_slow(node.0, self.filled_size().0))
        } else {
            None
        }
    }

    fn post_order_offset(&self, node: TreeNode) -> Option<PostOrderOffset> {
        if self.is_sealed(node) {
            Some(PostOrderOffset::Stable(node.post_order_offset()))
        } else {
            // a leaf node that only has data on the left is not persisted
            if !self.is_persisted(node) {
                None
            } else {
                // compute the offset based on the total size and the height of the node
                self.outboard_hash_pairs()
                    .checked_sub(u64::from(node.right_count()) + 1)
                    .map(|i| PostOrderOffset::Unstable(i))
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
    pub const fn to_bytes(&self) -> ByteNum {
        ByteNum(self.0 << 10)
    }
}

/// truncate a range so that it overlaps with the range 0..end if possible, and has no extra boundaries behind end
fn canonicalize_range(
    range: &RangeSetRef<ChunkNum>,
    end: ChunkNum,
) -> result::Result<&RangeSetRef<ChunkNum>, RangeFrom<ChunkNum>> {
    let (range, _) = range.split(end);
    if !range.is_empty() {
        Ok(range)
    } else if !end.is_min_value() {
        Err(end - 1..)
    } else {
        Err(end..)
    }
}

fn range_ok(range: &RangeSetRef<ChunkNum>, end: ChunkNum) -> bool {
    match canonicalize_range(range, end) {
        Ok(_) => true,
        Err(x) => x.start.is_min_value(),
    }
}

#[cfg(test)]
fn canonicalize_range_owned(range: &RangeSetRef<ChunkNum>, end: ByteNum) -> RangeSet2<ChunkNum> {
    use smallvec::SmallVec;

    match canonicalize_range(range, end.chunks()) {
        Ok(range) => {
            let t = SmallVec::from(range.boundaries());
            RangeSet2::new(t).unwrap()
        }
        Err(range) => RangeSet2::from(range),
    }
}

/// An u64 that defines a node in a bao tree.
///
/// You typically don't have to use this, but it can be useful for debugging
/// and error handling. Hash validation errors contain a `TreeNode` that allows
/// you to find the position where validation failed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeNode(u64);

/// A tree node for which we know that it is a leaf node.
#[derive(Clone, Copy)]
pub struct LeafNode(u64);

impl From<LeafNode> for TreeNode {
    fn from(leaf: LeafNode) -> TreeNode {
        Self(leaf.0)
    }
}

impl LeafNode {
    #[inline]
    pub fn block_range(&self) -> Range<BlockNum> {
        BlockNum(self.0)..BlockNum(self.0 + 2)
    }
}

impl fmt::Debug for LeafNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LeafNode({})", self.0)
    }
}

impl fmt::Debug for TreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !f.alternate() {
            write!(f, "TreeNode({})", self.0)
        } else {
            if self.is_leaf() {
                write!(f, "TreeNode::Leaf({})", self.0)
            } else {
                write!(f, "TreeNode::Branch({}, level={})", self.0, self.level())
            }
        }
    }
}

impl TreeNode {
    /// Given a number of chunks, gives root node
    fn root(blocks: BlockNum) -> TreeNode {
        Self(((blocks.0 + 1) / 2).next_power_of_two() - 1)
    }

    // the middle of the tree node, in blocks
    pub fn mid(&self) -> BlockNum {
        BlockNum(self.0 + 1)
    }

    #[inline]
    const fn half_span(&self) -> u64 {
        1 << self.level()
    }

    #[inline]
    pub const fn level(&self) -> u32 {
        (!self.0).trailing_zeros()
    }

    #[inline]
    pub const fn is_leaf(&self) -> bool {
        self.level() == 0
    }

    pub fn byte_range(&self, block_size: BlockSize) -> Range<ByteNum> {
        let range = self.block_range();
        let shift = 10 + block_size.0;
        ByteNum(range.start.0 << shift)..ByteNum(range.end.0 << shift)
    }

    pub const fn as_leaf(&self) -> Option<LeafNode> {
        if self.is_leaf() {
            Some(LeafNode(self.0))
        } else {
            None
        }
    }

    #[inline]
    pub const fn count_below(&self) -> u64 {
        (1 << (self.level() + 1)) - 2
    }

    pub fn next_left_ancestor(&self) -> Option<Self> {
        self.next_left_ancestor0().map(Self)
    }

    pub fn left_child(&self) -> Option<Self> {
        self.left_child0().map(Self)
    }

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

    pub const fn node_range(&self) -> Range<Self> {
        let half_span = self.half_span();
        let nn = self.0;
        let r = nn + half_span;
        let l = nn + 1 - half_span;
        Self(l)..Self(r)
    }

    pub fn block_range(&self) -> Range<BlockNum> {
        let Range { start, end } = self.block_range0();
        BlockNum(start)..BlockNum(end)
    }

    /// Range of blocks this node covers
    const fn block_range0(&self) -> Range<u64> {
        let level = self.level();
        let span = 1 << level;
        let mid = self.0 + 1;
        // at level 0 (leaf), range will be nn..nn+2
        // at level >0 (branch), range will be centered on nn+1
        mid - span..mid + span
    }

    pub fn post_order_offset(&self) -> u64 {
        self.post_order_offset0()
    }

    /// the number of times you have to go right from the root to get to this node
    ///
    /// 0 for a root node
    pub fn right_count(&self) -> u32 {
        (self.0 + 1).count_ones() - 1
    }

    const fn post_order_offset0(&self) -> u64 {
        // compute number of nodes below me
        let below_me = self.count_below();
        // compute next ancestor that is to the left
        let next_left_ancestor = self.next_left_ancestor0();
        // compute offset
        let offset = match next_left_ancestor {
            Some(nla) => below_me + nla + 1 - ((nla + 1).count_ones() as u64),
            None => below_me,
        };
        offset
    }

    pub fn post_order_range(&self) -> Range<u64> {
        self.post_order_range0()
    }

    const fn post_order_range0(&self) -> Range<u64> {
        let offset = self.post_order_offset0();
        let end = offset + 1;
        let start = offset - self.count_below();
        start..end
    }

    #[inline]
    const fn next_left_ancestor0(&self) -> Option<u64> {
        let level = self.level();
        let i = self.0;
        ((i + 1) & !(1 << level)).checked_sub(1)
    }
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

/// Hash a block.
///
/// `start_chunk` is the chunk index of the first chunk in the block, `data` is the block data,
/// and `is_root` is true if this is the only block.
///
/// It is up to the user to make sure `data.len() <= 1024 * 2^chunk_group_log`
/// It does not make sense to set start_chunk to a value that is not a multiple of 2^chunk_group_log.
pub(crate) fn hash_block(start_chunk: ChunkNum, data: &[u8], is_root: bool) -> blake3::Hash {
    let mut buffer = [0u8; 1024];
    let data_len = ByteNum(data.len() as u64);
    let data = Cursor::new(data);
    io::blake3_hash_inner(data, data_len, start_chunk, is_root, &mut buffer).unwrap()
}

/// Slow iterative way to find the offset of a node in a pre-order traversal.
///
/// I am sure there is a way that does not require a loop, but this will do for now.
fn pre_order_offset_slow(node: u64, len: u64) -> u64 {
    // node level, 0 for leaf nodes
    let level = (!node).trailing_zeros();
    // span of the node, 1 for leaf nodes
    let span = 1u64 << level;
    // nodes to the left of the tree of this node
    let left = node + 1 - span;
    // count the parents with a loop
    let mut pc = 0;
    let mut offset = node;
    let mut span = span;
    loop {
        let pspan = span * 2;
        offset = if (offset & pspan) == 0 {
            offset + span
        } else {
            offset - span
        };
        if offset < len {
            pc += 1;
        }
        if pspan >= len {
            break;
        }
        span = pspan;
    }
    left - (left.count_ones() as u64) + pc
}

/// The outboard size of a file of size `size` with a blogk size of `block_size`
pub fn outboard_size(size: u64, block_size: BlockSize) -> u64 {
    BaoTree::outboard_size(ByteNum(size), block_size).0
}

/// The encoded size of a file of size `size` with a blogk size of `block_size`
pub fn encoded_size(size: u64, block_size: BlockSize) -> u64 {
    outboard_size(size, block_size) + size
}

/// Computes the  pre order outboard of a file in memory.
pub fn outboard(input: impl AsRef<[u8]>, block_size: BlockSize) -> (Vec<u8>, blake3::Hash) {
    let outboard = BaoTree::outboard_post_order_mem(input, block_size).flip();
    let hash = *outboard.hash();
    (outboard.into_inner(), hash)
}
