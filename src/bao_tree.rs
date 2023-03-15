use crate::tree::{hash_chunk, BlockNum, ByteNum, ChunkNum, PONum};
use blake3::guts::parent_cv;
use range_collections::{RangeSet2, RangeSetRef};
use std::{
    fmt::{self, Debug},
    io,
    ops::Range,
};

/// Defines a Bao tree.
///
/// This is just the specification of the tree, it does not contain any actual data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaoTree {
    /// Total number of bytes in the file
    size: ByteNum,
    /// Log base 2 of the chunk group size
    chunk_group_log: u8,
    /// start block of the tree, 0 for self-contained trees
    start_block: BlockNum,
}

#[derive(Debug, Clone, Copy)]
pub enum PostOrderOffset {
    /// the node should not be considered
    Skip,
    /// the node is stable
    Stable(PONum),
    /// the node is unstable
    Unstable(PONum),
}

impl PostOrderOffset {
    pub fn value(self) -> Option<PONum> {
        match self {
            Self::Skip => None,
            Self::Stable(n) => Some(n),
            Self::Unstable(n) => Some(n),
        }
    }
}

impl BaoTree {
    /// Create a new BaoTree
    pub fn new(size: ByteNum, chunk_group_log: u8) -> BaoTree {
        BaoTree {
            size,
            chunk_group_log,
            start_block: BlockNum(0),
        }
    }

    /// Root of the tree
    pub fn root(&self) -> TreeNode {
        TreeNode::root(self.blocks())
    }

    /// number of blocks in the tree
    ///
    /// At chunk group size 1, this is the same as the number of chunks
    /// Even a tree with 0 bytes size has a single block
    ///
    /// This is used very frequently, so init it on creation?
    pub fn blocks(&self) -> BlockNum {
        // handle the case of an empty tree having 1 block
        self.size.blocks(self.chunk_group_log).max(BlockNum(1))
    }

    pub fn chunks(&self) -> ChunkNum {
        self.size.chunks()
    }

    /// Total number of nodes in the tree
    ///
    /// Each leaf node contains up to 2 blocks, and for n leaf nodes there will
    /// be n-1 branch nodes
    ///
    /// Note that this is not the same as the number of hashes in the outboard.
    fn node_count(&self) -> u64 {
        let blocks = self.blocks().0 - 1;
        blocks.saturating_sub(1).max(1)
    }

    /// Number of hash pairs in the outboard
    fn outboard_hash_pairs(&self) -> u64 {
        self.blocks().0 - 1
    }

    fn filled_size(&self) -> TreeNode {
        let blocks = self.blocks();
        let n = (blocks.0 + 1) / 2;
        TreeNode(n + n.saturating_sub(1))
    }

    pub fn chunk_num(&self, node: LeafNode) -> ChunkNum {
        // block number of a leaf node is just the node number
        // multiply by chunk_group_size to get the chunk number
        ChunkNum(node.0 << self.chunk_group_log)
    }

    /// Compute the post order outboard for the given data
    pub fn outboard_post_order(data: &[u8], chunk_group_log: u8) -> (Vec<u8>, blake3::Hash) {
        let mut stack = Vec::<blake3::Hash>::with_capacity(16);
        let tree = Self::new(ByteNum(data.len() as u64), chunk_group_log);
        let root = tree.root();
        let outboard_len: usize = (tree.outboard_hash_pairs() * 64 + 8).try_into().unwrap();
        let mut res = Vec::with_capacity(outboard_len);
        for node in tree.iterate() {
            let is_root = node == root;
            let hash = if let Some(leaf) = node.as_leaf() {
                let chunk0 = tree.chunk_num(leaf);
                let cgc = tree.chunk_group_chunks();
                match tree.leaf_byte_ranges(leaf) {
                    Ok((l, r)) => {
                        let l_data = &data[l.start.to_usize()..l.end.to_usize()];
                        let r_data = &data[r.start.to_usize()..r.end.to_usize()];
                        let l_hash = hash_chunk(chunk0, l_data, false);
                        let r_hash = hash_chunk(chunk0 + cgc, r_data, false);
                        res.extend_from_slice(l_hash.as_bytes());
                        res.extend_from_slice(r_hash.as_bytes());
                        parent_cv(&l_hash, &r_hash, is_root)
                    }
                    Err(l) => {
                        let left = &data[l.start.to_usize()..l.end.to_usize()];
                        hash_chunk(chunk0, left, is_root)
                    }
                }
            } else {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                res.extend_from_slice(left_hash.as_bytes());
                res.extend_from_slice(right_hash.as_bytes());
                parent_cv(&left_hash, &right_hash, is_root)
            };
            stack.push(hash);
        }
        res.extend_from_slice(&(data.len() as u64).to_le_bytes());
        debug_assert_eq!(stack.len(), 1);
        debug_assert_eq!(res.len(), outboard_len);
        let hash = stack.pop().unwrap();
        (res, hash)
    }

    /// Compute the blake3 hash for the given data
    pub fn blake3_hash(data: &[u8]) -> blake3::Hash {
        let mut stack = Vec::with_capacity(16);
        let tree = Self::new(ByteNum(data.len() as u64), 0);
        let root = tree.root();
        for node in tree.iterate() {
            let is_root = node == root;
            let hash = if let Some(leaf) = node.as_leaf() {
                let chunk0 = tree.chunk_num(leaf);
                match tree.leaf_byte_ranges(leaf) {
                    Ok((l, r)) => {
                        let left = &data[l.start.to_usize()..l.end.to_usize()];
                        let right = &data[r.start.to_usize()..r.end.to_usize()];
                        let left_hash = hash_chunk(chunk0, left, false);
                        let right_hash =
                            hash_chunk(chunk0 + tree.chunk_group_chunks(), right, false);
                        parent_cv(&left_hash, &right_hash, is_root)
                    }
                    Err(l) => {
                        let left = &data[l.start.to_usize()..l.end.to_usize()];
                        hash_chunk(chunk0, left, is_root)
                    }
                }
            } else {
                let right = stack.pop().unwrap();
                let left = stack.pop().unwrap();
                parent_cv(&left, &right, is_root)
            };
            stack.push(hash);
        }
        debug_assert_eq!(stack.len(), 1);
        stack.pop().unwrap()
    }

    /// Decode encoded ranges given the root hash
    pub fn decode_ranges<'a>(
        root: blake3::Hash,
        encoded: &'a [u8],
        ranges: &RangeSetRef<ChunkNum>,
    ) -> impl Iterator<Item = io::Result<(ByteNum, &'a [u8])>> + 'a {
        if encoded.len() < 8 {
            return vec![Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encoded data too short",
            ))]
            .into_iter();
        }
        let size = ByteNum(u64::from_le_bytes(encoded[..8].try_into().unwrap()));
        let chunks = size.chunks();
        let res = if ranges.intersects(&RangeSet2::from(ChunkNum(0)..chunks)) {
            Self::decode_ranges_impl(root, &encoded[8..], size, ranges)
        } else {
            // If the range doesn't intersect with the data, ask for the last chunk
            // this is so it matches the behavior of bao
            let ranges = RangeSet2::from(ChunkNum(chunks.0.saturating_sub(1))..chunks);
            Self::decode_ranges_impl(root, &encoded[8..], size, &ranges)
        };
        res.into_iter()
    }

    fn decode_ranges_impl<'a>(
        root: blake3::Hash,
        encoded: &'a [u8],
        size: ByteNum,
        ranges: &RangeSetRef<ChunkNum>,
    ) -> Vec<io::Result<(ByteNum, &'a [u8])>> {
        let mut res = Vec::new();
        let mut remaining = encoded;
        let mut stack = vec![root];
        let tree = Self::new(size, 0);
        let mut is_root = true;
        for (node, tl, tr) in tree.iterate_part_preorder(ranges) {
            let has_parent = tree.is_persisted(node);
            if has_parent {
                let (parent, rest) = remaining.split_at(64);
                remaining = rest;
                let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&parent[..32]).unwrap());
                let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&parent[32..]).unwrap());
                let parent_hash = stack.pop().unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                is_root = false;
                if parent_hash != actual {
                    res.push(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Hash mismatch",
                    )));
                    break;
                }
                if tr {
                    stack.push(r_hash);
                }
                if tl {
                    stack.push(l_hash);
                }
            }
            if let Some(leaf) = node.as_leaf() {
                let (l_range, r_range) = tree.leaf_byte_ranges2(leaf);
                let start_chunk = tree.chunk_num(leaf);
                if tl {
                    let l_hash = stack.pop().unwrap();
                    let l_size = (l_range.end - l_range.start).to_usize();
                    let (l_data, rest) = remaining.split_at(l_size);
                    remaining = rest;
                    let actual = hash_chunk(start_chunk, l_data, is_root);
                    is_root = false;
                    if l_hash != actual {
                        res.push(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Hash mismatch",
                        )));
                        break;
                    }
                    res.push(Ok((l_range.start, l_data)));
                }
                if tr {
                    let r_hash = stack.pop().unwrap();
                    let r_size = (r_range.end - r_range.start).to_usize();
                    let (r_data, rest) = remaining.split_at(r_size);
                    remaining = rest;
                    let actual =
                        hash_chunk(start_chunk + tree.chunk_group_chunks(), r_data, is_root);
                    is_root = false;
                    if r_hash != actual {
                        res.push(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Hash mismatch",
                        )));
                        break;
                    }
                    res.push(Ok((l_range.start, r_data)));
                }
            }
        }
        res
    }

    /// Given a *post order* outboard, encode a slice of data
    ///
    /// Todo: validate on read option
    pub fn encode_ranges(data: &[u8], outboard: &[u8], ranges: &RangeSetRef<ChunkNum>) -> Vec<u8> {
        let size = ByteNum(data.len() as u64);
        let chunks = size.chunks();
        if ranges.intersects(&RangeSet2::from(ChunkNum(0)..chunks)) {
            Self::encode_ranges_impl(data, outboard, ranges)
        } else {
            // If the range doesn't intersect with the data, ask for the last chunk
            // this is so it matches the behavior of bao
            let ranges = RangeSet2::from(ChunkNum(chunks.0.saturating_sub(1))..chunks);
            Self::encode_ranges_impl(data, outboard, &ranges)
        }
    }

    fn encode_ranges_impl(data: &[u8], outboard: &[u8], ranges: &RangeSetRef<ChunkNum>) -> Vec<u8> {
        let mut res = Vec::new();
        let tree = Self::new(ByteNum(data.len() as u64), 0);
        res.extend_from_slice(&tree.size.0.to_le_bytes());
        for (node, tl, tr) in tree.iterate_part_preorder(ranges) {
            if let Some(offset) = tree.post_order_offset(node).value() {
                let hash_offset = (offset * 64).to_usize();
                res.extend_from_slice(&outboard[hash_offset..hash_offset + 64]);
            }
            if let Some(leaf) = node.as_leaf() {
                let (l, r) = tree.leaf_byte_ranges2(leaf);
                if tl {
                    res.extend_from_slice(&data[l.start.to_usize()..l.end.to_usize()]);
                }
                if tr {
                    res.extend_from_slice(&data[r.start.to_usize()..r.end.to_usize()]);
                }
            }
        }
        res
    }

    /// Compute the byte range for a leaf node
    fn leaf_byte_range(&self, leaf: LeafNode) -> Range<ByteNum> {
        let chunk_group_bytes = self.chunk_group_bytes();
        let start = chunk_group_bytes * leaf.0;
        let end = start + chunk_group_bytes * 2;
        debug_assert!(start < self.size || (start == 0 && self.size == 0));
        start..end.min(self.size)
    }

    /// Compute the byte ranges for a leaf node
    ///
    /// Returns Ok((left, right)) if the leaf is fully contained in the tree
    /// Returns Err(left) if the leaf is partially contained in the tree
    fn leaf_byte_ranges(
        &self,
        leaf: LeafNode,
    ) -> std::result::Result<(Range<ByteNum>, Range<ByteNum>), Range<ByteNum>> {
        let chunk_group_bytes = self.chunk_group_bytes();
        let start = chunk_group_bytes * leaf.0;
        let mid = start + chunk_group_bytes;
        let end = start + chunk_group_bytes * 2;
        debug_assert!(start < self.size || (start == 0 && self.size == 0));
        if mid >= self.size {
            Err(start..self.size)
        } else {
            Ok((start..mid, mid..end.min(self.size)))
        }
    }

    /// Compute the byte ranges for a leaf node
    ///
    /// Returns two ranges, the first is the left range, the second is the right range
    /// If the leaf is partially contained in the tree, the right range will be empty
    fn leaf_byte_ranges2(&self, leaf: LeafNode) -> (Range<ByteNum>, Range<ByteNum>) {
        let chunk_group_bytes = self.chunk_group_bytes();
        let start = chunk_group_bytes * leaf.0;
        let mid = start + chunk_group_bytes;
        let end = start + chunk_group_bytes * 2;
        debug_assert!(start < self.size || (start == 0 && self.size == 0));
        (
            start..mid.min(self.size),
            mid.min(self.size)..end.min(self.size),
        )
    }

    /// Compute the chunk ranges for a leaf node
    ///
    /// Returns two ranges, the first is the left range, the second is the right range
    /// If the leaf is partially contained in the tree, the right range will be empty
    fn leaf_chunk_ranges2(&self, leaf: LeafNode) -> (Range<ChunkNum>, Range<ChunkNum>) {
        let max = self.chunks();
        let chunk_group_chunks = self.chunk_group_chunks();
        let start = chunk_group_chunks * leaf.0;
        let mid = start + chunk_group_chunks;
        let end = start + chunk_group_chunks * 2;
        debug_assert!(start < max || (start == 0 && self.size == 0));
        (start..mid.min(max), mid.min(max)..end.min(max))
    }

    /// iterate over all nodes in the tree in depth first, left to right, post order
    pub fn iterate(&self) -> impl Iterator<Item = TreeNode> {
        // todo: make this a proper iterator
        let nodes = self.node_count();
        let mut res = Vec::with_capacity(nodes.try_into().unwrap());
        self.iterate_rec(self.root(), &mut res);
        res.into_iter()
    }

    fn iterate_rec(&self, nn: TreeNode, res: &mut Vec<TreeNode>) {
        if !nn.is_leaf() {
            let valid_nodes = self.filled_size();
            let l = nn.left_child().unwrap();
            let r = nn.right_descendant(valid_nodes).unwrap();
            self.iterate_rec(l, res);
            self.iterate_rec(r, res);
        }
        res.push(nn);
    }

    /// iterate over all nodes in the tree in depth first, left to right, pre order
    /// that are required to validate the given ranges
    pub fn iterate_part_preorder(
        &self,
        ranges: &RangeSetRef<ChunkNum>,
    ) -> impl Iterator<Item = (TreeNode, bool, bool)> {
        let mut res = Vec::new();
        self.iterate_part_rec(self.root(), ranges, &mut res);
        res.into_iter()
    }

    /// true if the given node is complete/sealed
    fn is_sealed(&self, node: TreeNode) -> bool {
        node.byte_range(self.chunk_group_log).end <= self.size
    }

    /// true if the given node is persisted
    ///
    /// the only node that is not persisted is the last leaf node, if it is
    /// less than half full
    fn is_persisted(&self, node: TreeNode) -> bool {
        !node.is_leaf() || self.bytes(node.mid()) < self.size.0
    }

    fn bytes(&self, blocks: BlockNum) -> ByteNum {
        ByteNum(blocks.0 << (10 + self.chunk_group_log))
    }

    fn post_order_offset(&self, node: TreeNode) -> PostOrderOffset {
        if self.is_sealed(node) {
            PostOrderOffset::Stable(node.post_order_offset())
        } else {
            // a leaf node that only has data on the left is not persisted
            if !self.is_persisted(node) {
                PostOrderOffset::Skip
            } else {
                // compute the offset based on the total size and the height of the node
                self.outboard_hash_pairs()
                    .checked_sub(u64::from(node.right_count()) + 1)
                    .map(|i| PostOrderOffset::Unstable(PONum(i)))
                    .unwrap_or(PostOrderOffset::Skip)
            }
        }
    }

    fn iterate_part_rec(
        &self,
        node: TreeNode,
        ranges: &RangeSetRef<ChunkNum>,
        res: &mut Vec<(TreeNode, bool, bool)>,
    ) {
        if ranges.is_empty() {
            return;
        }
        // the middle chunk of the node
        let mid = node.mid().to_chunks(self.chunk_group_log);
        // split the ranges into left and right
        let (l_ranges, r_ranges) = ranges.split(mid);
        // push no matter if leaf or not
        res.push((node, !l_ranges.is_empty(), !r_ranges.is_empty()));
        // if not leaf, recurse
        if !node.is_leaf() {
            let valid_nodes = self.filled_size();
            let l = node.left_child().unwrap();
            let r = node.right_descendant(valid_nodes).unwrap();
            self.iterate_part_rec(l, l_ranges, res);
            self.iterate_part_rec(r, r_ranges, res);
        }
    }

    const fn chunk_group_chunks(&self) -> ChunkNum {
        ChunkNum(1 << self.chunk_group_log)
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
    /// given a block size of `2^chunk_group_log` chunks
    pub const fn blocks(&self, chunk_group_log: u8) -> BlockNum {
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

fn is_odd(x: usize) -> bool {
    x & 1 == 1
}

type Parent = (blake3::Hash, blake3::Hash);

struct Outboard {
    stable: Vec<Parent>,
    unstable: Vec<Parent>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeNode(u64);

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
    /// Given a number of chunks, gives the size of the fully filled
    /// tree in nodes. One leaf node is responsible for 2 chunks.
    fn filled_size(chunks: ChunkNum) -> TreeNode {
        let n = (chunks.0 + 1) / 2;
        TreeNode(n + n.saturating_sub(1))
    }

    /// Given a number of chunks, gives root node
    fn root(blocks: BlockNum) -> TreeNode {
        Self(((blocks.0 + 1) / 2).next_power_of_two() - 1)
    }

    // the middle of the tree node, in blocks
    fn mid(&self) -> BlockNum {
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

    pub fn byte_range(&self, chunk_group_log: u8) -> Range<ByteNum> {
        let range = self.block_range();
        let shift = 10 + chunk_group_log;
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
        let nn = self.0;
        let span = 1 << level;
        let mid = nn + 1;
        // at level 0 (leaf), range will be nn..nn+2
        // at level >0 (branch), range will be centered on nn+1
        mid - span..mid + span
    }

    pub fn post_order_offset(&self) -> PONum {
        PONum(self.post_order_offset0())
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

    pub fn post_order_range(&self) -> Range<PONum> {
        let Range { start, end } = self.post_order_range0();
        PONum(start)..PONum(end)
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

#[cfg(test)]
mod tests {

    use std::{
        io::{Cursor, Read, Write},
        ops::Range,
    };

    use proptest::prelude::*;
    use range_collections::RangeSet2;

    use super::BaoTree;
    use crate::tree::{hash_chunk, ByteNum, ChunkNum, PONum, BLAKE3_CHUNK_SIZE};

    fn make_test_data(n: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(n);
        for i in 0..n {
            data.push((i / 1024) as u8);
        }
        data
    }

    fn bao_tree_blake3_impl(data: Vec<u8>) {
        let h1 = BaoTree::blake3_hash(&data);
        let h2 = blake3::hash(&data);
        assert_eq!(h1, h2);
    }

    fn post_order_outboard_reference(data: &[u8]) -> (Vec<u8>, blake3::Hash) {
        let mut expected = Vec::new();
        let cursor = std::io::Cursor::new(&mut expected);
        let mut encoder = abao::encode::Encoder::new_outboard(cursor);
        encoder.write_all(&data).unwrap();
        // requires non standard fn finalize_post_order
        let expected_hash = encoder.finalize_post_order().unwrap();
        (expected, expected_hash)
    }

    fn encode_slice_reference(
        data: &[u8],
        chunk_range: Range<ChunkNum>,
    ) -> (Vec<u8>, blake3::Hash) {
        let (outboard, hash) = abao::encode::outboard(data);
        let slice_start = chunk_range.start.to_bytes().0;
        let slice_len = (chunk_range.end - chunk_range.start).to_bytes().0;
        let mut encoder = abao::encode::SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            slice_start,
            slice_len,
        );
        let mut res = Vec::new();
        encoder.read_to_end(&mut res).unwrap();
        (res, hash)
    }

    /// range is a range of chunks. Just using u64 for convenience in tests
    fn bao_tree_encode_slice_impl(data: Vec<u8>, range: Range<u64>) {
        let mut range = ChunkNum(range.start)..ChunkNum(range.end);
        let expected = encode_slice_reference(&data, range.clone()).0;
        let (outboard, _hash) = BaoTree::outboard_post_order(&data, 0);
        // extend empty range to contain at least 1 byte
        if range.start == range.end {
            range.end.0 += 1;
        };
        let actual = BaoTree::encode_ranges(&data, &outboard, &RangeSet2::from(range));
        if expected.len() != actual.len() {
            println!("expected");
            println!("{}", hex::encode(&expected));
            println!("actual");
            println!("{}", hex::encode(&actual));
        }
        assert_eq!(expected.len(), actual.len());
        assert_eq!(expected, actual);
    }

    /// range is a range of chunks. Just using u64 for convenience in tests
    fn bao_tree_decode_slice_impl(data: Vec<u8>, range: Range<u64>) {
        let mut range = ChunkNum(range.start)..ChunkNum(range.end);
        let (encoded, root) = encode_slice_reference(&data, range.clone());
        let expected = data;
        let mut actual = Vec::new();
        for item in BaoTree::decode_ranges(root, &encoded, &RangeSet2::from(range)) {
            let (pos, data) = item.unwrap();
            actual.extend_from_slice(data);
        }
    }

    fn bao_tree_outboard_impl(data: Vec<u8>) {
        let (expected, expected_hash) = post_order_outboard_reference(&data);
        let (actual, actual_hash) = BaoTree::outboard_post_order(&data, 0);
        assert_eq!(expected_hash, actual_hash);
        assert_eq!(expected, actual);
    }

    fn bao_tree_slice_impl(data: Vec<u8>) {
        let (expected, expected_hash) = post_order_outboard_reference(&data);
        let (actual, actual_hash) = BaoTree::outboard_post_order(&data, 0);
        assert_eq!(expected_hash, actual_hash);
        assert_eq!(expected, actual);
    }

    #[test]
    fn bao_tree_outboard_0() {
        use make_test_data as td;
        bao_tree_outboard_impl(td(0));
        bao_tree_outboard_impl(td(1));
        bao_tree_outboard_impl(td(1023));
        bao_tree_outboard_impl(td(1024));
        bao_tree_outboard_impl(td(1025));
        bao_tree_outboard_impl(td(2047));
        bao_tree_outboard_impl(td(2048));
        bao_tree_outboard_impl(td(2049));
        bao_tree_outboard_impl(td(10000));
        bao_tree_outboard_impl(td(20000));
        bao_tree_outboard_impl(td(24577));
    }

    #[test]
    fn bao_tree_encode_slice_0() {
        use make_test_data as td;
        bao_tree_encode_slice_impl(td(0), 0..1);
        bao_tree_encode_slice_impl(td(1), 0..1);
        bao_tree_encode_slice_impl(td(1023), 0..1);
        bao_tree_encode_slice_impl(td(1024), 0..1);
        bao_tree_encode_slice_impl(td(1025), 0..1);
        bao_tree_encode_slice_impl(td(2047), 0..1);
        bao_tree_encode_slice_impl(td(2048), 0..1);
        bao_tree_encode_slice_impl(td(10000), 0..1);
        bao_tree_encode_slice_impl(td(20000), 0..1);
        bao_tree_encode_slice_impl(td(24 * 1024 + 1), 0..25);
        bao_tree_encode_slice_impl(td(1025), 1..2);
        bao_tree_encode_slice_impl(td(2047), 1..2);
        bao_tree_encode_slice_impl(td(2048), 1..2);
        bao_tree_encode_slice_impl(td(10000), 1..2);
        bao_tree_encode_slice_impl(td(20000), 1..2);
    }

    #[test]
    fn bao_tree_decode_slice_0() {
        use make_test_data as td;
        // bao_tree_decode_slice_impl(td(0), 0..1);
        // bao_tree_decode_slice_impl(td(1), 0..1);
        // bao_tree_decode_slice_impl(td(1023), 0..1);
        // bao_tree_decode_slice_impl(td(1024), 0..1);
        // bao_tree_decode_slice_impl(td(1025), 0..2);
        // bao_tree_decode_slice_impl(td(2047), 0..2);
        // bao_tree_decode_slice_impl(td(2048), 0..2);
        // bao_tree_encode_slice_impl(td(24 * 1024 + 1), 0..25);
        // bao_tree_decode_slice_impl(td(1025), 0..1);
        bao_tree_decode_slice_impl(td(1025), 1..2);
    }

    #[test]
    fn bao_tree_blake3_0() {
        use make_test_data as td;
        bao_tree_blake3_impl(td(0));
        bao_tree_blake3_impl(td(1));
        bao_tree_blake3_impl(td(1023));
        bao_tree_blake3_impl(td(1024));
        bao_tree_blake3_impl(td(1025));
        bao_tree_blake3_impl(td(2047));
        bao_tree_blake3_impl(td(2048));
        bao_tree_blake3_impl(td(2049));
        bao_tree_blake3_impl(td(10000));
    }

    fn size_and_slice_overlapping() -> impl Strategy<Value = (ByteNum, ChunkNum, ChunkNum)> {
        (0..32768u64).prop_flat_map(|len| {
            let len = ByteNum(len);
            let chunks = len.chunks();
            let slice_start = 0..=chunks.0.saturating_sub(1);
            let slice_len = 1..=(chunks.0 + 1);
            (
                Just(len),
                slice_start.prop_map(ChunkNum),
                slice_len.prop_map(ChunkNum),
            )
        })
    }

    fn size_and_slice() -> impl Strategy<Value = (ByteNum, ChunkNum, ChunkNum)> {
        (0..32768u64).prop_flat_map(|len| {
            let len = ByteNum(len);
            let chunks = len.chunks();
            let slice_start = 0..=chunks.0;
            let slice_len = 0..=chunks.0;
            (
                Just(len),
                slice_start.prop_map(ChunkNum),
                slice_len.prop_map(ChunkNum),
            )
        })
    }

    proptest! {

        #[test]
        fn bao_tree_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_blake3_impl(data);
        }

        #[test]
        fn bao_tree_encode_slice_all(len in 0..32768usize) {
            let data = make_test_data(len);
            let chunk_range = 0..(data.len() / 1024 + 1) as u64;
            bao_tree_encode_slice_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_decode_slice_all(len in 0..32768usize) {
            let data = make_test_data(len);
            let chunk_range = 0..(data.len() / 1024 + 1) as u64;
            bao_tree_decode_slice_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_encode_slice_part_overlapping((len, start, size) in size_and_slice_overlapping()) {
            let data = make_test_data(len.to_usize());
            let chunk_range = start.0 .. start.0 + size.0;
            bao_tree_encode_slice_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_encode_slice_part_any((len, start, size) in size_and_slice()) {
            let data = make_test_data(len.to_usize());
            let chunk_range = start.0 .. start.0 + size.0;
            bao_tree_encode_slice_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_outboard_impl(data);
        }
    }

    #[test]
    fn bao_tree_iterate_all() {
        let tree = BaoTree::new(ByteNum(1024 * 15), 0);
        println!("{}", tree.outboard_hash_pairs());
        for node in tree.iterate() {
            println!(
                "{:#?}\t{}\t{:?}",
                node,
                tree.is_sealed(node),
                tree.post_order_offset(node)
            );
        }
    }

    #[test]
    fn bao_tree_iterate_part() {
        let tree = BaoTree::new(ByteNum(1024 * 5), 0);
        println!();
        let spec = RangeSet2::from(ChunkNum(2)..ChunkNum(3));
        for (node, ..) in tree.iterate_part_preorder(&spec) {
            println!(
                "{:#?}\t{}\t{:?}",
                node,
                tree.is_sealed(node),
                tree.post_order_offset(node)
            );
        }
    }
}
