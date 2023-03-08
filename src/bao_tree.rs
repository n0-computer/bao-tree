use crate::tree::{hash_chunk, ByteNum, ChunkNum, PONum, BLAKE3_CHUNK_SIZE};
use blake3::guts::parent_cv;
use range_collections::AbstractRangeSet;
use std::{
    fmt,
    num::NonZeroU64,
    ops::{Bound, Range, RangeBounds},
};

/// todo: change once we have chunk groups
type BlockNum = ChunkNum;

/// Defines a Bao tree.
///
/// This is just the specification of the tree, it does not contain any actual data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaoTree {
    /// Total number of bytes in the file
    size: ByteNum,
    /// Log base 2 of the chunk group size
    chunk_group_log: u8,
}

impl BaoTree {
    /// Create a new BaoTree
    pub fn new(size: ByteNum, chunk_group_log: u8) -> BaoTree {
        // no support for chunk groups yet
        assert!(chunk_group_log == 0);
        BaoTree {
            size,
            chunk_group_log,
        }
    }

    /// Root of the tree
    pub fn root(&self) -> TreeNode {
        let blocks = self.blocks().0;
        TreeNode(((blocks + 1) / 2).next_power_of_two() - 1)
    }

    /// number of blocks in the tree
    ///
    /// At chunk group size 1, this is the same as the number of chunks
    /// Even a tree with 0 bytes size has a single block
    pub fn blocks(&self) -> BlockNum {
        let size = self.size.0;
        let block_bits = self.chunk_group_log + 10;
        let block_mask = (1 << block_bits) - 1;
        let full_blocks = size >> block_bits;
        let open_block = ((size & block_mask) != 0) as u64;
        ChunkNum((full_blocks + open_block).max(1))
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

    /// iterate over all nodes in the tree in depth first, left to right, post order
    pub fn iterate(&self) -> impl Iterator<Item = TreeNode> {
        // todo: make this a proper iterator
        let nodes = self.node_count();
        let mut res = Vec::with_capacity(nodes.try_into().unwrap());
        self.iterate_rec(self.root(), &mut res);
        res.into_iter()
    }

    pub fn chunk_num(&self, node: TreeNode) -> ChunkNum {
        assert!(node.is_leaf());
        // block number of a leaf node is just the node number
        // multiply by chunk_group_size to get the chunk number
        ChunkNum(node.0 << self.chunk_group_log)
    }

    pub fn outboard_post_order(data: &[u8]) -> (Vec<u8>, blake3::Hash) {
        let mut stack = Vec::<blake3::Hash>::with_capacity(16);
        let tree = Self::new(ByteNum(data.len() as u64), 0);
        let root = tree.root();
        let outboard_len: usize = (tree.outboard_hash_pairs() * 64 + 8).try_into().unwrap();
        let mut res = Vec::with_capacity(outboard_len);
        for node in tree.iterate() {
            let hash = if node.is_leaf() {
                match tree.leaf_ranges(node) {
                    Ok((l, r)) => {
                        let left = &data[l.start.to_usize()..l.end.to_usize()];
                        let right = &data[r.start.to_usize()..r.end.to_usize()];
                        let left_hash = hash_chunk(tree.chunk_num(node), left, false);
                        let right_hash = hash_chunk(
                            tree.chunk_num(node) + tree.chunk_group_chunks(),
                            right,
                            false,
                        );
                        res.extend_from_slice(left_hash.as_bytes());
                        res.extend_from_slice(right_hash.as_bytes());
                        parent_cv(&left_hash, &right_hash, node == root)
                    }
                    Err(Range { start, end }) => {
                        let left = &data[start.to_usize()..end.to_usize()];
                        hash_chunk(tree.chunk_num(node), left, node == root)
                    }
                }
            } else {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                res.extend_from_slice(left_hash.as_bytes());
                res.extend_from_slice(right_hash.as_bytes());
                parent_cv(&left_hash, &right_hash, node == root)
            };
            stack.push(hash);
        }
        res.extend_from_slice(&(data.len() as u64).to_le_bytes());
        debug_assert_eq!(stack.len(), 1);
        debug_assert_eq!(res.len(), outboard_len);
        let hash = stack.pop().unwrap();
        (res, hash)
    }

    pub fn blake3_hash(data: &[u8]) -> blake3::Hash {
        let mut stack = Vec::with_capacity(16);
        let tree = Self::new(ByteNum(data.len() as u64), 0);
        let root = tree.root();
        for node in tree.iterate() {
            let hash = if node.is_leaf() {
                match tree.leaf_ranges(node) {
                    Ok((l, r)) => {
                        let left = &data[l.start.to_usize()..l.end.to_usize()];
                        let right = &data[r.start.to_usize()..r.end.to_usize()];
                        let left_hash = hash_chunk(tree.chunk_num(node), left, false);
                        let right_hash = hash_chunk(
                            tree.chunk_num(node) + tree.chunk_group_chunks(),
                            right,
                            false,
                        );
                        parent_cv(&left_hash, &right_hash, node == root)
                    }
                    Err(Range { start, end }) => {
                        let left = &data[start.to_usize()..end.to_usize()];
                        hash_chunk(tree.chunk_num(node), left, node == root)
                    }
                }
            } else {
                let right = stack.pop().unwrap();
                let left = stack.pop().unwrap();
                parent_cv(&left, &right, node == root)
            };
            stack.push(hash);
        }
        debug_assert_eq!(stack.len(), 1);
        stack.pop().unwrap()
    }

    fn leaf_ranges(
        &self,
        leaf: TreeNode,
    ) -> std::result::Result<(Range<ByteNum>, Range<ByteNum>), Range<ByteNum>> {
        assert!(leaf.is_leaf());
        let chunk_group_bytes = self.chunk_group_bytes();
        let start = chunk_group_bytes * leaf.0;
        let mid = start + chunk_group_bytes;
        let end = start + chunk_group_bytes * 2;
        if !(start < self.size || (start == 0 && self.size == 0)) {
            debug_assert!(start < self.size || (start == 0 && self.size == 0));
        }
        if mid >= self.size {
            Err((start..self.size))
        } else {
            Ok((start..mid, mid..end.min(self.size)))
        }
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

    /// iterate over all nodes in the tree in depth first, left to right, post order
    /// that are required to validate the given ranges
    pub fn iterate_part(
        &self,
        ranges: impl AbstractRangeSet<u64>,
    ) -> impl Iterator<Item = TreeNode> {
        todo!();
        vec![].into_iter()
    }

    const fn chunk_group_chunks(&self) -> ChunkNum {
        ChunkNum(1 << self.chunk_group_log)
    }

    const fn chunk_group_bytes(&self) -> ByteNum {
        self.chunk_group_chunks().to_bytes()
    }
}

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

    fn partition(&self, x: u64) -> (RangeSetRef, RangeSetRef) {
        match self.boundaries.binary_search(&x) {
            Ok(i) => {
                let (left, right) = self.boundaries.split_at(i);
                (
                    RangeSetRef::new(self.below ^ is_odd(i), left),
                    RangeSetRef::new(self.below ^ !is_odd(i), right),
                )
            }
            Err(i) => {
                let (left, right) = self.boundaries.split_at(i);
                (
                    RangeSetRef::new(self.below ^ !is_odd(i), left),
                    RangeSetRef::new(self.below ^ is_odd(i), right),
                )
            }
        }
    }

    fn serialize(&self) -> (bool, impl Iterator<Item = NonZeroU64> + '_) {
        let below = self.below;
        let iter = (0..self.boundaries.len().checked_sub(1).unwrap_or_default()).map(move |i| {
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

fn root(leafs: ChunkNum) -> TreeNode {
    TreeNode(root0(leafs))
}

fn root0(leafs: ChunkNum) -> u64 {
    leafs.0.next_power_of_two() - 1
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TreeNode(u64);

#[derive(Clone, Copy)]
pub struct LeafNode(u64);

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
    fn root(chunks: ChunkNum) -> TreeNode {
        TreeNode(((chunks.0 + 1) / 2).next_power_of_two() - 1)
    }

    fn iterate(nn: Self, len: TreeNode, s: RangeSetRef, res: &mut Vec<Self>) {
        if s.is_empty() {
            return;
        }
        if s.is_all() {
            res.push(nn);
            return;
        }
        if nn.is_leaf() {
            res.push(nn);
            return;
        } else {
            let mid = nn.0 + 1;
            let (sl, sr) = s.partition(mid);
            let l = nn.left_child().unwrap();
            let r = nn.right_descendant(len).unwrap();
            Self::iterate(l, len, sl, res);
            Self::iterate(r, len, sr, res);
        }
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

    pub const fn leaf_number(&self) -> Option<ChunkNum> {
        if self.is_leaf() {
            Some(ChunkNum(self.0))
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

    pub fn chunk_range(&self) -> Range<ChunkNum> {
        let Range { start, end } = self.chunk_range0();
        ChunkNum(start)..ChunkNum(end)
    }

    pub fn byte_range(&self) -> Range<ByteNum> {
        let Range { start, end } = self.chunk_range0();
        ByteNum(start * BLAKE3_CHUNK_SIZE)..ByteNum(end * BLAKE3_CHUNK_SIZE)
    }

    /// Rang of chunks this node covers
    const fn chunk_range0(&self) -> Range<u64> {
        let level = self.level();
        let nn = self.0;
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

    pub fn post_order_offset(&self) -> PONum {
        PONum(self.post_order_offset0())
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

pub fn post_order_outboard(data: &[u8]) -> Vec<(blake3::Hash, blake3::Hash)> {
    let has_incomplete = data.len() % 1024 != 0;
    let chunks = ChunkNum((data.len() / 1024 + has_incomplete as usize).max(1) as u64);
    let nodes = TreeNode::filled_size(chunks);
    let root = TreeNode::root(chunks);
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

fn post_order_outboard_rec(
    data: &[u8],
    valid_nodes: TreeNode,
    nn: TreeNode,
    is_root: bool,
    incomplete: usize,
    res: &mut Outboard,
) -> blake3::Hash {
    let (lh, rh) = if nn.is_leaf() {
        let chunk_range = nn.chunk_range();
        let range = nn.byte_range();
        let mut data = &data[range.start.to_usize()..];
        if data.len() <= 1024 {
            return hash_chunk(chunk_range.start, data, is_root);
        } else {
            if data.len() > 2048 {
                data = &data[..2048];
            }
            let lh = hash_chunk(chunk_range.start, &data[..1024], false);
            let rh = hash_chunk(chunk_range.start + 1, &data[1024..], false);
            (lh, rh)
        }
    } else {
        let left = nn.left_child().unwrap();
        let right = nn.right_descendant(valid_nodes).unwrap();
        let lh = post_order_outboard_rec(data, valid_nodes, left, false, 0, res);
        let rh = post_order_outboard_rec(
            data,
            valid_nodes,
            right,
            false,
            incomplete.saturating_sub(1),
            res,
        );
        (lh, rh)
    };
    if incomplete > 0 {
        res.unstable[incomplete - 1] = (lh, rh);
    } else {
        let offset = nn.post_order_offset();
        res.stable[offset.to_usize()] = (lh, rh);
    }
    parent_cv(&lh, &rh, is_root)
}

pub fn blake3_hash(data: &[u8]) -> blake3::Hash {
    let chunks = ChunkNum((data.len() / 1024 + if data.len() % 1024 != 0 { 1 } else { 0 }) as u64);
    let nodes = TreeNode::filled_size(chunks);
    let root = TreeNode::root(chunks);
    blake3_hash_rec(data, nodes, root, true)
}

fn blake3_hash_rec(
    data: &[u8],
    valid_nodes: TreeNode,
    nn: TreeNode,
    is_root: bool,
) -> blake3::Hash {
    if nn.is_leaf() {
        let chunk_range = nn.chunk_range();
        let range = nn.byte_range();
        let mut data = &data[range.start.to_usize()..];
        if data.len() <= 1024 {
            hash_chunk(chunk_range.start, &data, is_root)
        } else {
            if data.len() > 2048 {
                data = &data[..2048];
            }
            let lh = hash_chunk(chunk_range.start, &data[..1024], false);
            let rh = hash_chunk(chunk_range.start + 1, &data[1024..], false);
            parent_cv(&lh, &rh, is_root)
        }
    } else {
        let left = nn.left_child().unwrap();
        let right = nn.right_descendant(valid_nodes).unwrap();
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

#[cfg(test)]
mod tests {

    use std::io::Write;

    use proptest::prelude::*;

    use super::{blake3_hash, post_order_outboard, BaoTree, TreeNode};
    use crate::tree::{hash_chunk, ByteNum, ChunkNum, PONum, BLAKE3_CHUNK_SIZE};

    fn compare_blake3_impl(data: Vec<u8>) {
        let h1 = blake3_hash(&data);
        let h2 = blake3::hash(&data);
        assert_eq!(h1, h2);
    }

    fn bao_tree_blake3_impl(data: Vec<u8>) {
        let h1 = BaoTree::blake3_hash(&data);
        let h2 = blake3::hash(&data);
        assert_eq!(h1, h2);
    }

    fn bao_tree_outboard_impl(data: Vec<u8>) {
        let mut expected = Vec::new();
        let cursor = std::io::Cursor::new(&mut expected);
        let mut encoder = abao::encode::Encoder::new_outboard(cursor);
        encoder.write_all(&data).unwrap();
        let expected_hash = encoder.finalize_post_order().unwrap();
        let (actual, actual_hash) = BaoTree::outboard_post_order(&data);
        assert_eq!(expected_hash, actual_hash);
    }

    fn compare_bao_outboard_impl(data: Vec<u8>) {
        let mut storage = Vec::new();
        let cursor = std::io::Cursor::new(&mut storage);
        let mut encoder = abao::encode::Encoder::new_outboard(cursor);
        encoder.write_all(&data).unwrap();
        encoder.finalize_post_order().unwrap();

        println!("{}", storage.len() / 64);
        let hashes1 = post_order_outboard(&data);
        let mut data1 = hashes1
            .iter()
            .map(|(l, r)| {
                let mut res = [0; 64];
                res[..32].copy_from_slice(l.as_bytes());
                res[32..].copy_from_slice(r.as_bytes());
                res
            })
            .flatten()
            .collect::<Vec<_>>();
        data1.extend_from_slice((data.len() as u64).to_le_bytes().as_ref());

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
    fn bao_tree_outboard_0() {
        bao_tree_outboard_impl(vec![]);
        bao_tree_outboard_impl(vec![0; 1]);
        bao_tree_outboard_impl(vec![0; 1023]);
        bao_tree_outboard_impl(vec![0; 1024]);
        bao_tree_outboard_impl(vec![0; 1025]);
        bao_tree_outboard_impl(vec![0; 2047]);
        bao_tree_outboard_impl(vec![0; 2048]);
        bao_tree_outboard_impl(vec![0; 2049]);
        bao_tree_outboard_impl(vec![0; 10000]);
        bao_tree_outboard_impl(vec![0; 20000]);
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

    #[test]
    fn bao_tree_blake3_0() {
        bao_tree_blake3_impl(vec![]);
        bao_tree_blake3_impl(vec![0; 1]);
        bao_tree_blake3_impl(vec![0; 1023]);
        bao_tree_blake3_impl(vec![0; 1024]);
        bao_tree_blake3_impl(vec![0; 1025]);
        bao_tree_blake3_impl(vec![0; 2047]);
        bao_tree_blake3_impl(vec![0; 2048]);
        bao_tree_blake3_impl(vec![0; 2049]);
        bao_tree_blake3_impl(vec![0; 10000]);
    }

    proptest! {
        #[test]
        fn compare_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            compare_blake3_impl(data);
        }

        #[test]
        fn bao_tree_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_blake3_impl(data);
        }

        #[test]
        fn bao_tree_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_outboard_impl(data);
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
            let nn = TreeNode(i);
            let o = nn.post_order_offset();
            let level = nn.level();
            let text = if level == 0 {
                format!("c {:?} {:?}", nn.leaf_number().unwrap(), nn.chunk_range())
            } else {
                format!(
                    "p ({:?}) ({:?}) ({:?})",
                    nn.node_range(),
                    nn.post_order_range(),
                    nn.chunk_range()
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
            println!(
                "{} {:?} {:?}",
                i,
                TreeNode::filled_size(ChunkNum(i)),
                TreeNode::root(ChunkNum(i))
            );
        }
    }

    #[test]
    fn bao_tree_iterate() {
        let tree = BaoTree::new(ByteNum(0), 0);
        for node in tree.iterate() {
            println!("{:#?}", node);
        }
    }
}
