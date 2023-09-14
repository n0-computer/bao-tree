//! Iterators over BaoTree nodes
//!
//! Range iterators take a reference to the ranges, and therefore require a lifetime parameter.
//! They can be used without lifetime parameters using self referencing structs.
use std::fmt;

use range_collections::{RangeSet2, RangeSetRef};
use self_cell::self_cell;
use smallvec::SmallVec;

use crate::{blake3, BaoTree, BlockSize, ByteNum, ChunkNum, TreeNode};

/// Extended node info.
///
/// Some of the information is redundant, but it is convenient to have it all in one place.
///
/// Usually this is used within an iterator, so we hope that the compiler will optimize away
/// the redundant information.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeInfo<'a> {
    /// the node
    pub node: TreeNode,
    /// the node is the root node (needs special handling when computing hash)
    pub is_root: bool,
    /// ranges of the node and it's two children
    pub ranges: &'a RangeSetRef<ChunkNum>,
    /// left child intersection with the query range
    pub l_ranges: &'a RangeSetRef<ChunkNum>,
    /// right child intersection with the query range
    pub r_ranges: &'a RangeSetRef<ChunkNum>,
    /// the node is fully included in the query range
    pub full: bool,
    /// the node is a leaf for the purpose of this query
    pub query_leaf: bool,
    /// true if this node is the last leaf, and it is <= half full
    pub is_half_leaf: bool,
}

/// Iterator over all nodes in a BaoTree in pre-order that overlap with a given chunk range.
///
/// This is mostly used internally
#[derive(Debug)]
pub struct PreOrderPartialIterRef<'a> {
    /// the tree we want to traverse
    tree: BaoTree,
    /// the maximum level that is skipped from the traversal if it is fully
    /// included in the query range.
    max_skip_level: u8,
    /// stack of nodes to visit, together with the ranges that are relevant for the node
    ///
    /// The node is shifted by the block size, so these are not normal nodes!
    stack: SmallVec<[(TreeNode, &'a RangeSetRef<ChunkNum>); 8]>,
    /// number of valid nodes, needed in shifted.right_descendant
    ///
    /// This is also shifted by the block size!
    shifted_filled_size: TreeNode,
    /// The root node, shifted by the block size, needed for the is_root check
    shifted_root: TreeNode,
}

impl<'a> PreOrderPartialIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>, max_skip_level: u8) -> Self {
        let mut stack = SmallVec::new();
        let (shifted_root, shifted_filled_size) = shift_tree(tree.size, tree.block_size);
        stack.push((shifted_root, ranges));
        Self {
            tree,
            max_skip_level,
            stack,
            shifted_filled_size,
            shifted_root,
        }
    }

    /// Get a reference to the tree.
    pub fn tree(&self) -> &BaoTree {
        &self.tree
    }
}

impl<'a> Iterator for PreOrderPartialIterRef<'a> {
    type Item = NodeInfo<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = &self.tree;
        loop {
            let (shifted, ranges) = self.stack.pop()?;
            if ranges.is_empty() {
                continue;
            }
            let node = shifted.subtract_block_size(tree.block_size.0);
            let is_half_leaf = !tree.is_persisted(node);
            let (l_ranges, r_ranges) = if !is_half_leaf {
                // the middle chunk of the node
                let mid = node.mid();
                // split the ranges into left and right
                ranges.split(mid)
            } else {
                (ranges, ranges)
            };
            // the start chunk of the node
            let start = node.chunk_range().start;
            // check if the node is fully included
            let full = ranges.boundaries().len() == 1 && ranges.boundaries()[0] <= start;
            // we can't recurse if the node is a leaf
            // we don't want to recurse if the node is full and below the minimum level
            let query_leaf =
                self.tree.is_leaf(node) || (full && node.level() <= self.max_skip_level as u32);
            // recursion is just pushing the children onto the stack
            if !query_leaf {
                let l = shifted.left_child().unwrap();
                let r = shifted.right_descendant(self.shifted_filled_size).unwrap();
                // push right first so we pop left first
                self.stack.push((r, r_ranges));
                self.stack.push((l, l_ranges));
            }
            // the first node is the root, so just set the flag to false afterwards
            let is_root = shifted == self.shifted_root;
            // emit the node in any case
            break Some(NodeInfo {
                node,
                ranges,
                l_ranges,
                r_ranges,
                full,
                query_leaf,
                is_root,
                is_half_leaf,
            });
        }
    }
}

/// Given a tree of size `size` and block size `block_size`,
/// compute the root node and the number of nodes for a shifted tree.
pub(crate) fn shift_tree(size: ByteNum, level: BlockSize) -> (TreeNode, TreeNode) {
    let level = level.0;
    let size = size.0;
    let shift = 10 + level;
    let mask = (1 << shift) - 1;
    // number of full blocks of size 1024 << level
    let full_blocks = size >> shift;
    // 1 if the last block is non zero, 0 otherwise
    let open_block = ((size & mask) != 0) as u64;
    // total number of blocks, rounding up to 1 if there are no blocks
    let blocks = (full_blocks + open_block).max(1);
    let n = (blocks + 1) / 2;
    // root node
    let root = n.next_power_of_two() - 1;
    // number of nodes in the tree
    let filled_size = n + n.saturating_sub(1);
    (TreeNode(root), TreeNode(filled_size))
}

/// Iterator over all nodes in a tree in post-order.
///
/// If you want to iterate only down to some level, you need to shift the tree
/// before.
#[derive(Debug)]
pub struct PostOrderNodeIter {
    /// the overall number of nodes in the tree
    len: TreeNode,
    /// the current node, None if we are done
    curr: TreeNode,
    /// where we came from, used to determine the next node
    prev: Prev,
}

impl PostOrderNodeIter {
    /// Create a new iterator given a root node and a len
    pub fn new(root: TreeNode, len: TreeNode) -> Self {
        Self {
            curr: root,
            len,
            prev: Prev::Parent,
        }
    }

    fn go_up(&mut self, curr: TreeNode) {
        let prev = curr;
        (self.curr, self.prev) = if let Some(parent) = curr.restricted_parent(self.len) {
            (
                parent,
                if prev < parent {
                    Prev::Left
                } else {
                    Prev::Right
                },
            )
        } else {
            (curr, Prev::Done)
        };
    }
}

impl Iterator for PostOrderNodeIter {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let curr = self.curr;
            match self.prev {
                Prev::Parent => {
                    if let Some(child) = curr.left_child() {
                        // go left first when coming from above, don't emit curr
                        self.curr = child;
                        self.prev = Prev::Parent;
                    } else {
                        // we are a left or right leaf, go up and emit curr
                        self.go_up(curr);
                        break Some(curr);
                    }
                }
                Prev::Left => {
                    // no need to check is_leaf, since we come from a left child
                    // go right when coming from left, don't emit curr
                    self.curr = curr.right_descendant(self.len).unwrap();
                    self.prev = Prev::Parent;
                }
                Prev::Right => {
                    // go up in any case, do emit curr
                    self.go_up(curr);
                    break Some(curr);
                }
                Prev::Done => {
                    break None;
                }
            }
        }
    }
}

/// Iterator over all nodes in a BaoTree in pre-order.
#[derive(Debug)]
pub struct PreOrderNodeIter {
    /// the overall number of nodes in the tree
    len: TreeNode,
    /// the current node, None if we are done
    curr: TreeNode,
    /// where we came from, used to determine the next node
    prev: Prev,
}

impl PreOrderNodeIter {
    /// Create a new iterator given a root node and a len
    pub fn new(root: TreeNode, len: TreeNode) -> Self {
        Self {
            curr: root,
            len,
            prev: Prev::Parent,
        }
    }

    fn go_up(&mut self, curr: TreeNode) {
        let prev = curr;
        (self.curr, self.prev) = if let Some(parent) = curr.restricted_parent(self.len) {
            (
                parent,
                if prev < parent {
                    Prev::Left
                } else {
                    Prev::Right
                },
            )
        } else {
            (curr, Prev::Done)
        };
    }
}

impl Iterator for PreOrderNodeIter {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let curr = self.curr;
            match self.prev {
                Prev::Parent => {
                    if let Some(child) = curr.left_child() {
                        // go left first when coming from above
                        self.curr = child;
                        self.prev = Prev::Parent;
                    } else {
                        // we are a left or right leaf, go up
                        self.go_up(curr);
                    }
                    // emit curr before children (pre-order)
                    break Some(curr);
                }
                Prev::Left => {
                    // no need to check is_leaf, since we come from a left child
                    // go right when coming from left, don't emit curr
                    self.curr = curr.right_descendant(self.len).unwrap();
                    self.prev = Prev::Parent;
                }
                Prev::Right => {
                    // go up in any case
                    self.go_up(curr);
                }
                Prev::Done => {
                    break None;
                }
            }
        }
    }
}

#[derive(Debug)]
enum Prev {
    Parent,
    Left,
    Right,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A chunk describeds what to read or write next
///
/// In some cases you want additional information about what part of the chunk matches the query.
/// That is what the `R` type parameter is for. By default it is `()`.
pub enum BaoChunk<R = ()> {
    /// expect a 64 byte parent node.
    ///
    /// To validate, use parent_cv using the is_root value
    Parent {
        /// The tree node, useful for error reporting
        node: TreeNode,
        /// This is the root, to be passed to parent_cv
        is_root: bool,
        /// Push the left hash to the stack, since it will be needed later
        left: bool,
        /// Push the right hash to the stack, since it will be needed later
        right: bool,
        /// Additional information about what part of the chunk matches the query
        ranges: R,
    },
    /// expect data of size `size`
    ///
    /// To validate, use hash_block using the is_root and start_chunk values
    Leaf {
        /// Start chunk, to be passed to hash_block
        start_chunk: ChunkNum,
        /// Size of the data to expect. Will be chunk_group_bytes for all but the last block.
        size: usize,
        /// This is the root, to be passed to hash_block
        is_root: bool,
        /// Additional information about what part of the chunk matches the query
        ranges: R,
    },
}

impl<T> BaoChunk<T> {
    /// Create a dummy empty range
    fn empty(ranges: T) -> Self {
        Self::Leaf {
            size: 0,
            is_root: false,
            start_chunk: ChunkNum(0),
            ranges,
        }
    }

    /// Set the ranges to the unit value
    pub fn without_ranges(&self) -> BaoChunk {
        match self {
            Self::Parent {
                node,
                is_root,
                left,
                right,
                ..
            } => BaoChunk::Parent {
                node: *node,
                is_root: *is_root,
                left: *left,
                right: *right,
                ranges: (),
            },
            Self::Leaf {
                start_chunk,
                size,
                is_root,
                ..
            } => BaoChunk::Leaf {
                start_chunk: *start_chunk,
                size: *size,
                is_root: *is_root,
                ranges: (),
            },
        }
    }
}

/// Iterator over all chunks in a BaoTree in post-order.
#[derive(Debug)]
pub struct PostOrderChunkIter {
    tree: BaoTree,
    inner: PostOrderNodeIter,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk; 2],
    index: usize,
    shifted_root: TreeNode,
}

impl PostOrderChunkIter {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree) -> Self {
        let (shifted_root, shifted_len) = shift_tree(tree.size, tree.block_size);
        let inner = PostOrderNodeIter::new(shifted_root, shifted_len);
        Self {
            tree,
            inner,
            stack: Default::default(),
            index: 0,
            shifted_root,
        }
    }

    fn push(&mut self, item: BaoChunk) {
        self.stack[self.index] = item;
        self.index += 1;
    }

    fn pop(&mut self) -> Option<BaoChunk> {
        if self.index > 0 {
            self.index -= 1;
            Some(self.stack[self.index])
        } else {
            None
        }
    }
}

impl Iterator for PostOrderChunkIter {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pop() {
                return Some(item);
            }
            let shifted = self.inner.next()?;
            // the is_root check needs to be done before shifting the node
            let is_root = shifted == self.shifted_root;
            let node = shifted.subtract_block_size(self.tree.block_size.0);
            if shifted.is_leaf() {
                let tree = &self.tree;
                let (s, m, e) = tree.leaf_byte_ranges3(node);
                let l_start_chunk = node.chunk_range().start;
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                let is_half_leaf = m == e;
                // for the half leaf we emit just the leaf
                // for all other leaves we emit the parent and two leaves
                if !is_half_leaf {
                    self.push(BaoChunk::Parent {
                        node,
                        is_root,
                        left: true,
                        right: true,
                        ranges: (),
                    });
                    self.push(BaoChunk::Leaf {
                        is_root: false,
                        start_chunk: r_start_chunk,
                        size: (e - m).to_usize(),
                        ranges: (),
                    });
                };
                break Some(BaoChunk::Leaf {
                    is_root: is_root && is_half_leaf,
                    start_chunk: l_start_chunk,
                    size: (m - s).to_usize(),
                    ranges: (),
                });
            } else {
                self.push(BaoChunk::Parent {
                    node,
                    is_root,
                    left: true,
                    right: true,
                    ranges: (),
                });
            }
        }
    }
}

impl BaoChunk {
    /// Return the size of the chunk in bytes.
    pub fn size(&self) -> usize {
        match self {
            Self::Parent { .. } => 64,
            Self::Leaf { size, .. } => *size,
        }
    }
}

impl<T: Default> Default for BaoChunk<T> {
    fn default() -> Self {
        Self::Leaf {
            is_root: true,
            size: 0,
            start_chunk: ChunkNum(0),
            ranges: T::default(),
        }
    }
}

/// An iterator that produces chunks in pre order, but only for the parts of the
/// tree that are relevant for a query.
#[derive(Debug)]
pub struct PreOrderPartialChunkIterRef<'a> {
    inner: PreOrderPartialIterRef<'a>,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk<&'a RangeSetRef<ChunkNum>>; 2],
    index: usize,
}

impl<'a> PreOrderPartialChunkIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>, max_skip_level: u8) -> Self {
        Self {
            inner: tree.ranges_pre_order_nodes_iter(ranges, max_skip_level),
            // todo: get rid of this when &RangeSetRef has a default
            stack: [BaoChunk::empty(ranges), BaoChunk::empty(ranges)],
            index: 0,
        }
    }

    /// Return a reference to the underlying tree.
    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
    }

    fn push(&mut self, item: BaoChunk<&'a RangeSetRef<ChunkNum>>) {
        self.stack[self.index] = item;
        self.index += 1;
    }

    fn pop(&mut self) -> Option<BaoChunk<&'a RangeSetRef<ChunkNum>>> {
        if self.index > 0 {
            self.index -= 1;
            Some(self.stack[self.index])
        } else {
            None
        }
    }
}

impl<'a> Iterator for PreOrderPartialChunkIterRef<'a> {
    type Item = BaoChunk<&'a RangeSetRef<ChunkNum>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pop() {
                return Some(item);
            }
            let NodeInfo {
                node,
                is_root,
                is_half_leaf,
                ranges,
                l_ranges,
                r_ranges,
                query_leaf,
                ..
            } = self.inner.next()?;
            let tree = &self.inner.tree;
            let is_leaf = tree.is_leaf(node);
            if is_leaf {
                let (s, m, e) = tree.leaf_byte_ranges3(node);
                let l_start_chunk = node.chunk_range().start;
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                if !r_ranges.is_empty() && !is_half_leaf {
                    self.push(BaoChunk::Leaf {
                        is_root: false,
                        start_chunk: r_start_chunk,
                        size: (e - m).to_usize(),
                        ranges: r_ranges,
                    });
                };
                if !l_ranges.is_empty() {
                    self.push(BaoChunk::Leaf {
                        is_root: is_root && is_half_leaf,
                        start_chunk: l_start_chunk,
                        size: (m - s).to_usize(),
                        ranges: l_ranges,
                    });
                };
            }
            // the last leaf is a special case, since it does not have a parent if it is <= half full
            if !is_half_leaf {
                let chunk = if query_leaf && !is_leaf {
                    // the node is a leaf for the purpose of this query despite not being a leaf,
                    // so we need to return a BaoChunk::Leaf spanning the whole node
                    let tree = self.tree();
                    let bytes = tree.byte_range(node);
                    let start_chunk = bytes.start.chunks();
                    let size = (bytes.end.0 - bytes.start.0) as usize;
                    BaoChunk::Leaf {
                        start_chunk,
                        is_root,
                        size,
                        ranges,
                    }
                } else {
                    // the node is not a leaf, so we need to return a BaoChunk::Parent
                    BaoChunk::Parent {
                        is_root,
                        left: !l_ranges.is_empty(),
                        right: !r_ranges.is_empty(),
                        node,
                        ranges,
                    }
                };
                break Some(chunk);
            }
        }
    }
}

/// An iterator that produces chunks in pre order.
///
/// This wraps a `PreOrderPartialIterRef` and iterates over the chunk groups
/// all the way down to individual chunks if needed.
#[derive(Debug)]
pub struct ResponseIterRef<'a> {
    inner: PreOrderPartialChunkIterRef<'a>,
    buffer: SmallVec<[BaoChunk; 10]>,
}

impl<'a> ResponseIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>) -> Self {
        Self {
            inner: PreOrderPartialChunkIterRef::new(tree, ranges, tree.block_size.0),
            buffer: SmallVec::new(),
        }
    }

    /// Return a reference to the underlying tree.
    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
    }
}

/// Reference implementation of the response iterator, using just the simple recursive
/// implementation [crate::iter::select_nodes_rec].
pub(crate) fn response_iter_ref_reference(
    tree: BaoTree,
    ranges: &RangeSetRef<ChunkNum>,
) -> Vec<BaoChunk> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.to_usize(),
        true,
        ranges,
        tree.block_size.0 as u32,
        &mut |x| res.push(x.without_ranges()),
    );
    res
}

///
#[derive(Debug)]
pub struct ResponseIterRef3<'a> {
    iter: std::vec::IntoIter<BaoChunk>,
    tree: BaoTree,
    _ranges: &'a RangeSetRef<ChunkNum>,
}

impl<'a> ResponseIterRef3<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>) -> Self {
        let iter = response_iter_ref_reference(tree, ranges).into_iter();
        Self {
            iter,
            tree,
            _ranges: ranges,
        }
    }

    /// Return a reference to the underlying tree.
    pub fn tree(&self) -> &BaoTree {
        &self.tree
    }
}

impl<'a> Iterator for ResponseIterRef3<'a> {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a> Iterator for ResponseIterRef<'a> {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.buffer.pop() {
                break Some(item);
            }
            let inner_next = self.inner.next()?;
            match inner_next {
                BaoChunk::Parent {
                    node,
                    is_root,
                    right,
                    left,
                    ranges: _,
                } => {
                    break Some(BaoChunk::Parent {
                        node,
                        is_root,
                        right,
                        left,
                        ranges: (),
                    });
                }
                BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                    ranges,
                } => {
                    if self.tree().block_size == BlockSize(0) || ranges.is_all() {
                        break Some(BaoChunk::Leaf {
                            size,
                            is_root,
                            start_chunk,
                            ranges: (),
                        });
                    } else {
                        // create a little tree just for this leaf
                        self.buffer.clear();
                        select_nodes_rec(
                            start_chunk,
                            size,
                            is_root,
                            ranges,
                            u32::MAX,
                            &mut |item| self.buffer.push(item),
                        );
                        self.buffer.reverse();
                    }
                }
            }
        }
    }
}

/// An iterator that produces chunks in pre order.
///
/// This wraps a `PreOrderPartialIterRef` and iterates over the chunk groups
/// all the way down to individual chunks if needed.
#[derive(Debug)]
pub struct ResponseIterRef2<'a> {
    inner: PreOrderPartialChunkIterRef<'a>,
}

impl<'a> ResponseIterRef2<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>) -> Self {
        let tree1 = BaoTree::new(tree.size, BlockSize::ZERO);
        Self {
            inner: PreOrderPartialChunkIterRef::new(tree1, ranges, tree.block_size.0),
        }
    }

    /// Return a reference to the underlying tree.
    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
    }
}

impl<'a> Iterator for ResponseIterRef2<'a> {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        let inner_next = self.inner.next()?;
        Some(match inner_next {
            BaoChunk::Parent {
                node,
                is_root,
                right,
                left,
                ranges: _,
            } => BaoChunk::Parent {
                node,
                is_root,
                right,
                left,
                ranges: (),
            },
            BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
                ..
            } => BaoChunk::Leaf {
                size,
                is_root,
                start_chunk,
                ranges: (),
            },
        })
    }
}

self_cell! {
    pub(crate) struct ResponseIterInner {
        owner: range_collections::RangeSet2<ChunkNum>,
        #[not_covariant]
        dependent: ResponseIterRef,
    }
}

impl ResponseIterInner {
    fn next(&mut self) -> Option<BaoChunk> {
        self.with_dependent_mut(|_, iter| iter.next())
    }

    fn tree(&self) -> &BaoTree {
        self.with_dependent(|_, iter| iter.tree())
    }
}

/// The owned version of `ResponseIterRef`.
pub struct ResponseIter(ResponseIterInner);

impl fmt::Debug for ResponseIter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseIter").finish_non_exhaustive()
    }
}

impl ResponseIter {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: RangeSet2<ChunkNum>) -> Self {
        Self(ResponseIterInner::new(ranges, |ranges| {
            ResponseIterRef::new(tree, ranges)
        }))
    }

    /// The tree this iterator is iterating over.
    pub fn tree(&self) -> &BaoTree {
        self.0.tree()
    }
}

impl Iterator for ResponseIter {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// Encode ranges relevant to a query from a slice and outboard to a buffer
///
/// This will compute the root hash, so it will have to traverse the entire tree.
/// The `ranges` parameter just controls which parts of the data are written.
///
/// Except for writing to a buffer, this is the same as [hash_subtree].
pub(crate) fn encode_selected_rec(
    start_chunk: ChunkNum,
    data: &[u8],
    is_root: bool,
    query: &RangeSetRef<ChunkNum>,
    min_level: u32,
    res: &mut Vec<u8>,
) -> blake3::Hash {
    use blake3::guts::{ChunkState, CHUNK_LEN};
    if data.len() <= CHUNK_LEN {
        if !query.is_empty() {
            res.extend_from_slice(data);
        }
        let mut hasher = ChunkState::new(start_chunk.0);
        hasher.update(data);
        hasher.finalize(is_root)
    } else {
        let chunks = data.len() / CHUNK_LEN + (data.len() % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let level = chunks.trailing_zeros() / 2;
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let mid_chunk = start_chunk + (mid as u64);
        let (l_ranges, r_ranges) = query.split(mid_chunk);
        // for empty ranges, we don't want to emit anything.
        // for full ranges where the level is at or below max_skip_level, we want to emit
        // just the data.
        //
        // todo: maybe call into blake3::guts::hash_subtree directly for this case? it would be faster.
        let emit_parent = !query.is_empty() && (!query.is_all() || level >= min_level);
        let hash_offset = if emit_parent {
            // make some room for the hashes
            res.extend_from_slice(&[0xFFu8; 64]);
            Some(res.len() - 64)
        } else {
            None
        };
        // recurse to the left and right to compute the hashes and emit data
        let left = encode_selected_rec(
            start_chunk,
            &data[..mid_bytes],
            false,
            l_ranges,
            min_level,
            res,
        );
        let right = encode_selected_rec(
            mid_chunk,
            &data[mid_bytes..],
            false,
            r_ranges,
            min_level,
            res,
        );
        // backfill the hashes if needed
        if let Some(o) = hash_offset {
            res[o..o + 32].copy_from_slice(left.as_bytes());
            res[o + 32..o + 64].copy_from_slice(right.as_bytes());
        }
        blake3::guts::parent_cv(&left, &right, is_root)
    }
}

/// Select nodes relevant to a query
///
/// This is the receive side equivalent of [bao_encode_selected_recursive].
/// It does not do any hashing, but just emits the nodes that are relevant to a query.
pub(crate) fn select_nodes_rec(
    start_chunk: ChunkNum,
    size: usize,
    is_root: bool,
    ranges: &RangeSetRef<ChunkNum>,
    min_level: u32,
    emit: &mut impl FnMut(BaoChunk),
) {
    if ranges.is_empty() {
        return;
    }
    use blake3::guts::CHUNK_LEN;
    if size <= CHUNK_LEN {
        emit(BaoChunk::Leaf {
            start_chunk,
            size,
            is_root,
            ranges: (),
        });
    } else {
        let chunks: usize = size / CHUNK_LEN + (size % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        // chunks is always a power of two, 2 for level 0
        // so we must subtract 1 to get the level, and this is also safe
        let level = chunks.trailing_zeros() - 1;
        if ranges.is_all() && level < min_level {
            // we are allowed to just emit the entire data as a leaf
            emit(BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
                ranges: (),
            });
        } else {
            // split in half and recurse
            assert!(start_chunk.0 % 2 == 0);
            let mid = chunks / 2;
            let mid_bytes = mid * CHUNK_LEN;
            let mid_chunk = start_chunk + (mid as u64);
            let (l_ranges, r_ranges) = ranges.split(mid_chunk);
            let node = TreeNode::from_start_chunk_and_level(start_chunk, BlockSize(level as u8));
            debug_assert_eq!(node.chunk_range().start, start_chunk);
            debug_assert_eq!(node.level(), level);
            emit(BaoChunk::Parent {
                node,
                is_root,
                left: !l_ranges.is_empty(),
                right: !r_ranges.is_empty(),
                ranges: (),
            });
            // recurse to the left and right to compute the hashes and emit data
            select_nodes_rec(start_chunk, mid_bytes, false, l_ranges, min_level, emit);
            select_nodes_rec(
                mid_chunk,
                size - mid_bytes,
                false,
                r_ranges,
                min_level,
                emit,
            );
        }
    }
}
