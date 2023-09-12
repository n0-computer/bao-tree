//! Iterators over BaoTree nodes
//!
//! Range iterators take a reference to the ranges, and therefore require a lifetime parameter.
//! They can be used without lifetime parameters using self referencing structs.
use std::fmt;

use range_collections::{RangeSet2, RangeSetRef};
use self_cell::self_cell;
use smallvec::SmallVec;

use crate::{blake3, BaoTree, BlockSize, ChunkNum, TreeNode};

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
    /// number of valid nodes, needed in node.right_descendant
    tree_filled_size: TreeNode,
    /// the maximum level that is skipped from the traversal if it is fully
    /// included in the query range.
    max_skip_level: u8,
    /// is root
    is_root: bool,
    /// stack of nodes to visit
    stack: SmallVec<[(TreeNode, &'a RangeSetRef<ChunkNum>); 8]>,
}

impl<'a> PreOrderPartialIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>, max_skip_level: u8) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root_for_level(), ranges));
        Self {
            tree,
            tree_filled_size: tree.filled_size(),
            max_skip_level,
            stack,
            is_root: true,
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
            let (node, ranges) = self.stack.pop()?;
            if ranges.is_empty() {
                continue;
            }
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
                let l = node.left_child().unwrap();
                let r = node
                    .right_descendant_to(self.tree_filled_size, self.tree.block_size)
                    .unwrap();
                // push right first so we pop left first
                self.stack.push((r, r_ranges));
                self.stack.push((l, l_ranges));
            }
            let is_root = self.is_root;
            self.is_root = false;
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

/// Iterator over all nodes in a BaoTree in post-order.
#[derive(Debug)]
pub struct PostOrderNodeIter {
    /// the overall number of nodes in the tree
    len: TreeNode,
    /// the current node, None if we are done
    curr: TreeNode,
    /// where we came from, used to determine the next node
    prev: Prev,
    /// block size
    block_size: BlockSize,
}

impl PostOrderNodeIter {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree) -> Self {
        Self {
            len: tree.filled_size(),
            curr: tree.root_for_level(),
            prev: Prev::Parent,
            block_size: tree.block_size,
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

    fn next0(&mut self) -> Option<TreeNode> {
        loop {
            let curr = self.curr;
            match self.prev {
                Prev::Parent => {
                    if curr.level() > self.block_size.0 as u32 {
                        // go left first when coming from above, don't emit curr
                        self.curr = curr.left_child().unwrap();
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
                    self.curr = curr.right_descendant_to(self.len, self.block_size).unwrap();
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

impl Iterator for PostOrderNodeIter {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0()
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
    /// block size
    block_size: BlockSize,
}

impl PreOrderNodeIter {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree) -> Self {
        Self {
            len: tree.filled_size(),
            curr: tree.root_for_level(),
            prev: Prev::Parent,
            block_size: tree.block_size,
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
                    if curr.level() > self.block_size.0 as u32 {
                        // go left first when coming from above
                        self.curr = curr.left_child().unwrap();
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
                    self.curr = curr.right_descendant_to(self.len, self.block_size).unwrap();
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

/// A chunk describes what to expect from a response stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseChunk {
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
        ///
        is_subchunk: bool,
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
    },
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
}

/// Iterator over all chunks in a BaoTree in post-order.
#[derive(Debug)]
pub struct PostOrderChunkIter {
    tree: BaoTree,
    inner: PostOrderNodeIter,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk; 2],
    index: usize,
    root: TreeNode,
}

impl PostOrderChunkIter {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree) -> Self {
        let inner = PostOrderNodeIter::new(tree);
        let root = inner.curr;
        Self {
            tree,
            inner,
            stack: Default::default(),
            index: 0,
            root,
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
            let node = self.inner.next()?;
            let is_root = node == self.root;
            if self.tree.is_persisted(node) {
                self.push(BaoChunk::Parent {
                    node,
                    is_root,
                    left: true,
                    right: true,
                    ranges: (),
                });
            }
            if node.level() == self.tree.block_size.0 as u32 {
                let tree = &self.tree;
                let (s, m, e) = tree.leaf_byte_ranges3(node);
                let l_start_chunk = node.chunk_range().start;
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                let is_half_leaf = m == e;
                if !is_half_leaf {
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
pub struct PreOrderChunkIterRef<'a> {
    inner: PreOrderPartialIterRef<'a>,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk<&'a RangeSetRef<ChunkNum>>; 2],
    index: usize,
}

impl<'a> PreOrderChunkIterRef<'a> {
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

impl<'a> Iterator for PreOrderChunkIterRef<'a> {
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
    inner: PreOrderChunkIterRef<'a>,
    buffer: SmallVec<[ResponseChunk; 10]>,
}

impl<'a> ResponseIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a RangeSetRef<ChunkNum>) -> Self {
        Self {
            inner: PreOrderChunkIterRef::new(tree, ranges, 0),
            buffer: SmallVec::new(),
        }
    }

    /// Return a reference to the underlying tree.
    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
    }
}
impl<'a> Iterator for ResponseIterRef<'a> {
    type Item = ResponseChunk;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.buffer.pop() {
                break Some(item);
            }
            match self.inner.next()? {
                BaoChunk::Parent {
                    node,
                    is_root,
                    right,
                    left,
                    ..
                } => {
                    break Some(ResponseChunk::Parent {
                        node: node.subtract_block_size(self.tree().block_size.0),
                        is_root,
                        right,
                        left,
                        is_subchunk: false,
                    });
                }
                BaoChunk::Leaf {
                    size,
                    is_root,
                    start_chunk,
                    ranges,
                    ..
                } => {
                    if self.tree().block_size == BlockSize(0) || ranges.is_all() {
                        break Some(ResponseChunk::Leaf {
                            size,
                            is_root,
                            start_chunk,
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

self_cell! {
    pub(crate) struct ResponseIterInner {
        owner: range_collections::RangeSet2<ChunkNum>,
        #[not_covariant]
        dependent: ResponseIterRef,
    }
}

impl ResponseIterInner {
    fn next(&mut self) -> Option<ResponseChunk> {
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
    type Item = ResponseChunk;

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
    max_skip_level: u32,
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
        let level = chunks.trailing_zeros();
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let mid_chunk = start_chunk + (mid as u64);
        let (l_ranges, r_ranges) = query.split(mid_chunk);
        // for empty ranges, we don't want to emit anything.
        // for full ranges where the level is at or below max_skip_level, we want to emit
        // just the data.
        //
        // todo: maybe call into blake3::guts::hash_subtree directly for this case? it would be faster.
        #[allow(clippy::nonminimal_bool)]
        let emit_parent = !query.is_empty() && !(query.is_all() && level <= max_skip_level);
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
            max_skip_level,
            res,
        );
        let right = encode_selected_rec(
            mid_chunk,
            &data[mid_bytes..],
            false,
            r_ranges,
            max_skip_level,
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
    max_skip_level: u32,
    emit: &mut impl FnMut(ResponseChunk),
) {
    if ranges.is_empty() {
        return;
    }
    use blake3::guts::CHUNK_LEN;
    if size <= CHUNK_LEN {
        emit(ResponseChunk::Leaf {
            start_chunk,
            size,
            is_root,
        });
    } else {
        let chunks: usize = size / CHUNK_LEN + (size % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let level = chunks.trailing_zeros();
        if ranges.is_all() && level <= max_skip_level {
            // we are allowed to just emit the entire data as a leaf
            emit(ResponseChunk::Leaf {
                start_chunk,
                size,
                is_root,
            });
        } else {
            // split in half and recurse
            let mid = chunks / 2;
            let mid_bytes = mid * CHUNK_LEN;
            let mid_chunk = start_chunk + (mid as u64);
            let (l_ranges, r_ranges) = ranges.split(mid_chunk);
            emit(ResponseChunk::Parent {
                // TODO: do not use the marker here!
                // node: TreeNode(u64::MAX),
                node: TreeNode::from_start_chunk_and_level(start_chunk, BlockSize(0)),
                is_root,
                left: !l_ranges.is_empty(),
                right: !r_ranges.is_empty(),
                is_subchunk: true,
            });
            // recurse to the left and right to compute the hashes and emit data
            select_nodes_rec(
                start_chunk,
                mid_bytes,
                false,
                l_ranges,
                max_skip_level,
                emit,
            );
            select_nodes_rec(
                mid_chunk,
                size - mid_bytes,
                false,
                r_ranges,
                max_skip_level,
                emit,
            );
        }
    }
}
