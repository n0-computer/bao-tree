use range_collections::RangeSetRef;
use smallvec::SmallVec;

use crate::{BaoTree, ChunkNum, TreeNode};

/// Extended node info.
///
/// Some of the information is redundant, but it is convenient to have it all in one place.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeInfo<'a> {
    /// the node
    pub node: TreeNode,
    /// left child intersection with the query range
    pub l_ranges: &'a RangeSetRef<ChunkNum>,
    /// right child intersection with the query range
    pub r_ranges: &'a RangeSetRef<ChunkNum>,
    /// the node is fully included in the query range
    pub full: bool,
    /// the node is a leaf for the purpose of this query
    pub query_leaf: bool,
    /// the node is the root node (needs special handling when computing hash)
    pub is_root: bool,
    /// true if this node is the last leaf, and it is <= half full
    pub is_half_leaf: bool,
}

/// Iterator over all nodes in a BaoTree in pre-order that overlap with a given chunk range.
///
/// This is mostly used internally
pub struct PreOrderPartialIterRef<'a> {
    /// the tree we want to traverse
    tree: BaoTree,
    /// number of valid nodes, needed in node.right_descendant
    tree_filled_size: TreeNode,
    /// minimum level of *full* nodes to visit
    min_level: u8,
    /// is root
    is_root: bool,
    /// stack of nodes to visit
    stack: SmallVec<[(TreeNode, &'a RangeSetRef<ChunkNum>); 8]>,
}

impl<'a> PreOrderPartialIterRef<'a> {
    pub fn new(tree: BaoTree, range: &'a RangeSetRef<ChunkNum>, min_level: u8) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root(), range));
        Self {
            tree,
            tree_filled_size: tree.filled_size(),
            min_level,
            stack,
            is_root: tree.start_chunk == 0,
        }
    }

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
            // the middle chunk of the node
            let mid = node.mid().to_chunks(tree.block_size);
            // the start chunk of the node
            let start = node.block_range().start.to_chunks(tree.block_size);
            // check if the node is fully included
            let full = ranges.boundaries().len() == 1 && ranges.boundaries()[0] <= start;
            // split the ranges into left and right
            let (l_ranges, r_ranges) = ranges.split(mid);
            // we can't recurse if the node is a leaf
            // we don't want to recurse if the node is full and below the minimum level
            let query_leaf = node.is_leaf() || (full && node.level() < self.min_level as u32);
            // recursion is just pushing the children onto the stack
            if !query_leaf {
                let l = node.left_child().unwrap();
                let r = node.right_descendant(self.tree_filled_size).unwrap();
                // push right first so we pop left first
                self.stack.push((r, r_ranges));
                self.stack.push((l, l_ranges));
            }
            let is_root = self.is_root;
            self.is_root = false;
            let is_half_leaf = !tree.is_persisted(node);
            // emit the node in any case
            break Some(NodeInfo {
                node,
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

// use ouroboros::self_referencing;
// #[self_referencing]
// struct PreOrderPartialIterInner<R: 'static> {
//     ranges: R,
//     #[borrows(ranges)]
//     #[not_covariant]
//     iter: PreOrderPartialIterRef<'this>,
// }

// /// Same as PreOrderPartialIterRef, but owns the ranges so it can be converted into a stream conveniently.
// pub struct PreOrderPartialIter<R: AsRef<RangeSetRef<ChunkNum>> + 'static>(
//     PreOrderPartialIterInner<R>,
// );

// impl<R: AsRef<RangeSetRef<ChunkNum>> + 'static> PreOrderPartialIter<R> {
//     /// Create a new PreOrderPartialIter.
//     ///
//     /// ranges has to implement `AsRef<RangeSetRef<ChunkNum>>`, so you can pass e.g. a RangeSet2.
//     pub fn new(tree: BaoTree, ranges: R) -> Self {
//         Self(
//             PreOrderPartialIterInnerBuilder {
//                 ranges,
//                 iter_builder: |ranges| PreOrderPartialIterRef::new(tree, ranges.as_ref(), 0),
//             }
//             .build(),
//         )
//     }
// }

/// Iterator over all nodes in a BaoTree in post-order.
pub struct PostOrderTreeIter {
    /// the overall number of nodes in the tree
    len: TreeNode,
    /// the current node, None if we are done
    curr: TreeNode,
    /// where we came from, used to determine the next node
    prev: Prev,
}

impl PostOrderTreeIter {
    pub fn new(tree: BaoTree) -> Self {
        Self {
            len: tree.filled_size(),
            curr: tree.root(),
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

impl Iterator for PostOrderTreeIter {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let curr = self.curr;
            match self.prev {
                Prev::Done => {
                    break None;
                }
                Prev::Parent => {
                    if curr.is_leaf() {
                        self.go_up(curr);
                        break Some(curr);
                    } else {
                        // go left first when coming from above, don't emit curr
                        self.curr = curr.left_child().unwrap();
                        self.prev = Prev::Parent;
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
            }
        }
    }
}

enum Prev {
    Parent,
    Left,
    Right,
    Done,
}

#[cfg(test)]
pub struct PostOrderTreeIterStack {
    len: TreeNode,
    // stack of (node, done) pairs
    // done=true means we immediately return the node
    //
    // this is not big enough for the worst case, but it's fine to allocate
    // for a giant tree
    //
    // todo: figure out how to get rid of the done flag
    stack: SmallVec<[(TreeNode, bool); 8]>,
}

#[cfg(test)]
impl PostOrderTreeIterStack {
    pub(crate) fn new(tree: BaoTree) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root(), false));
        let len = tree.filled_size();
        Self { len, stack }
    }
}

#[cfg(test)]
impl Iterator for PostOrderTreeIterStack {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, done) = self.stack.pop()?;
            if done || node.is_leaf() {
                return Some(node);
            } else {
                // push node back on stack, with done=true
                self.stack.push((node, true));
                // push right child on stack first, with done=false
                self.stack
                    .push((node.right_descendant(self.len).unwrap(), false));
                // push left child on stack, with done=false
                self.stack.push((node.left_child().unwrap(), false));
            }
        }
    }
}

/// Iterator over all chunks in a BaoTree in post-order.
pub struct PostOrderChunkIter {
    tree: BaoTree,
    inner: PostOrderTreeIter,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk; 2],
    index: usize,
    root: TreeNode,
}

impl PostOrderChunkIter {
    pub fn new(tree: BaoTree) -> Self {
        Self {
            tree,
            inner: PostOrderTreeIter::new(tree),
            stack: Default::default(),
            index: 0,
            root: tree.root(),
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
                });
            }
            if let Some(leaf) = node.as_leaf() {
                let tree = &self.tree;
                let (s, m, e) = tree.leaf_byte_ranges3(leaf);
                let l_start_chunk = tree.chunk_num(leaf);
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                let is_half_leaf = m == e;
                if !is_half_leaf {
                    self.push(BaoChunk::Leaf {
                        is_root: false,
                        start_chunk: r_start_chunk,
                        size: (e - m).to_usize(),
                    });
                };
                break Some(BaoChunk::Leaf {
                    is_root: is_root && is_half_leaf,
                    start_chunk: l_start_chunk,
                    size: (m - s).to_usize(),
                });
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A read item describes what comes next
pub enum BaoChunk {
    /// expect a 64 byte parent node.
    ///
    /// To validate, use parent_cv using the is_root value
    Parent {
        /// This is the root, to be passed to parent_cv
        is_root: bool,
        /// Push the right hash to the stack, since it will be needed later
        right: bool,
        /// Push the left hash to the stack, since it will be needed later
        left: bool,
        /// The tree node, useful for error reporting
        node: TreeNode,
    },
    Leaf {
        /// Size of the data to expect. Will be chunk_group_bytes for all but the last block.
        size: usize,
        /// This is the root, to be passed to hash_block
        is_root: bool,
        /// Start chunk, to be passed to hash_block
        start_chunk: ChunkNum,
    },
}

impl BaoChunk {
    pub fn size(&self) -> usize {
        match self {
            Self::Parent { .. } => 64,
            Self::Leaf { size, .. } => *size,
        }
    }
}

impl Default for BaoChunk {
    fn default() -> Self {
        Self::Leaf {
            is_root: true,
            size: 0,
            start_chunk: ChunkNum(0),
        }
    }
}

/// An iterator that produces chunks in pre order, but only for the parts of the
/// tree that are relevant for a query.
pub struct ChunkIterRef<'a> {
    inner: PreOrderPartialIterRef<'a>,
    // stack with 2 elements, since we can only have 2 items in flight
    stack: [BaoChunk; 2],
    index: usize,
}

impl<'a> ChunkIterRef<'a> {
    pub fn new(tree: BaoTree, query: &'a RangeSetRef<ChunkNum>, min_level: u8) -> Self {
        Self {
            inner: PreOrderPartialIterRef::new(tree, query, min_level),
            stack: Default::default(),
            index: 0,
        }
    }

    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
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

impl<'a> Iterator for ChunkIterRef<'a> {
    type Item = BaoChunk;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pop() {
                return Some(item);
            }
            let NodeInfo {
                node,
                is_root,
                is_half_leaf,
                l_ranges,
                r_ranges,
                ..
            } = self.inner.next()?;
            if let Some(leaf) = node.as_leaf() {
                let tree = &self.inner.tree;
                let (s, m, e) = tree.leaf_byte_ranges3(leaf);
                let l_start_chunk = tree.chunk_num(leaf);
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                if !r_ranges.is_empty() && !is_half_leaf {
                    self.push(BaoChunk::Leaf {
                        is_root: false,
                        start_chunk: r_start_chunk,
                        size: (e - m).to_usize(),
                    });
                };
                if !l_ranges.is_empty() {
                    self.push(BaoChunk::Leaf {
                        is_root: is_root && is_half_leaf,
                        start_chunk: l_start_chunk,
                        size: (m - s).to_usize(),
                    });
                };
            }
            // the last leaf is a special case, since it does not have a parent if it is <= half full
            if !is_half_leaf {
                break Some(BaoChunk::Parent {
                    is_root,
                    left: !l_ranges.is_empty(),
                    right: !r_ranges.is_empty(),
                    node,
                });
            }
        }
    }
}
