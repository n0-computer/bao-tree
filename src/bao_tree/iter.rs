use ouroboros::self_referencing;
use range_collections::RangeSetRef;
use smallvec::SmallVec;

use crate::{bao_tree::TreeNode, BaoTree, ChunkNum};

/// Simple node info.
///
/// Just a node and how it relates to the query range.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeInfo {
    /// the node
    pub node: TreeNode,
    /// left child intersects with the query range
    pub tl: bool,
    /// right child intersects with the query range
    pub tr: bool,
}

/// Extended node info.
///
/// Compared to a simple node info, this also contains information about whether
/// the node is fully included in the query range.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeInfo2 {
    /// the node
    pub node: TreeNode,
    /// left child intersects with the query range
    pub tl: bool,
    /// right child intersects with the query range
    pub tr: bool,
    /// the node is fully included in the query range
    pub full: bool,
}

/// Iterator over all nodes in a BaoTree in pre-order that overlap with a given chunk range.
pub struct PreOrderPartialIterRef<'a> {
    /// the tree we want to traverse
    tree: BaoTree,
    /// number of valid nodes, needed in node.right_descendant
    valid_nodes: TreeNode,
    /// stack of nodes to visit
    stack: SmallVec<[(TreeNode, &'a RangeSetRef<ChunkNum>); 8]>,
}

impl<'a> PreOrderPartialIterRef<'a> {
    pub fn new(tree: BaoTree, range: &'a RangeSetRef<ChunkNum>) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root(), range));
        Self {
            tree,
            stack,
            valid_nodes: tree.filled_size(),
        }
    }
}

impl<'a> Iterator for PreOrderPartialIterRef<'a> {
    type Item = NodeInfo;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = &self.tree;
        loop {
            let (node, ranges) = self.stack.pop()?;
            if ranges.is_empty() {
                continue;
            }
            // the middle chunk of the node
            let mid = node.mid().to_chunks(tree.chunk_group_log);
            // split the ranges into left and right
            let (l_ranges, r_ranges) = ranges.split(mid);
            if !node.is_leaf() {
                let l = node.left_child().unwrap();
                let r = node.right_descendant(self.valid_nodes).unwrap();
                // push right first so we pop left first
                self.stack.push((r, r_ranges));
                self.stack.push((l, l_ranges));
            }
            // emit the node in any case
            break Some(NodeInfo {
                node,
                tl: !l_ranges.is_empty(),
                tr: !r_ranges.is_empty(),
            });
        }
    }
}

#[self_referencing]
struct PreOrderPartialIterInner<R: 'static> {
    ranges: R,
    #[borrows(ranges)]
    #[not_covariant]
    iter: PreOrderPartialIterRef<'this>,
}

/// Same as PreOrderPartialIterRef, but owns the ranges so it can be converted into a stream conveniently.
pub struct PreOrderPartialIter<R: AsRef<RangeSetRef<ChunkNum>> + 'static>(
    PreOrderPartialIterInner<R>,
);

impl<R: AsRef<RangeSetRef<ChunkNum>> + 'static> PreOrderPartialIter<R> {
    /// Create a new PreOrderPartialIter.
    ///
    /// ranges has to implement AsRef<RangeSetRef<ChunkNum>>, so you can pass e.g. a RangeSet2.
    pub fn new(tree: BaoTree, ranges: R) -> Self {
        Self(
            PreOrderPartialIterInnerBuilder {
                ranges,
                iter_builder: |ranges| PreOrderPartialIterRef::new(tree, ranges.as_ref()),
            }
            .build(),
        )
    }
}

impl<A: AsRef<RangeSetRef<ChunkNum>> + 'static> Iterator for PreOrderPartialIter<A> {
    type Item = NodeInfo;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.with_iter_mut(|x| x.next())
    }
}

/// Iterator over all nodes in a BaoTree in pre-order that overlap with a given chunk range.
pub struct PreOrderPartialTreeIterRef<'a> {
    /// the tree we want to traverse
    tree: BaoTree,
    /// number of valid nodes, needed in node.right_descendant
    valid_nodes: TreeNode,
    /// stack of nodes to visit
    stack: SmallVec<[(TreeNode, &'a RangeSetRef<ChunkNum>); 8]>,
}

impl<'a> PreOrderPartialTreeIterRef<'a> {
    pub fn new(tree: BaoTree, range: &'a RangeSetRef<ChunkNum>) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root(), range));
        Self {
            tree,
            stack,
            valid_nodes: tree.filled_size(),
        }
    }
}

impl<'a> Iterator for PreOrderPartialTreeIterRef<'a> {
    type Item = NodeInfo2;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = &self.tree;
        loop {
            let (node, ranges) = self.stack.pop()?;
            if ranges.is_empty() {
                continue;
            }
            // the middle chunk of the node
            let mid = node.mid().to_chunks(tree.chunk_group_log);
            // the start chunk of the node
            let start = node.block_range().start.to_chunks(tree.chunk_group_log);
            // split the ranges into left and right
            let (l_ranges, r_ranges) = ranges.split(mid);
            // check if the node is fully included
            let is_fully_included =
                ranges.boundaries().len() == 1 && ranges.boundaries()[0] <= start;
            // this is the only difference to the other iterator.
            // When a node is fully included in the ranges we don't traverse the children.
            if !is_fully_included && !node.is_leaf() {
                let l = node.left_child().unwrap();
                let r = node.right_descendant(self.valid_nodes).unwrap();
                // push right first so we pop left first
                self.stack.push((r, r_ranges));
                self.stack.push((l, l_ranges));
            }
            // emit the node in any case
            break Some(NodeInfo2 {
                node,
                tl: !l_ranges.is_empty(),
                tr: !r_ranges.is_empty(),
                full: is_fully_included,
            });
        }
    }
}

#[self_referencing]
struct PreOrderPartialTreeIterInner<R: 'static> {
    ranges: R,
    #[borrows(ranges)]
    #[not_covariant]
    iter: PreOrderPartialTreeIterRef<'this>,
}

/// Same as PreOrderPartialIterRef, but owns the ranges so it can be converted into a stream conveniently.
pub struct PreOrderPartialTreeIter<R: AsRef<RangeSetRef<ChunkNum>> + 'static>(
    PreOrderPartialTreeIterInner<R>,
);

impl<R: AsRef<RangeSetRef<ChunkNum>> + 'static> PreOrderPartialTreeIter<R> {
    /// Create a new PreOrderPartialIter.
    ///
    /// ranges has to implement AsRef<RangeSetRef<ChunkNum>>, so you can pass e.g. a RangeSet2.
    pub fn new(tree: BaoTree, ranges: R) -> Self {
        Self(
            PreOrderPartialTreeIterInnerBuilder {
                ranges,
                iter_builder: |ranges| PreOrderPartialTreeIterRef::new(tree, ranges.as_ref()),
            }
            .build(),
        )
    }
}

impl<A: AsRef<RangeSetRef<ChunkNum>> + 'static> Iterator for PreOrderPartialTreeIter<A> {
    type Item = NodeInfo2;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.with_iter_mut(|x| x.next())
    }
}

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
