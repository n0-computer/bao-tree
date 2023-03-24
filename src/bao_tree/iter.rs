use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    result,
};

use blake3::guts::parent_cv;
use ouroboros::self_referencing;
use range_collections::RangeSetRef;
use smallvec::SmallVec;

use crate::{
    bao_tree::{canonicalize_range, hash_block, range_ok, read_bytes_io, TreeNode},
    BaoTree, ByteNum, ChunkNum,
};

use super::{
    outboard::{self, Outboard},
    parse_hash_pair, read_len_io, read_parent_io, read_range_io,
};

/// Extended node info.
///
/// Some of the information is redundant, but it is convenient to have it all in one place.
#[derive(Debug, PartialEq, Eq)]
pub struct NodeInfo<'a> {
    /// the node
    pub node: TreeNode,
    /// left child intersection with the query range
    pub l_range: &'a RangeSetRef<ChunkNum>,
    /// right child intersection with the query range
    pub r_range: &'a RangeSetRef<ChunkNum>,
    /// the node is fully included in the query range
    pub full: bool,
    /// the node is a leaf for the purpose of this query
    pub query_leaf: bool,
    /// the node is the root node (needs special handling when computing hash)
    pub is_root: bool,
}

/// Iterator over all nodes in a BaoTree in pre-order that overlap with a given chunk range.
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

    pub fn tree(&self) -> BaoTree {
        self.tree
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
            let mid = node.mid().to_chunks(tree.chunk_group_log);
            // the start chunk of the node
            let start = node.block_range().start.to_chunks(tree.chunk_group_log);
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
            // emit the node in any case
            break Some(NodeInfo {
                node,
                l_range: l_ranges,
                r_range: r_ranges,
                full,
                query_leaf,
                is_root,
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
                iter_builder: |ranges| PreOrderPartialIterRef::new(tree, ranges.as_ref(), 0),
            }
            .build(),
        )
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

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}

/// Encode the relevant part for a range set given an outboard in post-order and the corresponding data.
/// chunk_group_log is for the outboard.
pub fn encode_ranges<D: Read + Seek, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> io::Result<()> {
    let mut data = data;
    let mut encoded = encoded;
    // validate roughly that the outboard is correct
    let tree = outboard.tree();
    let file_len = ByteNum(data.seek(SeekFrom::End(0))?);
    let ob_len = tree.size;
    if file_len != ob_len {
        io_error!(
            "length from outboard does not match actual file length: {:?} != {:?}",
            ob_len,
            file_len
        );
    }
    if !range_ok(ranges, tree.chunks()) {
        io_error!("ranges are not valid for this tree");
    }
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let buf = &mut buffer;
    // write header
    encoded.write_all(ob_len.0.to_le_bytes().as_slice())?;
    // traverse tree and write encoded from outboard and data
    for NodeInfo {
        node,
        l_range: lr,
        r_range: rr,
        ..
    } in tree.iterate_part_preorder_ref(ranges, 0)
    {
        let tl = !lr.is_empty();
        let tr = !rr.is_empty();
        // each node corresponds to 64 bytes we have to write
        if let Some(pair) = outboard.load_raw(node)? {
            encoded.write_all(pair.as_slice())?;
        }
        // each leaf corresponds to 2 ranges we have to write
        if let Some(leaf) = node.as_leaf() {
            let (l, m, r) = tree.leaf_byte_ranges3(leaf);
            if tl {
                let ld = read_range_io(&mut data, l..m, buf)?;
                encoded.write_all(ld)?;
            }
            if tr {
                let rd = read_range_io(&mut data, m..r, buf)?;
                encoded.write_all(rd)?;
            }
        }
    }
    Ok(())
}

// pub struct Outboard<'a> {
//     len: ByteNum,
//     data: &'a [u8],
//     chunk_group_log: u8,
// }

// impl<'a> Outboard<'a> {
//     pub fn new(data: &'a [u8], chunk_group_log: u8) -> anyhow::Result<Self> {
//         Self {
//             data,
//             chunk_group_log,
//             len,
//         }
//     }
// }

/// Encode the relevant part for a range set given an outboard in post-order and the corresponding data.
/// chunk_group_log is for the outboard.
pub fn encode_ranges_validated<D: Read + Seek, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    ranges: &RangeSetRef<ChunkNum>,
    encoded: W,
) -> io::Result<()> {
    let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let mut data = data;
    let mut encoded = encoded;
    let file_len = ByteNum(data.seek(SeekFrom::End(0))?);
    let tree = outboard.tree();
    let ob_len = tree.size;
    if file_len != ob_len {
        io_error!(
            "length from outboard does not match actual file length: {ob_len:?} != {file_len:?}",
        );
    }
    if !range_ok(ranges, tree.chunks()) {
        io_error!("ranges are not valid for this tree");
    }
    let mut buffer = vec![0u8; tree.chunk_group_bytes().to_usize()];
    let buf = &mut buffer;
    // write header
    encoded.write_all(ob_len.0.to_le_bytes().as_slice())?;
    // traverse tree and write encoded from outboard and data
    for NodeInfo {
        node,
        l_range,
        r_range,
        is_root,
        ..
    } in tree.iterate_part_preorder_ref(ranges, 0)
    {
        let tl = !l_range.is_empty();
        let tr = !r_range.is_empty();
        // each node corresponds to 64 bytes we have to write
        if let Some((l_hash, r_hash)) = outboard.load(node)? {
            let actual = parent_cv(&l_hash, &r_hash, is_root);
            let expected = stack.pop().unwrap();
            if actual != expected {
                io_error!("hash mismatch");
            }
            if tr {
                stack.push(r_hash);
            }
            if tl {
                stack.push(l_hash);
            }
            encoded.write_all(l_hash.as_bytes())?;
            encoded.write_all(r_hash.as_bytes())?;
        }
        // each leaf corresponds to 2 ranges we have to write
        if let Some(leaf) = node.as_leaf() {
            // let (l, m, r) = tree.leaf_byte_ranges3(leaf);
            let chunk0 = tree.chunk_num(leaf);
            let chunkm = chunk0 + tree.chunk_group_chunks();
            match tree.leaf_byte_ranges(leaf) {
                Ok((l, r)) => {
                    if tl {
                        let l_data = read_range_io(&mut data, l, buf)?;
                        let l_hash = hash_block(chunk0, l_data, false);
                        if l_hash != stack.pop().unwrap() {
                            io_error!("hash mismatch");
                        }
                        encoded.write_all(l_data)?;
                    }
                    if tr {
                        let r_data = read_range_io(&mut data, r, buf)?;
                        let r_hash = hash_block(chunkm, r_data, false);
                        if r_hash != stack.pop().unwrap() {
                            io_error!("hash mismatch");
                        }
                        encoded.write_all(r_data)?;
                    }
                }
                Err(l) => {
                    if tl {
                        let l_data = read_range_io(&mut data, l, buf)?;
                        let l_hash = hash_block(chunk0, l_data, is_root);
                        if l_hash != stack.pop().unwrap() {
                            io_error!("hash mismatch");
                        }
                        encoded.write_all(l_data)?;
                    }
                }
            }
        }
    }
    Ok(())
}
enum Position<'a> {
    /// currently reading the header, so don't know how big the tree is
    /// so we need to store the ranges and the chunk group log
    Header {
        ranges: &'a RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    },
    /// currently reading the tree, all the info we need is in the iter
    Content { iter: PreOrderPartialIterRef<'a> },
}

pub struct DecodeSliceIter<'a, R> {
    inner: Position<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    scratch: &'a mut [u8],
}

pub enum DecodeSliceError {
    Io(io::Error),
    HashMismatch(TreeNode),
    InvalidQueryRange,
}

impl From<DecodeSliceError> for io::Error {
    fn from(e: DecodeSliceError) -> Self {
        match e {
            DecodeSliceError::Io(e) => e,
            DecodeSliceError::HashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "hash mismatch")
            }
            DecodeSliceError::InvalidQueryRange => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid query range")
            }
        }
    }
}

impl From<io::Error> for DecodeSliceError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl<'a, R: Read> DecodeSliceIter<'a, R> {
    pub fn new(
        root: blake3::Hash,
        chunk_group_log: u8,
        encoded: R,
        ranges: &'a RangeSetRef<ChunkNum>,
        scratch: &'a mut [u8],
    ) -> Self {
        // make sure the buffer is big enough
        assert!(scratch.len() >= 2048 << chunk_group_log);
        let mut stack = SmallVec::new();
        stack.push(root);
        Self {
            stack,
            inner: Position::Header {
                ranges,
                chunk_group_log,
            },
            encoded,
            scratch,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.scratch
    }

    pub fn tree(&self) -> Option<BaoTree> {
        match &self.inner {
            Position::Content { iter } => Some(iter.tree().clone()),
            Position::Header { .. } => None,
        }
    }

    fn next0(&mut self) -> result::Result<Option<Range<ByteNum>>, DecodeSliceError> {
        loop {
            let inner = match &mut self.inner {
                Position::Content { ref mut iter } => iter,
                Position::Header {
                    chunk_group_log,
                    ranges: range,
                } => {
                    let size = read_len_io(&mut self.encoded)?;
                    // make sure the range is valid and canonical
                    if !range_ok(range, size.chunks()) {
                        break Err(DecodeSliceError::InvalidQueryRange);
                    }
                    let tree = BaoTree::new(size, *chunk_group_log);
                    self.inner = Position::Content {
                        iter: tree.iterate_part_preorder_ref(range, 0),
                    };
                    continue;
                }
            };
            let info = match inner.next() {
                Some(node) => node,
                None => break Ok(None),
            };
            let NodeInfo {
                node,
                l_range,
                r_range,
                is_root,
                ..
            } = info;
            let tree = &inner.tree();
            let tl = !l_range.is_empty();
            let tr = !r_range.is_empty();
            let is_half_leaf = !tree.is_persisted(node);
            // do not expect a parent pair for a half leaf
            if !is_half_leaf {
                let (l_hash, r_hash) = read_parent_io(&mut self.encoded)?;
                let parent_hash = self.stack.pop().unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                // Push the children in reverse order so they are popped in the correct order
                // only push right if the range intersects with the right child
                if tr {
                    self.stack.push(r_hash);
                }
                // only push left if the range intersects with the left child
                if tl {
                    self.stack.push(l_hash);
                }
                // Validate after pushing the children so that we could in principle continue
                if parent_hash != actual {
                    break Err(DecodeSliceError::HashMismatch(node));
                }
            }
            if let Some(leaf) = node.as_leaf() {
                let (start, mid, end) = tree.leaf_byte_ranges3(leaf);
                let l_start_chunk = tree.chunk_num(leaf);
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                let mut offset = 0usize;
                let buf = &mut self.scratch;
                if tl {
                    let l_hash = self.stack.pop().unwrap();
                    let l_data = read_bytes_io(&mut self.encoded, start..mid, buf)?;
                    offset += (mid - start).to_usize();
                    // if is_persisted is true, this is just the left child of a leaf with 2 children
                    // and therefore not the root. Only if it is a half full leaf can it be root
                    let l_is_root = is_root && is_half_leaf;
                    let actual = hash_block(l_start_chunk, l_data, l_is_root);
                    if l_hash != actual {
                        break Err(DecodeSliceError::HashMismatch(node));
                    }
                }
                if tr && mid < end {
                    let r_hash = self.stack.pop().unwrap();
                    let r_data = read_bytes_io(&mut self.encoded, mid..end, &mut buf[offset..])?;
                    offset += (end - mid).to_usize();
                    // right side can never be root, sorry
                    let r_is_root = false;
                    let actual = hash_block(r_start_chunk, r_data, r_is_root);
                    if r_hash != actual {
                        break Err(DecodeSliceError::HashMismatch(node));
                    }
                }
                let start = if tl { start } else { mid };
                let end = if tr { end } else { mid };
                assert!(tl || tr);
                assert!(offset == (end - start).to_usize());
                break Ok(Some(start..end));
            }
        }
    }
}

impl<'a, R: Read> Iterator for DecodeSliceIter<'a, R> {
    type Item = result::Result<Range<ByteNum>, DecodeSliceError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}
