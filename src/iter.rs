use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Range,
    result,
};

use blake3::guts::parent_cv;
use ouroboros::self_referencing;
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;

use crate::{
    BaoTree, BlockSize, ByteNum, ChunkNum, {hash_block, range_ok, TreeNode},
};

use super::{outboard::Outboard, read_len_io, read_parent_io, read_range_io};

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
    /// ranges has to implement [AsRef<RangeSetRef<ChunkNum>>], so you can pass e.g. a RangeSet2.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A read item describes what comes next
pub enum ReadItem {
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

impl ReadItem {
    pub fn size(&self) -> usize {
        match self {
            Self::Parent { .. } => 64,
            Self::Leaf { size, .. } => *size,
        }
    }
}

impl Default for ReadItem {
    fn default() -> Self {
        Self::Leaf {
            is_root: true,
            size: 0,
            start_chunk: ChunkNum(0),
        }
    }
}

/// An iterator that produces read items that are convenient to use when reading an encoded query response.
pub struct ReadItemIterRef<'a> {
    inner: PreOrderPartialIterRef<'a>,
    // stack with 3 elements, since we can only have 3 items in flight
    elems: [ReadItem; 3],
    index: usize,
}

impl<'a> ReadItemIterRef<'a> {
    pub fn new(tree: BaoTree, query: &'a RangeSetRef<ChunkNum>, min_level: u8) -> Self {
        Self {
            inner: PreOrderPartialIterRef::new(tree, query, min_level),
            elems: Default::default(),
            index: 0,
        }
    }

    pub fn tree(&self) -> &BaoTree {
        self.inner.tree()
    }

    fn push(&mut self, item: ReadItem) {
        self.elems[self.index] = item;
        self.index += 1;
    }

    fn pop(&mut self) -> Option<ReadItem> {
        if self.index > 0 {
            self.index -= 1;
            Some(self.elems[self.index])
        } else {
            None
        }
    }
}

impl<'a> Iterator for ReadItemIterRef<'a> {
    type Item = ReadItem;

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
                if !r_ranges.is_empty() && m < e {
                    self.push(ReadItem::Leaf {
                        is_root: false,
                        start_chunk: r_start_chunk,
                        size: (e - m).to_usize(),
                    });
                };
                if !l_ranges.is_empty() {
                    self.push(ReadItem::Leaf {
                        is_root: is_root && is_half_leaf,
                        start_chunk: l_start_chunk,
                        size: (m - s).to_usize(),
                    });
                };
            }
            // the last leaf is a special case, since it does not have a parent if it is <= half full
            if !is_half_leaf {
                self.push(ReadItem::Parent {
                    is_root,
                    left: !l_ranges.is_empty(),
                    right: !r_ranges.is_empty(),
                    node,
                });
            }
        }
    }
}

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}

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
        l_ranges: lr,
        r_ranges: rr,
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

pub fn encode_validated<D: Read + Seek, O: Outboard, W: Write>(
    data: D,
    outboard: O,
    encoded: W,
) -> io::Result<()> {
    let range = RangeSet2::from(ChunkNum(0)..);
    encode_ranges_validated(data, outboard, &range, encoded)
}

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
        l_ranges,
        r_ranges,
        is_root,
        ..
    } in tree.iterate_part_preorder_ref(ranges, 0)
    {
        let tl = !l_ranges.is_empty();
        let tr = !r_ranges.is_empty();
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
        block_size: BlockSize,
    },
    /// currently reading the tree, all the info we need is in the iter
    Content { iter: ReadItemIterRef<'a> },
}

pub struct DecodeSliceIter<'a, R> {
    inner: Position<'a>,
    stack: SmallVec<[blake3::Hash; 10]>,
    encoded: R,
    scratch: &'a mut [u8],
}

/// Error when decoding from a reader
/// 
/// This can either be a io error or a more specific error like a hash mismatch
#[derive(Debug)]
pub enum DecodeError {
    /// There was an error reading from the underlying io
    Io(io::Error),
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// The query range was invalid
    InvalidQueryRange,
}

impl From<DecodeError> for io::Error {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::Io(e) => e,
            DecodeError::ParentHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "hash mismatch")
            }
            DecodeError::LeafHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "leaf hash mismatch")
            }
            DecodeError::InvalidQueryRange => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid query range")
            }
        }
    }
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl<'a, R: Read> DecodeSliceIter<'a, R> {
    pub fn new(
        root: blake3::Hash,
        block_size: BlockSize,
        encoded: R,
        ranges: &'a RangeSetRef<ChunkNum>,
        scratch: &'a mut [u8],
    ) -> Self {
        // make sure the buffer is big enough
        assert!(scratch.len() >= block_size.size());
        let mut stack = SmallVec::new();
        stack.push(root);
        Self {
            stack,
            inner: Position::Header { ranges, block_size },
            encoded,
            scratch,
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.scratch
    }

    pub fn tree(&self) -> Option<&BaoTree> {
        match &self.inner {
            Position::Content { iter } => Some(iter.tree()),
            Position::Header { .. } => None,
        }
    }

    fn next0(&mut self) -> result::Result<Option<Range<ByteNum>>, DecodeError> {
        loop {
            let inner = match &mut self.inner {
                Position::Content { ref mut iter } => iter,
                Position::Header {
                    block_size,
                    ranges: range,
                } => {
                    let size = read_len_io(&mut self.encoded)?;
                    // make sure the range is valid and canonical
                    if !range_ok(range, size.chunks()) {
                        break Err(DecodeError::InvalidQueryRange);
                    }
                    let tree = BaoTree::new(size, *block_size);
                    self.inner = Position::Content {
                        iter: tree.read_item_iter_ref(range, 0),
                    };
                    continue;
                }
            };
            match inner.next() {
                Some(ReadItem::Parent {
                    is_root,
                    left,
                    right,
                    node,
                }) => {
                    let (l_hash, r_hash) = read_parent_io(&mut self.encoded)?;
                    let parent_hash = self.stack.pop().unwrap();
                    let actual = parent_cv(&l_hash, &r_hash, is_root);
                    if parent_hash != actual {
                        break Err(DecodeError::ParentHashMismatch(node));
                    }
                    if right {
                        self.stack.push(r_hash);
                    }
                    if left {
                        self.stack.push(l_hash);
                    }
                }
                Some(ReadItem::Leaf {
                    size,
                    is_root,
                    start_chunk,
                }) => {
                    let buf = &mut self.scratch[..size];
                    self.encoded.read_exact(buf)?;
                    let actual = hash_block(start_chunk, buf, is_root);
                    let leaf_hash = self.stack.pop().unwrap();
                    if leaf_hash != actual {
                        break Err(DecodeError::LeafHashMismatch(start_chunk));
                    }
                    let start = start_chunk.to_bytes();
                    let end = start + (size as u64);
                    break Ok(Some(start..end));
                }
                None => break Ok(None),
            }
        }
    }
}

impl<'a, R: Read> Iterator for DecodeSliceIter<'a, R> {
    type Item = result::Result<Range<ByteNum>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next0().transpose()
    }
}
