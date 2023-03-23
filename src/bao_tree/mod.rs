use crate::tree::{BlockNum, ByteNum, ChunkNum, PONum};
use blake3::guts::parent_cv;
use range_collections::{range_set::RangeSetEntry, RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use std::{
    fmt::{self, Debug},
    io::{self, Cursor, Read, Write},
    ops::{Range, RangeFrom},
    result,
};
mod iter;
use iter::*;

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
    start_chunk: ChunkNum,
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
        Self::new_with_start_chunk(size, chunk_group_log, ChunkNum(0))
    }

    pub fn new_with_start_chunk(
        size: ByteNum,
        chunk_group_log: u8,
        start_chunk: ChunkNum,
    ) -> BaoTree {
        BaoTree {
            size,
            chunk_group_log,
            start_chunk,
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

    pub fn outboard_size(size: ByteNum, chunk_group_log: u8) -> ByteNum {
        let tree = Self::new(size, chunk_group_log);
        ByteNum(tree.outboard_hash_pairs() * 64 + 8)
    }

    pub(crate) fn filled_size(&self) -> TreeNode {
        let blocks = self.blocks();
        let n = (blocks.0 + 1) / 2;
        TreeNode(n + n.saturating_sub(1))
    }

    pub fn chunk_num(&self, node: LeafNode) -> ChunkNum {
        // block number of a leaf node is just the node number
        // multiply by chunk_group_size to get the chunk number
        ChunkNum(node.0 << self.chunk_group_log) + self.start_chunk
    }

    /// Compute the post order outboard for the given data, returning a Vec
    pub fn outboard_post_order_mem(
        data: impl AsRef<[u8]>,
        chunk_group_log: u8,
    ) -> (Vec<u8>, blake3::Hash) {
        let data = data.as_ref();
        let tree =
            Self::new_with_start_chunk(ByteNum(data.len() as u64), chunk_group_log, ChunkNum(0));
        let outboard_len: usize = (tree.outboard_hash_pairs() * 64 + 8).try_into().unwrap();
        let mut res = Vec::with_capacity(outboard_len);
        let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
        let hash = tree
            .outboard_post_order_sync_impl(&mut Cursor::new(data), &mut res, &mut buffer)
            .unwrap();
        res.extend_from_slice(&(data.len() as u64).to_le_bytes());
        (res, hash)
    }

    /// Compute the post order outboard for the given data, writing into a io::Write
    pub fn outboard_post_order_io(
        data: &mut impl Read,
        size: u64,
        chunk_group_log: u8,
        outboard: &mut impl Write,
    ) -> io::Result<blake3::Hash> {
        let tree = Self::new_with_start_chunk(ByteNum(size), chunk_group_log, ChunkNum(0));
        let mut buffer = vec![0; tree.chunk_group_bytes().to_usize()];
        let hash = tree.outboard_post_order_sync_impl(data, outboard, &mut buffer)?;
        outboard.write_all(&size.to_le_bytes())?;
        Ok(hash)
    }

    /// Compute the post order outboard for the given data
    ///
    /// This is the internal version that takes a start chunk and does not append the size!
    fn outboard_post_order_sync_impl(
        &self,
        data: &mut impl Read,
        outboard: &mut impl Write,
        buffer: &mut [u8],
    ) -> io::Result<blake3::Hash> {
        // do not allocate for small trees
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        debug_assert!(buffer.len() == self.chunk_group_bytes().to_usize());
        let root = self.root();
        for node in self.iterate() {
            let is_root = node == root;
            let hash = if let Some(leaf) = node.as_leaf() {
                let chunk0 = self.chunk_num(leaf);
                let cgc = self.chunk_group_chunks();
                match self.leaf_byte_ranges(leaf) {
                    Ok((l, r)) => {
                        let l_data = read_range_io(data, l, buffer)?;
                        let l_hash = hash_block(chunk0, l_data, false);
                        let r_data = read_range_io(data, r, buffer)?;
                        let r_hash = hash_block(chunk0 + cgc, r_data, false);
                        outboard.write_all(l_hash.as_bytes())?;
                        outboard.write_all(r_hash.as_bytes())?;
                        parent_cv(&l_hash, &r_hash, is_root)
                    }
                    Err(l) => {
                        let l_data = read_range_io(data, l, buffer)?;
                        let l_hash = hash_block(chunk0, l_data, is_root);
                        l_hash
                    }
                }
            } else {
                let right_hash = stack.pop().unwrap();
                let left_hash = stack.pop().unwrap();
                outboard.write_all(left_hash.as_bytes())?;
                outboard.write_all(right_hash.as_bytes())?;
                parent_cv(&left_hash, &right_hash, is_root)
            };
            stack.push(hash);
        }
        debug_assert_eq!(stack.len(), 1);
        let hash = stack.pop().unwrap();
        Ok(hash)
    }

    /// Compute the blake3 hash for the given data
    pub fn blake3_hash(data: impl AsRef<[u8]>) -> blake3::Hash {
        let data = data.as_ref();
        let cursor = Cursor::new(data);
        let mut buffer = [0u8; 1024];
        Self::blake3_hash_inner(
            cursor,
            ByteNum(data.len() as u64),
            ChunkNum(0),
            true,
            &mut buffer,
        )
        .unwrap()
    }

    /// Internal hash computation. This allows to also compute a non root hash, e.g. for a block
    pub fn blake3_hash_inner(
        mut data: impl Read,
        data_len: ByteNum,
        start_chunk: ChunkNum,
        is_root: bool,
        buf: &mut [u8],
    ) -> io::Result<blake3::Hash> {
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        let tree = Self::new_with_start_chunk(data_len, 0, start_chunk);
        let root = tree.root();
        let can_be_root = is_root;
        for node in tree.iterate() {
            // if our is_root is not set, this can never be true
            let is_root = can_be_root && node == root;
            let hash = if let Some(leaf) = node.as_leaf() {
                let chunk0 = tree.chunk_num(leaf);
                match tree.leaf_byte_ranges(leaf) {
                    Ok((l, r)) => {
                        let ld = read_range_io(&mut data, l, buf)?;
                        let left_hash = hash_chunk(chunk0, ld, false);
                        let rd = read_range_io(&mut data, r, buf)?;
                        let right_hash = hash_chunk(chunk0 + tree.chunk_group_chunks(), rd, false);
                        parent_cv(&left_hash, &right_hash, is_root)
                    }
                    Err(l) => {
                        let ld = read_range_io(&mut data, l, buf)?;
                        hash_chunk(chunk0, ld, is_root)
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
        Ok(stack.pop().unwrap())
    }

    /// Decode encoded ranges given the root hash
    pub fn decode_ranges<'a>(
        root: blake3::Hash,
        encoded: &'a [u8],
        ranges: &RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    ) -> impl Iterator<Item = io::Result<(ByteNum, &'a [u8])>> + 'a {
        if encoded.len() < 8 {
            return vec![Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "encoded data too short",
            ))]
            .into_iter();
        }
        let size = ByteNum(u64::from_le_bytes(encoded[..8].try_into().unwrap()));
        let res = match canonicalize_range(ranges, size.chunks()) {
            Ok(ranges) => {
                Self::decode_ranges_impl(root, &encoded[8..], size, ranges, chunk_group_log)
            }
            Err(range) => {
                let ranges = RangeSet2::from(range);
                // If the range doesn't intersect with the data, ask for the last chunk
                // this is so it matches the behavior of bao
                Self::decode_ranges_impl(root, &encoded[8..], size, &ranges, chunk_group_log)
            }
        };
        res.into_iter()
    }

    fn decode_ranges_impl<'a>(
        root: blake3::Hash,
        encoded: &'a [u8],
        size: ByteNum,
        ranges: &RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    ) -> Vec<io::Result<(ByteNum, &'a [u8])>> {
        let mut res = Vec::new();
        let mut remaining = encoded;
        let mut stack = SmallVec::<[blake3::Hash; 10]>::new();
        stack.push(root);
        let tree = Self::new(size, chunk_group_log);
        let mut is_root = true;
        for NodeInfo { node, tl, tr } in tree.iterate_part_preorder_ref(ranges) {
            if tree.is_persisted(node) {
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
                // Push the children in reverse order so they are popped in the correct order
                // only push right if the range intersects with the right child
                if tr {
                    stack.push(r_hash);
                }
                // only push left if the range intersects with the left child
                if tl {
                    stack.push(l_hash);
                }
            }
            if let Some(leaf) = node.as_leaf() {
                let (l_range, r_range) = tree.leaf_byte_ranges2(leaf);
                let l_start_chunk = tree.chunk_num(leaf);
                let r_start_chunk = l_start_chunk + tree.chunk_group_chunks();
                if tl {
                    let l_hash = stack.pop().unwrap();
                    let l_size = (l_range.end - l_range.start).to_usize();
                    let (l_data, rest) = remaining.split_at(l_size);
                    remaining = rest;
                    let actual = hash_block(l_start_chunk, l_data, is_root);
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
                if tr && r_range.start < size {
                    let r_hash = stack.pop().unwrap();
                    let r_size = (r_range.end - r_range.start).to_usize();
                    let (r_data, rest) = remaining.split_at(r_size);
                    remaining = rest;
                    let actual = hash_block(r_start_chunk, r_data, is_root);
                    is_root = false;
                    if r_hash != actual {
                        res.push(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Hash mismatch",
                        )));
                        break;
                    }
                    res.push(Ok((r_range.start, r_data)));
                }
            }
        }
        res
    }

    /// Given a *post order* outboard, encode a slice of data
    ///
    /// Todo: validate on read option
    pub fn encode_ranges(
        data: &[u8],
        outboard: &[u8],
        ranges: &RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    ) -> Vec<u8> {
        let size = ByteNum(data.len() as u64);
        match canonicalize_range(ranges, size.chunks()) {
            Ok(ranges) => Self::encode_ranges_impl(data, outboard, &ranges, chunk_group_log),
            Err(range) => {
                let ranges = RangeSet2::from(range);
                Self::encode_ranges_impl(data, outboard, &ranges, chunk_group_log)
            }
        }
    }

    fn encode_ranges_impl(
        data: &[u8],
        outboard: &[u8],
        ranges: &RangeSetRef<ChunkNum>,
        chunk_group_log: u8,
    ) -> Vec<u8> {
        let mut res = Vec::new();
        let tree = Self::new(ByteNum(data.len() as u64), chunk_group_log);
        res.extend_from_slice(&tree.size.0.to_le_bytes());
        for NodeInfo { node, tl, tr } in tree.iterate_part_preorder_ref(ranges) {
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

    pub fn iterate(&self) -> PostOrderTreeIter {
        PostOrderTreeIter::new(*self)
    }

    pub fn iterate_part_preorder_ref<'a>(
        &self,
        ranges: &'a RangeSetRef<ChunkNum>,
    ) -> impl Iterator<Item = NodeInfo> + 'a {
        PreOrderPartialIterRef::new(*self, ranges)
    }

    pub fn iterate_part_preorder<R: AsRef<RangeSetRef<ChunkNum>> + 'static>(
        &self,
        ranges: R,
    ) -> impl Iterator<Item = NodeInfo> {
        PreOrderPartialIter::new(*self, ranges)
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
    fn iterate_part_preorder_reference(&self, ranges: &RangeSetRef<ChunkNum>) -> Vec<NodeInfo> {
        fn iterate_part_rec(
            tree: &BaoTree,
            node: TreeNode,
            ranges: &RangeSetRef<ChunkNum>,
            res: &mut Vec<NodeInfo>,
        ) {
            if ranges.is_empty() {
                return;
            }
            // the middle chunk of the node
            let mid = node.mid().to_chunks(tree.chunk_group_log);
            // split the ranges into left and right
            let (l_ranges, r_ranges) = ranges.split(mid);
            // push no matter if leaf or not
            res.push(NodeInfo {
                node,
                tl: !l_ranges.is_empty(),
                tr: !r_ranges.is_empty(),
            });
            // if not leaf, recurse
            if !node.is_leaf() {
                let valid_nodes = tree.filled_size();
                let l = node.left_child().unwrap();
                let r = node.right_descendant(valid_nodes).unwrap();
                iterate_part_rec(tree, l, l_ranges, res);
                iterate_part_rec(tree, r, r_ranges, res);
            }
        }
        let mut res = Vec::new();
        iterate_part_rec(self, self.root(), ranges, &mut res);
        res
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

    fn pre_order_offset(&self, node: TreeNode) -> u64 {
        pre_order_offset_slow(node.0, self.filled_size().0)
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

fn read_range_io<'a>(
    from: &mut impl Read,
    range: Range<ByteNum>,
    buf: &'a mut [u8],
) -> io::Result<&'a [u8]> {
    let len = (range.end - range.start).to_usize();
    let mut buf = &mut buf[..len];
    from.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_range_mem(from: &[u8], range: Range<ByteNum>) -> &[u8] {
    let start = range.start.to_usize();
    let end = range.end.to_usize();
    &from[start..end]
}

// async fn read_range_tokio<'a>(
//     from: &mut impl AsyncRead,
//     range: Range<ByteNum>,
//     buf: &'a mut [u8],
// ) -> io::Result<&'a [u8]> {
//     let len = (range.end - range.start).to_usize();
//     let mut buf = &mut buf[..len];
//     from.read_exact(&mut buf)?;
//     Ok(buf)
// }

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
    BaoTree::blake3_hash_inner(data, data_len, start_chunk, is_root, &mut buffer).unwrap()
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

#[cfg(test)]
mod tests {

    use std::{
        collections::HashMap,
        io::{Cursor, Read, Write},
        ops::Range,
    };

    use proptest::prelude::*;
    use range_collections::RangeSet2;

    use super::BaoTree;
    use crate::{
        bao_tree::{
            pre_order_offset_slow, NodeInfo, PostOrderOffset, PostOrderTreeIter,
            PostOrderTreeIterStack, TreeNode,
        },
        tree::{ByteNum, ChunkNum, PONum, BLAKE3_CHUNK_SIZE},
    };

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

    fn bao_tree_encode_slice_comparison_impl(data: Vec<u8>, mut range: Range<ChunkNum>) {
        let expected = encode_slice_reference(&data, range.clone()).0;
        let (outboard, _hash) = BaoTree::outboard_post_order_mem(&data, 0);
        // extend empty range to contain at least 1 byte
        if range.start == range.end {
            range.end.0 += 1;
        };
        let actual = BaoTree::encode_ranges(&data, &outboard, &RangeSet2::from(range), 0);
        assert_eq!(expected.len(), actual.len());
        assert_eq!(expected, actual);
    }

    /// range is a range of chunks. Just using u64 for convenience in tests
    fn bao_tree_decode_slice_impl(data: Vec<u8>, range: Range<u64>) {
        let range = ChunkNum(range.start)..ChunkNum(range.end);
        let (encoded, root) = encode_slice_reference(&data, range.clone());
        let expected = data;
        for item in BaoTree::decode_ranges(root, &encoded, &RangeSet2::from(range), 0) {
            let (pos, slice) = item.unwrap();
            let pos = pos.to_usize();
            assert_eq!(expected[pos..pos + slice.len()], *slice);
        }
    }

    fn bao_tree_outboard_comparison_impl(data: Vec<u8>) {
        let (expected, expected_hash) = post_order_outboard_reference(&data);
        let (actual, actual_hash) = BaoTree::outboard_post_order_mem(&data, 0);
        assert_eq!(expected_hash, actual_hash);
        assert_eq!(expected, actual);
    }

    #[test]
    fn bao_tree_outboard_comparison_cases() {
        use make_test_data as td;
        bao_tree_outboard_comparison_impl(td(0));
        bao_tree_outboard_comparison_impl(td(1));
        bao_tree_outboard_comparison_impl(td(1023));
        bao_tree_outboard_comparison_impl(td(1024));
        bao_tree_outboard_comparison_impl(td(1025));
        bao_tree_outboard_comparison_impl(td(2047));
        bao_tree_outboard_comparison_impl(td(2048));
        bao_tree_outboard_comparison_impl(td(2049));
        bao_tree_outboard_comparison_impl(td(10000));
        bao_tree_outboard_comparison_impl(td(20000));
        bao_tree_outboard_comparison_impl(td(24577));
    }

    #[test]
    fn bao_tree_outboard_levels() {
        use make_test_data as td;
        let td = td(1024 * 32);
        let expected = BaoTree::blake3_hash(&td);
        for chunk_group_log in 0..4 {
            let (outboard, hash) = BaoTree::outboard_post_order_mem(&td, chunk_group_log);
            assert_eq!(expected, hash);
            assert_eq!(
                ByteNum(outboard.len() as u64),
                BaoTree::outboard_size(ByteNum(td.len() as u64), chunk_group_log)
            );
        }
    }

    /// encodes the data as outboard with the given chunk_group_log, then uses that outboard to
    /// encode a slice of the data, and compares the result to the original data
    fn bao_tree_slice_roundtrip_test(
        data: Vec<u8>,
        mut range: Range<ChunkNum>,
        chunk_group_log: u8,
    ) {
        let (outboard, root) = BaoTree::outboard_post_order_mem(&data, chunk_group_log);
        // extend empty range to contain at least 1 byte
        if range.start == range.end {
            range.end.0 += 1;
        };
        let encoded = BaoTree::encode_ranges(
            &data,
            &outboard,
            &RangeSet2::from(range.clone()),
            chunk_group_log,
        );
        let expected = data;
        let mut all_ranges = RangeSet2::empty();
        for item in BaoTree::decode_ranges(root, &encoded, &RangeSet2::from(range), chunk_group_log)
        {
            let (pos, slice) = item.unwrap();
            // compute all data ranges
            all_ranges |= RangeSet2::from(pos..pos + (slice.len() as u64));
            let pos = pos.to_usize();
            assert_eq!(expected[pos..pos + slice.len()], *slice);
        }
    }

    #[test]
    fn bao_tree_slice_roundtrip_cases() {
        use make_test_data as td;
        let cases = [
            (0, 0..1),
            (1, 0..1),
            (1023, 0..1),
            (1024, 0..1),
            (1025, 0..1),
            (2047, 0..1),
            (2048, 0..1),
            (10000, 0..1),
            (20000, 0..1),
            (24 * 1024 + 1, 0..25),
            (1025, 1..2),
            (2047, 1..2),
            (2048, 1..2),
            (10000, 1..2),
            (20000, 1..2),
        ];
        for chunk_group_log in 1..4 {
            for (count, range) in cases.clone() {
                bao_tree_slice_roundtrip_test(
                    td(count),
                    ChunkNum(range.start)..ChunkNum(range.end),
                    chunk_group_log,
                );
            }
        }
    }

    #[test]
    fn bao_tree_encode_slice_0() {
        use make_test_data as td;
        let cases = [
            (0, 0..1),
            (1, 0..1),
            (1023, 0..1),
            (1024, 0..1),
            (1025, 0..1),
            (2047, 0..1),
            (2048, 0..1),
            (10000, 0..1),
            (20000, 0..1),
            (24 * 1024 + 1, 0..25),
            (1025, 1..2),
            (2047, 1..2),
            (2048, 1..2),
            (10000, 1..2),
            (20000, 1..2),
        ];
        for (count, range) in cases {
            bao_tree_encode_slice_comparison_impl(
                td(count),
                ChunkNum(range.start)..ChunkNum(range.end),
            );
        }
    }

    #[test]
    fn bao_tree_decode_slice_0() {
        use make_test_data as td;
        bao_tree_decode_slice_impl(td(0), 0..1);
        bao_tree_decode_slice_impl(td(1), 0..1);
        bao_tree_decode_slice_impl(td(1023), 0..1);
        bao_tree_decode_slice_impl(td(1024), 0..1);
        bao_tree_decode_slice_impl(td(1025), 0..2);
        bao_tree_decode_slice_impl(td(2047), 0..2);
        bao_tree_decode_slice_impl(td(2048), 0..2);
        bao_tree_decode_slice_impl(td(24 * 1024 + 1), 0..25);
        bao_tree_decode_slice_impl(td(1025), 0..1);
        bao_tree_decode_slice_impl(td(1025), 1..2);
        bao_tree_decode_slice_impl(td(1024 * 17), 0..18);
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

    // create the mapping from a node number to the offset in the pre order traversal,
    // using brute force lookup in the bao output
    fn create_permutation_reference(size: usize) -> Vec<(TreeNode, usize)> {
        use make_test_data as td;
        let data = td(size);
        let (post, _) = BaoTree::outboard_post_order_mem(&data, 0);
        let (mut pre, _) = bao::encode::outboard(data);
        pre.splice(..8, []);
        let map = pre
            .chunks_exact(64)
            .enumerate()
            .map(|(i, h)| (h, i))
            .collect::<HashMap<_, _>>();
        let tree = BaoTree::new(ByteNum(size as u64), 0);
        let mut res = Vec::new();
        for c in 0..tree.filled_size().0 {
            let node = TreeNode(c);
            if let Some(offset) = tree.post_order_offset(node).value() {
                let offset = offset.to_usize();
                let hash = post[offset * 64..offset * 64 + 64].to_vec();
                let index = *map.get(hash.as_slice()).unwrap();
                res.push((node, index));
            }
        }
        res
    }

    fn right_parent_count(node: TreeNode, len: TreeNode) -> u64 {
        assert!(node < len);
        let mut node = node;
        let mut count = 0;
        while let Some(parent) = node.parent() {
            if parent < node || parent >= len {
                break;
            }
            count += 1;
            node = parent;
        }
        count
    }

    fn parent_count(node: TreeNode, tree: BaoTree, x: u64) -> u64 {
        let len = tree.filled_size();
        let root = tree.root();
        assert!(node < len);
        let mut node = node;
        let mut count = 0;
        while let Some(parent) = node.parent() {
            if parent < len && parent.0 >= x {
                count += 1;
            }
            node = parent;
            if parent == root {
                break;
            }
        }
        count
    }

    fn parent_count2(node: TreeNode, tree: BaoTree) -> u64 {
        let len = tree.filled_size();
        let root = tree.root();
        assert!(node < len);
        assert!(root < len);
        let mut node = node;
        let mut count = 0;
        while let Some(parent) = node.parent() {
            if parent < len {
                count += 1;
            }
            node = parent;
            if parent == root {
                break;
            }
        }
        count
    }

    /// Counts the number of parents that `node` would have if it were in a tree of size `len`
    fn parents_loop(node: u64, len: u64) -> u64 {
        assert!(node < len);
        let mut count = 0;
        let level = (!node).trailing_zeros();
        let mut span = 1u64 << level;
        let mut offset = node;
        loop {
            let pspan = span * 2;
            offset = if (offset & pspan) == 0 {
                offset + span
            } else {
                offset - span
            };
            if offset < len {
                count += 1;
            }
            if pspan >= len {
                break;
            }
            span = pspan;
        }
        count
    }

    fn compare_pre_order_outboard(case: usize) {
        let size = ByteNum(case as u64);
        let tree = BaoTree::new(size, 0);
        let perm = create_permutation_reference(case);
        for (k, v) in perm {
            let preo = k.block_range().start.0;
            let rpc = parent_count(k, tree, preo);
            let res = preo + rpc;
            let pc = parents_loop(k.0, tree.filled_size().0);
            let corr = preo.count_ones() as u64;
            let res2 = preo + pc - corr;
            // println!("{}\t{:b}\t:\t{}\t{}\t{}\t{}", k.0, k.0, v, preo, rpc, ld);
            // println!("{}\t{:b}\t:\t{}\t{}\t{}\t{}", k.0, k.0, v, preo, pc, res2);
            // println!("{}\t:\t{}\t{}", k.0, v, res);
            assert_eq!(v as u64, res);
            assert_eq!(v as u64, res2);
            assert_eq!(v as u64, pre_order_offset_slow(k.0, tree.filled_size().0));
        }
    }

    #[test]
    fn pre_post_outboard_cases() {
        let cases = [20000];
        for case in cases {
            compare_pre_order_outboard(case);
        }
    }

    proptest! {

        #[test]
        fn bao_tree_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_blake3_impl(data);
        }

        #[test]
        fn bao_tree_encode_slice_all(len in 0..32768usize) {
            let data = make_test_data(len);
            let chunk_range = ChunkNum(0)..ChunkNum((data.len() / 1024 + 1) as u64);
            bao_tree_encode_slice_comparison_impl(data, chunk_range);
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
            let chunk_range = start .. start + size;
            bao_tree_encode_slice_comparison_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_encode_slice_part_any((len, start, size) in size_and_slice()) {
            let data = make_test_data(len.to_usize());
            let chunk_range = start .. start + size;
            bao_tree_encode_slice_comparison_impl(data, chunk_range);
        }

        #[test]
        fn bao_tree_outboard_comparison(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            bao_tree_outboard_comparison_impl(data);
        }

        #[test]
        fn bao_tree_slice_roundtrip((len, start, size) in size_and_slice_overlapping(), level in 0u8..6) {
            let data = make_test_data(len.to_usize());
            let chunk_range = start .. start + size;
            bao_tree_slice_roundtrip_test(data, chunk_range, level);
        }

        #[test]
        fn tree_iterator_comparison(len in 0u64..100000) {
            let tree = BaoTree::new(ByteNum(len), 0);
            let iter1 = tree.iterate_reference();
            let iter2 = PostOrderTreeIterStack::new(tree).collect::<Vec<_>>();
            let iter3 = PostOrderTreeIter::new(tree).collect::<Vec<_>>();
            prop_assert_eq!(&iter1, &iter2);
            prop_assert_eq!(&iter1, &iter3);
        }

        #[test]
        fn partial_iterator_comparison((len, start, size) in size_and_slice_overlapping()) {
            let tree = BaoTree::new(len, 0);
            let chunk_range = start .. start + size;
            let iter1 = tree.iterate_part_preorder_reference(&RangeSet2::from(chunk_range.clone()));
            let iter2 = tree.iterate_part_preorder_ref(&RangeSet2::from(chunk_range.clone())).collect::<Vec<_>>();
            let iter3 = tree.iterate_part_preorder(RangeSet2::from(chunk_range)).collect::<Vec<_>>();
            prop_assert_eq!(&iter1, &iter2);
            prop_assert_eq!(&iter1, &iter3);
        }

        #[test]
        fn pre_post_outboard(n in 0usize..1000000) {
            compare_pre_order_outboard(n);
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
        for NodeInfo { node, .. } in tree.iterate_part_preorder_ref(&spec) {
            println!(
                "{:#?}\t{}\t{:?}",
                node,
                tree.is_sealed(node),
                tree.post_order_offset(node)
            );
        }
    }
}
