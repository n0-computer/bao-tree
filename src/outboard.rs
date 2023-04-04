//! The [Outboard] trait and implementations
use blake3::guts::parent_cv;
use range_collections::RangeSet2;

use super::{outboard_size, TreeNode};
use crate::{BaoTree, BlockSize, ByteNum, ChunkNum};
use std::io::{self, Read};

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}

/// An outboard is a just a thing that knows how big it is and can get you the hashes for a node.
pub trait Outboard {
    /// The root hash
    fn root(&self) -> blake3::Hash;
    /// The tree. This contains the information about the size of the file and the block size.
    fn tree(&self) -> BaoTree;
    /// load the hash pair for a node
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>>;
}

pub trait OutboardMut: Outboard {
    /// Set the length of the file for which this outboard is
    fn set_size(&mut self, len: ByteNum) -> io::Result<()>;
    /// Save a hash pair for a node
    fn save(&mut self, node: TreeNode, hash_pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()>;
}

impl<O: Outboard> Outboard for &O {
    fn root(&self) -> blake3::Hash {
        (**self).root()
    }
    fn tree(&self) -> BaoTree {
        (**self).tree()
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        (**self).load(node)
    }
}

impl<O: Outboard> Outboard for &mut O {
    fn root(&self) -> blake3::Hash {
        (**self).root()
    }
    fn tree(&self) -> BaoTree {
        (**self).tree()
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        (**self).load(node)
    }
}

impl<O: OutboardMut> OutboardMut for &mut O {
    fn save(&mut self, node: TreeNode, hash_pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        (**self).save(node, hash_pair)
    }
    fn set_size(&mut self, len: ByteNum) -> io::Result<()> {
        (**self).set_size(len)
    }
}

/// Given an outboard, return a range set of all valid ranges
pub fn valid_ranges<O>(outboard: &O) -> io::Result<RangeSet2<ChunkNum>>
where
    O: Outboard,
{
    struct RecursiveValidator<'a, O: Outboard> {
        tree: BaoTree,
        valid_nodes: TreeNode,
        res: RangeSet2<ChunkNum>,
        outboard: &'a O,
    }

    impl<'a, O: Outboard> RecursiveValidator<'a, O> {
        fn validate_rec(
            &mut self,
            parent_hash: &blake3::Hash,
            node: TreeNode,
            is_root: bool,
        ) -> io::Result<()> {
            let (l_hash, r_hash) = if let Some((l_hash, r_hash)) = self.outboard.load(node)? {
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                if &actual != parent_hash {
                    // we got a validation error. Simply continue without adding the range
                    return Ok(());
                }
                (l_hash, r_hash)
            } else {
                (*parent_hash, blake3::Hash::from([0; 32]))
            };
            if let Some(leaf) = node.as_leaf() {
                let start = self.tree.chunk_num(leaf);
                let end = (start + self.tree.chunk_group_chunks() * 2).min(self.tree.chunks());
                self.res |= RangeSet2::from(start..end);
            } else {
                // recurse
                let left = node.left_child().unwrap();
                self.validate_rec(&l_hash, left, false)?;
                let right = node.right_descendant(self.valid_nodes).unwrap();
                self.validate_rec(&r_hash, right, false)?;
            }
            Ok(())
        }
    }
    let tree = outboard.tree();
    let root_hash = outboard.root();
    let mut validator = RecursiveValidator {
        tree,
        valid_nodes: tree.filled_size(),
        res: RangeSet2::empty(),
        outboard,
    };
    validator.validate_rec(&root_hash, tree.root(), true)?;
    Ok(validator.res)
}

/// An empty outboard, that just returns 0 hashes for all nodes.
///
/// Also allows you to write and will immediately discard the data, a bit like /dev/null
#[derive(Debug)]
pub struct EmptyOutboard {
    tree: BaoTree,
    root: blake3::Hash,
}

impl EmptyOutboard {
    pub fn new(tree: BaoTree, root: blake3::Hash) -> Self {
        Self { tree, root }
    }
}

impl Outboard for EmptyOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(if self.tree.is_persisted(node) {
            // behave as if it was an outboard file filled with 0s
            Some((blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32])))
        } else {
            None
        })
    }
}

impl OutboardMut for EmptyOutboard {
    fn save(&mut self, node: TreeNode, _pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        if self.tree.is_persisted(node) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            ))
        }
    }
    fn set_size(&mut self, size: ByteNum) -> io::Result<()> {
        self.tree = BaoTree::new(size, self.tree.block_size);
        Ok(())
    }
}

/// An outboard that is stored in memory, in a byte slice.
#[derive(Debug, Clone, Copy)]
pub struct PostOrderMemOutboardRef<'a> {
    root: blake3::Hash,
    tree: BaoTree,
    data: &'a [u8],
}

impl<'a> PostOrderMemOutboardRef<'a> {
    pub fn load(root: blake3::Hash, outboard: &'a [u8], block_size: BlockSize) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        if outboard.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let (data, size) = outboard.split_at(outboard.len() - 8);
        let len = u64::from_le_bytes(size.try_into().unwrap());
        let tree = BaoTree::new(ByteNum(len), block_size);
        let expected_outboard_len = tree.outboard_hash_pairs() * 64;
        if data.len() as u64 != expected_outboard_len {
            io_error!(
                "outboard length does not match expected outboard length: {} != {}",
                outboard.len(),
                expected_outboard_len
            );
        }
        Ok(Self { root, tree, data })
    }

    pub fn flip(&self) -> PreOrderMemOutboard {
        let tree = self.tree;
        let mut data = vec![0; self.data.len() + 8];
        data[0..8].copy_from_slice(tree.size.0.to_le_bytes().as_slice());
        for node in self.tree.post_order_nodes_iter() {
            if let Some((l, r)) = self.load(node).unwrap() {
                let offset = tree.pre_order_offset(node).unwrap();
                let offset = (offset as usize) * 64 + 8;
                data[offset..offset + 32].copy_from_slice(l.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(r.as_bytes());
            }
        }
        PreOrderMemOutboard {
            root: self.root,
            tree,
            data,
            changes: None,
        }
    }
}

impl<'a> Outboard for PostOrderMemOutboardRef<'a> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_raw_post_mem(&self.tree, self.data, node).map(parse_hash_pair))
    }
}

/// Post-order outboard, stored in memory.
///
/// This is the default outboard type for bao-tree, and is faster than the pre-order outboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostOrderMemOutboard {
    /// root hash
    pub(crate) root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes without length suffix
    pub(crate) data: Vec<u8>,
}

impl PostOrderMemOutboard {
    pub fn new(root: blake3::Hash, tree: BaoTree, data: Vec<u8>) -> Self {
        assert!(data.len() as u64 == tree.outboard_hash_pairs() * 64);
        Self { root, tree, data }
    }

    pub fn load(
        root: blake3::Hash,
        mut data: impl Read,
        block_size: BlockSize,
    ) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        let mut outboard = Vec::new();
        data.read_to_end(&mut outboard)?;
        if outboard.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let suffix = &outboard[outboard.len() - 8..];
        let len = u64::from_le_bytes(suffix.try_into().unwrap());
        let expected_outboard_size = outboard_size(len, block_size);
        let outboard_size = outboard.len() as u64;
        if outboard_size != expected_outboard_size {
            io_error!(
                "outboard length does not match expected outboard length: {outboard_size} != {expected_outboard_size}"                
            );
        }
        let tree = BaoTree::new(ByteNum(len), block_size);
        outboard.truncate(outboard.len() - 8);
        Ok(Self::new(root, tree, outboard))
    }

    /// The outboard data, without the length suffix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }

    pub fn flip(&self) -> PreOrderMemOutboard {
        self.as_outboard_ref().flip()
    }

    /// returns the outboard data, with the length suffix.
    pub fn into_inner(self) -> Vec<u8> {
        let mut res = self.data;
        res.extend_from_slice(self.tree.size.0.to_le_bytes().as_slice());
        res
    }

    pub fn as_outboard_ref(&self) -> PostOrderMemOutboardRef {
        PostOrderMemOutboardRef {
            root: self.root,
            tree: self.tree,
            data: &self.data,
        }
    }
}

impl Outboard for PostOrderMemOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        self.as_outboard_ref().load(node)
    }
}

impl OutboardMut for PostOrderMemOutboard {
    fn save(&mut self, node: TreeNode, pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        match self.tree.post_order_offset(node) {
            Some(offset) => {
                let offset = usize::try_from(offset.value() * 64).unwrap();
                self.data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                self.data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }

    fn set_size(&mut self, size: ByteNum) -> io::Result<()> {
        if self.data.is_empty() {
            self.tree = BaoTree::new(size, self.tree.block_size);
            self.data = vec![0; usize::try_from(self.tree.outboard_hash_pairs() * 64).unwrap()];
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot set size on non-empty outboard",
            ))
        }
    }
}

fn load_raw_post_mem(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    let offset = tree.post_order_offset(node)?.value();
    let offset = usize::try_from(offset * 64).unwrap();
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}

/// Pre-order outboard, stored in memory.
///
/// Mostly for compat with bao, not very fast.
#[derive(Debug, Clone, Copy)]
pub struct PreOrderMemOutboardRef<'a> {
    /// root hash
    root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes with length prefix
    data: &'a [u8],
}

impl<'a> PreOrderMemOutboardRef<'a> {
    pub fn new(root: blake3::Hash, block_size: BlockSize, data: &'a [u8]) -> Self {
        assert!(data.len() >= 8);
        let len = ByteNum(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, block_size);
        assert!(data.len() as u64 == tree.outboard_hash_pairs() * 64 + 8);
        Self { root, tree, data }
    }

    /// The outboard data, including the length prefix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }

    pub fn hash(&self) -> &blake3::Hash {
        &self.root
    }

    pub fn flip(&self) -> PostOrderMemOutboard {
        let tree = self.tree;
        let mut data = vec![0; self.data.len() - 8];
        for node in self.tree.post_order_nodes_iter() {
            if let Some((l, r)) = self.load(node).unwrap() {
                let offset = tree.post_order_offset(node).unwrap().value();
                let offset = usize::try_from(offset * 64).unwrap();
                data[offset..offset + 32].copy_from_slice(l.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(r.as_bytes());
            }
        }
        PostOrderMemOutboard {
            root: self.root,
            tree,
            data,
        }
    }
}

impl<'a> Outboard for PreOrderMemOutboardRef<'a> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_raw_pre_mem(&self.tree, &self.data, node).map(parse_hash_pair))
    }
}

/// Pre-order outboard, stored in memory.
///
/// Mostly for compat with bao, not very fast.
#[derive(Debug, Clone)]
pub struct PreOrderMemOutboard {
    /// root hash
    root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes with length prefix
    data: Vec<u8>,
    /// callbacks to track changes to the outboard
    changes: Option<RangeSet2<u64>>,
}

impl PreOrderMemOutboard {
    pub fn new(
        root: blake3::Hash,
        block_size: BlockSize,
        data: Vec<u8>,
        track_changes: bool,
    ) -> Self {
        assert!(data.len() >= 8);
        let len = ByteNum(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, block_size);
        assert!(data.len() as u64 == tree.outboard_hash_pairs() * 64 + 8);
        let changes = if track_changes {
            Some(RangeSet2::empty())
        } else {
            None
        };
        Self {
            root,
            tree,
            data,
            changes,
        }
    }
}

impl PreOrderMemOutboard {
    /// The outboard data, including the length prefix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }

    pub fn hash(&self) -> &blake3::Hash {
        &self.root
    }

    pub fn changes(&self) -> &Option<RangeSet2<u64>> {
        &self.changes
    }

    pub fn changes_mut(&mut self) -> &mut Option<RangeSet2<u64>> {
        &mut self.changes
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }

    pub fn as_outboard_ref(&self) -> PreOrderMemOutboardRef {
        PreOrderMemOutboardRef {
            root: self.root,
            tree: self.tree,
            data: &self.data,
        }
    }

    pub fn flip(&self) -> PostOrderMemOutboard {
        self.as_outboard_ref().flip()
    }
}

impl Outboard for PreOrderMemOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        self.as_outboard_ref().load(node)
    }
}

fn load_raw_pre_mem(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    // this is slow because pre_order_offset uses a loop.
    // pretty sure there is a way to write it as a single expression if you spend the time.
    let offset = tree.pre_order_offset(node)?;
    let offset = usize::try_from(offset * 64 + 8).unwrap();
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}

fn parse_hash_pair(buf: [u8; 64]) -> (blake3::Hash, blake3::Hash) {
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..]).unwrap());
    (l_hash, r_hash)
}

impl OutboardMut for PreOrderMemOutboard {
    fn save(&mut self, node: TreeNode, pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        match self.tree.pre_order_offset(node) {
            Some(offset) => {
                let offset_u64 = offset * 64;
                let offset = usize::try_from(offset_u64).unwrap();
                self.data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                self.data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                if let Some(changes) = &mut self.changes {
                    *changes |= RangeSet2::from(offset_u64..offset_u64 + 64);
                }
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }
    fn set_size(&mut self, size: ByteNum) -> io::Result<()> {
        if self.data.is_empty() {
            if size == ByteNum(0) {
                return Ok(());
            }
            self.tree = BaoTree::new(size, self.tree.block_size);
            self.data = vec![0; usize::try_from(self.tree.outboard_hash_pairs() * 64 + 8).unwrap()];
            self.data[0..8].copy_from_slice(&size.0.to_le_bytes());
            if let Some(changes) = &mut self.changes {
                *changes |= RangeSet2::from(0..8);
            }
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot set size on non-empty outboard",
            ))
        }
    }
}
