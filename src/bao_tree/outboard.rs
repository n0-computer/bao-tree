use super::{parse_hash_pair, parse_hash_pair2, TreeNode};
use crate::{BaoTree, ByteNum};
use std::io;

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
}

/// An outboard is a just a thing that knows how big it is and can get you the hashes for a node.
pub trait Outboard {
    /// the root hash
    fn root(&self) -> blake3::Hash;
    /// the tree
    fn tree(&self) -> BaoTree;
    /// load the raw bytes for a node, as raw bytes
    fn load_raw(&self, node: TreeNode) -> io::Result<Option<[u8; 64]>>;
    /// load the hash pair for a node, as a hash pair
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        let data = self.load_raw(node)?;
        Ok(data.map(parse_hash_pair2))
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
    pub fn load(root: blake3::Hash, outboard: &'a [u8], chunk_group_log: u8) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        if outboard.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let (data, size) = outboard.split_at(outboard.len() - 8);
        let len = u64::from_le_bytes(size.try_into().unwrap());
        let tree = BaoTree::new(ByteNum(len), chunk_group_log);
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
}

impl<'a> Outboard for PostOrderMemOutboardRef<'a> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load_raw(&self, node: TreeNode) -> io::Result<Option<[u8; 64]>> {
        Ok(load_raw_post(&self.tree, self.data, node))
    }
}

#[derive(Debug, Clone)]
pub struct PostOrderMemOutboard {
    /// root hash
    root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes without length suffix
    data: Vec<u8>,
}

impl PostOrderMemOutboard {
    pub fn new(root: blake3::Hash, tree: BaoTree, data: Vec<u8>) -> Self {
        assert!(data.len() as u64 == tree.outboard_hash_pairs() * 64);
        Self { root, tree, data }
    }

    /// The outboard data, without the length suffix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }

    pub fn outboard_with_suffix(&self) -> Vec<u8> {
        let mut res = self.data.clone();
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
    fn load_raw(&self, node: TreeNode) -> io::Result<Option<[u8; 64]>> {
        Ok(load_raw_post(&self.tree, &self.data, node))
    }
}

fn load_raw_post(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    let offset = tree.post_order_offset(node).value()?;
    let offset = offset.to_usize() * 64;
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
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
}

impl PreOrderMemOutboard {
    pub fn new(root: blake3::Hash, tree: BaoTree, data: Vec<u8>) -> Self {
        assert!(data.len() as u64 == tree.outboard_hash_pairs() * 64 + 8);
        Self { root, tree, data }
    }

    /// The outboard data, including the length prefix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }
}

impl Outboard for PreOrderMemOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load_raw(&self, node: TreeNode) -> io::Result<Option<[u8; 64]>> {
        Ok(load_raw_pre(&self.tree, &self.data, node))
    }
}

fn load_raw_pre(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    let offset = tree.pre_order_offset(node)?;
    let offset = (offset as usize) * 64 + 8;
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}
