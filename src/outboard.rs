use super::{outboard_size, parse_hash_pair, TreeNode};
use crate::{BaoTree, BlockSize, ByteNum};
use std::io::{self, Read};

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
        Ok(data.map(parse_hash_pair))
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
    pub fn load(
        root: blake3::Hash,
        outboard: &'a [u8],
        chunk_group_log: BlockSize,
    ) -> io::Result<Self> {
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

    pub fn flip(&self) -> PreOrderMemOutboard {
        let tree = self.tree;
        let mut data = vec![0; self.data.len() + 8];
        data[0..8].copy_from_slice(tree.size.0.to_le_bytes().as_slice());
        for node in self.tree.iterate() {
            if let Some(p) = self.load_raw(node).unwrap() {
                let offset = tree.pre_order_offset(node).unwrap();
                let offset = (offset as usize) * 64 + 8;
                data[offset..offset + 64].copy_from_slice(&p);
            }
        }
        PreOrderMemOutboard {
            root: self.root,
            tree,
            data,
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
    fn load_raw(&self, node: TreeNode) -> io::Result<Option<[u8; 64]>> {
        Ok(load_raw_post_mem(&self.tree, self.data, node))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn load(
        root: blake3::Hash,
        mut data: impl Read,
        chunk_group_log: BlockSize,
    ) -> io::Result<Self> {
        // validate roughly that the outboard is correct
        let mut outboard = Vec::new();
        data.read_to_end(&mut outboard)?;
        if outboard.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let suffix = &outboard[outboard.len() - 8..];
        let len = u64::from_le_bytes(suffix.try_into().unwrap());
        let expected_outboard_size = outboard_size(len, chunk_group_log);
        let outboard_size = outboard.len() as u64;
        if outboard_size != expected_outboard_size {
            io_error!(
                "outboard length does not match expected outboard length: {outboard_size} != {expected_outboard_size}"                
            );
        }
        let tree = BaoTree::new(ByteNum(len), chunk_group_log);
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
        Ok(load_raw_post_mem(&self.tree, &self.data, node))
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
    pub fn new(root: blake3::Hash, chunk_group_log: BlockSize, data: Vec<u8>) -> Self {
        assert!(data.len() >= 8);
        let len = ByteNum(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, chunk_group_log);
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

    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }

    pub fn flip(&self) -> PostOrderMemOutboard {
        let tree = self.tree;
        let mut data = vec![0; self.data.len() - 8];
        for node in self.tree.iterate() {
            if let Some(p) = self.load_raw(node).unwrap() {
                let offset = tree.post_order_offset(node).unwrap().value();
                let offset = usize::try_from(offset * 64).unwrap();
                data[offset..offset + 64].copy_from_slice(&p);
            }
        }
        PostOrderMemOutboard {
            root: self.root,
            tree,
            data,
        }
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
        Ok(load_raw_pre_mem(&self.tree, &self.data, node))
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
