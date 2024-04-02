//! Implementations of the outboard traits
//!
//! A number of implementations for the sync and async outboard traits are provided.
//! Implementations for in-memory outboards, for outboards where the data resides on disk,
//! and a special implementation [EmptyOutboard] that just ignores all writes.
use crate::{blake3, BaoTree, BlockSize, ByteNum, TreeNode};
use std::io;

/// An empty outboard, that just returns 0 hashes for all nodes.
///
/// Also allows you to write and will immediately discard the data, a bit like /dev/null
#[derive(Debug)]
pub struct EmptyOutboard {
    tree: BaoTree,
    root: blake3::Hash,
}

impl EmptyOutboard {
    /// Create a new empty outboard with the given hash and tree.
    pub fn new(tree: BaoTree, root: blake3::Hash) -> Self {
        Self { tree, root }
    }
}

impl crate::io::sync::Outboard for EmptyOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(if self.tree.is_relevant_for_outboard(node) {
            // behave as if it was an outboard file filled with 0s
            Some((blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32])))
        } else {
            None
        })
    }
}

#[cfg(feature = "tokio_fsm")]
impl crate::io::fsm::Outboard for EmptyOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(if self.tree.is_relevant_for_outboard(node) {
            // behave as if it was an outboard file filled with 0s
            Some((blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32])))
        } else {
            None
        })
    }
}

impl crate::io::sync::OutboardMut for EmptyOutboard {
    fn save(&mut self, node: TreeNode, _pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        if self.tree.is_relevant_for_outboard(node) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            ))
        }
    }
}

#[cfg(feature = "tokio_fsm")]
impl crate::io::fsm::OutboardMut for EmptyOutboard {
    async fn save(
        &mut self,
        node: TreeNode,
        _pair: &(blake3::Hash, blake3::Hash),
    ) -> io::Result<()> {
        if self.tree.is_relevant_for_outboard(node) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            ))
        }
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A generic outboard in pre order
///
/// Caution: unlike the outboard implementation in the bao crate, this
/// implementation does not assume an 8 byte size prefix.
#[derive(Debug, Clone)]
pub struct PreOrderOutboard<R> {
    /// root hash
    pub root: blake3::Hash,
    /// tree defining the data
    pub tree: BaoTree,
    /// hashes with length prefix
    pub data: R,
}

/// A generic outboard in post order
#[derive(Debug, Clone)]
pub struct PostOrderOutboard<R> {
    /// root hash
    pub root: blake3::Hash,
    /// tree defining the data
    pub tree: BaoTree,
    /// hashes with length prefix
    pub data: R,
}

/// A post order outboard that is optimized for memory storage.
///
/// The traits are implemented for fixed size slices or mutable slices, so you
/// must make sure that the data is already the right size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostOrderMemOutboard<T = Vec<u8>> {
    /// root hash
    pub root: blake3::Hash,
    /// tree defining the data
    pub tree: BaoTree,
    /// hashes without length suffix
    pub data: T,
}

impl PostOrderMemOutboard {
    /// Create a new outboard from `data` and a `block_size`.
    ///
    /// This will hash the data and create an outboard.
    ///
    /// It is just a shortcut that calls [crate::io::sync::outboard_post_order].
    pub fn create(data: impl AsRef<[u8]>, block_size: BlockSize) -> Self {
        let data = data.as_ref();
        let size = data.len() as u64;
        let tree = BaoTree::new(ByteNum(size), block_size);
        let mut outboard = Vec::with_capacity(tree.outboard_size().to_usize());
        let root = crate::io::sync::outboard_post_order(data, tree, &mut outboard).unwrap();
        Self {
            root,
            tree,
            data: outboard,
        }
    }

    /// returns the outboard data, with the length suffix.
    pub fn into_inner_with_suffix(self) -> Vec<u8> {
        let mut res = self.data;
        res.extend_from_slice(self.tree.size.0.to_le_bytes().as_slice());
        res
    }
}

impl<T: AsRef<[u8]>> PostOrderMemOutboard<T> {
    /// Create a new outboard from a root hash, tree, and existing outboard data.
    ///
    /// Note that when writing to a [PreOrderMemOutboard], you must make sure
    /// that the data is already the right size. The size can be computed with
    /// [BaoTree::outboard_size].
    pub fn new(root: blake3::Hash, tree: BaoTree, outboard_data: T) -> Self {
        Self {
            root,
            tree,
            data: outboard_data,
        }
    }

    /// Map the outboard data to a new type.
    pub fn map_data<F, U>(self, f: F) -> PostOrderMemOutboard<U>
    where
        F: FnOnce(T) -> U,
        U: AsRef<[u8]>,
    {
        PostOrderMemOutboard {
            root: self.root,
            tree: self.tree,
            data: f(self.data),
        }
    }

    /// Flip the outboard to pre order.
    pub fn flip(&self) -> PreOrderMemOutboard {
        flip_post(self.root, self.tree, self.data.as_ref())
    }
}

impl<T: AsRef<[u8]>> crate::io::sync::Outboard for PostOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_post(&self.tree, self.data.as_ref(), node))
    }
}

#[cfg(feature = "tokio_fsm")]
impl<T: AsRef<[u8]>> crate::io::fsm::Outboard for PostOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_post(&self.tree, self.data.as_ref(), node))
    }
}

impl<T: AsMut<[u8]>> crate::io::sync::OutboardMut for PostOrderMemOutboard<T> {
    fn save(&mut self, node: TreeNode, pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        match self.tree.post_order_offset(node) {
            Some(offset) => {
                let offset = usize::try_from(offset.value() * 64).unwrap();
                let data = self.data.as_mut();
                data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }
}

#[cfg(feature = "tokio_fsm")]
impl<T: AsMut<[u8]>> crate::io::fsm::OutboardMut for PostOrderMemOutboard<T> {
    async fn save(
        &mut self,
        node: TreeNode,
        pair: &(blake3::Hash, blake3::Hash),
    ) -> io::Result<()> {
        match self.tree.post_order_offset(node) {
            Some(offset) => {
                let offset = usize::try_from(offset.value() * 64).unwrap();
                let data = self.data.as_mut();
                data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn load_raw_post_mem(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    let offset = tree.post_order_offset(node)?.value();
    let offset = usize::try_from(offset * 64).unwrap();
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}

fn load_post(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<(blake3::Hash, blake3::Hash)> {
    load_raw_post_mem(tree, data, node).map(parse_hash_pair)
}

fn flip_post(root: blake3::Hash, tree: BaoTree, data: &[u8]) -> PreOrderMemOutboard {
    let mut out = vec![0; data.len()];
    for node in tree.post_order_nodes_iter() {
        if let Some((l, r)) = load_post(&tree, data, node) {
            let offset = tree.pre_order_offset(node).unwrap();
            let offset = (offset as usize) * 64;
            out[offset..offset + 32].copy_from_slice(l.as_bytes());
            out[offset + 32..offset + 64].copy_from_slice(r.as_bytes());
        }
    }
    PreOrderMemOutboard {
        root,
        tree,
        data: out,
    }
}

/// A pre order outboard that is optimized for memory storage.
///
/// The traits are implemented for fixed size slices or mutable slices, so you
/// must make sure that the data is already the right size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreOrderMemOutboard<T = Vec<u8>> {
    /// root hash
    pub root: blake3::Hash,
    /// tree defining the data
    pub tree: BaoTree,
    /// hashes with length prefix
    pub data: T,
}

impl PreOrderMemOutboard {
    /// returns the outboard data, with the length prefix added.
    pub fn into_inner_with_prefix(self) -> Vec<u8> {
        let mut res = self.data;
        res.splice(0..0, self.tree.size.0.to_le_bytes());
        res
    }

    /// Create a new outboard from `data` and a `block_size`.
    ///
    /// This will hash the data and create an outboard
    pub fn create(data: impl AsRef<[u8]>, block_size: BlockSize) -> Self {
        let data = data.as_ref();
        let size = data.len() as u64;
        let tree = BaoTree::new(ByteNum(size), block_size);
        // the outboard impl for PreOrderMemOutboard requires just AsMut<[u8]>,
        // so the data must already be the right size.
        let outboard = vec![0u8; tree.outboard_size().to_usize()];
        let mut res = Self::new(blake3::Hash::from([0; 32]), tree, outboard);
        let root = crate::io::sync::outboard(data, tree, &mut res).unwrap();
        res.root = root;
        res
    }
}

impl<T: AsRef<[u8]>> PreOrderMemOutboard<T> {
    /// Create a new outboard from a root hash, tree, and existing outboard data.
    ///
    /// Note that when writing to a [PreOrderMemOutboard], you must make sure
    /// that the data is already the right size. The size can be computed with
    /// [BaoTree::outboard_size].
    ///
    /// Note that if you have data with a length prefix, you have to remove the prefix first.
    pub fn new(root: blake3::Hash, tree: BaoTree, outboard_data: T) -> Self {
        Self {
            root,
            tree,
            data: outboard_data,
        }
    }

    /// Map the outboard data to a new type.
    pub fn map_data<F, U>(self, f: F) -> PreOrderMemOutboard<U>
    where
        F: FnOnce(T) -> U,
        U: AsRef<[u8]>,
    {
        PreOrderMemOutboard {
            root: self.root,
            tree: self.tree,
            data: f(self.data),
        }
    }

    /// Flip the outboard to a post order outboard.
    pub fn flip(&self) -> PostOrderMemOutboard {
        flip_pre(self.root, self.tree, self.data.as_ref())
    }
}

impl<T: AsRef<[u8]>> crate::io::sync::Outboard for PreOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_pre(&self.tree, self.data.as_ref(), node))
    }
}

impl<T: AsMut<[u8]>> crate::io::sync::OutboardMut for PreOrderMemOutboard<T> {
    fn save(&mut self, node: TreeNode, pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        match self.tree.pre_order_offset(node) {
            Some(offset) => {
                let offset_u64 = offset * 64;
                let offset = usize::try_from(offset_u64).unwrap();
                let data = self.data.as_mut();
                data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }
}

#[cfg(feature = "tokio_fsm")]
impl<T: AsRef<[u8]>> crate::io::fsm::Outboard for PreOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    async fn load(&mut self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_raw_pre_mem(&self.tree, self.data.as_ref(), node).map(parse_hash_pair))
    }
}

#[cfg(feature = "tokio_fsm")]
impl<T: AsMut<[u8]>> crate::io::fsm::OutboardMut for PreOrderMemOutboard<T> {
    async fn save(
        &mut self,
        node: TreeNode,
        pair: &(blake3::Hash, blake3::Hash),
    ) -> io::Result<()> {
        match self.tree.pre_order_offset(node) {
            Some(offset) => {
                let offset_u64 = offset * 64;
                let offset = usize::try_from(offset_u64).unwrap();
                let data = self.data.as_mut();
                data[offset..offset + 32].copy_from_slice(pair.0.as_bytes());
                data[offset + 32..offset + 64].copy_from_slice(pair.1.as_bytes());
                Ok(())
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            )),
        }
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn load_raw_pre_mem(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    // this is a bit slow because pre_order_offset uses a loop.
    // pretty sure there is a way to write it as a single expression if you spend the time.
    // but profiling still has this in the nanosecond range, so this is unlikely to be a
    // bottleneck.
    let offset = tree.pre_order_offset(node)?;
    let offset = usize::try_from(offset * 64).unwrap();
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}

fn load_pre(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<(blake3::Hash, blake3::Hash)> {
    load_raw_pre_mem(tree, data, node).map(parse_hash_pair)
}

fn flip_pre(root: blake3::Hash, tree: BaoTree, data: &[u8]) -> PostOrderMemOutboard {
    let mut out = vec![0; data.len()];
    for node in tree.post_order_nodes_iter() {
        if let Some((l, r)) = load_pre(&tree, data, node) {
            let offset = tree.post_order_offset(node).unwrap().value();
            let offset = usize::try_from(offset * 64).unwrap();
            out[offset..offset + 32].copy_from_slice(l.as_bytes());
            out[offset + 32..offset + 64].copy_from_slice(r.as_bytes());
        }
    }
    PostOrderMemOutboard {
        root,
        tree,
        data: out,
    }
}

pub(crate) fn parse_hash_pair(buf: [u8; 64]) -> (blake3::Hash, blake3::Hash) {
    let l_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[..32]).unwrap());
    let r_hash = blake3::Hash::from(<[u8; 32]>::try_from(&buf[32..]).unwrap());
    (l_hash, r_hash)
}
