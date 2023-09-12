//! Implementations of the outboard traits
//!
//! A number of implementations for the sync and async outboard traits are provided.
//! Implementations for in-memory outboards, for outboards where the data resides on disk,
//! and a special implementation [EmptyOutboard] that just ignores all writes.
use range_collections::RangeSet2;

use super::TreeNode;
use crate::{
    blake3,
    io::sync::{Outboard, OutboardMut},
    BaoTree, BlockSize, ByteNum,
};
use std::{fmt, io};

macro_rules! io_error {
    ($($arg:tt)*) => {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!($($arg)*)))
    };
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

impl crate::io::fsm::Outboard for EmptyOutboard {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    type LoadFuture<'a> = futures::future::Ready<io::Result<Option<(blake3::Hash, blake3::Hash)>>>;
    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        futures::future::ok(if self.tree.is_relevant_for_outboard(node) {
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

impl crate::io::fsm::OutboardMut for EmptyOutboard {
    fn save(
        &mut self,
        node: TreeNode,
        _pair: &(blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'_> {
        let res = if self.tree.is_relevant_for_outboard(node) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid node for this outboard",
            ))
        };
        futures::future::ready(res)
    }

    type SaveFuture<'a> = futures::future::Ready<io::Result<()>>;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        futures::future::ready(Ok(()))
    }

    type SyncFuture<'a> = futures::future::Ready<io::Result<()>>;
}

/// A generic outboard in pre order
#[derive(Debug, Clone)]
pub struct PreOrderOutboard<R> {
    /// root hash
    pub root: blake3::Hash,
    /// tree defining the data
    pub tree: BaoTree,
    /// hashes with length prefix
    pub data: R,
}

impl<R> PreOrderOutboard<R> {
    /// Return the inner reader
    pub fn into_inner(self) -> R {
        self.data
    }
}

/// A generic outboard in post order
#[derive(Debug, Clone)]
pub struct PostOrderOutboard<R> {
    /// root hash
    pub(crate) root: blake3::Hash,
    /// tree defining the data
    pub(crate) tree: BaoTree,
    /// hashes with length prefix
    pub(crate) data: R,
}

impl<R> PostOrderOutboard<R> {
    /// Return the inner reader
    pub fn into_inner(self) -> R {
        self.data
    }
}

/// A post order outboard that is optimized for memory storage.
#[derive(Clone, PartialEq, Eq)]
pub struct PostOrderMemOutboard<T = Vec<u8>> {
    /// root hash
    pub(crate) root: blake3::Hash,
    /// tree defining the data
    pub(crate) tree: BaoTree,
    /// hashes without length suffix
    pub(crate) data: T,
}

fn to_hex(data: &[u8], target: &mut String) {
    let table = b"0123456789abcdef";
    for &b in data.iter() {
        target.push(table[(b >> 4) as usize] as char);
        target.push(table[(b & 0xf) as usize] as char);
    }
}

impl<T: AsRef<[u8]>> fmt::Debug for PostOrderMemOutboard<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut text = String::new();
        for chunk in self.data.as_ref().chunks(64) {
            for hash in chunk.chunks(32) {
                to_hex(hash, &mut text);
                text.push(' ');
            }
            text.push(' ');
        }
        f.debug_struct("PostOrderMemOutboard")
            .field("root", &self.root)
            .field("tree", &self.tree)
            .field("data", &text.trim_end())
            .finish()
    }
}

impl PostOrderMemOutboard {
    /// returns the outboard data, with the length suffix.
    pub fn into_inner_with_suffix(self) -> Vec<u8> {
        let mut res = self.data;
        res.extend_from_slice(self.tree.size.0.to_le_bytes().as_slice());
        res
    }
}

impl<T: AsRef<[u8]>> PostOrderMemOutboard<T> {
    pub(crate) fn new(root: blake3::Hash, tree: BaoTree, data: T) -> Self {
        assert_eq!(data.as_ref().len() as u64, tree.outboard_hash_pairs() * 64);
        Self { root, tree, data }
    }

    /// The outboard data, without the length suffix.
    pub fn outboard(&self) -> &[u8] {
        self.data.as_ref()
    }

    /// Flip the outboard to pre order.
    pub fn flip(&self) -> PreOrderMemOutboardMut {
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

impl<T: AsRef<[u8]>> crate::io::fsm::Outboard for PostOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    type LoadFuture<'a> = futures::future::Ready<io::Result<Option<(blake3::Hash, blake3::Hash)>>>
        where T: 'a;
    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        futures::future::ok(load_post(&self.tree, self.data.as_ref(), node))
    }
}

impl<T: AsMut<[u8]>> crate::io::sync::OutboardMut for PostOrderMemOutboard<T> {
    fn save(&mut self, node: TreeNode, pair: &(blake3::Hash, blake3::Hash)) -> io::Result<()> {
        match self.tree.post_order_offset(node) {
            Some(offset) => {
                println!("{:?}", offset);
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

impl<T: AsMut<[u8]>> crate::io::fsm::OutboardMut for PostOrderMemOutboard<T> {
    type SaveFuture<'a> = futures::future::Ready<io::Result<()>> where T: 'a;

    fn save(
        &mut self,
        node: TreeNode,
        pair: &(blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'_> {
        let res = match self.tree.post_order_offset(node) {
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
        };
        futures::future::ready(res)
    }

    type SyncFuture<'a> = futures::future::Ready<io::Result<()>> where T: 'a;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        futures::future::ready(Ok(()))
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

fn flip_post(root: blake3::Hash, tree: BaoTree, data: &[u8]) -> PreOrderMemOutboardMut {
    let mut out = vec![0; data.len() + 8];
    out[0..8].copy_from_slice(tree.size.0.to_le_bytes().as_slice());
    for node in tree.post_order_nodes_iter() {
        if let Some((l, r)) = load_post(&tree, data, node) {
            let offset = tree.pre_order_offset(node).unwrap();
            let offset = (offset as usize) * 64 + 8;
            out[offset..offset + 32].copy_from_slice(l.as_bytes());
            out[offset + 32..offset + 64].copy_from_slice(r.as_bytes());
        }
    }
    PreOrderMemOutboardMut {
        root,
        tree,
        data: out,
        changes: None,
    }
}

/// A pre order outboard that is optimized for memory storage.
#[derive(Debug, Clone)]
pub struct PreOrderMemOutboard<T> {
    /// root hash
    root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes with length prefix
    data: T,
}

impl<T: AsRef<[u8]>> PreOrderMemOutboard<T> {
    /// Create a new outboard from a root hash, block size, and data.
    pub fn new(root: blake3::Hash, block_size: BlockSize, data: T) -> io::Result<Self> {
        let content = data.as_ref();
        if content.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let len = ByteNum(u64::from_le_bytes(content[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, block_size);
        let expected_outboard_size = crate::io::outboard_size(len.0, block_size);
        if content.len() as u64 != expected_outboard_size {
            io_error!("invalid outboard size");
        }
        // zero pad the rest, if needed.
        Ok(Self { root, tree, data })
    }

    /// The outboard data, including the length prefix.
    pub fn outboard(&self) -> &[u8] {
        self.data.as_ref()
    }

    /// The root hash.
    pub fn hash(&self) -> &blake3::Hash {
        &self.root
    }

    /// Get the inner data.
    pub fn into_inner(self) -> T {
        self.data
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

impl<T: AsRef<[u8]> + 'static> crate::io::fsm::Outboard for PreOrderMemOutboard<T> {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    type LoadFuture<'a> = futures::future::Ready<io::Result<Option<(blake3::Hash, blake3::Hash)>>>;
    fn load(&mut self, node: TreeNode) -> Self::LoadFuture<'_> {
        let res = load_raw_pre_mem(&self.tree, self.data.as_ref(), node).map(parse_hash_pair);
        futures::future::ok(res)
    }
}

impl<T: AsMut<[u8]>> crate::io::fsm::OutboardMut for PreOrderMemOutboard<T> {
    type SaveFuture<'a> = futures::future::Ready<io::Result<()>> where T: 'a;

    fn save(
        &mut self,
        node: TreeNode,
        pair: &(blake3::Hash, blake3::Hash),
    ) -> Self::SaveFuture<'_> {
        let res = match self.tree.pre_order_offset(node) {
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
        };
        futures::future::ready(res)
    }

    type SyncFuture<'a> = futures::future::Ready<io::Result<()>> where T: 'a;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        futures::future::ready(Ok(()))
    }
}

/// A mutable pre order outboard that is optimized for memory storage.
///
/// Mostly for compat with bao, not very fast.
#[derive(Debug, Clone)]
pub struct PreOrderMemOutboardMut {
    /// root hash
    root: blake3::Hash,
    /// tree defining the data
    tree: BaoTree,
    /// hashes with length prefix
    data: Vec<u8>,
    /// callbacks to track changes to the outboard
    changes: Option<RangeSet2<u64>>,
}

impl PreOrderMemOutboardMut {
    /// Create a new mutable outboard.
    pub fn new(
        root: blake3::Hash,
        block_size: BlockSize,
        mut data: Vec<u8>,
        track_changes: bool,
    ) -> io::Result<Self> {
        if data.len() < 8 {
            io_error!("outboard must be at least 8 bytes");
        };
        let len = ByteNum(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let tree = BaoTree::new(len, block_size);
        let expected_outboard_size = crate::io::outboard_size(len.0, block_size);
        if data.len() as u64 > expected_outboard_size {
            io_error!("outboard too large");
        }
        // zero pad the rest, if needed.
        data.resize(expected_outboard_size as usize, 0u8);
        let changes = if track_changes {
            Some(RangeSet2::empty())
        } else {
            None
        };
        Ok(Self {
            root,
            tree,
            data,
            changes,
        })
    }

    /// The outboard data, including the length prefix.
    pub fn outboard(&self) -> &[u8] {
        &self.data
    }

    /// The root hash.
    pub fn hash(&self) -> &blake3::Hash {
        &self.root
    }

    /// Get a set of changes to the outboard.
    pub fn changes(&self) -> &Option<RangeSet2<u64>> {
        &self.changes
    }

    /// Mutable reference to the set of changes.
    pub fn changes_mut(&mut self) -> &mut Option<RangeSet2<u64>> {
        &mut self.changes
    }

    /// The outboard data, including the length prefix.
    pub fn into_inner(self) -> Vec<u8> {
        self.data
    }

    /// Flip this outboard into a post order outboard.
    pub fn flip(&self) -> PostOrderMemOutboard {
        flip_pre(self.root, self.tree, self.data.as_ref())
    }
}

impl Outboard for PreOrderMemOutboardMut {
    fn root(&self) -> blake3::Hash {
        self.root
    }
    fn tree(&self) -> BaoTree {
        self.tree
    }
    fn load(&self, node: TreeNode) -> io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        Ok(load_pre(&self.tree, &self.data, node))
    }
}

fn load_raw_pre_mem(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<[u8; 64]> {
    // this is a bit slow because pre_order_offset uses a loop.
    // pretty sure there is a way to write it as a single expression if you spend the time.
    // but profiling still has this in the nanosecond range, so this is unlikely to be a
    // bottleneck.
    let offset = tree.pre_order_offset(node)?;
    let offset = usize::try_from(offset * 64 + 8).unwrap();
    let slice = &data[offset..offset + 64];
    Some(slice.try_into().unwrap())
}

fn load_pre(tree: &BaoTree, data: &[u8], node: TreeNode) -> Option<(blake3::Hash, blake3::Hash)> {
    load_raw_pre_mem(tree, data, node).map(parse_hash_pair)
}

fn flip_pre(root: blake3::Hash, tree: BaoTree, data: &[u8]) -> PostOrderMemOutboard {
    let mut out = vec![0; data.len() - 8];
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

impl OutboardMut for PreOrderMemOutboardMut {
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
}
