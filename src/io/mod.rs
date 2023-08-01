//! Implementation of bao streaming for std io and tokio io
use crate::{blake3, BaoTree, BlockSize, ByteNum, TreeNode};
use bytes::Bytes;

mod error;
pub use error::*;
#[cfg(feature = "tokio_fsm")]
pub mod fsm;
pub mod outboard;
pub mod sync;

/// A bao header, containing the size of the file.
#[derive(Debug)]
pub struct Header {
    /// The size of the file.
    ///
    /// This is not the size of the data you are being sent, but the oveall size
    /// of the file.
    pub size: ByteNum,
}

/// A parent hash pair.
#[derive(Debug)]
pub struct Parent {
    /// The node in the tree for which the hashes are.
    pub node: TreeNode,
    /// The pair of hashes for the node.
    pub pair: (blake3::Hash, blake3::Hash),
}

/// A leaf node.
#[derive(Debug)]
pub struct Leaf {
    /// The byte offset of the leaf in the file.
    pub offset: ByteNum,
    /// The data of the leaf.
    pub data: Bytes,
}

/// The outboard size of a file of size `size` with a block size of `block_size`
pub fn outboard_size(size: u64, block_size: BlockSize) -> u64 {
    BaoTree::outboard_size(ByteNum(size), block_size).0
}

/// The encoded size of a file of size `size` with a block size of `block_size`
pub fn encoded_size(size: u64, block_size: BlockSize) -> u64 {
    outboard_size(size, block_size) + size
}

/// Computes the pre order outboard of a file in memory.
pub fn outboard(input: impl AsRef<[u8]>, block_size: BlockSize) -> (Vec<u8>, blake3::Hash) {
    let outboard = BaoTree::outboard_post_order_mem(input, block_size).flip();
    let hash = *outboard.hash();
    (outboard.into_inner(), hash)
}
