//! Implementation of bao streaming for std io and tokio io
use crate::{ByteNum, TreeNode};
use bytes::Bytes;

pub mod error;
#[cfg(feature = "tokio_fsm")]
pub mod fsm;
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
