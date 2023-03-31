//! Implementation of bao streaming for std io and tokio io
use crate::{ByteNum, TreeNode};
use bytes::Bytes;

pub mod error;
pub mod sync;
#[cfg(feature = "tokio_io")]
pub mod tokio;

/// An item of a decode response
///
/// This is used by both sync and tokio decoders
#[derive(Debug)]
pub enum DecodeResponseItem {
    /// We got the header and now know how big the overall size is
    ///
    /// Actually this is just how big the remote side *claims* the overall size is.
    /// In an adversarial setting, this could be wrong.
    Header { size: ByteNum },
    /// a parent node, to update the outboard
    Parent {
        node: TreeNode,
        pair: (blake3::Hash, blake3::Hash),
    },
    /// a leaf node, to write to the file
    Leaf { offset: ByteNum, data: Bytes },
}
