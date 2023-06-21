//! Implementation of bao streaming for std io and tokio io
use crate::{ByteNum, TreeNode};
use bytes::Bytes;

pub mod error;
#[cfg(feature = "tokio_fsm")]
pub mod fsm;
pub mod sync;

/// An item of a decode response
///
/// This is used by both sync and tokio decoders
#[derive(Debug)]
pub enum DecodeResponseItem {
    /// We got the header and now know how big the overall size is
    ///
    /// Actually this is just how big the remote side *claims* the overall size is.
    /// In an adversarial setting, this could be wrong.
    Header(Header),
    /// a parent node, to update the outboard
    Parent(Parent),
    /// a leaf node, to write to the file
    Leaf(Leaf),
}

impl From<Header> for DecodeResponseItem {
    fn from(h: Header) -> Self {
        Self::Header(h)
    }
}

impl From<Parent> for DecodeResponseItem {
    fn from(p: Parent) -> Self {
        Self::Parent(p)
    }
}

impl From<Leaf> for DecodeResponseItem {
    fn from(l: Leaf) -> Self {
        Self::Leaf(l)
    }
}

#[derive(Debug)]
pub struct Header {
    pub size: ByteNum,
}

#[derive(Debug)]
pub struct Parent {
    pub node: TreeNode,
    pub pair: (blake3::Hash, blake3::Hash),
}

#[derive(Debug)]
pub struct Leaf {
    pub offset: ByteNum,
    pub data: Bytes,
}
