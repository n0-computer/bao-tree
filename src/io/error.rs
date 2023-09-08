//! Errors when encoding or decoding
//!
//! These erros contain more specific information about e.g. where a hash mismatch occured
use crate::{ChunkNum, TreeNode};
use std::{fmt, io};

/// Error when starting to decode from a reader
#[derive(Debug)]
pub enum StartDecodeError {
    /// We got an EOF when reading the size, indicating that the remote end does not have the blob
    NotFound,
    /// A generic io error
    Io(io::Error),
}

impl fmt::Display for StartDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for StartDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StartDecodeError> for io::Error {
    fn from(e: StartDecodeError) -> Self {
        use StartDecodeError::*;
        match e {
            Io(e) => e,
            NotFound => io::Error::new(io::ErrorKind::UnexpectedEof, e),
        }
    }
}

impl StartDecodeError {
    pub(crate) fn maybe_not_found(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::NotFound
        } else {
            Self::Io(e)
        }
    }
}

/// Error when decoding from a reader, both when reading the size and after
///
/// This is an union of [`StartDecodeError`] and [`DecodeError`] for convenience.
#[derive(Debug)]
pub enum AnyDecodeError {
    /// We got an EOF when reading the size, indicating that the remote end does not have the blob
    NotFound,
    /// We got an EOF while reading a parent hash pair, indicating that the remote end does not have the outboard
    ParentNotFound(TreeNode),
    /// We got an EOF while reading a chunk, indicating that the remote end does not have the data
    LeafNotFound(ChunkNum),
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// There was an error reading from the underlying io
    Io(io::Error),
}

impl From<DecodeError> for AnyDecodeError {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::Io(e) => Self::Io(e),
            DecodeError::ParentHashMismatch(node) => Self::ParentHashMismatch(node),
            DecodeError::LeafHashMismatch(chunk) => Self::LeafHashMismatch(chunk),
            DecodeError::LeafNotFound(chunk) => Self::LeafNotFound(chunk),
            DecodeError::ParentNotFound(node) => Self::ParentNotFound(node),
        }
    }
}

impl From<StartDecodeError> for AnyDecodeError {
    fn from(e: StartDecodeError) -> Self {
        match e {
            StartDecodeError::Io(e) => Self::Io(e),
            StartDecodeError::NotFound => Self::NotFound,
        }
    }
}

impl fmt::Display for AnyDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for AnyDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<AnyDecodeError> for io::Error {
    fn from(e: AnyDecodeError) -> Self {
        match e {
            AnyDecodeError::Io(e) => e,
            AnyDecodeError::ParentHashMismatch(node) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "parent hash mismatch (level {}, block {})",
                    node.level(),
                    node.mid().0
                ),
            ),
            AnyDecodeError::LeafHashMismatch(chunk) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("leaf hash mismatch (offset {})", chunk.to_bytes().0),
            ),
            AnyDecodeError::LeafNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            AnyDecodeError::ParentNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            AnyDecodeError::NotFound => io::Error::new(io::ErrorKind::UnexpectedEof, e),
        }
    }
}

/// Error when decoding from a reader, after the size has been read
#[derive(Debug)]
pub enum DecodeError {
    /// We got an EOF while reading a parent hash pair, indicating that the remote end does not have the outboard
    ParentNotFound(TreeNode),
    /// We got an EOF while reading a chunk, indicating that the remote end does not have the data
    LeafNotFound(ChunkNum),
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// There was an error reading from the underlying io
    Io(io::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<DecodeError> for io::Error {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::Io(e) => e,
            DecodeError::ParentHashMismatch(node) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "parent hash mismatch (level {}, block {})",
                    node.level(),
                    node.mid().0
                ),
            ),
            DecodeError::LeafHashMismatch(chunk) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("leaf hash mismatch (offset {})", chunk.to_bytes().0),
            ),
            DecodeError::LeafNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            DecodeError::ParentNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
        }
    }
}

impl DecodeError {
    pub(crate) fn maybe_parent_not_found(e: io::Error, node: TreeNode) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::ParentNotFound(node)
        } else {
            Self::Io(e)
        }
    }

    pub(crate) fn maybe_leaf_not_found(e: io::Error, chunk: ChunkNum) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Self::LeafNotFound(chunk)
        } else {
            Self::Io(e)
        }
    }
}

/// Error when encoding from outboard and data
///
/// This can either be a io error or a more specific error like a hash mismatch
/// or a size mismatch. If the remote end stops listening while we are writing,
/// the error will indicate which parent or chunk we were writing at the time.
#[derive(Debug)]
pub enum EncodeError {
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// We got a ConnectionReset while writing a parent hash pair, indicating that the remote end stopped listening
    ParentWrite(TreeNode),
    /// We got a ConnectionReset while writing a chunk, indicating that the remote end stopped listening
    LeafWrite(ChunkNum),
    /// File size does not match size in outboard
    SizeMismatch,
    /// There was an error reading from the underlying io
    Io(io::Error),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EncodeError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<EncodeError> for io::Error {
    fn from(e: EncodeError) -> Self {
        match e {
            EncodeError::Io(e) => e,
            EncodeError::ParentHashMismatch(node) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "parent hash mismatch (level {}, block {})",
                    node.level(),
                    node.mid().0
                ),
            ),
            EncodeError::LeafHashMismatch(chunk) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("leaf hash mismatch at {}", chunk.to_bytes().0),
            ),
            EncodeError::ParentWrite(node) => io::Error::new(
                io::ErrorKind::ConnectionReset,
                format!(
                    "parent write failed (level {}, block {})",
                    node.level(),
                    node.mid().0
                ),
            ),
            EncodeError::LeafWrite(chunk) => io::Error::new(
                io::ErrorKind::ConnectionReset,
                format!("leaf write failed at {}", chunk.to_bytes().0),
            ),
            EncodeError::SizeMismatch => {
                io::Error::new(io::ErrorKind::InvalidData, "size mismatch")
            }
        }
    }
}

impl From<io::Error> for EncodeError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl EncodeError {
    pub(crate) fn maybe_parent_write(e: io::Error, node: TreeNode) -> Self {
        if e.kind() == io::ErrorKind::ConnectionReset {
            Self::ParentWrite(node)
        } else {
            Self::Io(e)
        }
    }

    pub(crate) fn maybe_leaf_write(e: io::Error, chunk: ChunkNum) -> Self {
        if e.kind() == io::ErrorKind::ConnectionReset {
            Self::LeafWrite(chunk)
        } else {
            Self::Io(e)
        }
    }
}
