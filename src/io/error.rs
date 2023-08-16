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
    /// The query range was invalid for the given size
    InvalidQueryRange,
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
            InvalidQueryRange => io::Error::new(io::ErrorKind::InvalidInput, "invalid query range"),
            NotFound => io::Error::new(io::ErrorKind::UnexpectedEof, e),
        }
    }
}

/// Error when decoding from a reader
///
/// This can either be a io error or a more specific error like a hash mismatch
#[derive(Debug)]
pub enum DecodeError {
    /// We got an EOF when reading the size, indicating that the remote end does not have the blob
    NotFound,
    /// The query range was invalid
    InvalidQueryRange,
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
            DecodeError::InvalidQueryRange => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid query range")
            }
            DecodeError::LeafNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            DecodeError::ParentNotFound(_) => io::Error::new(io::ErrorKind::UnexpectedEof, e),
            DecodeError::NotFound => io::Error::new(io::ErrorKind::UnexpectedEof, e),
        }
    }
}

impl From<io::Error> for DecodeError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Error when encoding from outboard and data
///
/// This can either be a io error or a more specific error like a hash mismatch
/// or a size mismatch.
#[derive(Debug)]
pub enum EncodeError {
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// The query range was invalid
    InvalidQueryRange,
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
            EncodeError::InvalidQueryRange => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid query range")
            }
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
