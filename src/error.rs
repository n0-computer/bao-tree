//! Errors when encoding or decoding
//! 
//! These erros contain more specific information about e.g. where a hash mismatch occured
use crate::{ChunkNum, TreeNode};
use std::{fmt, io};

/// Error when decoding from a reader
///
/// This can either be a io error or a more specific error like a hash mismatch
#[derive(Debug)]
pub enum DecodeError {
    /// There was an error reading from the underlying io
    Io(io::Error),
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// The query range was invalid
    InvalidQueryRange,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DecodeError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<DecodeError> for io::Error {
    fn from(e: DecodeError) -> Self {
        match e {
            DecodeError::Io(e) => e,
            DecodeError::ParentHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "parent hash mismatch")
            }
            DecodeError::LeafHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "leaf hash mismatch")
            }
            DecodeError::InvalidQueryRange => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid query range")
            }
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
    /// There was an error reading from the underlying io
    Io(io::Error),
    /// The hash of a parent did not match the expected hash
    ParentHashMismatch(TreeNode),
    /// The hash of a leaf did not match the expected hash
    LeafHashMismatch(ChunkNum),
    /// The query range was invalid
    InvalidQueryRange,
    /// File size does not match size in outboard
    SizeMismatch,
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
            EncodeError::ParentHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "parent hash mismatch")
            }
            EncodeError::LeafHashMismatch(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "leaf hash mismatch")
            }
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
