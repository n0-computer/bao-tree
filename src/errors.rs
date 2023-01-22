use std::io;

#[derive(Debug)]
pub enum TraversalError<IoError> {
    Io(IoError),
    Unavailable,
}

#[derive(Debug)]
pub enum AddSliceError<IoError> {
    /// io error when reading from the slice
    Io(io::Error),
    /// io error when reading from or writing to the local store
    LocalIo(IoError),
    /// slice length does not match the expected length
    WrongLength(u64),
    /// hash validation failed
    Validation(ValidateError<IoError>),
}

#[derive(Debug)]
pub enum ValidateError<IoError> {
    /// io error when reading from or writing to the local store
    Io(IoError),
    HashMismatch(u64),
    MissingHash(u64),
}

#[derive(Debug)]
pub(crate) enum TraversalResult<T> {
    IoError(T),
    Unavailable,
    Done,
}
