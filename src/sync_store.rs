use std::{
    fs::File,
    io::{self, Read},
    iter::FusedIterator,
    ops::Range,
    path::{Path, PathBuf},
};

use crate::{errors::*, tree::*, BlakeFile};

/// Methods that are common to the sync and async store
///
/// mostly methods concerned with the tree structure
pub trait StoreCommon: Sized {
    /// block level
    fn block_level(&self) -> BlockLevel;

    /// length of the stored data
    fn data_len(&self) -> ByteNum;

    /// length of the flattened merkle tree in nodes
    fn tree_len(&self) -> NodeNum;

    /// byte range for a given offset
    fn leaf_byte_range(&self, index: BlockNum) -> Range<ByteNum> {
        let start = index.to_bytes(self.block_level());
        let end = (index + 1)
            .to_bytes(self.block_level())
            .min(self.data_len().max(start));
        start..end
    }

    fn block_size(&self) -> ByteNum {
        block_size(self.block_level())
    }

    fn block_count(&self) -> BlockNum {
        blocks(self.data_len(), self.block_level())
    }

    /// number of leaf hashes in our tree
    ///
    /// will return 1 for empty data, since even empty data has a root hash
    fn leafs(&self) -> BlockNum {
        leafs(self.tree_len())
    }

    /// offset of the root hash
    fn root(&self) -> NodeNum {
        root(self.leafs())
    }

    /// the byte range the store covers
    fn byte_range(&self) -> Range<ByteNum> {
        ByteNum(0)..self.data_len()
    }
}

/// Interface for a synchronous store
///
/// This includs just methods that have to be implemented
pub trait SyncStore: StoreCommon {
    /// the type of io error when interacting with the store
    ///
    /// for an in-memory store, this is can be infallible
    type IoError: std::fmt::Debug;

    /// get a hash from the merkle tree, with existence check
    ///
    /// will panic if the offset is out of bounds
    fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError>;

    /// set or clear a hash in the merkle tree
    ///
    /// will panic if the offset is out of bounds
    fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError>;

    /// get a block of data
    ///
    /// this will be the block size for all blocks except the last one, which may be smaller
    ///
    /// will panic if the offset is out of bounds
    fn get_block(&self, block: BlockNum) -> Result<Option<&[u8]>, Self::IoError>;

    /// set or clear a block of data
    ///
    /// when setting data, the length must match the block size except for the last block,
    /// for which it must match the remainder of the data length
    ///
    /// will panic if the offset is out of bounds
    fn set_block(&mut self, block: BlockNum, data: Option<&[u8]>) -> Result<(), Self::IoError>;

    /// new empty store with the given block level
    ///
    /// the store will be initialized with the given block level, but no data.
    /// the hash will be set to the hash of the empty slice.
    ///
    /// note that stores with different block levels are not compatible.
    fn empty(block_level: BlockLevel) -> Result<Self, Self::IoError>;

    /// grow the store to the given length
    ///
    /// this will grow the tree and the data, but not invalidate the hashes that
    /// are no longer correct, so it will leave the store in an inconsistent state.
    ///
    /// Use grow to grow the store and invalidate the hashes.
    fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError>;
}

impl<S: SyncStore> BlakeFile<S> {
    pub fn wrap(store: S) -> Self {
        Self(store)
    }

    pub fn empty(block_level: BlockLevel) -> Result<Self, S::IoError> {
        Ok(Self(S::empty(block_level)?))
    }

    /// create a new completely initialized store from a slice of data
    pub fn new(data: &[u8], block_level: BlockLevel) -> Result<Self, S::IoError> {
        let mut res = Self::empty(block_level)?;
        res.grow(ByteNum(data.len() as u64))?;
        for (block, data) in data.chunks(res.0.block_size().to_usize()).enumerate() {
            res.0.set_block(BlockNum(block as u64), Some(data))?;
        }
        res.rehash()?;
        Ok(res)
    }

    pub fn byte_range(&self) -> Range<ByteNum> {
        self.0.byte_range()
    }

    pub fn hash(&self) -> Result<Option<blake3::Hash>, S::IoError> {
        self.0.get_hash(self.0.root())
    }

    /// add a slice of data to the store
    ///
    /// returns
    /// - AddSliceError::WrongLength if the length of the data does not match the length of the store
    /// - AddSliceError::Io if there is an IO error
    /// - AddSliceError::LocalIo if there is a local IO error reading or writing the hashes or the data
    /// - AddSliceError::Validation if the data does not match the hashes
    /// - Ok(()) if the slice was successfully added
    pub fn add_from_slice(
        &mut self,
        byte_range: Range<ByteNum>,
        reader: &mut impl Read,
    ) -> Result<(), AddSliceError<S::IoError>> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).map_err(AddSliceError::Io)?;
        let len = u64::from_le_bytes(buf);
        if len != self.0.data_len() {
            return Err(AddSliceError::WrongLength(len));
        }
        let offset_range = node_range(byte_range, self.0.block_level());
        let mut buffer = vec![0u8; block_size(self.0.block_level()).to_usize()];
        self.add_from_slice_0(self.0.root(), &offset_range, reader, &mut buffer, true)?;
        Ok(())
    }

    /// extract a slice of data from the store, producing a verifiable slice
    ///
    /// the returned reader will fail with an io error if either
    /// - the slice is not available or
    /// - there is an internal io error accessing the data or the needed hashes.
    pub fn extract_slice(&self, byte_range: Range<ByteNum>) -> SliceReader<'_, S> {
        let iter = self.slice_iter(byte_range);
        let buffer = vec![0u8; block_size(self.0.block_level()).to_usize()];
        SliceReader {
            iter,
            buffer,
            start: 0,
            end: 0,
        }
    }

    /// set or validate a node in the tree, with bounds check
    fn set_or_validate(
        &mut self,
        offset: NodeNum,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<S::IoError>> {
        match self.0.get_hash(offset).map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(_) => Err(ValidateError::HashMismatch(offset.0)),
            None => {
                self.0
                    .set_hash(offset, Some(hash))
                    .map_err(ValidateError::Io)?;
                Ok(())
            }
        }
    }
    /// validate a node in the tree, with bounds check
    fn validate(
        &self,
        offset: NodeNum,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<S::IoError>> {
        match self.0.get_hash(offset).map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(_) => Err(ValidateError::HashMismatch(offset.0)),
            None => Err(ValidateError::MissingHash(offset.0)),
        }
    }

    /// grow the store to the given length and update the hashes
    fn grow(&mut self, new_len: ByteNum) -> Result<(), S::IoError> {
        if new_len < self.0.data_len() {
            panic!("shrink not allowed");
        }
        if new_len == self.0.data_len() {
            return Ok(());
        }
        // clear the last leaf hash
        // todo: we only have to do this if it was not full, but we do it always for now
        self.0.set_hash(self.0.tree_len() - 1, None)?;
        // clear all non leaf hashes
        // todo: this is way too much. we should only clear the hashes that are affected by the new data
        for i in (1..self.0.tree_len().0).step_by(2) {
            self.0.set_hash(NodeNum(i), None)?;
        }
        self.0.grow_storage(new_len)?;
        Ok(())
    }

    /// todo: precisely invalidate the hashes that are affected by the new data
    ///
    /// to be called from grow
    fn invalidate0(&mut self, offset: NodeNum) -> Result<(), S::IoError> {
        let full_blocks = full_blocks(self.0.data_len(), self.0.block_level());
        let len = NodeNum(full_blocks.0 * 2);
        let mut offset = offset;
        loop {
            let range = range(offset);
            if range.end > len {
                self.0.set_hash(offset, None)?;
            }
            if let Some(x) = right_descendant(offset, self.0.tree_len()) {
                offset = x;
            } else {
                break;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    /// produce a blake3 outboard for the entire data
    ///
    /// returns TraversalError::Unavailable if the required hashes are not fully available
    /// returns TraversalError::Io if there is an IO error
    /// returns Ok(outboard) if the outboard was successfully produced
    pub(crate) fn outboard(&self) -> Result<Vec<u8>, TraversalError<S::IoError>> {
        let outboard_len = ByteNum((self.0.leafs().0 - 1) * 64) + 8;
        let mut res = Vec::with_capacity(outboard_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.0.data_len().0.to_le_bytes());
        self.outboard0(self.0.root(), &mut res)?;
        debug_assert_eq!(res.len() as u64, outboard_len);
        Ok(res)
    }

    #[cfg(test)]
    fn outboard0(
        &self,
        offset: NodeNum,
        target: &mut Vec<u8>,
    ) -> Result<(), TraversalError<S::IoError>> {
        if let Some((l, r)) = descendants(offset, self.0.tree_len()) {
            let lh = self
                .0
                .get_hash(l)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            let rh = self
                .0
                .get_hash(r)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.outboard0(l, target)?;
            self.outboard0(r, target)?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// produce a blake3 encoding for the entire data
    ///
    /// returns TraversalError::Unavailable if the required hashes or data are not fully available
    /// returns TraversalError::Io if there is an IO error
    /// returns Ok(encoded) if the outboard was successfully produced
    pub(crate) fn encode(&self) -> Result<Vec<u8>, TraversalError<S::IoError>> {
        let encoded_len = ByteNum((self.0.leafs().0 - 1) * 64) + self.0.data_len() + 8;
        let mut res = Vec::with_capacity(encoded_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.0.data_len().0.to_le_bytes());
        self.encode0(self.0.root(), &mut res)?;
        debug_assert_eq!(res.len() as u64, encoded_len);
        Ok(res)
    }

    #[cfg(test)]
    fn encode0(
        &self,
        offset: NodeNum,
        target: &mut Vec<u8>,
    ) -> Result<(), TraversalError<S::IoError>> {
        let store = &self.0;
        if let Some((l, r)) = descendants(offset, self.0.tree_len()) {
            let lh = store
                .get_hash(l)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            let rh = store
                .get_hash(r)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.encode0(l, target)?;
            self.encode0(r, target)?;
        } else {
            let index = index(offset);
            let slice = store
                .get_block(index)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(slice);
        }
        Ok(())
    }

    /// fill holes in our hashes as much as possible from either the data or lower hashes
    fn rehash(&mut self) -> Result<(), S::IoError> {
        self.rehash0(self.0.root(), true)
    }

    fn rehash0(&mut self, offset: NodeNum, is_root: bool) -> Result<(), S::IoError> {
        assert!(offset < self.0.tree_len());
        if self.0.get_hash(offset)?.is_none() {
            if let Some((l, r)) = descendants(offset, self.0.tree_len()) {
                self.rehash0(l, false)?;
                self.rehash0(r, false)?;
                if let (Some(left_child), Some(right_child)) =
                    (self.0.get_hash(l)?, self.0.get_hash(r)?)
                {
                    let hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
                    self.0.set_hash(offset, Some(hash))?;
                }
            } else {
                // rehash from data
                let index = index(offset);
                match self.0.get_block(index)? {
                    Some(data) => {
                        let hash = hash_block(index, data, self.0.block_level(), is_root);
                        self.0.set_hash(offset, Some(hash))?;
                    }
                    None => {
                        // nothing to do
                        //
                        // we don't clear the hash here.
                        // If we have the hash but not the data, we want to keep the hash.
                    }
                }
            }
        } else {
            // nothing to do
        }
        Ok(())
    }

    /// return an iterator that produces a verifiable encoding of the data in the given range
    pub(crate) fn slice_iter(&self, byte_range: Range<ByteNum>) -> SliceIter<'_, S> {
        let offset_range = node_range(byte_range, self.0.block_level());
        SliceIter {
            store: &self.0,
            offset_range,
            stack: vec![self.0.root()],
            emit: Some(SliceIterItem::Header(self.0.data_len().0)),
        }
    }

    fn add_from_slice_0(
        &mut self,
        offset: NodeNum,
        offset_range: &Range<NodeNum>,
        reader: &mut impl Read,
        buffer: &mut [u8],
        is_root: bool,
    ) -> Result<(), AddSliceError<S::IoError>> {
        use AddSliceError as E;
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Ok(());
        }
        if let Some((l, r)) = descendants(offset, self.0.tree_len()) {
            let mut lh = [0u8; 32];
            let mut rh = [0u8; 32];
            reader.read_exact(&mut lh).map_err(E::Io)?;
            reader.read_exact(&mut rh).map_err(E::Io)?;
            let left_child = lh.into();
            let right_child = rh.into();
            let expected_hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
            self.validate(offset, expected_hash)
                .map_err(E::Validation)?;
            self.set_or_validate(l, left_child).map_err(E::Validation)?;
            self.set_or_validate(r, right_child)
                .map_err(E::Validation)?;
            self.add_from_slice_0(l, offset_range, reader, buffer, false)?;
            self.add_from_slice_0(r, offset_range, reader, buffer, false)?;
        } else {
            let index = index(offset);
            let leaf_byte_range = self.0.leaf_byte_range(index);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            assert!(len.to_usize() <= buffer.len(), "leaf too big");
            reader
                .read_exact(&mut buffer[0..len.to_usize()])
                .map_err(E::Io)?;
            let expected_hash = hash_block(
                index,
                &buffer[..len.to_usize()],
                self.0.block_level(),
                is_root,
            );
            self.validate(offset, expected_hash)
                .map_err(E::Validation)?;
            self.0
                .set_block(index, Some(&buffer[..len.to_usize()]))
                .map_err(E::LocalIo)?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// clear all hashes except the root
    ///
    /// the data is unchanged
    pub(crate) fn clear(&mut self) -> Result<(), S::IoError> {
        for i in 0..self.0.tree_len().0 {
            if self.0.root() != i {
                self.0.set_hash(NodeNum(i), None)?;
            }
        }
        Ok(())
    }
}

pub(crate) struct SliceIter<'a, S: SyncStore> {
    /// the store
    store: &'a S,
    /// the range of offsets to visit
    offset_range: Range<NodeNum>,
    /// stack of offsets to visit
    stack: Vec<NodeNum>,
    /// if Some, this is something to emit immediately
    emit: Option<SliceIterItem<'a>>,
}

pub(crate) enum SliceIterItem<'a> {
    /// header containing the full size of the data from which this slice originates
    Header(u64),
    /// a hash
    Hash(blake3::Hash),
    /// data reference
    Data(&'a [u8]),
}

impl<'a> std::fmt::Debug for SliceIterItem<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SliceIterItem::Hash(h) => write!(f, "Hash({})", h),
            SliceIterItem::Data(d) => write!(f, "Data(len={})", d.len()),
            SliceIterItem::Header(h) => write!(f, "Header({})", h),
        }
    }
}

impl<'a> SliceIterItem<'a> {
    pub fn copy_to(&self, target: &mut [u8]) {
        match self {
            SliceIterItem::Hash(h) => target.copy_from_slice(h.as_bytes()),
            SliceIterItem::Data(d) => target.copy_from_slice(d),
            SliceIterItem::Header(h) => target.copy_from_slice(&h.to_le_bytes()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SliceIterItem::Header(_) => 8,
            SliceIterItem::Hash(_) => 32,
            SliceIterItem::Data(d) => d.len(),
        }
    }

    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = vec![0u8; self.len()];
        self.copy_to(&mut res);
        res
    }
}

/// a reader that reads from a slice iter
///
/// this serves as an adapter from the types SliceIter to just a stream of bytes
pub struct SliceReader<'a, S: SyncStore> {
    iter: SliceIter<'a, S>,
    buffer: Vec<u8>,
    start: usize,
    end: usize,
}

impl<'a, S: SyncStore> Read for SliceReader<'a, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.start >= self.end {
                match self.iter.next() {
                    Some(Ok(item)) => {
                        self.start = 0;
                        self.end = item.len();
                        item.copy_to(&mut self.buffer[..self.end]);
                    }
                    Some(Err(cause)) => {
                        // finish the iterator so if somebody calls read again
                        // it will indicate termination by returning 0 bytes read
                        self.iter.finish();
                        // produce a good error - distinguish between io errors
                        // and data unavailability
                        break Err(match cause {
                            TraversalError::Io(e) => io::Error::new(
                                io::ErrorKind::Other,
                                format!("io error accessing the data: {:?}", e),
                            ),
                            TraversalError::Unavailable => {
                                io::Error::new(io::ErrorKind::Other, format!("data unavailable"))
                            }
                        });
                    }
                    None => {
                        // iterator is done and buffer is empty, so signal EOF
                        break Ok(0);
                    }
                }
            }
            // when we get here we have data in the buffer, so n won't be 0
            let n = buf.len().min(self.end - self.start);
            // copy the data from the buffer to the output
            // this is safe because we know the buffer is at least as big as n
            buf[..n].copy_from_slice(&self.buffer[self.start..self.start + n]);
            // advance the start pointer
            self.start += n;
            // return the number of read bytes
            break Ok(n);
        }
    }
}

impl<'a, T: SyncStore> SliceIter<'a, T> {
    fn next0(&mut self) -> Result<SliceIterItem<'a>, TraversalResult<T::IoError>> {
        use TraversalResult as E;
        loop {
            if let Some(emit) = self.emit.take() {
                break Ok(emit);
            }
            let offset = self.stack.pop().ok_or(TraversalResult::Done)?;
            let range = range(offset);
            // if the range of this node is entirely outside the slice, we can skip it
            if range.end <= self.offset_range.start || range.start >= self.offset_range.end {
                continue;
            }
            if let Some((l, r)) = descendants(offset, self.store.tree_len()) {
                // r comes second, so we push it first
                self.stack.push(r);
                // l comes first, so we push it second
                self.stack.push(l);
                let lh = self
                    .store
                    .get_hash(l)
                    .map_err(E::IoError)?
                    .ok_or(E::Unavailable)?;
                let rh = self
                    .store
                    .get_hash(r)
                    .map_err(E::IoError)?
                    .ok_or(E::Unavailable)?;
                // rh comes second, so we put it into emit
                self.emit = Some(SliceIterItem::Hash(rh));
                // lh comes first, so we return it immediately
                break Ok(SliceIterItem::Hash(lh));
            } else {
                let slice = self
                    .store
                    .get_block(index(offset))
                    .map_err(E::IoError)?
                    .ok_or(E::Unavailable)?;
                break Ok(SliceIterItem::Data(slice));
            }
        }
    }

    /// finish the iterator, so that it will only ever return None from now on
    fn finish(&mut self) {
        self.stack.clear();
        self.emit = None;
    }
}

impl<'a, T: SyncStore> Iterator for SliceIter<'a, T> {
    type Item = Result<SliceIterItem<'a>, TraversalError<T::IoError>>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.next0();
        match res {
            Ok(item) => Some(Ok(item)),
            Err(TraversalResult::Done) => None,
            Err(TraversalResult::Unavailable) => {
                self.finish();
                Some(Err(TraversalError::Unavailable))
            }
            Err(TraversalResult::IoError(e)) => {
                self.finish();
                Some(Err(TraversalError::Io(e)))
            }
        }
    }
}

impl<'a, T: SyncStore> FusedIterator for SliceIter<'a, T> {}

struct SyncFileStore<P> {
    root: P,
    block_level: BlockLevel,
    tree: memmap2::MmapMut,
    tree_bitmap: memmap2::MmapMut,
    blocks: memmap2::MmapMut,
    block_bitmap: memmap2::MmapMut,
}

impl<P: AsRef<Path>> SyncFileStore<P> {
    fn leaf_byte_range_usize(&self, index: BlockNum) -> Range<usize> {
        let range = self.leaf_byte_range(index);
        range.start.to_usize()..range.end.to_usize()
    }
}

impl<P: AsRef<Path>> StoreCommon for SyncFileStore<P> {
    fn block_level(&self) -> BlockLevel {
        self.block_level
    }

    fn data_len(&self) -> ByteNum {
        ByteNum(self.blocks.len() as u64)
    }

    fn tree_len(&self) -> NodeNum {
        let len = self.tree.len();
        assert!(len % 32 == 0);
        NodeNum(len as u64 / 32)
    }
}

impl<P: AsRef<Path>> SyncStore for SyncFileStore<P> {
    type IoError = io::Error;

    fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError> {
        assert!(offset < self.tree_len());
        let offset = offset.0 as usize;
        let have = self.tree_bitmap[offset] != 0;
        if have {
            let base = offset * 32;
            let hash: [u8; 32] = self.tree[base..base + 32].try_into().unwrap();
            Ok(Some(blake3::Hash::from(hash)))
        } else {
            Ok(None)
        }
    }

    fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError> {
        assert!(offset < self.tree_len());
        let offset = offset.0 as usize;
        let base = offset * 32;
        if let Some(hash) = hash {
            self.tree_bitmap[offset] = 1;
            self.tree[base..base + 32].copy_from_slice(hash.as_bytes());
        } else {
            self.tree_bitmap[offset] = 0;
            self.tree[base..base + 32].fill(0);
        }
        Ok(())
    }

    fn get_block(&self, block: BlockNum) -> Result<Option<&[u8]>, Self::IoError> {
        assert!(block < self.block_count());
        let offset = block.0 as usize;
        let have = self.block_bitmap[offset] != 0;
        if have {
            let range = self.leaf_byte_range_usize(block);
            let slice = &self.blocks[range];
            Ok(Some(slice))
        } else {
            Ok(None)
        }
    }

    fn set_block(&mut self, block: BlockNum, data: Option<&[u8]>) -> Result<(), Self::IoError> {
        assert!(block < self.block_count());
        let offset = block.0 as usize;
        let range = self.leaf_byte_range_usize(block);
        if let Some(data) = data {
            self.block_bitmap[offset] = 1;
            self.blocks[range].copy_from_slice(data);
        } else {
            self.block_bitmap[offset] = 0;
            self.blocks[range].fill(0);
        }
        Ok(())
    }

    fn empty(block_level: BlockLevel) -> Result<Self, Self::IoError> {
        todo!()
    }

    fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError> {
        todo!()
    }
}
