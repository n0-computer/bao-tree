use std::{io::Read, iter::FusedIterator, ops::Range};

use crate::tree::*;

pub trait SyncStore: Sized {
    type IoError;
    /// length of the tree in nodes
    fn tree_len(&self) -> Nodes;
    /// get a node from the tree, with existence check and bounds check
    fn get(&self, offset: Nodes) -> Result<Option<blake3::Hash>, Self::IoError>;
    /// set or clear a node in the tree, with bounds check
    fn set(&mut self, offset: Nodes, hash: Option<blake3::Hash>) -> Result<(), Self::IoError>;

    /// length of the stored data
    fn data_len(&self) -> Bytes;
    /// block level
    fn block_level(&self) -> BlockLevel;
    /// get a block of data.
    fn get_block(&self, block: Blocks) -> Result<Option<&[u8]>, Self::IoError>;
    /// set a block of data
    fn set_block(&mut self, block: Blocks, data: Option<&[u8]>) -> Result<(), Self::IoError>;

    /// new empty store with the block level
    fn empty(block_level: BlockLevel) -> Self;

    /// grow the store to the given length
    ///
    /// this will grow the tree and the data, but not invalidate the hashes that
    /// are no longer correct, so it will leave the store in an inconsistent state.
    ///
    /// Use grow to grow the store and invalidate the hashes.
    fn grow_storage(&mut self, new_len: Bytes) -> Result<(), Self::IoError>;
}

pub trait SyncStoreExt: SyncStore {}

#[derive(Debug)]
pub enum TraversalError<IoError> {
    Io(IoError),
    Unavailable,
}

pub enum AddSliceError<IoError> {
    /// io error when reading from the slice
    Io(std::io::Error),
    /// io error when reading from or writing to the local store
    LocalIo(IoError),
    /// slice length does not match the expected length
    WrongLength(u64),
    /// hash validation failed
    Validation(ValidateError<IoError>),
}

pub enum ValidateError<IoError> {
    /// io error when reading from or writing to the local store
    Io(IoError),
    HashMismatch(Nodes),
    MissingHash(Nodes),
}

/// A bunch of useful methods for syncstores
pub(crate) trait SyncStoreUtil: SyncStore {
    /// set or validate a node in the tree, with bounds check
    fn set_or_validate(
        &mut self,
        offset: Nodes,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<Self::IoError>> {
        match self.get(offset).map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(h) => Err(ValidateError::HashMismatch(offset)),
            None => {
                self.set(offset, Some(hash)).map_err(ValidateError::Io)?;
                Ok(())
            }
        }
    }
    /// validate a node in the tree, with bounds check
    fn validate(
        &self,
        offset: Nodes,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<Self::IoError>> {
        match self.get(offset).map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(h) => Err(ValidateError::HashMismatch(offset)),
            None => Err(ValidateError::MissingHash(offset)),
        }
    }

    /// byte range for a given offset
    fn leaf_byte_range(&self, index: Blocks) -> Range<Bytes> {
        let start = index.to_bytes(self.block_level());
        let end = (index + 1)
            .to_bytes(self.block_level())
            .min(self.data_len().max(start));
        start..end
    }

    fn block_size(&self) -> Bytes {
        block_size(self.block_level())
    }

    fn block_count(&self) -> Blocks {
        blocks(self.data_len(), self.block_level())
    }

    /// number of leaf hashes in our tree
    ///
    /// will return 1 for empty data, since even empty data has a root hash
    fn leafs(&self) -> Blocks {
        leafs(self.tree_len())
    }

    /// offset of the root hash
    fn root(&self) -> Nodes {
        root(self.leafs())
    }

    /// the blake3 hash of the entire data
    fn hash(&self) -> Result<Option<blake3::Hash>, Self::IoError> {
        self.get(self.root())
    }

    /// the byte range the store covers
    fn byte_range(&self) -> Range<Bytes> {
        Bytes(0)..self.data_len()
    }

    /// grow the store to the given length and update the hashes
    fn grow(&mut self, new_len: Bytes) -> Result<(), Self::IoError> {
        if new_len < self.data_len() {
            panic!("shrink not allowed");
        }
        if new_len == self.data_len() {
            return Ok(());
        }
        // clear the last leaf hash
        // todo: we only have to do this if it was not full, but we do it always for now
        self.set(self.tree_len() - 1, None)?;
        // clear all non leaf hashes
        // todo: this is way too much. we should only clear the hashes that are affected by the new data
        for i in (1..self.tree_len().0).step_by(2) {
            self.set(Nodes(i), None)?;
        }
        self.grow_storage(new_len)?;
        Ok(())
    }

    fn new(data: &[u8], block_level: BlockLevel) -> Result<Self, Self::IoError> {
        let mut res = Self::empty(block_level);
        res.grow(Bytes(data.len() as u64))?;
        for (block, data) in data.chunks(res.block_size().to_usize()).enumerate() {
            res.set_block(Blocks(block as u64), Some(data))?;
        }
        res.rehash()?;
        Ok(res)
    }

    /// produce a blake3 outboard for the entire data
    ///
    /// returns TraversalError::Unavailable if the required hashes are not fully available
    /// returns TraversalError::Io if there is an IO error
    /// returns Ok(outboard) if the outboard was successfully produced
    fn outboard(&self) -> Result<Vec<u8>, TraversalError<Self::IoError>> {
        let outboard_len = Bytes((self.leafs().0 - 1) * 64) + 8;
        let mut res = Vec::with_capacity(outboard_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.data_len().0.to_le_bytes());
        self.outboard0(self.root(), &mut res)?;
        debug_assert_eq!(res.len() as u64, outboard_len);
        Ok(res)
    }

    fn outboard0(
        &self,
        offset: Nodes,
        target: &mut Vec<u8>,
    ) -> Result<(), TraversalError<Self::IoError>> {
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let lh = self
                .get(l)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            let rh = self
                .get(r)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.outboard0(l, target)?;
            self.outboard0(r, target)?;
        }
        Ok(())
    }

    /// produce a blake3 encoding for the entire data
    ///
    /// returns TraversalError::Unavailable if the required hashes or data are not fully available
    /// returns TraversalError::Io if there is an IO error
    /// returns Ok(encoded) if the outboard was successfully produced
    fn encode(&self) -> Result<Vec<u8>, TraversalError<Self::IoError>> {
        let encoded_len = Bytes((self.leafs().0 - 1) * 64) + self.data_len() + 8;
        let mut res = Vec::with_capacity(encoded_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.data_len().0.to_le_bytes());
        self.encode0(self.root(), &mut res)?;
        debug_assert_eq!(res.len() as u64, encoded_len);
        Ok(res)
    }

    fn encode0(
        &self,
        offset: Nodes,
        target: &mut Vec<u8>,
    ) -> Result<(), TraversalError<Self::IoError>> {
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let lh = self
                .get(l)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            let rh = self
                .get(r)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.encode0(l, target)?;
            self.encode0(r, target)?;
        } else {
            let index = index(offset);
            let slice = self
                .get_block(index)
                .map_err(TraversalError::Io)?
                .ok_or(TraversalError::Unavailable)?;
            target.extend_from_slice(slice);
        }
        Ok(())
    }

    /// fill holes in our hashes as much as possible from either the data or lower hashes
    fn rehash(&mut self) -> Result<(), Self::IoError> {
        self.rehash0(self.root(), true)
    }

    fn rehash0(&mut self, offset: Nodes, is_root: bool) -> Result<(), Self::IoError> {
        assert!(offset < self.tree_len());
        if self.get(offset)?.is_none() {
            if let Some((l, r)) = descendants(offset, self.tree_len()) {
                self.rehash0(l, false)?;
                self.rehash0(r, false)?;
                if let (Some(left_child), Some(right_child)) = (self.get(l)?, self.get(r)?) {
                    let hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
                    self.set(offset, Some(hash))?;
                }
            } else {
                // rehash from data
                let index = index(offset);
                match self.get_block(index)? {
                    Some(data) => {
                        let hash = hash_block(index, data, self.block_level(), is_root);
                        self.set(offset, Some(hash))?;
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

    fn slice_iter(&self, byte_range: Range<Bytes>) -> SliceIter<'_, Self> {
        let offset_range = node_range(byte_range, self.block_level());
        SliceIter {
            store: self,
            offset_range,
            stack: vec![self.root()],
            emit: Some(SliceIterItem::Header(self.data_len().0)),
        }
    }

    fn add_from_slice(
        &mut self,
        byte_range: Range<Bytes>,
        reader: &mut impl Read,
    ) -> Result<(), AddSliceError<Self::IoError>> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).map_err(AddSliceError::Io)?;
        let len = u64::from_le_bytes(buf);
        if len != self.data_len() {
            return Err(AddSliceError::WrongLength(len));
        }
        let offset_range = node_range(byte_range, self.block_level());
        let mut buffer = vec![0u8; block_size(self.block_level()).to_usize()];
        self.add_from_slice_0(self.root(), &offset_range, reader, &mut buffer, true)?;
        Ok(())
    }

    fn add_from_slice_0(
        &mut self,
        offset: Nodes,
        offset_range: &Range<Nodes>,
        reader: &mut impl Read,
        buffer: &mut [u8],
        is_root: bool,
    ) -> Result<(), AddSliceError<Self::IoError>> {
        use AddSliceError as E;
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Ok(());
        }
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
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
            let leaf_byte_range = self.leaf_byte_range(index);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            assert!(len.to_usize() <= buffer.len(), "leaf too big");
            reader
                .read_exact(&mut buffer[0..len.to_usize()])
                .map_err(E::Io)?;
            let expected_hash = hash_block(
                index,
                &buffer[..len.to_usize()],
                self.block_level(),
                is_root,
            );
            self.validate(offset, expected_hash)
                .map_err(E::Validation)?;
            self.set_block(index, Some(&buffer[..len.to_usize()]))
                .map_err(E::LocalIo)?;
        }
        Ok(())
    }
}

impl<T: SyncStore> SyncStoreUtil for T {}

pub struct SliceIter<'a, S: SyncStore> {
    /// the store
    store: &'a S,
    /// the range of offsets to visit
    offset_range: Range<Nodes>,
    /// stack of offsets to visit
    stack: Vec<Nodes>,
    /// if Some, this is something to emit immediately
    emit: Option<SliceIterItem<'a>>,
}

pub enum SliceIterItem<'a> {
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
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            SliceIterItem::Hash(h) => h.as_bytes().to_vec(),
            SliceIterItem::Data(d) => d.to_vec(),
            SliceIterItem::Header(h) => h.to_le_bytes().to_vec(),
        }
    }
}

enum TraversalResult<T> {
    IoError(T),
    Unavailable,
    Done,
}

impl<'a, T: SyncStore> SliceIter<'a, T> {
    fn next0(&mut self) -> Result<SliceIterItem<'a>, TraversalResult<T::IoError>> {
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
                    .get(l)
                    .map_err(TraversalResult::IoError)?
                    .ok_or(TraversalResult::Unavailable)?;
                let rh = self
                    .store
                    .get(r)
                    .map_err(TraversalResult::IoError)?
                    .ok_or(TraversalResult::Unavailable)?;
                // rh comes second, so we put it into emit
                self.emit = Some(SliceIterItem::Hash(rh));
                // lh comes first, so we return it immediately
                break Ok(SliceIterItem::Hash(lh));
            } else {
                let slice = self
                    .store
                    .get_block(index(offset))
                    .map_err(TraversalResult::IoError)?
                    .ok_or(TraversalResult::Unavailable)?;
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

pub struct VecSyncStore {
    tree: Vec<blake3::Hash>,
    tree_bitmap: Vec<bool>,
    block_level: BlockLevel,
    data: Vec<u8>,
    data_bitmap: Vec<bool>,
}

impl VecSyncStore {
    fn leaf_byte_range_usize(&self, index: Blocks) -> Range<usize> {
        let range = self.leaf_byte_range(index);
        range.start.to_usize()..range.end.to_usize()
    }
}

impl SyncStore for VecSyncStore {
    type IoError = std::convert::Infallible;
    fn tree_len(&self) -> Nodes {
        Nodes(self.tree.len() as u64)
    }
    fn get(&self, offset: Nodes) -> Result<Option<blake3::Hash>, Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        Ok(if self.tree_bitmap[offset] {
            Some(self.tree[offset])
        } else {
            None
        })
    }
    fn set(&mut self, offset: Nodes, hash: Option<blake3::Hash>) -> Result<(), Self::IoError> {
        let offset = offset.to_usize();
        if offset >= self.tree.len() {
            panic!()
        }
        if let Some(hash) = hash {
            self.tree[offset] = hash;
            self.tree_bitmap[offset] = true;
        } else {
            self.tree[offset] = zero_hash();
            self.tree_bitmap[offset] = false;
        }
        Ok(())
    }
    fn data_len(&self) -> Bytes {
        Bytes(self.data.len() as u64)
    }
    fn block_level(&self) -> BlockLevel {
        self.block_level
    }
    fn get_block(&self, block: Blocks) -> Result<Option<&[u8]>, Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        Ok(if self.data_bitmap[offset] {
            let range = self.leaf_byte_range_usize(block);
            Some(&self.data[range])
        } else {
            None
        })
    }
    fn set_block(&mut self, block: Blocks, data: Option<&[u8]>) -> Result<(), Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        let range = self.leaf_byte_range_usize(block);
        if let Some(data) = data {
            self.data_bitmap[offset] = true;
            self.data[range].copy_from_slice(data);
        } else {
            self.data_bitmap[offset] = false;
            self.data[range].fill(0);
        }
        Ok(())
    }
    fn empty(block_level: BlockLevel) -> Self {
        let tree_len_usize = 1;
        VecSyncStore {
            tree: vec![empty_root_hash(); tree_len_usize],
            tree_bitmap: vec![true; tree_len_usize],
            block_level,
            data: Vec::new(),
            data_bitmap: Vec::new(),
        }
    }
    fn grow_storage(&mut self, new_len: Bytes) -> Result<(), Self::IoError> {
        if new_len < self.data_len() {
            panic!();
        }
        if new_len == self.data_len() {
            return Ok(());
        }
        let blocks = blocks(new_len, self.block_level());
        self.data.resize(new_len.to_usize(), 0u8);
        self.data_bitmap.resize(blocks.to_usize(), false);
        let new_tree_len = num_hashes(blocks);
        self.tree.resize(new_tree_len.to_usize(), zero_hash());
        self.tree_bitmap.resize(new_tree_len.to_usize(), false);
        Ok(())
    }
}
