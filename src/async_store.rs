use async_recursion::async_recursion;
use async_stream::stream;
use bytes::Bytes;
use std::{
    io::{self, Read},
    iter::FusedIterator,
    ops::Range,
};
use tokio::io::{AsyncRead, AsyncReadExt};

use async_trait::async_trait;
use futures::{future::BoxFuture, stream::FusedStream, FutureExt, Stream, TryFutureExt};

use crate::{tree::*, BlakeFile};

/// Interface for a synchronous store
///
/// This includs just methods that have to be implemented
#[async_trait]
pub trait AsyncStore: Sized + Send + Sync + 'static {
    /// the type of io error when interacting with the store
    ///
    /// for an in-memory store, this is can be infallible
    type IoError: std::fmt::Debug + Send + Sync + 'static;

    /// length of the stored data
    fn data_len(&self) -> ByteNum;

    /// block level
    fn block_level(&self) -> BlockLevel;

    /// length of the tree in nodes
    fn tree_len(&self) -> NodeNum;

    /// offset of the root hash
    fn root(&self) -> NodeNum {
        root(self.leafs())
    }

    /// number of leaf hashes in our tree
    ///
    /// will return 1 for empty data, since even empty data has a root hash
    fn leafs(&self) -> BlockNum {
        leafs(self.tree_len())
    }

    /// get a hash from the merkle tree, with existence check
    ///
    /// will panic if the offset is out of bounds
    async fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError>;

    /// set or clear a hash in the merkle tree
    ///
    /// will panic if the offset is out of bounds
    async fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError>;

    /// get a block of data
    ///
    /// this will be the block size for all blocks except the last one, which may be smaller
    ///
    /// will panic if the offset is out of bounds
    async fn get_block(&self, block: BlockNum) -> Result<Option<Bytes>, Self::IoError>;

    /// set or clear a block of data
    ///
    /// when setting data, the length must match the block size except for the last block,
    /// for which it must match the remainder of the data length
    ///
    /// will panic if the offset is out of bounds
    async fn set_block(
        &mut self,
        block: BlockNum,
        data: Option<Bytes>,
    ) -> Result<(), Self::IoError>;

    /// new empty store with the given block level
    ///
    /// the store will be initialized with the given block level, but no data.
    /// the hash will be set to the hash of the empty slice.
    ///
    /// note that stores with different block levels are not compatible.
    async fn empty(block_level: BlockLevel) -> Self;

    /// grow the store to the given length
    ///
    /// this will grow the tree and the data, but not invalidate the hashes that
    /// are no longer correct, so it will leave the store in an inconsistent state.
    ///
    /// Use grow to grow the store and invalidate the hashes.
    async fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError>;

    fn block_count(&self) -> BlockNum {
        blocks(self.data_len(), self.block_level())
    }

    /// byte range for a given offset
    fn leaf_byte_range(&self, index: BlockNum) -> Range<ByteNum> {
        let start = index.to_bytes(self.block_level());
        let end = (index + 1)
            .to_bytes(self.block_level())
            .min(self.data_len().max(start));
        start..end
    }
}

pub struct AsyncBlakeFile<S>(S);

impl<S: AsyncStore> AsyncBlakeFile<S> {
    pub async fn empty(block_level: BlockLevel) -> Self {
        let store = S::empty(block_level).await;
        Self(store)
    }

    /// create a new completely initialized store from a slice of data
    pub async fn new(data: Bytes, block_level: BlockLevel) -> Result<Self, S::IoError> {
        let mut res = Self::empty(block_level).await;
        res.grow(ByteNum(data.len() as u64)).await?;
        for (block, chunk) in data.chunks(res.block_size().to_usize()).enumerate() {
            let chunk = data.slice_ref(chunk);
            res.0.set_block(BlockNum(block as u64), Some(chunk)).await?;
        }
        res.rehash().await?;
        Ok(res)
    }

    /// return a stream that produces a verifiable encoding of the data in the given range
    fn slice_stream(
        &self,
        byte_range: Range<ByteNum>,
    ) -> impl Stream<Item = Result<SliceStreamItem, TraversalResult<S::IoError>>> + '_ {
        use SliceStreamItem as I;
        use TraversalResult as E;
        let offset_range = node_range(byte_range, self.block_level());
        let mut stack = vec![self.root()];
        stream! {
            // emit the header first
            yield Ok(SliceStreamItem::Header(self.data_len().0));
            while let Some(offset) = stack.pop() {
                let range = range(offset);
                // if the range of this node is entirely outside the slice, we can skip it
                if range.end <= offset_range.start || range.start >= offset_range.end {
                    continue;
                }
                if let Some((l, r)) = descendants(offset, self.0.tree_len()) {
                    // r comes second, so we push it first
                    stack.push(r);
                    // l comes first, so we push it second
                    stack.push(l);
                    let lh = self
                        .0
                        .get_hash(l).await
                        .map_err(E::IoError)?
                        .ok_or(E::Unavailable)?;
                    let rh = self
                        .0
                        .get_hash(r).await
                        .map_err(E::IoError)?
                        .ok_or(E::Unavailable)?;
                    yield Ok(I::Hash(lh));
                    yield Ok(I::Hash(rh));
                } else {
                    let slice = self
                         .0
                         .get_block(index(offset)).await
                         .map_err(E::IoError)?
                         .ok_or(E::Unavailable)?;
                    yield Ok(I::Data(slice));
                }
            }
        }
    }

    /// grow the store to the given length and update the hashes
    async fn grow(&mut self, new_len: ByteNum) -> Result<(), S::IoError> {
        if new_len < self.data_len() {
            panic!("shrink not allowed");
        }
        if new_len == self.data_len() {
            return Ok(());
        }
        // clear the last leaf hash
        // todo: we only have to do this if it was not full, but we do it always for now
        self.0.set_hash(self.tree_len() - 1, None).await?;
        // clear all non leaf hashes
        // todo: this is way too much. we should only clear the hashes that are affected by the new data
        for i in (1..self.tree_len().0).step_by(2) {
            self.0.set_hash(NodeNum(i), None).await?;
        }
        self.0.grow_storage(new_len).await?;
        Ok(())
    }

    /// fill holes in our hashes as much as possible from either the data or lower hashes
    async fn rehash(&mut self) -> Result<(), S::IoError> {
        self.rehash0(self.root(), true).await
    }

    #[async_recursion]
    async fn rehash0(&mut self, offset: NodeNum, is_root: bool) -> Result<(), S::IoError> {
        assert!(offset < self.tree_len());
        if self.0.get_hash(offset).await?.is_none() {
            if let Some((l, r)) = descendants(offset, self.tree_len()) {
                self.rehash0(l, false).await?;
                self.rehash0(r, false).await?;
                if let (Some(left_child), Some(right_child)) =
                    (self.0.get_hash(l).await?, self.0.get_hash(r).await?)
                {
                    let hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
                    self.0.set_hash(offset, Some(hash)).await?;
                }
            } else {
                // rehash from data
                let index = index(offset);
                match self.0.get_block(index).await? {
                    Some(data) => {
                        let hash = hash_block(index, &data, self.block_level(), is_root);
                        self.0.set_hash(offset, Some(hash)).await?;
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

    /// add a slice of data to the store
    ///
    /// returns
    /// - AddSliceError::WrongLength if the length of the data does not match the length of the store
    /// - AddSliceError::Io if there is an IO error
    /// - AddSliceError::LocalIo if there is a local IO error reading or writing the hashes or the data
    /// - AddSliceError::Validation if the data does not match the hashes
    /// - Ok(()) if the slice was successfully added
    pub async fn add_from_slice(
        &mut self,
        byte_range: Range<ByteNum>,
        reader: &mut (impl AsyncRead + Unpin + Send + 'static),
    ) -> Result<(), AddSliceError<S::IoError>> {
        use AddSliceError as E;
        let len = reader.read_u64_le().await.map_err(E::Io)?;
        if len != self.0.data_len() {
            return Err(E::WrongLength(len));
        }
        let offset_range = node_range(byte_range, self.0.block_level());
        let mut buffer = vec![0u8; block_size(self.0.block_level()).to_usize()];
        self.add_from_slice_0(self.0.root(), &offset_range, reader, &mut buffer, true)
            .await?;
        Ok(())
    }

    #[async_recursion]
    async fn add_from_slice_0(
        &mut self,
        offset: NodeNum,
        offset_range: &Range<NodeNum>,
        reader: &mut (impl AsyncRead + Unpin + Send + 'static),
        buffer: &mut [u8],
        is_root: bool,
    ) -> Result<(), AddSliceError<S::IoError>> {
        use AddSliceError as E;
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Ok(());
        }
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let mut lh = [0u8; 32];
            let mut rh = [0u8; 32];
            reader.read_exact(&mut lh).await.map_err(E::Io)?;
            reader.read_exact(&mut rh).await.map_err(E::Io)?;
            let left_child = lh.into();
            let right_child = rh.into();
            let expected_hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
            self.validate(offset, expected_hash)
                .await
                .map_err(E::Validation)?;
            self.set_or_validate(l, left_child)
                .await
                .map_err(E::Validation)?;
            self.set_or_validate(r, right_child)
                .await
                .map_err(E::Validation)?;
            self.add_from_slice_0(l, offset_range, reader, buffer, false)
                .await?;
            self.add_from_slice_0(r, offset_range, reader, buffer, false)
                .await?;
        } else {
            let index = index(offset);
            let leaf_byte_range = self.0.leaf_byte_range(index);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            assert!(len.to_usize() <= buffer.len(), "leaf too big");
            reader
                .read_exact(&mut buffer[0..len.to_usize()])
                .await
                .map_err(E::Io)?;
            let expected_hash = hash_block(
                index,
                &buffer[..len.to_usize()],
                self.block_level(),
                is_root,
            );
            self.validate(offset, expected_hash)
                .await
                .map_err(E::Validation)?;
            self.0
                .set_block(index, Some(buffer[..len.to_usize()].to_vec().into()))
                .await
                .map_err(E::LocalIo)?;
        }
        Ok(())
    }

    /// validate a node in the tree, with bounds check
    async fn validate(
        &self,
        offset: NodeNum,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<S::IoError>> {
        match self.0.get_hash(offset).await.map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(h) => Err(ValidateError::HashMismatch(offset)),
            None => Err(ValidateError::MissingHash(offset)),
        }
    }

    /// set or validate a node in the tree, with bounds check
    async fn set_or_validate(
        &mut self,
        offset: NodeNum,
        hash: blake3::Hash,
    ) -> Result<(), ValidateError<S::IoError>> {
        match self.0.get_hash(offset).await.map_err(ValidateError::Io)? {
            Some(h) if h == hash => Ok(()),
            Some(h) => Err(ValidateError::HashMismatch(offset)),
            None => {
                self.0
                    .set_hash(offset, Some(hash))
                    .await
                    .map_err(ValidateError::Io)?;
                Ok(())
            }
        }
    }

    /// number of leaf hashes in our tree
    ///
    /// will return 1 for empty data, since even empty data has a root hash
    fn leafs(&self) -> BlockNum {
        leafs(self.tree_len())
    }

    fn root(&self) -> NodeNum {
        root(self.leafs())
    }

    fn tree_len(&self) -> NodeNum {
        self.0.tree_len()
    }

    fn data_len(&self) -> ByteNum {
        self.0.data_len()
    }

    fn block_level(&self) -> BlockLevel {
        self.0.block_level()
    }

    fn block_size(&self) -> ByteNum {
        block_size(self.block_level())
    }

    fn block_count(&self) -> BlockNum {
        blocks(self.data_len(), self.block_level())
    }
}

pub enum SliceStreamItem {
    /// header containing the full size of the data from which this slice originates
    Header(u64),
    /// a hash
    Hash(blake3::Hash),
    /// data reference
    Data(Bytes),
}

impl std::fmt::Debug for SliceStreamItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hash(h) => write!(f, "Hash({})", h),
            Self::Data(d) => write!(f, "Data(len={})", d.len()),
            Self::Header(h) => write!(f, "Header({})", h),
        }
    }
}

impl SliceStreamItem {
    pub fn copy_to(&self, target: &mut [u8]) {
        match self {
            Self::Hash(h) => target.copy_from_slice(h.as_bytes()),
            Self::Data(d) => target.copy_from_slice(d),
            Self::Header(h) => target.copy_from_slice(&h.to_le_bytes()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Header(_) => 8,
            Self::Hash(_) => 32,
            Self::Data(d) => d.len(),
        }
    }

    #[cfg(test)]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut res = vec![0u8; self.len()];
        self.copy_to(&mut res);
        res
    }
}

#[derive(Debug)]
pub enum TraversalError<IoError> {
    Io(IoError),
    Unavailable,
}

#[derive(Debug)]
enum TraversalResult<T> {
    IoError(T),
    Unavailable,
    Done,
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
    HashMismatch(NodeNum),
    MissingHash(NodeNum),
}

pub struct VecAsyncStore {
    block_level: BlockLevel,
    tree: Vec<blake3::Hash>,
    tree_bitmap: Vec<bool>,
    data: Vec<u8>,
    data_bitmap: Vec<bool>,
}

impl VecAsyncStore {
    fn leaf_byte_range_usize(&self, index: BlockNum) -> Range<usize> {
        let range = self.leaf_byte_range(index);
        range.start.to_usize()..range.end.to_usize()
    }
}

#[async_trait]
impl AsyncStore for VecAsyncStore {
    type IoError = std::convert::Infallible;
    fn tree_len(&self) -> NodeNum {
        NodeNum(self.tree.len() as u64)
    }
    async fn get_hash(&self, offset: NodeNum) -> Result<Option<blake3::Hash>, Self::IoError> {
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
    async fn set_hash(
        &mut self,
        offset: NodeNum,
        hash: Option<blake3::Hash>,
    ) -> Result<(), Self::IoError> {
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
    fn data_len(&self) -> ByteNum {
        ByteNum(self.data.len() as u64)
    }
    fn block_level(&self) -> BlockLevel {
        self.block_level
    }
    async fn get_block(&self, block: BlockNum) -> Result<Option<Bytes>, Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        Ok(if self.data_bitmap[offset] {
            let range = self.leaf_byte_range_usize(block);
            Some(self.data[range].to_vec().into())
        } else {
            None
        })
    }
    async fn set_block(
        &mut self,
        block: BlockNum,
        data: Option<Bytes>,
    ) -> Result<(), Self::IoError> {
        if block > self.block_count() {
            panic!();
        }
        let offset = block.to_usize();
        let range = self.leaf_byte_range_usize(block);
        if let Some(data) = data {
            self.data_bitmap[offset] = true;
            self.data[range].copy_from_slice(&data);
        } else {
            self.data_bitmap[offset] = false;
            self.data[range].fill(0);
        }
        Ok(())
    }
    async fn empty(block_level: BlockLevel) -> Self {
        let tree_len_usize = 1;
        Self {
            tree: vec![empty_root_hash(); tree_len_usize],
            tree_bitmap: vec![true; tree_len_usize],
            block_level,
            data: Vec::new(),
            data_bitmap: Vec::new(),
        }
    }
    async fn grow_storage(&mut self, new_len: ByteNum) -> Result<(), Self::IoError> {
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
