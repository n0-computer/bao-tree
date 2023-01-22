use crate::tree::*;
use std::{io::Read, iter::FusedIterator, ops::Range};

/// Hash a blake3 chunk.
///
/// `chunk` is the chunk index, `data` is the chunk data, and `is_root` is true if this is the only chunk.
fn hash_chunk(chunk: Chunks, data: &[u8], is_root: bool) -> blake3::Hash {
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN);
    let mut hasher = blake3::guts::ChunkState::new(chunk.0);
    hasher.update(data);
    hasher.finalize(is_root)
}

/// Hash a block that is made of a power of two number of chunks.
///
/// `block` is the block index, `data` is the block data, `is_root` is true if this is the only block,
/// and `block_level` indicates how many chunks make up a block. Chunks = 2^level.
fn hash_block(block: Blocks, data: &[u8], block_level: BlockLevel, is_root: bool) -> blake3::Hash {
    // ensure that the data is not too big
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN << block_level.0);
    // compute the cunk number for the first chunk in this block
    let chunk0 = Chunks(block.0 << block_level.0);
    // simple recursive hash.
    // Note that this should really call in to blake3 hash_all_at_once, but
    // that is not exposed in the public API and also does not allow providing
    // the chunk.
    hash_block0(chunk0, data, block_level.0, is_root)
}

/// Recursive helper for hash_block.
fn hash_block0(chunk0: Chunks, data: &[u8], block_level: u32, is_root: bool) -> blake3::Hash {
    if block_level == 0 {
        // we have just a single chunk
        hash_chunk(chunk0, data, is_root)
    } else {
        // number of chunks at this level
        let chunks = 1 << block_level;
        // size corresponding to this level. Data must not be bigger than this.
        let size = blake3::guts::CHUNK_LEN << block_level;
        // mid point of the data
        let mid = size / 2;
        debug_assert!(data.len() <= size);
        if data.len() <= mid {
            // we don't subdivide, so we need to pass through the is_root flag
            hash_block0(chunk0, data, block_level - 1, is_root)
        } else {
            // block_level is > 0 here, so this is safe
            let child_level = block_level - 1;
            // data is larger than mid, so this is safe
            let l = &data[..mid];
            let r = &data[mid..];
            let chunkl = chunk0;
            let chunkr = chunk0 + chunks / 2;
            let l = hash_block0(chunkl, l, child_level, false);
            let r = hash_block0(chunkr, r, child_level, false);
            blake3::guts::parent_cv(&l, &r, is_root)
        }
    }
}

fn block_hashes_iter(
    data: &[u8],
    block_level: BlockLevel,
) -> impl Iterator<Item = (Blocks, blake3::Hash)> + '_ {
    let block_size = block_size(block_level);
    let is_root = data.len() <= block_size.to_usize();
    data.chunks(block_size.to_usize())
        .enumerate()
        .map(move |(i, data)| {
            let block = Blocks(i as u64);
            (block, hash_block(block, data, block_level, is_root))
        })
}

/// Given a range of bytes, returns a range of nodes that cover that range.
fn node_range(byte_range: Range<Bytes>, block_level: BlockLevel) -> Range<Nodes> {
    let block_size = block_size(block_level).0;
    let start_page = byte_range.start.0 / block_size;
    let end_page = (byte_range.end.0 + block_size - 1) / block_size;
    let start_offset = start_page * 2;
    let end_offset = end_page * 2;
    Nodes(start_offset)..Nodes(end_offset)
}

fn zero_hash() -> blake3::Hash {
    blake3::Hash::from([0u8; 32])
}

pub struct SparseOutboard {
    /// even offsets are leaf hashes, odd offsets are branch hashes
    tree: Vec<blake3::Hash>,
    /// occupancy bitmap for the tree
    bitmap: Vec<bool>,
    /// total length of the data
    len: Bytes,
    /// block level. 0 means blocks = chunks, aka bao style.
    block_level: BlockLevel,
}

pub struct SliceIter<'a> {
    /// the outboard
    outboard: &'a SparseOutboard,
    /// the data
    data: &'a [u8],
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

trait SyncStore {
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
}

/// A bunch of useful methods for syncstores
trait SyncStoreExt: SyncStore {
    /// set or validate a node in the tree, with bounds check
    fn set_or_validate(
        &mut self,
        offset: Nodes,
        hash: blake3::Hash,
    ) -> Result<anyhow::Result<()>, Self::IoError> {
        match self.get(offset)? {
            Some(h) if h == hash => Ok(Ok(())),
            Some(h) => Ok(Err(anyhow::anyhow!(
                "hash mismatch at offset {}: expected {}, got {}",
                offset.0,
                hash,
                h
            ))),
            None => {
                self.set(offset, Some(hash))?;
                Ok(Ok(()))
            }
        }
    }
    /// validate a node in the tree, with bounds check
    fn validate(
        &self,
        offset: Nodes,
        hash: blake3::Hash,
    ) -> Result<anyhow::Result<()>, Self::IoError> {
        match self.get(offset)? {
            Some(h) if h == hash => Ok(Ok(())),
            Some(h) => Ok(Err(anyhow::anyhow!(
                "hash mismatch at offset {}: expected {}, got {}",
                offset.0,
                hash,
                h
            ))),
            None => Ok(Err(anyhow::anyhow!("missing hash at offset {}", offset.0))),
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
}

impl<T: SyncStore> SyncStoreExt for T {}

struct VecSyncStore {
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
    fn to_vec(&self) -> Vec<u8> {
        match self {
            SliceIterItem::Hash(h) => h.as_bytes().to_vec(),
            SliceIterItem::Data(d) => d.to_vec(),
            SliceIterItem::Header(h) => h.to_le_bytes().to_vec(),
        }
    }
}

impl<'a> SliceIter<'a> {
    fn next0(&mut self) -> Option<SliceIterItem<'a>> {
        loop {
            if let Some(emit) = self.emit.take() {
                break Some(emit);
            }
            let offset = self.stack.pop()?;
            let range = range(offset);
            // if the range of this node is entirely outside the slice, we can skip it
            if range.end <= self.offset_range.start || range.start >= self.offset_range.end {
                continue;
            }
            if let Some((l, r)) = descendants(offset, self.outboard.tree_len()) {
                // r comes second, so we push it first
                self.stack.push(r);
                // l comes first, so we push it second
                self.stack.push(l);
                let lh = self.outboard.get(l)?;
                let rh = self.outboard.get(r)?;
                // rh comes second, so we put it into emit
                self.emit = Some(SliceIterItem::Hash(*rh));
                // lh comes first, so we return it immediately
                break Some(SliceIterItem::Hash(*lh));
            } else {
                let leaf_byte_range = self.outboard.leaf_byte_range_usize(offset);
                let slice = &self.data[leaf_byte_range];
                break Some(SliceIterItem::Data(slice));
            }
        }
    }
}

impl<'a> Iterator for SliceIter<'a> {
    type Item = SliceIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.next0();
        if res.is_none() {
            // make sure we stop after returning None
            self.stack.clear();
            self.emit.take();
        }
        res
    }
}

impl FusedIterator for SliceIter<'_> {}

impl SparseOutboard {
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
    pub fn hash(&self) -> Option<&blake3::Hash> {
        self.get_hash(self.root())
    }

    pub fn byte_range(&self) -> Range<Bytes> {
        Bytes(0)..self.len
    }

    /// produce a blake3 outboard for the entire data
    ///
    /// returns None if the required hashes are not fully available
    pub fn outboard(&self) -> Option<Vec<u8>> {
        let outboard_len = Bytes((self.leafs().0 - 1) * 64) + 8;
        let mut res = Vec::with_capacity(outboard_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.len.0.to_le_bytes());
        self.outboard0(self.root(), &mut res)?;
        debug_assert_eq!(res.len() as u64, outboard_len);
        Some(res)
    }

    /// the number of hashes in the tree
    fn tree_len(&self) -> Nodes {
        debug_assert!(self.tree.len() == self.bitmap.len());
        Nodes(self.tree.len() as u64)
    }

    fn outboard0(&self, offset: Nodes, target: &mut Vec<u8>) -> Option<()> {
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let lh = self.get(l)?;
            let rh = self.get(r)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.outboard0(l, target)?;
            self.outboard0(r, target)?;
        }
        Some(())
    }

    pub fn encode(&self, data: &[u8]) -> Option<Vec<u8>> {
        assert!(data.len() as u64 == self.len);
        let encoded_len = Bytes((self.leafs().0 - 1) * 64) + self.len + 8;
        let mut res = Vec::with_capacity(encoded_len.to_usize());
        // write the header - total length of the data
        res.extend_from_slice(&self.len.0.to_le_bytes());
        self.encode0(self.root(), data, &mut res)?;
        debug_assert_eq!(res.len() as u64, encoded_len);
        Some(res)
    }

    fn encode0(&self, offset: Nodes, data: &[u8], target: &mut Vec<u8>) -> Option<()> {
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let lh = self.get(l)?;
            let rh = self.get(r)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.encode0(l, data, target)?;
            self.encode0(r, data, target)?;
        } else {
            let index = index(offset);
            let start = index.to_bytes(self.block_level);
            let end = (index + 1).to_bytes(self.block_level).min(self.len);
            let slice = &data[start.to_usize()..end.to_usize()];
            target.extend_from_slice(slice);
        }
        Some(())
    }

    pub fn slice_iter<'a>(&'a self, data: &'a [u8], byte_range: Range<Bytes>) -> SliceIter<'a> {
        assert!(data.len() as u64 == self.len);
        let offset_range = node_range(byte_range, self.block_level);
        SliceIter {
            outboard: self,
            data,
            offset_range,
            stack: vec![self.root()],
            emit: Some(SliceIterItem::Header(self.len.0)),
        }
    }

    /// Compute a verifiable slice of the data
    pub fn slice(&self, data: &[u8], byte_range: Range<Bytes>) -> Option<Vec<u8>> {
        assert!(data.len() as u64 == self.len);
        let offset_range = node_range(byte_range, self.block_level);
        let mut res = Vec::new();
        // write the header - total length of the data
        res.extend_from_slice(&self.len.0.to_le_bytes());
        self.slice0(self.root(), data, &offset_range, &mut res)?;
        Some(res)
    }

    fn slice0(
        &self,
        offset: Nodes,
        data: &[u8],
        offset_range: &Range<Nodes>,
        target: &mut Vec<u8>,
    ) -> Option<()> {
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Some(());
        }
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let lh = self.get(l)?;
            let rh = self.get(r)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.slice0(l, data, offset_range, target)?;
            self.slice0(r, data, offset_range, target)?;
        } else {
            let leaf_byte_range = self.leaf_byte_range_usize(offset);
            let slice = &data[leaf_byte_range];
            target.extend_from_slice(slice);
        }
        Some(())
    }

    pub fn add_from_slice(
        &mut self,
        data: &mut [u8],
        byte_range: Range<Bytes>,
        reader: &mut impl Read,
    ) -> anyhow::Result<()> {
        assert!(data.len() as u64 == self.len);
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let len = u64::from_le_bytes(buf);
        anyhow::ensure!(len == self.len, "wrong length");
        let offset_range = node_range(byte_range, self.block_level);
        let mut buffer = vec![0u8; block_size(self.block_level).to_usize()];
        self.add_from_slice_0(self.root(), data, &offset_range, reader, &mut buffer, true)?;
        Ok(())
    }

    fn add_from_slice_0(
        &mut self,
        offset: Nodes,
        data: &mut [u8],
        offset_range: &Range<Nodes>,
        reader: &mut impl Read,
        buffer: &mut [u8],
        is_root: bool,
    ) -> anyhow::Result<()> {
        let block_size = block_size(self.block_level);
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Ok(());
        }
        if let Some((l, r)) = descendants(offset, self.tree_len()) {
            let mut lh = [0u8; 32];
            let mut rh = [0u8; 32];
            reader.read_exact(&mut lh)?;
            reader.read_exact(&mut rh)?;
            let left_child = lh.into();
            let right_child = rh.into();
            let expected_hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
            self.validate(offset, expected_hash)?;
            self.set_or_validate(l, left_child)?;
            self.set_or_validate(r, right_child)?;
            self.add_from_slice_0(l, data, offset_range, reader, buffer, false)?;
            self.add_from_slice_0(r, data, offset_range, reader, buffer, false)?;
        } else {
            let leaf_byte_range = self.leaf_byte_range(offset);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            anyhow::ensure!(len <= block_size, "leaf too big");
            reader.read_exact(&mut buffer[0..len.to_usize()])?;
            let expected_hash = hash_block(
                index(offset),
                &buffer[..len.to_usize()],
                self.block_level,
                is_root,
            );
            self.validate(offset, expected_hash)?;
            let leaf_byte_range = bo_range_to_usize(leaf_byte_range);
            data[leaf_byte_range].copy_from_slice(&buffer[..len.to_usize()]);
        }
        Ok(())
    }

    /// byte range for a given offset
    fn leaf_byte_range(&self, offset: Nodes) -> Range<Bytes> {
        let index = index(offset);
        let start = index.to_bytes(self.block_level);
        let end = (index + 1).to_bytes(self.block_level).min(self.len);
        start..end
    }

    fn leaf_byte_range_usize(&self, offset: Nodes) -> Range<usize> {
        let range = self.leaf_byte_range(offset);
        range.start.to_usize()..range.end.to_usize()
    }

    /// get the hash for the given offset, with bounds check and check if we have it
    fn get(&self, offset: Nodes) -> Option<&blake3::Hash> {
        if offset < self.tree_len() && self.has0(offset) {
            Some(self.get_hash0(offset))
        } else {
            None
        }
    }

    /// get the hash for the given offset, without bounds check
    fn set0(&mut self, offset: Nodes, hash: blake3::Hash) {
        self.bitmap[offset.to_usize()] = true;
        self.tree[offset.to_usize()] = hash;
    }

    /// set the hash for the given offset, or validate it
    fn set_or_validate(&mut self, offset: Nodes, hash: blake3::Hash) -> anyhow::Result<()> {
        anyhow::ensure!(offset < self.tree_len());
        if self.has0(offset) {
            println!("validating hash at {:?} {:?}", offset, level(offset));
            anyhow::ensure!(self.get_hash0(offset) == &hash, "hash mismatch");
        } else {
            println!("storing hash at {:?} {:?}", offset, level(offset));
            self.set0(offset, hash);
        }
        Ok(())
    }

    /// validate the hash for the given offset
    fn validate(&mut self, offset: Nodes, hash: blake3::Hash) -> anyhow::Result<()> {
        anyhow::ensure!(offset < self.tree_len());
        anyhow::ensure!(self.has0(offset), "hash not set");
        println!("validating hash at {:?} {:?}", offset, level(offset));
        anyhow::ensure!(self.get_hash0(offset) == &hash, "hash mismatch");
        Ok(())
    }

    /// create a new sparse outboard for the given data
    ///
    /// - the data is not copied.
    /// - this also works for empty data.
    pub fn new(data: &[u8], block_level: BlockLevel) -> Self {
        let len = Bytes(data.len() as u64);
        // number of blocks in our data
        let pages = blocks(len, block_level);
        // number of hashes (leaf and branch) for the pages
        let num_hashes = num_hashes(pages);
        let mut tree = vec![zero_hash(); num_hashes.to_usize()];
        let mut bitmap = vec![false; num_hashes.to_usize()];
        if pages == 0 {
            tree[0] = hash_chunk(Chunks(0), &[], true);
            bitmap[0] = true;
        } else {
            for (offset, hash) in block_hashes_iter(data, block_level) {
                tree[offset.to_usize() * 2] = hash;
                bitmap[offset.to_usize() * 2] = true;
            }
        }
        let mut res = Self {
            tree,
            bitmap,
            len,
            block_level,
        };
        res.rehash(None);
        res
    }

    /// clear all hashes except the root
    pub fn clear(&mut self) {
        for i in 0..self.bitmap.len() {
            if self.root() != (i as u64) {
                self.bitmap[i] = false;
                self.tree[i] = zero_hash();
            }
        }
    }

    /// Check if we have a hash for the given offset, without bounds check
    fn has0(&self, offset: Nodes) -> bool {
        self.bitmap[offset.to_usize()]
    }

    /// Get the has for the given offset, without have check or bounds check
    fn get_hash0(&self, offset: Nodes) -> &blake3::Hash {
        &self.tree[offset.to_usize()]
    }

    /// Get the hash for the given offset, with bounds check and check if we have it
    fn get_hash(&self, offset: Nodes) -> Option<&blake3::Hash> {
        if offset < self.tree_len() {
            if self.has0(offset) {
                Some(&self.get_hash0(offset))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn rehash(&mut self, data: Option<&[u8]>) {
        self.rehash0(data, self.root(), true);
    }

    fn rehash0(&mut self, data: Option<&[u8]>, offset: Nodes, is_root: bool) {
        assert!(self.bitmap.len() == self.tree.len());
        if offset < self.tree_len() {
            if !self.has0(offset) {
                if let Some((l, r)) = descendants(offset, self.tree_len()) {
                    self.rehash0(data, l, false);
                    self.rehash0(data, r, false);
                    if let (Some(left_child), Some(right_child)) =
                        (self.get_hash(l), self.get_hash(r))
                    {
                        let hash = blake3::guts::parent_cv(left_child, right_child, is_root);
                        self.set0(offset, hash);
                    }
                } else if let Some(data) = data {
                    // rehash from data
                    let index = index(offset);
                    let min = index.to_bytes(self.block_level);
                    let max = (index + 1).to_bytes(self.block_level).min(self.len);
                    let slice = &data[min.to_usize()..max.to_usize()];
                    let hash = hash_block(index, slice, self.block_level, is_root);
                    self.set0(offset, hash);
                } else {
                    // we can't rehash since we don't have the data
                }
            } else {
                // nothing to do
            }
        } else {
            if let Some(left_child) = left_child(offset) {
                self.rehash0(data, left_child, false);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        any::Any,
        cmp::{max, min},
        io::{self, Cursor, Read, Write},
    };

    use super::*;
    use bao::encode::SliceExtractor;
    use proptest::prelude::*;

    fn fmt_outboard(outboard: &[u8]) -> String {
        let mut res = String::new();
        res += &hex::encode(&outboard[0..8]);
        for i in (8..outboard.len()).step_by(32) {
            res += " ";
            res += &hex::encode(&outboard[i..i + 32]);
        }
        res
    }

    fn size_start_len() -> impl Strategy<Value = (Bytes, Bytes, Bytes)> {
        (0u64..32768)
            .prop_flat_map(|size| {
                let start = 0u64..size;
                let len = 0u64..size;
                (Just(size), start, len)
            })
            .prop_map(|(size, start, len)| {
                let size = Bytes(size);
                let start = Bytes(start);
                let len = Bytes(len);
                (size, start, len)
            })
    }

    /// Compare the bao slice extractor to the sparse outboard
    ///
    /// This can only be done at the block level 0
    fn compare_slice_impl(size: Bytes, start: Bytes, len: Bytes) {
        let data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let so = SparseOutboard::new(&data, BlockLevel(0));
        let slice2 = so.slice(&data, start..start + len).unwrap();
        if slice1 != slice2 {
            println!("{} {}", slice1.len(), slice2.len());
            println!("{}\n{}", hex::encode(&slice1), hex::encode(&slice2));
        }
        assert_eq!(slice1, slice2);
    }

    /// Compare the bao slice extractor to the sparse outboard
    ///
    /// This can only be done at the block level 0
    fn compare_slice_iter_impl(size: Bytes, start: Bytes, len: Bytes) {
        let data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let so = SparseOutboard::new(&data, BlockLevel(0));
        let slices2 = so.slice_iter(&data, start..start + len).collect::<Vec<_>>();
        let slice2 = slices2
            .iter()
            .map(|x| x.to_vec())
            .flatten()
            .collect::<Vec<_>>();
        println!("{} {}", slice1.len(), slice2.len());
        if slice1 != slice2 {
            println!("{:?}", slices2);
            println!("{} {}", slice1.len(), slice2.len());
            println!("{}\n\n{}", hex::encode(&slice1), hex::encode(&slice2));
        }
        assert_eq!(slice1, slice2);
    }

    fn add_from_slice_impl(size: Bytes, start: Bytes, len: Bytes) {
        let mut data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let mut so = SparseOutboard::new(&data, BlockLevel(0));
        let byte_range = start..start + len;
        so.add_from_slice(&mut data, byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        so.clear();
        so.add_from_slice(&mut data, byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        slice1[8] ^= 1;
        assert!(so
            .add_from_slice(&mut data, byte_range, &mut Cursor::new(&slice1))
            .is_err());
    }

    proptest! {

        #[test]
        fn compare_hash(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let hash = blake3::hash(&data);
            for level in 0..10 {
                // Sparse outboard must produce the same root hash regardless of the block level
                let so = SparseOutboard::new(&data, BlockLevel(level));
                let hash2 = *so.hash().unwrap();
                assert_eq!(hash, hash2);
            }
        }

        #[test]
        fn compare_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let (outboard, hash) = bao::encode::outboard(&data);
            let so = SparseOutboard::new(&data, BlockLevel(0));
            let hash2 = *so.hash().unwrap();
            let outboard2 = so.outboard().unwrap();
            assert_eq!(hash, hash2);
            assert_eq!(outboard, outboard2);
        }

        #[test]
        fn compare_encoded(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let (encoded, hash) = bao::encode::encode(&data);
            let so = SparseOutboard::new(&data, BlockLevel(0));
            let hash2 = *so.hash().unwrap();
            let encoded2 = so.encode(&data).unwrap();
            assert_eq!(hash, hash2);
            assert_eq!(encoded, encoded2);
        }

        #[test]
        fn compare_slice((size, start, len) in size_start_len()) {
            compare_slice_impl(size, start, len)
        }

        #[test]
        fn compare_slice_iter((size, start, len) in size_start_len()) {
            compare_slice_iter_impl(size, start, len)
        }

        #[test]
        fn add_from_slice((size, start, len) in size_start_len()) {
            add_from_slice_impl(size, start, len)
        }

        #[test]
        fn compare_hash_block_recursive(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let hash = blake3::hash(&data);
            let hash2 = hash_block(Blocks(0), &data, BlockLevel(10), true);
            assert_eq!(hash, hash2);
        }
    }

    #[test]
    fn compare_slice_0() {
        compare_slice_impl(Bytes(1025), Bytes(182), Bytes(843));
    }

    #[test]
    fn non_zero_block_level() {
        let data = [0u8; 4096];
        let l0 = SparseOutboard::new(&data, BlockLevel(0));
        let l1 = SparseOutboard::new(&data, BlockLevel(1));
        let l2 = SparseOutboard::new(&data, BlockLevel(2));
        println!("{:?}", l0.tree_len());
        println!("{:?}", l1.tree_len());
        println!("{:?}", l2.tree_len());
        println!("iter 0");
        for item in l0.slice_iter(&data, l0.byte_range()) {
            println!("{:?}", item);
        }
        println!("iter 1");
        for item in l1.slice_iter(&data, l1.byte_range()) {
            println!("{:?}", item);
        }
        println!("iter 2");
        for item in l2.slice_iter(&data, l2.byte_range()) {
            println!("{:?}", item);
        }
    }
}
