use crate::tree::*;
use std::{io::Read, iter::FusedIterator, ops::Range, sync::Arc};

/// Hash a blake3 chunk.
///
/// `chunk` is the chunk index, `data` is the chunk data, and `is_root` is true if this is the only chunk.
fn hash_chunk(chunk: Chunks, data: &[u8], is_root: bool) -> blake3::Hash {
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN);
    let mut hasher = blake3::guts::ChunkState::new(chunk.0 as u64);
    hasher.update(data);
    hasher.finalize(is_root)
}

/// Hash a block that is made of a power of two number of chunks.
///
/// `block` is the block index, `data` is the block data, `is_root` is true if this is the only block,
/// and `level` indicates how many chunks make up a block. Chunks = 2^level.
fn hash_block(block: Blocks, data: &[u8], level: u32, is_root: bool) -> blake3::Hash {
    // ensure that the data is not too big
    debug_assert!(data.len() <= blake3::guts::CHUNK_LEN << level);
    // compute the cunk number for the first chunk in this block
    let chunk0 = Chunks(block.0 << level);
    // simple recursive hash.
    // Note that this should really call in to blake3 hash_all_at_once, but
    // that is not exposed in the public API and also does not allow providing
    // the chunk.
    hash_block_recursive(chunk0, data, level, is_root)
}

/// Recursive helper for hash_block.
fn hash_block_recursive(chunk0: Chunks, data: &[u8], level: u32, is_root: bool) -> blake3::Hash {
    if level == 0 {
        // we have just a single chunk
        hash_chunk(chunk0, data, is_root)
    } else {
        // number of chunks at this level
        let chunks = 1 << level;
        // size corresponding to this level. Data must not be bigger than this.
        let size = blake3::guts::CHUNK_LEN << level;
        // mid point of the data
        let mid = size / 2;
        debug_assert!(data.len() <= size);
        if data.len() <= mid {
            hash_block_recursive(chunk0, data, level - 1, is_root)
        } else {
            let l = &data[..mid];
            let r = &data[mid..];
            let l = hash_block_recursive(chunk0, l, level - 1, false);
            let r = hash_block_recursive(chunk0 + chunks / 2, r, level - 1, false);
            blake3::guts::parent_cv(&l, &r, is_root)
        }
    }
}

fn leaf_hashes_iter(data: &[u8]) -> impl Iterator<Item = (usize, blake3::Hash)> + '_ {
    let is_root = data.len() <= 1024;
    data.chunks(1024)
        .enumerate()
        .map(move |(i, data)| (i, hash_chunk(Chunks(i as u64), data, is_root)))
}

/// Given a range of bytes, returns a range of nodes that cover that range.
fn node_range(byte_range: Range<Bytes>) -> Range<Nodes> {
    let start_page = byte_range.start.0 / 1024;
    let end_page = (byte_range.end.0 + 1023) / 1024;
    let start_offset = start_page * 2;
    let end_offset = end_page * 2;
    Nodes(start_offset)..Nodes(end_offset)
}

fn pages(len: Bytes) -> Blocks {
    Blocks(pages0(len.0))
}

fn pages0(len: u64) -> u64 {
    len / 1024 + if len % 1024 == 0 { 0 } else { 1 }
}

fn num_hashes(pages: Blocks) -> Nodes {
    Nodes(num_hashes0(pages.0))
}

fn num_hashes0(pages: u64) -> u64 {
    if pages > 0 {
        pages * 2 - 1
    } else {
        1
    }
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
}

struct SliceIter<'a> {
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

struct TestIter {
    data: Arc<Vec<u8>>,
}

enum TestIterItem {
    Header(u64),
    Hash(blake3::Hash),
    Data(Arc<Vec<u8>>),
}

impl Iterator for TestIter {
    type Item = TestIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        Some(TestIterItem::Data(self.data.clone()))
    }
}

#[derive(Debug, Clone)]
enum SliceIterItem<'a> {
    /// header containing the full size
    Header(u64),
    /// a hash
    Hash(blake3::Hash),
    /// data reference
    Data(&'a [u8]),
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
            let start = index(offset).0 * 1024;
            let end = (start + 1024).min(self.len.0);
            let slice = &data[start as usize..end as usize];
            target.extend_from_slice(slice);
        }
        Some(())
    }

    pub fn slice_iter<'a>(&'a self, data: &'a [u8], byte_range: Range<Bytes>) -> SliceIter<'a> {
        assert!(data.len() as u64 == self.len);
        let offset_range = node_range(byte_range);
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
        let offset_range = node_range(byte_range);
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
        let offset_range = node_range(byte_range);
        self.add_from_slice_0(self.root(), data, &offset_range, reader, true)?;
        Ok(())
    }

    fn add_from_slice_0(
        &mut self,
        offset: Nodes,
        data: &mut [u8],
        offset_range: &Range<Nodes>,
        reader: &mut impl Read,
        is_root: bool,
    ) -> anyhow::Result<()> {
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
            self.add_from_slice_0(l, data, offset_range, reader, false)?;
            self.add_from_slice_0(r, data, offset_range, reader, false)?;
        } else {
            let leaf_byte_range = self.leaf_byte_range(offset);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            anyhow::ensure!(len <= 1024, "leaf too big");
            let mut leaf_slice = [0u8; 1024];
            reader.read_exact(&mut leaf_slice[0..len.to_usize()])?;
            let expected_hash = hash_chunk(index(offset), &leaf_slice[..len.to_usize()], is_root);
            self.validate(offset, expected_hash)?;
            let leaf_byte_range = bo_range_to_usize(leaf_byte_range);
            data[leaf_byte_range].copy_from_slice(&leaf_slice[..len.to_usize()]);
        }
        Ok(())
    }

    /// byte range for a given offset
    fn leaf_byte_range(&self, offset: Nodes) -> Range<Bytes> {
        let start = Bytes(index(offset).0 * 1024);
        let end = (start + 1024).min(self.len);
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
            println!("validating hash at {:?} level {}", offset, level(offset));
            anyhow::ensure!(self.get_hash0(offset) == &hash, "hash mismatch");
        } else {
            println!("storing hash at {:?} level {}", offset, level(offset));
            self.set0(offset, hash);
        }
        Ok(())
    }

    /// validate the hash for the given offset
    fn validate(&mut self, offset: Nodes, hash: blake3::Hash) -> anyhow::Result<()> {
        anyhow::ensure!(offset < self.tree_len());
        anyhow::ensure!(self.has0(offset), "hash not set");
        println!("validating hash at {:?} level {}", offset, level(offset));
        anyhow::ensure!(self.get_hash0(offset) == &hash, "hash mismatch");
        Ok(())
    }

    /// create a new sparse outboard for the given data
    ///
    /// - the data is not copied.
    /// - this also works for empty data.
    pub fn new(data: &[u8]) -> Self {
        let len = Bytes(data.len() as u64);
        // number of 1024 byte pages in our data
        let pages = pages(len);
        // number of hashes (leaf and branch) for the pages
        let num_hashes = num_hashes(pages);
        let mut tree = vec![zero_hash(); num_hashes.to_usize()];
        let mut bitmap = vec![false; num_hashes.to_usize()];
        if pages == 0 {
            tree[0] = hash_chunk(Chunks(0), &[], true);
            bitmap[0] = true;
        } else {
            for (offset, hash) in leaf_hashes_iter(data) {
                tree[offset * 2] = hash;
                bitmap[offset * 2] = true;
            }
        }
        let mut res = Self { tree, bitmap, len };
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
                    let min = Bytes(index.0 * 1024);
                    let max = std::cmp::min(min + 1024, self.len);
                    let slice = &data[min.to_usize()..max.to_usize()];
                    let hash = hash_chunk(index, slice, is_root);
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

    fn compare_slice_impl(size: Bytes, start: Bytes, len: Bytes) {
        let data = (0..size.to_usize())
            .map(|i| (i / 1024) as u8)
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
        let so = SparseOutboard::new(&data);
        let slice2 = so.slice(&data, start..start + len).unwrap();
        if slice1 != slice2 {
            println!("{} {}", slice1.len(), slice2.len());
            println!("{}\n{}", hex::encode(&slice1), hex::encode(&slice2));
        }
        assert_eq!(slice1, slice2);
    }

    fn compare_slice_iter_impl(size: Bytes, start: Bytes, len: Bytes) {
        let data = (0..size.0).map(|i| (i / 1024) as u8).collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let so = SparseOutboard::new(&data);
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
        let mut data = (0..size.0).map(|i| (i / 1024) as u8).collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let mut so = SparseOutboard::new(&data);
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
            let hash2 = *SparseOutboard::new(&data).hash().unwrap();
            assert_eq!(hash, hash2);
        }

        #[test]
        fn compare_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let (outboard, hash) = bao::encode::outboard(&data);
            let so = SparseOutboard::new(&data);
            let hash2 = *so.hash().unwrap();
            let outboard2 = so.outboard().unwrap();
            assert_eq!(hash, hash2);
            assert_eq!(outboard, outboard2);
        }

        #[test]
        fn compare_encoded(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let (encoded, hash) = bao::encode::encode(&data);
            let so = SparseOutboard::new(&data);
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
        fn children_parent(i in any::<Nodes>()) {
            if let Some((l, r)) = children(i) {
                assert_eq!(parent(l), i);
                assert_eq!(parent(r), i);
            }
        }

        /// Checks that left_child/right_child are consistent with children
        #[test]
        fn children_consistent(i in any::<Nodes>()) {
            let lc = left_child(i);
            let rc = right_child(i);
            let c = children(i);
            let lc1 = c.map(|(l, _)| l);
            let rc1 = c.map(|(_, r)| r);
            assert_eq!(lc, lc1);
            assert_eq!(rc, rc1);
        }

        #[test]
        fn sibling_sibling(i in any::<Nodes>()) {
            let s = sibling(i);
            let distance = max(s, i) - min(s, i);
            // sibling is at a distance of 2*span
            assert_eq!(distance, span(i) * 2);
            // sibling of sibling is value itself
            assert_eq!(sibling(s), i);
        }

        #[test]
        fn compare_descendants(i in any::<Nodes>(), len in any::<Nodes>()) {
            let d = descendants(i, len);
            let lc = left_child(i);
            let rc = right_descendant(i, len);
            if let (Some(lc), Some(rc)) = (lc, rc) {
                assert_eq!(d, Some((lc, rc)));
            } else {
                assert_eq!(d, None);
            }
        }

        #[test]
        fn compare_hash_block_recursive(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let hash = blake3::hash(&data);
            let hash2 = hash_block(Blocks(0), &data, 10, true);
            assert_eq!(hash, hash2);
        }
    }

    #[test]
    fn compare_slice_0() {
        compare_slice_impl(Bytes(1025), Bytes(182), Bytes(843));
    }
}
