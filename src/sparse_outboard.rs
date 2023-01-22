use crate::tree::*;
use std::{io::Read, iter::FusedIterator, ops::Range};

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
    #[cfg(test)]
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

    /// the number of hashes in the tree
    fn tree_len(&self) -> Nodes {
        debug_assert!(self.tree.len() == self.bitmap.len());
        Nodes(self.tree.len() as u64)
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
    fn get_hash(&self, offset: Nodes) -> Option<&blake3::Hash> {
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
            tree[0] = empty_root_hash();
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
    fn get(&self, offset: Nodes) -> Option<&blake3::Hash> {
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
    use super::*;
    use crate::sync_store::*;
    use bao::encode::SliceExtractor;
    use proptest::prelude::*;
    use std::io::{Cursor, Read};

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

    fn compare_slice_iter_impl(size: Bytes, start: Bytes, len: Bytes) {
        // generate data, different value for each chunk so the hashes are intersting
        let data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();

        // encode a slice using bao
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();

        // encode a slice using sparse outboard and its slice_iter
        let so = SparseOutboard::new(&data, BlockLevel(0));
        let slices2 = so.slice_iter(&data, start..start + len).collect::<Vec<_>>();
        let slice2 = slices2
            .iter()
            .map(|x| x.to_vec())
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(slice1, slice2);

        // encode a slice using vec outboard and its slice_iter
        let vs = VecSyncStore::new(&data, BlockLevel(0)).unwrap();

        // use the iterator and flatten it
        let slices2 = vs.slice_iter(start..start + len).collect::<Vec<_>>();
        let slice2 = slices2
            .into_iter()
            .map(|x| x.unwrap().to_vec())
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(slice1, slice2);

        // use the reader and use read_to_end, should be the same
        let vs = BlakeFile::<VecSyncStore>::new(&data, BlockLevel(0)).unwrap();
        let mut reader = vs.extract_slice(start..start + len);
        let mut slice2 = Vec::new();
        reader.read_to_end(&mut slice2).unwrap();
        assert_eq!(slice1, slice2);
    }

    fn add_from_slice_impl(size: Bytes, start: Bytes, len: Bytes) {
        // generate data, different value for each chunk so the hashes are intersting
        let mut data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();

        // encode a slice using bao
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();

        // add from the bao slice and check that it validates
        let mut so = SparseOutboard::new(&data, BlockLevel(0));
        let byte_range = start..start + len;
        so.add_from_slice(&mut data, byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        so.clear();
        so.add_from_slice(&mut data, byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        slice1[8] ^= 1;

        // add from the bao slice with a single bit flipped and check that it fails to validate
        assert!(so
            .add_from_slice(&mut data, byte_range, &mut Cursor::new(&slice1))
            .is_err());
    }

    fn add_from_slice_impl_2(size: Bytes, start: Bytes, len: Bytes) {
        // generate data, different value for each chunk so the hashes are intersting
        let data = (0..size.0)
            .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
            .collect::<Vec<_>>();

        // encode a slice using bao
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start.0,
            len.0,
        );
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();

        // add from the bao slice and check that it validates
        let mut vs = BlakeFile::<VecSyncStore>::new(&data, BlockLevel(0)).unwrap();
        let byte_range = start..start + len;
        vs.add_from_slice(byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        vs.clear().unwrap();
        vs.add_from_slice(byte_range.clone(), &mut Cursor::new(&slice1))
            .unwrap();
        slice1[8] ^= 1;

        // add from the bao slice with a single bit flipped and check that it fails to validate
        let err = vs
            .add_from_slice(byte_range, &mut Cursor::new(&slice1))
            .unwrap_err();
        assert!(matches!(err, AddSliceError::Validation(_)));
    }

    fn compare_hash_impl(data: &[u8]) {
        let hash = blake3::hash(data);
        for level in 0..10 {
            // Sparse outboard must produce the same root hash regardless of the block level
            let so = SparseOutboard::new(data, BlockLevel(level));
            let hash2 = *so.hash().unwrap();
            assert_eq!(hash, hash2);
        }

        for level in 0..10 {
            let vs = VecSyncStore::new(data, BlockLevel(level)).unwrap();
            let hash2 = vs.hash().unwrap().unwrap();
            assert_eq!(hash, hash2);
        }
    }

    fn compare_outboard_impl(data: &[u8]) {
        let (outboard, hash) = bao::encode::outboard(&data);
        // compare with bao outboard - only makes sense for block level 0
        {
            let so = SparseOutboard::new(&data, BlockLevel(0));
            let hash2 = *so.hash().unwrap();
            let outboard2 = so.outboard().unwrap();
            assert_eq!(hash, hash2);
            assert_eq!(outboard, outboard2);
        }
        // compare internally
        for level in 0..3 {
            let level = BlockLevel(level);
            let so = SparseOutboard::new(&data, level);
            let hash1 = *so.hash().unwrap();
            let outboard1 = so.outboard().unwrap();
            let vs = VecSyncStore::new(&data, level).unwrap();
            let hash2 = vs.hash().unwrap().unwrap();
            let outboard2 = vs.outboard().unwrap();
            assert_eq!(hash1, hash2);
            assert_eq!(outboard1, outboard2);
            assert_eq!(hash, hash2);
        }
    }

    fn compare_encoded_impl(data: &[u8]) {
        let (encoded, hash) = bao::encode::encode(&data);
        // compare with bao outboard - only makes sense for block level 0
        {
            let so = SparseOutboard::new(&data, BlockLevel(0));
            let hash2 = *so.hash().unwrap();
            let encoded2 = so.encode(&data).unwrap();
            assert_eq!(hash, hash2);
            assert_eq!(encoded, encoded2);
        }
        // compare internally
        for level in 0..3 {
            let level = BlockLevel(level);
            let so = SparseOutboard::new(&data, level);
            let hash1 = *so.hash().unwrap();
            let encoded1 = so.encode(data).unwrap();
            let vs = VecSyncStore::new(&data, level).unwrap();
            let hash2 = vs.hash().unwrap().unwrap();
            let encoded2 = vs.encode().unwrap();
            assert_eq!(hash1, hash2);
            assert_eq!(encoded1, encoded2);
            assert_eq!(hash, hash2);
        }
    }

    proptest! {

        #[test]
        fn compare_hash(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            compare_hash_impl(&data);
        }

        #[test]
        fn compare_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            compare_outboard_impl(&data);
        }

        #[test]
        fn compare_encoded(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            compare_encoded_impl(&data);
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
            add_from_slice_impl(size, start, len);
            add_from_slice_impl_2(size, start, len);
        }

        #[test]
        fn compare_hash_block_recursive(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let hash = blake3::hash(&data);
            let hash2 = hash_block(Blocks(0), &data, BlockLevel(10), true);
            assert_eq!(hash, hash2);
        }
    }

    #[test]
    fn compare_hash_0() {
        compare_hash_impl(&[0u8; 1024]);
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
