use std::{ops::Range, io::Read};

#[repr(transparent)]
struct Offset(usize);

fn leaf_hash(offset: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    let mut hasher = blake3::guts::ChunkState::new(offset);
    hasher.update(data);
    hasher.finalize(is_root)
}

fn leaf_hashes_iter(data: &[u8]) -> impl Iterator<Item = (usize, blake3::Hash)> + '_ {
    let is_root = data.len() <= 1024;
    data.chunks(1024)
        .enumerate()
        .map(move |(i, data)| (i, leaf_hash(i as u64, data, is_root)))
}

fn pages(len: usize) -> usize {
    len / 1024 + if len % 1024 == 0 { 0 } else { 1 }
}

fn num_hashes(pages: usize) -> usize {
    if pages > 0 {
        pages * 2 - 1
    } else {
        1
    }
}

fn leafs(blocks: usize) -> usize {
    (blocks + 1) / 2
}

/// Root offset given a number of leaves.
fn root(leafs: usize) -> usize {
    leafs.next_power_of_two() - 1
}

/// Level for an offset. 0 is for leaves, 1 is for the first level of branches, etc.
fn level(offset: usize) -> u32 {
    (!offset).trailing_zeros()
}

/// Span for an offset. 1 is for leaves, 2 is for the first level of branches, etc.
fn span(offset: usize) -> usize {
    1 << level(offset)
}

fn left_child(offset: usize) -> Option<usize> {
    let span = span(offset);
    if span == 1 {
        None
    } else {
        Some(offset - span / 2)
    }
}

fn right_child(offset: usize) -> Option<usize> {
    let span = span(offset);
    if span == 1 {
        None
    } else {
        Some(offset + span / 2)
    }
}

/// Get a valid right descendant for an offset
fn right_descendant(offset: usize, len: usize) -> Option<usize> {
    let mut offset = right_child(offset)?;
    while offset >= len {
        offset = left_child(offset)?;
    }
    Some(offset)
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
fn children(offset: usize) -> Option<(usize, usize)> {
    let span = span(offset);
    if span == 1 {
        None
    } else {
        Some((offset - span / 2, offset + span / 2))
    }
}

fn load_hash(hash: &[u8]) -> blake3::Hash {
    let hash: [u8; 32] = hash.try_into().unwrap();
    hash.into()
}

/// both children are at one level below the parent, but it is not guaranteed that they exist
fn descendants(offset: usize, len: usize) -> Option<(usize, usize)> {
    let lc = left_child(offset);
    let rc = right_descendant(offset, len);
    if let (Some(l), Some(r)) = (lc, rc) {
        Some((l, r))
    } else {
        None
    }
}

fn is_left_sibling(offset: usize) -> bool {
    let span = span(offset) * 2;
    (offset & span) == 0
}

fn parent(offset: usize) -> usize {
    let span = span(offset);
    // if is_left_sibling(offset) {
    if (offset & (span * 2)) == 0 {
        offset + span
    } else {
        offset - span
    }
}

fn index(offset: usize) -> usize {
    offset / 2
}

fn range(offset: usize) -> Range<usize> {
    let span = span(offset);
    offset + 1 - span..offset + span
}

fn sibling(offset: usize) -> usize {
    if is_left_sibling(offset) {
        offset + span(offset) * 2
    } else {
        offset - span(offset) * 2
    }
}

/// depth first, left to right traversal of a tree of size len
fn depth_first_left_to_right(len: usize) -> impl Iterator<Item = usize> {
    fn descend(offset: usize, len: usize, res: &mut Vec<usize>) {
        if offset < len {
            res.push(offset);
            if let Some((left, right)) = children(offset) {
                descend(left, len, res);
                descend(right, len, res);
            }
        } else if let Some(left_child) = left_child(offset) {
            descend(left_child, len, res)
        }
    }
    // compute number of leafs (this will be 1 even for empty data)
    let leafs = leafs(len);
    // compute root offset
    let root = root(leafs);
    // result vec
    let mut res = Vec::with_capacity(len);
    descend(root, len, &mut res);
    res.into_iter()
}

/// breadth first, left to right traversal of a tree of size len
fn breadth_first_left_to_right(len: usize) -> impl Iterator<Item = usize> {
    fn descend(current: Vec<usize>, len: usize, res: &mut Vec<usize>) {
        let mut next = Vec::new();
        for offset in current {
            if offset < len {
                res.push(offset);
                if let Some((left, right)) = children(offset) {
                    next.push(left);
                    next.push(right);
                }
            } else if let Some(left_child) = left_child(offset) {
                next.push(left_child);
            }
        }
        if !next.is_empty() {
            descend(next, len, res);
        }
    }
    // compute number of leafs (this will be 1 even for empty data)
    let leafs = leafs(len);
    // compute root offset
    let root = root(leafs);
    // result vec
    let mut res = Vec::with_capacity(len);
    descend(vec![root], len, &mut res);
    res.into_iter()
}

fn empty_hash() -> blake3::Hash {
    blake3::Hash::from([0u8; 32])
}

pub struct SparseOutboard {
    /// even offsets are leaf hashes, odd offsets are branch hashes
    tree: Vec<blake3::Hash>,
    /// occupancy bitmap for the tree
    bitmap: Vec<bool>,
    /// total length of the data
    len: usize,
}

impl SparseOutboard {

    /// number of leaf hashes in our tree
    /// 
    /// will return 1 for empty data, since even empty data has a root hash
    fn leafs(&self) -> usize {
        leafs(self.tree.len())
    }

    /// number of 1024 byte pages of our data, including the last partial page
    ///
    /// will return 0 for empty data
    fn pages(&self) -> usize {
        pages(self.len)
    }

    /// the blake3 hash of the entire data
    pub fn hash(&self) -> Option<&blake3::Hash> {
        self.get_hash(root(self.leafs()))
    }

    /// produce a blake3 outboard for the entire data
    ///
    /// returns None if the required hashes are not fully available
    pub fn outboard(&self) -> Option<Vec<u8>> {
        let outboard_len = 8 + (self.leafs() - 1) * 64;
        let mut res = Vec::with_capacity(outboard_len);
        // write the header - total length of the data
        res.extend_from_slice(&(self.len as u64).to_le_bytes());
        self.outboard0(root(self.leafs()), &mut res)?;
        debug_assert_eq!(res.len(), outboard_len);
        Some(res)
    }

    fn outboard0(&self, offset: usize, target: &mut Vec<u8>) -> Option<()> {
        if let Some((l, r)) = descendants(offset, self.tree.len()) {
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
        assert!(data.len() == self.len);
        let encoded_len = 8 + (self.leafs() - 1) * 64 + self.len;
        let mut res = Vec::with_capacity(encoded_len);
        // write the header - total length of the data
        res.extend_from_slice(&(self.len as u64).to_le_bytes());
        self.encode0(root(self.leafs()), data, &mut res)?;
        debug_assert_eq!(res.len(), encoded_len);
        Some(res)
    }

    fn encode0(&self, offset: usize, data: &[u8], target: &mut Vec<u8>) -> Option<()> {
        if let Some((l, r)) = descendants(offset, self.tree.len()) {
            let lh = self.get(l)?;
            let rh = self.get(r)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.encode0(l, data, target)?;
            self.encode0(r, data, target)?;
        } else {
            let start = index(offset) * 1024;
            let end = (start + 1024).min(self.len);
            let slice = &data[start..end];
            target.extend_from_slice(slice);
        }
        Some(())
    }

    /// Compute a verifiable slice of the data
    pub fn slice(&self, data: &[u8], byte_range: Range<usize>) -> Option<Vec<u8>> {
        assert!(data.len() == self.len);
        let offset_range = offset_range(byte_range);
        let mut res = Vec::new();
        // write the header - total length of the data
        res.extend_from_slice(&(self.len as u64).to_le_bytes());
        self.slice0(root(self.leafs()), data, &offset_range, &mut res)?;
        Some(res)
    }

    fn slice0(&self, offset: usize, data: &[u8], offset_range: &Range<usize>, target: &mut Vec<u8>) -> Option<()> {
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Some(());
        }
        if let Some((l, r)) = descendants(offset, self.tree.len()) {
            let lh = self.get(l)?;
            let rh = self.get(r)?;
            target.extend_from_slice(lh.as_bytes());
            target.extend_from_slice(rh.as_bytes());
            self.slice0(l, data, offset_range, target)?;
            self.slice0(r, data, offset_range, target)?;
        } else {
            let leaf_byte_range = self.leaf_byte_range(offset);
            let slice = &data[leaf_byte_range];
            target.extend_from_slice(slice);
        }
        Some(())
    }

    pub fn add_from_slice(&mut self, data: &mut [u8], byte_range: Range<usize>, slice: &mut impl Read) -> anyhow::Result<()> {
        assert!(data.len() == self.len);
        let mut buf = [0u8; 8];
        slice.read_exact(&mut buf)?;
        let len = u64::from_le_bytes(buf) as usize;
        anyhow::ensure!(len == self.len, "wrong length");
        let offset_range = offset_range(byte_range);
        self.add_from_slice_0(root(self.leafs()), data, &offset_range, slice, true)?;
        Ok(())
    }

    fn add_from_slice_0(&mut self, offset: usize, data: &mut [u8], offset_range: &Range<usize>, slice: &mut impl Read, is_root: bool) -> anyhow::Result<()> {
        let range = range(offset);
        // if the range of this node is entirely outside the slice, we can skip it
        if range.end <= offset_range.start || range.start >= offset_range.end {
            return Ok(());
        }
        if let Some((l, r)) = descendants(offset, self.tree.len()) {
            let mut lh = [0u8; 32];
            let mut rh = [0u8; 32];
            slice.read_exact(&mut lh)?;
            slice.read_exact(&mut rh)?;
            let left_child = lh.into();
            let right_child = rh.into();
            let expected_hash = blake3::guts::parent_cv(&left_child, &right_child, is_root);
            self.validate(offset, expected_hash)?;
            self.set_or_validate(l, left_child)?;
            self.set_or_validate(r, right_child)?;
            self.add_from_slice_0(l, data, offset_range, slice, false)?;
            self.add_from_slice_0(r, data, offset_range, slice, false)?;
        } else {
            let leaf_byte_range = self.leaf_byte_range(offset);
            let len = leaf_byte_range.end - leaf_byte_range.start;
            anyhow::ensure!(len <= 1024, "leaf too big");
            let mut leaf_slice = [0u8; 1024];
            slice.read_exact(&mut leaf_slice[0..len])?;
            let expected_hash = leaf_hash(index(offset) as u64, &leaf_slice[..len], is_root);
            self.validate(offset, expected_hash)?;
            data[leaf_byte_range].copy_from_slice(&leaf_slice[..len]);
        }
        Ok(())
    }

    fn leaf_byte_range(&self, offset: usize) -> Range<usize> {
        let start = index(offset) * 1024;
        let end = (start + 1024).min(self.len);
        start..end
    }

    fn get(&self, offset: usize) -> Option<&blake3::Hash> {
        if offset < self.tree.len() && self.bitmap[offset] {
            Some(&self.tree[offset])
        } else {
            None
        }
    }

    /// set the hash for the given offset, or validate it
    fn set_or_validate(&mut self, offset: usize, hash: blake3::Hash) -> anyhow::Result<()> {
        anyhow::ensure!(offset < self.tree.len());
        if self.bitmap[offset] {
            println!("validating hash at offset {} level {}", offset, level(offset));
            anyhow::ensure!(self.tree[offset] == hash, "hash mismatch");
        } else {
            println!("storing hash at offset {} level {}", offset, level(offset));
            self.bitmap[offset] = true;
            self.tree[offset] = hash;
        }
        Ok(())
    }

    /// validate the hash for the given offset
    fn validate(&mut self, offset: usize, hash: blake3::Hash) -> anyhow::Result<()> {
        anyhow::ensure!(offset < self.tree.len());
        anyhow::ensure!(self.bitmap[offset], "hash not set");
        println!("validating hash at offset {} level {}", offset, level(offset));
        anyhow::ensure!(self.tree[offset] == hash, "hash mismatch");
        Ok(())
    }

    /// create a new sparse outboard for the given data
    ///
    /// - the data is not copied.
    /// - this also works for empty data.
    pub fn new(data: &[u8]) -> Self {
        let len = data.len();
        // number of 1024 byte pages in our data
        let pages = pages(len);
        // number of hashes (leaf and branch) for the pages
        let num_hashes = num_hashes(pages);
        let mut tree = vec![empty_hash(); num_hashes];
        let mut bitmap = vec![false; num_hashes];
        for (offset, hash) in leaf_hashes_iter(data) {
            tree[offset * 2] = hash;
            bitmap[offset * 2] = true;
        }
        if pages == 0 {
            tree[0] = leaf_hash(0, &[], true);
            bitmap[0] = true;
        }
        let mut res = Self { tree, bitmap, len };
        res.rehash(None);
        res
    }

    fn clear(&mut self) {
        for i in 0..self.bitmap.len() {
            if i != root(self.leafs()) {
                self.bitmap[i] = false;
             
                self.tree[i] = empty_hash();
            }
        }
    }

    fn get_hash(&self, offset: usize) -> Option<&blake3::Hash> {
        if offset < self.bitmap.len() {
            if self.bitmap[offset] {
                Some(&self.tree[offset])
            } else {
                None
            }
        } else if let Some(left_child) = left_child(offset) {
            self.get_hash(left_child)
        } else {
            None
        }
    }

    fn rehash(&mut self, data: Option<&[u8]>) {
        self.rehash0(data, root(self.leafs()), true);
    }

    fn rehash0(&mut self, data: Option<&[u8]>, offset: usize, is_root: bool) {
        assert!(self.bitmap.len() == self.tree.len());
        if offset < self.bitmap.len() {
            if !self.bitmap[offset] {
                if let Some((l, r)) = children(offset) {
                    self.rehash0(data, l, false);
                    self.rehash0(data, r, false);
                    if let (Some(left_child), Some(right_child)) =
                        (self.get_hash(l), self.get_hash(r))
                    {
                        let res = blake3::guts::parent_cv(left_child, right_child, is_root);
                        self.bitmap[offset] = true;
                        self.tree[offset] = res;
                    }
                } else if let Some(data) = data {
                    // rehash from data
                    let index = index(offset);
                    let min = index * 1024;
                    let max = std::cmp::min(min + 1024, self.len);
                    let slice = &data[min..max];
                    self.bitmap[offset] = true;
                    self.tree[offset] = leaf_hash(index as u64, slice, is_root);
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

fn offset_range(byte_range: Range<usize>) -> Range<usize> {
    let start_page = byte_range.start / 1024;
    let end_page = (byte_range.end + 1023) / 1024;
    let start_offset = start_page * 2;
    let end_offset = end_page * 2;
    start_offset.. end_offset
}

#[cfg(test)]
mod tests {
    use std::{cmp::{max, min}, io::{Cursor, Read, self, Write}};

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

    fn size_start_len() -> impl Strategy<Value = (usize, usize, usize)> {
        (0usize..32768).prop_flat_map(|size| {
            let start = 0usize..size;
            let len = 0usize..size;
            (Just(size), start, len)
        })
    }

    fn compare_slice_impl(size: usize, start: usize, len: usize) {
        let data = (0..size).map(|i| (i / 1024) as u8).collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(Cursor::new(&data), Cursor::new(&outboard), start as u64, len as u64);
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

    fn add_from_slice_impl(size: usize, start: usize, len: usize) {
        let mut data = (0..size).map(|i| (i / 1024) as u8).collect::<Vec<_>>();
        let (outboard, _hash) = bao::encode::outboard(&data);
        let mut extractor = SliceExtractor::new_outboard(Cursor::new(&data), Cursor::new(&outboard), start as u64, len as u64);
        let mut slice1 = Vec::new();
        extractor.read_to_end(&mut slice1).unwrap();
        let mut so = SparseOutboard::new(&data);
        so.add_from_slice(&mut data, start..start + len, &mut Cursor::new(&slice1)).unwrap();
        so.clear();
        so.add_from_slice(&mut data, start..start + len, &mut Cursor::new(&slice1)).unwrap();
        slice1[8] ^= 1;
        assert!(so.add_from_slice(&mut data, start..start + len, &mut Cursor::new(&slice1)).is_err());
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
        fn add_from_slice((size, start, len) in size_start_len()) {
            add_from_slice_impl(size, start, len)
        }

        #[test]
        fn children_parent(i in any::<usize>()) {
            if let Some((l, r)) = children(i) {
                assert_eq!(parent(l), i);
                assert_eq!(parent(r), i);
            }
        }

        /// Checks that left_child/right_child are consistent with children
        #[test]
        fn children_consistent(i in any::<usize>()) {
            let lc = left_child(i);
            let rc = right_child(i);
            let c = children(i);
            let lc1 = c.map(|(l, _)| l);
            let rc1 = c.map(|(_, r)| r);
            assert_eq!(lc, lc1);
            assert_eq!(rc, rc1);
        }

        #[test]
        fn sibling_sibling(i in any::<usize>()) {
            let s = sibling(i);
            let distance = max(s, i) - min(s, i);
            // sibling is at a distance of 2*span
            assert_eq!(distance, span(i) * 2);
            // sibling of sibling is value itself
            assert_eq!(sibling(s), i);
        }

        #[test]
        fn compare_descendants(i in any::<usize>(), len in any::<usize>()) {
            let d = descendants(i, len);
            let lc = left_child(i);
            let rc = right_descendant(i, len);
            if let (Some(lc), Some(rc)) = (lc, rc) {
                assert_eq!(d, Some((lc, rc)));
            } else {
                assert_eq!(d, None);
            }
        }
    }

    #[test]
    fn compare_slice_0() {
        compare_slice_impl(1025, 182, 843);
    }

    #[test]
    fn test_right_descendant() {
        for i in (1..11) {
            println!("valid_right_child({}, 9), {:?}", i, right_descendant(i, 9));
        }
    }

    #[test]
    fn test_left() {
        for i in 0..20 {
            println!("assert_eq!(left_child({}), {:?})", i, left_child(i));
        }
        for i in 0..20 {
            println!("assert_eq!(is_left({}), {})", i, is_left_sibling(i));
        }
        for i in 0..20 {
            println!("assert_eq!(parent({}), {})", i, parent(i));
        }
        for i in 0..20 {
            println!("assert_eq!(sibling({}), {})", i, sibling(i));
        }
        assert_eq!(left_child(3), Some(1));
        assert_eq!(left_child(1), Some(0));
        assert_eq!(left_child(0), None);
    }

    #[test]
    fn test_span() {
        for i in 0..10 {
            println!("assert_eq!(span({}), {})", i, span(i))
        }
    }

    #[test]
    fn test_level() {
        for i in 0..10 {
            println!("assert_eq!(level({}), {})", i, level(i))
        }
        assert_eq!(level(0), 0);
        assert_eq!(level(1), 1);
        assert_eq!(level(2), 0);
        assert_eq!(level(3), 2);
    }

    #[test]
    fn test_dflr() {
        use depth_first_left_to_right as dflr;
        assert_eq!(dflr(1).collect::<Vec<_>>(), vec![0]);
        assert_eq!(dflr(3).collect::<Vec<_>>(), vec![1, 0, 2]);
        assert_eq!(dflr(5).collect::<Vec<_>>(), vec![3, 1, 0, 2, 4]);
        assert_eq!(dflr(7).collect::<Vec<_>>(), vec![3, 1, 0, 2, 5, 4, 6]);
        assert_eq!(dflr(9).collect::<Vec<_>>(), vec![7, 3, 1, 0, 2, 5, 4, 6, 8]);
    }

    #[test]
    fn test_bflr() {
        use breadth_first_left_to_right as bflr;
        assert_eq!(bflr(1).collect::<Vec<_>>(), vec![0]);
        assert_eq!(bflr(3).collect::<Vec<_>>(), vec![1, 0, 2]);
        assert_eq!(bflr(5).collect::<Vec<_>>(), vec![3, 1, 0, 2, 4]);
        assert_eq!(bflr(7).collect::<Vec<_>>(), vec![3, 1, 5, 0, 2, 4, 6]);
        assert_eq!(bflr(9).collect::<Vec<_>>(), vec![7, 3, 1, 5, 0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_range() {
        for i in 0..8 {
            println!("{} {:?}", i, range(i));
        }
    }

    #[test]
    fn test_root() {
        assert_eq!(root(0), 0);
        assert_eq!(root(1), 0);
        assert_eq!(root(2), 1);
        assert_eq!(root(3), 3);
        assert_eq!(root(4), 3);
        assert_eq!(root(5), 7);
        assert_eq!(root(6), 7);
        assert_eq!(root(7), 7);
        assert_eq!(root(8), 7);
        assert_eq!(root(9), 15);
        assert_eq!(root(10), 15);
        assert_eq!(root(11), 15);
        assert_eq!(root(12), 15);
        assert_eq!(root(13), 15);
        assert_eq!(root(14), 15);
        assert_eq!(root(15), 15);
        assert_eq!(root(16), 15);
        assert_eq!(root(17), 31);
        assert_eq!(root(18), 31);
        assert_eq!(root(19), 31);
        assert_eq!(root(20), 31);
        assert_eq!(root(21), 31);
        assert_eq!(root(22), 31);
        assert_eq!(root(23), 31);
        assert_eq!(root(24), 31);
        assert_eq!(root(25), 31);
        assert_eq!(root(26), 31);
        assert_eq!(root(27), 31);
        assert_eq!(root(28), 31);
        assert_eq!(root(29), 31);
        assert_eq!(root(30), 31);
        assert_eq!(root(31), 31);
        for i in 1..32 {
            println!("assert_eq!(root({}),{});", i, root(i))
        }
    }
}
