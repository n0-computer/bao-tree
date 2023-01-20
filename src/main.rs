use bao::decode::SliceDecoder;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

fn blocks(leafs: usize) -> usize {
    leafs * 2 - 1
}

fn leafs(blocks: usize) -> usize {
    (blocks + 1) / 2
}

/// Root offset given a number of leaves.
fn root(leafs: usize) -> usize {
    assert!(leafs > 0);
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

fn children(offset: usize) -> Option<(usize, usize)> {
    let span = span(offset);
    if span == 1 {
        None
    } else {
        Some((offset - span / 2, offset + span / 2))
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

fn sibling(offset: usize) -> usize {
    if is_left_sibling(offset) {
        offset + span(offset) * 2
    } else {
        offset - span(offset) * 2
    }
}

type BranchNode = [u8; 64];

fn empty_hash() -> blake3::Hash {
    blake3::Hash::from([0u8; 32])
}

struct SparseOutboard {
    /// even offsets are leaf hashes, odd offsets are branch hashes
    tree: Vec<blake3::Hash>,
    /// occupancy bitmap for the tree
    bitmap: Vec<bool>,
    /// total length of the data
    len: u64,
}

impl SparseOutboard {
    fn new() -> Self {
        Self {
            tree: Vec::new(),
            bitmap: Vec::new(),
            len: 0,
        }
    }

    fn leafs(&self) -> usize {
        leafs(self.tree.len())
    }

    fn from_data(data: &[u8]) -> Self {
        let len = data.len() as u64;
        let blocks = blocks(data.len());
        let mut tree = vec![empty_hash(); blocks * 2 - 1];
        let mut bitmap = vec![false; blocks * 2 - 1];
        for (offset, hash) in leaf_hashes_iter(data) {
            tree[offset * 2] = hash;
            bitmap[offset * 2] = true;
        }
        let mut res = Self { tree, bitmap, len };
        res.rehash();
        res
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

    fn rehash(&mut self) {
        self.rehash0(root(self.leafs()), true);
    }

    fn rehash0(&mut self, offset: usize, is_root: bool) {
        assert!(self.bitmap.len() == self.tree.len());
        if offset < self.bitmap.len() {
            if !self.bitmap[offset] {
                if let Some((l, r)) = children(offset) {
                    self.rehash0(l, false);
                    self.rehash0(r, false);
                    if let (Some(left_child), Some(right_child)) =
                        (self.get_hash(l), self.get_hash(r))
                    {
                        let res = blake3::guts::parent_cv(left_child, right_child, is_root);
                        self.bitmap[offset] = true;
                        self.tree[offset] = res;
                    }
                } else {
                    // would have to rehash from data
                }
            } else {
                // nothing to do
            }
        } else {
            if let Some(left_child) = left_child(offset) {
                self.rehash0(left_child, false);
            }
        }
    }
}

struct Inner {
    buffer: VecDeque<u8>,
    finished: bool,
}

pub struct AsyncSliceDecoder<R> {
    reader: R,
    decoder: SliceDecoder<IoBuffer>,
    buffer: IoBuffer,
}

impl<R: AsyncRead + Unpin> AsyncRead for AsyncSliceDecoder<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut tmp = [0; 4096];
        while !self.buffer.is_finished() && self.buffer.len() < 4096 {
            let mut rb = tokio::io::ReadBuf::new(&mut tmp);
            match Pin::new(&mut self.reader).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => {
                    if rb.filled().is_empty() {
                        self.buffer.finish();
                    } else {
                        self.buffer.write(rb.filled())?;
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        let max = buf.remaining().min(tmp.len());
        let n = self.decoder.read(&mut tmp[..max])?;
        buf.put_slice(&tmp[..n]);
        Poll::Ready(Ok(()))
    }
}

pub struct IoBuffer(Arc<RefCell<Inner>>);

impl IoBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Arc::new(RefCell::new(Inner {
            buffer: VecDeque::with_capacity(cap),
            finished: false,
        })))
    }

    pub fn len(&self) -> usize {
        self.0.borrow().buffer.len()
    }

    pub fn is_finished(&self) -> bool {
        self.0.borrow().finished
    }

    pub fn finish(&self) {
        let mut inner = self.0.borrow_mut();
        inner.finished = true;
    }
}

impl Read for IoBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut inner = self.0.borrow_mut();
        let read = inner.buffer.read(buf)?;
        if inner.buffer.is_empty() && !inner.finished {
            return Err(io::Error::new(io::ErrorKind::Other, "not finished"));
        }
        Ok(read)
    }
}

impl Write for IoBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.0.borrow_mut();
        if inner.finished {
            return Err(io::Error::new(io::ErrorKind::Other, "finished"));
        }
        inner.buffer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct BaoEncoder {
    encoder: blake3::guts::ChunkState,
    tree: Vec<blake3::Hash>,
    total_len: u64,
}

impl BaoEncoder {
    fn new() -> Self {
        Self {
            encoder: blake3::guts::ChunkState::new(0),
            tree: Vec::new(),
            total_len: 0,
        }
    }

    fn write(&mut self, data: &[u8]) {
        self.encoder.update(data);
        self.total_len += data.len() as u64;
    }
}

fn hash_leaf(offset: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    let mut hasher = blake3::guts::ChunkState::new(offset);
    hasher.update(data);
    hasher.finalize(is_root)
}

fn leaf_hashes_iter(data: &[u8]) -> impl Iterator<Item = (usize, blake3::Hash)> + '_ {
    let is_root = data.len() <= 1024;
    data.chunks(1024)
        .enumerate()
        .map(move |(i, data)| (i, hash_leaf(i as u64, data, is_root)))
}

fn leaf_hashes(data: &[u8]) -> Vec<blake3::Hash> {
    let is_root = data.len() <= 1024;
    if !data.is_empty() {
        data.chunks(1024)
            .enumerate()
            .map(|(i, data)| hash_leaf(i as u64, data, is_root))
            .collect::<Vec<_>>()
    } else {
        vec![hash_leaf(0, &[], is_root)]
    }
}

fn create_outboard(data: &[u8]) -> Vec<u8> {
    let mut outboard = Vec::new();
    outboard.extend_from_slice(&(data.len() as u64).to_be_bytes());
    let mut hashes = leaf_hashes(data);
    // while hashes.len() > 1 {
    //     for hash in &hashes {
    //         outboard.extend_from_slice(hash.as_bytes());
    //     }
    //     hashes = condense(&hashes);
    // }
    outboard
}

fn condense(hashes: &[blake3::Hash]) -> Vec<blake3::Hash> {
    let is_root = hashes.len() == 2;
    let mut condensed = Vec::new();
    for chunk in hashes.chunks(2) {
        let res = if chunk.len() == 1 {
            chunk[0]
        } else {
            let left_child = &chunk[0];
            let right_child = &chunk[1];
            blake3::guts::parent_cv(left_child, right_child, is_root)
        };
        condensed.push(res);
    }
    condensed
}

fn blake3_own(data: &[u8]) -> blake3::Hash {
    let mut hashes = leaf_hashes(data);
    while hashes.len() > 1 {
        hashes = condense(&hashes);
    }
    hashes[0]
}

use bao::encode;
use tokio::io::AsyncRead;

fn print_outboard(data: &[u8]) {
    println!("len:   {}", data.len());
    let (outboard, hash) = bao::encode::outboard(data);
    println!("outboard: {}", (outboard.len() - 8) / 64);
    println!("outboard: {}", hex::encode(outboard.as_slice()));
    println!("ob_hash:  {}", hex::encode(hash.as_bytes()));
    println!("blake3_h: {}", hex::encode(blake3::hash(&data).as_bytes()));
    if data.len() <= 1024 {
        println!(
            "manual: {}",
            hex::encode(hash_leaf(0, &data, true).as_bytes())
        );
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() <= 2048 {
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..], false);
        println!(
            "manual: {}",
            hex::encode(blake3::guts::parent_cv(&l0, &l1, true).as_bytes())
        );
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() <= 2048 + 1024 {
        println!("lopsided");
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..2048], false);
        let i0 = blake3::guts::parent_cv(&l0, &l1, false);
        let l2 = hash_leaf(2, &data[2048..], false);
        let root = blake3::guts::parent_cv(&i0, &l2, true);
        println!("manual: {}", hex::encode(root.as_bytes()));
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() < 2048 + 2048 {
        println!("even");
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..2048], false);
        let i0 = blake3::guts::parent_cv(&l0, &l1, false);
        let l2 = hash_leaf(2, &data[2048..3072], false);
        let l3 = hash_leaf(3, &data[3072..], false);
        let i1 = blake3::guts::parent_cv(&l2, &l3, false);
        let root = blake3::guts::parent_cv(&i0, &i1, true);
        println!("manual: {}", hex::encode(root.as_bytes()));
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    }
}

fn main() {
    for i in (0..(4096)).step_by(64) {
        let data = (0..i).map(|_| (i / 701) as u8).collect::<Vec<_>>();
        print_outboard(&data);
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::{max, min};

    use super::*;
    use proptest::prelude::*;
    proptest! {

        #[test]
        fn compare_hash(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
            let hash = blake3::hash(&data);
            let hash2 = blake3_own(&data);
            assert_eq!(hash, hash2);
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
    fn test_root() {
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
