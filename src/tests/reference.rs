//! Reference implementations and helpers shared by the tests.
//!
//! The reference implementations are simple and recursive. The tests compare
//! the real implementations to them, and the tests at the end compare them to
//! the bao crate.
use std::ops::Range;

use range_collections::{range_set::RangeSetEntry, RangeSet2};

use crate::{
    blake3,
    rec::{encode_selected_rec, truncate_ranges},
    split_inner, BaoChunk, BaoTree, BlockSize, ChunkNum, ChunkRanges, ChunkRangesRef, TreeNode,
};

/// Select nodes relevant to a query
///
/// This is the receive side equivalent of [bao_encode_selected_recursive].
/// It does not do any hashing, but just emits the nodes that are relevant to a query.
///
/// The tree is given as `start_chunk`, `size` and `is_root`.
///
/// To traverse an entire tree, use `0` for `start_chunk`, `tree.size` for `size` and
/// `true` for `is_root`.
///
/// `tree_level` is the smallest level the iterator will emit. Set this to `tree.block_size`
/// `min_full_level` is the smallest level that will be emitted as a leaf if it is fully
/// within the query range.
///
/// To disable chunk groups entirely, just set both `tree_level` and `min_full_level` to 0.
pub(crate) fn select_nodes_rec<'a>(
    start_chunk: ChunkNum,
    size: usize,
    is_root: bool,
    ranges: &'a ChunkRangesRef,
    tree_level: u32,
    min_full_level: u32,
    emit: &mut impl FnMut(BaoChunk<&'a ChunkRangesRef>),
) {
    if ranges.is_empty() {
        return;
    }
    use blake3::CHUNK_LEN;

    if size <= CHUNK_LEN {
        emit(BaoChunk::Leaf {
            start_chunk,
            size,
            is_root,
            ranges,
        });
    } else {
        let chunks: usize = size / CHUNK_LEN + (size % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        // chunks is always a power of two, 2 for level 0
        // so we must subtract 1 to get the level, and this is also safe
        let level = chunks.trailing_zeros() - 1;
        let full = ranges.is_all();
        if (level < tree_level) || (full && level < min_full_level) {
            // we are allowed to just emit the entire data as a leaf
            emit(BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
                ranges,
            });
        } else {
            // split in half and recurse
            assert!(start_chunk.0 % 2 == 0);
            let mid = chunks / 2;
            let mid_bytes = mid * CHUNK_LEN;
            let mid_chunk = start_chunk + (mid as u64);
            let (l_ranges, r_ranges) = split_inner(ranges, start_chunk, mid_chunk);
            let node = TreeNode::from_start_chunk_and_level(start_chunk, BlockSize(level as u8));
            emit(BaoChunk::Parent {
                node,
                is_root,
                left: !l_ranges.is_empty(),
                right: !r_ranges.is_empty(),
                ranges,
            });
            // recurse to the left and right to compute the hashes and emit data
            select_nodes_rec(
                start_chunk,
                mid_bytes,
                false,
                l_ranges,
                tree_level,
                min_full_level,
                emit,
            );
            select_nodes_rec(
                mid_chunk,
                size - mid_bytes,
                false,
                r_ranges,
                tree_level,
                min_full_level,
                emit,
            );
        }
    }
}

pub(crate) fn bao_outboard_reference(data: &[u8]) -> (Vec<u8>, blake3::Hash) {
    let mut res = Vec::new();
    res.extend_from_slice(&(data.len() as u64).to_le_bytes());
    let hash = encode_selected_rec(
        ChunkNum(0),
        data,
        true,
        &ChunkRanges::all(),
        0,
        false,
        &mut res,
        crate::HashMode::Standard,
    );
    (res, hash)
}

pub(crate) fn bao_encode_reference(data: &[u8]) -> (Vec<u8>, blake3::Hash) {
    let mut res = Vec::new();
    res.extend_from_slice(&(data.len() as u64).to_le_bytes());
    let hash = encode_selected_rec(
        ChunkNum(0),
        data,
        true,
        &ChunkRanges::all(),
        0,
        true,
        &mut res,
        crate::HashMode::Standard,
    );
    (res, hash)
}

/// Reference implementation of the response iterator, using just the simple recursive
/// implementation [select_nodes_rec].
pub(crate) fn partial_chunk_iter_reference(
    tree: BaoTree,
    ranges: &ChunkRangesRef,
    min_full_level: u8,
) -> Vec<BaoChunk<&ChunkRangesRef>> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.try_into().unwrap(),
        true,
        ranges,
        tree.block_size.to_u32(),
        min_full_level as u32,
        &mut |x| res.push(x),
    );
    res
}

/// Reference implementation of the response iterator, using just the simple recursive
/// implementation [select_nodes_rec].
pub(crate) fn response_iter_reference(tree: BaoTree, ranges: &ChunkRangesRef) -> Vec<BaoChunk> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.try_into().unwrap(),
        true,
        ranges,
        0,
        tree.block_size.to_u32(),
        &mut |x| res.push(x.without_ranges()),
    );
    res
}

/// A reference implementation of [PreOrderPartialChunkIterRef] that just uses the recursive
/// implementation [crate::iter::select_nodes_rec].
///
/// This is not a proper iterator since it computes all elements at once, but it is useful for
/// testing.
#[derive(Debug)]
pub struct ReferencePreOrderPartialChunkIterRef<'a> {
    iter: std::vec::IntoIter<BaoChunk<&'a ChunkRangesRef>>,
}

impl<'a> ReferencePreOrderPartialChunkIterRef<'a> {
    /// Create a new iterator over the tree.
    pub fn new(tree: BaoTree, ranges: &'a ChunkRangesRef, min_full_level: u8) -> Self {
        let iter = partial_chunk_iter_reference(tree, ranges, min_full_level).into_iter();
        Self { iter }
    }
}

impl<'a> Iterator for ReferencePreOrderPartialChunkIterRef<'a> {
    type Item = BaoChunk<&'a ChunkRangesRef>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Make test data that has each chunk filled with the cunk number as an u8
///
/// This makes sure that every chunk has a different hash, and makes it easy
/// to see pages in hex dumps.
pub(crate) fn make_test_data(n: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(n);
    for i in 0..n {
        data.push((i / 1024) as u8);
    }
    data
}

/// Compute the union of an iterator of ranges. The ranges should be non-overlapping, otherwise
/// the result is None
pub fn range_union<K: RangeSetEntry>(
    ranges: impl IntoIterator<Item = Range<K>>,
) -> Option<RangeSet2<K>> {
    let mut res = RangeSet2::empty();
    for r in ranges.into_iter() {
        let part = RangeSet2::from(r);
        if part.intersects(&res) {
            return None;
        }
        res |= part;
    }
    Some(res)
}

pub fn get_leaf_ranges<R>(
    iter: impl IntoIterator<Item = BaoChunk<R>>,
) -> impl Iterator<Item = Range<u64>> {
    iter.into_iter().filter_map(|e| {
        if let BaoChunk::Leaf {
            start_chunk, size, ..
        } = e
        {
            let start = start_chunk.to_bytes();
            let end = start + (size as u64);
            Some(start..end)
        } else {
            None
        }
    })
}

pub fn encode_ranges_reference(
    data: &[u8],
    ranges: &ChunkRangesRef,
    block_size: BlockSize,
) -> (Vec<u8>, blake3::Hash) {
    let mut res = Vec::new();
    let size = data.len() as u64;
    // canonicalize the ranges
    let ranges = truncate_ranges(ranges, size);
    let hash = encode_selected_rec(
        ChunkNum(0),
        data,
        true,
        ranges,
        block_size.to_u32(),
        true,
        &mut res,
        crate::HashMode::Standard,
    );
    (res, hash)
}

/// Check that l and r of a 2-tuple are equal
#[macro_export]
macro_rules! assert_tuple_eq {
    ($tuple:expr) => {
        assert_eq!($tuple.0, $tuple.1);
    };
}

/// Check that l and r of a 2-tuple are equal
#[macro_export]
macro_rules! prop_assert_tuple_eq {
    ($tuple:expr) => {
        let (a, b) = $tuple;
        ::proptest::prop_assert_eq!(a, b);
    };
}

/// These tests are not really tests for the crate itself, but tests for the reference
/// implementation that compare it to the bao crate.
mod bao_comparison {
    use std::{
        io::{Cursor, Read},
        ops::Range,
    };

    use proptest::prelude::*;

    use super::{
        bao_encode_reference, bao_outboard_reference, encode_ranges_reference, make_test_data,
    };
    use crate::{BlockSize, ChunkNum, ChunkRanges};

    fn size_and_slice() -> impl Strategy<Value = (usize, Range<usize>)> {
        (1..100000usize)
            .prop_flat_map(|len| (Just(len), (0..len), (0..len)))
            .prop_map(|(len, a, b)| {
                let start = a.min(b);
                let end = a.max(b);
                (len, start..end)
            })
    }

    fn size_and_start() -> impl Strategy<Value = (usize, usize)> {
        (1..100000usize).prop_flat_map(|len| (Just(len), (0..len)))
    }

    /// Compare the outboard from the bao crate to the outboard from the reference implementation
    ///
    /// Since bao does not support chunk groups, this can only be done for chunk group size 0.
    #[test_strategy::proptest]
    fn bao_outboard_comparison(#[strategy(0usize..100000)] size: usize) {
        let data = make_test_data(size);
        let (expected_outboard, expected_hash) = bao::encode::outboard(&data);
        let (actual_outboard, actual_hash) = bao_outboard_reference(&data);
        prop_assert_eq!(expected_outboard, actual_outboard);
        prop_assert_eq!(expected_hash.as_bytes(), actual_hash.as_bytes());
    }

    /// Compare the encoded from the bao crate to the encoded from the reference implementation
    ///
    /// Since bao does not support chunk groups, this can only be done for chunk group size 0.
    #[test_strategy::proptest]
    fn bao_encode_comparison(#[strategy(0usize..100000)] size: usize) {
        let data = make_test_data(size);
        let (expected_encoded, expected_hash) = bao::encode::encode(&data);
        let (actual_encoded, actual_hash) = bao_encode_reference(&data);
        prop_assert_eq!(expected_encoded, actual_encoded);
        prop_assert_eq!(expected_hash.as_bytes(), actual_hash.as_bytes());
    }

    /// Compare a random encoded slice from the bao crate to the encoded from the reference implementation
    ///
    /// Since bao does not support chunk groups, this can only be done for chunk group size 0.
    #[test_strategy::proptest]
    fn bao_encode_slice_comparison(
        #[strategy(size_and_slice())] size_and_slice: (usize, Range<usize>),
    ) {
        let (size, Range { start, end }) = size_and_slice;
        let data = make_test_data(size);
        let (outboard, _) = bao::encode::outboard(&data);
        let mut encoder = bao::encode::SliceExtractor::new_outboard(
            Cursor::new(&data),
            Cursor::new(&outboard),
            start as u64,
            (end - start) as u64,
        );
        let mut expected_encoded = Vec::new();
        encoder.read_to_end(&mut expected_encoded).unwrap();
        let chunk_start = ChunkNum::full_chunks(start as u64);
        let chunk_end = ChunkNum::chunks(end as u64).max(chunk_start + 1);
        let ranges = ChunkRanges::from(chunk_start..chunk_end);
        let mut actual_encoded = encode_ranges_reference(&data, &ranges, BlockSize::ZERO).0;
        actual_encoded.splice(..0, size.to_le_bytes().into_iter());
        prop_assert_eq!(expected_encoded, actual_encoded);
    }

    /// Decode a single chunk with the reference encode impl [encode_ranges_reference] and
    /// decode it using the bao crate
    #[test_strategy::proptest]
    fn bao_decode_slice_comparison(#[strategy(size_and_start())] size_and_start: (usize, usize)) {
        // ignore the end, we always want to encode a single chunk
        let (size, start) = size_and_start;
        let end = start + 1;
        let data = make_test_data(size);
        let chunk_start = ChunkNum::full_chunks(start as u64);
        let chunk_end = ChunkNum::chunks(end as u64).max(chunk_start + 1);
        let ranges = ChunkRanges::from(chunk_start..chunk_end);
        let (mut encoded, hash) = encode_ranges_reference(&data, &ranges, BlockSize::ZERO);
        encoded.splice(..0, size.to_le_bytes().into_iter());
        let bao_hash = bao::Hash::from(*hash.as_bytes());
        let mut decoder =
            bao::decode::SliceDecoder::new(Cursor::new(&encoded), &bao_hash, start as u64, 1);
        let mut expected_decoded = Vec::new();
        decoder.read_to_end(&mut expected_decoded).unwrap();
        let actual_decoded = data[start..end.min(data.len())].to_vec();
        prop_assert_eq!(expected_decoded, actual_decoded);
    }
}
