//! Compatibility with the [bao](https://crates.io/crates/bao) crate: encoding,
//! decoding and outboards at block size 0 must match the original format.
use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    ops::Range,
};

use proptest::prelude::*;
use range_collections::RangeSet2;

use crate::{
    blake3,
    io::{
        outboard::{PostOrderMemOutboard, PreOrderMemOutboard},
        sync::{encode_ranges, encode_ranges_validated, DecodeResponseIter, Outboard},
        BaoContentItem, Leaf,
    },
    rec::{encode_ranges_reference, make_test_data},
    BaoTree, BlockSize, ChunkNum, ChunkRanges, ChunkRangesRef, TreeNode,
};

/// Decode encoded ranges given the root hash
pub fn decode_ranges_into_chunks<'a>(
    root: blake3::Hash,
    tree: BaoTree,
    encoded: impl Read + 'a,
    ranges: &'a ChunkRangesRef,
) -> std::io::Result<impl Iterator<Item = std::io::Result<(u64, Vec<u8>)>> + 'a> {
    let iter = DecodeResponseIter::new(root, tree, encoded, ranges);
    Ok(iter.filter_map(|item| match item {
        Ok(item) => {
            if let BaoContentItem::Leaf(Leaf { offset, data }) = item {
                Some(Ok((offset, data.to_vec())))
            } else {
                None
            }
        }
        Err(e) => Some(Err(e.into())),
    }))
}

/// Computes a reference pre order outboard using the bao crate (chunk_group_log = 0) and then flips it to a post-order outboard.
fn post_order_outboard_bao(data: &[u8]) -> PostOrderMemOutboard {
    let mut outboard = Vec::new();
    let cursor = Cursor::new(&mut outboard);
    let mut encoder = bao::encode::Encoder::new_outboard(cursor);
    encoder.write_all(data).unwrap();
    let hash = encoder.finalize().unwrap();
    let hash = blake3::Hash::from(*hash.as_bytes());
    let tree = BaoTree::new(data.len() as u64, BlockSize::ZERO);
    outboard.splice(..8, []);
    let pre = PreOrderMemOutboard {
        root: hash,
        tree,
        data: outboard,
    };
    pre.flip()
}

fn encode_slice_bao(data: &[u8], chunk_range: Range<ChunkNum>) -> (Vec<u8>, blake3::Hash) {
    let (outboard, hash) = bao::encode::outboard(data);
    let slice_start = chunk_range.start.to_bytes();
    let slice_len = (chunk_range.end - chunk_range.start).to_bytes();
    let mut encoder = bao::encode::SliceExtractor::new_outboard(
        Cursor::new(&data),
        Cursor::new(&outboard),
        slice_start,
        slice_len,
    );
    let mut res = Vec::new();
    encoder.read_to_end(&mut res).unwrap();
    res.splice(..8, []);
    let hash = blake3::Hash::from(*hash.as_bytes());
    (res, hash)
}

fn bao_tree_encode_slice_comparison_impl(data: Vec<u8>, mut range: Range<ChunkNum>) {
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let expected = encode_slice_bao(&data, range.clone()).0;

    let ob = PostOrderMemOutboard::create(&data, BlockSize::ZERO);
    let ranges = ChunkRanges::from(range);
    let actual = encode_ranges_reference(&data, &ranges, BlockSize::ZERO).0;
    assert_eq!(expected.len(), actual.len());
    assert_eq!(expected, actual);

    let content_range = ChunkRanges::from(..ChunkNum::chunks(data.len() as u64));
    if !content_range.is_superset(&ranges) {
        // the behaviour of bao/abao and us is different in this case.
        // if the query ranges are non empty outside the content range, we will return
        // the last chunk of the content range, while bao/abao will not.
        //
        // this is intentional. it provides a way to get the size of a file
        return;
    }
    let mut actual2 = Vec::new();
    encode_ranges(&data, &ob, &ranges, Cursor::new(&mut actual2)).unwrap();
    assert_eq!(expected.len(), actual2.len());
    assert_eq!(expected, actual2);

    let mut actual3 = Vec::new();
    encode_ranges_validated(&data, &ob, &ranges, Cursor::new(&mut actual3)).unwrap();
    assert_eq!(expected.len(), actual3.len());
    assert_eq!(expected, actual3);
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn bao_tree_decode_slice_iter_impl(data: Vec<u8>, range: Range<u64>) {
    let tree = BaoTree::new(data.len() as u64, BlockSize::ZERO);
    let range = ChunkNum(range.start)..ChunkNum(range.end);
    let (encoded, root) = encode_slice_bao(&data, range.clone());
    let expected = data;
    let ranges = ChunkRanges::from(range);
    let mut ec = Cursor::new(encoded);
    for item in decode_ranges_into_chunks(root, tree, &mut ec, &ranges).unwrap() {
        let (pos, slice) = item.unwrap();
        let pos = pos.try_into().unwrap();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

fn bao_tree_outboard_comparison_impl(data: Vec<u8>) {
    let post1 = post_order_outboard_bao(&data);
    // let (expected, expected_hash) = post_order_outboard_reference_2(&data);
    let post2 = PostOrderMemOutboard::create(&data, BlockSize::ZERO);
    assert_eq!(post1, post2);
}

#[test]
fn bao_tree_outboard_comparison_cases() {
    use make_test_data as td;
    bao_tree_outboard_comparison_impl(td(0));
    bao_tree_outboard_comparison_impl(td(1));
    bao_tree_outboard_comparison_impl(td(1023));
    bao_tree_outboard_comparison_impl(td(1024));
    bao_tree_outboard_comparison_impl(td(1025));
    bao_tree_outboard_comparison_impl(td(2047));
    bao_tree_outboard_comparison_impl(td(2048));
    bao_tree_outboard_comparison_impl(td(2049));
    bao_tree_outboard_comparison_impl(td(10000));
    bao_tree_outboard_comparison_impl(td(20000));
    bao_tree_outboard_comparison_impl(td(24577));
}

#[test]
fn bao_tree_outboard_levels() {
    use make_test_data as td;
    let td = td(1024 * 32);
    let expected = blake3::hash(&td);
    for chunk_group_log in 0..4 {
        let block_size = BlockSize(chunk_group_log);
        let ob = PostOrderMemOutboard::create(&td, block_size);
        let hash = ob.root();
        let outboard = ob.into_inner_with_suffix();
        assert_eq!(expected, hash);
        assert_eq!(
            outboard.len() as u64,
            BaoTree::new(td.len() as u64, block_size).outboard_size() + 8
        );
    }
}

/// encodes the data as outboard with the given chunk_group_log, then uses that outboard to
/// encode a slice of the data, and compares the result to the original data
fn bao_tree_slice_roundtrip_test(data: Vec<u8>, mut range: Range<ChunkNum>, block_size: BlockSize) {
    let root = blake3::hash(&data);
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let encoded = encode_ranges_reference(&data, &ChunkRanges::from(range.clone()), block_size).0;
    let expected = data.clone();
    let mut all_ranges: range_collections::RangeSet<[u64; 2]> = RangeSet2::empty();
    let mut ec = Cursor::new(encoded);
    let tree = BaoTree::new(data.len() as u64, block_size);
    for item in decode_ranges_into_chunks(root, tree, &mut ec, &ChunkRanges::from(range)).unwrap() {
        let (pos, slice) = item.unwrap();
        // compute all data ranges
        all_ranges |= RangeSet2::from(pos..pos + (slice.len() as u64));
        let pos = pos.try_into().unwrap();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

#[test]
fn bao_tree_slice_roundtrip_cases() {
    use make_test_data as td;
    let cases = [
        // (0, 0..1),
        // (1, 0..1),
        // (1023, 0..1),
        // (1024, 0..1),
        // (1025, 0..1),
        // (2047, 0..1),
        // (2048, 0..1),
        // (10000, 0..1),
        // (20000, 0..1),
        // (24 * 1024 + 1, 0..25),
        // (1025, 1..2),
        // (2047, 1..2),
        // (2048, 1..2),
        // (10000, 1..2),
        // (20000, 1..2),
        (1025, 0..2),
    ];
    for chunk_group_log in 1..4 {
        let block_size = BlockSize(chunk_group_log);
        for (count, range) in cases.clone() {
            bao_tree_slice_roundtrip_test(
                td(count),
                ChunkNum(range.start)..ChunkNum(range.end),
                block_size,
            );
        }
    }
}

#[test]
fn bao_tree_encode_slice_0() {
    use make_test_data as td;
    let cases = [
        (0, 0..1),
        (1, 0..1),
        (1023, 0..1),
        (1024, 0..1),
        (1025, 0..1),
        (2047, 0..1),
        (2048, 0..1),
        (10000, 0..1),
        (20000, 0..1),
        (24 * 1024 + 1, 0..25),
        (1025, 1..2),
        (2047, 1..2),
        (2048, 1..2),
        (10000, 1..2),
        (20000, 1..2),
    ];
    for (count, range) in cases {
        bao_tree_encode_slice_comparison_impl(
            td(count),
            ChunkNum(range.start)..ChunkNum(range.end),
        );
    }
}

#[test]
fn bao_tree_decode_slice_0() {
    use make_test_data as td;
    bao_tree_decode_slice_iter_impl(td(0), 0..1);
    bao_tree_decode_slice_iter_impl(td(1), 0..1);
    bao_tree_decode_slice_iter_impl(td(1023), 0..1);
    bao_tree_decode_slice_iter_impl(td(1024), 0..1);
    bao_tree_decode_slice_iter_impl(td(1025), 0..2);
    bao_tree_decode_slice_iter_impl(td(2047), 0..2);
    bao_tree_decode_slice_iter_impl(td(2048), 0..2);
    bao_tree_decode_slice_iter_impl(td(24 * 1024 + 1), 0..25);
    bao_tree_decode_slice_iter_impl(td(1025), 0..1);
    bao_tree_decode_slice_iter_impl(td(1025), 1..2);
    bao_tree_decode_slice_iter_impl(td(1024 * 17), 0..18);
}

// create the mapping from a node number to the offset in the pre order traversal,
// using brute force lookup in the bao output
fn create_permutation_reference(size: usize) -> Vec<(TreeNode, usize)> {
    use make_test_data as td;
    let data = td(size);
    let po = PostOrderMemOutboard::create(&data, BlockSize::ZERO);
    let post = po.into_inner_with_suffix();
    let (mut pre, _) = bao::encode::outboard(data);
    pre.splice(..8, []);
    let map = pre
        .chunks_exact(64)
        .enumerate()
        .map(|(i, h)| (h, i))
        .collect::<HashMap<_, _>>();
    let tree = BaoTree::new(size as u64, BlockSize::ZERO);
    let mut res = Vec::new();
    for c in 0..tree.filled_size().0 {
        let node = TreeNode(c);
        if let Some(offset) = tree.post_order_offset(node) {
            let offset = usize::try_from(offset.value()).unwrap();
            let hash = post[offset * 64..offset * 64 + 64].to_vec();
            let index = *map.get(hash.as_slice()).unwrap();
            res.push((node, index));
        }
    }
    res
}

/// The pre order offsets of the nodes are their positions in the pre order
/// outboard of the bao crate.
fn compare_pre_order_outboard(size: usize) {
    let tree = BaoTree::new(size as u64, BlockSize::ZERO);
    for (node, offset) in create_permutation_reference(size) {
        assert_eq!(tree.pre_order_offset(node), Some(offset as u64), "{node:?}");
    }
}

#[test]
fn pre_order_outboard_cases() {
    for size in [0, 1, 1024, 1025, 78 * 1024, 256 * 1024 + 1] {
        compare_pre_order_outboard(size);
    }
}

fn size_and_slice_overlapping() -> impl Strategy<Value = (u64, ChunkNum, ChunkNum)> {
    (0..32768u64).prop_flat_map(|len| {
        let chunks = ChunkNum::chunks(len);
        let slice_start = 0..=chunks.0.saturating_sub(1);
        let slice_len = 1..=(chunks.0 + 1);
        (
            Just(len),
            slice_start.prop_map(ChunkNum),
            slice_len.prop_map(ChunkNum),
        )
    })
}

fn size_and_slice() -> impl Strategy<Value = (u64, ChunkNum, ChunkNum)> {
    (0..32768u64).prop_flat_map(|len| {
        let chunks = ChunkNum::chunks(len);
        let slice_start = 0..=chunks.0;
        let slice_len = 0..=chunks.0;
        (
            Just(len),
            slice_start.prop_map(ChunkNum),
            slice_len.prop_map(ChunkNum),
        )
    })
}

#[cfg(feature = "tokio_fsm")]
mod fsm_tests {

    use super::*;
    use crate::{io::fsm::*, rec::make_test_data};

    /// range is a range of chunks. Just using u64 for convenience in tests
    async fn bao_tree_decode_slice_fsm_impl(data: Vec<u8>, range: Range<u64>) {
        let tree = BaoTree::new(data.len() as u64, BlockSize::ZERO);
        let range = ChunkNum(range.start)..ChunkNum(range.end);
        let (encoded, root) = encode_slice_bao(&data, range.clone());
        let expected = data;
        let ranges = ChunkRanges::from(range);
        let encoded = Cursor::new(encoded.as_slice());
        let mut reading = ResponseDecoder::new(root, ranges, tree, encoded);
        while let ResponseDecoderNext::More((next_state, item)) = reading.next().await {
            if let BaoContentItem::Leaf(Leaf { offset, data }) = item.unwrap() {
                let pos = offset.try_into().unwrap();
                assert_eq!(expected[pos..pos + data.len()], *data);
            }
            reading = next_state;
        }
    }

    #[tokio::test]
    async fn bao_tree_decode_slice_fsm_0() {
        use make_test_data as td;
        bao_tree_decode_slice_fsm_impl(td(0), 0..1).await;
        bao_tree_decode_slice_fsm_impl(td(1), 0..1).await;
        bao_tree_decode_slice_fsm_impl(td(1023), 0..1).await;
        bao_tree_decode_slice_fsm_impl(td(1024), 0..1).await;
        bao_tree_decode_slice_fsm_impl(td(1025), 0..2).await;
        bao_tree_decode_slice_fsm_impl(td(2047), 0..2).await;
        bao_tree_decode_slice_fsm_impl(td(2048), 0..2).await;
        bao_tree_decode_slice_fsm_impl(td(24 * 1024 + 1), 0..25).await;
        bao_tree_decode_slice_fsm_impl(td(1025), 0..1).await;
        bao_tree_decode_slice_fsm_impl(td(1025), 1..2).await;
        bao_tree_decode_slice_fsm_impl(td(1024 * 17), 0..18).await;
    }

    ::proptest::proptest! {
        #[test]
        fn bao_tree_decode_slice_all_stream(len in 0..32768usize) {
            let data = make_test_data(len);
            let chunk_range = 0..(data.len() / 1024 + 1) as u64;
            tokio::runtime::Runtime::new().unwrap().block_on(bao_tree_decode_slice_fsm_impl(data, chunk_range));
        }
    }
}

::proptest::proptest! {
    #[test]
    fn flip(len in 0usize..100000) {
        let data = make_test_data(len);
        let post = post_order_outboard_bao(&data);
        prop_assert_eq!(&post, &post.flip().flip());
    }

    #[test]
    fn bao_tree_encode_slice_all(len in 0..32768usize) {
        let data = make_test_data(len);
        let chunk_range = ChunkNum(0)..ChunkNum((data.len() / 1024 + 1) as u64);
        bao_tree_encode_slice_comparison_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_decode_slice_all(len in 0..32768usize) {
        let data = make_test_data(len);
        let chunk_range = 0..(data.len() / 1024 + 1) as u64;
        bao_tree_decode_slice_iter_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_encode_slice_part_overlapping((len, start, size) in size_and_slice_overlapping()) {
        let data = make_test_data(len as usize);
        let chunk_range = start .. start + size;
        bao_tree_encode_slice_comparison_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_encode_slice_part_any((len, start, size) in size_and_slice()) {
        let data = make_test_data(len.try_into().unwrap());
        let chunk_range = start .. start + size;
        bao_tree_encode_slice_comparison_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_outboard_comparison(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        bao_tree_outboard_comparison_impl(data);
    }

    #[test]
    fn bao_tree_slice_roundtrip((len, start, size) in size_and_slice_overlapping(), level in 0u8..6) {
        let level = BlockSize(level);
        let data = make_test_data(len as usize);
        let chunk_range = start .. start + size;
        bao_tree_slice_roundtrip_test(data, chunk_range, level);
    }

    #[test]
    fn pre_post_outboard(n in 0usize..100_000) {
        compare_pre_order_outboard(n);
    }
}
