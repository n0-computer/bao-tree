use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    ops::Range,
};

use futures::StreamExt;
use proptest::prelude::*;
use range_collections::RangeSet2;

use super::{
    canonicalize_range, canonicalize_range_owned,
    outboard::{PostOrderMemOutboard, PreOrderMemOutboard},
    BaoTree,
};
use crate::{
    bao_tree::{
        iter::{encode_ranges, encode_ranges_validated, NodeInfo},
        outboard::{self, Outboard, PostOrderMemOutboardRef},
        pre_order_offset_slow, PostOrderTreeIter, PostOrderTreeIterStack, TreeNode,
    },
    tree::{ByteNum, ChunkNum},
};

fn make_test_data(n: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(n);
    for i in 0..n {
        data.push((i / 1024) as u8);
    }
    data
}

fn bao_tree_blake3_impl(data: Vec<u8>) {
    let h1 = BaoTree::blake3_hash(&data);
    let h2 = blake3::hash(&data);
    assert_eq!(h1, h2);
}

/// Computes a reference post order outboard using the abao crate (chunk_group_log = 0) and the non-standard finalize_post_order function.
fn post_order_outboard_reference_2(data: &[u8]) -> PostOrderMemOutboard {
    let mut outboard = Vec::new();
    let cursor = std::io::Cursor::new(&mut outboard);
    let mut encoder = abao::encode::Encoder::new_outboard(cursor);
    encoder.write_all(&data).unwrap();
    // requires non standard fn finalize_post_order
    let hash = encoder.finalize_post_order().unwrap();
    // remove the length suffix
    outboard.truncate(outboard.len() - 8);
    PostOrderMemOutboard::new(hash, BaoTree::new(ByteNum(data.len() as u64), 0), outboard)
}

/// Computes a reference pre order outboard using the bao crate (chunk_group_log = 0) and then flips it to a post-order outboard.
fn post_order_outboard_reference(data: &[u8]) -> PostOrderMemOutboard {
    let mut outboard = Vec::new();
    let cursor = Cursor::new(&mut outboard);
    let mut encoder = bao::encode::Encoder::new_outboard(cursor);
    encoder.write_all(&data).unwrap();
    let hash = encoder.finalize().unwrap();
    let pre = PreOrderMemOutboard::new(hash, 0, outboard);
    pre.flip()
}

fn encode_slice_reference(data: &[u8], chunk_range: Range<ChunkNum>) -> (Vec<u8>, blake3::Hash) {
    let (outboard, hash) = abao::encode::outboard(data);
    let slice_start = chunk_range.start.to_bytes().0;
    let slice_len = (chunk_range.end - chunk_range.start).to_bytes().0;
    let mut encoder = abao::encode::SliceExtractor::new_outboard(
        Cursor::new(&data),
        Cursor::new(&outboard),
        slice_start,
        slice_len,
    );
    let mut res = Vec::new();
    encoder.read_to_end(&mut res).unwrap();
    (res, hash)
}

fn bao_tree_encode_slice_comparison_impl(data: Vec<u8>, mut range: Range<ChunkNum>) {
    let expected = encode_slice_reference(&data, range.clone()).0;
    let ob = BaoTree::outboard_post_order_mem(&data, 0);
    let hash = ob.root();
    let outboard = ob.outboard_with_suffix();
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let ranges = RangeSet2::from(range);
    let actual = BaoTree::encode_ranges(&data, &outboard, &ranges, 0);
    assert_eq!(expected.len(), actual.len());
    assert_eq!(expected, actual);

    // for this we have to canonicalize the range before
    let ranges = canonicalize_range_owned(&ranges, ByteNum(data.len() as u64));
    let mut actual2 = Vec::new();
    let ob = PostOrderMemOutboardRef::load(hash, &outboard, 0).unwrap();
    encode_ranges(Cursor::new(&data), ob, &ranges, Cursor::new(&mut actual2)).unwrap();
    assert_eq!(expected.len(), actual2.len());
    assert_eq!(expected, actual2);

    let mut actual3 = Vec::new();
    encode_ranges_validated(Cursor::new(&data), ob, &ranges, Cursor::new(&mut actual3)).unwrap();
    assert_eq!(expected.len(), actual3.len());
    assert_eq!(expected, actual3);
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn bao_tree_decode_slice_iter_impl(data: Vec<u8>, range: Range<u64>) {
    let range = ChunkNum(range.start)..ChunkNum(range.end);
    let (encoded, root) = encode_slice_reference(&data, range.clone());
    let size = ByteNum(data.len() as u64);
    let expected = data;
    let ranges = canonicalize_range_owned(&RangeSet2::from(range), size);
    let mut ec = Cursor::new(encoded);
    let mut scratch = vec![0u8; 2048];
    for item in BaoTree::decode_ranges_into_chunks(root, 0, &mut ec, &ranges, &mut scratch) {
        let (pos, slice) = item.unwrap();
        let pos = pos.to_usize();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

/// range is a range of chunks. Just using u64 for convenience in tests
async fn bao_tree_decode_slice_stream_impl(data: Vec<u8>, range: Range<u64>) {
    let range = ChunkNum(range.start)..ChunkNum(range.end);
    let (encoded, root) = encode_slice_reference(&data, range.clone());
    let size = ByteNum(data.len() as u64);
    let expected = data;
    let ranges = canonicalize_range_owned(&RangeSet2::from(range), size);
    let mut ec = Cursor::new(encoded);
    let mut stream = crate::bao_tree::stream::DecodeSliceStream::new(root, &ranges, 0, &mut ec);
    while let Some(item) = stream.next().await {
        let (pos, slice) = item.unwrap();
        let pos = pos.to_usize();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

fn bao_tree_outboard_comparison_impl(data: Vec<u8>) {
    let post1 = post_order_outboard_reference(&data);
    // let (expected, expected_hash) = post_order_outboard_reference_2(&data);
    let post2 = BaoTree::outboard_post_order_mem(&data, 0);
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
    let expected = BaoTree::blake3_hash(&td);
    for chunk_group_log in 0..4 {
        let ob = BaoTree::outboard_post_order_mem(&td, chunk_group_log);
        let hash = ob.root();
        let outboard = ob.outboard_with_suffix();
        assert_eq!(expected, hash);
        assert_eq!(
            ByteNum(outboard.len() as u64),
            BaoTree::outboard_size(ByteNum(td.len() as u64), chunk_group_log)
        );
    }
}

/// encodes the data as outboard with the given chunk_group_log, then uses that outboard to
/// encode a slice of the data, and compares the result to the original data
fn bao_tree_slice_roundtrip_test(data: Vec<u8>, mut range: Range<ChunkNum>, chunk_group_log: u8) {
    let ob = BaoTree::outboard_post_order_mem(&data, chunk_group_log);
    let root = ob.root();
    let outboard = ob.outboard_with_suffix();
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let encoded = BaoTree::encode_ranges(
        &data,
        &outboard,
        &RangeSet2::from(range.clone()),
        chunk_group_log,
    );
    let expected = data;
    let mut all_ranges = RangeSet2::empty();
    let mut ec = Cursor::new(encoded);
    let mut scratch = vec![0u8; 2048 << chunk_group_log];
    for item in BaoTree::decode_ranges_into_chunks(
        root,
        chunk_group_log,
        &mut ec,
        &RangeSet2::from(range),
        &mut scratch,
    ) {
        let (pos, slice) = item.unwrap();
        // compute all data ranges
        all_ranges |= RangeSet2::from(pos..pos + (slice.len() as u64));
        let pos = pos.to_usize();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

#[test]
fn bao_tree_slice_roundtrip_cases() {
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
    for chunk_group_log in 1..4 {
        for (count, range) in cases.clone() {
            bao_tree_slice_roundtrip_test(
                td(count),
                ChunkNum(range.start)..ChunkNum(range.end),
                chunk_group_log,
            );
        }
    }
}

#[test]
fn bao_tree_encode_slice_0() {
    use make_test_data as td;
    let cases = [
        // (0, 0..1),
        // (1, 0..1),
        // (1023, 0..1),
        // (1024, 0..1),
        (1025, 0..1),
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

#[tokio::test]
async fn bao_tree_decode_slice_stream_0() {
    use make_test_data as td;
    bao_tree_decode_slice_stream_impl(td(0), 0..1).await;
    bao_tree_decode_slice_stream_impl(td(1), 0..1).await;
    bao_tree_decode_slice_stream_impl(td(1023), 0..1).await;
    bao_tree_decode_slice_stream_impl(td(1024), 0..1).await;
    bao_tree_decode_slice_stream_impl(td(1025), 0..2).await;
    bao_tree_decode_slice_stream_impl(td(2047), 0..2).await;
    bao_tree_decode_slice_stream_impl(td(2048), 0..2).await;
    bao_tree_decode_slice_stream_impl(td(24 * 1024 + 1), 0..25).await;
    bao_tree_decode_slice_stream_impl(td(1025), 0..1).await;
    bao_tree_decode_slice_stream_impl(td(1025), 1..2).await;
    bao_tree_decode_slice_stream_impl(td(1024 * 17), 0..18).await;
}

#[test]
fn bao_tree_blake3_0() {
    use make_test_data as td;
    bao_tree_blake3_impl(td(0));
    bao_tree_blake3_impl(td(1));
    bao_tree_blake3_impl(td(1023));
    bao_tree_blake3_impl(td(1024));
    bao_tree_blake3_impl(td(1025));
    bao_tree_blake3_impl(td(2047));
    bao_tree_blake3_impl(td(2048));
    bao_tree_blake3_impl(td(2049));
    bao_tree_blake3_impl(td(10000));
}

fn size_and_slice_overlapping() -> impl Strategy<Value = (ByteNum, ChunkNum, ChunkNum)> {
    (0..32768u64).prop_flat_map(|len| {
        let len = ByteNum(len);
        let chunks = len.chunks();
        let slice_start = 0..=chunks.0.saturating_sub(1);
        let slice_len = 1..=(chunks.0 + 1);
        (
            Just(len),
            slice_start.prop_map(ChunkNum),
            slice_len.prop_map(ChunkNum),
        )
    })
}

fn size_and_slice() -> impl Strategy<Value = (ByteNum, ChunkNum, ChunkNum)> {
    (0..32768u64).prop_flat_map(|len| {
        let len = ByteNum(len);
        let chunks = len.chunks();
        let slice_start = 0..=chunks.0;
        let slice_len = 0..=chunks.0;
        (
            Just(len),
            slice_start.prop_map(ChunkNum),
            slice_len.prop_map(ChunkNum),
        )
    })
}

// create the mapping from a node number to the offset in the pre order traversal,
// using brute force lookup in the bao output
fn create_permutation_reference(size: usize) -> Vec<(TreeNode, usize)> {
    use make_test_data as td;
    let data = td(size);
    let po = BaoTree::outboard_post_order_mem(&data, 0);
    let post = po.outboard_with_suffix();
    let (mut pre, _) = bao::encode::outboard(data);
    pre.splice(..8, []);
    let map = pre
        .chunks_exact(64)
        .enumerate()
        .map(|(i, h)| (h, i))
        .collect::<HashMap<_, _>>();
    let tree = BaoTree::new(ByteNum(size as u64), 0);
    let mut res = Vec::new();
    for c in 0..tree.filled_size().0 {
        let node = TreeNode(c);
        if let Some(offset) = tree.post_order_offset(node).value() {
            let offset = offset.to_usize();
            let hash = post[offset * 64..offset * 64 + 64].to_vec();
            let index = *map.get(hash.as_slice()).unwrap();
            res.push((node, index));
        }
    }
    res
}

fn right_parent_count(node: TreeNode, len: TreeNode) -> u64 {
    assert!(node < len);
    let mut node = node;
    let mut count = 0;
    while let Some(parent) = node.parent() {
        if parent < node || parent >= len {
            break;
        }
        count += 1;
        node = parent;
    }
    count
}

fn parent_count(node: TreeNode, tree: BaoTree, x: u64) -> u64 {
    let len = tree.filled_size();
    let root = tree.root();
    assert!(node < len);
    let mut node = node;
    let mut count = 0;
    while let Some(parent) = node.parent() {
        if parent < len && parent.0 >= x {
            count += 1;
        }
        node = parent;
        if parent == root {
            break;
        }
    }
    count
}

fn parent_count2(node: TreeNode, tree: BaoTree) -> u64 {
    let len = tree.filled_size();
    let root = tree.root();
    assert!(node < len);
    assert!(root < len);
    let mut node = node;
    let mut count = 0;
    while let Some(parent) = node.parent() {
        if parent < len {
            count += 1;
        }
        node = parent;
        if parent == root {
            break;
        }
    }
    count
}

/// Counts the number of parents that `node` would have if it were in a tree of size `len`
fn parents_loop(node: u64, len: u64) -> u64 {
    assert!(node < len);
    let mut count = 0;
    let level = (!node).trailing_zeros();
    let mut span = 1u64 << level;
    let mut offset = node;
    loop {
        let pspan = span * 2;
        offset = if (offset & pspan) == 0 {
            offset + span
        } else {
            offset - span
        };
        if offset < len {
            count += 1;
        }
        if pspan >= len {
            break;
        }
        span = pspan;
    }
    count
}

fn compare_pre_order_outboard(case: usize) {
    let size = ByteNum(case as u64);
    let tree = BaoTree::new(size, 0);
    let perm = create_permutation_reference(case);
    for (k, v) in perm {
        let preo = k.block_range().start.0;
        let rpc = parent_count(k, tree, preo);
        let res = preo + rpc;
        let pc = parents_loop(k.0, tree.filled_size().0);
        let corr = preo.count_ones() as u64;
        let res2 = preo + pc - corr;
        // println!("{}\t{:b}\t:\t{}\t{}\t{}\t{}", k.0, k.0, v, preo, rpc, ld);
        // println!("{}\t{:b}\t:\t{}\t{}\t{}\t{}", k.0, k.0, v, preo, pc, res2);
        // println!("{}\t:\t{}\t{}", k.0, v, res);
        assert_eq!(v as u64, res);
        assert_eq!(v as u64, res2);
        assert_eq!(v as u64, pre_order_offset_slow(k.0, tree.filled_size().0));
    }
}

#[test]
fn pre_post_outboard_cases() {
    let cases = [20000];
    for case in cases {
        compare_pre_order_outboard(case);
    }
}

proptest! {

    #[test]
    fn flip(len in 0usize..32768) {
        if len == 0 {
            println!();
        }
        let data = make_test_data(len as usize);
        let post1 = post_order_outboard_reference(&data);
        let post2 = post_order_outboard_reference_2(&data);
        prop_assert_eq!(&post1, &post2);
        prop_assert_eq!(&post1, &post1.flip().flip());
    }

    #[test]
    fn bao_tree_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        bao_tree_blake3_impl(data);
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
    fn bao_tree_decode_slice_all_stream(len in 0..32768usize) {
        let data = make_test_data(len);
        let chunk_range = 0..(data.len() / 1024 + 1) as u64;
        futures::executor::block_on(bao_tree_decode_slice_stream_impl(data, chunk_range));
    }

    #[test]
    fn bao_tree_encode_slice_part_overlapping((len, start, size) in size_and_slice_overlapping()) {
        let data = make_test_data(len.to_usize());
        let chunk_range = start .. start + size;
        bao_tree_encode_slice_comparison_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_encode_slice_part_any((len, start, size) in size_and_slice()) {
        let data = make_test_data(len.to_usize());
        let chunk_range = start .. start + size;
        bao_tree_encode_slice_comparison_impl(data, chunk_range);
    }

    #[test]
    fn bao_tree_outboard_comparison(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        bao_tree_outboard_comparison_impl(data);
    }

    #[test]
    fn bao_tree_slice_roundtrip((len, start, size) in size_and_slice_overlapping(), level in 0u8..6) {
        let data = make_test_data(len.to_usize());
        let chunk_range = start .. start + size;
        bao_tree_slice_roundtrip_test(data, chunk_range, level);
    }

    #[test]
    fn tree_iterator_comparison(len in 0u64..100000) {
        let tree = BaoTree::new(ByteNum(len), 0);
        let iter1 = tree.iterate_reference();
        let iter2 = PostOrderTreeIterStack::new(tree).collect::<Vec<_>>();
        let iter3 = PostOrderTreeIter::new(tree).collect::<Vec<_>>();
        prop_assert_eq!(&iter1, &iter2);
        prop_assert_eq!(&iter1, &iter3);
    }

    #[test]
    fn partial_iterator_comparison((len, start, size) in size_and_slice_overlapping()) {
        let tree = BaoTree::new(len, 0);
        let chunk_range = start .. start + size;
        let rs = RangeSet2::from(chunk_range.clone());
        let iter1 = tree.iterate_part_preorder_reference(&rs, 0);
        let iter2 = tree.iterate_part_preorder_ref(&rs, 0).collect::<Vec<_>>();
        prop_assert_eq!(&iter1, &iter2);
    }

    #[test]
    fn pre_post_outboard(n in 0usize..1000000) {
        compare_pre_order_outboard(n);
    }
}

#[test]
fn bao_tree_iterate_all() {
    let tree = BaoTree::new(ByteNum(1024 * 15), 0);
    println!("{}", tree.outboard_hash_pairs());
    for node in tree.iterate() {
        println!(
            "{:#?}\t{}\t{:?}",
            node,
            tree.is_sealed(node),
            tree.post_order_offset(node)
        );
    }
}

#[test]
fn bao_tree_iterate_part() {
    let tree = BaoTree::new(ByteNum(1024 * 5), 0);
    println!();
    let spec = RangeSet2::from(ChunkNum(2)..ChunkNum(3));
    for NodeInfo { node, .. } in tree.iterate_part_preorder_ref(&spec, 0) {
        println!(
            "{:#?}\t{}\t{:?}",
            node,
            tree.is_sealed(node),
            tree.post_order_offset(node)
        );
    }
}
