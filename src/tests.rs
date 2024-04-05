use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    ops::Range,
};

use bytes::Bytes;
use proptest::prelude::*;
use range_collections::RangeSet2;

use crate::{
    assert_tuple_eq, blake3,
    io::{full_chunk_groups, outboard::PreOrderMemOutboard, sync::Outboard, BaoContentItem, Leaf},
    iter::{PostOrderChunkIter, PreOrderPartialIterRef, ResponseIterRef},
    prop_assert_tuple_eq,
    rec::{
        encode_ranges_reference, encode_selected_rec, make_test_data, range_union, truncate_ranges,
        ReferencePreOrderPartialChunkIterRef,
    },
    recursive_hash_subtree, split, ChunkRanges, ChunkRangesRef, ResponseIter,
};

use super::{
    io::outboard::PostOrderMemOutboard,
    io::sync::{encode_ranges, encode_ranges_validated, DecodeResponseIter},
    iter::{BaoChunk, NodeInfo},
    pre_order_offset_loop,
    tree::ChunkNum,
    BaoTree, BlockSize, TreeNode,
};

fn read_len(mut from: impl std::io::Read) -> std::io::Result<u64> {
    let mut buf = [0; 8];
    from.read_exact(&mut buf)?;
    let len = u64::from_le_bytes(buf);
    Ok(len)
}

/// Compute the blake3 hash for the given data,
///
/// using blake3_hash_inner which is used in hash_block.
fn blake3_hash(data: impl AsRef<[u8]>) -> blake3::Hash {
    blake3::hash(data.as_ref())
}

fn bao_tree_blake3_impl(data: Vec<u8>) -> (blake3::Hash, blake3::Hash) {
    let expected = blake3::hash(&data);
    let actual = blake3_hash(&data);
    (expected, actual)
}

/// Computes a reference pre order outboard using the bao crate (chunk_group_log = 0) and then flips it to a post-order outboard.
fn post_order_outboard_reference(data: &[u8]) -> PostOrderMemOutboard {
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

fn encode_slice_reference(data: &[u8], chunk_range: Range<ChunkNum>) -> (Vec<u8>, blake3::Hash) {
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
    let hash = blake3::Hash::from(*hash.as_bytes());
    (res, hash)
}

fn bao_tree_encode_slice_comparison_impl(data: Vec<u8>, mut range: Range<ChunkNum>) {
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let expected = encode_slice_reference(&data, range.clone()).0;
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
    let range = ChunkNum(range.start)..ChunkNum(range.end);
    let (encoded, root) = encode_slice_reference(&data, range.clone());
    let expected = data;
    let ranges = ChunkRanges::from(range);
    let mut ec = Cursor::new(encoded);
    for item in decode_ranges_into_chunks(root, BlockSize::ZERO, &mut ec, &ranges).unwrap() {
        let (pos, slice) = item.unwrap();
        let pos = pos.try_into().unwrap();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

#[cfg(feature = "tokio_fsm")]
mod fsm_tests {
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::{io::fsm::*, rec::make_test_data};

    /// range is a range of chunks. Just using u64 for convenience in tests
    async fn bao_tree_decode_slice_fsm_impl(data: Vec<u8>, range: Range<u64>) {
        let range = ChunkNum(range.start)..ChunkNum(range.end);
        let (encoded, root) = encode_slice_reference(&data, range.clone());
        let expected = data;
        let ranges = ChunkRanges::from(range);
        let mut encoded = Cursor::new(encoded);
        let size = encoded.read_u64_le().await.unwrap();
        let mut reading =
            ResponseDecoder::new(root, ranges, BaoTree::new(size, BlockSize::ZERO), encoded);
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

    proptest! {
        #[test]
        fn bao_tree_decode_slice_all_stream(len in 0..32768usize) {
            let data = make_test_data(len);
            let chunk_range = 0..(data.len() / 1024 + 1) as u64;
            tokio::runtime::Runtime::new().unwrap().block_on(bao_tree_decode_slice_fsm_impl(data, chunk_range));
        }
    }
}

fn bao_tree_outboard_comparison_impl(data: Vec<u8>) {
    let post1 = post_order_outboard_reference(&data);
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
    for item in
        decode_ranges_into_chunks(root, block_size, &mut ec, &ChunkRanges::from(range)).unwrap()
    {
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

#[test]
fn bao_tree_blake3_0() {
    use make_test_data as td;
    assert_tuple_eq!(bao_tree_blake3_impl(td(0)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(1)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(1023)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(1024)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(1025)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(2047)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(2048)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(2049)));
    assert_tuple_eq!(bao_tree_blake3_impl(td(10000)));
}

#[test]
#[ignore]
fn outboard_from_level() {
    let data = make_test_data(1024 * 16 + 12345);
    for level in 1..2 {
        let block_size = BlockSize(level);
        let ob = PostOrderMemOutboard::create(&data, block_size);
        println!("{}", ob.data.len());
    }
}

#[test]
fn outboard_wrong_hash() {
    let data = make_test_data(100000000);
    let expected = blake3::hash(&data);
    let actual = PostOrderMemOutboard::create(&data, BlockSize(4)).root();
    assert_eq!(expected, actual);
}

#[test]
#[ignore]
fn wrong_hash_small() {
    let start_chunk = 3;
    let len = 2049;
    let is_root = false;
    let data = make_test_data(len);
    let expected = recursive_hash_subtree(start_chunk, &data, is_root);
    let actual = blake3::guts::hash_subtree(start_chunk, &data, is_root);
    assert_eq!(expected, actual);
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

/// Count valid parents of a node in a tree of a given size.
fn count_parents(node: u64, len: u64) -> u64 {
    // node level, 0 for leaf nodes
    let level = (!node).trailing_zeros();
    // span of the node, 1 for leaf nodes
    let span = 1u64 << level;
    // count the parents with a loop
    let mut parent_count = 0;
    let mut offset = node;
    let mut span = span;
    // loop until we reach the root, adding valid parents
    loop {
        let pspan = span * 2;
        // find parent
        offset = if (offset & pspan) == 0 {
            offset + span
        } else {
            offset - span
        };
        // if parent is inside the tree, increase parent count
        if offset < len {
            parent_count += 1;
        }
        if pspan >= len {
            // we are at the root
            break;
        }
        span = pspan;
    }
    parent_count
}

fn compare_pre_order_outboard(size: usize) {
    let tree = BaoTree::new(size as u64, BlockSize::ZERO);
    let perm = create_permutation_reference(size);

    // print!("{:08b}", perm.len());
    for (k, v) in perm {
        // let expected = v as u64;
        // repr of node number where trailing zeros indicate level
        // let x = k.0 + 1;
        // clear lowest bit, since we don't want to count left children below the node itself
        // let without_lowest_bit = x & (x - 1);
        // subtract all nodes that go to the right themselves
        // this is 0 for every bit where we go left, and left_below for every bit where we go right,
        // where left_below is the count of the left child of the node
        // let full_lefts = without_lowest_bit - (without_lowest_bit.count_ones() as u64);
        // count the parents for the node
        // let parents = (tree.root().level() - k.level()) as u64;
        // add the parents
        // let actual = full_lefts + parents;

        // let corrected = full_lefts + count_parents(k.0, tree.filled_size().0);
        // this works for full trees!
        // println!(
        //     "{:09b}\t{}\t{}\t{}",
        //     k.0,
        //     expected,
        //     corrected,
        //     actual - corrected
        // );
        // let depth = tree.root().level() as u64;
        // println!("{} {}", depth, k.0);
        assert_eq!(v as u64, pre_order_offset_loop(k.0, tree.filled_size().0));
    }
    println!();
}

fn pre_order_outboard_line(case: usize) {
    let size = case as u64;
    let tree = BaoTree::new(size, BlockSize::ZERO);
    let perm = create_permutation_reference(case);
    print!("{:08b}", perm.len());
    for (k, _v) in perm {
        // repr of node number where trailing zeros indicate level
        let x = k.0 + 1;
        // clear lowest bit, since we don't want to count left children below the node itself
        let without_lowest_bit = x & (x - 1);
        // subtract all nodes that go to the right themselves
        // this is 0 for every bit where we go left, and left_below for every bit where we go right,
        // where left_below is the count of the left child of the node
        let full_lefts = without_lowest_bit - (without_lowest_bit.count_ones() as u64);
        // count the parents for the node
        let parents = (tree.root().level() - k.level()) as u64;
        // add the parents
        let actual = full_lefts + parents;

        let corrected = full_lefts + count_parents(k.0, tree.filled_size().0);
        let delta = actual - corrected;
        if delta == 0 {
            print!(" ");
        } else {
            print!("{}", delta);
        }
    }
    println!();
}

#[test]
#[ignore]
fn test_pre_order_outboard_fast() {
    let cases = [1024 * 78];
    for case in cases {
        compare_pre_order_outboard(case);
    }

    for case in 0..256 {
        pre_order_outboard_line(case * 1024);
    }
}

/// Decode encoded ranges given the root hash
pub fn decode_ranges_into_chunks<'a>(
    root: blake3::Hash,
    block_size: BlockSize,
    mut encoded: impl Read + 'a,
    ranges: &'a ChunkRangesRef,
) -> std::io::Result<impl Iterator<Item = std::io::Result<(u64, Vec<u8>)>> + 'a> {
    let size = read_len(&mut encoded)?;
    let tree = BaoTree::new(size, block_size);
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

/// iterate over all nodes in the tree in depth first, left to right, pre order
/// that are required to validate the given ranges
///
/// Recursive reference implementation, just used in tests
fn iterate_part_preorder_reference<'a>(
    tree: &BaoTree,
    ranges: &'a ChunkRangesRef,
    max_skip_level: u8,
) -> Vec<NodeInfo<'a>> {
    fn iterate_part_rec<'a>(
        tree: &BaoTree,
        node: TreeNode,
        ranges: &'a ChunkRangesRef,
        max_skip_level: u32,
        is_root: bool,
        res: &mut Vec<NodeInfo<'a>>,
    ) {
        if ranges.is_empty() {
            return;
        }
        let is_half_leaf = !tree.is_relevant_for_outboard(node);
        // check if the node is fully included
        let full = ranges.is_all();
        // split the ranges into left and right
        let (l_ranges, r_ranges) = if !is_half_leaf {
            split(ranges, node)
        } else {
            (ranges, ranges)
        };

        let query_leaf = tree.is_leaf(node) || (full && node.level() <= max_skip_level);
        // push no matter if leaf or not
        res.push(NodeInfo {
            node,
            ranges,
            l_ranges,
            r_ranges,
            full,
            query_leaf,
            is_root,
            is_half_leaf,
        });
        // if not leaf, recurse
        if !query_leaf {
            let valid_nodes = tree.filled_size();
            let l = node.left_child().unwrap();
            let r = node.right_descendant(valid_nodes).unwrap();
            iterate_part_rec(tree, l, l_ranges, max_skip_level, false, res);
            iterate_part_rec(tree, r, r_ranges, max_skip_level, false, res);
        }
    }
    let mut res = Vec::new();
    iterate_part_rec(
        tree,
        tree.root(),
        ranges,
        max_skip_level as u32,
        true,
        &mut res,
    );
    res
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
        let len = len;
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

fn get_leaf_ranges(
    tree: BaoTree,
    ranges: &ChunkRangesRef,
    max_skip_level: u8,
) -> impl Iterator<Item = Range<u64>> + '_ {
    tree.ranges_pre_order_chunks_iter_ref(ranges, max_skip_level)
        .filter_map(|e| {
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

/// Create a random selection
/// `size` is the size of the data
/// `n` is the number of ranges, roughly the complexity of the selection
fn selection(size: u64, n: usize) -> impl Strategy<Value = ChunkRanges> {
    let chunks = BaoTree::new(size, BlockSize(0)).chunks();
    proptest::collection::vec((..chunks.0, ..chunks.0), n).prop_map(|e| {
        let mut res = ChunkRanges::empty();
        for (a, b) in e {
            let min = a.min(b);
            let max = a.max(b) + 1;
            let elem = ChunkRanges::from(ChunkNum(min)..ChunkNum(max));
            if res != elem {
                res ^= elem;
            }
        }
        res
    })
}

fn size_and_selection(
    size_range: Range<usize>,
    n: usize,
) -> impl Strategy<Value = (usize, ChunkRanges)> {
    size_range.prop_flat_map(move |size| (Just(size), selection(size as u64, n)))
}

#[test]
fn encode_selected_rec_cases() {
    let data = make_test_data(1024 * 3);
    let overhead = |data, min_level: u32| {
        let mut actual_encoded = Vec::new();
        encode_selected_rec(
            ChunkNum(0),
            data,
            true,
            &ChunkRanges::all(),
            min_level,
            true,
            &mut actual_encoded,
        );
        actual_encoded.len() - data.len()
    };
    assert_eq!(overhead(&data, 0), 64 * 2);
    assert_eq!(overhead(&data, 1), 64);
    assert_eq!(overhead(&data, 2), 0);
}

/// Reference implementation of encode_ranges_validated that uses the simple recursive impl
fn encode_selected_reference(
    data: &[u8],
    block_size: BlockSize,
    ranges: &ChunkRangesRef,
) -> (blake3::Hash, Vec<u8>) {
    let mut res = Vec::new();
    res.extend_from_slice(&(data.len() as u64).to_le_bytes());
    let max_skip_level = block_size.to_u32();
    let ranges = truncate_ranges(ranges, data.len() as u64);
    let hash = encode_selected_rec(
        ChunkNum(0),
        data,
        true,
        ranges,
        max_skip_level,
        true,
        &mut res,
    );
    (hash, res)
}

/// Encode a small subset of a large blob, and check that the encoded data is small
#[test]
fn encode_single_chunk_large() {
    // a rather big piece of data
    let data = make_test_data(1024 * 1024 * 16 + 12345);
    // compute an outboard at a block size of 2^4 = 16 chunks
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(4));

    // encode the given ranges
    let get_encoded = |ranges| {
        let mut actual_encoded = Vec::new();
        crate::io::sync::encode_ranges_validated(&data, &outboard, ranges, &mut actual_encoded)
            .unwrap();
        actual_encoded
    };

    // check the expected size for various ranges
    let ranges = ChunkRanges::from(..ChunkNum(1));
    let encoded = get_encoded(&ranges);
    assert_eq!(encoded.len(), 8 + 15 * 64 + 1024);

    let ranges = ChunkRanges::from(ChunkNum(1000)..ChunkNum(1001));
    let encoded = get_encoded(&ranges);
    assert_eq!(encoded.len(), 8 + 15 * 64 + 1024);

    let ranges = ChunkRanges::from(ChunkNum(3000)..ChunkNum(3001));
    let encoded = get_encoded(&ranges);
    assert_eq!(encoded.len(), 8 + 15 * 64 + 1024);
}

fn last_chunk(size: u64) -> Range<u64> {
    const CHUNK_LEN: u64 = 1024;
    const MASK: u64 = CHUNK_LEN - 1;
    if (size & MASK) == 0 {
        size - CHUNK_LEN..size
    } else {
        (size & !MASK)..size
    }
}

fn select_last_chunk_impl(size: u64, block_size: u8) -> (Vec<Range<u64>>, Vec<Range<u64>>) {
    let range = ChunkRanges::from(ChunkNum(u64::MAX)..);
    let selection = ResponseIterRef::new(BaoTree::new(size, BlockSize(block_size)), &range)
        .filter_map(|item| match item {
            BaoChunk::Leaf {
                start_chunk, size, ..
            } => {
                let start = start_chunk.to_bytes();
                let end = start + (size as u64);
                Some(start..end)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    (selection, vec![last_chunk(size)])
}

fn encode_last_chunk_impl(size: u64, block_size: u8) -> (Vec<u8>, Vec<u8>) {
    let data = make_test_data(size as usize);
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(block_size));

    let range = ChunkRanges::from(ChunkNum(u64::MAX)..);
    let mut encoded1 = Vec::new();
    encode_ranges_validated(&data, &outboard, &range, &mut encoded1).unwrap();

    let lc = last_chunk(size);
    let sc = ChunkNum::chunks(lc.start);
    let ec = ChunkNum::chunks(lc.end);
    let range = ChunkRanges::from(sc..ec);
    let mut encoded2 = Vec::new();
    encode_ranges_validated(&data, &outboard, &range, &mut encoded2).unwrap();
    (encoded1, encoded2)
}

#[test]
fn outboard_hash() {
    for i in 1..4 {
        let data = &[0u8];
        let outboard = PostOrderMemOutboard::create(data, BlockSize(i));
        let hash = outboard.root();
        assert_eq!(hash, blake3::hash(data));
    }
}

#[test]
fn select_last_chunk_0() {
    assert_tuple_eq!(select_last_chunk_impl(1, 0));
}

/// Compares the PostOrderNodeIter with a simple stack-based reference implementation.
#[test]
#[ignore]
fn test_post_order_node_iter() {
    let cases = [8193];
    for size in cases {
        for i in 0..5 {
            let tree = BaoTree::new(size, BlockSize(i));
            let items = tree.post_order_nodes_iter().collect::<Vec<_>>();
            println!("{}", i);
            for item in items {
                println!("{:?}", item);
            }
            println!();
        }
    }
}

#[test]
#[ignore]
fn test_pre_order_chunks_iter_ref() {
    let cases = [
        // (8193, ChunkRanges::all()),
        (8193, ChunkRanges::from(..ChunkNum(1))),
    ];
    for (size, ranges) in cases {
        for i in 0..5 {
            let tree = BaoTree::new(size, BlockSize(i));
            let items = PreOrderPartialIterRef::new(tree, &ranges, tree.block_size.0);
            println!("{}", i);
            for item in items {
                println!("{:?} {:?}", item.node.byte_range(), item);
            }
            println!();
        }
        for i in 0..5 {
            let tree = BaoTree::new(size, BlockSize(i));
            let items = ReferencePreOrderPartialChunkIterRef::new(tree, &ranges, tree.block_size.0);
            println!("{}", i);
            for item in items {
                println!("{:?}", item);
            }
            println!();
        }
    }
}

/// Compares the PostOrderNodeIter with a simple stack-based reference implementation.
#[test]
#[ignore]
fn test_post_order_chunk_iter() {
    for i in 1..5 {
        let tree = BaoTree::new(1, BlockSize(i));
        let items = PostOrderChunkIter::new(tree).collect::<Vec<_>>();
        println!("{}", i);
        for item in items {
            println!("{:?}", item);
        }
        println!();
    }
}

/// Compares the PostOrderNodeIter with a simple stack-based reference implementation.
#[test]
#[ignore]
fn test_post_order_outboard() {
    let data = make_test_data(3234);
    for i in 0..5 {
        let items = PostOrderMemOutboard::create(&data, BlockSize(i));
        println!("{} {}", i, items.data.len());
    }
}

type Pair<A> = (A, A);

fn pre_order_iter_comparison_impl(len: u64, level: u8) -> Pair<Vec<TreeNode>> {
    let tree = BaoTree::new(len, BlockSize(level));
    let iter1 = tree.pre_order_nodes_iter().collect::<Vec<_>>();
    let iter2 = tree
        .ranges_pre_order_nodes_iter(&ChunkRanges::all(), 0)
        .map(|x| x.node)
        .collect::<Vec<_>>();
    (iter1, iter2)
}

#[test]
fn pre_order_iter_comparison_cases() {
    let cases = [(2049, 1)];
    for (len, level) in cases {
        assert_tuple_eq!(pre_order_iter_comparison_impl(len, level));
    }
}

/// Check that a query outside the valid range always encodes the last chunk
#[test]
fn encode_last_chunk_cases() {
    let cases = [
        // (1, 0),
        // (1, 1),
        (4096, 0),
        // (8192, 0),
    ];
    for (size, block_size) in cases {
        assert_tuple_eq!(encode_last_chunk_impl(size, block_size));
    }
}

#[test]
fn test_full_chunk_groups() {
    let cases = vec![
        (
            ChunkRanges::from(ChunkNum(8)..),
            ChunkRanges::from(ChunkNum(16)..),
        ),
        (
            ChunkRanges::from(ChunkNum(8)..ChunkNum(16)),
            ChunkRanges::empty(),
        ),
        (
            ChunkRanges::from(ChunkNum(11)..ChunkNum(34)),
            ChunkRanges::from(ChunkNum(16)..ChunkNum(32)),
        ),
        (
            ChunkRanges::from(..ChunkNum(35)),
            ChunkRanges::from(..ChunkNum(32)),
        ),
    ];
    for (case, expected) in cases {
        let res = full_chunk_groups(&case, BlockSize(4));
        assert_eq!(res, expected);
    }
}

#[test]
fn sub_chunk_group_query() {
    let tree = BaoTree::new(1024 * 32, BlockSize(4));
    let ranges = ChunkRanges::from(ChunkNum(16)..ChunkNum(24));
    let items = ResponseIter::new(tree, ranges)
        .filter(|x| matches!(x, BaoChunk::Leaf { .. }))
        .collect::<Vec<_>>();
    assert_eq!(items.len(), 1);
}

proptest! {

    #[test]
    fn node_from_chunk_and_level(block in 0..100000u64, level in 0u8..8u8) {
        let chunk = block << (level + 1);
        let node = TreeNode::from_start_chunk_and_level(ChunkNum(chunk), BlockSize(level));
        prop_assert_eq!(node.level(), level as u32);
        prop_assert_eq!(node.chunk_range().start, ChunkNum(chunk));
    }

    /// Check that a query outside the valid range always selects the last chunk
    #[test]
    fn select_last_chunk(size in 1..100000u64, block_size in 0..4u8) {
        assert_tuple_eq!(select_last_chunk_impl(size, block_size));
    }

    /// Check that a query outside the valid range always encodes the last chunk
    #[test]
    fn encode_last_chunk(size in 1..100000u64, block_size in 0..4u8) {
        assert_tuple_eq!(encode_last_chunk_impl(size, block_size));
    }

    /// Checks that the simple recursive impl bao_encode_selected_recursive that
    /// does not need an outboard is the same as the more complex encode_ranges_validated
    /// that requires an outboard.
    #[test]
    fn encode_selected_reference_sync_proptest((size, ranges) in size_and_selection(1..100000, 2), block_size in 0..5u8) {
        let data = make_test_data(size);
        let expected_hash = blake3::hash(&data);
        let block_size = BlockSize(block_size);
        let (actual_hash, actual_encoded) = encode_selected_reference(&data, block_size, &ranges);
        let mut expected_encoded = Vec::new();
        let outboard = PostOrderMemOutboard::create(&data, block_size);
        crate::io::sync::encode_ranges_validated(
            &data,
            &outboard,
            &ranges,
            &mut expected_encoded,
        ).unwrap();
        prop_assert_eq!(expected_hash, actual_hash);
        prop_assert_eq!(hex::encode(expected_encoded), hex::encode(actual_encoded));
    }

    /// Checks that the simple recursive impl bao_encode_selected_recursive that
    /// does not need an outboard is the same as the more complex encode_ranges_validated
    /// that requires an outboard.
    #[test]
    fn encode_selected_reference_fsm_proptest((size, ranges) in size_and_selection(1..100000, 2), block_size in 0..4u8) {
        let data = make_test_data(size);
        let expected_hash = blake3::hash(&data);
        let block_size = BlockSize(block_size);
        let (actual_hash, actual_encoded) = encode_selected_reference(&data, block_size, &ranges);
        let mut expected_encoded = Vec::new();
        let outboard = PostOrderMemOutboard::create(&data, block_size);
        let data: Bytes = data.into();
        tokio::runtime::Runtime::new().unwrap().block_on(crate::io::fsm::encode_ranges_validated(
            data,
            outboard,
            &ranges,
            &mut expected_encoded,
        )).unwrap();
        prop_assert_eq!(expected_hash, actual_hash);
        prop_assert_eq!(expected_encoded, actual_encoded);
    }

    /// Checks that the leafs produced by ranges_pre_order_chunks_iter_ref
    /// cover the entire data exactly once.
    #[test]
    fn max_skip_level(size in 0..32786u64, block_size in 0..2u8, max_skip_level in 0..2u8) {
        let tree = BaoTree::new(size, BlockSize(block_size));
        let ranges = ChunkRanges::all();
        let leaf_ranges = get_leaf_ranges(tree, &ranges, max_skip_level).collect::<Vec<_>>();
        prop_assert_eq!(range_union(leaf_ranges), Some(RangeSet2::from(0..size)));
    }

    #[test]
    fn flip(len in 0usize..100000) {
        let data = make_test_data(len);
        let post = post_order_outboard_reference(&data);
        prop_assert_eq!(&post, &post.flip().flip());
    }



    /// Check that the unrestricted pre-order iterator is the same as the
    /// restricted pre-order iterator for the entire tree.
    #[test]
    fn pre_order_iter_comparison(len in 0..1000000u64, level in 0u8..4) {
        prop_assert_tuple_eq!(pre_order_iter_comparison_impl(len, level));
    }

    #[test]
    fn bao_tree_blake3(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        prop_assert_tuple_eq!(bao_tree_blake3_impl(data));
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

    /// Compares the ranges iter with a recursive reference implementation.
    #[test]
    fn partial_iterator_reference_comparison((len, start, size) in size_and_slice_overlapping()) {
        let tree = BaoTree::new(len, BlockSize::ZERO);
        let chunk_range = start .. start + size;
        let rs = ChunkRanges::from(chunk_range);
        let iter1 = iterate_part_preorder_reference(&tree, &rs, 0);
        let iter2 = tree.ranges_pre_order_nodes_iter(&rs, 0).collect::<Vec<_>>();
        prop_assert_eq!(&iter1, &iter2);
    }

    #[test]
    #[ignore]
    fn pre_post_outboard(n in 0usize..1000000) {
        compare_pre_order_outboard(n);
    }

    #[test]
    fn hash_subtree_bs4(block in 0u64..100000, size in 0usize..1024 << 4) {
        let chunk = block << 4;
        let data = make_test_data(size);
        let expected = recursive_hash_subtree(chunk, &data, false);
        let actual = crate::hash_subtree(chunk, &data, false);
        prop_assert_eq!(expected, actual);
    }
}
