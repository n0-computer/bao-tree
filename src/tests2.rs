//! About these tests
//!
//! The tests are structured as follows:
//!
//! There is a function <testname>_impl that is the actual implementation of the test.
//! There is a proptest called <testname>_proptest that calls the test multiple times with random data.
//! There is a test called <testname>_cases that calls the test with a few hardcoded values, either
//! handcrafted or from a previous failure of a proptest.
use std::ops::Range;

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use proptest::strategy::{Just, Strategy};
use range_collections::{range_set::RangeSetEntry, RangeSet2, RangeSetRef};

use crate::iter::{response_iter_ref_reference, ResponseIterRef, ResponseIterRef2};
use crate::{
    blake3, hash_subtree,
    io::{
        fsm::{BaoContentItem, ResponseDecoderReadingNext},
        outboard::PostOrderMemOutboard,
        sync::{DecodeResponseItem, Outboard},
        Header, Leaf, Parent,
    },
    iter::{select_nodes_rec, BaoChunk},
    BaoTree, BlockSize, ByteNum, ChunkNum, TreeNode,
};

macro_rules! assert_tuple_eq {
    ($tuple:expr) => {
        assert_eq!($tuple.0, $tuple.1);
    };
}

macro_rules! prop_assert_tuple_eq {
    ($tuple:expr) => {
        let (a, b) = $tuple;
        ::proptest::prop_assert_eq!(a, b);
    };
}

fn tree() -> impl Strategy<Value = BaoTree> {
    (0u64..100000, 0u8..5).prop_map(|(size, block_size)| {
        let block_size = BlockSize(block_size);
        let size = ByteNum(size);
        BaoTree::new(size, block_size)
    })
}

fn block_size() -> impl Strategy<Value = BlockSize> {
    (0..=6u8).prop_map(BlockSize)
}

/// Create a random selection
/// `size` is the size of the data
/// `n` is the number of ranges, roughly the complexity of the selection
fn selection(size: u64, n: usize) -> impl Strategy<Value = RangeSet2<ChunkNum>> {
    let chunks = BaoTree::new(ByteNum(size), BlockSize(0)).chunks();
    proptest::collection::vec((..chunks.0, ..chunks.0), n).prop_map(|e| {
        let mut res = RangeSet2::empty();
        for (a, b) in e {
            let min = a.min(b);
            let max = a.max(b) + 1;
            let elem = RangeSet2::from(ChunkNum(min)..ChunkNum(max));
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
) -> impl Strategy<Value = (usize, RangeSet2<ChunkNum>)> {
    let selection = prop_oneof! {
        8 => selection(size_range.end as u64, n),
        1 => Just(RangeSet2::all()),
    };
    size_range.prop_flat_map(move |size| (Just(size), selection.clone()))
}

/// Check that the pre order traversal iterator is consistent with the pre order
/// offset function.
fn pre_traversal_offset_impl(tree: BaoTree) {
    // iterate over all nodes
    let traversal: Vec<TreeNode> = tree.pre_order_nodes_iter().collect();
    // filter out the half leaf
    let relevant = traversal
        .iter()
        .copied()
        .filter(|x| tree.is_relevant_for_outboard(*x))
        .collect::<Vec<_>>();
    // check that there is at most one half leaf
    assert!(traversal.len() - relevant.len() <= 1);
    // for the relevant nodes, check that the offset is correct
    for (offset, node) in relevant.iter().enumerate() {
        let actual = tree.pre_order_offset(*node).unwrap();
        let expected = offset as u64;
        assert_eq!(actual, expected);
    }
}

#[test_strategy::proptest]
fn pre_traversal_offset_proptest(#[strategy(tree())] tree: BaoTree) {
    pre_traversal_offset_impl(tree);
}

/// Check that the post order traversal iterator is consistent with the post order
/// offset function.
fn post_traversal_offset_impl(tree: BaoTree) {
    let traversal = tree.post_order_nodes_iter().collect::<Vec<_>>();
    // filter out the half leaf
    let relevant = traversal
        .iter()
        .copied()
        .filter(|x| tree.is_relevant_for_outboard(*x))
        .collect::<Vec<_>>();
    // check that there is at most one half leaf
    assert!(traversal.len() - relevant.len() <= 1);
    // for the relevant nodes, check that the offset is correct
    for (offset, node) in relevant.iter().enumerate() {
        let expected = offset as u64;
        let actual = tree.post_order_offset(*node).unwrap().value();
        assert_eq!(actual, expected);
    }
}

#[test_strategy::proptest]
fn post_traversal_offset_proptest(#[strategy(tree())] tree: BaoTree) {
    post_traversal_offset_impl(tree);
}

/// Make test data that has each chunk filled with the cunk number as an u8
///
/// This makes sure that every chunk has a different hash, and makes it easy
/// to see pages in hex dumps.
fn make_test_data(n: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(n);
    for i in 0..n {
        data.push((i / 1024) as u8);
    }
    data
}

/// Compute the union of an iterator of ranges. The ranges should be non-overlapping, otherwise
/// the result is None
fn range_union<K: RangeSetEntry>(
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

fn get_leaf_ranges<R>(
    iter: impl IntoIterator<Item = BaoChunk<R>>,
) -> impl Iterator<Item = Range<ByteNum>> {
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

/// Check that the post order traversal iterator is consistent with the post order
/// offset function.
fn post_traversal_chunks_iter_impl(tree: BaoTree) {
    let chunks = tree.post_order_chunks_iter();
    let ranges = get_leaf_ranges(chunks);
    let union = range_union(ranges).unwrap();
    assert_eq!(union, RangeSet2::from(..tree.size));
}

#[test_strategy::proptest]
fn post_traversal_chunks_iter_proptest(#[strategy(tree())] tree: BaoTree) {
    post_traversal_chunks_iter_impl(tree);
}

/// Brute force test for an outboard that just computes the expected hash for each pair
fn outboard_test_sync(data: &[u8], outboard: impl crate::io::sync::Outboard) {
    let tree = outboard.tree();
    let nodes = tree
        .pre_order_nodes_iter()
        .enumerate()
        .map(|(i, node)| (node, i == 0))
        .filter(|(node, _)| tree.is_relevant_for_outboard(*node))
        .collect::<Vec<_>>();
    for (node, is_root) in nodes {
        let (l_hash, r_hash) = outboard.load(node).unwrap().unwrap();
        let start_chunk = node.chunk_range().start;
        let byte_range = tree.byte_range(node);
        let data = &data[byte_range.start.to_usize()..byte_range.end.to_usize()];
        let expected = hash_subtree(start_chunk.0, data, is_root);
        let actual = blake3::guts::parent_cv(&l_hash, &r_hash, is_root);
        assert_eq!(actual, expected);
    }
}

/// Brute force test for an outboard that just computes the expected hash for each pair
async fn outboard_test_fsm(data: &[u8], mut outboard: impl crate::io::fsm::Outboard) {
    let tree = outboard.tree();
    let nodes = tree
        .pre_order_nodes_iter()
        .enumerate()
        .map(|(i, node)| (node, i == 0))
        .filter(|(node, _)| tree.is_relevant_for_outboard(*node))
        .collect::<Vec<_>>();
    for (node, is_root) in nodes {
        let (l_hash, r_hash) = outboard.load(node).await.unwrap().unwrap();
        let start_chunk = node.chunk_range().start;
        let byte_range = tree.byte_range(node);
        let data = &data[byte_range.start.to_usize()..byte_range.end.to_usize()];
        let expected = hash_subtree(start_chunk.0, data, is_root);
        let actual = blake3::guts::parent_cv(&l_hash, &r_hash, is_root);
        assert_eq!(actual, expected);
    }
}

fn post_oder_outboard_sync_impl(tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    assert_eq!(
        outboard.data.len() as u64,
        outboard.tree().outboard_hash_pairs() * 64
    );
    outboard_test_sync(&data, outboard);
}

#[test]
fn post_oder_outboard_sync_cases() {
    let cases = [(0x3001, 0)];
    for (size, block_level) in cases {
        let tree = BaoTree::new(ByteNum(size), BlockSize(block_level));
        post_oder_outboard_sync_impl(tree);
    }
}

#[test_strategy::proptest]
fn post_oder_outboard_sync_proptest(#[strategy(tree())] tree: BaoTree) {
    post_oder_outboard_sync_impl(tree);
}

fn post_oder_outboard_fsm_impl(tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    assert_eq!(
        outboard.data.len() as u64,
        outboard.tree().outboard_hash_pairs() * 64
    );
    futures::executor::block_on(outboard_test_fsm(&data, outboard));
}

#[test_strategy::proptest]
fn post_oder_outboard_fsm_proptest(#[strategy(tree())] tree: BaoTree) {
    post_oder_outboard_fsm_impl(tree);
}

fn mem_outboard_flip_impl(tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    assert_eq!(outboard, outboard.flip().flip());
}

#[test_strategy::proptest]
fn mem_outboard_flip_proptest(#[strategy(tree())] tree: BaoTree) {
    mem_outboard_flip_impl(tree);
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn valid_ranges_sync(outboard: &PostOrderMemOutboard) -> RangeSet2<ChunkNum> {
    crate::io::sync::valid_ranges(outboard).unwrap()
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn valid_ranges_fsm(outboard: &mut PostOrderMemOutboard) -> RangeSet2<ChunkNum> {
    futures::executor::block_on(crate::io::fsm::valid_ranges(outboard)).unwrap()
}

fn validate_outboard_sync_pos_impl(tree: BaoTree) {
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    let actual = valid_ranges_sync(&outboard);
    assert_eq!(expected, actual)
}

#[test_strategy::proptest]
fn validate_outboard_sync_pos_proptest(#[strategy(tree())] tree: BaoTree) {
    validate_outboard_sync_pos_impl(tree);
}

fn validate_outboard_fsm_pos_impl(tree: BaoTree) {
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    let actual = valid_ranges_fsm(&mut outboard);
    assert_eq!(expected, actual)
}

#[test_strategy::proptest]
fn validate_outboard_fsm_pos_proptest(#[strategy(tree())] tree: BaoTree) {
    validate_outboard_fsm_pos_impl(tree);
}

fn flip_bit(data: &mut [u8], rand: usize) {
    // flip a random bit in the outboard
    // this is the post order outboard without the length suffix,
    // so it's all hashes
    let bit = rand % data.len() * 8;
    let byte = bit / 8;
    let bit = bit % 8;
    data[byte] ^= 1 << bit;
}

/// Check that flipping a random bit in the outboard makes at least one range invalid
fn validate_outboard_sync_neg_impl(tree: BaoTree, rand: u32) {
    let rand = rand as usize;
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    if !outboard.data.is_empty() {
        // flip a random bit in the outboard
        flip_bit(&mut outboard.data, rand);
        // Check that at least one range is invalid
        let actual = valid_ranges_sync(&outboard);
        assert_ne!(expected, actual);
    }
}

#[test]
fn validate_outboard_sync_neg_cases() {
    let cases = [((0x6001, 3), 1265277760)];
    for ((size, block_level), rand) in cases {
        let tree = BaoTree::new(ByteNum(size), BlockSize(block_level));
        validate_outboard_sync_neg_impl(tree, rand);
    }
}

#[test_strategy::proptest]
fn validate_outboard_sync_neg_proptest(#[strategy(tree())] tree: BaoTree, rand: u32) {
    validate_outboard_sync_neg_impl(tree, rand);
}

/// Check that flipping a random bit in the outboard makes at least one range invalid
fn validate_outboard_fsm_neg_impl(tree: BaoTree, rand: u32) {
    let rand = rand as usize;
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    if !outboard.data.is_empty() {
        // flip a random bit in the outboard
        flip_bit(&mut outboard.data, rand);
        // Check that at least one range is invalid
        let actual = valid_ranges_fsm(&mut outboard);
        assert_ne!(expected, actual);
    }
}

#[test_strategy::proptest]
fn validate_outboard_fsm_neg_proptest(#[strategy(tree())] tree: BaoTree, rand: u32) {
    validate_outboard_fsm_neg_impl(tree, rand);
}

/// Encode data fully, decode it again, and check that both data and outboard are the same
///
/// using the sync io api
fn encode_decode_full_sync_impl(
    data: &[u8],
    outboard: PostOrderMemOutboard,
) -> (
    (Vec<u8>, PostOrderMemOutboard),
    (Vec<u8>, PostOrderMemOutboard),
) {
    let ranges = RangeSet2::all();
    let mut encoded = Vec::new();
    crate::io::sync::encode_ranges_validated(&data, &outboard, &RangeSet2::all(), &mut encoded)
        .unwrap();
    let mut encoded_read = std::io::Cursor::new(encoded);
    let mut decoded = Vec::new();
    let ob_res_opt = crate::io::sync::decode_response_into(
        outboard.root(),
        outboard.tree().block_size,
        &ranges,
        &mut encoded_read,
        |tree, root: blake3::Hash| {
            let outboard_size = usize::try_from(tree.outboard_hash_pairs() * 64).unwrap();
            let outboard_data = vec![0; outboard_size];
            Ok(PostOrderMemOutboard::new(root, tree, outboard_data).unwrap())
        },
        &mut decoded,
    )
    .unwrap();
    let ob_res = ob_res_opt.unwrap_or_else(|| {
        PostOrderMemOutboard::new(outboard.root(), outboard.tree(), vec![]).unwrap()
    });
    ((decoded, ob_res), (data.to_vec(), outboard))
}

/// Encode data fully, decode it again, and check that both data and outboard are the same
///
/// using the fsm io api
async fn encode_decode_full_fsm_impl(
    data: Vec<u8>,
    outboard: PostOrderMemOutboard,
) -> (
    (Vec<u8>, PostOrderMemOutboard),
    (Vec<u8>, PostOrderMemOutboard),
) {
    let mut outboard = outboard;
    let ranges = RangeSet2::all();
    let mut encoded = Vec::new();
    crate::io::fsm::encode_ranges_validated(
        Bytes::from(data.clone()),
        &mut outboard,
        &RangeSet2::all(),
        &mut encoded,
    )
    .await
    .unwrap();
    let mut read_encoded = std::io::Cursor::new(encoded);
    let mut decoded = BytesMut::new();
    let ob_res_opt = crate::io::fsm::decode_response_into(
        outboard.root(),
        outboard.tree().block_size,
        ranges,
        &mut read_encoded,
        |root, tree| async move {
            let outboard_size = usize::try_from(tree.outboard_hash_pairs() * 64).unwrap();
            let outboard_data = vec![0u8; outboard_size];
            Ok(PostOrderMemOutboard::new(root, tree, outboard_data).unwrap())
        },
        &mut decoded,
    )
    .await
    .unwrap();
    let ob_res = ob_res_opt.unwrap_or_else(|| {
        PostOrderMemOutboard::new(outboard.root(), outboard.tree(), vec![]).unwrap()
    });
    ((decoded.to_vec(), ob_res), (data, outboard))
}

fn encode_decode_partial_sync_impl(
    data: &[u8],
    outboard: PostOrderMemOutboard,
    ranges: &RangeSetRef<ChunkNum>,
) -> bool {
    let mut encoded = Vec::new();
    crate::io::sync::encode_ranges_validated(&data, &outboard, &ranges, &mut encoded).unwrap();
    let expected_data = data;
    let encoded_read = std::io::Cursor::new(encoded);
    let buf = BytesMut::new();
    let iter = crate::io::sync::DecodeResponseIter::new(
        outboard.root,
        outboard.tree.block_size,
        encoded_read,
        ranges,
        buf,
    );
    for item in iter {
        let item = match item {
            Ok(item) => item,
            Err(_) => {
                return false;
            }
        };
        match item {
            DecodeResponseItem::Header(Header { size }) => {
                // check that the size matches
                if size != outboard.tree.size {
                    return false;
                }
            }
            DecodeResponseItem::Parent(Parent { node, pair }) => {
                // check that the hash pair matches
                if let Some(expected_pair) = outboard.load(node).unwrap() {
                    if pair != expected_pair {
                        return false;
                    }
                }
            }
            DecodeResponseItem::Leaf(Leaf { offset, data }) => {
                // check that the data matches
                if expected_data[offset.to_usize()..offset.to_usize() + data.len()] != data {
                    return false;
                }
            }
        }
    }
    true
}

async fn encode_decode_partial_fsm_impl(
    data: &[u8],
    outboard: PostOrderMemOutboard,
    ranges: RangeSet2<ChunkNum>,
) -> bool {
    let mut encoded = Vec::new();
    let mut outboard = outboard;
    crate::io::fsm::encode_ranges_validated(
        Bytes::from(data.to_vec()),
        &mut outboard,
        &ranges,
        &mut encoded,
    )
    .await
    .unwrap();
    let expected_data = data;
    let encoded_read = std::io::Cursor::new(encoded);
    let initial = crate::io::fsm::ResponseDecoderStart::new(
        outboard.root,
        ranges,
        outboard.tree.block_size,
        encoded_read,
    );
    let (mut reading, size) = initial.next().await.unwrap();
    if size != outboard.tree.size {
        return false;
    }
    loop {
        match reading.next().await {
            ResponseDecoderReadingNext::More((reading1, result)) => {
                let item = match result {
                    Ok(item) => item,
                    Err(_) => {
                        return false;
                    }
                };
                match item {
                    BaoContentItem::Leaf(Leaf { offset, data }) => {
                        // check that the data matches
                        if expected_data[offset.to_usize()..offset.to_usize() + data.len()] != data
                        {
                            return false;
                        }
                    }
                    BaoContentItem::Parent(Parent { node, pair }) => {
                        // check that the hash pair matches
                        if let Some(expected_pair) = outboard.load(node).unwrap() {
                            if pair != expected_pair {
                                return false;
                            }
                        }
                    }
                }
                reading = reading1;
            }
            ResponseDecoderReadingNext::Done(_reader) => {
                break;
            }
        }
    }
    true
}

#[test]
fn encode_decode_full_sync_cases() {
    let cases = [(1024 + 1, 1)];
    for (size, block_level) in cases {
        let data = &make_test_data(size);
        let block_size = BlockSize(block_level);
        let outboard = PostOrderMemOutboard::create(&data, block_size);
        let pair = encode_decode_full_sync_impl(data, outboard);
        assert_tuple_eq!(pair);
    }
}

#[test_strategy::proptest]
fn encode_decode_full_sync_proptest(#[strategy(tree())] tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    prop_assert_tuple_eq!(encode_decode_full_sync_impl(&data, outboard));
}

#[test_strategy::proptest]
fn encode_decode_partial_sync_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, RangeSet2<ChunkNum>),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, selection) = size_and_selection;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(&data, block_size);
    let ok = encode_decode_partial_sync_impl(&data, outboard, &selection);
    prop_assert!(ok);
}

#[test_strategy::proptest]
fn encode_decode_full_fsm_proptest(#[strategy(tree())] tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    let pair = futures::executor::block_on(encode_decode_full_fsm_impl(data.into(), outboard));
    prop_assert_tuple_eq!(pair);
}

#[test_strategy::proptest]
fn encode_decode_partial_fsm_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, RangeSet2<ChunkNum>),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, selection) = size_and_selection;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(&data, block_size);
    let ok =
        futures::executor::block_on(encode_decode_partial_fsm_impl(&data, outboard, selection));
    prop_assert!(ok);
}

fn pre_order_nodes_iter_reference(tree: BaoTree, ranges: &RangeSetRef<ChunkNum>) -> Vec<TreeNode> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.to_usize(),
        true,
        ranges,
        tree.block_size.0 as u32 + 1,
        &mut |x| {
            let node = match x {
                BaoChunk::Parent { node, .. } => node,
                BaoChunk::Leaf { start_chunk, .. } => {
                    TreeNode::from_start_chunk_and_level(start_chunk, tree.block_size)
                }
            };
            res.push(node);
        },
    );
    res
}

#[test_strategy::proptest]
fn pre_order_node_iter_proptest(#[strategy(tree())] tree: BaoTree) {
    let actual = tree.pre_order_nodes_iter().collect::<Vec<_>>();
    let expected = pre_order_nodes_iter_reference(tree, &RangeSet2::all());
    prop_assert_eq!(expected, actual);
}

#[test]
fn selection_reference_comparison_cases() {
    let cases = [((1026, 1), RangeSet2::all()), ((2050, 1), RangeSet2::all())];
    for ((size, block_level), ranges) in cases {
        println!("{} {} {:?}", size, block_level, ranges);
        let tree = BaoTree::new(ByteNum(size), BlockSize(block_level));
        let expected = response_iter_ref_reference(tree, &ranges);

        let actual1 = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();

        let actual2 = ResponseIterRef2::new(tree, &ranges).collect::<Vec<_>>();
        if actual1 != expected {
            println!("actual old {:?}", actual1);
            println!("actual new {:?}", actual2);
            println!("expected   {:?}", expected);
            panic!();
        }
    }
}

#[test_strategy::proptest]
fn selection_reference_comparison_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, RangeSet2<ChunkNum>),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, ranges) = size_and_selection;
    let tree = BaoTree::new(ByteNum(size as u64), block_size);
    let expected = response_iter_ref_reference(tree, &ranges);
    let actual1 = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();
    let actual2 = ResponseIterRef2::new(tree, &ranges).collect::<Vec<_>>();
    if actual1 != expected {
        println!("");
        println!("{:?} {:?}", tree, ranges);
        println!("actual old {:?}", actual1);
        println!("actual new {:?}", actual2);
        println!("expected   {:?}", expected);
        panic!();
    }
}
