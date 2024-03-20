//! About these tests
//!
//! The tests are structured as follows:
//!
//! There is a function <testname>_impl that is the actual implementation of the test.
//! There is a proptest called <testname>_proptest that calls the test multiple times with random data.
//! There is a test called <testname>_cases that calls the test with a few hardcoded values, either
//! handcrafted or from a previous failure of a proptest.
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use proptest::prelude::*;
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use std::ops::Range;
use test_strategy::proptest;

use crate::io::outboard::PreOrderMemOutboard;
use crate::rec::{
    get_leaf_ranges, make_test_data, partial_chunk_iter_reference, range_union,
    response_iter_reference, truncate_ranges, ReferencePreOrderPartialChunkIterRef,
};
use crate::{assert_tuple_eq, prop_assert_tuple_eq, ChunkRanges, ChunkRangesRef};
use crate::{
    blake3, hash_subtree,
    io::{
        fsm::{BaoContentItem, ResponseDecoderReadingNext},
        outboard::PostOrderMemOutboard,
        sync::{DecodeResponseItem, Outboard},
        Header, Leaf, Parent,
    },
    iter::{BaoChunk, PreOrderPartialChunkIterRef, ResponseIterRef},
    rec::{encode_selected_rec, select_nodes_rec},
    BaoTree, BlockSize, ByteNum, ChunkNum, TreeNode,
};

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
fn selection(size: u64, n: usize) -> impl Strategy<Value = ChunkRanges> {
    let chunks = BaoTree::new(ByteNum(size), BlockSize(0)).chunks();
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
    let selection = prop_oneof! {
        8 => selection(size_range.end as u64, n),
        1 => Just(ChunkRanges::all()),
        1 => Just(ChunkRanges::from(ChunkNum(u64::MAX)..))
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

#[proptest]
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

#[proptest]
fn post_traversal_offset_proptest(#[strategy(tree())] tree: BaoTree) {
    post_traversal_offset_impl(tree);
}

/// Check that the post order traversal iterator is consistent with the post order
/// offset function.
fn post_traversal_chunks_iter_impl(tree: BaoTree) {
    let chunks = tree.post_order_chunks_iter();
    let ranges = get_leaf_ranges(chunks);
    let union = range_union(ranges).unwrap();
    assert_eq!(union, RangeSet2::from(..tree.size));
}

#[proptest]
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

#[proptest]
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

#[proptest]
fn post_oder_outboard_fsm_proptest(#[strategy(tree())] tree: BaoTree) {
    post_oder_outboard_fsm_impl(tree);
}

fn mem_outboard_flip_impl(tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let post = PostOrderMemOutboard::create(&data, tree.block_size);
    let pre = PreOrderMemOutboard::create(data, tree.block_size);
    assert_eq!(post, pre.flip());
    assert_eq!(pre, post.flip());
    assert_eq!(post, post.flip().flip());
}

#[proptest]
fn mem_outboard_flip_proptest(#[strategy(tree())] tree: BaoTree) {
    mem_outboard_flip_impl(tree);
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn valid_ranges_sync(outboard: &PostOrderMemOutboard) -> ChunkRanges {
    crate::io::sync::valid_ranges(outboard).unwrap()
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn valid_ranges_fsm(outboard: &mut PostOrderMemOutboard) -> ChunkRanges {
    futures::executor::block_on(crate::io::fsm::valid_ranges(outboard)).unwrap()
}

fn validate_outboard_sync_pos_impl(tree: BaoTree) {
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = ChunkRanges::from(..outboard.tree().chunks());
    let actual = valid_ranges_sync(&outboard);
    assert_eq!(expected, actual)
}

async fn valid_file_ranges_test_impl() {
    // interesting cases:
    // below 16 chunks
    // exactly 16 chunks
    // 16 chunks + 1
    // 32 chunks
    // 32 chunks + 1 < seems to fail!
    let data = make_test_data(1024 * 16 * 2 + 1024 * 15);
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(4));
    let ranges = ChunkRanges::from(ChunkNum(0)..ChunkNum(120));
    // data[32768] = 0;
    let data = Bytes::from(data);
    let mut stream = crate::io::fsm::valid_file_ranges(outboard, data, &ranges);
    while let Some(item) = stream.next().await {
        let item = item.unwrap();
        println!("{:?}", item);
    }
}

/// range is a range of chunks. Just using u64 for convenience in tests
#[test]
fn valid_file_ranges_fsm() {
    futures::executor::block_on(valid_file_ranges_test_impl())
}

#[proptest]
fn validate_outboard_sync_pos_proptest(#[strategy(tree())] tree: BaoTree) {
    validate_outboard_sync_pos_impl(tree);
}

fn validate_outboard_fsm_pos_impl(tree: BaoTree) {
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = ChunkRanges::from(..outboard.tree().chunks());
    let actual = valid_ranges_fsm(&mut outboard);
    assert_eq!(expected, actual)
}

#[proptest]
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
    let expected = ChunkRanges::from(..outboard.tree().chunks());
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

#[proptest]
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
    let expected = ChunkRanges::from(..outboard.tree().chunks());
    if !outboard.data.is_empty() {
        // flip a random bit in the outboard
        flip_bit(&mut outboard.data, rand);
        // Check that at least one range is invalid
        let actual = valid_ranges_fsm(&mut outboard);
        assert_ne!(expected, actual);
    }
}

#[proptest]
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
    let ranges = ChunkRanges::all();
    let mut encoded = Vec::new();
    crate::io::sync::encode_ranges_validated(data, &outboard, &ChunkRanges::all(), &mut encoded)
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
    let ranges = ChunkRanges::all();
    let mut encoded = Vec::new();
    crate::io::fsm::encode_ranges_validated(
        Bytes::from(data.clone()),
        &mut outboard,
        &ChunkRanges::all(),
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
    ((data, outboard), (decoded.to_vec(), ob_res))
}

fn encode_decode_partial_sync_impl(
    data: &[u8],
    outboard: PostOrderMemOutboard,
    ranges: &ChunkRangesRef,
) -> bool {
    let mut encoded = Vec::new();
    crate::io::sync::encode_ranges_validated(data, &outboard, ranges, &mut encoded).unwrap();
    let expected_data = data;
    let encoded_read = std::io::Cursor::new(encoded);
    let iter = crate::io::sync::DecodeResponseIter::new(
        outboard.root,
        outboard.tree.block_size,
        encoded_read,
        ranges,
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
    ranges: ChunkRanges,
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
    while let ResponseDecoderReadingNext::More((reading1, result)) = reading.next().await {
        let item = match result {
            Ok(item) => item,
            Err(_) => {
                return false;
            }
        };
        match item {
            BaoContentItem::Leaf(Leaf { offset, data }) => {
                // check that the data matches
                if expected_data[offset.to_usize()..offset.to_usize() + data.len()] != data {
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
    true
}

#[test]
fn encode_decode_full_sync_cases() {
    let cases = [(1024 + 1, 1)];
    for (size, block_level) in cases {
        let data = &make_test_data(size);
        let block_size = BlockSize(block_level);
        let outboard = PostOrderMemOutboard::create(data, block_size);
        let pair = encode_decode_full_sync_impl(data, outboard);
        assert_tuple_eq!(pair);
    }
}

#[proptest]
fn encode_decode_full_sync_proptest(#[strategy(tree())] tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    prop_assert_tuple_eq!(encode_decode_full_sync_impl(&data, outboard));
}

#[proptest]
fn encode_decode_partial_sync_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, ChunkRanges),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, selection) = size_and_selection;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(&data, block_size);
    let ok = encode_decode_partial_sync_impl(&data, outboard, &selection);
    prop_assert!(ok);
}

#[test]
fn encode_decode_full_fsm_cases() {
    let cases = [BaoTree::new(ByteNum(0x1001), BlockSize(1))];
    for tree in cases {
        let data = make_test_data(tree.size.to_usize());
        let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
        let pair = futures::executor::block_on(encode_decode_full_fsm_impl(data, outboard));
        assert_tuple_eq!(pair);
    }
}

#[proptest]
fn encode_decode_full_fsm_proptest(#[strategy(tree())] tree: BaoTree) {
    let data = make_test_data(tree.size.to_usize());
    let outboard = PostOrderMemOutboard::create(&data, tree.block_size);
    let pair = futures::executor::block_on(encode_decode_full_fsm_impl(data, outboard));
    prop_assert_tuple_eq!(pair);
}

#[proptest]
fn encode_decode_partial_fsm_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, ChunkRanges),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, selection) = size_and_selection;
    let data = make_test_data(size);
    let outboard = PostOrderMemOutboard::create(&data, block_size);
    let ok =
        futures::executor::block_on(encode_decode_partial_fsm_impl(&data, outboard, selection));
    prop_assert!(ok);
}

fn pre_order_nodes_iter_reference(tree: BaoTree, ranges: &ChunkRangesRef) -> Vec<TreeNode> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.to_usize(),
        true,
        ranges,
        tree.block_size.to_u32(),
        tree.block_size.to_u32() + 1,
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

#[proptest]
fn pre_order_node_iter_proptest(#[strategy(tree())] tree: BaoTree) {
    let actual = tree.pre_order_nodes_iter().collect::<Vec<_>>();
    let expected = pre_order_nodes_iter_reference(tree, &ChunkRanges::all());
    prop_assert_eq!(expected, actual);
}

#[test]
fn selection_reference_comparison_cases() {
    let cases = [
        ((1026, 1), ChunkRanges::all()),
        ((2050, 1), ChunkRanges::all()),
        ((1066, 2), ChunkRanges::from(..ChunkNum(1))),
        ((1045, 0), ChunkRanges::all()),
        ((10000, 0), ChunkRanges::from(ChunkNum(u64::MAX)..)),
    ];
    for ((size, block_level), ranges) in cases {
        // println!("{} {} {:?}", size, block_level, ranges);
        let tree = BaoTree::new(ByteNum(size), BlockSize(block_level));
        let expected = partial_chunk_iter_reference(tree, &ranges, u8::MAX);
        let actual =
            ReferencePreOrderPartialChunkIterRef::new(tree, &ranges, u8::MAX).collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }
}

#[proptest]
fn selection_reference_comparison_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, ChunkRanges),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, ranges) = size_and_selection;
    let tree = BaoTree::new(ByteNum(size as u64), block_size);
    let expected = partial_chunk_iter_reference(tree, &ranges, 0);
    // let actual1 = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();
    let actual2 = ReferencePreOrderPartialChunkIterRef::new(tree, &ranges, 0).collect::<Vec<_>>();
    if actual2 != expected {
        println!();
        println!("{:?} {:?}", tree, ranges);
        println!("actual new {:?}", actual2);
        println!("expected   {:?}", expected);
        panic!();
    }
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

fn cases() -> impl Iterator<Item = (BaoTree, ChunkRanges, u8)> {
    [
        // ((1, 0), ChunkRanges::all(), 0),
        // ((1025, 0), ChunkRanges::all(), 0),
        // ((2048, 0), ChunkRanges::all(), 0),
        // ((2049, 0), ChunkRanges::all(), 0),
        // ((2049, 1), ChunkRanges::all(), 0),
        // ((2049, 0), ChunkRanges::from(..ChunkNum(1)), 0),
        // ((1024 * 32, 0), ChunkRanges::from(..ChunkNum(1)), 4),
        // ((1024 * 32, 0), ChunkRanges::all(), 4),
        // ((1024 * 32, 0), ChunkRanges::from(ChunkNum(16)..), 4),
        // ((1024 * 32, 0), ChunkRanges::from(..ChunkNum(16)), 4),
        // ((2048 + 1, 0), ChunkRanges::from(..ChunkNum(2)), 0),
        // ((8192 + 1, 0), ChunkRanges::from(..ChunkNum(8)), 2),
        // ((1037, 0), ChunkRanges::all(), 1),
        // ((1024 + 1, 0), ChunkRanges::from(..ChunkNum(1)), 2),
        ((1024 * 32 - 1, 0), ChunkRanges::from(ChunkNum(16)..), 4),
        (
            (1024 * 32 - 1, 0),
            ChunkRanges::from(ChunkNum(16)..ChunkNum(32)),
            4,
        ),
    ]
    .into_iter()
    .map(|((size, block_level), ranges, min_full_level)| {
        (
            BaoTree::new(ByteNum(size), BlockSize(block_level)),
            ranges,
            min_full_level,
        )
    })
}

#[test]
fn filtered_chunks() {
    for (tree, ranges, min_full_level) in cases() {
        println!("{:?} {:?}", tree, ranges);
        println!("encode:");
        let data = make_test_data(tree.size.to_usize());
        let (_, encoded) = encode_selected_reference(&data, BlockSize(min_full_level), &ranges);
        println!("{}", hex::encode(&encoded));
        println!("select:");
        let selected = ReferencePreOrderPartialChunkIterRef::new(tree, &ranges, min_full_level)
            .collect::<Vec<_>>();
        for item in selected {
            println!("{}", item.to_debug_string(10));
        }
    }
}

#[test]
fn response_iter_cases() {
    for (tree, ranges, min_full_level) in cases() {
        let expected = partial_chunk_iter_reference(tree, &ranges, min_full_level);
        let actual =
            PreOrderPartialChunkIterRef::new(tree, &ranges, min_full_level).collect::<Vec<_>>();
        // if expected != actual {
        println!("expected:");
        for chunk in expected {
            println!("{}", chunk.to_debug_string(10));
        }
        println!("actual:");
        for chunk in actual {
            println!("{}", chunk.to_debug_string(10));
        }
        // panic!();
        // }
    }
}

#[proptest]
fn response_iter_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, ChunkRanges),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, ranges) = size_and_selection;
    let tree = BaoTree::new(ByteNum(size as u64), BlockSize::ZERO);
    let expected = partial_chunk_iter_reference(tree, &ranges, block_size.0);
    let actual = PreOrderPartialChunkIterRef::new(tree, &ranges, block_size.0).collect::<Vec<_>>();
    if expected != actual {
        println!("expected:");
        for chunk in expected {
            println!("{}", chunk.to_debug_string(10));
        }
        println!("actual:");
        for chunk in actual {
            println!("{}", chunk.to_debug_string(10));
        }
        panic!();
    }
}

#[test]
fn response_iter_2_cases() {
    let cases =
        [((1024 + 1, 1), ChunkRanges::all())]
            .into_iter()
            .map(|((size, block_level), ranges)| {
                (BaoTree::new(ByteNum(size), BlockSize(block_level)), ranges)
            });
    for (tree, ranges) in cases {
        let expected = response_iter_reference(tree, &ranges);
        let actual = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();
        if expected != actual {
            println!("expected:");
            for chunk in expected {
                println!("{}", chunk.to_debug_string(10));
            }
            println!("actual:");
            for chunk in actual {
                println!("{}", chunk.to_debug_string(10));
            }
            panic!();
        }
    }
}

#[proptest]
fn response_iter_2_proptest(
    #[strategy(size_and_selection(0..100000, 2))] size_and_selection: (usize, ChunkRanges),
    #[strategy(block_size())] block_size: BlockSize,
) {
    let (size, ranges) = size_and_selection;
    let tree = BaoTree::new(ByteNum(size as u64), block_size);
    let expected = response_iter_reference(tree, &ranges);
    let actual = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();
    if expected != actual {
        println!("expected:");
        for chunk in expected {
            println!("{}", chunk.to_debug_string(10));
        }
        println!("actual:");
        for chunk in actual {
            println!("{}", chunk.to_debug_string(10));
        }
        panic!();
    }
}

fn chunk_ranges(int_ranges: &RangeSetRef<u64>) -> ChunkRanges {
    let mut bounds = SmallVec::with_capacity(int_ranges.boundaries().len());
    for b in int_ranges.boundaries() {
        bounds.push(ChunkNum(*b));
    }
    ChunkRanges::new_unchecked(bounds)
}

fn cr(x: impl Into<RangeSet2<u64>>) -> RangeSet2<ChunkNum> {
    chunk_ranges(&x.into())
}

#[test]
fn canonicalize_ranges_test() {
    fn size(x: u64) -> ByteNum {
        ByteNum(x)
    }

    let cases = [
        // gets canonicalized to all
        (cr(..1), size(1024), cr(..)),
        // gets left alone
        (cr(1..), size(1024), cr(1..)),
        // gets canonicalized to all
        (cr(..31), size(31 * 1024), cr(..)),
        // gets canonicalized to all
        (cr(..31) | cr(40..50), size(31 * 1024), cr(..)),
        // gets left alone
        (cr(99..), size(31 * 1024), cr(99..)),
        // gets left alone, last chunk is not included
        (cr(..30), size(31 * 1024), cr(..30)),
        // ..30 is missing the last chunk. but that will be included due to
        // having something behind the end, so canonicalize to all
        (cr(..30) | cr(1000..), size(31 * 1024), cr(..)),
        // gets left alone, two chunks are missing
        (
            cr(..29) | cr(1000..),
            size(31 * 1024),
            cr(0..29) | cr(1000..),
        ),
        // gets left alone, two chunks are missing
        (
            cr(1..2) | cr(4..29) | cr(1000..),
            size(31 * 1024),
            cr(1..2) | cr(4..29) | cr(1000..),
        ),
        // gets turned into open range
        (
            cr(1..2) | cr(4..30) | cr(1000..),
            size(31 * 1024),
            cr(1..2) | cr(4..),
        ),
        (cr(..6), size(7 * 1024), cr(..6)),
        (cr(..7), size(7 * 1024), cr(..)),
        (cr(7..), size(7 * 1024), cr(7..)),
        (cr(..10) | cr(11..12), size(7 * 1024), cr(..)),
        (cr(..6) | cr(7..10), size(7 * 1024), cr(..)),
        (cr(3..6) | cr(7..10), size(7 * 1024), cr(3..)),
        (cr(..5) | cr(7..10), size(7 * 1024), cr(..5) | cr(7..)),
    ];
    for (ranges, size, expected) in cases {
        let expected: &RangeSetRef<ChunkNum> = &expected;
        let actual = truncate_ranges(&ranges, size);
        assert_eq!(expected, actual);
    }
}
