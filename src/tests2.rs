//! About these tests
//!
//! The tests are structured as follows:
//!
//! There is a function <testname>_impl that is the actual implementation of the test.
//! There is a proptest called <testname>_proptest that calls the test multiple times with random data.
//! There is a test called <testname>_cases that calls the test with a few hardcoded values, either
//! handcrafted or from a previous failure of a proptest.
use std::ops::Range;

use proptest::{prop_assert, strategy::Strategy};
use range_collections::{range_set::RangeSetEntry, RangeSet2};

use crate::{
    blake3, hash_subtree,
    io::{outboard::PostOrderMemOutboard, sync::Outboard},
    iter::{select_nodes_rec, BaoChunk, ResponseChunk},
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
    let ranges = RangeSet2::all();
    let mut nodes = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.to_usize(),
        true,
        &ranges,
        tree.block_size.0 as u32,
        &mut |chunk| {
            if let ResponseChunk::Parent { node, is_root, .. } = chunk {
                nodes.push((node, is_root));
            }
        },
    );
    for (node, is_root) in nodes {
        if let Some((l_hash, r_hash)) = outboard.load(node).unwrap() {
            let start_chunk = node.chunk_range().start;
            let byte_range = tree.byte_range(node);
            let data = &data[byte_range.start.to_usize()..byte_range.end.to_usize()];
            let expected = hash_subtree(start_chunk.0, data, is_root);
            let actual = blake3::guts::parent_cv(&l_hash, &r_hash, is_root);
            assert_eq!(actual, expected);
        }
    }
}

/// Brute force test for an outboard that just computes the expected hash for each pair
async fn outboard_test_fsm(data: &[u8], mut outboard: impl crate::io::fsm::Outboard) {
    let tree = outboard.tree();
    let ranges = RangeSet2::all();
    let mut nodes = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.to_usize(),
        true,
        &ranges,
        tree.block_size.0 as u32,
        &mut |chunk| {
            if let ResponseChunk::Parent { node, is_root, .. } = chunk {
                nodes.push((node, is_root));
            }
        },
    );
    for (node, is_root) in nodes {
        if let Some((l_hash, r_hash)) = outboard.load(node).await.unwrap() {
            let start_chunk = node.chunk_range().start;
            let byte_range = tree.byte_range(node);
            let data = &data[byte_range.start.to_usize()..byte_range.end.to_usize()];
            let expected = hash_subtree(start_chunk.0, data, is_root);
            let actual = blake3::guts::parent_cv(&l_hash, &r_hash, is_root);
            assert_eq!(actual, expected);
        }
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

fn validate_outboard_sync_neg_impl(tree: BaoTree, rand: u32) {
    let rand = rand as usize;
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    if !outboard.data.is_empty() {
        // flip a random bit in the outboard
        // this is the post order outboard without the length suffix,
        // so it's all hashes
        let bit = rand % outboard.data.len() * 8;
        let byte = bit / 8;
        let bit = bit % 8;
        outboard.data[byte] ^= 1 << bit;
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

fn validate_outboard_fsm_neg_impl(tree: BaoTree, rand: u32) {
    let rand = rand as usize;
    let size = tree.size.to_usize();
    let block_size = tree.block_size;
    let data = make_test_data(size);
    let mut outboard = PostOrderMemOutboard::create(data, block_size);
    let expected = RangeSet2::from(..outboard.tree().chunks());
    if !outboard.data.is_empty() {
        // flip a random bit in the outboard
        // this is the post order outboard without the length suffix,
        // so it's all hashes
        let bit = rand % outboard.data.len() * 8;
        let byte = bit / 8;
        let bit = bit % 8;
        outboard.data[byte] ^= 1 << bit;
        // Check that at least one range is invalid
        let actual = valid_ranges_fsm(&mut outboard);
        assert_ne!(expected, actual);
    }
}

#[test_strategy::proptest]
fn validate_outboard_fsm_neg_proptest(#[strategy(tree())] tree: BaoTree, rand: u32) {
    validate_outboard_fsm_neg_impl(tree, rand);
}
