//! Tests for the tree geometry and traversals: node offsets, the iterators
//! over a [BaoTree](crate::BaoTree) compared to recursive reference
//! implementations, range selection and truncation.
use std::ops::Range;

use proptest::prelude::*;

use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use test_strategy::proptest;

use crate::{
    assert_tuple_eq, blake3,
    io::full_chunk_groups,
    iter::{BaoChunk, PreOrderPartialChunkIterRef, ResponseIterRef},
    rec::{
        encode_selected_rec, get_leaf_ranges, make_test_data, partial_chunk_iter_reference,
        range_union, response_iter_reference, select_nodes_rec, truncate_ranges,
        ReferencePreOrderPartialChunkIterRef,
    },
    BaoTree, BlockSize, ChunkNum, ChunkRanges, ChunkRangesRef, HashMode, ResponseIter, TreeNode,
};

pub(super) fn last_chunk(size: u64) -> Range<u64> {
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

#[test]
fn select_last_chunk_0() {
    assert_tuple_eq!(select_last_chunk_impl(1, 0));
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

fn tree() -> impl Strategy<Value = BaoTree> {
    (0u64..100000, 0u8..5).prop_map(|(size, block_size)| {
        let block_size = BlockSize(block_size);
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
    let selection = prop_oneof! {
        40 => selection(size_range.end as u64, n),
        5 => Just(ChunkRanges::all()),
        5 => Just(ChunkRanges::from(ChunkNum(u64::MAX)..)),
        1 => Just(ChunkRanges::empty())
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

#[test]
fn huge_tree_last_leaf_cases() {
    let sizes = [
        u64::MAX,
        u64::MAX - 1,
        u64::MAX - 1023,
        u64::MAX - 1024,
        1 << 63,
        (1 << 63) + 1,
    ];
    for size in sizes {
        for block_size in [BlockSize(0), BlockSize(4)] {
            let tree = BaoTree::new(size, block_size);
            let ranges = ChunkRanges::from(ChunkNum(u64::MAX - 5)..);
            let leaves = crate::iter::ResponseIter::new(tree, ranges)
                .filter_map(|chunk| match chunk {
                    BaoChunk::Leaf {
                        start_chunk, size, ..
                    } => Some((start_chunk.to_bytes(), size as u64)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(leaves.len(), 1);
            let (start, len) = leaves[0];
            assert_eq!(start, ChunkNum::chunks(size).0.saturating_sub(1) * 1024);
            assert_eq!(start + len, size);
        }
    }
}

fn pre_order_nodes_iter_reference(tree: BaoTree, ranges: &ChunkRangesRef) -> Vec<TreeNode> {
    let mut res = Vec::new();
    select_nodes_rec(
        ChunkNum(0),
        tree.size.try_into().unwrap(),
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
        let tree = BaoTree::new(size, BlockSize(block_level));
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
    let tree = BaoTree::new(size as u64, block_size);
    let expected = partial_chunk_iter_reference(tree, &ranges, 0);
    // let actual1 = ResponseIterRef::new(tree, &ranges).collect::<Vec<_>>();
    let actual2 = ReferencePreOrderPartialChunkIterRef::new(tree, &ranges, 0).collect::<Vec<_>>();
    if actual2 != expected {
        println!();
        println!("{tree:?} {ranges:?}");
        println!("actual new {actual2:?}");
        println!("expected   {expected:?}");
        panic!();
    }
}

/// Reference implementation of encode_ranges_validated that uses the simple recursive impl.
///
/// Uses unkeyed BLAKE3 mode.
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
        HashMode::Standard,
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
            BaoTree::new(size, BlockSize(block_level)),
            ranges,
            min_full_level,
        )
    })
}

#[test]
fn filtered_chunks() {
    for (tree, ranges, min_full_level) in cases() {
        println!("{tree:?} {ranges:?}");
        println!("encode:");
        let data = make_test_data(tree.size.try_into().unwrap());
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
    let tree = BaoTree::new(size as u64, BlockSize::ZERO);
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
    let cases = [((1024 + 1, 1), ChunkRanges::all())]
        .into_iter()
        .map(|((size, block_level), ranges)| (BaoTree::new(size, BlockSize(block_level)), ranges));
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
    let tree = BaoTree::new(size as u64, block_size);
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
    fn size(x: u64) -> u64 {
        x
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

::proptest::proptest! {
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

    /// Checks that the leafs produced by ranges_pre_order_chunks_iter_ref
    /// cover the entire data exactly once.
    #[test]
    fn max_skip_level(size in 0..32786u64, block_size in 0..2u8, max_skip_level in 0..2u8) {
        let tree = BaoTree::new(size, BlockSize(block_size));
        let ranges = ChunkRanges::all();
        let leaf_ranges = get_leaf_ranges(tree.ranges_pre_order_chunks_iter_ref(&ranges, max_skip_level)).collect::<Vec<_>>();
        prop_assert_eq!(range_union(leaf_ranges), Some(RangeSet2::from(0..size)));
    }

}

fn byte_ranges() -> impl Strategy<Value = crate::ByteRanges> {
    let ranges = proptest::collection::vec((0..12_000u64, 0..12_000u64), 0..4);
    (ranges, proptest::option::of(0..12_000u64)).prop_map(|(ranges, open)| {
        let mut res = crate::ByteRanges::empty();
        for (a, b) in ranges {
            res |= crate::ByteRanges::from(a.min(b)..a.max(b));
        }
        if let Some(start) = open {
            res |= crate::ByteRanges::from(start..);
        }
        res
    })
}

/// A chunk is in the result of round_up_to_chunks if and only if it overlaps
/// the byte ranges.
#[proptest]
fn round_up_to_chunks_proptest(#[strategy(byte_ranges())] ranges: crate::ByteRanges) {
    let chunks = crate::io::round_up_to_chunks(&ranges);
    for chunk in 0..14u64 {
        let bytes = crate::ByteRanges::from(chunk * 1024..(chunk + 1) * 1024);
        let overlaps = !(&ranges & &bytes).is_empty();
        prop_assert_eq!(
            chunks.contains(&ChunkNum(chunk)),
            overlaps,
            "chunk {}",
            chunk
        );
    }
}

/// All nodes of a tree with 64 leaf pairs.
fn small_tree_nodes() -> impl Iterator<Item = TreeNode> {
    (0..127).map(TreeNode)
}

/// The node range of a node contains exactly the node and the nodes below it.
#[test]
fn node_range() {
    for node in small_tree_nodes() {
        let chunks = node.chunk_range();
        let below = small_tree_nodes()
            .filter(|other| {
                let range = other.chunk_range();
                chunks.start <= range.start && range.end <= chunks.end
            })
            .collect::<Vec<_>>();
        let range = node.node_range();
        let expected = (range.start.0..range.end.0)
            .map(TreeNode)
            .collect::<Vec<_>>();
        assert_eq!(below, expected, "{node:?}");
    }
}

/// The post order range of a node contains exactly the post order offsets of
/// the node and the nodes below it.
#[test]
fn post_order_range() {
    for node in small_tree_nodes() {
        let range = node.node_range();
        let mut offsets = (range.start.0..range.end.0)
            .map(|n| TreeNode(n).post_order_offset())
            .collect::<Vec<_>>();
        offsets.sort();
        let expected = node.post_order_range().collect::<Vec<_>>();
        assert_eq!(offsets, expected, "{node:?}");
    }
}

/// The next left ancestor is the closest ancestor with a smaller node number.
#[test]
fn next_left_ancestor() {
    for node in small_tree_nodes() {
        let mut ancestor = node.parent();
        while let Some(candidate) = ancestor {
            if candidate < node {
                break;
            }
            ancestor = candidate.parent();
        }
        assert_eq!(node.next_left_ancestor(), ancestor, "{node:?}");
    }
}

/// The root covers all chunks of the tree, and its left child does not. The
/// block size is ignored.
#[test]
#[allow(deprecated)]
fn root() {
    for size in [0u64, 1, 1024, 1025, 2048, 2049, 16 * 1024, 100_000] {
        let tree = BaoTree::new(size, BlockSize::ZERO);
        let root = tree.root();
        let chunks = tree.chunks().max(ChunkNum(1));
        assert!(root.chunk_range().end >= chunks, "size {size}");
        if let Some(left) = root.left_child() {
            assert!(left.chunk_range().end < chunks, "size {size}");
        }
        assert_eq!(BaoTree::new(size, BlockSize(4)).root(), root);
    }
}
