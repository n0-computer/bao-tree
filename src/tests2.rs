//! About these tests
//!
//! The tests are structured as follows:
//!
//! There is a function <testname>_impl that is the actual implementation of the test.
//! There is a proptest called <testname>_proptest that calls the test multiple times with random data.
//! There is a test called <testname>_cases that calls the test with a few hardcoded values, either
//! handcrafted or from a previous failure of a proptest.
use std::ops::Range;

use bytes::Bytes;
use proptest::prelude::*;
use range_collections::{RangeSet2, RangeSetRef};
use smallvec::SmallVec;
use test_strategy::proptest;

use crate::{
    blake3,
    io::outboard::PreOrderMemOutboard,
    iter::{BaoChunk, PreOrderPartialChunkIterRef, ResponseIterRef},
    rec::{
        encode_selected_rec, get_leaf_ranges, make_test_data, partial_chunk_iter_reference,
        range_union, response_iter_reference, select_nodes_rec, truncate_ranges,
        ReferencePreOrderPartialChunkIterRef,
    },
    BaoTree, BlockSize, ChunkNum, ChunkRanges, ChunkRangesRef, HashMode, TreeNode,
};

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

/// An outboard that violates the contract of [crate::io::sync::Outboard::load]
/// by returning `Ok(None)` for nodes that should be stored.
struct NoneOutboard(PreOrderMemOutboard);

impl crate::io::sync::Outboard for NoneOutboard {
    fn root(&self) -> blake3::Hash {
        self.0.root
    }
    fn tree(&self) -> BaoTree {
        self.0.tree
    }
    fn load(&self, node: TreeNode) -> std::io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        if node.level() == 0 {
            Ok(None)
        } else {
            crate::io::sync::Outboard::load(&self.0, node)
        }
    }
}

impl crate::io::fsm::Outboard for NoneOutboard {
    fn root(&self) -> blake3::Hash {
        self.0.root
    }
    fn tree(&self) -> BaoTree {
        self.0.tree
    }
    async fn load(
        &mut self,
        node: TreeNode,
    ) -> std::io::Result<Option<(blake3::Hash, blake3::Hash)>> {
        crate::io::sync::Outboard::load(self, node)
    }
}

fn is_missing_hash_pair(res: Result<(), crate::io::EncodeError>) -> bool {
    matches!(res, Err(crate::io::EncodeError::Io(e)) if e.kind() == std::io::ErrorKind::InvalidData)
}

#[test]
fn encode_outboard_none_cases() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let data = make_test_data(1024 * 16);
    let ranges = ChunkRanges::all();
    let outboard = || NoneOutboard(PreOrderMemOutboard::create(&data, BlockSize(0)));
    let mut encoded = Vec::new();
    assert!(is_missing_hash_pair(crate::io::sync::encode_ranges(
        &data[..],
        outboard(),
        &ranges,
        &mut encoded,
    )));
    assert!(is_missing_hash_pair(
        crate::io::sync::encode_ranges_validated(&data[..], outboard(), &ranges, &mut encoded)
    ));
    assert!(is_missing_hash_pair(rt.block_on(
        crate::io::fsm::encode_ranges(Bytes::from(data.clone()), outboard(), &ranges, &mut encoded,)
    )));
    assert!(is_missing_hash_pair(rt.block_on(
        crate::io::fsm::encode_ranges_validated(
            Bytes::from(data.clone()),
            outboard(),
            &ranges,
            &mut encoded,
        )
    )));
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

/// Validate a large blob with the fsm validator.
#[cfg(feature = "validate")]
#[test]
fn validate_bug() {
    use futures_lite::StreamExt;
    let data = Bytes::from(make_test_data(19308432));
    let outboard = crate::io::outboard::PostOrderMemOutboard::create(&data, BlockSize(4));
    let expected = ChunkRanges::from(..ChunkNum::chunks(data.len() as u64));
    let ranges = ChunkRanges::all();
    let actual = futures_lite::future::block_on(
        crate::io::fsm::valid_ranges(outboard, data, &ranges).collect::<Vec<_>>(),
    );
    let mut res = ChunkRanges::empty();
    for range in actual {
        res |= ChunkRanges::from(range.unwrap());
    }
    assert_eq!(res, expected);
}
