use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    ops::Range,
};

use bytes::BytesMut;
use proptest::prelude::*;
use range_collections::{range_set::RangeSetEntry, RangeSet2, RangeSetRef};
use smallvec::SmallVec;

use crate::{
    blake3,
    io::{
        sync::{DecodeResponseItem, Outboard},
        Leaf,
    },
    recursive_hash_subtree,
};

use super::{
    canonicalize_range,
    io::outboard::{PostOrderMemOutboard, PreOrderMemOutboardMut},
    io::sync::{encode_ranges, encode_ranges_validated, DecodeResponseIter},
    iter::{BaoChunk, NodeInfo},
    pre_order_offset_slow,
    tree::{ByteNum, ChunkNum},
    BaoTree, BlockSize, PostOrderNodeIter, TreeNode,
};

macro_rules! assert_tuple_eq {
    ($tuple:expr) => {
        assert_eq!($tuple.0, $tuple.1);
    };
}

macro_rules! prop_assert_tuple_eq {
    ($tuple:expr) => {
        let (a, b) = $tuple;
        prop_assert_eq!(a, b);
    };
}

fn canonicalize_range_owned(range: &RangeSetRef<ChunkNum>, end: ByteNum) -> RangeSet2<ChunkNum> {
    match canonicalize_range(range, end.chunks()) {
        Ok(range) => {
            let t = SmallVec::from(range.boundaries());
            RangeSet2::new(t).unwrap()
        }
        Err(range) => RangeSet2::from(range),
    }
}

fn make_test_data(n: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(n);
    for i in 0..n {
        data.push((i / 1024) as u8);
    }
    data
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

/// Given a *post order* outboard, encode a slice of data
fn encode_ranges_2(
    data: &[u8],
    outboard: &[u8],
    ranges: &RangeSetRef<ChunkNum>,
    block_size: BlockSize,
) -> Vec<u8> {
    let size = ByteNum(data.len() as u64);
    match canonicalize_range(ranges, size.chunks()) {
        Ok(ranges) => encode_ranges_impl_2(data, ranges, block_size),
        Err(range) => {
            let ranges = RangeSet2::from(range);
            encode_ranges_impl_2(data, &ranges, block_size)
        }
    }
}

fn encode_ranges_impl_2(
    data: &[u8],
    ranges: &RangeSetRef<ChunkNum>,
    block_size: BlockSize,
) -> Vec<u8> {
    let mut res = Vec::new();
    let size = ByteNum(data.len() as u64);
    res.extend_from_slice(&size.0.to_le_bytes());
    let _hash = crate::io::sync::bao_encode_selected_recursive(
        ChunkNum(0),
        data,
        true,
        ranges,
        block_size.0 as u32,
        &mut res,
    );
    res
}

fn encode_ranges_impl(
    data: &[u8],
    outboard: &[u8],
    ranges: &RangeSetRef<ChunkNum>,
    block_size: BlockSize,
) -> Vec<u8> {
    let mut res = Vec::new();
    let tree = BaoTree::new(ByteNum(data.len() as u64), block_size);
    res.extend_from_slice(&tree.size.0.to_le_bytes());
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent { node, .. } => {
                let offset = tree.post_order_offset(node).unwrap();
                let hash_offset = usize::try_from(offset.value() * 64).unwrap();
                res.extend_from_slice(&outboard[hash_offset..hash_offset + 64]);
            }
            BaoChunk::Leaf {
                size, start_chunk, ..
            } => {
                let start = start_chunk.to_bytes().to_usize();
                res.extend_from_slice(&data[start..start + size]);
            }
        }
    }
    res
}

/// Computes a reference post order outboard using the abao crate (chunk_group_log = 0) and the non-standard finalize_post_order function.
fn post_order_outboard_reference_2(data: &[u8]) -> PostOrderMemOutboard {
    let mut outboard = Vec::new();
    let cursor = std::io::Cursor::new(&mut outboard);
    let mut encoder = abao::encode::Encoder::new_outboard(cursor);
    encoder.write_all(data).unwrap();
    // requires non standard fn finalize_post_order
    let hash = encoder.finalize_post_order().unwrap();
    // remove the length suffix
    outboard.truncate(outboard.len() - 8);
    let hash = blake3::Hash::from(*hash.as_bytes());
    PostOrderMemOutboard::new(
        hash,
        BaoTree::new(ByteNum(data.len() as u64), BlockSize::DEFAULT),
        outboard,
    )
}

/// Computes a reference pre order outboard using the bao crate (chunk_group_log = 0) and then flips it to a post-order outboard.
fn post_order_outboard_reference(data: &[u8]) -> PostOrderMemOutboard {
    let mut outboard = Vec::new();
    let cursor = Cursor::new(&mut outboard);
    let mut encoder = bao::encode::Encoder::new_outboard(cursor);
    encoder.write_all(data).unwrap();
    let hash = encoder.finalize().unwrap();
    let hash = blake3::Hash::from(*hash.as_bytes());
    let pre = PreOrderMemOutboardMut::new(hash, BlockSize::DEFAULT, outboard, false);
    pre.unwrap().flip()
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
    let hash = blake3::Hash::from(*hash.as_bytes());
    (res, hash)
}

fn bao_tree_encode_slice_comparison_impl(data: Vec<u8>, mut range: Range<ChunkNum>) {
    let expected = encode_slice_reference(&data, range.clone()).0;
    let ob = BaoTree::outboard_post_order_mem(&data, BlockSize::DEFAULT);
    let hash = ob.root();
    let outboard = ob.into_inner_with_suffix();
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let ranges = RangeSet2::from(range);
    let actual = encode_ranges_2(&data, &outboard, &ranges, BlockSize::DEFAULT);
    assert_eq!(expected.len(), actual.len());
    assert_eq!(expected, actual);

    // for this we have to canonicalize the range before
    let ranges = canonicalize_range_owned(&ranges, ByteNum(data.len() as u64));
    let mut actual2 = Vec::new();
    let ob = PostOrderMemOutboard::load(hash, &outboard, BlockSize::DEFAULT).unwrap();
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
    let size = ByteNum(data.len() as u64);
    let expected = data;
    let ranges = canonicalize_range_owned(&RangeSet2::from(range), size);
    let mut ec = Cursor::new(encoded);
    for item in decode_ranges_into_chunks(root, BlockSize::DEFAULT, &mut ec, &ranges) {
        let (pos, slice) = item.unwrap();
        let pos = pos.to_usize();
        assert_eq!(expected[pos..pos + slice.len()], *slice);
    }
}

#[cfg(feature = "fsm")]
mod fsm_tests {
    /// range is a range of chunks. Just using u64 for convenience in tests
    async fn bao_tree_decode_slice_fsm_impl(data: Vec<u8>, range: Range<u64>) {
        let range = ChunkNum(range.start)..ChunkNum(range.end);
        let (encoded, root) = encode_slice_reference(&data, range.clone());
        let size = ByteNum(data.len() as u64);
        let expected = data;
        let ranges = canonicalize_range_owned(&RangeSet2::from(range), size);
        let mut ec = Cursor::new(encoded);
        let at_start = ResponseDecoderStart::new(root, ranges, BlockSize::DEFAULT, &mut ec);
        let (mut reading, _size) = at_start.next().await.unwrap();
        while let ResponseDecoderReadingNext::More((next_state, item)) = reading.next().await {
            if let DecodeResponseItem::Leaf(Leaf { offset, data }) = item.unwrap() {
                let pos = offset.to_usize();
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
            futures::executor::block_on(bao_tree_decode_slice_fsm_impl(data, chunk_range));
        }
    }
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn validate_outboard_sync_impl(
    outboard: &PostOrderMemOutboard,
) -> (RangeSet2<ChunkNum>, RangeSet2<ChunkNum>) {
    let expected = RangeSet2::from(..outboard.tree().chunks());
    let actual = crate::io::sync::valid_ranges(outboard).unwrap();
    (expected, actual)
}

/// range is a range of chunks. Just using u64 for convenience in tests
fn validate_outboard_async_impl(
    outboard: &mut PostOrderMemOutboard,
) -> (RangeSet2<ChunkNum>, RangeSet2<ChunkNum>) {
    let expected = RangeSet2::from(..outboard.tree().chunks());
    let actual = futures::executor::block_on(crate::io::fsm::valid_ranges(outboard)).unwrap();
    (expected, actual)
}

fn bao_tree_outboard_comparison_impl(data: Vec<u8>) {
    let post1 = post_order_outboard_reference(&data);
    // let (expected, expected_hash) = post_order_outboard_reference_2(&data);
    let post2 = BaoTree::outboard_post_order_mem(&data, BlockSize::DEFAULT);
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
        let ob = BaoTree::outboard_post_order_mem(&td, block_size);
        let hash = ob.root();
        let outboard = ob.into_inner_with_suffix();
        assert_eq!(expected, hash);
        assert_eq!(
            ByteNum(outboard.len() as u64),
            BaoTree::outboard_size(ByteNum(td.len() as u64), block_size)
        );
    }
}

/// encodes the data as outboard with the given chunk_group_log, then uses that outboard to
/// encode a slice of the data, and compares the result to the original data
fn bao_tree_slice_roundtrip_test(data: Vec<u8>, mut range: Range<ChunkNum>, block_size: BlockSize) {
    let ob = BaoTree::outboard_post_order_mem(&data, block_size);
    let root = ob.root();
    let outboard = ob.into_inner_with_suffix();
    // extend empty range to contain at least 1 byte
    if range.start == range.end {
        range.end.0 += 1;
    };
    let encoded = encode_ranges_2(
        &data,
        &outboard,
        &RangeSet2::from(range.clone()),
        block_size,
    );
    let expected = data.clone();
    let mut all_ranges: range_collections::RangeSet<[ByteNum; 2]> = RangeSet2::empty();
    println!("{} {:?} {}", data.len(), range, block_size.0);
    println!("{}", hex::encode(&encoded));
    let mut ec = Cursor::new(encoded);
    for item in decode_ranges_into_chunks(root, block_size, &mut ec, &RangeSet2::from(range)) {
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
        // (0, 0..1),
        // (1, 0..1),
        // (1023, 0..1),
        // (1024, 0..1),
        // (1025, 0..1),
        // (2047, 0..1),
        // (2048, 0..1),
        (10000, 0..1),
        // (20000, 0..1),
        // (24 * 1024 + 1, 0..25),
        // (1025, 1..2),
        // (2047, 1..2),
        // (2048, 1..2),
        // (10000, 1..2),
        // (20000, 1..2),
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
fn outboard_wrong_hash() {
    let data = make_test_data(100000000);
    let expected = blake3::hash(&data);
    let actual = BaoTree::outboard_post_order_mem(&data, BlockSize(4)).root();
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
    let po = BaoTree::outboard_post_order_mem(&data, BlockSize::DEFAULT);
    let post = po.into_inner_with_suffix();
    let (mut pre, _) = bao::encode::outboard(data);
    pre.splice(..8, []);
    let map = pre
        .chunks_exact(64)
        .enumerate()
        .map(|(i, h)| (h, i))
        .collect::<HashMap<_, _>>();
    let tree = BaoTree::new(ByteNum(size as u64), BlockSize::DEFAULT);
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

fn compare_pre_order_outboard(case: usize) {
    let size = ByteNum(case as u64);
    let tree = BaoTree::new(size, BlockSize::DEFAULT);
    let perm = create_permutation_reference(case);

    // print!("{:08b}", perm.len());
    for (k, v) in perm {
        let expected = v as u64;
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
        // this works for full trees!
        println!(
            "{:09b}\t{}\t{}\t{}",
            k.0,
            expected,
            corrected,
            actual - corrected
        );
        // let depth = tree.root().level() as u64;
        // println!("{} {}", depth, k.0);
        assert_eq!(v as u64, pre_order_offset_slow(k.0, tree.filled_size().0));
    }
    println!();
}

fn pre_order_outboard_line(case: usize) {
    let size = ByteNum(case as u64);
    let tree = BaoTree::new(size, BlockSize::DEFAULT);
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
fn test_pre_order_outboard_fast() {
    let cases = [1024 * 78];
    for case in cases {
        compare_pre_order_outboard(case);
    }

    for case in 0..256 {
        pre_order_outboard_line(case * 1024);
    }
}

pub struct PostOrderTreeIterStack {
    len: TreeNode,
    // stack of (node, done) pairs
    // done=true means we immediately return the node
    //
    // this is not big enough for the worst case, but it's fine to allocate
    // for a giant tree
    //
    // todo: figure out how to get rid of the done flag
    stack: SmallVec<[(TreeNode, bool); 8]>,
}

impl PostOrderTreeIterStack {
    pub(crate) fn new(tree: BaoTree) -> Self {
        let mut stack = SmallVec::new();
        stack.push((tree.root(), false));
        let len = tree.filled_size();
        Self { len, stack }
    }
}
impl Iterator for PostOrderTreeIterStack {
    type Item = TreeNode;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, done) = self.stack.pop()?;
            if done || node.is_leaf() {
                return Some(node);
            } else {
                // push node back on stack, with done=true
                self.stack.push((node, true));
                // push right child on stack first, with done=false
                self.stack
                    .push((node.right_descendant(self.len).unwrap(), false));
                // push left child on stack, with done=false
                self.stack.push((node.left_child().unwrap(), false));
            }
        }
    }
}

/// Decode encoded ranges given the root hash
pub fn decode_ranges_into_chunks<'a>(
    root: blake3::Hash,
    block_size: BlockSize,
    encoded: impl Read + 'a,
    ranges: &'a RangeSetRef<ChunkNum>,
) -> impl Iterator<Item = std::io::Result<(ByteNum, Vec<u8>)>> + 'a {
    let scratch = BytesMut::with_capacity(block_size.bytes());
    let iter = DecodeResponseIter::new(root, block_size, encoded, ranges, scratch);
    iter.filter_map(|item| match item {
        Ok(item) => {
            if let DecodeResponseItem::Leaf(Leaf { offset, data }) = item {
                Some(Ok((offset, data.to_vec())))
            } else {
                None
            }
        }
        Err(e) => Some(Err(e.into())),
    })
}

/// Total number of nodes in the tree
///
/// Each leaf node contains up to 2 blocks, and for n leaf nodes there will
/// be n-1 branch nodes
///
/// Note that this is not the same as the number of hashes in the outboard.
fn node_count(tree: &BaoTree) -> u64 {
    let blocks = tree.blocks().0 - 1;
    blocks.saturating_sub(1).max(1)
}

/// iterate over all nodes in the tree in depth first, left to right, post order
///
/// Recursive reference implementation, just used in tests
fn iterate_reference(tree: &BaoTree) -> Vec<TreeNode> {
    fn iterate_rec(valid_nodes: TreeNode, nn: TreeNode, res: &mut Vec<TreeNode>) {
        if !nn.is_leaf() {
            let l = nn.left_child().unwrap();
            let r = nn.right_descendant(valid_nodes).unwrap();
            iterate_rec(valid_nodes, l, res);
            iterate_rec(valid_nodes, r, res);
        }
        res.push(nn);
    }
    // todo: make this a proper iterator
    let nodes = node_count(tree);
    let mut res = Vec::with_capacity(nodes.try_into().unwrap());
    iterate_rec(tree.filled_size(), tree.root(), &mut res);
    res
}

/// iterate over all nodes in the tree in depth first, left to right, pre order
/// that are required to validate the given ranges
///
/// Recursive reference implementation, just used in tests
fn iterate_part_preorder_reference<'a>(
    tree: &BaoTree,
    ranges: &'a RangeSetRef<ChunkNum>,
    max_skip_level: u8,
) -> Vec<NodeInfo<'a>> {
    fn iterate_part_rec<'a>(
        tree: &BaoTree,
        node: TreeNode,
        ranges: &'a RangeSetRef<ChunkNum>,
        max_skip_level: u32,
        is_root: bool,
        res: &mut Vec<NodeInfo<'a>>,
    ) {
        if ranges.is_empty() {
            return;
        }
        // the middle chunk of the node
        let mid = node.mid().to_chunks(tree.block_size);
        // the start chunk of the node
        let start = node.block_range().start.to_chunks(tree.block_size);
        // check if the node is fully included
        let full = ranges.boundaries().len() == 1 && ranges.boundaries()[0] <= start;
        // split the ranges into left and right
        let (l_ranges, r_ranges) = ranges.split(mid);

        let query_leaf = node.is_leaf() || (full && node.level() <= max_skip_level);
        let is_half_leaf = !tree.is_persisted(node);
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
    let can_be_root = tree.start_chunk == 0;
    iterate_part_rec(
        tree,
        tree.root(),
        ranges,
        max_skip_level as u32,
        can_be_root,
        &mut res,
    );
    res
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

fn get_leaf_ranges(
    tree: BaoTree,
    ranges: &RangeSetRef<ChunkNum>,
    max_skip_level: u8,
) -> impl Iterator<Item = Range<u64>> + '_ {
    tree.ranges_pre_order_chunks_iter_ref(ranges, max_skip_level)
        .filter_map(|e| {
            if let BaoChunk::Leaf {
                start_chunk, size, ..
            } = e
            {
                let start = start_chunk.to_bytes().0;
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
    size_range.prop_flat_map(move |size| (Just(size), selection(size as u64, n)))
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

#[test]
fn bao_encode_selected_recursive_0() {
    let data = make_test_data(1024 * 3);
    let overhead = |data, max_skip_level| {
        let mut actual_encoded = Vec::new();
        crate::io::sync::bao_encode_selected_recursive(
            ChunkNum(0),
            data,
            true,
            &RangeSet2::all(),
            max_skip_level,
            &mut actual_encoded,
        );
        actual_encoded.len() - data.len()
    };
    assert_eq!(overhead(&data, 0), 64 * 2);
    assert_eq!(overhead(&data, 1), 64 * 1);
    assert_eq!(overhead(&data, 2), 64 * 0);
}

/// Encode a small subset of a large blob, and check that the encoded data is small
#[test]
fn bao_encode_selected_recursive_large() {
    // a rather big piece of data
    let data = make_test_data(1024 * 1024 * 16 + 12345);
    // compute an outboard at a block size of 2^4 = 16 chunks
    let outboard = BaoTree::outboard_post_order_mem(&data, BlockSize(4));

    let get_encoded = |ranges| {
        let mut encoded = Vec::new();
        crate::io::sync::encode_ranges_validated(&data, &outboard, ranges, &mut encoded).unwrap();
        encoded
    };
    //
    // let ranges = RangeSet2::from(..ChunkNum(1));
    // let encoded = get_encoded(&ranges);
    // assert_eq!(encoded.len() - 8, 1024 + 15 * 64);
    // let ranges = RangeSet2::from(ChunkNum(1000)..ChunkNum(1001));
    // let encoded = get_encoded(&ranges);
    // assert_eq!(encoded.len() - 8, 1024 + 15 * 64);
    let ranges = RangeSet2::from(ChunkNum(3000)..ChunkNum(3001));
    let encoded = get_encoded(&ranges);
    assert_eq!(encoded.len() - 8, 1024 + 15 * 64);

    for chunk in decode_ranges_into_chunks(
        outboard.root,
        outboard.tree().block_size,
        &encoded[..],
        &ranges,
    ) {
        println!("{:?}", chunk);
    }
}

proptest! {

    /// Checks that the simple recursive impl bao_encode_selected_recursive that
    /// does not need an outboard is the same as the more complex encode_ranges_validated
    /// that requires an outboard.
    #[test]
    fn bao_encode_selected_recursive((size, ranges) in size_and_selection(1..32768, 2)) {
        let data = make_test_data(size);
        let expected = blake3::hash(&data);
        let mut actual_encoded = Vec::new();
        let actual = crate::io::sync::bao_encode_selected_recursive(
            ChunkNum(0),
            &data,
            true,
            &ranges,
            0,
            &mut actual_encoded,
        );
        let mut expected_encoded = Vec::new();
        let outboard = BaoTree::outboard_post_order_mem(&data, BlockSize::DEFAULT);
        encode_ranges_validated(
            &data,
            &outboard,
            &ranges,
            &mut expected_encoded,
        ).unwrap();
        expected_encoded.splice(..8, []);
        prop_assert_eq!(expected, actual);
        prop_assert_eq!(expected_encoded, actual_encoded);
    }

    /// Checks that the leafs produced by ranges_pre_order_chunks_iter_ref
    /// cover the entire data exactly once.
    #[test]
    fn max_skip_level(size in 0..32786u64, block_size in 0..2u8, max_skip_level in 0..2u8) {
        let tree = BaoTree::new(ByteNum(size), BlockSize(block_size));
        let ranges = RangeSet2::all();
        let leaf_ranges = get_leaf_ranges(tree, &ranges, max_skip_level).collect::<Vec<_>>();
        prop_assert_eq!(range_union(leaf_ranges), Some(RangeSet2::from(0..size)));
    }

    #[test]
    fn flip(len in 0usize..32768) {
        let data = make_test_data(len);
        let post1 = post_order_outboard_reference(&data);
        let post2 = post_order_outboard_reference_2(&data);
        prop_assert_eq!(&post1, &post2);
        prop_assert_eq!(&post1, &post1.flip().flip());
    }

    /// Check that the unrestricted pre-order iterator is the same as the
    /// restricted pre-order iterator for the entire tree.
    #[test]
    fn pre_order_iter_comparison(len in 0..1000000u64, level in 0u8..4) {
        let tree = BaoTree::new(ByteNum(len), BlockSize(level));
        let iter1 = tree.pre_order_nodes_iter().collect::<Vec<_>>();
        let iter2 = tree.ranges_pre_order_nodes_iter(&RangeSet2::all(), 0).map(|x| x.node).collect::<Vec<_>>();
        prop_assert_eq!(iter1, iter2);
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
        let level = BlockSize(level);
        let data = make_test_data(len.to_usize());
        let chunk_range = start .. start + size;
        bao_tree_slice_roundtrip_test(data, chunk_range, level);
    }

    /// Compares the PostOrderNodeIter with a simple stack-based reference implementation.
    #[test]
    fn iterator_reference_comparison(len in 0u64..100000) {
        let tree = BaoTree::new(ByteNum(len), BlockSize::DEFAULT);
        let iter1 = iterate_reference(&tree);
        let iter2 = PostOrderTreeIterStack::new(tree).collect::<Vec<_>>();
        let iter3 = PostOrderNodeIter::new(tree).collect::<Vec<_>>();
        prop_assert_eq!(&iter1, &iter2);
        prop_assert_eq!(&iter1, &iter3);
    }

    /// Compares the ranges iter with a recursive reference implementation.
    #[test]
    fn partial_iterator_reference_comparison((len, start, size) in size_and_slice_overlapping()) {
        let tree = BaoTree::new(len, BlockSize::DEFAULT);
        let chunk_range = start .. start + size;
        let rs = RangeSet2::from(chunk_range);
        let iter1 = iterate_part_preorder_reference(&tree, &rs, 0);
        let iter2 = tree.ranges_pre_order_nodes_iter(&rs, 0).collect::<Vec<_>>();
        prop_assert_eq!(&iter1, &iter2);
    }

    #[test]
    fn pre_post_outboard(n in 0usize..1000000) {
        compare_pre_order_outboard(n);
    }

    #[test]
    fn validate_outboard_test(size in 0usize..32768, rand in any::<usize>()) {
        let data = make_test_data(size);
        let mut outboard = BaoTree::outboard_post_order_mem(data, BlockSize::DEFAULT);
        let (expected, actual) = validate_outboard_sync_impl(&outboard);
        prop_assert_eq!(expected, actual);

        let (expected, actual) = validate_outboard_async_impl(&mut outboard);
        prop_assert_eq!(expected, actual);
        if !outboard.data.is_empty() {
            // flip a random bit in the outboard
            // this is the post order outboard without the length suffix,
            // so it's all hashes
            let bit = rand % outboard.data.len() * 8;
            let byte = bit / 8;
            let bit = bit % 8;
            outboard.data[byte] ^= 1 << bit;
            // Check that at least one range is invalid
            let (expected, actual) = validate_outboard_sync_impl(&outboard);
            prop_assert_ne!(expected, actual);

            let (expected, actual) = validate_outboard_async_impl(&mut outboard);
            prop_assert_ne!(expected, actual);
        }
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
