//! Consistency tests for the io implementations.
//!
//! Every way to encode, decode, create or validate an outboard is wrapped in an
//! adapter with a uniform signature and a normalized result. Each property runs
//! all adapters on the same case and checks that they agree with each other and
//! with the expected result.
//!
//! A case is a blob size, a block size, a query and a hash mode, so keyed
//! hashing is covered by every property.
use std::{future::Future, io::Cursor};

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use test_strategy::proptest;

use crate::{
    blake3,
    io::{
        fsm,
        outboard::{
            EmptyOutboard, PostOrderMemOutboard, PostOrderOutboard, PreOrderMemOutboard,
            PreOrderOutboard,
        },
        sync, BaoContentItem, DecodeError, EncodeError, Leaf, Parent,
    },
    rec::{encode_selected_rec, truncate_ranges},
    BaoTree, BlockSize, ChunkNum, ChunkRanges, HashMode, TreeNode,
};

const KEY: [u8; 32] = [7; 32];

fn block_on<F: Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}

/// Map `pos` evenly onto `0..len`.
fn select(pos: u64, len: usize) -> usize {
    ((pos as u128 * len as u128) >> 64) as usize
}

/// `n` evenly spaced positions for the fixed cases.
fn positions(n: u64) -> impl Iterator<Item = u64> {
    (0..n).map(move |i| u64::MAX / n * i)
}

/// Test data without all zero chunks, so zero padding never looks valid.
fn test_data(size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| ((i / 1024) as u8).wrapping_mul(31) ^ i as u8 ^ 0x5a)
        .collect()
}

#[derive(Debug, Clone)]
struct Case {
    size: usize,
    block_size: BlockSize,
    ranges: ChunkRanges,
    mode: HashMode,
}

impl Case {
    fn tree(&self) -> BaoTree {
        BaoTree::new(self.size as u64, self.block_size)
    }

    fn data(&self) -> Vec<u8> {
        test_data(self.size)
    }

    fn outboard(&self, data: &[u8]) -> PostOrderMemOutboard {
        match self.mode {
            HashMode::Standard => PostOrderMemOutboard::create(data, self.block_size),
            HashMode::Keyed(key) => PostOrderMemOutboard::create_keyed(data, self.block_size, &key),
        }
    }

    #[cfg(feature = "validate")]
    fn with_mode(&self, mode: HashMode) -> Self {
        Self {
            mode,
            ..self.clone()
        }
    }

    #[cfg(feature = "validate")]
    /// The chunk ranges a validator reports for intact data: every chunk group
    /// that intersects the query. A query past the end of the blob asks for the
    /// last chunk.
    fn expected_valid(&self) -> ChunkRanges {
        let chunks = self.tree().chunks();
        let last = ChunkNum(chunks.0.saturating_sub(1));
        let mut ranges = &self.ranges & &ChunkRanges::from(..chunks);
        if !(&self.ranges & &ChunkRanges::from(last..)).is_empty() {
            ranges |= ChunkRanges::from(last..last + 1);
        }
        crate::io::round_up_to_chunks_groups(ranges, self.block_size) & ChunkRanges::from(..chunks)
    }
}

fn other_mode(mode: HashMode) -> HashMode {
    match mode {
        HashMode::Standard => HashMode::Keyed(KEY),
        HashMode::Keyed(_) => HashMode::Standard,
    }
}

fn mode() -> impl Strategy<Value = HashMode> {
    prop_oneof![Just(HashMode::Standard), Just(HashMode::Keyed(KEY))]
}

fn ranges(size: usize) -> impl Strategy<Value = ChunkRanges> {
    let chunks = ChunkNum::chunks(size as u64).0.max(1);
    let random = proptest::collection::vec((0..chunks + 2, 0..chunks + 2), 1..4).prop_map(|v| {
        let mut res = ChunkRanges::empty();
        for (a, b) in v {
            res |= ChunkRanges::from(ChunkNum(a.min(b))..ChunkNum(a.max(b) + 1));
        }
        res
    });
    prop_oneof![
        6 => random,
        1 => Just(ChunkRanges::all()),
        1 => Just(ChunkRanges::empty()),
        1 => Just(ChunkRanges::from(ChunkNum(u64::MAX)..)),
        1 => Just(ChunkRanges::from(ChunkNum(chunks - 1)..ChunkNum(chunks))),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (0..100_000usize, 0..=4u8, mode()).prop_flat_map(|(size, block_size, mode)| {
        ranges(size).prop_map(move |ranges| Case {
            size,
            block_size: BlockSize(block_size),
            ranges,
            mode,
        })
    })
}

fn fixed_cases() -> Vec<Case> {
    let cr = |start: u64, end: u64| ChunkRanges::from(ChunkNum(start)..ChunkNum(end));
    let sizes = [
        0,
        1,
        1023,
        1024,
        1025,
        2048,
        2049,
        16 * 1024 - 1,
        16 * 1024,
        16 * 1024 + 1,
        40 * 1024 + 7,
        100_000,
    ];
    let mut cases = Vec::new();
    for size in sizes {
        let chunks = ChunkNum::chunks(size as u64).0.max(1);
        let ranges = [
            ChunkRanges::empty(),
            ChunkRanges::all(),
            cr(0, 1),
            cr(1, 2),
            cr(3, 9),
            cr(5, 40),
            cr(2, 3) | cr(7, 8),
            ChunkRanges::from(ChunkNum(u64::MAX)..),
            cr(0, 1) | ChunkRanges::from(ChunkNum(u64::MAX)..),
            cr(chunks - 1, chunks),
        ];
        for block_size in [0, 2, 4] {
            for ranges in &ranges {
                for mode in [HashMode::Standard, HashMode::Keyed(KEY)] {
                    cases.push(Case {
                        size,
                        block_size: BlockSize(block_size),
                        ranges: ranges.clone(),
                        mode,
                    });
                }
            }
        }
    }
    cases
}

fn encode_error(e: EncodeError) -> String {
    match e {
        EncodeError::Io(e) => format!("Io({:?})", e.kind()),
        e => format!("{e:?}"),
    }
}

fn decode_error(e: DecodeError) -> String {
    match e {
        DecodeError::Io(e) => format!("Io({:?})", e.kind()),
        e => format!("{e:?}"),
    }
}

/// A decoded item. Adjacent leaves are merged, since implementations may
/// split the data of a chunk group into leaves differently.
#[derive(Debug, PartialEq)]
enum Item {
    Parent(TreeNode, (blake3::Hash, blake3::Hash)),
    Leaf(u64, Vec<u8>),
    Err(String),
}

fn push(items: &mut Vec<Item>, item: Item) {
    if let (Some(Item::Leaf(offset, data)), Item::Leaf(next, more)) = (items.last_mut(), &item) {
        if *offset + data.len() as u64 == *next {
            data.extend_from_slice(more);
            return;
        }
    }
    items.push(item);
}

fn decoded_item(item: Result<BaoContentItem, DecodeError>) -> Item {
    match item {
        Ok(BaoContentItem::Parent(Parent { node, pair })) => Item::Parent(node, pair),
        Ok(BaoContentItem::Leaf(Leaf { offset, data })) => Item::Leaf(offset, data.to_vec()),
        Err(e) => Item::Err(decode_error(e)),
    }
}

struct EncodeInput<'a> {
    data: &'a [u8],
    outboard: &'a PostOrderMemOutboard,
    ranges: &'a ChunkRanges,
    mode: HashMode,
}

type Encoder = fn(&EncodeInput) -> Result<Vec<u8>, String>;

/// The recursive reference implementation. It does not use the outboard.
fn encode_reference(i: &EncodeInput) -> Result<Vec<u8>, String> {
    let tree = i.outboard.tree;
    let ranges = truncate_ranges(i.ranges, tree.size());
    let mut res = Vec::new();
    encode_selected_rec(
        ChunkNum(0),
        i.data,
        true,
        ranges,
        tree.block_size().to_u32(),
        true,
        &mut res,
        i.mode,
    );
    Ok(res)
}

fn encode_sync(i: &EncodeInput) -> Result<Vec<u8>, String> {
    let mut res = Vec::new();
    match i.mode {
        HashMode::Standard => sync::encode_ranges(i.data, i.outboard, i.ranges, &mut res),
        HashMode::Keyed(key) => {
            sync::keyed_encode_ranges(i.data, i.outboard, i.ranges, &mut res, &key)
        }
    }
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_sync_validated(i: &EncodeInput) -> Result<Vec<u8>, String> {
    let mut res = Vec::new();
    match i.mode {
        HashMode::Standard => sync::encode_ranges_validated(i.data, i.outboard, i.ranges, &mut res),
        HashMode::Keyed(key) => {
            sync::keyed_encode_ranges_validated(i.data, i.outboard, i.ranges, &mut res, &key)
        }
    }
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_fsm(i: &EncodeInput) -> Result<Vec<u8>, String> {
    let data = Bytes::copy_from_slice(i.data);
    let mut res = Vec::new();
    block_on(async {
        match i.mode {
            HashMode::Standard => {
                fsm::encode_ranges(data, i.outboard.clone(), i.ranges, &mut res).await
            }
            HashMode::Keyed(key) => {
                fsm::keyed_encode_ranges(data, i.outboard.clone(), i.ranges, &mut res, &key).await
            }
        }
    })
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_fsm_validated(i: &EncodeInput) -> Result<Vec<u8>, String> {
    let data = Bytes::copy_from_slice(i.data);
    let mut res = Vec::new();
    block_on(async {
        match i.mode {
            HashMode::Standard => {
                fsm::encode_ranges_validated(data, i.outboard.clone(), i.ranges, &mut res).await
            }
            HashMode::Keyed(key) => {
                fsm::keyed_encode_ranges_validated(
                    data,
                    i.outboard.clone(),
                    i.ranges,
                    &mut res,
                    &key,
                )
                .await
            }
        }
    })
    .map_err(encode_error)?;
    Ok(res)
}

#[cfg(feature = "experimental-mixed")]
mod mixed_adapter {
    use std::{convert::Infallible, future::Future};

    use super::*;
    use crate::io::mixed::{self, EncodedItem};

    struct Collect(Vec<EncodedItem>);

    impl mixed::Sender for Collect {
        type Error = Infallible;
        fn send(
            &mut self,
            item: EncodedItem,
        ) -> impl Future<Output = std::result::Result<(), Infallible>> + '_ {
            self.0.push(item);
            std::future::ready(Ok(()))
        }
    }

    fn traverse(i: &EncodeInput) -> Vec<EncodedItem> {
        let mut send = Collect(Vec::new());
        block_on(async {
            match i.mode {
                HashMode::Standard => {
                    mixed::traverse_ranges_validated(i.data, i.outboard, i.ranges, &mut send).await
                }
                HashMode::Keyed(key) => {
                    mixed::keyed_traverse_ranges_validated(
                        i.data, i.outboard, i.ranges, &mut send, &key,
                    )
                    .await
                }
            }
        })
        .unwrap();
        send.0
    }

    pub(super) fn encode_mixed(i: &EncodeInput) -> Result<Vec<u8>, String> {
        let mut res = Vec::new();
        for item in traverse(i) {
            match item {
                EncodedItem::Parent(Parent { pair: (l, r), .. }) => {
                    res.extend_from_slice(l.as_bytes());
                    res.extend_from_slice(r.as_bytes());
                }
                EncodedItem::Leaf(Leaf { data, .. }) => res.extend_from_slice(&data),
                EncodedItem::Error(e) => return Err(encode_error(e)),
                EncodedItem::Size(_) | EncodedItem::Done => {}
            }
        }
        Ok(res)
    }

    pub(super) fn mixed_items(i: &EncodeInput) -> Vec<Item> {
        let mut res = Vec::new();
        for item in traverse(i) {
            let item = match item {
                EncodedItem::Parent(Parent { node, pair }) => Item::Parent(node, pair),
                EncodedItem::Leaf(Leaf { offset, data }) => Item::Leaf(offset, data.to_vec()),
                EncodedItem::Error(e) => Item::Err(encode_error(e)),
                EncodedItem::Size(_) | EncodedItem::Done => continue,
            };
            push(&mut res, item);
        }
        res
    }
}

const ENCODERS: &[(&str, Encoder)] = &[
    ("reference", encode_reference),
    ("sync", encode_sync),
    ("sync validated", encode_sync_validated),
    ("fsm", encode_fsm),
    ("fsm validated", encode_fsm_validated),
    #[cfg(feature = "experimental-mixed")]
    ("mixed", mixed_adapter::encode_mixed),
];

const VALIDATING_ENCODERS: &[(&str, Encoder)] = &[
    ("sync validated", encode_sync_validated),
    ("fsm validated", encode_fsm_validated),
    #[cfg(feature = "experimental-mixed")]
    ("mixed", mixed_adapter::encode_mixed),
];

struct DecodeInput<'a> {
    root: blake3::Hash,
    tree: BaoTree,
    encoded: &'a [u8],
    ranges: &'a ChunkRanges,
    mode: HashMode,
}

type Decoder = fn(&DecodeInput) -> Vec<Item>;

fn decode_sync(i: &DecodeInput) -> Vec<Item> {
    let encoded = Cursor::new(i.encoded);
    let iter = match i.mode {
        HashMode::Standard => sync::DecodeResponseIter::new(i.root, i.tree, encoded, i.ranges),
        HashMode::Keyed(key) => {
            sync::DecodeResponseIter::new_keyed(i.root, i.tree, encoded, i.ranges, &key)
        }
    };
    let mut res = Vec::new();
    for item in iter {
        push(&mut res, decoded_item(item));
    }
    res
}

fn decode_fsm(i: &DecodeInput) -> Vec<Item> {
    let encoded = Cursor::new(i.encoded);
    let ranges = i.ranges.clone();
    let mut decoder = match i.mode {
        HashMode::Standard => fsm::ResponseDecoder::new(i.root, ranges, i.tree, encoded),
        HashMode::Keyed(key) => {
            fsm::ResponseDecoder::new_keyed(i.root, ranges, i.tree, encoded, &key)
        }
    };
    let mut res = Vec::new();
    block_on(async {
        while let fsm::ResponseDecoderNext::More((next, item)) = decoder.next().await {
            assert_eq!(*next.hash(), i.root);
            push(&mut res, decoded_item(item));
            decoder = next;
        }
    });
    res
}

const DECODERS: &[(&str, Decoder)] = &[("sync", decode_sync), ("fsm", decode_fsm)];

/// The parents and leaves the decoders yield for a valid encoding.
fn valid_items(case: &Case, data: &[u8], outboard: &PostOrderMemOutboard) -> (Vec<u8>, Vec<Item>) {
    let encoded = encode_reference(&EncodeInput {
        data,
        outboard,
        ranges: &case.ranges,
        mode: case.mode,
    })
    .unwrap();
    let items = decode_sync(&DecodeInput {
        root: outboard.root,
        tree: outboard.tree,
        encoded: &encoded,
        ranges: &case.ranges,
        mode: case.mode,
    });
    (encoded, items)
}

/// Property 1: all encoders produce the same bytes as the reference.
fn encoders_agree(case: &Case) {
    let data = case.data();
    let outboard = case.outboard(&data);
    let input = EncodeInput {
        data: &data,
        outboard: &outboard,
        ranges: &case.ranges,
        mode: case.mode,
    };
    let expected = encode_reference(&input);
    for (name, encode) in ENCODERS {
        assert_eq!(encode(&input), expected, "{name} differs for {case:?}");
    }
}

/// Property 2: all decoders, and the mixed traversal, yield the same items
/// for a valid encoding, and the leaves contain the original data.
fn decoders_agree(case: &Case) {
    let data = case.data();
    let outboard = case.outboard(&data);
    let (encoded, expected) = valid_items(case, &data, &outboard);
    for item in &expected {
        match item {
            Item::Leaf(offset, leaf) => {
                let start = *offset as usize;
                assert_eq!(leaf[..], data[start..start + leaf.len()], "{case:?}");
            }
            Item::Parent(..) => {}
            Item::Err(e) => panic!("valid encoding failed to decode: {e} for {case:?}"),
        }
    }
    let input = DecodeInput {
        root: outboard.root,
        tree: outboard.tree,
        encoded: &encoded,
        ranges: &case.ranges,
        mode: case.mode,
    };
    for (name, decode) in DECODERS {
        assert_eq!(decode(&input), expected, "{name} differs for {case:?}");
    }
    #[cfg(feature = "experimental-mixed")]
    {
        let input = EncodeInput {
            data: &data,
            outboard: &outboard,
            ranges: &case.ranges,
            mode: case.mode,
        };
        assert_eq!(
            mixed_adapter::mixed_items(&input),
            expected,
            "mixed differs for {case:?}"
        );
    }
}

/// Check that decode_ranges wrote the leaves and, if there is an outboard,
/// the stored parents.
fn check_decoded(
    case: &Case,
    name: &str,
    items: &[Item],
    target: &[u8],
    outboard: Option<&dyn sync::Outboard>,
) {
    let tree = case.tree();
    for item in items {
        match item {
            Item::Leaf(offset, leaf) => {
                let start = *offset as usize;
                assert_eq!(
                    target[start..start + leaf.len()],
                    leaf[..],
                    "{name} {case:?}"
                );
            }
            Item::Parent(node, pair) if tree.pre_order_offset(*node).is_some() => {
                if let Some(outboard) = outboard {
                    assert_eq!(
                        outboard.load(*node).unwrap(),
                        Some(*pair),
                        "{name} {case:?}"
                    );
                }
            }
            _ => {}
        }
    }
}

fn decode_ranges_sync<O: sync::Outboard + sync::OutboardMut>(
    case: &Case,
    encoded: &[u8],
    outboard: &mut O,
) -> Vec<u8> {
    let mut target = Vec::new();
    match case.mode {
        HashMode::Standard => sync::decode_ranges(encoded, &case.ranges, &mut target, outboard),
        HashMode::Keyed(key) => {
            sync::keyed_decode_ranges(encoded, &case.ranges, &mut target, outboard, &key)
        }
    }
    .unwrap();
    target
}

fn decode_ranges_fsm<O: fsm::Outboard + fsm::OutboardMut>(
    case: &Case,
    encoded: &[u8],
    outboard: &mut O,
) -> Vec<u8> {
    let mut target = BytesMut::new();
    let ranges = case.ranges.clone();
    block_on(async {
        match case.mode {
            HashMode::Standard => fsm::decode_ranges(encoded, ranges, &mut target, outboard).await,
            HashMode::Keyed(key) => {
                fsm::keyed_decode_ranges(encoded, ranges, &mut target, outboard, &key).await
            }
        }
    })
    .unwrap();
    target.to_vec()
}

/// Property 3: decode_ranges writes the data and the stored parents into
/// every kind of outboard.
fn decode_ranges_round_trip(case: &Case) {
    let data = case.data();
    let source = case.outboard(&data);
    let (encoded, items) = valid_items(case, &data, &source);
    let (root, tree, size) = (source.root, source.tree, source.data.len());
    let check = |name, target: Vec<u8>, outboard: Option<&dyn sync::Outboard>| {
        check_decoded(case, name, &items, &target, outboard)
    };

    let mut ob = PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    check(
        "sync pre mem",
        decode_ranges_sync(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = PostOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    check(
        "sync post mem",
        decode_ranges_sync(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = PreOrderOutboard {
        root,
        tree,
        data: Vec::new(),
    };
    check(
        "sync pre file",
        decode_ranges_sync(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = PostOrderOutboard {
        root,
        tree,
        data: Vec::new(),
    };
    check(
        "sync post file",
        decode_ranges_sync(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = EmptyOutboard { root, tree };
    check(
        "sync empty",
        decode_ranges_sync(case, &encoded, &mut ob),
        None,
    );

    let mut ob = PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    check(
        "fsm pre mem",
        decode_ranges_fsm(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = PostOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    check(
        "fsm post mem",
        decode_ranges_fsm(case, &encoded, &mut ob),
        Some(&ob),
    );
    let mut ob = PreOrderOutboard {
        root,
        tree,
        data: BytesMut::new(),
    };
    let target = decode_ranges_fsm(case, &encoded, &mut ob);
    let ob = PreOrderOutboard {
        root,
        tree,
        data: ob.data.to_vec(),
    };
    check("fsm pre file", target, Some(&ob));
    let mut ob = PostOrderOutboard {
        root,
        tree,
        data: BytesMut::new(),
    };
    let target = decode_ranges_fsm(case, &encoded, &mut ob);
    let ob = PostOrderOutboard {
        root,
        tree,
        data: ob.data.to_vec(),
    };
    check("fsm post file", target, Some(&ob));
    let mut ob = EmptyOutboard { root, tree };
    check(
        "fsm empty",
        decode_ranges_fsm(case, &encoded, &mut ob),
        None,
    );
}

/// Property 4: all decoders yield the same items for a corrupted or truncated
/// encoding, ending with exactly one error.
fn decoders_agree_on_corruption(case: &Case, pos: u64) {
    let data = case.data();
    let outboard = case.outboard(&data);
    let (encoded, _) = valid_items(case, &data, &outboard);
    if encoded.is_empty() {
        return;
    }
    let pos = select(pos, encoded.len());
    let mut flipped = encoded.clone();
    flipped[pos] ^= 1;
    for broken in [flipped, encoded[..pos].to_vec()] {
        let input = DecodeInput {
            root: outboard.root,
            tree: outboard.tree,
            encoded: &broken,
            ranges: &case.ranges,
            mode: case.mode,
        };
        let expected = decode_sync(&input);
        let errors = expected
            .iter()
            .filter(|item| matches!(item, Item::Err(_)))
            .count();
        assert_eq!(errors, 1, "{case:?} pos {pos}");
        assert!(matches!(expected.last(), Some(Item::Err(_))), "{case:?}");
        for (name, decode) in DECODERS {
            assert_eq!(
                decode(&input),
                expected,
                "{name} differs for {case:?} pos {pos}"
            );
        }
    }
}

type Pairs = Vec<Option<(blake3::Hash, blake3::Hash)>>;

/// Root and stored hash pairs of an outboard, in pre order.
fn outboard_shape(outboard: impl sync::Outboard) -> (blake3::Hash, Pairs) {
    let tree = outboard.tree();
    let pairs = tree
        .pre_order_nodes_iter()
        .map(|node| outboard.load(node).unwrap())
        .collect();
    (outboard.root(), pairs)
}

/// Property 5: every way to create an outboard gives the same outboard, and
/// the root is the BLAKE3 hash of the data.
fn outboards_agree(case: &Case) {
    let data = case.data();
    let tree = case.tree();
    let root = match case.mode {
        HashMode::Standard => blake3::hash(&data),
        HashMode::Keyed(key) => blake3::keyed_hash(&key, &data),
    };
    let expected = outboard_shape(case.outboard(&data));
    assert_eq!(expected.0, root, "{case:?}");
    let size = tree.outboard_size() as usize;
    let mut shapes = Vec::new();

    let pre = match case.mode {
        HashMode::Standard => PreOrderMemOutboard::create(&data, case.block_size),
        HashMode::Keyed(key) => PreOrderMemOutboard::create_keyed(&data, case.block_size, &key),
    };
    shapes.push(("pre mem create", outboard_shape(pre)));

    let mut pre = PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    pre.root = match case.mode {
        HashMode::Standard => sync::outboard(&data[..], tree, &mut pre),
        HashMode::Keyed(key) => sync::keyed_outboard(&data[..], tree, &mut pre, &key),
    }
    .unwrap();
    shapes.push(("sync outboard", outboard_shape(pre)));

    let mut post = Vec::new();
    let root2 = match case.mode {
        HashMode::Standard => sync::outboard_post_order(&data[..], tree, &mut post),
        HashMode::Keyed(key) => sync::keyed_outboard_post_order(&data[..], tree, &mut post, &key),
    }
    .unwrap();
    let post = PostOrderMemOutboard {
        root: root2,
        tree,
        data: post,
    };
    shapes.push(("sync outboard_post_order", outboard_shape(post)));

    let mut pre = PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    pre.root = block_on(async {
        let reader = Cursor::new(Bytes::copy_from_slice(&data));
        match case.mode {
            HashMode::Standard => fsm::outboard(reader, tree, &mut pre).await,
            HashMode::Keyed(key) => fsm::keyed_outboard(reader, tree, &mut pre, &key).await,
        }
    })
    .unwrap();
    shapes.push(("fsm outboard", outboard_shape(pre)));

    let mut post = Vec::new();
    let root2 = block_on(async {
        let reader = Cursor::new(Bytes::copy_from_slice(&data));
        match case.mode {
            HashMode::Standard => fsm::outboard_post_order(reader, tree, &mut post).await,
            HashMode::Keyed(key) => {
                fsm::keyed_outboard_post_order(reader, tree, &mut post, &key).await
            }
        }
    })
    .unwrap();
    let post = PostOrderMemOutboard {
        root: root2,
        tree,
        data: post,
    };
    shapes.push(("fsm outboard_post_order", outboard_shape(post)));

    if let HashMode::Standard = case.mode {
        let pre = <PreOrderOutboard<Vec<u8>> as sync::CreateOutboard>::create(
            Cursor::new(&data),
            case.block_size,
        )
        .unwrap();
        shapes.push(("sync create pre", outboard_shape(pre)));
        let post = <PostOrderOutboard<Vec<u8>> as sync::CreateOutboard>::create(
            Cursor::new(&data),
            case.block_size,
        )
        .unwrap();
        shapes.push(("sync create post", outboard_shape(post)));
        let pre = block_on(<PreOrderOutboard<BytesMut> as fsm::CreateOutboard>::create(
            Bytes::copy_from_slice(&data),
            case.block_size,
        ))
        .unwrap();
        let pre = PreOrderOutboard {
            root: pre.root,
            tree: pre.tree,
            data: pre.data.to_vec(),
        };
        shapes.push(("fsm create pre", outboard_shape(pre)));
        let post = block_on(
            <PostOrderOutboard<BytesMut> as fsm::CreateOutboard>::create(
                Bytes::copy_from_slice(&data),
                case.block_size,
            ),
        )
        .unwrap();
        let post = PostOrderOutboard {
            root: post.root,
            tree: post.tree,
            data: post.data.to_vec(),
        };
        shapes.push(("fsm create post", outboard_shape(post)));
    }

    for (name, shape) in shapes {
        assert_eq!(shape, expected, "{name} differs for {case:?}");
    }
}

#[cfg(feature = "validate")]
mod validators {
    use std::ops::Range;

    use futures_lite::StreamExt;

    use super::*;

    fn union(ranges: Vec<std::io::Result<Range<ChunkNum>>>) -> ChunkRanges {
        let mut res = ChunkRanges::empty();
        for range in ranges {
            let range = range.unwrap();
            if range.start < range.end {
                res |= ChunkRanges::from(range);
            }
        }
        res
    }

    fn valid_ranges_sync(case: &Case, outboard: impl sync::Outboard, data: &[u8]) -> ChunkRanges {
        union(match case.mode {
            HashMode::Standard => sync::valid_ranges(outboard, data, &case.ranges)
                .into_iter()
                .collect(),
            HashMode::Keyed(key) => sync::keyed_valid_ranges(outboard, data, &case.ranges, &key)
                .into_iter()
                .collect(),
        })
    }

    fn valid_ranges_fsm(case: &Case, outboard: impl fsm::Outboard, data: &[u8]) -> ChunkRanges {
        let data = Bytes::copy_from_slice(data);
        union(block_on(async {
            match case.mode {
                HashMode::Standard => {
                    fsm::valid_ranges(outboard, data, &case.ranges)
                        .collect()
                        .await
                }
                HashMode::Keyed(key) => {
                    fsm::keyed_valid_ranges(outboard, data, &case.ranges, &key)
                        .collect()
                        .await
                }
            }
        }))
    }

    fn valid_outboard_ranges_sync(case: &Case, outboard: impl sync::Outboard) -> ChunkRanges {
        union(match case.mode {
            HashMode::Standard => sync::valid_outboard_ranges(outboard, &case.ranges)
                .into_iter()
                .collect(),
            HashMode::Keyed(key) => sync::keyed_valid_outboard_ranges(outboard, &case.ranges, &key)
                .into_iter()
                .collect(),
        })
    }

    fn valid_outboard_ranges_fsm(case: &Case, outboard: impl fsm::Outboard) -> ChunkRanges {
        union(block_on(async {
            match case.mode {
                HashMode::Standard => {
                    fsm::valid_outboard_ranges(outboard, &case.ranges)
                        .collect()
                        .await
                }
                HashMode::Keyed(key) => {
                    fsm::keyed_valid_outboard_ranges(outboard, &case.ranges, &key)
                        .collect()
                        .await
                }
            }
        }))
    }

    /// Validate with all validators, using a pre order outboard given as bytes.
    ///
    /// The outboard bytes may be shorter than the outboard, so they are used
    /// with the file outboards.
    fn validate(
        case: &Case,
        root: blake3::Hash,
        outboard: &[u8],
        data: &[u8],
    ) -> (ChunkRanges, ChunkRanges) {
        let tree = case.tree();
        let sync_ob = || PreOrderOutboard {
            root,
            tree,
            data: outboard.to_vec(),
        };
        let fsm_ob = || PreOrderOutboard {
            root,
            tree,
            data: Bytes::copy_from_slice(outboard),
        };
        let data_ranges = valid_ranges_sync(case, sync_ob(), data);
        assert_eq!(
            valid_ranges_fsm(case, fsm_ob(), data),
            data_ranges,
            "fsm valid_ranges {case:?}"
        );
        let outboard_ranges = valid_outboard_ranges_sync(case, sync_ob());
        assert_eq!(
            valid_outboard_ranges_fsm(case, fsm_ob()),
            outboard_ranges,
            "fsm valid_outboard_ranges {case:?}"
        );
        (data_ranges, outboard_ranges)
    }

    fn chunk_group(case: &Case, chunk: u64) -> ChunkRanges {
        let start = ChunkNum::chunk_group_start(ChunkNum(chunk), case.block_size);
        let end = ChunkNum::chunk_group_end(ChunkNum(chunk + 1), case.block_size);
        ChunkRanges::from(start..end)
    }

    /// The subtrees whose hash pairs are stored in the given bytes of a pre
    /// order outboard.
    fn subtrees(case: &Case, bytes: Range<usize>) -> ChunkRanges {
        let tree = case.tree();
        let mut res = ChunkRanges::empty();
        for node in tree.pre_order_nodes_iter() {
            if let Some(offset) = tree.pre_order_offset(node) {
                let pair = (offset * 64) as usize..(offset * 64 + 64) as usize;
                if pair.start < bytes.end && bytes.start < pair.end {
                    res |= ChunkRanges::from(node.chunk_range());
                }
            }
        }
        res
    }

    /// Property 6: the validators agree with each other and report exactly the
    /// chunk groups that are intact.
    pub(super) fn validators_agree(case: &Case, pos: u64) {
        let data = case.data();
        let pre = PostOrderMemOutboard::flip(&case.outboard(&data));
        let root = pre.root;
        let expected = case.expected_valid();

        let (data_ranges, outboard_ranges) = validate(case, root, &pre.data, &data);
        assert_eq!(data_ranges, expected, "intact {case:?}");
        assert_eq!(outboard_ranges, expected, "intact outboard {case:?}");

        if !data.is_empty() {
            let pos = select(pos, data.len());
            let mut corrupted = data.clone();
            corrupted[pos] ^= 1;
            let invalid = chunk_group(case, pos as u64 / 1024);
            let (data_ranges, _) = validate(case, root, &pre.data, &corrupted);
            assert_eq!(
                data_ranges,
                &expected - &invalid,
                "corrupted data at {pos} {case:?}"
            );
            let limit = ChunkNum::chunk_group_start(ChunkNum(pos as u64 / 1024), case.block_size);
            let (data_ranges, _) = validate(case, root, &pre.data, &data[..pos]);
            assert_eq!(
                data_ranges,
                &expected & &ChunkRanges::from(..limit),
                "truncated data at {pos} {case:?}"
            );
        }

        if !pre.data.is_empty() {
            let pos = select(pos, pre.data.len());
            let mut corrupted = pre.data.clone();
            corrupted[pos] ^= 1;
            let invalid = subtrees(case, pos..pos + 1);
            let (data_ranges, outboard_ranges) = validate(case, root, &corrupted, &data);
            assert_eq!(
                data_ranges,
                &expected - &invalid,
                "corrupted outboard at {pos} {case:?}"
            );
            assert_eq!(
                outboard_ranges,
                &expected - &invalid,
                "corrupted outboard at {pos} {case:?}"
            );
            let invalid = subtrees(case, pos..pre.data.len());
            let (data_ranges, outboard_ranges) = validate(case, root, &pre.data[..pos], &data);
            assert_eq!(
                data_ranges,
                &expected - &invalid,
                "truncated outboard at {pos} {case:?}"
            );
            assert_eq!(
                outboard_ranges,
                &expected - &invalid,
                "truncated outboard at {pos} {case:?}"
            );
        }
    }

    /// Validating with the wrong hash mode finds nothing valid.
    pub(super) fn validate_wrong_mode(case: &Case) {
        let data = case.data();
        let pre = PostOrderMemOutboard::flip(&case.outboard(&data));
        let wrong = case.with_mode(other_mode(case.mode));
        let sync_ob = PreOrderOutboard {
            root: pre.root,
            tree: pre.tree,
            data: pre.data.clone(),
        };
        assert_eq!(
            valid_ranges_sync(&wrong, sync_ob, &data),
            ChunkRanges::empty(),
            "{case:?}"
        );
    }
}

/// Property 7: using the wrong hash mode fails in every validating encoder,
/// every decoder and every validator.
fn wrong_mode_fails(case: &Case) {
    let data = case.data();
    let outboard = case.outboard(&data);
    let wrong = other_mode(case.mode);
    let (encoded, _) = valid_items(case, &data, &outboard);
    let ranges = truncate_ranges(&case.ranges, case.size as u64);
    let input = EncodeInput {
        data: &data,
        outboard: &outboard,
        ranges: &case.ranges,
        mode: wrong,
    };
    for (name, encode) in VALIDATING_ENCODERS {
        let res = encode(&input);
        if ranges.is_empty() {
            assert_eq!(res, Ok(Vec::new()), "{name} {case:?}");
        } else {
            assert!(res.is_err(), "{name} accepted the wrong mode for {case:?}");
        }
    }
    let input = DecodeInput {
        root: outboard.root,
        tree: outboard.tree,
        encoded: &encoded,
        ranges: &case.ranges,
        mode: wrong,
    };
    for (name, decode) in DECODERS {
        let items = decode(&input);
        if ranges.is_empty() {
            assert_eq!(items, Vec::new(), "{name} {case:?}");
        } else {
            assert!(
                matches!(&items[..], [Item::Err(_)]),
                "{name} accepted the wrong mode for {case:?}: {items:?}"
            );
        }
    }
    #[cfg(feature = "validate")]
    validators::validate_wrong_mode(case);
}

#[test]
fn encoders_agree_cases() {
    fixed_cases().iter().for_each(encoders_agree);
}

#[proptest]
fn encoders_agree_proptest(#[strategy(case())] case: Case) {
    encoders_agree(&case);
}

#[test]
fn decoders_agree_cases() {
    fixed_cases().iter().for_each(decoders_agree);
}

#[proptest]
fn decoders_agree_proptest(#[strategy(case())] case: Case) {
    decoders_agree(&case);
}

#[test]
fn decode_ranges_round_trip_cases() {
    fixed_cases().iter().for_each(decode_ranges_round_trip);
}

#[proptest]
fn decode_ranges_round_trip_proptest(#[strategy(case())] case: Case) {
    decode_ranges_round_trip(&case);
}

#[test]
fn decoders_agree_on_corruption_cases() {
    for case in fixed_cases() {
        positions(8).for_each(|pos| decoders_agree_on_corruption(&case, pos));
    }
}

#[proptest]
fn decoders_agree_on_corruption_proptest(#[strategy(case())] case: Case, pos: u64) {
    decoders_agree_on_corruption(&case, pos);
}

#[test]
fn outboards_agree_cases() {
    fixed_cases().iter().for_each(outboards_agree);
}

#[proptest]
fn outboards_agree_proptest(#[strategy(case())] case: Case) {
    outboards_agree(&case);
}

#[cfg(feature = "validate")]
#[test]
fn validators_agree_cases() {
    for case in fixed_cases() {
        positions(4).for_each(|pos| validators::validators_agree(&case, pos));
    }
}

#[cfg(feature = "validate")]
#[proptest]
fn validators_agree_proptest(#[strategy(case())] case: Case, pos: u64) {
    validators::validators_agree(&case, pos);
}

#[test]
fn wrong_mode_fails_cases() {
    fixed_cases().iter().for_each(wrong_mode_fails);
}

#[proptest]
fn wrong_mode_fails_proptest(#[strategy(case())] case: Case) {
    wrong_mode_fails(&case);
}
