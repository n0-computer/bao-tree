//! Tests for the io implementations.
//!
//! Every way to encode, decode, create or validate an outboard is wrapped in an
//! adapter with a uniform signature and a normalized result. Each property runs
//! all adapters on the same case and checks that they agree with each other and
//! with the expected result.
//!
//! A case is a blob size, a block size, a query and a hash mode, so keyed
//! hashing is covered by every property.
//!
//! At the end are cases the properties do not cover: an outboard that
//! violates its contract, large blobs, and the size of encodings.
use std::{future::Future, io::Cursor};

use bytes::{Bytes, BytesMut};
use proptest::prelude::*;
use test_strategy::proptest;
use testresult::TestResult;

use super::tree::last_chunk;
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
    rec::{encode_selected_rec, make_test_data, truncate_ranges},
    BaoTree, BlockSize, ChunkNum, ChunkRanges, HashMode, TreeNode,
};

const KEY: [u8; 32] = [7; 32];
const OTHER_KEY: [u8; 32] = [9; 32];

/// Call the unkeyed or the keyed variant of an io function, depending on the
/// hash mode, and apply the same method calls to the result. The keyed
/// variant takes the key as its last argument.
macro_rules! with_mode {
    ($mode:expr, $f:path, $keyed:path, ($($arg:expr),* $(,)?) $($rest:tt)*) => {
        match $mode {
            HashMode::Standard => $f($($arg),*) $($rest)*,
            HashMode::Keyed(key) => $keyed($($arg,)* &key) $($rest)*,
        }
    };
}

mod errors;

fn block_on<F: Future>(future: F) -> F::Output {
    futures_lite::future::block_on(future)
}

/// Use a property in a proptest. Errors already panic where they occur.
fn check(res: TestResult) {
    if let Err(e) = res {
        match e {}
    }
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

fn reference_outboard(data: &[u8], block_size: BlockSize, mode: HashMode) -> PostOrderMemOutboard {
    with_mode!(
        mode,
        PostOrderMemOutboard::create,
        PostOrderMemOutboard::create_keyed,
        (data, block_size)
    )
}

#[derive(Debug, Clone)]
struct Case {
    size: usize,
    block_size: BlockSize,
    ranges: ChunkRanges,
    mode: HashMode,
}

/// The data of a case and its outboard.
struct Blob {
    data: Vec<u8>,
    outboard: PostOrderMemOutboard,
}

impl Case {
    fn tree(&self) -> BaoTree {
        BaoTree::new(self.size as u64, self.block_size)
    }

    fn blob(&self) -> Blob {
        let data = test_data(self.size);
        let outboard = reference_outboard(&data, self.block_size, self.mode);
        Blob { data, outboard }
    }

    fn encode_input<'a>(&'a self, blob: &'a Blob) -> EncodeInput<'a> {
        EncodeInput {
            data: &blob.data,
            outboard: &blob.outboard,
            ranges: &self.ranges,
            mode: self.mode,
        }
    }

    fn decode_input<'a>(&'a self, blob: &Blob, encoded: &'a [u8]) -> DecodeInput<'a> {
        DecodeInput {
            root: blob.outboard.root,
            tree: blob.outboard.tree,
            encoded,
            ranges: &self.ranges,
            mode: self.mode,
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

/// All hash modes other than `mode`: unkeyed, keyed, and keyed with a different key.
fn other_modes(mode: HashMode) -> Vec<HashMode> {
    let all = [
        HashMode::Standard,
        HashMode::Keyed(KEY),
        HashMode::Keyed(OTHER_KEY),
    ];
    all.into_iter()
        .filter(|other| match (mode, other) {
            (HashMode::Standard, HashMode::Standard) => false,
            (HashMode::Keyed(a), HashMode::Keyed(b)) => a != *b,
            _ => true,
        })
        .collect()
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

type Pair = (blake3::Hash, blake3::Hash);

/// A decoded item. Adjacent leaves are merged, since implementations may
/// split the data of a chunk group into leaves differently.
#[derive(Debug, PartialEq)]
enum Item {
    Parent(TreeNode, Pair),
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

impl EncodeInput<'_> {
    fn with_mode(self, mode: HashMode) -> Self {
        Self { mode, ..self }
    }
}

/// The encoded bytes, or the error.
type Encoded = Result<Vec<u8>, String>;

type Encoder = fn(&EncodeInput) -> Encoded;

/// The recursive reference implementation. It does not use the outboard.
fn encode_reference(i: &EncodeInput) -> Encoded {
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

fn encode_sync(i: &EncodeInput) -> Encoded {
    let mut res = Vec::new();
    with_mode!(
        i.mode,
        sync::encode_ranges,
        sync::keyed_encode_ranges,
        (i.data, i.outboard, i.ranges, &mut res)
    )
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_sync_validated(i: &EncodeInput) -> Encoded {
    let mut res = Vec::new();
    with_mode!(
        i.mode,
        sync::encode_ranges_validated,
        sync::keyed_encode_ranges_validated,
        (i.data, i.outboard, i.ranges, &mut res)
    )
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_fsm(i: &EncodeInput) -> Encoded {
    let data = Bytes::copy_from_slice(i.data);
    let mut res = Vec::new();
    block_on(async {
        with_mode!(
            i.mode,
            fsm::encode_ranges,
            fsm::keyed_encode_ranges,
            (data, i.outboard.clone(), i.ranges, &mut res).await
        )
    })
    .map_err(encode_error)?;
    Ok(res)
}

fn encode_fsm_validated(i: &EncodeInput) -> Encoded {
    let data = Bytes::copy_from_slice(i.data);
    let mut res = Vec::new();
    block_on(async {
        with_mode!(
            i.mode,
            fsm::encode_ranges_validated,
            fsm::keyed_encode_ranges_validated,
            (data, i.outboard.clone(), i.ranges, &mut res).await
        )
    })
    .map_err(encode_error)?;
    Ok(res)
}

#[cfg(feature = "experimental-mixed")]
mod mixed_adapter {
    use std::convert::Infallible;

    use super::*;
    use crate::io::mixed::{self, EncodedItem, ReadBytesAt};

    pub(super) struct Collect(pub(super) Vec<EncodedItem>);

    impl mixed::Sender for Collect {
        type Error = Infallible;
        fn send(&mut self, item: EncodedItem) -> impl Future<Output = Result<(), Infallible>> + '_ {
            self.0.push(item);
            std::future::ready(Ok(()))
        }
    }

    /// A sender that fails after `limit` items.
    struct FailAfter {
        items: Vec<EncodedItem>,
        limit: usize,
    }

    impl mixed::Sender for FailAfter {
        type Error = ();
        fn send(&mut self, item: EncodedItem) -> impl Future<Output = Result<(), ()>> + '_ {
            let res = if self.items.len() < self.limit {
                self.items.push(item);
                Ok(())
            } else {
                Err(())
            };
            std::future::ready(res)
        }
    }

    fn traverse<S: mixed::Sender>(
        i: &EncodeInput,
        data: impl ReadBytesAt,
        send: &mut S,
    ) -> Result<(), S::Error> {
        block_on(async {
            with_mode!(
                i.mode,
                mixed::traverse_ranges_validated,
                mixed::keyed_traverse_ranges_validated,
                (data, i.outboard, i.ranges, send).await
            )
        })
    }

    fn items(i: &EncodeInput, data: impl ReadBytesAt) -> Vec<EncodedItem> {
        let mut send = Collect(Vec::new());
        if let Err(e) = traverse(i, data, &mut send) {
            match e {}
        }
        send.0
    }

    pub(super) fn encode_mixed(i: &EncodeInput) -> Encoded {
        flatten(items(i, i.data))
    }

    /// The mixed traversal with `Bytes` as the data, which is read without copying.
    pub(super) fn encode_mixed_bytes(i: &EncodeInput) -> Encoded {
        flatten(items(i, Bytes::copy_from_slice(i.data)))
    }

    /// The bytes of an encoding, or the error at its end.
    pub(super) fn flatten(items: Vec<EncodedItem>) -> Encoded {
        let mut res = Vec::new();
        for item in items {
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
        for item in items(i, i.data) {
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

    /// Property 9: when sending fails, the mixed traversal returns the send
    /// error and does not send anything after it.
    pub(super) fn mixed_send_fails(case: &Case, pos: u64) -> TestResult {
        let blob = case.blob();
        let input = case.encode_input(&blob);
        let all = items(&input, input.data);
        let limit = select(pos, all.len());
        let mut send = FailAfter {
            items: Vec::new(),
            limit,
        };
        let res = traverse(&input, input.data, &mut send);
        assert_eq!(res, Err(()), "{case:?} limit {limit}");
        assert_eq!(
            format!("{:?}", send.items),
            format!("{:?}", &all[..limit]),
            "{case:?} limit {limit}"
        );
        Ok(())
    }

    /// Property 10: the items of the mixed traversal survive a serialization
    /// round trip.
    pub(super) fn encoded_items_round_trip(case: &Case) -> TestResult {
        let blob = case.blob();
        let input = case.encode_input(&blob);
        for item in items(&input, input.data) {
            let bytes = postcard::to_allocvec(&item)?;
            let back: EncodedItem = postcard::from_bytes(&bytes)?;
            assert_eq!(postcard::to_allocvec(&back)?, bytes, "{case:?}");
            assert_eq!(format!("{back:?}"), format!("{item:?}"), "{case:?}");
        }
        Ok(())
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
    #[cfg(feature = "experimental-mixed")]
    ("mixed bytes", mixed_adapter::encode_mixed_bytes),
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

impl DecodeInput<'_> {
    fn with_mode(self, mode: HashMode) -> Self {
        Self { mode, ..self }
    }
}

type Decoder = fn(&DecodeInput) -> Vec<Item>;

fn decode_sync(i: &DecodeInput) -> Vec<Item> {
    let iter = with_mode!(
        i.mode,
        sync::DecodeResponseIter::new,
        sync::DecodeResponseIter::new_keyed,
        (i.root, i.tree, Cursor::new(i.encoded), i.ranges)
    );
    let mut res = Vec::new();
    for item in iter {
        push(&mut res, decoded_item(item));
    }
    res
}

fn decode_fsm(i: &DecodeInput) -> Vec<Item> {
    let mut decoder = with_mode!(
        i.mode,
        fsm::ResponseDecoder::new,
        fsm::ResponseDecoder::new_keyed,
        (i.root, i.ranges.clone(), i.tree, Cursor::new(i.encoded))
    );
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

/// The encoding of the reference implementation, and the items the decoders
/// yield for it.
fn valid_items(case: &Case, blob: &Blob) -> TestResult<(Vec<u8>, Vec<Item>)> {
    let encoded = encode_reference(&case.encode_input(blob))?;
    let items = decode_sync(&case.decode_input(blob, &encoded));
    Ok((encoded, items))
}

/// Property 1: all encoders produce the same bytes as the reference.
fn encoders_agree(case: &Case) -> TestResult {
    let blob = case.blob();
    let input = case.encode_input(&blob);
    let expected = encode_reference(&input);
    for (name, encode) in ENCODERS {
        assert_eq!(encode(&input), expected, "{name} differs for {case:?}");
    }
    Ok(())
}

/// Property 2: all decoders, and the mixed traversal, yield the same items
/// for a valid encoding, and the leaves contain the original data.
fn decoders_agree(case: &Case) -> TestResult {
    let blob = case.blob();
    let (encoded, expected) = valid_items(case, &blob)?;
    for item in &expected {
        match item {
            Item::Leaf(offset, leaf) => {
                let start = *offset as usize;
                assert_eq!(leaf[..], blob.data[start..start + leaf.len()], "{case:?}");
            }
            Item::Parent(..) => {}
            Item::Err(e) => panic!("valid encoding failed to decode: {e} for {case:?}"),
        }
    }
    let input = case.decode_input(&blob, &encoded);
    for (name, decode) in DECODERS {
        assert_eq!(decode(&input), expected, "{name} differs for {case:?}");
    }
    #[cfg(feature = "experimental-mixed")]
    assert_eq!(
        mixed_adapter::mixed_items(&case.encode_input(&blob)),
        expected,
        "mixed differs for {case:?}"
    );
    Ok(())
}

/// Check that decode_ranges wrote the leaves and, if there is an outboard,
/// the stored parents.
fn check_decoded(
    case: &Case,
    name: &str,
    items: &[Item],
    target: &[u8],
    outboard: Option<&dyn sync::Outboard>,
) -> TestResult {
    let tree = case.tree();
    for item in items {
        match (item, outboard) {
            (Item::Leaf(offset, leaf), _) => {
                let start = *offset as usize;
                let written = &target[start..start + leaf.len()];
                assert_eq!(written, &leaf[..], "{name} {case:?}");
            }
            (Item::Parent(node, pair), Some(outboard))
                if tree.pre_order_offset(*node).is_some() =>
            {
                assert_eq!(outboard.load(*node)?, Some(*pair), "{name} {case:?}");
            }
            _ => {}
        }
    }
    Ok(())
}

fn decode_ranges_sync<O>(case: &Case, encoded: &[u8], mut outboard: O) -> TestResult<(Vec<u8>, O)>
where
    O: sync::Outboard + sync::OutboardMut,
{
    let mut target = Vec::new();
    with_mode!(
        case.mode,
        sync::decode_ranges,
        sync::keyed_decode_ranges,
        (encoded, &case.ranges, &mut target, &mut outboard)
    )?;
    Ok((target, outboard))
}

fn decode_ranges_fsm<O>(case: &Case, encoded: &[u8], mut outboard: O) -> TestResult<(Vec<u8>, O)>
where
    O: fsm::Outboard + fsm::OutboardMut,
{
    let mut target = BytesMut::new();
    block_on(async {
        with_mode!(
            case.mode,
            fsm::decode_ranges,
            fsm::keyed_decode_ranges,
            (encoded, case.ranges.clone(), &mut target, &mut outboard).await
        )
    })?;
    Ok((target.to_vec(), outboard))
}

/// Property 3: decode_ranges writes the data and the stored parents into
/// every kind of outboard.
fn decode_ranges_round_trip(case: &Case) -> TestResult {
    let blob = case.blob();
    let (encoded, items) = valid_items(case, &blob)?;
    let (root, tree) = (blob.outboard.root, blob.outboard.tree);
    let size = tree.outboard_size() as usize;
    let pre_mem = || PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    let post_mem = || PostOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; size],
    };
    let check = |name, target: Vec<u8>, outboard: Option<&dyn sync::Outboard>| {
        check_decoded(case, name, &items, &target, outboard)
    };

    let (target, ob) = decode_ranges_sync(case, &encoded, pre_mem())?;
    check("sync pre mem", target, Some(&ob))?;
    let (target, ob) = decode_ranges_sync(case, &encoded, post_mem())?;
    check("sync post mem", target, Some(&ob))?;
    let pre_file = PreOrderOutboard {
        root,
        tree,
        data: Vec::new(),
    };
    let (target, ob) = decode_ranges_sync(case, &encoded, pre_file)?;
    check("sync pre file", target, Some(&ob))?;
    let post_file = PostOrderOutboard {
        root,
        tree,
        data: Vec::new(),
    };
    let (target, ob) = decode_ranges_sync(case, &encoded, post_file)?;
    check("sync post file", target, Some(&ob))?;
    let (target, _) = decode_ranges_sync(case, &encoded, EmptyOutboard { root, tree })?;
    check("sync empty", target, None)?;

    let (target, ob) = decode_ranges_fsm(case, &encoded, pre_mem())?;
    check("fsm pre mem", target, Some(&ob))?;
    let (target, ob) = decode_ranges_fsm(case, &encoded, post_mem())?;
    check("fsm post mem", target, Some(&ob))?;
    let pre_file = PreOrderOutboard {
        root,
        tree,
        data: BytesMut::new(),
    };
    let (target, ob) = decode_ranges_fsm(case, &encoded, pre_file)?;
    let ob = PreOrderOutboard {
        root,
        tree,
        data: ob.data.to_vec(),
    };
    check("fsm pre file", target, Some(&ob))?;
    let post_file = PostOrderOutboard {
        root,
        tree,
        data: BytesMut::new(),
    };
    let (target, ob) = decode_ranges_fsm(case, &encoded, post_file)?;
    let ob = PostOrderOutboard {
        root,
        tree,
        data: ob.data.to_vec(),
    };
    check("fsm post file", target, Some(&ob))?;
    let (target, _) = decode_ranges_fsm(case, &encoded, EmptyOutboard { root, tree })?;
    check("fsm empty", target, None)
}

/// Property 4: all decoders yield the same items for a corrupted or truncated
/// encoding, ending with exactly one error.
fn decoders_agree_on_corruption(case: &Case, pos: u64) -> TestResult {
    let blob = case.blob();
    let (encoded, _) = valid_items(case, &blob)?;
    if encoded.is_empty() {
        return Ok(());
    }
    let pos = select(pos, encoded.len());
    let mut flipped = encoded.clone();
    flipped[pos] ^= 1;
    for broken in [flipped, encoded[..pos].to_vec()] {
        let input = case.decode_input(&blob, &broken);
        let expected = decode_sync(&input);
        let errors = expected
            .iter()
            .filter(|item| matches!(item, Item::Err(_)))
            .count();
        assert_eq!(errors, 1, "{case:?} pos {pos}");
        assert!(matches!(expected.last(), Some(Item::Err(_))), "{case:?}");
        for (name, decode) in DECODERS {
            let items = decode(&input);
            assert_eq!(items, expected, "{name} differs for {case:?} pos {pos}");
        }
    }
    Ok(())
}

type Pairs = Vec<Option<Pair>>;

/// The root of an outboard and its stored hash pairs, in pre order.
///
type Shape = (blake3::Hash, Pairs);

fn outboard_shape(outboard: impl sync::Outboard) -> std::io::Result<Shape> {
    let tree = outboard.tree();
    let pairs = tree
        .pre_order_nodes_iter()
        .map(|node| outboard.load(node))
        .collect::<std::io::Result<_>>()?;
    Ok((outboard.root(), pairs))
}

fn outboard_shape_fsm(mut outboard: impl fsm::Outboard) -> std::io::Result<Shape> {
    let tree = outboard.tree();
    let mut pairs = Vec::new();
    block_on(async {
        for node in tree.pre_order_nodes_iter() {
            pairs.push(outboard.load(node).await?);
        }
        std::io::Result::Ok(())
    })?;
    Ok((outboard.root(), pairs))
}

/// Create an outboard for `tree` from `data` with each outboard function.
fn created_outboards(
    data: &[u8],
    tree: BaoTree,
    mode: HashMode,
) -> Vec<(&'static str, std::io::Result<Shape>)> {
    let pre = || PreOrderMemOutboard {
        root: blake3::Hash::from([0; 32]),
        tree,
        data: vec![0u8; tree.outboard_size() as usize],
    };
    let mut res = Vec::new();

    let mut ob = pre();
    let root = with_mode!(
        mode,
        sync::outboard,
        sync::keyed_outboard,
        (data, tree, &mut ob)
    );
    let shape = root.and_then(|root| outboard_shape(PreOrderMemOutboard { root, ..ob }));
    res.push(("sync outboard", shape));

    let mut post = Vec::new();
    let root = with_mode!(
        mode,
        sync::outboard_post_order,
        sync::keyed_outboard_post_order,
        (data, tree, &mut post)
    );
    let shape = root.and_then(|root| {
        outboard_shape(PostOrderMemOutboard {
            root,
            tree,
            data: post,
        })
    });
    res.push(("sync outboard_post_order", shape));

    let mut ob = pre();
    let root = block_on(async {
        with_mode!(
            mode,
            fsm::outboard,
            fsm::keyed_outboard,
            (Cursor::new(Bytes::copy_from_slice(data)), tree, &mut ob).await
        )
    });
    let shape = root.and_then(|root| outboard_shape(PreOrderMemOutboard { root, ..ob }));
    res.push(("fsm outboard", shape));

    let mut post = Vec::new();
    let root = block_on(async {
        with_mode!(
            mode,
            fsm::outboard_post_order,
            fsm::keyed_outboard_post_order,
            (Cursor::new(Bytes::copy_from_slice(data)), tree, &mut post).await
        )
    });
    let shape = root.and_then(|root| {
        outboard_shape(PostOrderMemOutboard {
            root,
            tree,
            data: post,
        })
    });
    res.push(("fsm outboard_post_order", shape));
    res
}

/// Property 5: every way to create or read an outboard gives the same
/// outboard, and the root is the BLAKE3 hash of the data. Creating an
/// outboard for a tree that is larger than the data fails, and for a smaller
/// tree only uses the start of the data.
fn outboards_agree(case: &Case) -> TestResult {
    let Blob {
        data,
        outboard: mut post,
    } = case.blob();
    let tree = case.tree();
    let root = match case.mode {
        HashMode::Standard => blake3::hash(&data),
        HashMode::Keyed(key) => blake3::keyed_hash(&key, &data),
    };
    let expected = outboard_shape(&post)?;
    assert_eq!(expected.0, root, "{case:?}");
    let mut pre = post.flip();

    let created = with_mode!(
        case.mode,
        PreOrderMemOutboard::create,
        PreOrderMemOutboard::create_keyed,
        (&data, case.block_size)
    );
    let mut shapes = vec![("pre mem create", outboard_shape(created)?)];
    for (name, shape) in created_outboards(&data, tree, case.mode) {
        shapes.push((name, shape?));
    }
    shapes.push(("sync load through &mut", outboard_shape(&mut post)?));
    shapes.push(("fsm load pre mem", outboard_shape_fsm(&mut pre)?));
    shapes.push(("fsm load post mem", outboard_shape_fsm(&mut post)?));
    let pre_file = PreOrderOutboard {
        root,
        tree,
        data: Bytes::from(pre.data.clone()),
    };
    shapes.push(("fsm load pre file", outboard_shape_fsm(pre_file)?));
    let post_file = PostOrderOutboard {
        root,
        tree,
        data: Bytes::from(post.data.clone()),
    };
    shapes.push(("fsm load post file", outboard_shape_fsm(post_file)?));
    let mut copied = PreOrderMemOutboard {
        root,
        tree,
        data: vec![0u8; tree.outboard_size() as usize],
    };
    block_on(fsm::copy(&mut post, &mut copied))?;
    shapes.push(("fsm copy", outboard_shape(copied)?));

    if let HashMode::Standard = case.mode {
        use fsm::CreateOutboard as _;
        use sync::CreateOutboard;
        let ob = <PreOrderOutboard<Vec<u8>> as CreateOutboard>::create(
            Cursor::new(&data),
            case.block_size,
        )?;
        shapes.push(("sync create pre", outboard_shape(ob)?));
        let ob = <PostOrderOutboard<Vec<u8>> as CreateOutboard>::create(
            Cursor::new(&data),
            case.block_size,
        )?;
        shapes.push(("sync create post", outboard_shape(ob)?));
        let reader = Bytes::copy_from_slice(&data);
        let ob = block_on(PreOrderOutboard::<BytesMut>::create(
            reader,
            case.block_size,
        ))?;
        shapes.push(("fsm create pre", outboard_shape_fsm(ob)?));
        let reader = Bytes::copy_from_slice(&data);
        let ob = block_on(PostOrderOutboard::<BytesMut>::create(
            reader,
            case.block_size,
        ))?;
        shapes.push(("fsm create post", outboard_shape_fsm(ob)?));
    }

    for (name, shape) in shapes {
        assert_eq!(shape, expected, "{name} differs for {case:?}");
    }

    let larger = BaoTree::new(case.size as u64 + 1, case.block_size);
    for (name, res) in created_outboards(&data, larger, case.mode) {
        assert!(res.is_err(), "{name} accepted a larger tree for {case:?}");
    }
    if case.size > 0 {
        let small = case.size / 2;
        let smaller = BaoTree::new(small as u64, case.block_size);
        let expected = reference_outboard(&data[..small], case.block_size, case.mode);
        let expected = outboard_shape(expected)?;
        for (name, res) in created_outboards(&data, smaller, case.mode) {
            assert_eq!(res?, expected, "{name} for a smaller tree, {case:?}");
        }
    }
    Ok(())
}

/// Property 8: hash pairs past the end of a truncated outboard file read as
/// zero hashes, in pre and post order, with sync and fsm.
fn truncated_outboards_read_zeros(case: &Case, pos: u64) -> TestResult {
    let post = case.blob().outboard;
    let pre = post.flip();
    let (root, tree) = (post.root, post.tree);
    let full = outboard_shape(&post)?.1;
    if pre.data.is_empty() {
        return Ok(());
    }
    let len = select(pos, pre.data.len());
    let zero = (blake3::Hash::from([0; 32]), blake3::Hash::from([0; 32]));
    let expected = |offset: &dyn Fn(TreeNode) -> Option<u64>| -> Pairs {
        tree.pre_order_nodes_iter()
            .zip(&full)
            .map(|(node, pair)| {
                let pair = (*pair)?;
                let end = offset(node)? as usize * 64 + 64;
                Some(if end <= len { pair } else { zero })
            })
            .collect()
    };
    let expected_pre = expected(&|node| tree.pre_order_offset(node));
    let expected_post = expected(&|node| tree.post_order_offset(node).map(|o| o.value()));
    let pre_data = &pre.data[..len];
    let post_data = &post.data[..len];

    let ob = PreOrderOutboard {
        root,
        tree,
        data: pre_data.to_vec(),
    };
    assert_eq!(
        outboard_shape(ob)?.1,
        expected_pre,
        "sync pre {case:?} len {len}"
    );
    let ob = PostOrderOutboard {
        root,
        tree,
        data: post_data.to_vec(),
    };
    assert_eq!(
        outboard_shape(ob)?.1,
        expected_post,
        "sync post {case:?} len {len}"
    );
    let ob = PreOrderOutboard {
        root,
        tree,
        data: Bytes::copy_from_slice(pre_data),
    };
    assert_eq!(
        outboard_shape_fsm(ob)?.1,
        expected_pre,
        "fsm pre {case:?} len {len}"
    );
    let ob = PostOrderOutboard {
        root,
        tree,
        data: Bytes::copy_from_slice(post_data),
    };
    assert_eq!(
        outboard_shape_fsm(ob)?.1,
        expected_post,
        "fsm post {case:?} len {len}"
    );
    Ok(())
}

#[cfg(feature = "validate")]
mod validators {
    use std::ops::Range;

    use futures_lite::StreamExt;

    use super::*;

    fn union(ranges: Vec<std::io::Result<Range<ChunkNum>>>) -> TestResult<ChunkRanges> {
        let mut res = ChunkRanges::empty();
        for range in ranges {
            let range = range?;
            if range.start < range.end {
                res |= ChunkRanges::from(range);
            }
        }
        Ok(res)
    }

    fn valid_ranges_sync(
        case: &Case,
        outboard: impl sync::Outboard,
        data: &[u8],
    ) -> TestResult<ChunkRanges> {
        union(with_mode!(
            case.mode,
            sync::valid_ranges,
            sync::keyed_valid_ranges,
            (outboard, data, &case.ranges).into_iter().collect()
        ))
    }

    fn valid_ranges_fsm(
        case: &Case,
        outboard: impl fsm::Outboard,
        data: &[u8],
    ) -> TestResult<ChunkRanges> {
        let data = Bytes::copy_from_slice(data);
        union(block_on(async {
            with_mode!(
                case.mode,
                fsm::valid_ranges,
                fsm::keyed_valid_ranges,
                (outboard, data, &case.ranges).collect().await
            )
        }))
    }

    fn valid_outboard_ranges_sync(
        case: &Case,
        outboard: impl sync::Outboard,
    ) -> TestResult<ChunkRanges> {
        union(with_mode!(
            case.mode,
            sync::valid_outboard_ranges,
            sync::keyed_valid_outboard_ranges,
            (outboard, &case.ranges).into_iter().collect()
        ))
    }

    fn valid_outboard_ranges_fsm(
        case: &Case,
        outboard: impl fsm::Outboard,
    ) -> TestResult<ChunkRanges> {
        union(block_on(async {
            with_mode!(
                case.mode,
                fsm::valid_outboard_ranges,
                fsm::keyed_valid_outboard_ranges,
                (outboard, &case.ranges).collect().await
            )
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
    ) -> TestResult<(ChunkRanges, ChunkRanges)> {
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
        let data_ranges = valid_ranges_sync(case, sync_ob(), data)?;
        let fsm_ranges = valid_ranges_fsm(case, fsm_ob(), data)?;
        assert_eq!(fsm_ranges, data_ranges, "fsm valid_ranges {case:?}");
        let outboard_ranges = valid_outboard_ranges_sync(case, sync_ob())?;
        let fsm_ranges = valid_outboard_ranges_fsm(case, fsm_ob())?;
        assert_eq!(
            fsm_ranges, outboard_ranges,
            "fsm valid_outboard_ranges {case:?}"
        );
        Ok((data_ranges, outboard_ranges))
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
    pub(super) fn validators_agree(case: &Case, pos: u64) -> TestResult {
        let Blob { data, outboard } = case.blob();
        let pre = outboard.flip();
        let root = pre.root;
        let expected = case.expected_valid();

        let (data_ranges, outboard_ranges) = validate(case, root, &pre.data, &data)?;
        assert_eq!(data_ranges, expected, "intact {case:?}");
        assert_eq!(outboard_ranges, expected, "intact outboard {case:?}");

        if !data.is_empty() {
            let pos = select(pos, data.len());
            let mut corrupted = data.clone();
            corrupted[pos] ^= 1;
            let invalid = chunk_group(case, pos as u64 / 1024);
            let (data_ranges, _) = validate(case, root, &pre.data, &corrupted)?;
            let what = format!("corrupted data at {pos} {case:?}");
            assert_eq!(data_ranges, &expected - &invalid, "{what}");
            let limit = ChunkNum::chunk_group_start(ChunkNum(pos as u64 / 1024), case.block_size);
            let (data_ranges, _) = validate(case, root, &pre.data, &data[..pos])?;
            let what = format!("truncated data at {pos} {case:?}");
            assert_eq!(
                data_ranges,
                &expected & &ChunkRanges::from(..limit),
                "{what}"
            );
        }

        if !pre.data.is_empty() {
            let pos = select(pos, pre.data.len());
            let mut corrupted = pre.data.clone();
            corrupted[pos] ^= 1;
            let invalid = subtrees(case, pos..pos + 1);
            let (data_ranges, outboard_ranges) = validate(case, root, &corrupted, &data)?;
            let what = format!("corrupted outboard at {pos} {case:?}");
            assert_eq!(data_ranges, &expected - &invalid, "{what}");
            assert_eq!(outboard_ranges, &expected - &invalid, "{what}");
            let invalid = subtrees(case, pos..pre.data.len());
            let (data_ranges, outboard_ranges) = validate(case, root, &pre.data[..pos], &data)?;
            let what = format!("truncated outboard at {pos} {case:?}");
            assert_eq!(data_ranges, &expected - &invalid, "{what}");
            assert_eq!(outboard_ranges, &expected - &invalid, "{what}");
        }
        Ok(())
    }

    /// Validating with the wrong hash mode finds nothing valid. The outboard of
    /// a tree with a single chunk group has no hash pairs, so validating just the
    /// outboard can not detect the wrong mode there.
    pub(super) fn validate_wrong_mode(case: &Case, wrong: HashMode) -> TestResult {
        let Blob { data, outboard } = case.blob();
        let pre = outboard.flip();
        let wrong = case.with_mode(wrong);
        let ob = || PreOrderOutboard {
            root: pre.root,
            tree: pre.tree,
            data: pre.data.clone(),
        };
        let valid = valid_ranges_sync(&wrong, ob(), &data)?;
        assert_eq!(valid, ChunkRanges::empty(), "{case:?}");
        if case.tree().blocks() > 1 {
            let valid = valid_outboard_ranges_sync(&wrong, ob())?;
            assert_eq!(valid, ChunkRanges::empty(), "{case:?}");
        }
        Ok(())
    }
}

/// Property 7: using the wrong hash mode fails in every validating encoder,
/// every decoder and every validator.
fn wrong_mode_fails(case: &Case) -> TestResult {
    let blob = case.blob();
    let (encoded, _) = valid_items(case, &blob)?;
    let empty = truncate_ranges(&case.ranges, case.size as u64).is_empty();
    for wrong in other_modes(case.mode) {
        let input = case.encode_input(&blob).with_mode(wrong);
        for (name, encode) in VALIDATING_ENCODERS {
            let res = encode(&input);
            if empty {
                assert_eq!(res, Ok(Vec::new()), "{name} {case:?}");
            } else {
                assert!(res.is_err(), "{name} accepted {wrong:?} for {case:?}");
            }
        }
        let input = case.decode_input(&blob, &encoded).with_mode(wrong);
        for (name, decode) in DECODERS {
            let items = decode(&input);
            if empty {
                assert_eq!(items, Vec::new(), "{name} {case:?}");
            } else {
                let what = format!("{name} accepted {wrong:?} for {case:?}: {items:?}");
                assert!(matches!(&items[..], [Item::Err(_)]), "{what}");
            }
        }
        #[cfg(feature = "validate")]
        validators::validate_wrong_mode(case, wrong)?;
    }
    Ok(())
}

/// Property 11: the items the decoders yield survive a serialization round
/// trip.
#[cfg(feature = "serde")]
fn decoded_items_round_trip(case: &Case) -> TestResult {
    let blob = case.blob();
    let (encoded, _) = valid_items(case, &blob)?;
    let i = case.decode_input(&blob, &encoded);
    let iter = with_mode!(
        i.mode,
        sync::DecodeResponseIter::new,
        sync::DecodeResponseIter::new_keyed,
        (i.root, i.tree, Cursor::new(i.encoded), i.ranges)
    );
    for item in iter {
        let item = item?;
        let bytes = postcard::to_allocvec(&item)?;
        let back: BaoContentItem = postcard::from_bytes(&bytes)?;
        assert_eq!(decoded_item(Ok(back)), decoded_item(Ok(item)), "{case:?}");
    }
    Ok(())
}

#[test]
fn encoders_agree_cases() -> TestResult {
    for case in fixed_cases() {
        encoders_agree(&case)?;
    }
    Ok(())
}

#[proptest]
fn encoders_agree_proptest(#[strategy(case())] case: Case) {
    check(encoders_agree(&case));
}

#[test]
fn decoders_agree_cases() -> TestResult {
    for case in fixed_cases() {
        decoders_agree(&case)?;
    }
    Ok(())
}

#[proptest]
fn decoders_agree_proptest(#[strategy(case())] case: Case) {
    check(decoders_agree(&case));
}

#[test]
fn decode_ranges_round_trip_cases() -> TestResult {
    for case in fixed_cases() {
        decode_ranges_round_trip(&case)?;
    }
    Ok(())
}

#[proptest]
fn decode_ranges_round_trip_proptest(#[strategy(case())] case: Case) {
    check(decode_ranges_round_trip(&case));
}

#[test]
fn decoders_agree_on_corruption_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(8) {
            decoders_agree_on_corruption(&case, pos)?;
        }
    }
    Ok(())
}

#[proptest]
fn decoders_agree_on_corruption_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(decoders_agree_on_corruption(&case, pos));
}

#[test]
fn outboards_agree_cases() -> TestResult {
    for case in fixed_cases() {
        outboards_agree(&case)?;
    }
    Ok(())
}

#[proptest]
fn outboards_agree_proptest(#[strategy(case())] case: Case) {
    check(outboards_agree(&case));
}

#[cfg(feature = "validate")]
#[test]
fn validators_agree_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(4) {
            validators::validators_agree(&case, pos)?;
        }
    }
    Ok(())
}

#[cfg(feature = "validate")]
#[proptest]
fn validators_agree_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(validators::validators_agree(&case, pos));
}

#[test]
fn wrong_mode_fails_cases() -> TestResult {
    for case in fixed_cases() {
        wrong_mode_fails(&case)?;
    }
    Ok(())
}

#[proptest]
fn wrong_mode_fails_proptest(#[strategy(case())] case: Case) {
    check(wrong_mode_fails(&case));
}

#[test]
fn truncated_outboards_read_zeros_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(8) {
            truncated_outboards_read_zeros(&case, pos)?;
        }
    }
    Ok(())
}

#[proptest]
fn truncated_outboards_read_zeros_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(truncated_outboards_read_zeros(&case, pos));
}

#[cfg(feature = "experimental-mixed")]
#[test]
fn mixed_send_fails_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(4) {
            mixed_adapter::mixed_send_fails(&case, pos)?;
        }
    }
    Ok(())
}

#[cfg(feature = "experimental-mixed")]
#[proptest]
fn mixed_send_fails_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(mixed_adapter::mixed_send_fails(&case, pos));
}

#[cfg(feature = "experimental-mixed")]
#[test]
fn encoded_items_round_trip_cases() -> TestResult {
    for case in fixed_cases() {
        mixed_adapter::encoded_items_round_trip(&case)?;
    }
    Ok(())
}

#[cfg(feature = "experimental-mixed")]
#[proptest]
fn encoded_items_round_trip_proptest(#[strategy(case())] case: Case) {
    check(mixed_adapter::encoded_items_round_trip(&case));
}

#[cfg(feature = "serde")]
#[test]
fn decoded_items_round_trip_cases() -> TestResult {
    for case in fixed_cases() {
        decoded_items_round_trip(&case)?;
    }
    Ok(())
}

#[cfg(feature = "serde")]
#[proptest]
fn decoded_items_round_trip_proptest(#[strategy(case())] case: Case) {
    check(decoded_items_round_trip(&case));
}

/// Deserializing a parent from malformed data fails instead of panicking.
#[cfg(feature = "serde")]
#[test]
fn parent_malformed() -> TestResult {
    let parent = Parent {
        node: TreeNode(3),
        pair: (blake3::hash(b"l"), blake3::hash(b"r")),
    };
    let bytes = postcard::to_allocvec(&parent)?;
    for len in 0..bytes.len() {
        let res = postcard::from_bytes::<Parent>(&bytes[..len]);
        assert!(res.is_err(), "prefix of {len} bytes");
    }
    for elements in 0..3 {
        let mut bytes = bytes.clone();
        bytes[0] = elements;
        let res = postcard::from_bytes::<Parent>(&bytes);
        assert!(res.is_err(), "sequence of {elements} elements");
    }
    Ok(())
}

#[cfg(feature = "serde")]
#[test]
fn encode_error_round_trip() -> TestResult {
    let errors = [
        EncodeError::ParentHashMismatch(TreeNode(5)),
        EncodeError::LeafHashMismatch(ChunkNum(3)),
        EncodeError::ParentWrite(TreeNode(7)),
        EncodeError::LeafWrite(ChunkNum(9)),
        EncodeError::SizeMismatch,
    ];
    for error in errors {
        let bytes = postcard::to_allocvec(&error)?;
        let back: EncodeError = postcard::from_bytes(&bytes)?;
        assert_eq!(format!("{back:?}"), format!("{error:?}"));
    }
    let error = std::io::Error::new(std::io::ErrorKind::InvalidData, "broken");
    let bytes = postcard::to_allocvec(&EncodeError::Io(error))?;
    let back: EncodeError = postcard::from_bytes(&bytes)?;
    let EncodeError::Io(back) = back else {
        panic!("expected an io error, got {back:?}");
    };
    assert!(back.to_string().contains("broken"), "{back}");
    Ok(())
}

/// An outboard that violates the contract of [crate::io::sync::Outboard::load]
/// by returning `Ok(None)` for nodes that should be stored.
struct NoneOutboard(PreOrderMemOutboard);

impl sync::Outboard for NoneOutboard {
    fn root(&self) -> blake3::Hash {
        self.0.root
    }
    fn tree(&self) -> BaoTree {
        self.0.tree
    }
    fn load(&self, node: TreeNode) -> std::io::Result<Option<Pair>> {
        if node.level() == 0 {
            Ok(None)
        } else {
            sync::Outboard::load(&self.0, node)
        }
    }
}

impl fsm::Outboard for NoneOutboard {
    fn root(&self) -> blake3::Hash {
        self.0.root
    }
    fn tree(&self) -> BaoTree {
        self.0.tree
    }
    async fn load(&mut self, node: TreeNode) -> std::io::Result<Option<Pair>> {
        sync::Outboard::load(self, node)
    }
}

fn is_missing_hash_pair(res: Result<(), EncodeError>) -> bool {
    matches!(res, Err(EncodeError::Io(e)) if e.kind() == std::io::ErrorKind::InvalidData)
}

#[test]
fn encode_outboard_none_cases() {
    let data = make_test_data(1024 * 16);
    let ranges = ChunkRanges::all();
    let outboard = || NoneOutboard(PreOrderMemOutboard::create(&data, BlockSize(0)));
    let bytes = || Bytes::from(data.clone());
    let mut encoded = Vec::new();
    let res = sync::encode_ranges(&data[..], outboard(), &ranges, &mut encoded);
    assert!(is_missing_hash_pair(res));
    let res = sync::encode_ranges_validated(&data[..], outboard(), &ranges, &mut encoded);
    assert!(is_missing_hash_pair(res));
    let res = block_on(fsm::encode_ranges(
        bytes(),
        outboard(),
        &ranges,
        &mut encoded,
    ));
    assert!(is_missing_hash_pair(res));
    let res = block_on(fsm::encode_ranges_validated(
        bytes(),
        outboard(),
        &ranges,
        &mut encoded,
    ));
    assert!(is_missing_hash_pair(res));
}

#[test]
fn outboard_wrong_hash() {
    let data = make_test_data(100000000);
    let expected = blake3::hash(&data);
    let actual = PostOrderMemOutboard::create(&data, BlockSize(4)).root;
    assert_eq!(expected, actual);
}

#[test]
fn outboard_hash() {
    for i in 1..4 {
        let data = &[0u8];
        let outboard = PostOrderMemOutboard::create(data, BlockSize(i));
        assert_eq!(outboard.root, blake3::hash(data));
    }
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
            HashMode::Standard,
        );
        actual_encoded.len() - data.len()
    };
    assert_eq!(overhead(&data, 0), 64 * 2);
    assert_eq!(overhead(&data, 1), 64);
    assert_eq!(overhead(&data, 2), 0);
}

/// Encode a small subset of a large blob, and check that the encoded data is small
#[test]
fn encode_single_chunk_large() -> TestResult {
    let data = make_test_data(1024 * 1024 * 16 + 12345);
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(4));
    for start in [0, 1000, 3000] {
        let ranges = ChunkRanges::from(ChunkNum(start)..ChunkNum(start + 1));
        let mut encoded = Vec::new();
        sync::encode_ranges_validated(&data, &outboard, &ranges, &mut encoded)?;
        assert_eq!(encoded.len(), 15 * 64 + 1024);
    }
    Ok(())
}

/// A query outside the valid range encodes the last chunk.
fn encode_last_chunk(size: u64, block_size: u8) -> TestResult {
    let data = make_test_data(size as usize);
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(block_size));
    let encode = |ranges: ChunkRanges| -> TestResult<Vec<u8>> {
        let mut encoded = Vec::new();
        sync::encode_ranges_validated(&data, &outboard, &ranges, &mut encoded)?;
        Ok(encoded)
    };
    let last = last_chunk(size);
    let last = ChunkNum::chunks(last.start)..ChunkNum::chunks(last.end);
    let past_end = encode(ChunkRanges::from(ChunkNum(u64::MAX)..))?;
    assert_eq!(past_end, encode(ChunkRanges::from(last))?);
    Ok(())
}

#[test]
fn encode_last_chunk_cases() -> TestResult {
    encode_last_chunk(4096, 0)
}

#[proptest]
fn encode_last_chunk_proptest(
    #[strategy(1..100000u64)] size: u64,
    #[strategy(0..4u8)] block_size: u8,
) {
    check(encode_last_chunk(size, block_size));
}

/// Validate a large blob with the fsm validator.
#[cfg(feature = "validate")]
#[test]
fn validate_bug() -> TestResult {
    use futures_lite::StreamExt;
    let data = Bytes::from(make_test_data(19308432));
    let outboard = PostOrderMemOutboard::create(&data, BlockSize(4));
    let expected = ChunkRanges::from(..ChunkNum::chunks(data.len() as u64));
    let ranges = ChunkRanges::all();
    let items = block_on(fsm::valid_ranges(outboard, data, &ranges).collect::<Vec<_>>());
    let mut valid = ChunkRanges::empty();
    for range in items {
        valid |= ChunkRanges::from(range?);
    }
    assert_eq!(valid, expected);
    Ok(())
}
