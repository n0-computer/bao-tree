use super::*;
use crate::{errors::AddSliceError, sync_store::*, tree::*, vec_store::VecStore};
use bao::encode::SliceExtractor;
use proptest::prelude::*;
use std::io::{Cursor, Read};

fn size_start_len() -> impl Strategy<Value = (ByteNum, ByteNum, ByteNum)> {
    (0u64..32768)
        .prop_flat_map(|size| {
            let start = 0u64..size;
            let len = 0u64..size;
            (Just(size), start, len)
        })
        .prop_map(|(size, start, len)| {
            let size = ByteNum(size);
            let start = ByteNum(start);
            let len = ByteNum(len);
            (size, start, len)
        })
}

fn compare_slice_iter_impl(size: ByteNum, start: ByteNum, len: ByteNum) {
    // generate data, different value for each chunk so the hashes are intersting
    let data = (0..size.0)
        .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
        .collect::<Vec<_>>();

    // encode a slice using bao
    let (outboard, _hash) = bao::encode::outboard(&data);
    let mut extractor =
        SliceExtractor::new_outboard(Cursor::new(&data), Cursor::new(&outboard), start.0, len.0);
    let mut slice1 = Vec::new();
    extractor.read_to_end(&mut slice1).unwrap();

    // encode a slice using vec outboard and its slice_iter
    let vs = BlakeFile::<VecStore>::new(&data, BlockLevel(0)).unwrap();

    // use the iterator and flatten it
    let slices2 = vs.slice_iter(start..start + len).collect::<Vec<_>>();
    let slice2 = slices2
        .into_iter()
        .map(|x| x.unwrap().to_vec())
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(slice1, slice2);

    // use the reader and use read_to_end, should be the same
    let vs = BlakeFile::<VecStore>::new(&data, BlockLevel(0)).unwrap();
    let mut reader = vs.extract_slice(start..start + len);
    let mut slice2 = Vec::new();
    reader.read_to_end(&mut slice2).unwrap();
    assert_eq!(slice1, slice2);
}

fn add_from_slice_impl(size: ByteNum, start: ByteNum, len: ByteNum) {
    // generate data, different value for each chunk so the hashes are intersting
    let data = (0..size.0)
        .map(|i| (i / BLAKE3_CHUNK_SIZE) as u8)
        .collect::<Vec<_>>();

    // encode a slice using bao
    let (outboard, _hash) = bao::encode::outboard(&data);
    let mut extractor =
        SliceExtractor::new_outboard(Cursor::new(&data), Cursor::new(&outboard), start.0, len.0);
    let mut slice1 = Vec::new();
    extractor.read_to_end(&mut slice1).unwrap();

    // add from the bao slice and check that it validates
    let mut vs = BlakeFile::<VecStore>::new(&data, BlockLevel(0)).unwrap();
    let byte_range = start..start + len;
    vs.add_from_slice(byte_range.clone(), &mut Cursor::new(&slice1))
        .unwrap();
    vs.clear().unwrap();
    vs.add_from_slice(byte_range.clone(), &mut Cursor::new(&slice1))
        .unwrap();
    slice1[8] ^= 1;

    // add from the bao slice with a single bit flipped and check that it fails to validate
    let err = vs
        .add_from_slice(byte_range, &mut Cursor::new(&slice1))
        .unwrap_err();
    assert!(matches!(err, AddSliceError::Validation(_)));
}

fn compare_hash_impl(data: &[u8]) {
    let hash = blake3::hash(data);
    for level in 0..10 {
        let vs = BlakeFile::<VecStore>::new(data, BlockLevel(level)).unwrap();
        let hash2 = vs.hash().unwrap().unwrap();
        assert_eq!(hash, hash2);
    }
}

fn compare_outboard_impl(data: &[u8]) {
    let (outboard1, hash1) = bao::encode::outboard(&data);
    // compare internally
    for level in 0..3 {
        let vs = BlakeFile::<VecStore>::new(&data, BlockLevel(level)).unwrap();
        let hash2 = vs.hash().unwrap().unwrap();
        let outboard2 = vs.outboard().unwrap();
        assert_eq!(hash1, hash2);
        if level == 0 {
            assert_eq!(outboard1, outboard2);
        }
    }
}

fn compare_encoded_impl(data: &[u8]) {
    let (encoded1, hash1) = bao::encode::encode(&data);
    // compare internally
    for level in 0..3 {
        let vs = BlakeFile::<VecStore>::new(&data, BlockLevel(level)).unwrap();
        let hash2 = vs.hash().unwrap().unwrap();
        let encoded2 = vs.encode().unwrap();
        assert_eq!(hash1, hash2);
        if level == 0 {
            assert_eq!(encoded1, encoded2);
        }
    }
}

proptest! {

    #[test]
    fn compare_hash(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        compare_hash_impl(&data);
    }

    #[test]
    fn compare_outboard(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        compare_outboard_impl(&data);
    }

    #[test]
    fn compare_encoded(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        compare_encoded_impl(&data);
    }

    #[test]
    fn compare_slice_iter((size, start, len) in size_start_len()) {
        compare_slice_iter_impl(size, start, len)
    }

    #[test]
    fn add_from_slice((size, start, len) in size_start_len()) {
        add_from_slice_impl(size, start, len);
    }

    #[test]
    fn compare_hash_block_recursive(data in proptest::collection::vec(any::<u8>(), 0..32768)) {
        let hash = blake3::hash(&data);
        let hash2 = hash_block(BlockNum(0), &data, BlockLevel(10), true);
        assert_eq!(hash, hash2);
    }
}

#[test]
fn compare_hash_0() {
    compare_hash_impl(&[0u8; 1024]);
}

#[test]
fn compare_slice_iter_0() {
    compare_slice_iter_impl(ByteNum(1025), ByteNum(182), ByteNum(843));
}

#[test]
fn non_zero_block_level() {
    let data = [0u8; 4096];
    let l0 = BlakeFile::<VecStore>::new(&data, BlockLevel(0)).unwrap();
    let l1 = BlakeFile::<VecStore>::new(&data, BlockLevel(1)).unwrap();
    let l2 = BlakeFile::<VecStore>::new(&data, BlockLevel(2)).unwrap();
    println!("{:?}", l0.0.tree_len());
    println!("{:?}", l1.0.tree_len());
    println!("{:?}", l2.0.tree_len());
    println!("iter 0");
    for item in l0.slice_iter(l0.byte_range()) {
        println!("{:?}", item);
    }
    println!("iter 1");
    for item in l1.slice_iter(l1.byte_range()) {
        println!("{:?}", item);
    }
    println!("iter 2");
    for item in l2.slice_iter(l2.byte_range()) {
        println!("{:?}", item);
    }
}
