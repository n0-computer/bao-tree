//! Trivial code that the other tests do not reach: formatting, accessors,
//! operators and default implementations.
//!
//! These tests only call the code, so that the coverage report shows the code
//! that is actually untested. Formatting is checked to produce something, not
//! the exact text.
use bytes::Bytes;

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
    iter::{BaoChunk, ResponseIter, ResponseIterRef},
    BaoTree, BlockSize, ChunkNum, ChunkRanges, HashMode, TreeNode,
};

fn formatted(s: String) {
    assert!(!s.is_empty());
}

#[test]
fn formatting() {
    let decode_errors = [
        DecodeError::ParentNotFound(TreeNode(1)),
        DecodeError::LeafNotFound(ChunkNum(1)),
        DecodeError::ParentHashMismatch(TreeNode(1)),
        DecodeError::LeafHashMismatch(ChunkNum(1)),
        DecodeError::Io(std::io::ErrorKind::Other.into()),
    ];
    for error in decode_errors {
        formatted(format!("{error} {error:?}"));
    }
    formatted(format!(
        "{}",
        DecodeError::from(std::io::Error::other("io"))
    ));
    formatted(format!("{}", EncodeError::SizeMismatch));
    for node in [TreeNode(0), TreeNode(3)] {
        formatted(format!("{node} {node:?} {node:#?}"));
    }
    formatted(format!(
        "{} {:?} {:#?}",
        ChunkNum(5),
        ChunkNum(5),
        ChunkNum(5)
    ));
    formatted(format!("{}", BlockSize::ZERO));
    formatted(format!(
        "{:?} {:?}",
        HashMode::Standard,
        HashMode::Keyed([1; 32])
    ));
    let leaf = Leaf {
        offset: 0,
        data: Bytes::from_static(b"data"),
    };
    formatted(format!("{leaf:?}"));
    let tree = BaoTree::new(4096, BlockSize::ZERO);
    formatted(format!("{:?}", ResponseIter::new(tree, ChunkRanges::all())));
}

#[test]
fn chunk_num_operators() {
    let n = ChunkNum(6);
    assert_eq!(n * 2, ChunkNum(12));
    assert_eq!(n / 2, ChunkNum(3));
    assert_eq!(n - 1, ChunkNum(5));
    assert_eq!(n - ChunkNum(1), ChunkNum(5));
    assert_eq!(n + ChunkNum(1), ChunkNum(7));
    assert!(n == 6 && 6 == n && n < 7);
    assert_eq!(n.to_usize(), 6);
    assert_eq!(BlockSize::from_chunk_log(4).chunk_log(), 4);
}

#[test]
fn accessors() {
    let tree = BaoTree::new(4096, BlockSize(1));
    let ranges = ChunkRanges::all();
    assert_eq!(BaoChunk::<()>::default().size(), 0);
    let parent = BaoChunk::<()>::Parent {
        node: TreeNode(1),
        is_root: true,
        left: true,
        right: true,
        ranges: (),
    };
    assert_eq!(parent.size(), 64);
    let iter = tree.ranges_pre_order_chunks_iter_ref(&ranges, 1);
    assert_eq!(*iter.tree(), tree);
    assert_eq!(iter.min_full_level(), 1);
    assert_eq!(ResponseIterRef::new(tree, &ranges).tree(), tree);
    assert_eq!(ResponseIter::new(tree, ranges.clone()).tree(), tree);

    let item = BaoContentItem::from(Leaf {
        offset: 0,
        data: Bytes::new(),
    });
    assert!(item.is_leaf() && !item.is_parent());
    let pair = (blake3::hash(b"l"), blake3::hash(b"r"));
    let item = BaoContentItem::from(Parent {
        node: TreeNode(1),
        pair,
    });
    assert!(item.is_parent() && !item.is_leaf());

    let root = blake3::hash(b"");
    let iter = sync::DecodeResponseIter::new(root, tree, std::io::empty(), &ranges);
    assert!(iter.buffer().is_empty());
    let _ = iter.tree();
    let decoder =
        fsm::ResponseDecoder::new(root, ranges.clone(), tree, std::io::Cursor::new(&[][..]));
    let _ = decoder.tree();
    let _ = decoder.finish();
}

#[test]
fn outboards() -> testresult::TestResult {
    let _ = PreOrderOutboard::<Vec<u8>>::default();
    let _ = PostOrderOutboard::<Vec<u8>>::default();
    let pre = PreOrderMemOutboard::<Vec<u8>>::default();
    let post = PostOrderMemOutboard::<Vec<u8>>::default();
    let pre = pre.map_data(Bytes::from);
    let post = post.map_data(Bytes::from);
    assert_eq!(pre.data.len(), 0);
    assert_eq!(post.data.len(), 0);
    let data = vec![1u8; 4096];
    let pre = PreOrderMemOutboard::create(&data, BlockSize::ZERO);
    let size = pre.data.len();
    assert_eq!(pre.into_inner_with_prefix().len(), size + 8);

    let tree = BaoTree::new(4096, BlockSize(1));
    let mut empty = EmptyOutboard {
        tree,
        root: blake3::hash(&data),
    };
    for node in [TreeNode(1), TreeNode(0)] {
        let _ = sync::Outboard::load(&empty, node)?;
        let _ = futures_lite::future::block_on(fsm::Outboard::load(&mut empty, node))?;
    }
    sync::OutboardMut::sync(&mut &mut empty)?;
    futures_lite::future::block_on(fsm::OutboardMut::sync(&mut &mut empty))?;
    let mut pre = PreOrderMemOutboard::create(&data, BlockSize::ZERO);
    let mut post = PostOrderMemOutboard::create(&data, BlockSize::ZERO);
    sync::OutboardMut::sync(&mut pre)?;
    sync::OutboardMut::sync(&mut post)?;
    futures_lite::future::block_on(fsm::OutboardMut::sync(&mut pre))?;
    futures_lite::future::block_on(fsm::OutboardMut::sync(&mut post))?;
    Ok(())
}

#[cfg(feature = "experimental-mixed")]
#[test]
fn read_bytes_at() -> testresult::TestResult {
    use crate::io::mixed::ReadBytesAt;
    let data = vec![1u8; 100];
    let mut bytes = Bytes::from(data.clone());
    assert_eq!(
        <Vec<u8> as ReadBytesAt>::read_bytes_at(&data, 10, 5)?.len(),
        5
    );
    assert_eq!(
        <&Bytes as ReadBytesAt>::read_bytes_at(&&bytes, 10, 5)?.len(),
        5
    );
    assert_eq!(
        <&mut Bytes as ReadBytesAt>::read_bytes_at(&&mut bytes, 10, 5)?.len(),
        5
    );
    #[cfg(feature = "fs")]
    {
        use std::io::Write;
        let mut file = tempfile::tempfile()?;
        file.write_all(&data)?;
        assert_eq!(
            <std::fs::File as ReadBytesAt>::read_bytes_at(&file, 10, 5)?.len(),
            5
        );
    }
    Ok(())
}

#[cfg(feature = "experimental-mixed")]
#[test]
fn mixed() {
    use crate::io::mixed::{keyed_traverse_selected_rec, traverse_selected_rec, EncodedItem};
    let item = EncodedItem::from(EncodeError::SizeMismatch);
    assert!(matches!(item, EncodedItem::Error(_)));
    let data = Bytes::from(vec![1u8; 4096]);
    let ranges = ChunkRanges::all();
    let mut items = Vec::new();
    let hash = traverse_selected_rec(
        ChunkNum(0),
        data.clone(),
        true,
        &ranges,
        0,
        true,
        &mut items,
    );
    assert_eq!(hash, blake3::hash(&data));
    let key = [3; 32];
    let hash = keyed_traverse_selected_rec(
        ChunkNum(0),
        data.clone(),
        true,
        &ranges,
        0,
        true,
        &mut items,
        &key,
    );
    assert_eq!(hash, blake3::keyed_hash(&key, &data));
}

/// Deserializing from a value of the wrong type formats an error message
/// with the `expecting` text of the visitor.
#[cfg(feature = "serde")]
#[test]
fn serde_expecting() {
    use serde::{
        de::{value::Error, IntoDeserializer},
        Deserialize,
    };
    let res = Parent::deserialize(IntoDeserializer::<Error>::into_deserializer(5u32));
    formatted(res.unwrap_err().to_string());
    let res =
        crate::io_error_serde::deserialize(IntoDeserializer::<Error>::into_deserializer(5u32));
    formatted(res.unwrap_err().to_string());
}
