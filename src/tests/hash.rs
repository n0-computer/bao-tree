//! Tests for the keyed hash primitives against BLAKE3.

use super::reference::make_test_data;
use crate::{
    blake3,
    io::{outboard::PostOrderMemOutboard, sync::Outboard},
    keyed_hash_subtree, keyed_parent_cv, BlockSize, HashMode,
};

#[test]
fn keyed_domain_separation() {
    let data = make_test_data(50_000);
    let key1 = blake3::derive_key("bao-tree.test", b"format-1");
    let key2 = blake3::derive_key("bao-tree.test", b"format-2");
    let root1 = PostOrderMemOutboard::create_keyed(&data, BlockSize(2), &key1).root();
    let root2 = PostOrderMemOutboard::create_keyed(&data, BlockSize(2), &key2).root();
    assert_ne!(root1, root2);
    assert_ne!(root1, blake3::hash(&data));
}

#[test]
fn keyed_hash_subtree_differs_from_standard() {
    use blake3::hazmat::HasherExt;

    let data = make_test_data(2048);
    let key = blake3::derive_key("bao-tree.test", b"low-level-subtree");
    let standard = HashMode::Standard.hash_subtree(0, &data, true);
    let keyed = keyed_hash_subtree(0, &data, true, &key);
    assert_ne!(standard, keyed);
    assert_eq!(keyed, blake3::keyed_hash(&key, &data));
    let non_root_standard = HashMode::Standard.hash_subtree(1, &data[..1024], false);
    let non_root_keyed = keyed_hash_subtree(1, &data[..1024], false, &key);
    assert_ne!(non_root_standard, non_root_keyed);
    let mut hasher = blake3::Hasher::new_keyed(&key);
    hasher.set_input_offset(1024);
    hasher.update(&data[..1024]);
    let expected_non_root = blake3::Hash::from(hasher.finalize_non_root());
    assert_eq!(non_root_keyed, expected_non_root);
}

#[test]
fn keyed_parent_cv_differs_from_standard() {
    use blake3::hazmat::{merge_subtrees_non_root, merge_subtrees_root, ChainingValue, Mode};

    let left = blake3::hash(b"left");
    let right = blake3::hash(b"right");
    let key = blake3::derive_key("bao-tree.test", b"low-level-parent");
    let standard = HashMode::Standard.parent_cv(&left, &right, true);
    let keyed = keyed_parent_cv(&left, &right, true, &key);
    assert_ne!(standard, keyed);
    let standard_non_root = HashMode::Standard.parent_cv(&left, &right, false);
    let keyed_non_root = keyed_parent_cv(&left, &right, false, &key);
    assert_ne!(standard_non_root, keyed_non_root);
    let left_cv: ChainingValue = *left.as_bytes();
    let right_cv: ChainingValue = *right.as_bytes();
    let mode = Mode::KeyedHash(&key);
    assert_eq!(keyed, merge_subtrees_root(&left_cv, &right_cv, mode));
    assert_eq!(
        keyed_non_root,
        blake3::Hash::from(merge_subtrees_non_root(&left_cv, &right_cv, mode))
    );
}
