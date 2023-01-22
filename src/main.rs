use tree::BLAKE3_CHUNK_SIZE;
mod sync_store;
mod tree;

#[cfg(test)]
mod tests;

fn hash_leaf(offset: u64, data: &[u8], is_root: bool) -> blake3::Hash {
    let mut hasher = blake3::guts::ChunkState::new(offset);
    hasher.update(data);
    hasher.finalize(is_root)
}

fn leaf_hashes(data: &[u8]) -> Vec<blake3::Hash> {
    let is_root = data.len() as u64 <= BLAKE3_CHUNK_SIZE;
    if !data.is_empty() {
        data.chunks(BLAKE3_CHUNK_SIZE as usize)
            .enumerate()
            .map(|(i, data)| hash_leaf(i as u64, data, is_root))
            .collect::<Vec<_>>()
    } else {
        vec![hash_leaf(0, &[], is_root)]
    }
}

fn condense(hashes: &[blake3::Hash]) -> Vec<blake3::Hash> {
    let is_root = hashes.len() == 2;
    let mut condensed = Vec::new();
    for chunk in hashes.chunks(2) {
        let res = if chunk.len() == 1 {
            chunk[0]
        } else {
            let left_child = &chunk[0];
            let right_child = &chunk[1];
            blake3::guts::parent_cv(left_child, right_child, is_root)
        };
        condensed.push(res);
    }
    condensed
}

fn blake3_own(data: &[u8]) -> blake3::Hash {
    let mut hashes = leaf_hashes(data);
    while hashes.len() > 1 {
        hashes = condense(&hashes);
    }
    hashes[0]
}

use tokio::io::AsyncRead;

use crate::sync_store::{BlakeFile, VecSyncStore};
use crate::tree::BlockLevel;

fn print_outboard(data: &[u8]) {
    println!("len:   {}", data.len());
    let (outboard, hash) = bao::encode::outboard(data);
    println!("outboard: {}", (outboard.len() - 8) / 64);
    println!("outboard: {}", hex::encode(outboard.as_slice()));
    println!("ob_hash:  {}", hex::encode(hash.as_bytes()));
    println!("blake3_h: {}", hex::encode(blake3::hash(&data).as_bytes()));
    println!(
        "sparse_o: {}",
        hex::encode(
            BlakeFile::<VecSyncStore>::new(&data, BlockLevel(0))
                .unwrap()
                .hash()
                .unwrap()
                .unwrap()
                .as_bytes()
        )
    );
    if data.len() <= 1024 {
        println!(
            "manual: {}",
            hex::encode(hash_leaf(0, &data, true).as_bytes())
        );
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() <= 2048 {
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..], false);
        println!(
            "manual: {}",
            hex::encode(blake3::guts::parent_cv(&l0, &l1, true).as_bytes())
        );
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() <= 2048 + 1024 {
        println!("lopsided");
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..2048], false);
        let i0 = blake3::guts::parent_cv(&l0, &l1, false);
        let l2 = hash_leaf(2, &data[2048..], false);
        let root = blake3::guts::parent_cv(&i0, &l2, true);
        println!("manual: {}", hex::encode(root.as_bytes()));
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    } else if data.len() < 2048 + 2048 {
        println!("even");
        let l0 = hash_leaf(0, &data[..1024], false);
        let l1 = hash_leaf(1, &data[1024..2048], false);
        let i0 = blake3::guts::parent_cv(&l0, &l1, false);
        let l2 = hash_leaf(2, &data[2048..3072], false);
        let l3 = hash_leaf(3, &data[3072..], false);
        let i1 = blake3::guts::parent_cv(&l2, &l3, false);
        let root = blake3::guts::parent_cv(&i0, &i1, true);
        println!("manual: {}", hex::encode(root.as_bytes()));
        println!("man2:   {}", hex::encode(blake3_own(data).as_bytes()));
    }
}

fn main() {
    for i in (0..(4096)).step_by(64) {
        let data = (0..i).map(|_| (i / 701) as u8).collect::<Vec<_>>();
        print_outboard(&data);
    }
}
