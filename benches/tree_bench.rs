use bao_tree::{blake3, BaoTree, BlockSize, ByteNum};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use range_collections::RangeSet2;

fn offset_benches(c: &mut Criterion) {
    let tree = BaoTree::new(ByteNum(1024 * 1024 * 1024), BlockSize::DEFAULT);
    let node = tree.pre_order_nodes_iter().last().unwrap();
    c.bench_function("pre_order_offset", |b| {
        b.iter(|| tree.pre_order_offset(black_box(node)))
    });
    c.bench_function("post_order_offset", |b| {
        b.iter(|| tree.post_order_offset(black_box(node)))
    });
}

fn iter_benches(c: &mut Criterion) {
    let tree = BaoTree::new(ByteNum(1024 * 1024), BlockSize::DEFAULT);
    c.bench_function("pre_order_nodes_iter", |b| {
        b.iter(|| {
            for item in tree.pre_order_nodes_iter() {
                black_box(item);
            }
        })
    });
    c.bench_function("post_order_nodes_iter", |b| {
        b.iter(|| {
            for item in tree.post_order_nodes_iter() {
                black_box(item);
            }
        })
    });
    c.bench_function("post_order_chunks_iter", |b| {
        b.iter(|| {
            for item in tree.post_order_chunks_iter() {
                black_box(item);
            }
        })
    });
    c.bench_function("ranges_pre_order_chunks_iter_ref", |b| {
        b.iter(|| {
            for item in tree.ranges_pre_order_chunks_iter_ref(&RangeSet2::all(), 0) {
                black_box(item);
            }
        })
    });
}

fn hash_benches_large(c: &mut Criterion) {
    let data = (0..1024 * 16).map(|i| i as u8).collect::<Vec<_>>();
    c.bench_function("hash_blake3", |b| {
        b.iter(|| {
            blake3::hash(&data);
        })
    });
    c.bench_function("hash_blake3_hasher", |b| {
        b.iter(|| {
            let mut hasher = blake3::Hasher::new();
            hasher.update(&data);
            hasher.finalize()
        })
    });
    c.bench_function("hash_subtree", |b| {
        b.iter(|| {
            blake3::guts::hash_subtree(0, &data, true);
        })
    });
}

criterion_group!(benches, offset_benches, iter_benches, hash_benches_large,);
criterion_main!(benches);
