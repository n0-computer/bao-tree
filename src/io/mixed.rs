//! Read from sync, send to tokio sender
use bytes::Bytes;
use positioned_io::ReadAt;
use smallvec::SmallVec;
use std::result;

use crate::{hash_subtree, iter::BaoChunk, rec::truncate_ranges, ChunkRangesRef};
use crate::{split_inner, ChunkNum, TreeNode};

use super::{sync::Outboard, EncodeError};
use super::{Leaf, Parent};
use iroh_blake3 as blake3;
use iroh_blake3::guts::parent_cv;

/// A content item for the bao streaming protocol.
#[derive(Debug)]
pub enum EncodedItem {
    /// total data size, will be the first item
    Size(u64),
    /// a parent node
    Parent(Parent),
    /// a leaf node
    Leaf(Leaf),
    /// an error, will be the last item
    Error(EncodeError),
    ///
    Done,
}

impl From<Leaf> for EncodedItem {
    fn from(l: Leaf) -> Self {
        Self::Leaf(l)
    }
}

impl From<Parent> for EncodedItem {
    fn from(p: Parent) -> Self {
        Self::Parent(p)
    }
}

/// Traverse ranges relevant to a query from a reader and outboard to a stream
///
/// This function validates the data before writing.
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub async fn traverse_ranges_validated<D: ReadAt, O: Outboard>(
    data: D,
    outboard: O,
    ranges: &ChunkRangesRef,
    encoded: &tokio::sync::mpsc::Sender<EncodedItem>,
) {
    encoded
        .send(EncodedItem::Size(outboard.tree().size()))
        .await
        .ok();
    let res = match traverse_ranges_validated_impl(data, outboard, ranges, encoded).await {
        Ok(()) => EncodedItem::Done,
        Err(cause) => EncodedItem::Error(cause),
    };
    encoded.send(res).await.ok();
}

/// Encode ranges relevant to a query from a reader and outboard to a writer
///
/// This function validates the data before writing.
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
async fn traverse_ranges_validated_impl<D: ReadAt, O: Outboard>(
    data: D,
    outboard: O,
    ranges: &ChunkRangesRef,
    encoded: &tokio::sync::mpsc::Sender<EncodedItem>,
) -> result::Result<(), EncodeError> {
    if ranges.is_empty() {
        return Ok(());
    }
    let mut stack: SmallVec<[_; 10]> = SmallVec::<[blake3::Hash; 10]>::new();
    stack.push(outboard.root());
    let data = data;
    let tree = outboard.tree();
    // canonicalize ranges
    let ranges = truncate_ranges(ranges, tree.size());
    for item in tree.ranges_pre_order_chunks_iter_ref(ranges, 0) {
        match item {
            BaoChunk::Parent {
                is_root,
                left,
                right,
                node,
                ..
            } => {
                let (l_hash, r_hash) = outboard.load(node)?.unwrap();
                let actual = parent_cv(&l_hash, &r_hash, is_root);
                let expected = stack.pop().unwrap();
                if actual != expected {
                    return Err(EncodeError::ParentHashMismatch(node));
                }
                if right {
                    stack.push(r_hash);
                }
                if left {
                    stack.push(l_hash);
                }
                encoded
                    .send(
                        Parent {
                            node,
                            pair: (l_hash, r_hash),
                        }
                        .into(),
                    )
                    .await
                    .ok();
            }
            BaoChunk::Leaf {
                start_chunk,
                size,
                is_root,
                ranges,
                ..
            } => {
                let expected = stack.pop().unwrap();
                let start = start_chunk.to_bytes();
                let mut buffer = vec![0u8; size as usize];
                data.read_exact_at(start, &mut buffer)?;
                let buffer = buffer.into();
                if !ranges.is_all() {
                    // we need to encode just a part of the data
                    //
                    // write into an out buffer to ensure we detect mismatches
                    // before writing to the output.
                    let mut out_buf = Vec::new();
                    let actual = traverse_selected_rec(
                        start_chunk,
                        buffer,
                        is_root,
                        ranges,
                        tree.block_size.to_u32(),
                        true,
                        &mut out_buf,
                    );
                    if actual != expected {
                        return Err(EncodeError::LeafHashMismatch(start_chunk));
                    }
                    for item in out_buf.into_iter() {
                        encoded.send(item).await.ok();
                    }
                } else {
                    let actual = hash_subtree(start_chunk.0, &buffer, is_root);
                    #[allow(clippy::redundant_slicing)]
                    if actual != expected {
                        return Err(EncodeError::LeafHashMismatch(start_chunk));
                    }
                    encoded
                        .send(
                            Leaf {
                                data: buffer,
                                offset: start_chunk.to_bytes(),
                            }
                            .into(),
                        )
                        .await
                        .ok();
                };
            }
        }
    }
    Ok(())
}

/// Encode ranges relevant to a query from a slice and outboard to a buffer.
///
/// This will compute the root hash, so it will have to traverse the entire tree.
/// The `ranges` parameter just controls which parts of the data are written.
///
/// Except for writing to a buffer, this is the same as [hash_subtree].
/// The `min_level` parameter controls the minimum level that will be emitted as a leaf.
/// Set this to 0 to disable chunk groups entirely.
/// The `emit_data` parameter controls whether the data is written to the buffer.
/// When setting this to false and setting query to `RangeSet::all()`, this can be used
/// to write an outboard.
///
/// `res` will not contain the length prefix, so if you want a bao compatible format,
/// you need to prepend it yourself.
///
/// This is used as a reference implementation in tests, but also to compute hashes
/// below the chunk group size when creating responses for outboards with a chunk group
/// size of >0.
pub fn traverse_selected_rec(
    start_chunk: ChunkNum,
    data: Bytes,
    is_root: bool,
    query: &ChunkRangesRef,
    min_level: u32,
    emit_data: bool,
    res: &mut Vec<EncodedItem>,
) -> blake3::Hash {
    use blake3::guts::{ChunkState, CHUNK_LEN};
    if data.len() <= CHUNK_LEN {
        if emit_data && !query.is_empty() {
            res.push(
                Leaf {
                    data: data.clone(),
                    offset: start_chunk.to_bytes(),
                }
                .into(),
            );
        }
        let mut hasher = ChunkState::new(start_chunk.0);
        hasher.update(&data);
        hasher.finalize(is_root)
    } else {
        let chunks = data.len() / CHUNK_LEN + (data.len() % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let level = chunks.trailing_zeros() - 1;
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let mid_chunk = start_chunk + (mid as u64);
        let (l_ranges, r_ranges) = split_inner(query, start_chunk, mid_chunk);
        // for empty ranges, we don't want to emit anything.
        // for full ranges where the level is below min_level, we want to emit
        // just the data.
        //
        // todo: maybe call into blake3::guts::hash_subtree directly for this case? it would be faster.
        let full = query.is_all();
        let emit_parent = !query.is_empty() && (!full || level >= min_level);
        let hash_offset = if emit_parent {
            // make some room for the hash pair
            let pair = Parent {
                node: TreeNode(0),
                pair: ([0; 32].into(), [0; 32].into()),
            };
            res.push(pair.into());
            Some(res.len() - 1)
        } else {
            None
        };
        // recurse to the left and right to compute the hashes and emit data
        let left = traverse_selected_rec(
            start_chunk,
            data.slice(..mid_bytes),
            false,
            l_ranges,
            min_level,
            emit_data,
            res,
        );
        let right = traverse_selected_rec(
            mid_chunk,
            data.slice(mid_bytes..),
            false,
            r_ranges,
            min_level,
            emit_data,
            res,
        );
        // backfill the hashes if needed
        if let Some(o) = hash_offset {
            // todo: figure out how to get the tree node from the start chunk!
            let node = TreeNode(0);
            res[o] = Parent {
                node,
                pair: (left, right),
            }
            .into();
        }
        parent_cv(&left, &right, is_root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        io::{outboard::PreOrderMemOutboard, sync::encode_ranges_validated},
        BlockSize, ChunkRanges,
    };

    fn flatten(items: Vec<EncodedItem>) -> Vec<u8> {
        let mut res = Vec::new();
        for item in items {
            match item {
                EncodedItem::Leaf(Leaf { data, .. }) => res.extend_from_slice(&data),
                EncodedItem::Parent(Parent { pair: (l, r), .. }) => {
                    res.extend_from_slice(l.as_bytes());
                    res.extend_from_slice(r.as_bytes());
                }
                _ => {}
            }
        }
        res
    }

    #[tokio::test]
    async fn smoke() {
        let data = [0u8; 100000];
        let outboard = PreOrderMemOutboard::create(&data, BlockSize::from_chunk_log(4));
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let mut encoded = Vec::new();
        encode_ranges_validated(&data[..], &outboard, &ChunkRanges::empty(), &mut encoded).unwrap();
        tokio::spawn(async move {
            traverse_ranges_validated(&data[..], &outboard, &ChunkRanges::empty(), &tx).await;
        });
        let mut res = Vec::new();
        while let Some(item) = rx.recv().await {
            res.push(item);
        }
        println!("{:?}", res);
        let encoded2 = flatten(res);
        assert_eq!(encoded, encoded2);
    }
}
