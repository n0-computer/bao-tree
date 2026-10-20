//! Recursive encoding of parts of a chunk group, and truncation of ranges to
//! the size of a tree.
//!
//! The reference implementations that the tests use are in the tests module.
use crate::{blake3, split_inner, ChunkNum, ChunkRangesRef};

/// Given a set of chunk ranges, adapt them for a tree of the given size.
///
/// This will consider anything behind the tree size to be a request for the last chunk,
/// which makes things a bit more complex. This is useful to get a size proof for a
/// blob of an unknown size.
///
/// If you don't need this, you can just split the ranges on the tree size and then
/// keep the first part.
///
/// Examples:
///
/// assuming a size of 7 chunks:
///
/// 0..6 will remain 0..6
/// 0..7 will become 0.. (the entire blob)
/// 0..10, 11.12 will become 0.. (the entire blob)
/// 0..6, 7..10 will become 0.. (the entire blob). the last chunk will be included and the hole filled
/// 3..6, 7..10 will become 3.. the last chunk will be included and the hole filled
/// 0..5, 7..10 will become 0..5, 7.. (the last chunk will be included, but chunkk 5 will not be sent)
pub fn truncate_ranges(ranges: &ChunkRangesRef, size: u64) -> &ChunkRangesRef {
    let bs = ranges.boundaries();
    ChunkRangesRef::new_unchecked(&bs[..truncated_len(ranges, size)])
}

/// A version of [canonicalize_ranges] that takes and returns an owned [ChunkRanges].
///
/// This is needed for the state machines that own their ranges.
#[cfg(feature = "tokio_fsm")]
pub fn truncate_ranges_owned(ranges: crate::ChunkRanges, size: u64) -> crate::ChunkRanges {
    let n = truncated_len(&ranges, size);
    let mut boundaries = ranges.into_inner();
    boundaries.truncate(n);
    crate::ChunkRanges::new_unchecked(boundaries)
}

fn truncated_len(ranges: &ChunkRangesRef, size: u64) -> usize {
    let end = ChunkNum::chunks(size);
    let lc = ChunkNum(end.0.saturating_sub(1));
    let bs = ranges.boundaries();
    match bs.binary_search(&lc) {
        Ok(i) if (i & 1) == 0 => {
            // last chunk is included and is a start boundary.
            // keep it and remove the rest.
            i + 1
        }
        Ok(i) => {
            if bs.len() == i + 1 {
                // last chunk is included and is an end boundary, and there is nothing behind it.
                // nothing to do.
                i + 1
            } else {
                // last chunk is included and is an end boundary, and there is something behind it.
                // remove it to turn the range into an open range.
                i
            }
        }
        Err(ip) if (ip & 1) == 0 => {
            // insertion point would be a start boundary.
            if bs.len() == ip {
                // the insertion point is at the end, nothing to do.
                ip
            } else {
                // include one start boundary > lc to turn the range into an open range.
                ip + 1
            }
        }
        Err(ip) => {
            // insertion point is an end boundary
            // the value at ip must be > lc, so we can just omit it
            ip
        }
    }
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
///
/// `mode` is the hashing mode for subtree and parent hashes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_selected_rec(
    start_chunk: ChunkNum,
    data: &[u8],
    is_root: bool,
    query: &ChunkRangesRef,
    min_level: u32,
    emit_data: bool,
    res: &mut Vec<u8>,
    mode: crate::HashMode,
) -> blake3::Hash {
    use blake3::CHUNK_LEN;
    if data.len() <= CHUNK_LEN {
        if emit_data && !query.is_empty() {
            res.extend_from_slice(data);
        }
        mode.hash_subtree(start_chunk.0, data, is_root)
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
        // todo: maybe call into blake3::hazmat::hash_subtree directly for this case? it would be faster.
        let full = query.is_all();
        let emit_parent = !query.is_empty() && (!full || level >= min_level);
        let hash_offset = if emit_parent {
            // make some room for the hashes
            res.extend_from_slice(&[0xFFu8; 64]);
            Some(res.len() - 64)
        } else {
            None
        };
        // recurse to the left and right to compute the hashes and emit data
        let left = encode_selected_rec(
            start_chunk,
            &data[..mid_bytes],
            false,
            l_ranges,
            min_level,
            emit_data,
            res,
            mode,
        );
        let right = encode_selected_rec(
            mid_chunk,
            &data[mid_bytes..],
            false,
            r_ranges,
            min_level,
            emit_data,
            res,
            mode,
        );
        // backfill the hashes if needed
        if let Some(o) = hash_offset {
            res[o..o + 32].copy_from_slice(left.as_bytes());
            res[o + 32..o + 64].copy_from_slice(right.as_bytes());
        }
        mode.parent_cv(&left, &right, is_root)
    }
}
