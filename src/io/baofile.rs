//! A wrapper around a data reader and an outboard that supports Read, ReadAt and Seek.
use std::{
    io::{Read, Seek, SeekFrom},
    result,
};

use bytes::Bytes;
use iroh_blake3 as blake3;
use iroh_blake3::guts::parent_cv;
use positioned_io::ReadAt;
use smallvec::SmallVec;

use super::{mixed::ReadBytesAt, outboard::PreOrderOutboard, sync::Outboard, EncodeError, Leaf};
use crate::{
    hash_subtree, iter::BaoChunk, rec::truncate_ranges, split_inner, ChunkNum, ChunkRanges,
    ChunkRangesRef,
};

/// A content item for the bao streaming protocol.
#[derive(Debug)]
pub enum EncodedItem {
    /// a leaf node
    Leaf(Leaf),
    /// an error, will be the last item
    Error(EncodeError),
}

impl From<Leaf> for EncodedItem {
    fn from(l: Leaf) -> Self {
        Self::Leaf(l)
    }
}

impl From<EncodeError> for EncodedItem {
    fn from(e: EncodeError) -> Self {
        Self::Error(e)
    }
}

/// Traverse ranges relevant to a query from a reader and outboard to a stream
///
/// This function validates the data before writing.
///
/// It is possible to encode ranges from a partial file and outboard.
/// This will either succeed if the requested ranges are all present, or fail
/// as soon as a range is missing.
pub fn iter_ranges_validated<'a, D: ReadBytesAt, O: Outboard>(
    data: &'a D,
    outboard: &'a O,
    ranges: &'a ChunkRangesRef,
) -> impl Iterator<Item = EncodedItem> + 'a {
    genawaiter::rc::Gen::new(|co| async move {
        if let Err(cause) = iter_ranges_validated_impl(data, outboard, ranges, &co).await {
            co.yield_(EncodedItem::Error(cause)).await;
        }
    })
    .into_iter()
}

async fn iter_ranges_validated_impl<'a, D: ReadBytesAt, O: Outboard>(
    data: &'a D,
    outboard: &'a O,
    ranges: &'a ChunkRangesRef,
    co: &'a genawaiter::rc::Co<EncodedItem>,
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
                let buffer = data.read_bytes_at(start, size)?;
                if !ranges.is_all() {
                    // we need to encode just a part of the data
                    //
                    // write into an out buffer to ensure we detect mismatches
                    // before writing to the output.
                    //
                    // use a smallvec here?
                    let mut out_buf = Vec::new();
                    let actual = traverse_selected_rec(
                        start_chunk,
                        buffer,
                        is_root,
                        ranges,
                        tree.block_size.to_u32(),
                        &mut out_buf,
                    );
                    if actual != expected {
                        return Err(EncodeError::LeafHashMismatch(start_chunk));
                    }
                    for item in out_buf.into_iter() {
                        co.yield_(item).await;
                    }
                } else {
                    let actual = hash_subtree(start_chunk.0, &buffer, is_root);
                    #[allow(clippy::redundant_slicing)]
                    if actual != expected {
                        return Err(EncodeError::LeafHashMismatch(start_chunk));
                    }
                    let item = Leaf {
                        data: buffer,
                        offset: start_chunk.to_bytes(),
                    };
                    co.yield_(item.into()).await;
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
    res: &mut Vec<EncodedItem>,
) -> blake3::Hash {
    use blake3::guts::{ChunkState, CHUNK_LEN};
    if data.len() <= CHUNK_LEN {
        if !query.is_empty() {
            res.push(
                Leaf {
                    data: data.clone(),
                    offset: start_chunk.to_bytes(),
                }
                .into(),
            );
        };
        let mut hasher = ChunkState::new(start_chunk.0);
        hasher.update(&data);
        hasher.finalize(is_root)
    } else {
        let chunks = data.len() / CHUNK_LEN + (data.len() % CHUNK_LEN != 0) as usize;
        let chunks = chunks.next_power_of_two();
        let mid = chunks / 2;
        let mid_bytes = mid * CHUNK_LEN;
        let mid_chunk = start_chunk + (mid as u64);
        let (l_ranges, r_ranges) = split_inner(query, start_chunk, mid_chunk);
        // recurse to the left and right to compute the hashes and emit data
        let left = traverse_selected_rec(
            start_chunk,
            data.slice(..mid_bytes),
            false,
            l_ranges,
            min_level,
            res,
        );
        let right = traverse_selected_rec(
            mid_chunk,
            data.slice(mid_bytes..),
            false,
            r_ranges,
            min_level,
            res,
        );
        parent_cv(&left, &right, is_root)
    }
}

/// A wrapper around a data reader and an outboard that supports ReadAt.
pub struct BaoFile<D, O> {
    data: D,
    outboard: PreOrderOutboard<O>,
}

/// A wrapper around a data reader and an outboard that supports ReadAt and Seek.
pub struct SeekableBaoFile<D, O> {
    inner: BaoFile<D, O>,
    position: u64,
}

impl<D: ReadBytesAt, O: ReadAt> Seek for SeekableBaoFile<D, O> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(offset) => {
                self.position = offset;
            }
            SeekFrom::End(offset) => {
                let size = self.inner.outboard.tree.size();
                self.position = if offset > 0 {
                    size.saturating_add(offset as u64)
                } else {
                    size.saturating_sub(offset.wrapping_neg() as u64)
                };
            }
            SeekFrom::Current(offset) => {
                if let Some(pos) = self.position.checked_add(offset as u64) {
                    self.position = pos;
                } else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "overflow when seeking",
                    ));
                };
            }
        }
        Ok(self.position)
    }
}

impl<D: ReadBytesAt, O: ReadAt> Read for SeekableBaoFile<D, O> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.position >= self.inner.outboard.tree.size() {
            return Ok(0);
        }
        let read = self.inner.read_at(self.position, buf)?;
        if let Some(pos) = self.position.checked_add(read as u64) {
            self.position = pos;
        } else {
            // todo: can this ever be hit? read_at would fail
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "overflow when seeking",
            ));
        };
        Ok(read)
    }
}

impl<D: ReadBytesAt, O: ReadAt> ReadAt for SeekableBaoFile<D, O> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read_at(pos, buf)
    }
}

impl<D: ReadBytesAt, O: ReadAt> ReadAt for BaoFile<D, O> {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let end = offset + buf.len() as u64;
        let chunk_ranges = ChunkRanges::from(ChunkNum::full_chunks(offset)..ChunkNum::chunks(end));
        let mut end: usize = 0;
        println!("iter_ranges_validated {:?}", chunk_ranges);
        for item in iter_ranges_validated(&self.data, &self.outboard, &chunk_ranges) {
            match item {
                EncodedItem::Leaf(Leaf {
                    data,
                    offset: leaf_start,
                }) => {
                    // take the part of leaf that is relevant to the query and copy it into the buffer
                    // leaf start and end relative to the buffer
                    if leaf_start < offset {
                        let skip = usize::try_from(offset - leaf_start).unwrap();
                        end = (data.len() - skip).min(buf.len());
                        buf[..end].copy_from_slice(&data[skip..(skip + end)]);
                    } else {
                        let leaf_start = usize::try_from(leaf_start - offset).unwrap();
                        let leaf_end = leaf_start + data.len();
                        end = leaf_end.min(buf.len());
                        buf[leaf_start..end].copy_from_slice(&data[..(end - leaf_start)]);
                    }
                }
                EncodedItem::Error(e) => {
                    if end == 0 {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(end)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use testresult::TestResult;

    use super::*;
    use crate::{io::outboard::PreOrderMemOutboard, BlockSize};

    /// Generate test data for size n.
    ///
    /// We don't really care about the content, since we assume blake3 works.
    /// The only thing it should not be is all zeros, since that is what you
    /// will get for a gap, and it should have different values for each blake3
    /// chunk so we can detect block mixups.
    pub fn test_data(n: usize) -> Vec<u8> {
        let mut res = Vec::with_capacity(n);
        // Using uppercase A-Z (65-90), 26 possible characters
        for i in 0..n {
            // Change character every 1024 bytes
            let block_num = i / 1024;
            // Map to uppercase A-Z range (65-90)
            let ascii_val = 65 + (block_num % 26) as u8;
            res.push(ascii_val);
        }
        res
    }

    fn test_file(size: usize) -> SeekableBaoFile<Vec<u8>, Vec<u8>> {
        let data = test_data(size).into();
        let outboard = PreOrderMemOutboard::create(&data, BlockSize(4));
        let outboard = PreOrderOutboard {
            tree: outboard.tree,
            root: outboard.root,
            data: outboard.data.into(),
        };
        let file = BaoFile { data, outboard };
        let file = SeekableBaoFile {
            inner: file,
            position: 0,
        };
        file
    }

    #[test]
    fn smoke() -> TestResult<()> {
        for size in [10000] {
            let actual = test_file(size);
            let data = actual.inner.data.clone();
            // let mut expected = tempfile::tempfile()?;
            // expected.write_all(&data)?;
            // expected.rewind()?;
            let expected = Cursor::new(data);
            // let mut buf = [0u8; 5000];
            // let n = actual.read_at(5000, &mut buf)?;
            // println!("{}", n);
            run_consistency_tests(actual, expected, size);
        }
        Ok(())
    }

    use std::io::{Read, Seek, SeekFrom};

    // Generic test function comparing two instances implementing Read + Seek
    fn run_consistency_tests<T1, T2>(mut file1: T1, mut file2: T2, size: usize)
    where
        T1: Read + Seek,
        T2: Read + Seek,
    {
        // Test 1: Seek from Start and Read
        let pos = size / 2; // Middle of the data
        file1.seek(SeekFrom::Start(pos as u64)).unwrap();
        file2.seek(SeekFrom::Start(pos as u64)).unwrap();
        let mut buf1 = vec![0; size - pos];
        let mut buf2 = vec![0; size - pos];
        let read1 = file1.read(&mut buf1).unwrap();
        let read2 = file2.read(&mut buf2).unwrap();
        assert_eq!(read1, read2, "Read length mismatch after SeekFrom::Start");
        assert_eq!(buf1, buf2, "Read data mismatch after SeekFrom::Start");

        // Test 2: Seek from End (within bounds) and Read
        file1.seek(SeekFrom::End(-(pos as i64))).unwrap(); // Seek to middle from end
        file2.seek(SeekFrom::End(-(pos as i64))).unwrap();
        let mut buf1 = vec![0; size - pos];
        let mut buf2 = vec![0; size - pos];
        let read1 = file1.read(&mut buf1).unwrap();
        let read2 = file2.read(&mut buf2).unwrap();
        assert_eq!(
            read1, read2,
            "Read length mismatch after SeekFrom::End (within bounds)"
        );
        assert_eq!(
            buf1, buf2,
            "Read data mismatch after SeekFrom::End (within bounds)"
        );

        // Test 3: Seek beyond End and Read
        file1.seek(SeekFrom::End(10)).unwrap(); // Beyond end
        file2.seek(SeekFrom::End(10)).unwrap();
        let mut buf1 = vec![0; 10];
        let mut buf2 = vec![0; 10];
        let read1 = file1.read(&mut buf1).unwrap();
        let read2 = file2.read(&mut buf2).unwrap();
        assert_eq!(read1, 0, "Expected 0 bytes read beyond end for file1");
        assert_eq!(read2, 0, "Expected 0 bytes read beyond end for file2");
        assert_eq!(
            buf1,
            vec![0; 10],
            "Buffer should be unchanged after read beyond end"
        );
        assert_eq!(
            buf2,
            vec![0; 10],
            "Buffer should be unchanged after read beyond end"
        );

        // // Test 4: Seek before Start (should saturate to 0) and Read
        // file1.seek(SeekFrom::End(-(size as i64 * 2))).unwrap(); // Way before start
        // file2.seek(SeekFrom::End(-(size as i64 * 2))).unwrap();
        // let mut buf1 = vec![0; size];
        // let mut buf2 = vec![0; size];
        // let read1 = file1.read(&mut buf1).unwrap();
        // let read2 = file2.read(&mut buf2).unwrap();
        // assert_eq!(
        //     read1, size,
        //     "Expected full read from start after seek before start"
        // );
        // assert_eq!(
        //     read2, size,
        //     "Expected full read from start after seek before start"
        // );
        // assert_eq!(buf1, buf2, "Read data mismatch after seek before start");

        // Test 5: Seek from Current
        file1.seek(SeekFrom::Start(0)).unwrap();
        file2.seek(SeekFrom::Start(0)).unwrap();
        file1.seek(SeekFrom::Current(pos as i64)).unwrap(); // Move forward
        file2.seek(SeekFrom::Current(pos as i64)).unwrap();
        let mut buf1 = vec![0; size - pos];
        let mut buf2 = vec![0; size - pos];
        let read1 = file1.read(&mut buf1).unwrap();
        let read2 = file2.read(&mut buf2).unwrap();
        assert_eq!(read1, read2, "Read length mismatch after SeekFrom::Current");
        assert_eq!(buf1, buf2, "Read data mismatch after SeekFrom::Current");

        // Test 6: Verify position after seek
        file1.seek(SeekFrom::End(0)).unwrap();
        file2.seek(SeekFrom::End(0)).unwrap();
        let pos1 = file1.seek(SeekFrom::Current(0)).unwrap(); // Get current position
        let pos2 = file2.seek(SeekFrom::Current(0)).unwrap();
        assert_eq!(pos1, size as u64, "Position mismatch at end for file1");
        assert_eq!(pos2, size as u64, "Position mismatch at end for file2");
    }
}
