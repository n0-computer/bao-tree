//! Error handling of the io implementations.
//!
//! Streams that break after a number of bytes, like a peer that disconnects,
//! and files that fail to read at a given offset, like a broken disk. All
//! implementations must report the same errors at the same point.
use std::io::{self, Read};

use iroh_io::{AsyncSliceReader, AsyncStreamReader};
use positioned_io::{ReadAt, Size};

use super::*;

/// A stream that fails with `ConnectionReset` after `limit` bytes.
struct FailingStream {
    data: Vec<u8>,
    pos: usize,
    limit: usize,
}

impl FailingStream {
    fn new(data: &[u8], limit: usize) -> Self {
        Self {
            data: data.to_vec(),
            pos: 0,
            limit,
        }
    }

    fn take(&mut self, len: usize) -> io::Result<Bytes> {
        if self.pos + len > self.limit {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        if self.pos + len > self.data.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let res = Bytes::copy_from_slice(&self.data[self.pos..self.pos + len]);
        self.pos += len;
        Ok(res)
    }
}

impl Read for FailingStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.limit {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        let end = self.limit.min(self.data.len()).min(self.pos + buf.len());
        let n = end.saturating_sub(self.pos);
        buf[..n].copy_from_slice(&self.data[self.pos..end]);
        self.pos += n;
        Ok(n)
    }
}

impl AsyncStreamReader for FailingStream {
    async fn read_bytes(&mut self, len: usize) -> io::Result<Bytes> {
        if self.pos >= self.limit {
            return Err(io::ErrorKind::ConnectionReset.into());
        }
        let available = self.limit.min(self.data.len()) - self.pos;
        self.take(len.min(available))
    }

    async fn read<const L: usize>(&mut self) -> io::Result<[u8; L]> {
        let mut res = [0u8; L];
        res.copy_from_slice(&self.take(L)?);
        Ok(res)
    }

    async fn read_bytes_exact(&mut self, len: usize) -> io::Result<Bytes> {
        self.take(len)
    }
}

/// A file whose reads fail if they touch the byte at `broken`.
#[derive(Debug, Clone)]
struct FailingFile {
    data: Bytes,
    broken: u64,
}

impl FailingFile {
    fn intact(data: &[u8]) -> Self {
        Self::broken_at(data, u64::MAX)
    }

    fn broken_at(data: &[u8], broken: u64) -> Self {
        Self {
            data: Bytes::copy_from_slice(data),
            broken,
        }
    }

    fn check(&self, offset: u64, len: usize) -> io::Result<()> {
        if offset <= self.broken && self.broken < offset + len as u64 {
            Err(io::Error::other("broken disk"))
        } else {
            Ok(())
        }
    }

    fn slice(&self, offset: u64, len: usize) -> Bytes {
        let start = (offset as usize).min(self.data.len());
        let end = (start + len).min(self.data.len());
        self.data.slice(start..end)
    }
}

impl ReadAt for FailingFile {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.check(pos, buf.len())?;
        let slice = self.slice(pos, buf.len());
        buf[..slice.len()].copy_from_slice(&slice);
        Ok(slice.len())
    }
}

impl Size for FailingFile {
    fn size(&self) -> io::Result<Option<u64>> {
        Ok(Some(self.data.len() as u64))
    }
}

impl AsyncSliceReader for FailingFile {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        self.check(offset, len)?;
        Ok(self.slice(offset, len))
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.data.len() as u64)
    }
}

#[cfg(feature = "experimental-mixed")]
impl crate::io::mixed::ReadBytesAt for FailingFile {
    fn read_bytes_at(&self, offset: u64, size: usize) -> io::Result<Bytes> {
        self.check(offset, size)?;
        let res = self.slice(offset, size);
        if res.len() < size {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(res)
    }
}

type FileOutboard = PreOrderOutboard<FailingFile>;

/// Property 12: all decoders report the same items for a stream that breaks,
/// ending with exactly one `ConnectionReset` error.
fn decoders_on_broken_stream(case: &Case, pos: u64) -> TestResult {
    let blob = case.blob();
    let (encoded, _) = valid_items(case, &blob)?;
    if encoded.is_empty() {
        return Ok(());
    }
    let limit = select(pos, encoded.len());
    let i = case.decode_input(&blob, &encoded);
    let stream = || FailingStream::new(&encoded, limit);

    let iter = with_mode!(
        i.mode,
        sync::DecodeResponseIter::new,
        sync::DecodeResponseIter::new_keyed,
        (i.root, i.tree, stream(), i.ranges)
    );
    let mut expected = Vec::new();
    for item in iter {
        push(&mut expected, decoded_item(item));
    }
    let reset = Item::Err("Io(ConnectionReset)".into());
    assert_eq!(expected.last(), Some(&reset), "{case:?} limit {limit}");

    let mut decoder = with_mode!(
        i.mode,
        fsm::ResponseDecoder::new,
        fsm::ResponseDecoder::new_keyed,
        (i.root, i.ranges.clone(), i.tree, stream())
    );
    let mut actual = Vec::new();
    block_on(async {
        while let fsm::ResponseDecoderNext::More((next, item)) = decoder.next().await {
            push(&mut actual, decoded_item(item));
            decoder = next;
        }
    });
    assert_eq!(actual, expected, "fsm differs for {case:?} limit {limit}");
    Ok(())
}

/// Encode with each encoder from the given data and outboard.
fn encode_with(
    case: &Case,
    data: &FailingFile,
    outboard: &FileOutboard,
) -> Vec<(&'static str, Encoded)> {
    let ranges = &case.ranges;
    let mut results = Vec::new();

    let mut res = Vec::new();
    let r = with_mode!(
        case.mode,
        sync::encode_ranges,
        sync::keyed_encode_ranges,
        (data.clone(), outboard, ranges, &mut res)
    );
    results.push(("sync", r.map(|_| res).map_err(encode_error)));

    let mut res = Vec::new();
    let r = with_mode!(
        case.mode,
        sync::encode_ranges_validated,
        sync::keyed_encode_ranges_validated,
        (data.clone(), outboard, ranges, &mut res)
    );
    results.push(("sync validated", r.map(|_| res).map_err(encode_error)));

    let mut res = Vec::new();
    let r = block_on(async {
        with_mode!(
            case.mode,
            fsm::encode_ranges,
            fsm::keyed_encode_ranges,
            (data.clone(), outboard.clone(), ranges, &mut res).await
        )
    });
    results.push(("fsm", r.map(|_| res).map_err(encode_error)));

    let mut res = Vec::new();
    let r = block_on(async {
        with_mode!(
            case.mode,
            fsm::encode_ranges_validated,
            fsm::keyed_encode_ranges_validated,
            (data.clone(), outboard.clone(), ranges, &mut res).await
        )
    });
    results.push(("fsm validated", r.map(|_| res).map_err(encode_error)));

    #[cfg(feature = "experimental-mixed")]
    {
        use super::mixed_adapter::{flatten, Collect};
        use crate::io::mixed;
        let mut send = Collect(Vec::new());
        let r = block_on(async {
            with_mode!(
                case.mode,
                mixed::traverse_ranges_validated,
                mixed::keyed_traverse_ranges_validated,
                (data.clone(), outboard, ranges, &mut send).await
            )
        });
        if let Err(e) = r {
            match e {}
        }
        results.push(("mixed", flatten(send.0)));
    }
    results
}

/// The ranges or errors that a validator yields.
#[cfg(feature = "validate")]
fn validated(items: Vec<io::Result<std::ops::Range<ChunkNum>>>) -> Vec<String> {
    items
        .into_iter()
        .map(|item| match item {
            Ok(range) => format!("{}..{}", range.start, range.end),
            Err(e) => format!("Err({:?})", e.kind()),
        })
        .collect()
}

/// Validate the data with the sync and fsm validator, and the outboard alone
/// with the sync and fsm validator.
#[cfg(feature = "validate")]
fn validate_with(case: &Case, data: &FailingFile, outboard: &FileOutboard) -> [Vec<String>; 4] {
    use futures_lite::StreamExt;
    let ranges = &case.ranges;
    let sync_data = with_mode!(
        case.mode,
        sync::valid_ranges,
        sync::keyed_valid_ranges,
        (outboard, data.clone(), ranges).into_iter().collect()
    );
    let fsm_data = block_on(async {
        with_mode!(
            case.mode,
            fsm::valid_ranges,
            fsm::keyed_valid_ranges,
            (outboard.clone(), data.clone(), ranges).collect().await
        )
    });
    let sync_outboard = with_mode!(
        case.mode,
        sync::valid_outboard_ranges,
        sync::keyed_valid_outboard_ranges,
        (outboard, ranges).into_iter().collect()
    );
    let fsm_outboard = block_on(async {
        with_mode!(
            case.mode,
            fsm::valid_outboard_ranges,
            fsm::keyed_valid_outboard_ranges,
            (outboard.clone(), ranges).collect().await
        )
    });
    [
        validated(sync_data),
        validated(fsm_data),
        validated(sync_outboard),
        validated(fsm_outboard),
    ]
}

/// Check that all encoders agree, and that the sync and fsm validators agree.
fn check_agree(case: &Case, data: &FailingFile, outboard: &FileOutboard, what: &str) {
    let results = encode_with(case, data, outboard);
    let (first, expected) = &results[0];
    for (name, res) in &results {
        assert_eq!(
            res, expected,
            "{name} differs from {first} with {what} for {case:?}"
        );
    }
    #[cfg(feature = "validate")]
    {
        let [sync_data, fsm_data, sync_outboard, fsm_outboard] =
            validate_with(case, data, outboard);
        assert_eq!(fsm_data, sync_data, "valid_ranges with {what} for {case:?}");
        assert_eq!(
            fsm_outboard, sync_outboard,
            "valid_outboard_ranges with {what} for {case:?}"
        );
    }
}

/// Property 13: with a data file or an outboard file that fails to read at
/// some offset, all encoders report the same result, and so do the sync and
/// fsm validators.
fn broken_files(case: &Case, pos: u64) -> TestResult {
    let Blob { data, outboard } = case.blob();
    let pre = outboard.flip();
    let (root, tree) = (pre.root, pre.tree);
    let outboard = |data: FailingFile| PreOrderOutboard { root, tree, data };
    if !data.is_empty() {
        let broken = select(pos, data.len()) as u64;
        let file = FailingFile::broken_at(&data, broken);
        let what = format!("data broken at {broken}");
        check_agree(
            case,
            &file,
            &outboard(FailingFile::intact(&pre.data)),
            &what,
        );
    }
    if !pre.data.is_empty() {
        let broken = select(pos, pre.data.len()) as u64;
        let file = FailingFile::broken_at(&pre.data, broken);
        let what = format!("outboard broken at {broken}");
        check_agree(case, &FailingFile::intact(&data), &outboard(file), &what);
    }
    Ok(())
}

#[test]
fn decoders_on_broken_stream_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(8) {
            decoders_on_broken_stream(&case, pos)?;
        }
    }
    Ok(())
}

#[proptest]
fn decoders_on_broken_stream_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(decoders_on_broken_stream(&case, pos));
}

#[test]
fn broken_files_cases() -> TestResult {
    for case in fixed_cases() {
        for pos in positions(8) {
            broken_files(&case, pos)?;
        }
    }
    Ok(())
}

#[proptest]
fn broken_files_proptest(#[strategy(case())] case: Case, pos: u64) {
    check(broken_files(&case, pos));
}
