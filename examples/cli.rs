//! A command line interface to encode and decode parts of files.
//!
//! This is a simple example of how to use bao-tree to encode and decode parts of files.
use std::{
    io::{self, Cursor, Write},
    path::PathBuf,
};

use anyhow::Context;
use bao_tree::{blake3, io::round_up_to_chunks};
use bao_tree::{
    io::{
        outboard::PreOrderMemOutboard,
        sync::{encode_ranges_validated, DecodeResponseItem, DecodeResponseIter, Outboard},
        Header, Leaf, Parent,
    },
    BlockSize, ChunkNum, ChunkRanges,
};
use clap::{Parser, Subcommand};
use positioned_io::WriteAt;
use range_collections::RangeSet2;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

const BLOCK_SIZE: BlockSize = BlockSize(4);

#[derive(Parser, Debug)]
struct Args {
    /// Block size log. Actual block size is 1024 * 2^block_size_log
    #[clap(long, default_value_t = 4)]
    block_size: u8,
    #[clap(long)]
    r#async: bool,
    #[clap(long, short)]
    quiet: bool,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Encode a part of a file.
    Encode(EncodeArgs),
    /// Decode a part of a file.
    Decode(DecodeArgs),
}

#[derive(Parser, Debug)]
struct EncodeArgs {
    /// The file to encode.
    file: PathBuf,
    /// Byte ranges to encode.
    #[clap(long)]
    ranges: Vec<String>,
    /// The file to write the encoded data to.
    ///
    /// If not given, the encoded data is written to stdout.
    #[clap(long)]
    out: Option<PathBuf>,
}

#[derive(Parser, Debug)]
struct DecodeArgs {
    /// The file to decode.
    file: PathBuf,
    /// The file to encode into
    ///
    /// If not given, the data is dumped to stdout.
    /// If the message is just a part of the file, only the chunks in the message
    /// are written to the file, and the rest left as is or zeroed out.
    #[clap(long)]
    target: Option<PathBuf>,
}

/// A message that contains a self-contained verifiable encoding of a part of a file.
///
/// The block size needs to be common for sender and receiver.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "MessageWireFormat", into = "MessageWireFormat")]
struct Message {
    hash: blake3::Hash,
    ranges: ChunkRanges,
    encoded: Vec<u8>,
}

/// Helper struct to serialize and deserialize messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageWireFormat {
    hash: [u8; 32],
    ranges: Vec<u64>,
    encoded: Vec<u8>,
}

impl From<Message> for MessageWireFormat {
    fn from(msg: Message) -> Self {
        Self {
            hash: msg.hash.into(),
            ranges: msg.ranges.boundaries().iter().map(|b| b.0).collect(),
            encoded: msg.encoded,
        }
    }
}

impl TryFrom<MessageWireFormat> for Message {
    type Error = anyhow::Error;

    fn try_from(msg: MessageWireFormat) -> Result<Self, Self::Error> {
        let hash = blake3::Hash::from(msg.hash);
        let ranges = msg
            .ranges
            .iter()
            .map(|b| ChunkNum(*b))
            .collect::<SmallVec<_>>();
        let ranges = ChunkRanges::new(ranges).context("chunks not sorted")?;
        Ok(Self {
            hash,
            ranges,
            encoded: msg.encoded,
        })
    }
}

macro_rules! log {
    ($verbose:expr, $($arg:tt)*) => {
        if $verbose {
            eprintln!($($arg)*);
        }
    };
}

/// Encode a part of a file, given the outboard.
fn encode(data: &[u8], outboard: impl Outboard, ranges: ChunkRanges) -> Message {
    let mut encoded = Vec::new();
    encode_ranges_validated(data, &outboard, &ranges, &mut encoded).unwrap();
    Message {
        hash: outboard.root(),
        ranges: ranges.clone(),
        encoded,
    }
}

fn decode_into_file(msg: &Message, mut target: std::fs::File, v: bool) -> io::Result<()> {
    let iter =
        DecodeResponseIter::new(msg.hash, BLOCK_SIZE, Cursor::new(&msg.encoded), &msg.ranges);
    for response in iter {
        match response? {
            DecodeResponseItem::Header(Header { size }) => {
                log!(v, "got header claiming a size of {}", size);
                target.set_len(size.0)?;
            }
            DecodeResponseItem::Parent(Parent { node, pair: (l, r) }) => {
                log!(
                    v,
                    "got parent {:?} with hashes {} and {}",
                    node,
                    l.to_hex(),
                    r.to_hex()
                );
            }
            DecodeResponseItem::Leaf(Leaf { offset, data }) => {
                log!(v, "got data at offset {} and len {}", offset, data.len());
                target.write_at(offset.0, &data)?;
            }
        }
    }
    Ok(())
}

fn decode_to_stdout(msg: &Message, v: bool) -> io::Result<()> {
    let iter =
        DecodeResponseIter::new(msg.hash, BLOCK_SIZE, Cursor::new(&msg.encoded), &msg.ranges);
    for response in iter {
        match response? {
            DecodeResponseItem::Header(Header { size }) => {
                log!(v, "got header claiming a size of {}", size);
            }
            DecodeResponseItem::Parent(Parent { node, pair: (l, r) }) => {
                log!(
                    v,
                    "got parent {:?} with hashes {} and {}",
                    node,
                    l.to_hex(),
                    r.to_hex()
                );
            }
            DecodeResponseItem::Leaf(Leaf { offset, data }) => {
                log!(v, "got data at offset {} and len {}", offset, data.len());
                io::stdout().write_all(&data)?;
            }
        }
    }
    Ok(())
}

/// Parse a range string into a range set containing a single range
fn parse_range(range_str: &str) -> anyhow::Result<RangeSet2<u64>> {
    let parts: Vec<&str> = range_str.split("..").collect();
    match parts.as_slice() {
        [start, end] if !start.is_empty() && !end.is_empty() => {
            Ok(RangeSet2::from(start.parse()?..end.parse()?))
        }
        [start, _] if !start.is_empty() => Ok(RangeSet2::from(start.parse()?..)),
        [_, end] if !end.is_empty() => Ok(RangeSet2::from(..end.parse()?)),
        [_, _] => Ok(RangeSet2::from(..)),
        _ => anyhow::bail!("invalid range"),
    }
}

fn parse_ranges(ranges: Vec<String>) -> anyhow::Result<RangeSet2<u64>> {
    let ranges = ranges
        .iter()
        .flat_map(|x| x.split(&[',', ';']))
        .map(|range| parse_range(range))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let ranges = ranges
        .into_iter()
        .fold(RangeSet2::empty(), |acc, item| acc | item);
    Ok(ranges)
}

fn main_sync(args: Args) -> anyhow::Result<()> {
    assert!(args.block_size <= 8);
    let block_size = BlockSize(args.block_size);
    let v = !args.quiet;
    match args.command {
        Command::Encode(EncodeArgs { file, ranges, out }) => {
            let ranges = parse_ranges(ranges)?;
            log!(v, "byte ranges: {:?}", ranges);
            let ranges = round_up_to_chunks(&ranges);
            log!(v, "chunk ranges: {:?}", ranges);
            log!(v, "reading file");
            let data = std::fs::read(file)?;
            log!(v, "computing outboard");
            let t0 = std::time::Instant::now();
            let outboard = PreOrderMemOutboard::create(&data, block_size);
            log!(v, "done in {:?}.", t0.elapsed());
            log!(v, "encoding message");
            let t0 = std::time::Instant::now();
            let msg = encode(&data, &outboard, ranges);
            log!(v, "done in {:?}.", t0.elapsed());
            let bytes = postcard::to_stdvec(&msg)?;
            log!(v, "serialized message. {} bytes", bytes.len());
            if let Some(out) = out {
                std::fs::write(out, bytes)?;
            } else {
                std::io::stdout().write_all(&bytes)?;
            }
        }
        Command::Decode(DecodeArgs { file, target }) => {
            let data = std::fs::read(file)?;
            let msg: Message = postcard::from_bytes(&data)?;
            if let Some(target) = target {
                let target = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(target)?;
                decode_into_file(&msg, target, v)?;
            } else {
                decode_to_stdout(&msg, v)?;
            }
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.r#async {
        todo!()
    } else {
        main_sync(args)
    }
}
