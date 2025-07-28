use std::{io::Write, path::PathBuf};

use anyhow::Context;
use bao_tree::{BaoTree, BlockSize};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug, Clone)]
#[clap(version)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Command,
    #[clap(
        short,
        long,
        default_value = "0",
        help = "Bao block size, the actual block size in bytes is 1024 << block_size"
    )]
    pub block_size: u8,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Command {
    Outboard {
        path: PathBuf,
        #[clap(long)]
        out: Option<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let bs = BlockSize::from_chunk_log(args.block_size);
    if args.block_size != 0 {
        println!("Using block size: {}", bs.bytes());
    }
    match args.command {
        Command::Outboard { path, out } => {
            let meta = std::fs::metadata(&path)?;
            anyhow::ensure!(meta.is_file(), "Path must be a file");
            let size = meta.len();
            let out = if let Some(out) = out {
                out
            } else {
                let name = path.file_name().context("context")?;
                let extension = "obao";
                std::env::current_dir()?.join(format!("{}.{}", name.to_string_lossy(), extension))
            };
            let source = std::fs::File::open(&path)?;
            let target = std::fs::File::create(out)?;
            let source = std::io::BufReader::with_capacity(1024 * 1024 * 16, source);
            let mut target = std::io::BufWriter::with_capacity(1024 * 1024 * 16, target);
            let t0 = std::time::Instant::now();
            let tree = BaoTree::new(size, bs);
            let hash = bao_tree::io::sync::outboard_post_order(source, tree, &mut target)?;
            target.write_all(size.to_le_bytes().as_ref())?;
            let dt = t0.elapsed();
            let rate = size as f64 / dt.as_secs_f64();
            println!("{hash}");
            println!(
                "{} bytes in {} seconds, {} bytes/sec",
                size,
                dt.as_secs_f64(),
                rate
            );
        }
    }
    Ok(())
}
