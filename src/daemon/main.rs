use anyhow::Result;
use clap::{crate_version, Parser};

mod fuse;

/// Cli is a commnad line struct for the daemon
#[derive(Parser)]
#[command(name = "cfsd")]
#[command(about = "cfs daemon", long_about = None)]
#[command(author = "Cheng Pan", version = crate_version!())]
struct Cli {
    /// The mount point of the daemon
    #[arg(required(true), help = "The mount point of the filesystem", index(2))]
    mount_point: String,

    /// The digest of the root directory
    #[arg(required(true), help = "The digest of the root directory")]
    digest: String,

    /// Automatically unmount on process exit
    #[arg(short, long, help = "Automatically unmount on process exit")]
    auto_unmount: bool,
}

#[derive(Debug)]
struct Settings {
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let tokens: Vec<_> = cli.digest.split("/").collect();
    if tokens.len() != 2 {
        return Err(anyhow::Error::msg("malformed digest"));
    }
    let hash = tokens[0];
    let size = tokens[1].parse::<i64>().unwrap();

    let mountpoint = cli.mount_point;
    fuse::run(&mountpoint, hash, size).map_err(|e| e.into())
}

