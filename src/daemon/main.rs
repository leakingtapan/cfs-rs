use std::path::PathBuf;

use anyhow::Result;
use cfs::cas::configs::Configs;
use clap::{crate_version, Parser};
use config::{Config, Environment, File};

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

    #[arg(short, long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Automatically unmount on process exit
    #[arg(short, long, help = "Automatically unmount on process exit")]
    auto_unmount: Option<bool>,
}

fn load_config(cli: &Cli) -> Result<Configs, config::ConfigError> {
    let builder = Config::builder()
        // Start with default values
        .set_default("port", 8080)?
        // Add config file if specified
        .add_source(
            cli.config
                .as_ref()
                .map(|p| File::with_name(p.to_str().unwrap()))
                .unwrap_or_else(|| File::with_name("config"))
                .required(false),
        )
        // Add in environment variables with prefix "CAS_"
        .add_source(Environment::with_prefix("CAS"))
        // Add CLI arguments last so they take highest precedence
        .set_override_option("auto_unmount", cli.auto_unmount.clone())?;

    builder.build()?.try_deserialize()
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let configs = load_config(&cli)?;

    let tokens: Vec<_> = cli.digest.split("/").collect();
    if tokens.len() != 2 {
        return Err(anyhow::Error::msg("malformed digest"));
    }
    let hash = tokens[0];
    let size = tokens[1].parse::<i64>().unwrap();

    let mountpoint = cli.mount_point;
    println!("mounting the digest {} at {}", cli.digest, mountpoint);
    fuse::run(&mountpoint, hash, size, configs).map_err(|e| e.into())
}

