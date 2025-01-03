use anyhow::Result;
use clap::{crate_version, Arg, Command};

mod fuse;

fn main() -> Result<()> {
    let app = Command::new("cfs daemon")
        .version(crate_version!())
        .author("Cheng Pan")
        .arg(Arg::new("MOUNT_POINT").required(true).index(2))
        .arg(
            Arg::new("auto_unmount")
                .long("auto_unmount")
                .help("Automatically unmount on process exit"),
        )
        .arg(
            Arg::new("DIGEST")
                .required(true)
                .index(1)
                .help("The digest of the root directory"),
        )
        .get_matches();

    let digest = app
        .value_of("DIGEST")
        .ok_or(anyhow::Error::msg("fail to parse DIGEST"))?;
    let tokens: Vec<_> = digest.split("/").collect();
    if tokens.len() != 2 {
        return Err(anyhow::Error::msg("malformed digest"));
    }

    let hash = tokens[0];
    let size = tokens[1].parse::<i64>().unwrap();

    let mountpoint = app.value_of("MOUNT_POINT").unwrap();
    fuse::run(mountpoint, hash, size).map_err(|e| e.into())
}
