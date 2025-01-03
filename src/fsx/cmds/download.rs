use anyhow::Result;
use cfs::cas;
use std::fs::File;
use std::io::prelude::*;

pub fn download(path: String, digest: String) -> Result<()> {
    println!("Download digest {} at {}", digest, path);
    let tokens: Vec<_> = digest.split("/").collect();
    if tokens.len() != 2 {
        return Err(anyhow::Error::msg("malformed digest"));
    }

    let hash = tokens[0];
    let size = tokens[1].parse::<i64>().unwrap();

    let mut cas_client = cas::blocking::CacheClient::new()?;
    let blob = cas_client.read_blob(hash, size)?;

    let mut file = File::create(path)?;
    file.write_all(&blob).map_err(|e| anyhow::Error::new(e))
}
