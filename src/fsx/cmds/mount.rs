use anyhow::Result;
use std::os::unix::fs;

pub fn mount(path: String, digest: String) -> Result<()> {
    println!("Mount digest {} at {}", digest, path);
    let original = format!(
        "/home/cheng.pan/fuse/{}",
        digest
    );
    fs::symlink(original, path)
        .map_err(|e| anyhow::Error::msg(format!("failed to create symlink {}", e)))
}
