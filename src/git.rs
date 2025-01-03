use anyhow::Result;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn get_git_root(path: &Path) -> Result<PathBuf> {
    let path = path
        .to_str()
        .ok_or(anyhow::Error::msg("failed to convert"))?;
    let output = Command::new("git")
        .args(["-C", path, "rev-parse", "--show-toplevel"])
        .output()?;

    if output.status.success() {
        let stdout = output.stdout;
        return String::from_utf8(stdout)
            .map(|p| PathBuf::from(p.trim()))
            .map_err(|e| anyhow::Error::msg(format!("failed to parse stdout {}", e)));
    } else {
        return Err(anyhow::Error::msg("failed to get git root directory"));
    }
}

// generate the lfs object path under the LFS storage at
// .git/lfs/objects/hash[0:2]/[2:4]/hash
pub fn get_lfs_object_path(root: PathBuf, hash: String) -> PathBuf {
    let mut ret = PathBuf::new();
    ret.push(root);
    ret.push(".git/lfs/objects");
    ret.push(&hash[0..2]);
    ret.push(&hash[2..4]);
    ret.push(hash);
    return ret;
}

// calling `git lfs fetch -I {path}` to download the LFS object
pub fn git_lfs_fetch(root: &Path, path: &Path) -> Result<()> {
    let path = path
        .to_str()
        .ok_or(anyhow::Error::msg("failed to convert path"))?;
    let root = root
        .to_str()
        .ok_or(anyhow::Error::msg("failed to convert path"))?;

    let mut cmd = Command::new("git");
    cmd.args(["-C", root, "lfs", "fetch", "-I", path]);

    println!("{:?}", cmd);

    let output = cmd.output()?;
    if output.status.success() {
        return Ok(());
    } else {
        return Err(anyhow::Error::msg("failed to get git root directory"));
    }
}
