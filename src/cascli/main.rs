use anyhow::Result;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::{Digest, Directory};
use cfs::cas::blocking::{CacheClient, Client};
use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::io;

#[derive(Parser)]
#[clap(
    name = "cascli",
    about = "Inspect objects in content-addressable storage"
)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Write a blob's raw bytes to stdout.
    Cat {
        /// CAS digest in HASH/SIZE format.
        digest: String,
    },

    /// List the immediate entries in an encoded REAPI Directory.
    Ls {
        /// Directory digest in HASH/SIZE format.
        digest: String,
    },

    /// Recursively list an encoded REAPI directory tree.
    Tree {
        /// Root directory digest in HASH/SIZE format.
        digest: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let digest = match &cli.command {
        Commands::Cat { digest } | Commands::Ls { digest } | Commands::Tree { digest } => {
            parse_digest(digest)?
        }
    };
    match cli.command {
        Commands::Cat { .. } => {
            let mut client = Client::new()?;
            client.read_blob_to(&digest.hash, digest.size_bytes, &mut io::stdout().lock())?;
        }
        Commands::Ls { .. } => {
            let mut client = CacheClient::new()?;
            let directory = client.get_dir(&digest.hash, digest.size_bytes)?;
            print_directory(&directory, "")?;
        }
        Commands::Tree { .. } => {
            let mut client = CacheClient::new()?;
            println!("directory\t.\t{}", display_digest(&digest));
            print_tree(&mut client, directory_frame(digest, String::new()))?;
        }
    }
    Ok(())
}

fn parse_digest(value: &str) -> Result<Digest> {
    let (hash, size) = value
        .split_once('/')
        .ok_or_else(|| anyhow::Error::msg("digest must use HASH/SIZE format"))?;
    if size.contains('/') {
        return Err(anyhow::Error::msg("digest must use HASH/SIZE format"));
    }
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow::Error::msg(
            "digest hash must be a 64-character hexadecimal SHA-256",
        ));
    }
    let size_bytes = size
        .parse::<i64>()
        .map_err(|_| anyhow::Error::msg("digest size must be a non-negative integer"))?;
    if size_bytes < 0 {
        return Err(anyhow::Error::msg(
            "digest size must be a non-negative integer",
        ));
    }
    Ok(Digest {
        hash: hash.to_string(),
        size_bytes,
    })
}

fn print_directory(directory: &Directory, prefix: &str) -> Result<()> {
    validate_directory(directory)?;
    for file in &directory.files {
        println!(
            "file\t{}\t{}",
            escape_field(&join_path(prefix, &file.name)),
            display_digest(required_digest(&file.digest, &file.name)?)
        );
    }
    for child in &directory.directories {
        println!(
            "directory\t{}\t{}",
            escape_field(&join_path(prefix, &child.name)),
            display_digest(required_digest(&child.digest, &child.name)?)
        );
    }
    for symlink in &directory.symlinks {
        println!(
            "symlink\t{}\t{}",
            escape_field(&join_path(prefix, &symlink.name)),
            escape_field(&symlink.target)
        );
    }
    Ok(())
}

fn validate_directory(directory: &Directory) -> Result<()> {
    let mut names = HashSet::new();
    for name in directory
        .files
        .iter()
        .map(|node| node.name.as_str())
        .chain(directory.directories.iter().map(|node| node.name.as_str()))
        .chain(directory.symlinks.iter().map(|node| node.name.as_str()))
    {
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(anyhow::Error::msg(format!(
                "entry name {:?} is not a single path segment",
                name
            )));
        }
        if !names.insert(name) {
            return Err(anyhow::Error::msg(format!(
                "directory contains duplicate entry {:?}",
                name
            )));
        }
    }
    Ok(())
}

fn print_tree(client: &mut CacheClient, root: DirectoryFrame) -> Result<()> {
    let mut pending = vec![root];
    while let Some(frame) = pending.pop() {
        let directory = client.get_dir(&frame.digest.hash, frame.digest.size_bytes)?;
        print_directory(&directory, &frame.path)?;

        for child in directory.directories.into_iter().rev() {
            let path = join_path(&frame.path, &child.name);
            let digest = required_digest(&child.digest, &path)?.clone();
            pending.push(directory_frame(digest, path));
        }
    }
    Ok(())
}

struct DirectoryFrame {
    digest: Digest,
    path: String,
}

fn directory_frame(digest: Digest, path: String) -> DirectoryFrame {
    DirectoryFrame { digest, path }
}

fn required_digest<'a>(digest: &'a Option<Digest>, name: &str) -> Result<&'a Digest> {
    digest
        .as_ref()
        .ok_or_else(|| anyhow::Error::msg(format!("entry {} has no digest", name)))
}

fn display_digest(digest: &Digest) -> String {
    format!("{}/{}", digest.hash, digest.size_bytes)
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", prefix, name)
    }
}

fn escape_field(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\r' => "\\r".chars().collect(),
            character => vec![character],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_digest() {
        let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(
            parse_digest(&format!("{}/12", hash)).unwrap(),
            Digest {
                hash: hash.to_string(),
                size_bytes: 12,
            }
        );
    }

    #[test]
    fn rejects_malformed_digest() {
        let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        for digest in [
            "abc",
            "/1",
            "abc/1",
            &format!("{}/-1", hash),
            &format!("{}/nope", hash),
            &format!("{}/1/extra", hash),
        ] {
            assert!(parse_digest(digest).is_err(), "accepted {}", digest);
        }
    }

    #[test]
    fn escapes_record_delimiters() {
        assert_eq!(escape_field("a\\b\tc\nd\r"), "a\\\\b\\tc\\nd\\r");
    }

    #[test]
    fn rejects_invalid_directory_entries() {
        let directory = Directory {
            files: vec![],
            directories: vec![],
            symlinks: vec![
                bazel_remote_apis_rs::build::bazel::remote::execution::v2::SymlinkNode {
                    name: "../outside".to_string(),
                    target: "target".to_string(),
                    node_properties: None,
                },
            ],
            node_properties: None,
        };
        assert!(validate_directory(&directory).is_err());
    }
}
