use super::traverse::Traverse;
use anyhow::Result;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::Digest;
use cfs::cas::blocking;
use cfs::hash::sha256_read;
use std::fs::{self, File};
use std::path::Path;
use tokio::sync::mpsc;

/// Uploads the path to CAS. The path being a file or a directory.
///
/// As a result of the upload, creates the sha256 hash for a given path
/// following the bazel remote api direcotry's canonicalized structure
/// [https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L789]
pub fn upload<P: AsRef<Path>>(path: P, out: Option<P>, dry_run: bool) -> Result<()> {
    // Since receiver shutdown depends on all senders being out of scope,
    // need to create the receiver independent of the uploader (which uses sender)
    // to avoid cyclic dependency when joining the handle
    // Create the receiver regardless for now. Optimize later.
    let (send, handle) = blocking::spawn_receiver();
    let uploader: Box<dyn BlobUploader> = if dry_run {
        Box::new(NoopBlobUploader {})
    } else {
        Box::new(CasBlobUploader::new(send)?)
    };

    let path = path.as_ref();
    //println!("Uploading {}", path.display());

    let digest = if path.is_dir() {
        upload_dir(uploader, path)
    } else if path.is_file() {
        upload_file(uploader, path)
    } else {
        Err(anyhow::Error::msg("unsupported file type"))
    }?;

    let res = handle.join();
    if res.is_err() {
        return Err(anyhow::Error::msg(format!(
            "failed to join handle {:?}",
            res.unwrap_err()
        )));
    }

    let digest_str = format!("{}/{}", digest.hash, digest.size_bytes);
    match out {
        Some(out_path) => {
            //println!("Writing at {}", out_path.as_ref().display());
            fs::write(out_path, digest_str)
                .map_err(|e| anyhow::Error::msg(format!("failed to write digest {}", e)))
        }
        None => {
            println!("{}", digest_str);
            Ok(())
        }
    }
}

fn upload_dir(uploader: Box<dyn BlobUploader>, path: &Path) -> Result<Digest> {
    let mut t = Traverse::new(uploader)?;
    t.root_digest(path)
}

fn upload_file(mut uploader: Box<dyn BlobUploader>, path: &Path) -> Result<Digest> {
    let mut f = File::open(path)?;
    let (hash, size) = sha256_read(&mut f)?;
    let digest = Digest {
        hash: hash,
        size_bytes: size as i64,
    };
    uploader.upload_file(&digest, path).map(|_v| digest)
}

/// BlobkUpload is the trait for uploading blobs
pub trait BlobUploader {
    /// Upload a blob to the backend storage
    fn upload_blob(&mut self, digest: &Digest, buff: Vec<u8>) -> Result<()>;

    /// Upload a file to the backend storage given the file path
    fn upload_file(&mut self, digest: &Digest, path: &Path) -> Result<()>;
}

pub struct NoopBlobUploader {}

impl BlobUploader for NoopBlobUploader {
    fn upload_blob(&mut self, digest: &Digest, _: Vec<u8>) -> Result<()> {
        //println!("skip upload blob {:?}", digest);
        Ok(())
    }

    fn upload_file(&mut self, _: &Digest, path: &Path) -> Result<()> {
        //println!("skip upload file {:?}", path);
        Ok(())
    }
}

/// CasBlobUploader uploads blobs to CAS
pub struct CasBlobUploader {
    /// cas client
    cas_client: blocking::NonBlockingClient,
}

impl CasBlobUploader {
    fn new(send: mpsc::Sender<blocking::WriteTask>) -> Result<CasBlobUploader> {
        let cas_client = blocking::NonBlockingClient::new(send)?;
        Ok(CasBlobUploader {
            cas_client: cas_client,
        })
    }
}

impl BlobUploader for CasBlobUploader {
    fn upload_blob(&mut self, digest: &Digest, buff: Vec<u8>) -> Result<()> {
        self.cas_client.write_blob(digest, buff)
    }

    fn upload_file(&mut self, digest: &Digest, path: &Path) -> Result<()> {
        self.cas_client.write_file(digest, path)
    }
}
