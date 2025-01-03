use super::upload::BlobUploader;
use anyhow::Result;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::{
    Digest, Directory, DirectoryNode, FileNode, NodeProperties, SymlinkNode,
};
use cfs::hash::{sha256, sha256_read};
use cfs::lfs::LfsFile;
use prost::Message;
use rayon::prelude::*;
use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::SeekFrom;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct FileDigest {
    path: PathBuf,
    digest: Digest,
}

impl FileDigest {
    pub fn new(path: &Path) -> Result<FileDigest> {
        let mut file = File::open(path)
            .map_err(|e| anyhow::Error::msg(format!("failed to read path: {:?} {:?}", path, e)))?;

        if let Ok(lfs_file) = LfsFile::new(&mut file) {
            return Ok(FileDigest {
                // TODO: fix the path to use git lfs storage path
                // and fetch the object when it is missing with
                //   git lfs fetch -I {path}
                path: path.to_path_buf(),
                digest: Digest {
                    hash: lfs_file.hash,
                    size_bytes: lfs_file.size,
                },
            });
        }
        // reset the read offset to 0
        file.seek(SeekFrom::Start(0))?;
        let (hash, size) = sha256_read(&mut file)?;
        Ok(FileDigest {
            path: path.to_path_buf(),
            digest: Digest {
                hash: hash,
                size_bytes: size as i64,
            },
        })
    }
}

pub struct Traverse {
    /// map from file path to hash
    digests: HashMap<OsString, Digest>,
    /// Uploader uploads the blobs
    uploader: Box<dyn BlobUploader>,
}

impl Traverse {
    pub fn new(uploader: Box<dyn BlobUploader>) -> Result<Traverse> {
        //let cas_client = blocking::Client::new()?;

        Ok(Traverse {
            digests: HashMap::new(),
            //cas_client: cas_client,
            uploader: uploader,
        })
    }

    /// calculate the root directory's digest
    pub fn root_digest(&mut self, path: &Path) -> Result<Digest> {
        // calculate the digests for file in parallel
        let digests = self.load_file_hashs(path)?;
        self.digests = digests;

        let dir = self.create_directory(&path.into())?;
        let digest = self.create_directory_digest(dir)?;

        Ok(digest)
    }

    fn load_file_hashs(&mut self, path: &Path) -> Result<HashMap<OsString, Digest>> {
        // TODO: consolidate the list in two places
        let ignored_file_names = vec![".git"];
        let paths: Vec<_> = WalkDir::new(path)
            .into_iter()
            .filter_entry(|e| {
                !e.file_name()
                    .to_str()
                    .map(|s| ignored_file_names.contains(&s))
                    .unwrap_or(false)
            })
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| e.into_path())
            .collect();

        let files: Vec<_> = paths.par_iter().map(|p| FileDigest::new(p)).collect();
        let mut res = HashMap::new();
        for file in files {
            let f = file?;
            res.insert(f.path.into_os_string(), f.digest);
        }

        // upload the blobs
        for (path, digest) in &res {
            self.uploader.upload_file(digest, Path::new(path));
        }

        Ok(res)
    }

    fn create_directory_node(&mut self, name: String, dir: Directory) -> Result<DirectoryNode> {
        let digest = self.create_directory_digest(dir)?;

        Ok(DirectoryNode {
            name: name,
            digest: Some(digest),
        })
    }

    fn create_directory_digest(&mut self, dir: Directory) -> Result<Digest> {
        let mut buff = vec![];
        dir.encode(&mut buff)?;
        let hash = sha256(&buff);
        let size = buff.len() as i64;
        let digest = Digest {
            hash: hash,
            size_bytes: size,
        };

        self.uploader.upload_blob(&digest, buff);

        Ok(digest)
    }

    fn create_file_node(&self, path: &PathBuf) -> Result<FileNode> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.to_string())
            .ok_or(anyhow::Error::msg("failed to get file name"))?;

        let f = File::open(path)?;
        let md = f.metadata()?;
        let permissions = md.permissions();
        // let timestamp = md.modified()?;

        let node_properties = NodeProperties {
            properties: vec![],
            mtime: None,
            unix_mode: Some(permissions.mode()),
        };

        self.digests
            .get(path.as_os_str())
            .map(|d| FileNode {
                name: name,
                digest: Some(d.clone()),
                is_executable: true,
                node_properties: Some(node_properties),
            })
            .ok_or(anyhow::Error::msg(format!(
                "digest for path {:?} not found",
                path
            )))

        //let mut file = File::open(path)?;
        //let (hash, size) = sha256_read(&mut file)?;

        //Ok(FileNode {
        //    name: name.to_string(),
        //    digest: Some(Digest {
        //        hash: hash,
        //        size_bytes: size as i64,
        //    }),
        //    is_executable: true,
        //    node_properties: None,
        //})
    }

    fn create_symlink_node(&self, path: &PathBuf) -> Result<SymlinkNode> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.to_string())
            .ok_or(anyhow::Error::msg("failed to get file name"))?;
        let link_to = fs::read_link(&path)?;

        Ok(SymlinkNode {
            name: name,
            target: String::from(link_to.to_string_lossy()),
            node_properties: None,
        })
    }

    fn create_directory(&mut self, path: &PathBuf) -> Result<Directory> {
        let ignored_file_names = vec![".git"];
        let mut files = vec![];
        let mut directories = vec![];
        let mut symlinks = vec![];
        for entry in fs::read_dir(path)? {
            if let Ok(entry) = entry {
                let path = entry.path();
                let file_name = path.file_name().and_then(|n| n.to_str()).unwrap();

                let mut found = false;
                for name_to_skip in &ignored_file_names {
                    if file_name == name_to_skip.to_string() {
                        found = true;
                        break;
                    }
                }
                if found {
                    //println!("Skip {} from {}", file_name, path.display());
                    continue;
                }

                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_dir() {
                        if let Some(name) = path.file_name() {
                            if let Some(name) = name.to_str() {
                                let dir = self.create_directory(&path)?;
                                match self.create_directory_node(name.to_string(), dir) {
                                    Ok(dir_node) => directories.push(dir_node),
                                    Err(e) => println!("create_directory_node failed {}", e),
                                }
                            }
                        }
                    } else if file_type.is_file() {
                        match self.create_file_node(&path) {
                            Ok(file_node) => files.push(file_node),
                            Err(e) => println!("create_file_node failed {}", e),
                        };
                    } else if file_type.is_symlink() {
                        match self.create_symlink_node(&path) {
                            Ok(link_node) => symlinks.push(link_node),
                            Err(e) => println!("create_symlink_node failed {}", e),
                        };
                    } else {
                        println!("unknow path type: {:?}", path);
                    }
                }
            }
        }

        files.sort_by(|f1, f2| f1.name.cmp(&f2.name));
        directories.sort_by(|f1, f2| f1.name.cmp(&f2.name));
        symlinks.sort_by(|f1, f2| f1.name.cmp(&f2.name));

        let f = File::open(path)?;
        let md = f.metadata()?;
        let permission = md.permissions();
        let node_properties = NodeProperties {
            properties: vec![],
            mtime: None,
            unix_mode: Some(permission.mode()),
        };

        Ok(Directory {
            files: files,
            directories: directories,
            symlinks: symlinks,
            node_properties: Some(node_properties),
        })
    }
}
