use anyhow::Result;
use cfs::cas;
use fuser::consts::FOPEN_KEEP_CACHE;
use fuser::FileType;
use fuser::{
    Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyXattr, Request,
};
use libc::{EINVAL, EIO, ENOSYS};
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::ops::Add;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::str;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;

use bazel_remote_apis_rs::build::bazel::remote::execution::v2::Directory as BazelDirectory;

#[derive(Debug, Clone)]
struct Inode {
    inode: u64,
    attr: InodeAttr,
}

#[derive(Debug, Clone)]
struct InodeAttr {
    //TODO: digest struct
    hash: String,
    size: i64,
    kind: FileKind,
    mode: u32,
}

impl From<Inode> for fuser::FileAttr {
    fn from(node: Inode) -> Self {
        // let ts = SystemTime::now();
        let ts = SystemTime::UNIX_EPOCH;
        let ts = ts.add(Duration::from_secs(1656311481));
        // let perm: u16 = match node.attr.kind {
        //     FileKind::Directory => 0o0770,
        //     FileKind::File => 0o0660,
        //     FileKind::Symlink => 0o0770,
        // };
        fuser::FileAttr {
            ino: node.inode,
            size: node.attr.size as u64,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts, //SystemTime::UNIX_EPOCH,
            kind: node.attr.kind.into(),
            perm: node.attr.mode as u16,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum FileKind {
    File,
    Directory,
    Symlink,
}

impl From<FileKind> for fuser::FileType {
    fn from(kind: FileKind) -> Self {
        match kind {
            FileKind::File => FileType::RegularFile,
            FileKind::Directory => FileType::Directory,
            FileKind::Symlink => FileType::Symlink,
        }
    }
}

fn find_attr_by_name(d: BazelDirectory, name: &str) -> Option<InodeAttr> {
    for f in d.files {
        if f.name == name {
            let digest = f.digest?;
            let node_properties = f.node_properties;
            let mode = match node_properties {
                Some(p) => match p.unix_mode {
                    Some(mode) => mode,
                    None => 0o0660,
                },
                None => 0o0660,
            };

            return Some(InodeAttr {
                size: digest.size_bytes,
                hash: digest.hash,
                kind: FileKind::File,
                mode: mode,
            });
        }
    }

    for f in d.directories {
        if f.name == name {
            let digest = f.digest?;
            return Some(InodeAttr {
                size: digest.size_bytes,
                hash: digest.hash,
                kind: FileKind::Directory,
                mode: 0o0770,
            });
        }
    }

    //for f in d.symlinks {
    //    if f.name == name {
    //        return Some(InodeAttr{
    //            size: f.digest.size_bytes,
    //            hash: f.digest.hash,
    //            kind: FileKind::Symlink,
    //        });
    //    }
    //}
    None
}

/// Cfs stands for CAS File System or content addressable file system
/// that based on Bazel remote CAS service
struct Cfs {
    cas_client: cas::async_client::CacheClient,
    runtime: Runtime,
    state: Arc<Mutex<CfsState>>,
}

struct CfsState {
    /// CAS digest
    hash: String,
    size: i64,

    /// inodes is the list of inodes being allocated
    inodes: HashMap<u64, Inode>,

    /// directories map inode to the list of (name, inode) entries under the directory
    /// TODO: use OsString
    directories: HashMap<u64, HashMap<OsString, Inode>>,
}

impl CfsState {
    fn new(hash: &str, size: i64) -> Self {
        Self {
            hash: hash.to_string(),
            size: size,
            inodes: HashMap::new(),
            directories: HashMap::new(),
        }
    }

    fn next_inode_id(&self) -> u64 {
        self.inodes.len() as u64 + 1
    }
}

impl Cfs {
    fn new(hash: &str, size: i64) -> Result<Cfs> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let cas_client = runtime.block_on(cas::async_client::CacheClient::new())?;

        Ok(Cfs {
            cas_client,
            runtime,
            state: Arc::new(Mutex::new(CfsState::new(hash, size))),
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, CfsState>> {
        self.state
            .lock()
            .map_err(|_| anyhow::Error::msg("filesystem state lock is poisoned"))
    }
}

impl Filesystem for Cfs {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        let mut state = self.state().map_err(|_| EIO)?;
        let hash = state.hash.clone();
        let size = state.size;
        state.inodes.insert(
            1,
            Inode {
                inode: 1,
                attr: InodeAttr {
                    hash,
                    size,
                    kind: FileKind::Directory,
                    mode: 0o0770,
                },
            },
        );
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup: parent {} name {:?}", parent, name);

        let parent_inode = {
            let state = match self.state() {
                Ok(state) => state,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            if let Some(entries) = state.directories.get(&parent) {
                if let Some(inode) = entries.get(&name.to_os_string()) {
                    reply.entry(&Duration::new(60, 0), &inode.clone().into(), 0);
                    return;
                }
            }

            match state.inodes.get(&parent) {
                Some(inode) => inode.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };

        let name_str = match name.to_str() {
            Some(name) => name.to_string(),
            None => {
                reply.error(EINVAL);
                return;
            }
        };
        let cas_client = self.cas_client.clone();
        let state = self.state.clone();
        let name = name.to_os_string();
        self.runtime.spawn(async move {
            let dir = match cas_client
                .get_dir(&parent_inode.attr.hash, parent_inode.attr.size)
                .await
            {
                Ok(dir) => dir,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            let node_attr = match find_attr_by_name(dir, &name_str) {
                Some(attr) => attr,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let inode = {
                let mut state = match state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        reply.error(EIO);
                        return;
                    }
                };
                if let Some(inode) = state
                    .directories
                    .get(&parent)
                    .and_then(|entries| entries.get(&name))
                {
                    inode.clone()
                } else {
                    let inode = Inode {
                        inode: state.next_inode_id(),
                        attr: node_attr,
                    };
                    state.inodes.insert(inode.inode, inode.clone());
                    state
                        .directories
                        .entry(parent)
                        .or_insert_with(HashMap::new)
                        .insert(name, inode.clone());
                    inode
                }
            };
            reply.entry(&Duration::new(60, 0), &inode.into(), 0);
        });
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        let state = match self.state() {
            Ok(state) => state,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        match state.inodes.get(&ino) {
            Some(inode) => {
                reply.attr(&Duration::new(60, 0), &inode.clone().into());
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }

    // TODO: how to directory cache
    // fn opendir(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
    //     println!("opendir(ino={} flags={})", ino, flags);
    //     reply.opened(0, FOPEN_CACHE_DIR);
    // }
    // Set the keep_cache flag to enable caching the file at page cache for subsequent file opens
    // Otherwise, the files will be read from fuse each time it's opened, this performance degradation
    // is pretty notice when execute binary commands
    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, FOPEN_KEEP_CACHE);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        println!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        let inode = {
            let state = match self.state() {
                Ok(state) => state,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            match state.inodes.get(&ino) {
                Some(inode) => inode.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };
        let cas_client = self.cas_client.clone();
        self.runtime.spawn(async move {
            let dir = match cas_client.get_dir(&inode.attr.hash, inode.attr.size).await {
                Ok(dir) => dir,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            let mut entries = BTreeMap::new();
            for file in dir.files {
                entries.insert(file.name, FileKind::File);
            }
            for directory in dir.directories {
                entries.insert(directory.name, FileKind::Directory);
            }
            for symlink in dir.symlinks {
                entries.insert(symlink.name, FileKind::Symlink);
            }

            for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
                let (name, file_type) = entry;
                if reply.add(
                    ino,
                    offset + index as i64 + 1,
                    (*file_type).into(),
                    OsStr::from_bytes(name.as_bytes()),
                ) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn read(
        &mut self,
        _req: &Request,
        inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        println!(
            "read (inode = {}, fh = {}, offset = {}, size = {})",
            inode, fh, offset, size
        );
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        let inode = {
            let state = match self.state() {
                Ok(state) => state,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            match state.inodes.get(&inode) {
                Some(inode) => inode.clone(),
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };
        let cas_client = self.cas_client.clone();
        self.runtime.spawn(async move {
            let blob = match cas_client
                .read_blob(&inode.attr.hash, inode.attr.size)
                .await
            {
                Ok(blob) => blob,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            let start = offset as usize;
            if start >= blob.len() {
                reply.data(&[]);
                return;
            }
            let end = cmp::min(blob.len(), start.saturating_add(size as usize));
            reply.data(&blob[start..end]);
        });
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        inode: u64,
        _name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        println!("getxattr (inode = {}, size = {})", inode, size);
        reply.error(ENOSYS);
    }
}

pub fn run(mountpoint: &str, hash: &str, size: i64) -> Result<()> {
    if !Path::new(mountpoint).is_dir() {
        let res = fs::create_dir(mountpoint);
        if res.is_err() {
            return res
                .map_err(|e| anyhow::Error::msg(format!("failed to create mount point {}", e)));
        }
    }

    let fs = Cfs::new(hash, size)?;
    // TODO: why need to edit /etc/fuse.conf to enable user_allow_others to allow autoumount?
    let mountoptions = vec![MountOption::AutoUnmount];
    fuser::mount2(fs, &mountpoint, &mountoptions).map_err(|e| e.into())
}
