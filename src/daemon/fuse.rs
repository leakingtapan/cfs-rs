use cfs::cas::configs::Configs;
use libc::ENOSYS;
use anyhow::Result;
use cfs::cas;
use fuser::consts::FOPEN_KEEP_CACHE;
use fuser::FileType;
use fuser::{
    Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, ReplyXattr, Request,
};
use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::ops::Add;
use std::os::raw::c_int;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::str;
use std::time::{Duration, SystemTime};

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
    cas_client: cas::blocking::CacheClient,

    /// CAS digest
    hash: String,
    size: i64,

    /// inodes is the list of inodes being allocated
    inodes: HashMap<u64, Inode>,

    /// directories map inode to the list of (name, inode) entries under the directory
    /// TODO: use OsString
    directories: HashMap<u64, HashMap<OsString, Inode>>,
}

impl Cfs {
    fn new(hash: &str, size: i64, configs: Configs) -> Result<Cfs> {
        let cas_client = cas::blocking::CacheClient::new(configs)?;

        Ok(Cfs {
            cas_client: cas_client,
            hash: hash.to_string(),
            size: size,
            inodes: HashMap::new(),
            directories: HashMap::new(),
        })
    }

    fn next_inode_id(&self) -> u64 {
        self.inodes.len() as u64 + 1
    }

    /// get the contents of a directory as tree map
    /// tree map is required for readdir when buf is full and readdir is called
    /// with offset > 0. tree map guarantees the order when elements are skipped
    fn get_directory_content(&mut self, inode: u64) -> Result<BTreeMap<String, FileKind>> {
        let inode = self
            .inodes
            .get(&inode)
            .ok_or(anyhow::Error::msg("inode not found"))?;

        let dir = self.cas_client.get_dir(&inode.attr.hash, inode.attr.size)?;
        let mut entries = BTreeMap::new();
        for f in dir.files {
            entries.insert(f.name, FileKind::File);
        }
        for f in dir.directories {
            entries.insert(f.name, FileKind::Directory);
        }
        for f in dir.symlinks {
            entries.insert(f.name, FileKind::Symlink);
        }
        Ok(entries)
    }
}

impl Filesystem for Cfs {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> Result<(), c_int> {
        self.inodes.insert(
            1,
            Inode {
                inode: 1,
                attr: InodeAttr {
                    hash: self.hash.to_string(),
                    size: self.size,
                    kind: FileKind::Directory,
                    mode: 0o0770,
                },
            },
        );
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup: parent {} name {:?}", parent, name);

        if let Some(entries) = self.directories.get(&parent) {
            if let Some(inode) = entries.get(&name.to_os_string()) {
                reply.entry(&Duration::new(60, 0), &inode.clone().into(), 0);
                return;
            }
        }
        let inode = match self.inodes.get(&parent) {
            Some(inode) => inode,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let dir = match self.cas_client.get_dir(&inode.attr.hash, inode.attr.size) {
            Ok(dir) => dir,
            Err(_) => {
                reply.error(libc::ENOSYS);
                return;
            }
        };

        let next_inode_id = self.next_inode_id();
        // TODOs: fix directory node properties
        let name_str = match name.to_str() {
            Some(name) => name,
            None => {
                //TODO: better error code
                reply.error(libc::ENOSYS);
                return;
            }
        };
        let node_attr = match find_attr_by_name(dir, name_str) {
            Some(attr) => attr,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let inode = Inode {
            inode: next_inode_id,
            attr: node_attr,
        };
        reply.entry(&Duration::new(60, 0), &inode.clone().into(), 0);
        self.inodes.insert(next_inode_id as u64, inode.clone());
        match self.directories.get_mut(&parent) {
            Some(entries) => {
                entries.insert(name.to_os_string(), inode);
            }
            None => {
                let mut entries = HashMap::new();
                entries.insert(name.to_os_string(), inode);
                self.directories.insert(parent, entries);
            }
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        match self.inodes.get(&ino) {
            Some(inode) => {
                reply.attr(&Duration::new(60, 0), &inode.clone().into());
            }
            None => {
                reply.error(libc::ENOENT);
                return;
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
        assert!(offset >= 0);

        let entries = match self.get_directory_content(ino) {
            Ok(entries) => entries,
            Err(_) => {
                reply.error(libc::ENOSYS);
                return;
            }
        };

        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let (name, file_type) = entry;

            let buffer_full: bool = reply.add(
                ino,
                offset + index as i64 + 1,
                (*file_type).into(),
                OsStr::from_bytes(name.as_bytes()),
            );

            if buffer_full {
                break;
            }
        }

        reply.ok();
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
        let inode = match self.inodes.get(&inode) {
            Some(inode) => inode,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let blob = match self.cas_client.read_blob(&inode.attr.hash, inode.attr.size) {
            Ok(blob) => blob,
            Err(_) => {
                reply.error(libc::ENOSYS);
                return;
            }
        };

        let end: usize = cmp::min(inode.attr.size as usize, offset as usize + size as usize);
        reply.data(&blob[offset as usize..end]);
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

pub fn run(mountpoint: &str, hash: &str, size: i64, configs: Configs) -> Result<()> {
    if !Path::new(mountpoint).is_dir() {
        let res = fs::create_dir(mountpoint);
        if res.is_err() {
            return res
                .map_err(|e| anyhow::Error::msg(format!("failed to create mount point {}", e)));
        }
    }

    let fs = Cfs::new(hash, size, configs)?;
    // TODO: why need to edit /etc/fuse.conf to enable user_allow_others to allow autoumount?
    let mountoptions = vec![MountOption::AutoUnmount];
    fuser::mount2(fs, &mountpoint, &mountoptions).map_err(|e| e.into())
}
