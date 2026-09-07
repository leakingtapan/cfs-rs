use bazel_remote_apis_rs::build::bazel::remote::execution::v2::{
    Digest as ReapiDigest, Directory as ReapiDirectory,
};
use cfs::cas::blocking::{CacheClient, Client};
use cfs::cas::memory::reapi;
use cfs::cas::test_server::TestCasServer;
use cfs::hash::sha256;
use once_cell::sync::Lazy;
use prost::Message;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard};
use tempfile::TempDir;

fn env_lock() -> MutexGuard<'static, ()> {
    static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    ENV_LOCK.lock().unwrap()
}

fn run_fsx(home: &Path, cas: &TestCasServer, args: &[&str]) -> Output {
    run_fsx_at(home, cas.endpoint(), "e2e", args)
}

fn run_fsx_at(home: &Path, endpoint: &str, instance_name: &str, args: &[&str]) -> Output {
    let output = run_fsx_at_result(home, endpoint, instance_name, args);
    assert!(
        output.status.success(),
        "fsx {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_fsx_at_result(home: &Path, endpoint: &str, instance_name: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_fsx"))
        .args(args)
        .env("HOME", home)
        .env("CAS_ENDPOINT", endpoint)
        .env("INSTANCE_NAME", instance_name)
        .env("CAS_ALLOW_INSECURE_HTTP", "true")
        .env_remove("CA_CERT_PATH")
        .output()
        .expect("run fsx")
}

fn run_cascli(home: &Path, cas: &TestCasServer, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cascli"))
        .args(args)
        .env("HOME", home)
        .env("CAS_ENDPOINT", cas.endpoint())
        .env("INSTANCE_NAME", "e2e")
        .env("CAS_ALLOW_INSECURE_HTTP", "true")
        .env_remove("CA_CERT_PATH")
        .output()
        .expect("run cascli")
}

fn parse_digest(value: &str) -> ReapiDigest {
    let (hash, size) = value.trim().split_once('/').expect("digest separator");
    ReapiDigest {
        hash: hash.to_string(),
        size_bytes: size.parse().expect("digest size"),
    }
}

fn configure_client(home: &Path, cas: &TestCasServer) {
    fs::write(home.join(".rbe-auth-token"), format!("{}\n", cas.token())).unwrap();
    std::env::set_var("HOME", home);
    std::env::set_var("CAS_ENDPOINT", cas.endpoint());
    std::env::set_var("INSTANCE_NAME", "e2e");
    std::env::set_var("CAS_ALLOW_INSECURE_HTTP", "true");
    std::env::remove_var("CA_CERT_PATH");
}

struct E2eFixture {
    cas: TestCasServer,
    temp: TempDir,
    _env_guard: MutexGuard<'static, ()>,
}

impl E2eFixture {
    fn start() -> Self {
        let env_guard = env_lock();
        let cas = TestCasServer::start();
        let temp = TempDir::new().unwrap();
        configure_client(temp.path(), &cas);
        Self {
            cas,
            temp,
            _env_guard: env_guard,
        }
    }

    fn home(&self) -> &Path {
        self.temp.path()
    }

    fn fsx(&self, args: &[&str]) -> Output {
        run_fsx(self.home(), &self.cas, args)
    }

    fn fsx_result(&self, args: &[&str]) -> Output {
        run_fsx_at_result(self.home(), self.cas.endpoint(), "e2e", args)
    }

    fn cascli(&self, args: &[&str]) -> Output {
        run_cascli(self.home(), &self.cas, args)
    }

    fn create_source_tree(&self) -> SourceTree {
        let root = self.home().join("fixture");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).unwrap();
        assert!(Command::new("git")
            .args(["init", "--quiet", root.to_str().unwrap()])
            .status()
            .unwrap()
            .success());
        fs::write(root.join("hello.txt"), b"hello from cfs").unwrap();
        fs::set_permissions(root.join("hello.txt"), fs::Permissions::from_mode(0o744)).unwrap();
        fs::write(nested.join("small.bin"), [0, 1, 2, 3, 4]).unwrap();
        let large = vec![42; 3 * 1024 * 1024 + 1];
        fs::write(nested.join("large.bin"), &large).unwrap();
        fs::write(root.join(".git/ignored"), b"not uploaded").unwrap();
        symlink("hello.txt", root.join("hello-link")).unwrap();

        let lfs_content = b"content stored through git lfs";
        let lfs_hash = sha256(lfs_content);
        let lfs_object = root
            .join(".git/lfs/objects")
            .join(&lfs_hash[0..2])
            .join(&lfs_hash[2..4])
            .join(&lfs_hash);
        fs::create_dir_all(lfs_object.parent().unwrap()).unwrap();
        fs::write(&lfs_object, lfs_content).unwrap();
        fs::write(
            root.join("asset.lfs"),
            format!(
                "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {}\n",
                lfs_hash,
                lfs_content.len()
            ),
        )
        .unwrap();

        SourceTree {
            root,
            large,
            lfs_hash,
        }
    }
}

struct SourceTree {
    root: PathBuf,
    large: Vec<u8>,
    lfs_hash: String,
}

#[test]
fn cas_cli_serves_seeded_files() {
    let _env_guard = env_lock();
    let temp = TempDir::new().unwrap();
    let seed_path = temp.path().join("seed.txt");
    let download_path = temp.path().join("download.txt");
    let seed = b"seeded by cas cli";
    fs::write(&seed_path, seed).unwrap();
    fs::write(temp.path().join(".rbe-auth-token"), "cli-token\n").unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_cas"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--instance-name",
            "cli-test",
            "--token",
            "cli-token",
            "--seed-file",
            seed_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("start cas CLI");

    let mut endpoint = None;
    let mut output = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        let line = line.unwrap();
        if let Some(value) = line.strip_prefix("CAS_ENDPOINT=") {
            endpoint = Some(value.to_string());
        }
        let ready = line == "READY";
        output.push(line);
        if ready {
            break;
        }
    }

    let digest = format!("{}/{}", sha256(seed), seed.len());
    assert!(output
        .iter()
        .any(|line| line == &format!("SEEDED={}={}", seed_path.display(), digest)));
    run_fsx_at(
        temp.path(),
        endpoint.as_deref().expect("CAS endpoint"),
        "cli-test",
        &["download", download_path.to_str().unwrap(), digest.as_str()],
    );
    assert_eq!(fs::read(download_path).unwrap(), seed);

    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
    assert!(child.wait().unwrap().success());
}

#[test]
fn upload_reports_invalid_paths_and_partial_batch_failures() {
    let fixture = E2eFixture::start();

    let missing = fixture.home().join("does-not-exist");
    let output = fixture.fsx_result(&["upload", missing.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("No such file"));

    let batch = fixture.home().join("partial-batch");
    fs::create_dir(&batch).unwrap();
    let accepted_data = b"accept this batch item";
    let accepted_hash = sha256(accepted_data);
    fs::write(batch.join("accepted.bin"), accepted_data).unwrap();
    let rejected_data = b"reject this batch item";
    let rejected_hash = sha256(rejected_data);
    fs::write(batch.join("rejected.bin"), rejected_data).unwrap();
    fixture.cas.reject_batch_write(&rejected_hash);

    let output = fixture.fsx_result(&["upload", batch.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("injected batch upload failure"));
    assert_eq!(fixture.cas.blob(&accepted_hash).unwrap(), accepted_data);
    assert!(fixture.cas.blob(&rejected_hash).is_none());
}

#[test]
fn upload_preserves_tree_content_metadata_and_deduplicates() {
    let fixture = E2eFixture::start();
    let source = fixture.create_source_tree();

    let file_digest_path = fixture.home().join("file.digest");
    fixture.fsx(&[
        "upload",
        source.root.join("hello.txt").to_str().unwrap(),
        "--out",
        file_digest_path.to_str().unwrap(),
    ]);
    let file_digest = parse_digest(&fs::read_to_string(file_digest_path).unwrap());
    assert_eq!(
        fixture.cas.blob(&file_digest.hash).unwrap(),
        b"hello from cfs"
    );

    let dry_run_digest_path = fixture.home().join("dry-run.digest");
    fixture.fsx(&[
        "upload",
        source.root.to_str().unwrap(),
        "--dry-run",
        "--out",
        dry_run_digest_path.to_str().unwrap(),
    ]);
    let dry_run_digest = fs::read_to_string(dry_run_digest_path).unwrap();
    assert!(
        fixture
            .cas
            .blob(&parse_digest(&dry_run_digest).hash)
            .is_none(),
        "dry-run must not populate CAS"
    );

    let root_digest_path = fixture.home().join("root.digest");
    fixture.fsx(&[
        "upload",
        source.root.to_str().unwrap(),
        "--out",
        root_digest_path.to_str().unwrap(),
    ]);
    let root_digest = parse_digest(&fs::read_to_string(&root_digest_path).unwrap());
    assert_eq!(
        dry_run_digest.trim(),
        format!("{}/{}", root_digest.hash, root_digest.size_bytes)
    );

    let root_bytes = fixture
        .cas
        .blob(&root_digest.hash)
        .expect("uploaded root directory");
    let root = ReapiDirectory::decode(root_bytes.as_slice()).unwrap();
    assert_eq!(
        root.files
            .iter()
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>(),
        ["asset.lfs", "hello.txt"]
    );
    assert_eq!(
        root.directories
            .iter()
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>(),
        ["nested"]
    );
    assert_eq!(root.symlinks[0].name, "hello-link");
    assert_eq!(root.symlinks[0].target, "hello.txt");
    assert_eq!(
        root.files
            .iter()
            .find(|node| node.name == "hello.txt")
            .unwrap()
            .node_properties
            .as_ref()
            .and_then(|properties| properties.unix_mode),
        Some(0o100744)
    );
    assert!(fixture.cas.blob(&sha256(b"not uploaded")).is_none());
    assert_eq!(
        fixture.cas.blob(&source.lfs_hash).unwrap(),
        b"content stored through git lfs"
    );

    let nested_digest = root.directories[0].digest.as_ref().unwrap();
    let nested_bytes = fixture
        .cas
        .blob(&nested_digest.hash)
        .expect("uploaded nested directory");
    let nested = ReapiDirectory::decode(nested_bytes.as_slice()).unwrap();
    assert_eq!(
        nested
            .files
            .iter()
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>(),
        ["large.bin", "small.bin"]
    );
    assert_eq!(
        fixture.cas.blob(&sha256(&source.large)).unwrap(),
        source.large
    );

    let root_writes = fixture.cas.write_count(&root_digest.hash);
    let large_hash = sha256(&source.large);
    let large_writes = fixture.cas.write_count(&large_hash);
    fixture.fsx(&[
        "upload",
        source.root.to_str().unwrap(),
        "--out",
        root_digest_path.to_str().unwrap(),
    ]);
    assert_eq!(fixture.cas.write_count(&root_digest.hash), root_writes);
    assert_eq!(fixture.cas.write_count(&large_hash), large_writes);
}

#[test]
fn fsx_download_test_and_mount_commands_work() {
    let fixture = E2eFixture::start();
    let seeded = fixture.cas.insert_blob(b"seeded by test harness".to_vec());
    let digest = format!("{}/{}", seeded.hash, seeded.size_bytes);

    let download_path = fixture.home().join("downloaded.txt");
    fixture.fsx(&["download", download_path.to_str().unwrap(), &digest]);
    assert_eq!(fs::read(&download_path).unwrap(), b"seeded by test harness");

    let output = fixture.fsx(&["test", download_path.to_str().unwrap()]);
    assert!(String::from_utf8_lossy(&output.stdout).contains("seeded by test harness"));

    let mount_path = fixture.home().join("mount-link");
    fixture.fsx(&["mount", mount_path.to_str().unwrap(), &digest]);
    assert_eq!(
        fs::read_link(mount_path).unwrap(),
        Path::new(&format!("/home/cheng.pan/fuse/{}", digest))
    );
}

#[test]
fn cascli_inspects_blobs_directories_and_trees() {
    let fixture = E2eFixture::start();
    let seeded = fixture.cas.insert_blob(b"seeded by test harness".to_vec());
    let seeded_digest = format!("{}/{}", seeded.hash, seeded.size_bytes);
    let large = vec![42; 3 * 1024 * 1024 + 1];
    let large_digest = fixture.cas.insert_blob(large.clone());

    let nested = reapi::Directory {
        files: vec![reapi::FileNode {
            name: "large.bin".to_string(),
            digest: Some(large_digest.clone()),
            is_executable: false,
            node_properties: None,
        }],
        directories: vec![],
        symlinks: vec![],
        node_properties: None,
    };
    let nested_digest = fixture.cas.insert_directory(&nested);
    let root = reapi::Directory {
        files: vec![reapi::FileNode {
            name: "seeded.txt".to_string(),
            digest: Some(seeded.clone()),
            is_executable: false,
            node_properties: None,
        }],
        directories: vec![reapi::DirectoryNode {
            name: "nested".to_string(),
            digest: Some(nested_digest),
        }],
        symlinks: vec![reapi::SymlinkNode {
            name: "seeded-link".to_string(),
            target: "seeded.txt".to_string(),
            node_properties: None,
        }],
        node_properties: None,
    };
    let root_digest = fixture.cas.insert_directory(&root);
    let root_digest = format!("{}/{}", root_digest.hash, root_digest.size_bytes);

    let output = fixture.cascli(&["cat", &seeded_digest]);
    assert!(output.status.success());
    assert_eq!(output.stdout, b"seeded by test harness");

    let large_digest = format!("{}/{}", large_digest.hash, large_digest.size_bytes);
    let output = fixture.cascli(&["cat", &large_digest]);
    assert!(output.status.success());
    assert_eq!(output.stdout, large);

    let output = fixture.cascli(&["ls", &root_digest]);
    assert!(output.status.success());
    let listing = String::from_utf8(output.stdout).unwrap();
    assert!(listing.contains(&format!("file\tseeded.txt\t{}\n", seeded_digest)));
    assert!(listing.contains("directory\tnested\t"));
    assert!(listing.contains("symlink\tseeded-link\tseeded.txt"));

    let output = fixture.cascli(&["tree", &root_digest]);
    assert!(output.status.success());
    let tree = String::from_utf8(output.stdout).unwrap();
    assert!(tree.contains("directory\t.\t"));
    assert!(tree.contains("directory\tnested\t"));
    assert!(tree.contains("file\tnested/large.bin\t"));
    assert!(tree.contains("symlink\tseeded-link\tseeded.txt"));

    let malformed = fixture.cascli(&["cat", "invalid"]);
    assert!(!malformed.status.success());
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("HASH/SIZE"));

    let missing = format!("{}/1", "0".repeat(64));
    let missing = fixture.cascli(&["cat", &missing]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("NotFound"));
}

#[test]
fn cache_client_reuses_downloaded_blobs() {
    let fixture = E2eFixture::start();
    let seeded = fixture.cas.insert_blob(b"cached contents".to_vec());
    let directory = reapi::Directory {
        files: vec![reapi::FileNode {
            name: "seeded.txt".to_string(),
            digest: Some(seeded.clone()),
            is_executable: false,
            node_properties: None,
        }],
        directories: vec![],
        symlinks: vec![],
        node_properties: None,
    };
    let directory_digest = fixture.cas.insert_directory(&directory);

    let mut cache = CacheClient::new().unwrap();
    assert_eq!(
        cache
            .read_blob(&seeded.hash, seeded.size_bytes)
            .unwrap()
            .as_slice(),
        b"cached contents"
    );
    assert_eq!(fixture.cas.read_count(&seeded.hash), 1);
    assert_eq!(
        cache
            .read_blob(&seeded.hash, seeded.size_bytes)
            .unwrap()
            .as_slice(),
        b"cached contents"
    );
    assert_eq!(fixture.cas.read_count(&seeded.hash), 1);

    let decoded = cache
        .get_dir(&directory_digest.hash, directory_digest.size_bytes)
        .unwrap();
    assert_eq!(decoded.files[0].name, "seeded.txt");
}

#[test]
fn cas_client_reads_paginated_trees_and_writes_blobs() {
    let fixture = E2eFixture::start();
    let file = fixture.cas.insert_blob(b"tree file".to_vec());
    let empty_directory = fixture.cas.insert_directory(&reapi::Directory::default());
    let root = reapi::Directory {
        files: vec![reapi::FileNode {
            name: "mode.txt".to_string(),
            digest: Some(file),
            is_executable: true,
            node_properties: Some(reapi::NodeProperties {
                properties: vec![],
                mtime: None,
                unix_mode: Some(0o100744),
            }),
        }],
        directories: (0..17)
            .map(|index| reapi::DirectoryNode {
                name: format!("page-{:02}", index),
                digest: Some(empty_directory.clone()),
            })
            .collect(),
        symlinks: vec![],
        node_properties: None,
    };
    let root_digest = fixture.cas.insert_directory(&root);

    let mut client = Client::new().unwrap();
    let tree = client
        .get_tree(&root_digest.hash, root_digest.size_bytes)
        .unwrap();
    assert_eq!(tree.len(), 18);
    assert_eq!(
        tree[0].files[0]
            .node_properties
            .as_ref()
            .and_then(|properties| properties.unix_mode),
        Some(0o100744)
    );

    let empty_digest = ReapiDigest {
        hash: sha256(&[]),
        size_bytes: 0,
    };
    client.write_blob(&empty_digest, &[]).unwrap();
    assert_eq!(
        fixture.cas.blob(&empty_digest.hash).unwrap(),
        Vec::<u8>::new()
    );

    let path = fixture.home().join("direct-write.txt");
    fs::write(&path, b"direct client write").unwrap();
    let digest = ReapiDigest {
        hash: sha256(b"direct client write"),
        size_bytes: 19,
    };
    client.write_file(&digest, &path).unwrap();
    assert_eq!(
        fixture.cas.blob(&digest.hash).unwrap(),
        b"direct client write"
    );
}

#[test]
fn daemon_rejects_malformed_digest() {
    let fixture = E2eFixture::start();
    let output = Command::new(env!("CARGO_BIN_EXE_cfsd"))
        .args(["malformed-digest", fixture.home().to_str().unwrap()])
        .output()
        .expect("run cfsd");
    assert!(!output.status.success());
}
