use bazel_remote_apis_rs::build::bazel::remote::execution::v2::{
    Digest as ReapiDigest, Directory as ReapiDirectory,
};
use cfs::cas::blocking::{CacheClient, Client};
use cfs::cas::memory::{reapi, InMemoryCas};
use cfs::hash::sha256;
use prost::Message;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use tempfile::TempDir;

fn env_lock() -> MutexGuard<'static, ()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

fn run_fsx(home: &Path, cas: &InMemoryCas, args: &[&str]) -> Output {
    run_fsx_at(home, cas.endpoint(), "e2e", args)
}

fn run_fsx_at(home: &Path, endpoint: &str, instance_name: &str, args: &[&str]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_fsx"))
        .args(args)
        .env("HOME", home)
        .env("CAS_ENDPOINT", endpoint)
        .env("INSTANCE_NAME", instance_name)
        .env("CAS_ALLOW_INSECURE_HTTP", "true")
        .env_remove("CA_CERT_PATH")
        .output()
        .expect("run fsx");
    assert!(
        output.status.success(),
        "fsx {:?} failed\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_cascli(home: &Path, cas: &InMemoryCas, args: &[&str]) -> Output {
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

fn configure_client(home: &Path, cas: &InMemoryCas) {
    fs::write(home.join(".rbe-auth-token"), format!("{}\n", cas.token())).unwrap();
    std::env::set_var("HOME", home);
    std::env::set_var("CAS_ENDPOINT", cas.endpoint());
    std::env::set_var("INSTANCE_NAME", "e2e");
    std::env::set_var("CAS_ALLOW_INSECURE_HTTP", "true");
    std::env::remove_var("CA_CERT_PATH");
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
fn all_supported_cas_workflows() {
    let _env_guard = env_lock();
    let cas = InMemoryCas::start();
    let temp = TempDir::new().unwrap();
    configure_client(temp.path(), &cas);

    let fixture = temp.path().join("fixture");
    let nested = fixture.join("nested");
    fs::create_dir_all(&nested).unwrap();
    assert!(Command::new("git")
        .args(["init", "--quiet", fixture.to_str().unwrap()])
        .status()
        .unwrap()
        .success());
    fs::write(fixture.join("hello.txt"), b"hello from cfs").unwrap();
    fs::set_permissions(fixture.join("hello.txt"), fs::Permissions::from_mode(0o744)).unwrap();
    fs::write(nested.join("small.bin"), [0, 1, 2, 3, 4]).unwrap();
    let large = vec![42; 3 * 1024 * 1024 + 1];
    fs::write(nested.join("large.bin"), &large).unwrap();
    fs::write(fixture.join(".git/ignored"), b"not uploaded").unwrap();
    symlink("hello.txt", fixture.join("hello-link")).unwrap();

    for index in 0..17 {
        let page_dir = fixture.join(format!("page-{:02}", index));
        fs::create_dir(&page_dir).unwrap();
        fs::write(page_dir.join("value"), format!("page {}", index)).unwrap();
    }

    let lfs_content = b"content stored through git lfs";
    let lfs_hash = sha256(lfs_content);
    let lfs_object = fixture
        .join(".git/lfs/objects")
        .join(&lfs_hash[0..2])
        .join(&lfs_hash[2..4])
        .join(&lfs_hash);
    fs::create_dir_all(lfs_object.parent().unwrap()).unwrap();
    fs::write(&lfs_object, lfs_content).unwrap();
    fs::write(
        fixture.join("asset.lfs"),
        format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize {}\n",
            lfs_hash,
            lfs_content.len()
        ),
    )
    .unwrap();

    let file_digest_path = temp.path().join("file.digest");
    run_fsx(
        temp.path(),
        &cas,
        &[
            "upload",
            fixture.join("hello.txt").to_str().unwrap(),
            "--out",
            file_digest_path.to_str().unwrap(),
        ],
    );
    let file_digest = parse_digest(&fs::read_to_string(file_digest_path).unwrap());
    assert_eq!(cas.blob(&file_digest.hash).unwrap(), b"hello from cfs");

    let dry_run_digest = temp.path().join("dry-run.digest");
    run_fsx(
        temp.path(),
        &cas,
        &[
            "upload",
            fixture.to_str().unwrap(),
            "--dry-run",
            "--out",
            dry_run_digest.to_str().unwrap(),
        ],
    );
    let dry_run_digest = fs::read_to_string(dry_run_digest).unwrap();
    assert!(
        cas.blob(&parse_digest(&dry_run_digest).hash).is_none(),
        "dry-run must not populate CAS"
    );

    let root_digest_path = temp.path().join("root.digest");
    run_fsx(
        temp.path(),
        &cas,
        &[
            "upload",
            fixture.to_str().unwrap(),
            "--out",
            root_digest_path.to_str().unwrap(),
        ],
    );
    let root_digest = parse_digest(&fs::read_to_string(&root_digest_path).unwrap());
    assert_eq!(
        dry_run_digest.trim(),
        format!("{}/{}", root_digest.hash, root_digest.size_bytes)
    );

    let root_bytes = cas
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
        [
            "nested", "page-00", "page-01", "page-02", "page-03", "page-04", "page-05", "page-06",
            "page-07", "page-08", "page-09", "page-10", "page-11", "page-12", "page-13", "page-14",
            "page-15", "page-16"
        ]
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
    assert!(cas.blob(&sha256(b"not uploaded")).is_none());
    assert_eq!(cas.blob(&lfs_hash).unwrap(), lfs_content);

    let nested_digest = root.directories[0].digest.as_ref().unwrap();
    let nested_bytes = cas
        .blob(&nested_digest.hash)
        .expect("uploaded nested directory");
    let nested_directory = ReapiDirectory::decode(nested_bytes.as_slice()).unwrap();
    assert_eq!(
        nested_directory
            .files
            .iter()
            .map(|node| node.name.as_str())
            .collect::<Vec<_>>(),
        ["large.bin", "small.bin"]
    );
    assert_eq!(cas.blob(&sha256(&large)).unwrap(), large);

    let writes_before = cas.write_count(&root_digest.hash);
    let large_writes_before = cas.write_count(&sha256(&large));
    run_fsx(
        temp.path(),
        &cas,
        &[
            "upload",
            fixture.to_str().unwrap(),
            "--out",
            root_digest_path.to_str().unwrap(),
        ],
    );
    assert_eq!(
        cas.write_count(&root_digest.hash),
        writes_before,
        "existing blobs must not be uploaded again"
    );
    assert_eq!(
        cas.write_count(&sha256(&large)),
        large_writes_before,
        "existing streamed blobs must not be uploaded again"
    );

    let seeded = cas.insert_blob(b"seeded by test harness".to_vec());
    let download_path = temp.path().join("downloaded.txt");
    let seeded_digest = format!("{}/{}", seeded.hash, seeded.size_bytes);
    run_fsx(
        temp.path(),
        &cas,
        &["download", download_path.to_str().unwrap(), &seeded_digest],
    );
    assert_eq!(fs::read(download_path).unwrap(), b"seeded by test harness");

    let inspect_output = run_fsx(
        temp.path(),
        &cas,
        &["test", fixture.join("hello.txt").to_str().unwrap()],
    );
    assert!(String::from_utf8_lossy(&inspect_output.stdout).contains("hello from cfs"));

    let mount_path = temp.path().join("mount-link");
    run_fsx(
        temp.path(),
        &cas,
        &["mount", mount_path.to_str().unwrap(), &seeded_digest],
    );
    assert_eq!(
        fs::read_link(mount_path).unwrap(),
        Path::new(&format!("/home/cheng.pan/fuse/{}", seeded_digest))
    );

    let seeded_directory = reapi::Directory {
        files: vec![reapi::FileNode {
            name: "seeded.txt".to_string(),
            digest: Some(reapi::Digest {
                hash: seeded.hash.clone(),
                size_bytes: seeded.size_bytes,
            }),
            is_executable: false,
            node_properties: None,
        }],
        directories: vec![],
        symlinks: vec![],
        node_properties: None,
    };
    let seeded_directory_digest = cas.insert_directory(&seeded_directory);
    let cat_output = run_cascli(temp.path(), &cas, &["cat", &seeded_digest]);
    assert!(cat_output.status.success());
    assert_eq!(cat_output.stdout, b"seeded by test harness");

    let large_digest = format!("{}/{}", sha256(&large), large.len());
    let large_cat_output = run_cascli(temp.path(), &cas, &["cat", &large_digest]);
    assert!(large_cat_output.status.success());
    assert_eq!(large_cat_output.stdout, large);

    let seeded_directory_digest_string = format!(
        "{}/{}",
        seeded_directory_digest.hash, seeded_directory_digest.size_bytes
    );
    let ls_output = run_cascli(
        temp.path(),
        &cas,
        &["ls", seeded_directory_digest_string.as_str()],
    );
    assert!(
        ls_output.status.success(),
        "{}",
        String::from_utf8_lossy(&ls_output.stderr)
    );
    assert_eq!(
        String::from_utf8(ls_output.stdout).unwrap(),
        format!("file\tseeded.txt\t{}\n", seeded_digest)
    );

    let tree_output = run_cascli(
        temp.path(),
        &cas,
        &[
            "tree",
            &format!("{}/{}", root_digest.hash, root_digest.size_bytes),
        ],
    );
    assert!(
        tree_output.status.success(),
        "{}",
        String::from_utf8_lossy(&tree_output.stderr)
    );
    let tree_output = String::from_utf8(tree_output.stdout).unwrap();
    assert!(tree_output.contains("directory\t.\t"));
    assert!(tree_output.contains("directory\tnested\t"));
    assert!(tree_output.contains("file\tnested/large.bin\t"));
    assert!(tree_output.contains("symlink\thello-link\thello.txt"));

    let malformed = run_cascli(temp.path(), &cas, &["cat", "invalid"]);
    assert!(!malformed.status.success());
    assert!(String::from_utf8_lossy(&malformed.stderr).contains("HASH/SIZE"));

    let missing = format!("{}/1", "0".repeat(64));
    let missing = run_cascli(temp.path(), &cas, &["cat", &missing]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("NotFound"));

    let mut cache = CacheClient::new().unwrap();
    assert_eq!(
        cache
            .read_blob(&seeded.hash, seeded.size_bytes)
            .unwrap()
            .as_slice(),
        b"seeded by test harness"
    );
    assert_eq!(
        cache
            .read_blob(&seeded.hash, seeded.size_bytes)
            .unwrap()
            .as_slice(),
        b"seeded by test harness"
    );
    let decoded = cache
        .get_dir(
            &seeded_directory_digest.hash,
            seeded_directory_digest.size_bytes,
        )
        .unwrap();
    assert_eq!(decoded.files[0].name, "seeded.txt");

    let mut client = Client::new().unwrap();
    let tree = client
        .get_tree(&root_digest.hash, root_digest.size_bytes)
        .unwrap();
    assert_eq!(tree.len(), 19);
    let tree_root = &tree[0];
    assert_eq!(
        tree_root
            .files
            .iter()
            .find(|node| node.name == "hello.txt")
            .and_then(|node| node.node_properties.as_ref())
            .and_then(|properties| properties.unix_mode),
        Some(0o100744)
    );

    let empty_digest = ReapiDigest {
        hash: sha256(&[]),
        size_bytes: 0,
    };
    client.write_blob(&empty_digest, &[]).unwrap();
    assert_eq!(cas.blob(&empty_digest.hash).unwrap(), Vec::<u8>::new());

    let direct_file = temp.path().join("direct-write.txt");
    fs::write(&direct_file, b"direct client write").unwrap();
    let direct_digest = ReapiDigest {
        hash: sha256(b"direct client write"),
        size_bytes: 19,
    };
    client.write_file(&direct_digest, &direct_file).unwrap();
    assert_eq!(
        cas.blob(&direct_digest.hash).unwrap(),
        b"direct client write"
    );

    let daemon = Command::new(env!("CARGO_BIN_EXE_cfsd"))
        .args(["malformed-digest", temp.path().to_str().unwrap()])
        .output()
        .expect("run cfsd");
    assert!(!daemon.status.success());
}
