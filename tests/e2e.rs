mod support;

use bazel_remote_apis_rs::build::bazel::remote::execution::v2::{
    Digest as ReapiDigest, Directory as ReapiDirectory,
};
use cfs::cas::blocking::{CacheClient, Client};
use cfs::hash::sha256;
use prost::Message;
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::{Command, Output};
use support::cas::{reapi, InMemoryCas};
use tempfile::TempDir;

fn run_fsx(home: &Path, cas: &InMemoryCas, args: &[&str]) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_fsx"))
        .args(args)
        .env("HOME", home)
        .env("CAS_ENDPOINT", cas.endpoint())
        .env("INSTANCE_NAME", "e2e")
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
    std::env::remove_var("CA_CERT_PATH");
}

#[test]
fn all_supported_cas_workflows() {
    let cas = InMemoryCas::start();
    let temp = TempDir::new().unwrap();
    configure_client(temp.path(), &cas);

    let fixture = temp.path().join("fixture");
    let nested = fixture.join("nested");
    fs::create_dir_all(fixture.join(".git")).unwrap();
    fs::create_dir_all(&nested).unwrap();
    fs::write(fixture.join("hello.txt"), b"hello from cfs").unwrap();
    fs::set_permissions(fixture.join("hello.txt"), fs::Permissions::from_mode(0o744)).unwrap();
    fs::write(nested.join("small.bin"), [0, 1, 2, 3, 4]).unwrap();
    let large = vec![42; 3 * 1024 * 1024 + 1];
    fs::write(nested.join("large.bin"), &large).unwrap();
    fs::write(fixture.join(".git/ignored"), b"not uploaded").unwrap();
    symlink("hello.txt", fixture.join("hello-link")).unwrap();

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
        ["hello.txt"]
    );
    assert_eq!(root.directories[0].name, "nested");
    assert_eq!(root.symlinks[0].name, "hello-link");
    assert_eq!(root.symlinks[0].target, "hello.txt");
    assert_eq!(
        root.files[0]
            .node_properties
            .as_ref()
            .and_then(|properties| properties.unix_mode),
        Some(0o100744)
    );
    assert!(!root.files.iter().any(|node| node.name == "ignored"));

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
        }],
        directories: vec![],
        symlinks: vec![],
    };
    let seeded_directory_digest = cas.insert_directory(&seeded_directory);
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
    assert_eq!(tree.len(), 2);

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
