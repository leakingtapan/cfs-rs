use cfs::hash::sha256;
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use tempfile::TempDir;

struct ServerProcess(Option<Child>);

impl ServerProcess {
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("CAS process is running")
    }

    fn stop(mut self) {
        let mut child = self.0.take().unwrap();
        unsafe {
            libc::kill(child.id() as i32, libc::SIGINT);
        }
        assert!(child.wait().unwrap().success());
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn cascli_inspects_the_standalone_cas() {
    let temp = TempDir::new().unwrap();
    let seed_path = temp.path().join("seed.txt");
    let seed = b"cas and cascli interoperability";
    fs::write(&seed_path, seed).unwrap();
    fs::write(temp.path().join(".rbe-auth-token"), "interop-token\n").unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_cas"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--instance-name",
            "interop",
            "--token",
            "interop-token",
            "--seed-file",
            seed_path.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .spawn()
        .expect("start cas");
    let mut server = ServerProcess(Some(child));

    let stdout = server.child_mut().stdout.take().unwrap();
    let mut endpoint = None;
    let mut startup = Vec::new();
    for line in BufReader::new(stdout).lines() {
        let line = line.unwrap();
        if let Some(value) = line.strip_prefix("CAS_ENDPOINT=") {
            endpoint = Some(value.to_string());
        }
        let ready = line == "READY";
        startup.push(line);
        if ready {
            break;
        }
    }

    let digest = format!("{}/{}", sha256(seed), seed.len());
    assert!(startup
        .iter()
        .any(|line| line == &format!("SEEDED={}={}", seed_path.display(), digest)));
    let endpoint = endpoint.expect("cas reports its endpoint");

    let insecure_without_opt_in = Command::new(env!("CARGO_BIN_EXE_cascli"))
        .args(["cat", &digest])
        .env("HOME", temp.path())
        .env("CAS_ENDPOINT", &endpoint)
        .env("INSTANCE_NAME", "interop")
        .env_remove("CAS_ALLOW_INSECURE_HTTP")
        .env_remove("CA_CERT_PATH")
        .output()
        .expect("run cascli without insecure HTTP opt-in");
    assert!(!insecure_without_opt_in.status.success());
    assert!(String::from_utf8_lossy(&insecure_without_opt_in.stderr)
        .contains("CAS_ALLOW_INSECURE_HTTP=true"));

    let output = run_cascli(temp.path(), &endpoint, &["cat", &digest]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, seed);

    let missing = format!("{}/1", "0".repeat(64));
    let output = run_cascli(temp.path(), &endpoint, &["cat", &missing]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NotFound"));

    server.stop();
}

fn run_cascli(home: &std::path::Path, endpoint: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cascli"))
        .args(args)
        .env("HOME", home)
        .env("CAS_ENDPOINT", endpoint)
        .env("INSTANCE_NAME", "interop")
        .env("CAS_ALLOW_INSECURE_HTTP", "true")
        .env_remove("CA_CERT_PATH")
        .output()
        .expect("run cascli")
}
