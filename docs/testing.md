# Testing cfs-rs

The project has two test layers:

- Unit and binary tests validate hashing, Git LFS pointer parsing, and that the
  binaries compile as test targets.
- End-to-end tests run the real `fsx` binary and CAS clients against an
  in-memory implementation of the Bazel Remote Execution CAS and ByteStream
  APIs.

The E2E tests do not require an external CAS, credentials, TLS certificate, or
FUSE mount.

## Prerequisites

Install Rust and the Protocol Buffers compiler (`protoc`). For example:

```sh
# macOS
brew install protobuf

# Ubuntu or Debian
sudo apt-get update
sudo apt-get install --yes protobuf-compiler
```

## Run the tests

Run the same test targets used by CI:

```sh
cargo test --locked --lib --bins
cargo test --locked --test cas_cli_e2e
cargo test --locked --test e2e
```

To run every test target in one command:

```sh
cargo test --locked --all-targets
```

## Run the in-memory CAS

Start a local CAS and optionally pre-populate it with files:

```sh
cargo run --bin cas -- \
  --listen 127.0.0.1:50051 \
  --instance-name memory \
  --token test-token \
  --seed-file examples/data/test1 \
  --seed-file examples/data/test2
```

`--listen 127.0.0.1:0` selects an available port, which is useful in test
scripts. The command prints the resolved `CAS_ENDPOINT`, `INSTANCE_NAME`,
`CAS_ALLOW_INSECURE_HTTP`, `CAS_TOKEN`, and a
`SEEDED=<path>=<hash>/<size>` line for each seeded file. It prints `READY`
after initialization and serves until Ctrl-C.

Configure cfs-rs clients with the printed values:

```sh
export CAS_ENDPOINT=http://127.0.0.1:50051
export INSTANCE_NAME=memory
export CAS_ALLOW_INSECURE_HTTP=true
printf '%s' 'test-token' > "$HOME/.rbe-auth-token"

cargo run --bin fsx -- download /tmp/test1 HASH/SIZE
```

The local service uses plaintext HTTP, so `CA_CERT_PATH` is not required.
To prevent bearer credentials from being sent to arbitrary plaintext hosts,
cfs-rs accepts HTTP only for loopback endpoints and only when
`CAS_ALLOW_INSECURE_HTTP=true` is explicitly set. Production CAS endpoints
continue to require HTTPS and a CA certificate.
Seeded data is held only in memory and is discarded when the process exits.
Run `cargo run --bin cas -- --help` for all options.

## Inspect CAS objects

With `CAS_ENDPOINT`, `INSTANCE_NAME`, and `~/.rbe-auth-token` configured, use
`cascli` to inspect blobs and REAPI directories:

```sh
# Write the exact blob bytes to stdout.
cargo run --bin cascli -- cat HASH/SIZE

# List one encoded Directory.
cargo run --bin cascli -- ls HASH/SIZE

# Recursively walk an encoded Directory tree.
cargo run --bin cascli -- tree HASH/SIZE
```

`cat` does not add formatting or a trailing newline, so redirect it when
inspecting binary data:

```sh
cargo run --bin cascli -- cat HASH/SIZE > /tmp/blob
```

`ls` and `tree` print tab-separated records:

```text
file       path/to/file       hash/size
directory  path/to/directory  hash/size
symlink    path/to/link       target
```

Backslashes, tabs, newlines, and carriage returns in paths or symlink targets
are escaped as `\\`, `\t`, `\n`, and `\r`, respectively, so each object remains
one tab-separated record.

The `ls` and `tree` commands require the supplied digest to contain an encoded
Bazel REAPI `Directory`. Missing blobs, malformed digests, and malformed
directory objects return a non-zero exit status.

## E2E coverage

`tests/e2e.rs` covers the currently supported cfs-rs workflows:

- standalone file upload;
- recursive directory upload and deterministic dry-run digest generation;
- nested directories, symlinks, Unix file modes, and `.git` exclusion;
- Git LFS pointers backed by locally populated LFS objects;
- missing-blob filtering and upload deduplication;
- batched small-blob and streamed large-blob uploads;
- direct client blob and file writes, including empty blobs;
- ByteStream downloads and cached reads;
- directory decoding, paginated CAS tree traversal, and metadata preservation;
- the `fsx upload`, `download`, `mount`, and `test` subcommands;
- standalone `cas` startup, file seeding, client access, and shutdown;
- dedicated `cas` and `cascli` process interoperability in `cas_cli_e2e`;
- `cascli` raw blob, directory listing, and recursive tree inspection; and
- invalid daemon arguments without requiring a privileged FUSE mount.

An actual FUSE mount requires Linux kernel support and privileges. CI compiles
the Linux daemon but does not mount a filesystem in its unprivileged jobs.

## In-memory CAS test data

The reusable harness is in `src/cas/memory.rs`. Start it inside a test and
populate blobs or encoded REAPI directories before invoking cfs-rs:

```rust
let cas = InMemoryCas::start();

let file_digest = cas.insert_blob(b"fixture contents".to_vec());
let directory_digest = cas.insert_directory(&directory);
```

Use `cas.endpoint()` as `CAS_ENDPOINT`. The test helper in `tests/e2e.rs` also
creates `~/.rbe-auth-token` using `cas.token()` and sets `INSTANCE_NAME`.
Because those variables are process-global, in-process client tests hold the
shared `env_lock()` guard while configuring and using them. New tests that
change the same variables must use that guard as well.

The harness validates authorization, resource names, SHA-256 digests, sizes,
stream offsets, and finalization. It also exposes:

- `blob(hash)` to inspect stored content; and
- `write_count(hash)` to assert that existing content was not uploaded again.

Keep the server strict when extending it. Protocol validation is what lets the
E2E suite catch client defects that a permissive fake would hide.

## CI

`.github/workflows/ci.yml` runs on pushes and pull requests with separate jobs
for:

1. unit and binary tests; and
2. end-to-end tests, with explicit `cas_cli_e2e` interoperability coverage
   before the broader cfs E2E target.

Both jobs install `protoc` and use `--locked` so dependency resolution matches
`Cargo.lock`.
