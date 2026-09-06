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

Run the same two suites used by CI:

```sh
cargo test --locked --lib --bins
cargo test --locked --test e2e
```

To run every test target in one command:

```sh
cargo test --locked --all-targets
```

## E2E coverage

`tests/e2e.rs` covers the currently supported cfs-rs workflows:

- standalone file upload;
- recursive directory upload and deterministic dry-run digest generation;
- nested directories, symlinks, Unix file modes, and `.git` exclusion;
- missing-blob filtering and upload deduplication;
- batched small-blob and streamed large-blob uploads;
- direct client blob and file writes, including empty blobs;
- ByteStream downloads and cached reads;
- directory decoding and CAS tree traversal;
- the `fsx upload`, `download`, `mount`, and `test` subcommands; and
- invalid daemon arguments without requiring a privileged FUSE mount.

An actual FUSE mount requires Linux kernel support and privileges. CI compiles
the Linux daemon but does not mount a filesystem in its unprivileged jobs.

## In-memory CAS test data

The reusable harness is in `tests/support/cas.rs`. Start it inside a test and
populate blobs or encoded REAPI directories before invoking cfs-rs:

```rust
let cas = InMemoryCas::start();

let file_digest = cas.insert_blob(b"fixture contents".to_vec());
let directory_digest = cas.insert_directory(&directory);
```

Use `cas.endpoint()` as `CAS_ENDPOINT`. The test helper in `tests/e2e.rs` also
creates `~/.rbe-auth-token` using `cas.token()` and sets `INSTANCE_NAME`.

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
2. end-to-end tests.

Both jobs install `protoc` and use `--locked` so dependency resolution matches
`Cargo.lock`.
