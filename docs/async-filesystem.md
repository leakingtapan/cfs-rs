# Async filesystem design

## Status

Initial implementation.

## Context

`fuser` 0.9.1 dispatches requests through a synchronous `Filesystem` trait and a
non-concurrent receive loop. The current daemon calls `Runtime::block_on` from
`lookup`, `readdir`, and `read`, so a CAS network request prevents the session
from dispatching another kernel request.

The reply values passed to `Filesystem` methods are owned. A callback can move a
reply into another task, return immediately, and send the response later. This
allows asynchronous request handling without replacing `fuser`'s FUSE protocol
implementation.

## Goals

- Never call `Runtime::block_on` while dispatching a FUSE request.
- Permit unrelated CAS requests to run concurrently.
- Keep inode and directory state consistent under concurrent completions.
- Bound the number of concurrent CAS downloads.
- Coalesce concurrent cache misses for the same CAS digest.
- Preserve the existing synchronous CAS clients for CLI users.

## Non-goals

- Reimplementing the FUSE protocol or `/dev/fuse` session management.
- Defining a general-purpose async filesystem crate in the first iteration.
- Persistent or size-bounded blob caching.
- Range reads from the ByteStream service.
- Complete cancellation of an in-flight gRPC request after a FUSE interrupt.

## Architecture

The daemon remains a synchronous `fuser::Filesystem` implementation, but its
network-backed callbacks become a bridge to a Tokio runtime:

```yaml
kernel
  -> fuser Session::run
  -> synchronous callback
       1. copy borrowed request data
       2. read the required inode metadata
       3. spawn a Tokio task containing the owned reply
       4. return to fuser
  -> async cache/client
  -> reserve a download permit on a cache miss
  -> tonic CAS request
  -> update filesystem state
  -> send the FUSE reply
```

`getattr` and `open` remain synchronous because they do not perform network I/O.
Mount setup may use `block_on` once to establish the CAS client because no FUSE
session is dispatching requests at that point.

### Filesystem state

Inodes and directory entries are stored behind `Arc<std::sync::Mutex<_>>`.
Callbacks and completion tasks hold this mutex only while reading or updating
in-memory maps. They must release it before awaiting CAS I/O or sending a reply.

The initial implementation retains the existing monotonic inode allocation
behavior. A lookup completion checks the directory cache again before allocating
an inode so concurrent lookups for the same name converge on one entry.

### Async CAS cache

The async cache owns a clonable tonic ByteStream client and a shared blob map.
Each request uses a cloned client, allowing tonic/HTTP2 to multiplex requests.

A per-digest asynchronous mutex implements single-flight loading:

1. Check the blob cache.
2. Obtain the lock associated with the digest.
3. Check the cache again.
4. Fetch and insert the blob if it is still absent.

No global cache lock is held across `.await`. The cache remains unbounded,
matching existing behavior; cache eviction is a separate change.

### Backpressure

The async CAS client owns a Tokio semaphore that limits actual downloads.
Cache hits and tasks waiting for another task to load the same digest do not
consume permits. Tasks wait asynchronously for capacity instead of returning a
transient error to the application.

The initial limit is 64 concurrent CAS downloads. This is a conservative
starting point to be tuned with workload benchmarks. The kernel's FUSE request
limits provide upstream pressure on the number of outstanding replies; a
separate daemon queue can be added if measurements show that this is
insufficient.

### Errors and lifecycle

Each accepted request owns exactly one reply object. Its task sends either a
successful response or an errno. Missing inodes and entries use `ENOENT`,
invalid read offsets use `EINVAL`, and CAS failures currently use `EIO`.

The runtime is owned by the mounted filesystem and therefore lives for the
entire FUSE session. Unmounting drops the filesystem and runtime, cancelling
unfinished tasks.

## Alternatives considered

### Fork or rewrite `fuser`

Rejected for the first iteration. Parsing the FUSE ABI, negotiating kernel
capabilities, mounting, and cross-platform behavior are unrelated to async CAS
I/O. Deferred replies provide the needed concurrency while retaining upstream
protocol handling.

### One dedicated CAS actor

A single actor makes state ownership simple but serializes network operations
unless it delegates them to additional tasks. Shared short-lived state locks and
cloned tonic clients provide a smaller implementation.

### `spawn_blocking`

Rejected for CAS requests. Tonic already provides asynchronous network I/O, so
placing `block_on` inside blocking-pool jobs consumes one OS thread per request.
`spawn_blocking` remains appropriate for unavoidable blocking filesystem,
subprocess, or CPU-heavy work called from async tasks.

## Rollout

1. Add the asynchronous caching CAS client alongside the synchronous API.
2. Convert `lookup`, `readdir`, and `read` to deferred replies.
3. Add Linux integration coverage for concurrent cold reads and lookup/read
 overlap.
4. Benchmark queue depth, cache behavior, and CAS concurrency before tuning the
 initial limit.
5. Consider extracting a reusable async `fuser` adapter after the daemon's
 operation and cancellation semantics are proven.

## Validation

- Existing library, binary, and E2E tests continue to pass.
- The Linux daemon compiles with all deferred reply types required to be
  `Send + 'static`.
- Concurrent reads of different uncached digests overlap.
- Concurrent reads of one uncached digest issue one CAS download.
- Concurrent downloads do not exceed the configured limit.
