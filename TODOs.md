# TODOs
# CFS
- [ ] handle symlink
- [ ] implement read API with offset and size
- [ ] mount a single file as a directory with only one file
- [ ] add proper logging
- [ ] debug grpc server for inode lookup
- [ ] add progress bar
- [ ] fix directory entry size to match 4k for regular dir size
- [ ] make instance name configurable
- [ ] add signal handler for ctrl-C
- [ ] propergate errors from upload thread
- [ ] splice.read / splice.write / splice.move

## Write operations
- [ ] add support for write API
- [ ] overlay FS works on top of FUSE?

## Optimiation
- [ ] slow on exec large binary
- [ ] fuse2 vs fuse3
- [ ] optimize with `unix_digest_hash_attribute_name` https://github.com/bazelbuild/bazel/issues/12158

## cfs browser
- [ ] cas-browser to view the file tree from a browser

# Bazel Digest Rule
- [ ] Test out minimum setup for digest rule
- [ ] Make digest rule a rule library
- [ ] Verify the tar package rule and if it uses `exec` config or `host` config

