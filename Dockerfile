# syntax = docker/dockerfile:1.3
FROM rust:1.57.0 as builder

WORKDIR /src/cfs
COPY . .

RUN --mount=type=cache,target=/var/cache/apt --mount=type=cache,target=/var/lib/apt \
    apt-get update -y && apt-get install fuse libfuse-dev pkg-config -y
RUN --mount=type=cache,target=/root/.cargo/registry \
    rustup component add rustfmt && cargo build --release

FROM ubuntu:22.04

RUN apt-get update -y && apt-get install fuse libfuse-dev pkg-config ca-certificates -y
COPY --from=builder /src/cfs/target/release/cfsd /usr/bin/cfsd
COPY --from=builder /src/cfs/target/release/casctl /usr/bin/casctl

ENTRYPOINT ["/usr/bin/cfsd"]
