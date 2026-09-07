fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .out_dir("src/cas/generated")
        .extern_path(
            ".google.rpc.Status",
            "::bazel_remote_apis_rs::google::rpc::Status",
        )
        .compile(
            &["tests/proto/cas.proto", "tests/proto/bytestream.proto"],
            &["tests/proto"],
        )?;
    for empty_binding in [
        "src/cas/generated/google.protobuf.rs",
        "src/cas/generated/google.rpc.rs",
    ] {
        match std::fs::remove_file(empty_binding) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
