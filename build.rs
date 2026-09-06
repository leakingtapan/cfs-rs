fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=tests/proto/cas.proto");
    println!("cargo:rerun-if-changed=tests/proto/bytestream.proto");

    tonic_build::configure().build_client(false).compile(
        &["tests/proto/cas.proto", "tests/proto/bytestream.proto"],
        &["tests/proto"],
    )?;
    Ok(())
}
