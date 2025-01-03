use walkdir::WalkDir;
use anyhow::Result;
use bytes::BytesMut;
use futures::stream::{StreamExt, TryStreamExt};
use std::path::Path;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, Framed};
//use walkdir::WalkDir;

pub fn test(path: String) -> Result<()> {
    // let paths: Vec<_> = WalkDir::new(path)
    //    .into_iter()
    //    .filter_map(|e| e.ok())
    //    .filter(|e| e.file_type().is_file())
    //    .map(|e| e.into_path())
    //    .collect();

    // for path in paths {
    //    println!("{:?}", path);
    // }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(read(Path::new(&path)))
}

async fn read(path: &Path) -> Result<()> {
    let file = File::open(path).await?;
    // Map stream of `BytesMut` to stream of `Bytes`
    let mut stream = Framed::new(file, BytesCodec::new()).map_ok(BytesMut::freeze);
    while let Some(Ok(v)) = stream.next().await {
        println!("-> {:?}\n", v);
    }

    Ok(())
}
