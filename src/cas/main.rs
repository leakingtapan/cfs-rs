use anyhow::Result;
use cfs::cas::memory::MemoryCas;
use clap::Parser;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Parser)]
#[clap(name = "cas", about = "In-memory CAS service for local testing")]
struct Cli {
    /// Address on which to serve gRPC. Port 0 selects an available port.
    #[clap(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,

    /// REAPI instance name required from clients.
    #[clap(long, default_value = "memory")]
    instance_name: String,

    /// Bearer token required from clients.
    #[clap(long, default_value = "test-token")]
    token: String,

    /// File to load as a CAS blob. May be specified more than once.
    #[clap(long, parse(from_os_str))]
    seed_file: Vec<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let address = listener.local_addr()?;
    let cas = MemoryCas::new(&args.instance_name, &args.token);
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    println!("CAS_ENDPOINT=http://{}", address);
    println!("INSTANCE_NAME={}", args.instance_name);
    println!("CAS_TOKEN={}", args.token);
    for path in args.seed_file {
        let digest = cas.insert_blob(tokio::fs::read(&path).await?);
        println!(
            "SEEDED={}={}/{}",
            path.display(),
            digest.hash,
            digest.size_bytes
        );
    }
    println!("READY");
    io::stdout().flush()?;

    cas.serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
        #[cfg(unix)]
        {
            interrupt.recv().await;
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    })
    .await?;
    Ok(())
}
