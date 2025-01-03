use anyhow::Result;
use clap::{Parser, Subcommand};

mod cmds;

/// A fictional versioning CLI
#[derive(Parser)]
#[clap(name = "fsx")]
#[clap(about = "FSx client side utility", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Push file or directory to CAS
    #[clap(arg_required_else_help = true, visible_alias = "push")]
    Upload {
        /// The path to the file or directory to be uploaded
        path: String,

        /// The optional output path to write the root digest
        #[clap(short, long)]
        out: Option<String>,

        /// Generate the root digest without the actual upload
        #[clap(long)]
        dry_run: bool,
    },

    /// Download file or directory from CAS
    #[clap(arg_required_else_help = true)]
    Download {
        /// The path to the file or directory to be downloaded
        path: String,

        /// The digest of the content
        digest: String,
    },

    /// Mount the source 
    #[clap(arg_required_else_help = true)]
    Mount {
        /// The path to mount the source tree
        path: String,

        /// The tree root digest of the source tree
        digest: String,
    },


    Test {
        path: String,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();

    match args.command {
        Commands::Upload { path, out, dry_run } => cmds::upload(path, out, dry_run),
        Commands::Download { path, digest } => cmds::download(path, digest),
        Commands::Mount { path, digest } => cmds::mount(path, digest),
        Commands::Test { path } => cmds::test(path),
    }
}
