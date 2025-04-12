use std::{error::Error, path::PathBuf, process::exit};

use clap::{Parser, Subcommand};

mod repository;
use repository::Repository;

mod gitobject;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialise a new empty repository.
    Init {
        /// Where to create the repository.
        path: PathBuf,
    },

    ReadObject {
        /// Repository path to inspect.
        path: PathBuf,

        /// Object hash.
        sha1: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let result: Result<_, Box<dyn std::error::Error>> = match cli.command {
        Commands::Init { path } => init(path),
        Commands::ReadObject { path, sha1 } => read_object(path, sha1),
    };

    if let Err(error) = result {
        println!("Error: {}", error);
        exit(1);
    }

    println!("Success!");
}

fn read_object(path: PathBuf, sha1: String) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(&path)?;
    let obj = repo.object_read(&sha1)?;
    println!("{}", &*obj);
    Ok(())
}

fn init(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let repo = Repository::new(&path, true)?;
    repo.init()
}
