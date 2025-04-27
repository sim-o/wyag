use anyhow::Context;
use clap::Parser;
use cli::{Cli, CommandObjectType, Commands};
use hex::ToHex;
use log::error;
use logger::SimpleLogger;
use repository::Repository;
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::exit,
};

mod cli;
mod gitobject;
mod hashingreader;
mod kvlm;
mod logger;
mod logiterator;
mod pack;
mod packindex;
mod repository;
mod util;

static LOGGER: SimpleLogger = SimpleLogger;

fn main() {
    let cli = Cli::parse();

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(cli.log_level.filter()))
        .expect("failed to set logger");

    let result: anyhow::Result<()> = match cli.command {
        Commands::Init { path } => init(path),
        Commands::CatObject {
            object_type,
            name,
            repository,
        } => read_object(repository.unwrap_or(PathBuf::from(".")), object_type, name),
        Commands::HashObject { _type, write, file } => hash_object(_type, file, write),
        Commands::LsTree {
            recurse,
            tree,
            repository,
        } => ls_tree(&repository.unwrap_or(PathBuf::new()), tree, recurse),
        Commands::LsPack {
            repository,
            packfile,
        } => ls_pack(&repository.unwrap_or(PathBuf::new()), packfile),
        Commands::Log {
            repository,
            reference,
        } => log(repository.unwrap_or(PathBuf::new()), reference),
    };

    if let Err(error) = result {
        error!("Error: {}", error);
        exit(1);
    }
}

fn ls_pack(path: &Path, packfile: String) -> anyhow::Result<()> {
    let repository = Repository::find(path)
        .with_context(|| format!("loading repository at {}", path.to_string_lossy()))?;
    let objects = repository.read_packfile(&packfile)
        .with_context(|| format!("reading packfile {}", packfile))?;
    for o in objects.iter() {
        println!("object: {}", o);
    }
    Ok(())
}

fn ls_tree(path: &Path, tree: String, recurse: bool) -> anyhow::Result<()> {
    let repo = Repository::find(path)
        .context("loading repository")?;
    repo.ls_tree(&tree, recurse, Path::new("."))
        .context("reading tree")
}

fn hash_object(_type: CommandObjectType, file: PathBuf, write: bool) -> anyhow::Result<()> {
    let repo = Repository::find(Path::new(".")).context("loading repository")?;
    let sha1 = repo
        .object_hash(&file, _type, write)
        .context("hashing file")?;
    println!("{}", sha1.encode_hex::<String>());
    Ok(())
}

fn read_object(
    repository: PathBuf,
    _object_type: CommandObjectType,
    name: String,
) -> anyhow::Result<()> {
    let repo = Repository::find(&repository)
        .with_context(|| format!("loading repository at {}", repository.to_string_lossy()))?;
    let sha1 = repo
        .find_object(&name)
        .with_context(|| format!("finding object {}", name))?;
    let obj = repo
        .read_object(sha1)
        .with_context(|| format!("reading object {}", sha1.encode_hex::<String>()))?;
    std::io::stdout()
        .write_all(&obj.serialize())
        .context("writing serialized object to stdout")?;
    Ok(())
}

fn log(repository: PathBuf, name: String) -> anyhow::Result<()> {
    let repo = Repository::find(&repository)
        .with_context(|| format!("finding repository at {}", repository.to_string_lossy()))?;
    let sha1 = repo
        .find_object(&name)
        .with_context(|| format!("finding object {}", name))?;
    for msg in repo.log_iter(sha1) {
        println!("{}", msg.context("reading logs")?);
    }
    Ok(())
}

fn init(path: PathBuf) -> anyhow::Result<()> {
    let repo = Repository::new(&path, true)
        .with_context(|| format!("finding repository at {}", path.to_string_lossy()))?;
    repo.init().context("initialising repository")?;
    println!("Created repository at: {}", repo.worktree.to_string_lossy());
    Ok(())
}
