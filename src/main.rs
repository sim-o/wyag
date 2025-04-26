use std::{
    error::Error,
    io::Write,
    path::{Path, PathBuf},
    process::exit,
};

use clap::Parser;
use cli::{Cli, CommandObjectType, Commands};
use log::error;
use logger::SimpleLogger;
use repository::Repository;

mod cli;
mod gitobject;
mod kvlm;
mod logger;
mod pack;
mod packindex;
mod repository;
mod util;
mod hashingreader;
mod logiterator;

static LOGGER: SimpleLogger = SimpleLogger;

fn main() {
    let cli = Cli::parse();

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(cli.log_level.filter()))
        .expect("failed to set logger");

    let result: Result<_, Box<dyn Error>> = match cli.command {
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

fn ls_pack(path: &Path, packfile: String) -> Result<(), Box<dyn Error>> {
    let repository = Repository::find(path)?;
    let objects = repository.read_packfile(&packfile)?;
    objects.iter().for_each(|o| println!("object: {}", o));
    Ok(())
}

fn ls_tree(path: &Path, tree: String, recurse: bool) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(path)?;
    repo.ls_tree(&tree, recurse, Path::new("."))
}

fn hash_object(_type: CommandObjectType, file: PathBuf, write: bool) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(Path::new("."))?;
    let sha1 = repo.object_hash(&file, _type, write)?;
    println!("{}", sha1);
    Ok(())
}

fn read_object(
    repository: PathBuf,
    _object_type: CommandObjectType,
    name: String,
) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(&repository)?;
    let sha1 = repo.find_object(&name)?;
    let obj = repo.read_object(sha1)?;
    std::io::stdout().write_all(&obj.serialize())?;
    Ok(())
}

fn log(repository: PathBuf, name: String) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(&repository)?;
    let sha1 = repo.find_object(&name)?;
    for msg in repo.log_iter(sha1) {
        println!("{}", msg?);
    }
    Ok(())
}

fn init(path: PathBuf) -> Result<(), Box<dyn Error>> {
    let repo = Repository::new(&path, true)?;
    repo.init()?;
    println!("Created repository at: {}", repo.worktree.to_string_lossy());
    Ok(())
}
