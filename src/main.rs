use std::{
    error::Error,
    fmt::Display,
    io::Write,
    path::{Path, PathBuf},
    process::exit,
};

use clap::{Parser, Subcommand, ValueEnum};

mod repository;
use repository::Repository;

mod gitobject;
mod hex;
mod kvlm;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum ObjectType {
    Blob,
    Commit,
    Tag,
    Tree,
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self).to_string().to_lowercase())
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Initialise a new empty repository.
    Init {
        /// Where to create the repository.
        path: PathBuf,
    },

    /// Provide content of repository objects.
    CatObject {
        /// Specify the type.
        #[arg(value_enum)]
        object_type: ObjectType,

        /// The object to display.
        name: String,

        /// Path to repository.
        #[arg(long)]
        repository: Option<PathBuf>,
    },

    /// Compute object ID and optionally create an object from a file.
    HashObject {
        /// Specify the type.
        #[arg(short, long, default_value_t = ObjectType::Blob)]
        _type: ObjectType,

        /// Actually write the object into the database.
        #[arg(short, long)]
        write: bool,

        /// Read object from <FILE>.
        file: PathBuf,
    },

    /// Pretty-print a tree object.
    LsTree {
        /// Recurse into sub-trees.
        #[arg(short, long)]
        recurse: bool,

        /// Path to repository.
        #[arg(long)]
        repository: Option<PathBuf>,

        /// A tree-ish object.
        tree: String,
    },
}

fn main() {
    let cli = Cli::parse();

    let result: Result<_, Box<dyn std::error::Error>> = match cli.command {
        Commands::Init { path } => init(path),
        Commands::CatObject {
            object_type,
            name,
            repository,
        } => read_object(repository.unwrap_or(PathBuf::from(".")), object_type, name),
        Commands::HashObject { _type, write, file } => write_object(_type, file, write),
        Commands::LsTree {
            recurse,
            tree,
            repository,
        } => ls_tree(&repository.unwrap_or(PathBuf::new()), tree, recurse),
    };

    if let Err(error) = result {
        println!("Error: {}", error);
        exit(1);
    }
}

fn ls_tree(path: &Path, tree: String, recurse: bool) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(path)?;
    repo.ls_tree(&tree, recurse, Path::new("."))
}

fn write_object(_type: ObjectType, file: PathBuf, write: bool) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(Path::new("."))?;
    let sha1 = repo.object_hash(&file, _type, write)?;
    println!("{}", sha1);
    Ok(())
}

fn read_object(
    repository: PathBuf,
    object_type: ObjectType,
    name: String,
) -> Result<(), Box<dyn Error>> {
    let repo = Repository::find(&repository)?;
    let sha1 = repo.find_object(object_type, &name)?;
    let obj = repo.read_object(&sha1)?;
    std::io::stdout().write_all(&obj.serialize())?;
    Ok(())
}

fn init(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let repo = Repository::new(&path, true)?;
    repo.init()?;
    println!("Created repository at: {}", repo.worktree.to_string_lossy());
    Ok(())
}
