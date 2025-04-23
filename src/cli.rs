use clap::{Parser, Subcommand, ValueEnum};
use log::LevelFilter;
use std::fmt::Display;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    /// Set the log level.
    #[arg(short, long, default_value_t = CommandLogLevel::Off)]
    pub log_level: CommandLogLevel,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CommandLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl CommandLogLevel {
    pub fn filter(&self) -> LevelFilter {
        match self {
            CommandLogLevel::Trace => LevelFilter::Trace,
            CommandLogLevel::Debug => LevelFilter::Debug,
            CommandLogLevel::Info => LevelFilter::Info,
            CommandLogLevel::Warn => LevelFilter::Warn,
            CommandLogLevel::Error => LevelFilter::Error,
            CommandLogLevel::Off => LevelFilter::Off,
        }
    }
}

impl Display for CommandLogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CommandLogLevel::Trace => "trace",
            CommandLogLevel::Debug => "debug",
            CommandLogLevel::Info => "info",
            CommandLogLevel::Warn => "warn",
            CommandLogLevel::Error => "error",
            CommandLogLevel::Off => "off",
        };
        f.write_str(name)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum CommandObjectType {
    Blob,
    Commit,
    Tag,
    Tree,
}

impl Display for CommandObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            CommandObjectType::Blob => "blob",
            CommandObjectType::Commit => "commit",
            CommandObjectType::Tag => "tag",
            CommandObjectType::Tree => "tree",
        };
        f.write_str(name)
    }
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialise a new empty repository.
    Init {
        /// Where to create the repository.
        path: PathBuf,
    },

    /// Provide content of repository objects.
    CatObject {
        /// Specify the type.
        #[arg(value_enum)]
        object_type: CommandObjectType,

        /// The object to display.
        name: String,

        /// Path to repository.
        #[arg(long)]
        repository: Option<PathBuf>,
    },

    /// Compute object ID and optionally create an object from a file.
    HashObject {
        /// Specify the type.
        #[arg(short, long, default_value_t = CommandObjectType::Blob)]
        _type: CommandObjectType,

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

    /// Describe a pack file.
    LsPack {
        /// Path to repository.
        #[arg(long)]
        repository: Option<PathBuf>,

        /// A packfile sha.
        packfile: String,
    },
}
