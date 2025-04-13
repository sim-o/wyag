extern crate libflate;
extern crate sha1;

use bytes::BufMut;
use sha1::{Digest, Sha1};

use std::{
    error::Error,
    fs::{File, create_dir_all},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::from_utf8,
};

use configparser::ini::Ini;
use libflate::zlib::{Decoder, Encoder};

use crate::{
    ObjectType,
    gitobject::{BlobObject, CommitObject, GitObject, TreeObject},
    hex::hex,
};

pub struct Repository {
    pub worktree: PathBuf,
    gitdir: PathBuf,
    conf: Option<Ini>,
}

impl Repository {
    pub fn new(path: &Path, force: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let gitdir = path.join(".git");
        let config_file = gitdir.join("config");

        let conf = if config_file.is_file() {
            let file = File::open(config_file)?;
            let mut reader = BufReader::new(file);
            let mut conf = Ini::new();
            let mut config = String::new();
            reader.read_to_string(&mut config)?;
            conf.read(config)?;

            let vers = conf
                .get("core", "repositoryformatversion")
                .ok_or("version string does not exist".to_string())?;
            let vers = vers.parse::<i32>()?;
            if vers != 0 {
                return Err(format!("Unsupported repositoryformatversion: {}", vers))?;
            }

            Some(conf)
        } else {
            None
        };

        if conf.is_none() && !force {
            return Err("config file does not exist".to_string())?;
        }

        Ok(Self {
            worktree: path.into(),
            gitdir,
            conf,
        })
    }

    pub fn find(orig: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let mut path = if orig.is_absolute() {
            orig
        } else {
            &std::env::current_dir()?.join(orig)
        };

        while !path.join(".git").is_dir() {
            path = path
                .parent()
                .ok_or_else(|| format!("{} is not a repository!", orig.to_string_lossy()))?;
        }

        Self::new(path, false)
    }

    /// Compute path under repo gitdir
    fn repo_path(&self, path: &Path) -> PathBuf {
        self.gitdir.join(path)
    }

    fn repo_mkdir(&self, path: &Path) -> Option<PathBuf> {
        let repo_path = self.gitdir.join(path);
        create_dir_all(&repo_path).ok()?;
        Some(repo_path)
    }

    fn repo_file(&self, path: &Path, mkdir: bool) -> Option<PathBuf> {
        let file_path = self.repo_path(path);
        if let Some(parent) = file_path.parent() {
            if mkdir {
                create_dir_all(parent).ok()?;
            } else if !parent.is_dir() {
                return None;
            }
        }
        Some(file_path)
    }

    pub fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.worktree.exists() {
            if !self.worktree.is_dir() {
                Err(format!(
                    "{} is not a directory!",
                    self.worktree.to_string_lossy()
                ))?;
            }
            if self.gitdir.exists() {
                if !self.gitdir.is_dir() {
                    Err(format!(
                        "{} exists and is not a directory!",
                        self.gitdir.to_string_lossy(),
                    ))?;
                }
                if self.gitdir.read_dir()?.next().is_some() {
                    Err(format!(
                        "{} exists but is not empty!",
                        self.gitdir.to_string_lossy()
                    ))?;
                }
            }
        } else {
            println!("Creating worktree: {}", self.worktree.to_string_lossy());
            create_dir_all(&self.worktree)?;
        }

        for p in ["branches", "objects", "refs/tags", "refs/heads"] {
            self.repo_mkdir(Path::new(p))
                .ok_or(format!("could not create {} directory", p))?;
        }

        let files = [
            (
                "description",
                "Unnamed repository; edit this file 'description' to name the repository.\n",
            ),
            ("HEAD", "ref: refs/heads/master\n"),
            ("config", &default_config().writes()),
        ];

        for (f, contents) in files {
            let file = File::create_new(self.gitdir.join(f))?;
            std::io::BufWriter::new(file).write_all(contents.as_bytes())?;
        }

        Ok(())
    }

    pub fn read_object(&self, sha: &str) -> Result<GitObject, Box<dyn std::error::Error>> {
        let path = self
            .repo_file(&Path::new("objects").join(&sha[..2]).join(&sha[2..]), false)
            .ok_or(format!("Could not load object {}", sha))?;
        if !path.is_file() {
            Err(format!("file {} does not exist", path.to_string_lossy()))?;
        }

        let mut file = File::open(path)?;
        let mut decoder =
            Decoder::new(&mut file).map_err(|e| format!("Error decompressing: {}", e))?;
        let mut raw: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut raw)?;

        let type_idx = raw
            .iter()
            .position(|&b| b == b' ')
            .ok_or("object corrupt: missing type")?;

        let size_idx = raw
            .iter()
            .skip(type_idx)
            .position(|&b| b == b'\x00')
            .ok_or("object corrupt: missing size")?
            + type_idx;

        let size = std::str::from_utf8(&raw[type_idx + 1..size_idx])?.parse::<usize>()?;
        if size != raw.len() - size_idx - 1 {
            Err(format!(
                "object corrupt: size {} does not match expected {}",
                size,
                raw.len() - size_idx - 1,
            ))?;
        }

        let object_type = &raw[..type_idx];
        let data = Vec::from(&raw[size_idx + 1..]);
        let result = match object_type {
            b"blob" => GitObject::Blob(BlobObject::from(data)),
            b"commit" => GitObject::Commit(CommitObject::from(&data)?),
            b"tree" => GitObject::Tree(TreeObject::from(&data)?),
            _ => todo!(
                "unhandled type: {}",
                from_utf8(object_type).unwrap_or("--unknown--")
            ),
        };

        Ok(result)
    }

    pub fn find_object(&self, _: ObjectType, name: String) -> Result<String, Box<dyn Error>> {
        Ok(name)
    }

    pub fn write_object(&self, obj: &GitObject, write: bool) -> Result<String, Box<dyn Error>> {
        let bytes = {
            let serialized = obj.serialize();
            let mut writer = Vec::new().writer();
            writer.write_all(obj.name())?;
            writer.write_all(b" ")?;
            writer.write_all(serialized.len().to_string().as_bytes())?;
            writer.write_all(b"\x00")?;
            writer.write_all(&serialized)?;
            writer.into_inner()
        };

        let sha1 = {
            let mut hasher = Sha1::new();
            hasher.update(&bytes);
            hex(&hasher.finalize()[..])
        };

        if write {
            let file = self
                .repo_file(
                    &Path::new("objects").join(&sha1[..2]).join(&sha1[2..]),
                    true,
                )
                .ok_or(format!("could not create object: {}", sha1))?;

            let compressed = {
                let mut encoder = Encoder::new(Vec::new())?;
                encoder.write_all(&bytes)?;
                encoder.finish().into_result()?
            };

            let mut object =
                BufWriter::new(File::open(file).or(Err("Failed to open object file"))?);
            object.write_all(&compressed)?;
        }

        Ok(sha1)
    }

    pub fn object_hash(
        &self,
        path: &Path,
        object_type: ObjectType,
        write: bool,
    ) -> Result<String, Box<dyn Error>> {
        let data = {
            let mut file = File::open(path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            buf
        };

        let obj = match object_type {
            ObjectType::Blob => GitObject::Blob(BlobObject::from(data)),
            _ => todo!(),
        };

        self.write_object(&obj, write)
    }
}

fn default_config() -> Ini {
    let mut ini = Ini::new();
    ini.setstr("core", "repositoryformatversion", Some("0"));
    ini.setstr("core", "filemode", Some("false"));
    ini.setstr("core", "bare", Some("false"));
    ini
}
