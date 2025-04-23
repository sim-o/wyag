extern crate sha1;

use std::{
    error::Error,
    fs::{create_dir_all, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::from_utf8,
};
use std::io::Seek;

use bytes::{Buf, BufMut};
use configparser::ini::Ini;
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use hex::{decode, ToHex};

use BinaryObject::{OffsetDelta, RefDelta};

use crate::{
    CommandObjectType,
    gitobject::{BlobObject, GitObject},
};
use crate::gitobject::DeltaObject;
use crate::pack::{BinaryObject, Pack, parse_object_data};
use crate::pack::BinaryObject::{Blob, Commit, Tag, Tree};
use crate::packindex::PackIndex;
use crate::repository::ObjectLocation::{ObjectFile, PackFile};
use crate::util::{get_sha1, validate_sha1};

pub struct Repository {
    pub worktree: PathBuf,
    gitdir: PathBuf,
    conf: Option<Ini>,
}

impl Repository {
    pub fn new(path: &Path, force: bool) -> Result<Self, Box<dyn Error>> {
        let gitdir = path.join(".git");
        let config_file = gitdir.join("config");

        println!("constructing repo");

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

        println!("constructed");

        Ok(Self {
            worktree: path.into(),
            gitdir,
            conf,
        })
    }

    pub fn find(orig: &Path) -> Result<Self, Box<dyn Error>> {
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

    pub fn init(&self) -> Result<(), Box<dyn Error>> {
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
            BufWriter::new(file).write_all(contents.as_bytes())?;
        }

        Ok(())
    }

    fn read_object_file_data(&self, sha1: &str) -> Result<(BinaryObject, Vec<u8>), Box<dyn Error>> {
        let path = self
            .object_file_path(sha1)
            .ok_or(format!("Could not load object {}", sha1))?;
        if !path.is_file() {
            Err(format!("file {} does not exist", path.to_string_lossy()))?;
        }

        let mut file = File::open(path)?;
        let mut decoder = ZlibDecoder::new(BufReader::new(&mut file));
        let mut raw: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut raw)?;

        let type_idx = raw
            .iter()
            .position(|&b| b == b' ')
            .ok_or("object corrupt: missing type")?;

        let size_idx = type_idx
            + raw
                .iter()
                .skip(type_idx)
                .position(|&b| b == b'\x00')
                .ok_or("object corrupt: missing size")?;

        println!("reading size...");
        let size = from_utf8(&raw[type_idx + 1..size_idx])?.parse::<usize>()?;
        if size != raw.len() - size_idx - 1 {
            Err(format!(
                "object corrupt: size {} does not match expected {}",
                size,
                raw.len() - size_idx - 1,
            ))?;
        }

        let object_type = &raw[..type_idx];
        let data = raw[size_idx + 1..].to_vec();
        println!(
            "type = '{}' size = {}",
            from_utf8(object_type).unwrap(),
            size
        );

        let object_type = match object_type {
            b"blob" => Blob,
            b"commit" => Commit,
            b"tree" => Tree,
            b"tag" => Tag,
            _ => unimplemented!(
                "unexpected type {}",
                from_utf8(object_type).unwrap_or("<<invalid utf8>>")
            ),
        };

        validate_sha1(sha1, &object_type, &data);
        Ok((object_type, data))
    }

    fn object_file_path(&self, sha: &str) -> Option<PathBuf> {
        let path = Path::new("objects").join(&sha[..2]).join(&sha[2..]);
        self.repo_file(&path, false)
    }

    pub fn read_object(&self, name: &str) -> Result<GitObject, Box<dyn Error>> {
        let (object_type, data) = self.read_object_data(name)?;
        parse_object_data(object_type, data)
    }

    fn find_object_location(&self, name: &str) -> Option<ObjectLocation> {
        if self.object_file_path(name).is_some() {
            return Some(ObjectFile);
        }

        let sha1 = decode(name).ok()?;
        let found = self
            .repo_path(&Path::new("objects").join("pack"))
            .read_dir()
            .ok()?
            .filter_map(|p| {
                if let Ok(p) = p {
                    if let Some(name) = p.file_name().to_str() {
                        let path = p.path();
                        if name.starts_with("pack-") && name.ends_with(".idx") && path.is_file() {
                            let id = name[5..name.len() - 4].to_string();
                            println!("found pack {id}: {name}");
                            if let Ok(file) = File::open(path) {
                                return Some(PackIndex::new(id, BufReader::new(file)));
                            }
                        }
                    }
                }
                None
            })
            .flat_map(|mut pf| {
                let result = pf.find(sha1.as_slice());
                if let Ok(Some(offset)) = result {
                    println!("found pack offset {}", offset);
                    Some((pf.id(), offset))
                } else {
                    None
                }
            })
            .next();

        if let Some((pack, offset)) = found {
            return Some(PackFile(pack, offset));
        }

        None
    }

    fn read_object_data(&self, name: &str) -> Result<(BinaryObject, Vec<u8>), Box<dyn Error>> {
        let location = self
            .find_object_location(name)
            .ok_or("Failed to find object")?;
        self.read_object_from_location(name, &location)
    }

    fn open_pack(&self, sha1: &str) -> Result<Pack<File>, Box<dyn Error>> {
        let packfile_name = format!("pack-{}.pack", sha1);
        let packfile_path = Path::new("objects").join("pack").join(packfile_name);
        match self.repo_file(&packfile_path, false) {
            Some(packfile_path) => Ok(Pack::new(BufReader::new(File::open(packfile_path)?))?),
            None => Err("Failed to load packfile")?,
        }
    }

    fn read_object_from_location(
        &self,
        name: &str,
        location: &ObjectLocation,
    ) -> Result<(BinaryObject, Vec<u8>), Box<dyn Error>> {
        match location {
            ObjectFile => self.read_object_file_data(name),
            PackFile(pack, offset) => {
                let mut packfile = self.open_pack(pack)?;
                let (object_type, data) = packfile.read_object_data_at(*offset)?;
                let (object_type, data) =
                    self.unpack_delta(&mut packfile, *offset, object_type, data)?;
                validate_sha1(name, &object_type, &data);
                Ok((object_type, data))
            }
        }
    }

    fn unpack_delta<T: Read + Seek>(
        &self,
        packfile: &mut Pack<T>,
        offset: u64,
        object_type: BinaryObject,
        data: Vec<u8>,
    ) -> Result<(BinaryObject, Vec<u8>), Box<dyn Error>> {
        println!("unpacking delta");
        let data = match object_type {
            Blob | Commit | Tag | Tree => (object_type, data),
            OffsetDelta(delta_offset) => {
                let (next_object_type, next_data) =
                    packfile.read_object_data_at(offset - delta_offset)?;
                let (next_object_type, next_data) = self.unpack_delta(
                    packfile,
                    offset - delta_offset,
                    next_object_type,
                    next_data,
                )?;
                (
                    next_object_type,
                    DeltaObject::from(&data)?.rebuild(next_data),
                )
            }
            RefDelta(reference) => {
                let hex_reference = reference.encode_hex::<String>();
                let location = self
                    .find_object_location(&hex_reference)
                    .ok_or("reference not found")?;
                let (next_object_type, next_data) =
                    self.read_object_from_location(&hex_reference, &location)?;
                let (next_object_type, next_data) = match location {
                    ObjectFile => self.unpack_delta(packfile, 0, next_object_type, next_data)?,
                    PackFile(pack, offset) => {
                        let mut next_pack = self.open_pack(&pack)?;
                        self.unpack_delta(&mut next_pack, offset, next_object_type, next_data)?
                    }
                };
                (
                    next_object_type,
                    DeltaObject::from(&data)?.rebuild(next_data),
                )
            }
        };
        println!("\tunpacked");
        Ok(data)
    }

    pub fn find_object(&self, _: CommandObjectType, name: &str) -> Result<String, Box<dyn Error>> {
        Ok(name.to_string())
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

        let sha1 = get_sha1(&obj.to_binary_object(), &bytes);

        if write {
            let file = self
                .repo_file(
                    &Path::new("objects").join(&sha1[..2]).join(&sha1[2..]),
                    true,
                )
                .ok_or(format!("could not create object: {}", sha1))?;

            let compressed = {
                let mut encoder = ZlibEncoder::new(bytes.reader(), Compression::default());
                let mut compressed = Vec::new();
                encoder.read_to_end(&mut compressed)?;
                compressed
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
        object_type: CommandObjectType,
        write: bool,
    ) -> Result<String, Box<dyn Error>> {
        let data = {
            let mut file = File::open(path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            buf
        };

        let obj = match object_type {
            CommandObjectType::Blob => GitObject::Blob(BlobObject::from(data)),
            _ => todo!(),
        };

        self.write_object(&obj, write)
    }

    pub fn read_packfile(&self, packfile_sha: &str) -> Result<Vec<GitObject>, Box<dyn Error>> {
        let path = self
            .repo_file(
                &Path::new("objects")
                    .join("pack")
                    .join(format!("pack-{}.pack", packfile_sha)),
                false,
            )
            .ok_or("Packfile does not exist")?;

        let reader = BufReader::new(File::open(path)?);
        Pack::new(reader)?.read()
    }

    pub fn ls_tree(
        &self,
        reference: &str,
        recurse: bool,
        path: &Path,
    ) -> Result<(), Box<dyn Error>> {
        println!("finding object {}", reference);
        let sha1 = self.find_object(CommandObjectType::Tree, reference)?;
        println!("reading object {}", sha1);
        let object = match self.read_object(&sha1)? {
            GitObject::Tree(tree) => tree,
            _ => Err("object not a tree")?,
        };

        println!("iterating leaf {}", path.to_string_lossy());

        object.for_each_leaf(|item| {
            let _type = match &item.mode[..2] {
                "04" => "tree",
                "10" | "12" => "blob",
                "16" => "commit",
                _ => panic!("weird TreeLeaf mode {}", &item.mode[..2]),
            };

            if recurse && _type == "tree" {
                self.ls_tree(
                    &item.sha1.encode_hex::<String>(),
                    recurse,
                    &path.join(&item.path),
                )
                .expect("Failed to descend tree");
            } else {
                println!(
                    "{} {} {} {}",
                    item.mode,
                    _type,
                    item.sha1.encode_hex::<String>(),
                    path.join(&item.path).to_string_lossy()
                );
            }
        });

        Ok(())
    }
}

fn default_config() -> Ini {
    let mut ini = Ini::new();
    ini.setstr("core", "repositoryformatversion", Some("0"));
    ini.setstr("core", "filemode", Some("false"));
    ini.setstr("core", "bare", Some("false"));
    ini
}

#[derive(PartialEq)]
enum ObjectLocation {
    ObjectFile,
    PackFile(String, u64),
}
