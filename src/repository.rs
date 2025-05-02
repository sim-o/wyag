extern crate sha1;

use crate::cli::CommandObjectType;
use crate::gitobject::blob::BlobObject;
use crate::gitobject::delta::DeltaObject;
use crate::gitobject::tree::TreeObject;
use crate::gitobject::GitObject;
use crate::hashingreader::HashingReader;
use crate::logiterator::LogIterator;
use crate::pack::BinaryObject::{Blob, Commit, Tag, Tree};
use crate::pack::{BinaryObject, Pack};
use crate::packindex::PackIndex;
use crate::repository::ObjectLocation::{ObjectFile, PackFile};
use crate::util::validate_sha1;
use anyhow::{bail, ensure, Context, Result};
use bytes::{Buf, Bytes};
use configparser::ini::Ini;
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use hex::{decode, ToHex};
use log::{debug, trace};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{sink, Seek};
use std::rc::Rc;
use std::{
    fs::{create_dir_all, File},
    io,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::from_utf8,
};
use tempfile::NamedTempFile;
use BinaryObject::{OffsetDelta, RefDelta};

type PackRef = Rc<RefCell<Pack<File>>>;

pub struct Repository {
    pub worktree: PathBuf,
    gitdir: PathBuf,
    conf: Option<Ini>,
    index_cache: RefCell<HashMap<PathBuf, Rc<PackIndex>>>,
    pack_cache: RefCell<HashMap<[u8; 20], PackRef>>,
}

impl Repository {
    pub fn new(path: &Path, force: bool) -> Result<Self> {
        let gitdir = path.join(".git");
        let config_file = gitdir.join("config");

        debug!("constructing repo");

        let conf = if config_file.is_file() {
            let conf = {
                let file = File::open(config_file).context("opening config file")?;
                let mut reader = BufReader::new(file);

                let mut conf = Ini::new();
                let mut config = String::new();
                reader
                    .read_to_string(&mut config)
                    .context("reading config file")?;
                if let Err(e) = conf.read(config) {
                    bail!("error parsing config contents: {}", e);
                }
                conf
            };

            let vers = conf
                .get("core", "repositoryformatversion")
                .context("version string does not exist")?;
            let vers = vers.parse::<i32>().context("parsing repository version")?;
            anyhow::ensure!(vers == 0, "Unsupported repositoryformatversion: {}", vers);

            Some(conf)
        } else {
            None
        };

        if conf.is_none() && !force {
            bail!("config file does not exist");
        }

        trace!("constructed");

        Ok(Self {
            worktree: path.into(),
            gitdir,
            conf,
            index_cache: RefCell::new(HashMap::new()),
            pack_cache: RefCell::new(HashMap::new()),
        })
    }

    pub fn find(orig: &Path) -> Result<Self> {
        let mut path = if orig.is_absolute() {
            orig
        } else {
            &std::env::current_dir()
                .context("getting current dir")?
                .join(orig)
        };

        while !path.join(".git").is_dir() {
            path = path
                .parent()
                .with_context(|| format!("{} is not a repository!", orig.to_string_lossy()))?;
        }

        Self::new(path, false)
            .with_context(|| format!("loading repository at {}", path.to_string_lossy()))
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

    pub fn init(&self) -> Result<()> {
        if self.worktree.exists() {
            anyhow::ensure!(
                self.worktree.is_dir(),
                "{} is not a directory!",
                self.worktree.to_string_lossy()
            );

            if self.gitdir.exists() {
                anyhow::ensure!(
                    self.gitdir.is_dir(),
                    "{} exists and is not a directory!",
                    self.gitdir.to_string_lossy(),
                );
                anyhow::ensure!(
                    self.gitdir
                        .read_dir()
                        .with_context(|| format!(
                            "reading contents at {}",
                            self.gitdir.to_string_lossy()
                        ))?
                        .next()
                        .is_none(),
                    "{} exists but is not empty!",
                    self.gitdir.to_string_lossy()
                );
            }
        } else {
            debug!("Creating worktree: {}", self.worktree.to_string_lossy());
            create_dir_all(&self.worktree).with_context(|| {
                format!("creating worktree at {}", self.worktree.to_string_lossy())
            })?;
        }

        for p in ["branches", "objects", "refs/tags", "refs/heads"] {
            self.repo_mkdir(Path::new(p))
                .with_context(|| format!("could not create {} directory", p))?;
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
            let file = File::create_new(self.gitdir.join(f))
                .with_context(|| format!("creating file {}", f))?;
            BufWriter::new(file)
                .write_all(contents.as_bytes())
                .with_context(|| format!("writing file contents {}", f))?;
        }

        Ok(())
    }

    fn read_object_file_data(&self, sha1: [u8; 20]) -> Result<(BinaryObject, Vec<u8>)> {
        let path = self
            .object_file_path(sha1)
            .with_context(|| format!("Could not load object {}", sha1.encode_hex::<String>()))?;
        ensure!(
            path.is_file(),
            "file {} does not exist",
            path.to_string_lossy()
        );

        let mut file = File::open(path).context("opening object file")?;
        let mut decoder = ZlibDecoder::new(BufReader::new(&mut file));
        let mut raw: Vec<u8> = Vec::new();
        decoder
            .read_to_end(&mut raw)
            .context("reading object file")?;

        let type_idx = raw
            .iter()
            .position(|&b| b == b' ')
            .context("object corrupt: missing type")?;

        let size_idx = type_idx
            + raw
            .iter()
            .skip(type_idx)
            .position(|&b| b == b'\x00')
            .context("object corrupt: missing size")?;

        trace!("reading size...");
        let size = from_utf8(&raw[type_idx + 1..size_idx])
            .context("parsing size as utf8")?
            .parse::<usize>()
            .context("parsing size as usize")?;
        ensure!(
            size == raw.len() - size_idx - 1,
            "object corrupt: size {} does not match expected {}",
            size,
            raw.len() - size_idx - 1,
        );

        let object_type = &raw[..type_idx];
        let data = raw[size_idx + 1..].to_vec();
        debug!(
            "type = '{}' size = {}",
            from_utf8(object_type)
                .map(|b| b.to_string())
                .unwrap_or_else(|_| format!("hex={}", object_type.encode_hex::<String>())),
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

        validate_sha1(sha1, &object_type, &data).context("validating object sha1")?;
        Ok((object_type, data))
    }

    fn object_file_path(&self, sha1: [u8; 20]) -> Option<PathBuf> {
        let sha: String = sha1.encode_hex();
        let path = Path::new("objects").join(&sha[..2]).join(&sha[2..]);
        self.repo_file(&path, false)
    }

    fn find_object_location(&self, sha1: [u8; 20]) -> Option<ObjectLocation> {
        if let Some(path) = self.object_file_path(sha1) {
            if path.is_file() {
                return Some(ObjectFile);
            }
        }

        let found = self
            .repo_path(&Path::new("objects").join("pack"))
            .read_dir()
            .ok()?
            .filter_map(|p| {
                if let Ok(p) = p {
                    if let Some(name) = p.file_name().to_str() {
                        let path = p.path();
                        if name.starts_with("pack-") && name.ends_with(".idx") && path.is_file() {
                            debug!("found pack: {name}");
                            if let Ok(value) = self.open_index(&path) {
                                return Some(value);
                            }
                        }
                    }
                }
                None
            })
            .flat_map(|pf| {
                let result = pf.find(sha1);
                if let Some(offset) = result {
                    debug!("found pack offset {}", offset);
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

    fn open_index(&self, path: &Path) -> Result<Rc<PackIndex>> {
        if let Some(cached) = self.index_cache.borrow().get(path) {
            return Ok(cached.clone());
        }

        let file = File::open(path)
            .with_context(|| format!("opening pack index file {}", path.to_string_lossy()))?;
        let index = PackIndex::new(BufReader::new(file))
            .with_context(|| format!("opening pack index file {}", path.to_string_lossy()))?;
        let index = Rc::new(index);
        self.index_cache
            .borrow_mut()
            .insert(path.to_path_buf(), index.clone());
        Ok(index)
    }

    pub fn read_object_data(&self, sha1: [u8; 20]) -> Result<(BinaryObject, Vec<u8>)> {
        let location = self
            .find_object_location(sha1)
            .context("Failed to find object")?;
        self.read_object_from_location(sha1, &location)
            .context("reading object from location")
    }

    fn open_pack(&self, id: [u8; 20]) -> Result<Rc<RefCell<Pack<File>>>> {
        let value = {
            let cache = self.pack_cache.borrow();
            cache.get(&id).cloned()
        };
        let pack = match value {
            None => {
                let packfile_name = format!("pack-{}.pack", id.encode_hex::<String>());
                let packfile_path = Path::new("objects").join("pack").join(packfile_name);
                let pack = match self.repo_file(&packfile_path, false) {
                    Some(packfile_path) => {
                        let file = File::open(packfile_path).context("opening packfile file")?;
                        Pack::new(BufReader::new(file)).context("opening packfile")?
                    }
                    None => bail!("Failed to load packfile"),
                };
                let pack = Rc::new(RefCell::new(pack));
                self.pack_cache.borrow_mut().insert(id, pack.clone());
                pack
            }
            Some(pack) => pack,
        };
        Ok(pack)
    }

    fn read_object_from_location(
        &self,
        sha1: [u8; 20],
        location: &ObjectLocation,
    ) -> Result<(BinaryObject, Vec<u8>)> {
        match location {
            ObjectFile => self.read_object_file_data(sha1),
            PackFile(pack, offset) => {
                let rc = self
                    .open_pack(*pack)
                    .with_context(|| format!("opening pack {}", pack.encode_hex::<String>()))?;
                let mut packfile = rc.borrow_mut();
                let (object_type, data) =
                    packfile.read_object_data_at(*offset).with_context(|| {
                        format!(
                            "reading object {} from packfile {} at {}",
                            sha1.encode_hex::<String>(),
                            pack.encode_hex::<String>(),
                            offset
                        )
                    })?;
                let (object_type, data) = self
                    .unpack_delta(&mut packfile, *offset, object_type, data)
                    .with_context(|| {
                        format!(
                            "unpacking delta of object {} from packfile {} at {}",
                            sha1.encode_hex::<String>(),
                            pack.encode_hex::<String>(),
                            offset
                        )
                    })?;
                validate_sha1(sha1, &object_type, &data).with_context(|| {
                    format!(
                        "reading {} from pack {} at {}",
                        sha1.encode_hex::<String>(),
                        pack.encode_hex::<String>(),
                        *offset
                    )
                })?;
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
    ) -> Result<(BinaryObject, Vec<u8>)> {
        trace!("unpacking delta");
        let data = match object_type {
            Blob | Commit | Tag | Tree => (object_type, data),
            OffsetDelta(delta_offset) => {
                let (next_object_type, next_data) = packfile
                    .read_object_data_at(offset - delta_offset)
                    .context("reading object in packfile")?;
                let (next_object_type, next_data) = self
                    .unpack_delta(packfile, offset - delta_offset, next_object_type, next_data)
                    .context("unpacking delta")?;
                (
                    next_object_type,
                    DeltaObject::from(&data)
                        .context("reading delta data")?
                        .rebuild(next_data),
                )
            }
            RefDelta(reference) => {
                let location = self
                    .find_object_location(reference)
                    .context("reference not found")?;
                let (next_object_type, next_data) = self
                    .read_object_from_location(reference, &location)
                    .with_context(|| {
                        format!(
                            "unpacking ref delta reference {}",
                            reference.encode_hex::<String>()
                        )
                    })?;
                let (next_object_type, next_data) = match location {
                    ObjectFile => self
                        .unpack_delta(packfile, 0, next_object_type, next_data)
                        .context("unpacking delta from object file")?,
                    PackFile(pack, offset) => {
                        let rc = self.open_pack(pack).context("found ref delta ")?;
                        let mut next_pack = rc.borrow_mut();
                        self.unpack_delta(&mut next_pack, offset, next_object_type, next_data)
                            .context("unpacking ref delta")?
                    }
                };
                (
                    next_object_type,
                    DeltaObject::from(&data)
                        .context("unpacking delta data")?
                        .rebuild(next_data),
                )
            }
        };
        trace!("\tunpacked");
        Ok(data)
    }

    pub fn find_object(&self, name: &str) -> Result<[u8; 20]> {
        if let Ok(hash) = decode(name) {
            if let Ok(hash) = hash.try_into() {
                return Ok(hash);
            }
        }

        if let Some(buf) = self.repo_file(&Path::new("refs").join("heads").join(name), false) {
            if buf.is_file() {
                let mut ref_contents = String::new();
                File::open(buf)
                    .context("opening object file")?
                    .read_to_string(&mut ref_contents)
                    .context("reading object")?;
                let ref_contents = ref_contents.trim_end_matches([' ', '\t', '\n', '\r']);
                return if let Some(ref_contents) = ref_contents.strip_prefix("ref: ") {
                    self.find_object(ref_contents)
                } else {
                    let sha1_decode: Result<[u8; 20], _> = match decode(ref_contents) {
                        Ok(sha1) => sha1.try_into(),
                        _ => bail!(
                            "Failed to decode reference file contents: '{}'",
                            ref_contents
                        ),
                    };
                    match sha1_decode {
                        Ok(result) => Ok(result),
                        _ => bail!("sha1 has incorrect length"),
                    }
                };
            }
        }

        bail!("reference does not exist: {}", name)
    }

    pub fn write_object(&self, obj: &GitObject, write: bool) -> Result<[u8; 20]> {
        let mut bytes = {
            let serialized = obj.serialize();

            let bytes = Bytes::from_iter(
                obj.name()
                    .iter()
                    .chain(b" ")
                    .chain(serialized.len().to_string().as_bytes())
                    .chain(b"\0")
                    .chain(&serialized)
                    .copied(),
            )
                .reader();
            HashingReader::new(bytes)
        };

        let sha1 = if write {
            let file = NamedTempFile::new().context("creating temp file")?;

            let mut encoder = ZlibEncoder::new(BufReader::new(&mut bytes), Compression::default());
            io::copy(&mut encoder, &mut BufWriter::new(&file))
                .context("copying compressed data to object file")?;

            let sha1 = bytes.finalize();
            let sha1_hex = sha1.encode_hex::<String>();

            let path = file.path();
            let new_path = self
                .repo_file(
                    &Path::new("objects")
                        .join(&sha1_hex[..2])
                        .join(&sha1_hex[2..]),
                    true,
                )
                .context("could not create path to object file")?;
            std::fs::rename(path, new_path)?;

            sha1
        } else {
            io::copy(&mut bytes, &mut sink()).context("")?;
            bytes.finalize()
        };

        Ok(sha1)
    }

    pub fn object_hash(
        &self,
        path: &Path,
        object_type: CommandObjectType,
        write: bool,
    ) -> Result<[u8; 20]> {
        let data = {
            let mut file = File::open(path)?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            buf
        };

        let obj = match object_type {
            CommandObjectType::Blob => GitObject::Blob(BlobObject::from(&data)),
            _ => todo!(),
        };

        self.write_object(&obj, write)
    }

    pub fn read_packfile(&self, packfile_sha: &str) -> Result<Vec<(BinaryObject, Vec<u8>)>> {
        let path = self
            .repo_file(
                &Path::new("objects")
                    .join("pack")
                    .join(format!("pack-{}.pack", packfile_sha)),
                false,
            )
            .context("Packfile does not exist")?;

        let reader = BufReader::new(File::open(path)?);
        Pack::new(reader)?.read()
    }

    pub fn ls_tree(&self, reference: &str, recurse: bool, path: &Path) -> Result<()> {
        trace!("finding object {}", reference);
        let sha1 = self.find_object(reference)?;
        trace!("reading object {}", sha1.encode_hex::<String>());
        let object = match self.read_object_data(sha1)? {
            (Tree, data) => TreeObject::new(&data)?,
            _ => bail!("object not a tree"),
        };

        trace!("iterating leaf {}", path.to_string_lossy());

        for item in object.leaf_iter() {
            let _type = match &item.mode[..2] {
                "04" => "tree",
                "10" | "12" => "blob",
                "16" => "commit",
                _ => bail!(
                    "weird TreeLeaf mode {} on {}",
                    &item.mode[..2],
                    item.path.to_string_lossy()
                ),
            };

            if recurse && _type == "tree" {
                self.ls_tree(
                    &item.sha1.encode_hex::<String>(),
                    recurse,
                    &path.join(&item.path),
                )
                    .with_context(|| {
                        format!("Failed to descend tree in {}", item.path.to_string_lossy())
                    })?;
            } else {
                trace!(
                    "{} {} {} {}",
                    item.mode,
                    _type,
                    item.sha1.encode_hex::<String>(),
                    path.join(&item.path).to_string_lossy()
                );
            }
        }

        Ok(())
    }

    pub fn log_iter(&self, sha1: [u8; 20]) -> LogIterator {
        LogIterator::new(self, sha1)
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
    PackFile([u8; 20], u64),
}
