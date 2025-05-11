extern crate sha1;

use crate::cli::CommandObjectType;
use crate::gitobject::GitObject;
use crate::gitobject::blob::BlobObject;
use crate::gitobject::delta::DeltaObject;
use crate::gitobject::tree::TreeObject;
use crate::hashingreader::HashingReader;
use crate::logiterator::LogIterator;
use crate::pack::BinaryObject::{Blob, Commit, Tag, Tree};
use crate::pack::{BinaryObject, Pack};
use crate::packindex::{PackIndex, PackIndexItem};
use crate::repository::ObjectLocation::{ObjectFile, PackFile};
use crate::util::validate_sha1;
use BinaryObject::{OffsetDelta, RefDelta};
use anyhow::{Context, Result, bail, ensure};
use bytes::{Buf, Bytes};
use configparser::ini::Ini;
use flate2::Compression;
use flate2::bufread::{ZlibDecoder, ZlibEncoder};
use hex::{ToHex, decode};
use log::{debug, trace};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::sink;
use std::rc::Rc;
use std::{
    fs::{File, create_dir_all},
    io,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    str::from_utf8,
};
use tempfile::NamedTempFile;

type PackRef = Rc<Pack<File>>;

pub struct Repository {
    pub worktree: PathBuf,
    gitdir: PathBuf,
    #[allow(dead_code)]
    conf: Option<Ini>,
    pack_cache: RefCell<HashMap<[u8; 20], PackRef>>,
    global_index: RefCell<Option<GlobalIndex>>,
}

struct GlobalIndex {
    fanout: [u32; 256],
    hashes: Vec<[u8; 20]>,
    locations: Vec<ObjectLocation>,
}

impl GlobalIndex {
    pub fn search(&self, sha1: [u8; 20]) -> Option<ObjectLocation> {
        let mut left = if sha1[0] == 0 {
            0
        } else {
            self.fanout[sha1[0] as usize - 1]
        } as usize;
        let mut right = self.fanout[sha1[0] as usize] as usize;
        while left <= right {
            let i = (right - left) / 2 + left;
            match self.hashes[i].as_slice().cmp(&sha1) {
                Ordering::Less => left = i + 1,
                Ordering::Greater => right = i - 1,
                Ordering::Equal => return Some(self.locations[i]),
            }
        }
        None
    }
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
            pack_cache: RefCell::new(HashMap::new()),
            global_index: RefCell::new(None),
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

    fn read_object_file_data(&self, sha1: [u8; 20], data: &mut Vec<u8>) -> Result<BinaryObject> {
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
        let mut raw = [0; 64];
        decoder
            .read_exact(&mut raw)
            .context("reading object file")?;
        trace!(
            "first part: {}",
            from_utf8(&raw).unwrap_or_else(|e| from_utf8(&raw[..e.valid_up_to()]).unwrap())
        );
        let (object_type, raw) = raw.split_at(
            raw.iter()
                .position(|&b| b == b' ')
                .context("expected space")?,
        );
        let (size, raw) =
            raw[1..].split_at(raw.iter().position(|&b| b == 0).context("expected null")?);
        trace!(
            "reading size... [{}]",
            from_utf8(&size[..size.len() - 1]).unwrap_or("<<bad-utf8>>")
        );
        let size = from_utf8(&size[..size.len() - 1])
            .context("parsing size as utf8")?
            .parse::<usize>()
            .context("parsing size as usize")?;

        data.extend_from_slice(raw);
        trace!(
            "remaining [[{}]]",
            from_utf8(data).unwrap_or_else(|e| from_utf8(&data[..e.valid_up_to()]).unwrap())
        );
        decoder.read_to_end(data).context("reading object")?;
        trace!(
            "fully read [[{}]]",
            from_utf8(data).unwrap_or_else(|e| from_utf8(&data[..e.valid_up_to()]).unwrap())
        );

        ensure!(
            size == data.len(),
            "object corrupt: size {} does not match expected {}",
            size,
            raw.len(),
        );
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

        validate_sha1(sha1, object_type, data).context("validating object sha1")?;
        Ok(object_type)
    }

    fn object_file_path(&self, sha1: [u8; 20]) -> Option<PathBuf> {
        let sha: String = sha1.encode_hex();
        let path = Path::new("objects").join(&sha[..2]).join(&sha[2..]);
        self.repo_file(&path, false)
    }

    fn init_global_index(&self) -> Result<()> {
        let index_iter = self
            .repo_path(Path::new("objects/pack"))
            .read_dir()?
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
            });

        let mut all_items = Vec::new();
        for index in index_iter {
            index
                .iter()
                .map(|PackIndexItem(hash, offset)| (hash, index.id(), offset))
                .for_each(|item| all_items.push(item));
        }
        all_items.sort_by(|(hash1, _, _), (hash2, _, _)| hash1.cmp(hash2));

        let mut result = GlobalIndex {
            fanout: [0u32; 256],
            hashes: Vec::with_capacity(all_items.len()),
            locations: Vec::with_capacity(all_items.len()),
        };
        let mut hash_prefix = 0u8;
        for (i, (hash, pack, offset)) in all_items.into_iter().enumerate() {
            while hash_prefix < hash[0] {
                result.fanout[hash_prefix as usize] = (i - 1) as u32;
                hash_prefix += 1;
            }
            result.hashes.push(hash);
            result.locations.push(PackFile(pack, offset));
        }

        self.global_index.replace(Some(result));

        Ok(())
    }

    fn find_object_location(&self, sha1: [u8; 20]) -> Option<ObjectLocation> {
        {
            let global_index = self.global_index.borrow();
            if global_index.is_none() {
                drop(global_index);
                self.init_global_index().ok()?;
            }
        }
        {
            let global = self.global_index.borrow();
            let global = global.iter().next()?;
            let location = global.search(sha1);
            if location.is_some() {
                return location;
            }
        }

        if let Some(path) = self.object_file_path(sha1) {
            if path.is_file() {
                return Some(ObjectFile);
            }
        }

        None
    }

    fn open_index(&self, path: &Path) -> Result<PackIndex> {
        let file = File::open(path)
            .with_context(|| format!("opening pack index file {}", path.to_string_lossy()))?;
        let index = PackIndex::new(BufReader::new(file))
            .with_context(|| format!("opening pack index file {}", path.to_string_lossy()))?;
        Ok(index)
    }

    pub fn read_object_data(&self, sha1: [u8; 20], data: &mut Vec<u8>) -> Result<BinaryObject> {
        let location = self
            .find_object_location(sha1)
            .context("Failed to find object")?;
        self.read_object_from_location(sha1, location, data)
            .context("reading object from location")
    }

    fn open_pack(&self, id: [u8; 20]) -> Result<Rc<Pack<File>>> {
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
                let pack = Rc::new(pack);
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
        location: ObjectLocation,
        data: &mut Vec<u8>,
    ) -> Result<BinaryObject> {
        ensure!(
            data.is_empty(),
            "data must be empty before reading {}",
            sha1.encode_hex::<String>()
        );

        match location {
            ObjectFile => self.read_object_file_data(sha1, data),
            PackFile(pack, offset) => {
                let packfile = self
                    .open_pack(pack)
                    .with_context(|| format!("opening pack {}", pack.encode_hex::<String>()))?;
                let object_type =
                    packfile
                        .read_object_data_at(offset, data)
                        .with_context(|| {
                            format!(
                                "reading object {} from packfile {} at {}",
                                sha1.encode_hex::<String>(),
                                pack.encode_hex::<String>(),
                                offset
                            )
                        })?;
                if !object_type.is_delta() {
                    debug!(
                        "object is not a delta, not unpacking: {} {}, len = {}",
                        object_type.name(),
                        sha1.encode_hex::<String>(),
                        data.len()
                    );

                    validate_sha1(sha1, object_type, data).with_context(|| {
                        format!(
                            "reading {} from pack {} at {}",
                            sha1.encode_hex::<String>(),
                            pack.encode_hex::<String>(),
                            offset
                        )
                    })?;
                    return Ok(object_type);
                }

                let (object_type, unpacked_data) = self
                    .unpack_delta(&packfile, offset, object_type, data)
                    .with_context(|| {
                        format!(
                            "unpacking delta of object {} from packfile {} at {}",
                            sha1.encode_hex::<String>(),
                            pack.encode_hex::<String>(),
                            offset
                        )
                    })?;
                data.truncate(0);
                data.extend_from_slice(&unpacked_data);
                validate_sha1(sha1, object_type, data).with_context(|| {
                    format!(
                        "reading {} from pack {} at {} - unpacked",
                        sha1.encode_hex::<String>(),
                        pack.encode_hex::<String>(),
                        offset
                    )
                })?;
                Ok(object_type)
            }
        }
    }

    fn unpack_delta(
        &self,
        packfile: &Pack<File>,
        offset: u64,
        object_type: BinaryObject,
        data: &[u8],
    ) -> Result<(BinaryObject, Vec<u8>)> {
        trace!("unpacking {}", object_type.name());
        let (reference_type, reference_data) = match object_type {
            OffsetDelta(delta_offset) => {
                let mut reference_data = Vec::new();
                let reference_offset = offset - delta_offset;
                let reference_type = packfile
                    .read_object_data_at(reference_offset, &mut reference_data)
                    .context("reading object in packfile")?;

                if !reference_type.is_delta() {
                    (reference_type, reference_data)
                } else {
                    self.unpack_delta(packfile, reference_offset, reference_type, &reference_data)?
                }
            }
            RefDelta(reference) => {
                let location = self
                    .find_object_location(reference)
                    .context("reference not found")?;
                let mut reference_data = Vec::new();
                let reference_type = self
                    .read_object_from_location(reference, location, &mut reference_data)
                    .with_context(|| {
                        format!(
                            "unpacking ref delta reference {}",
                            reference.encode_hex::<String>()
                        )
                    })?;

                if !reference_type.is_delta() {
                    (reference_type, reference_data)
                } else {
                    match location {
                        ObjectFile => {
                            self.unpack_delta(packfile, 0, reference_type, &reference_data)?
                        }
                        PackFile(packfile_id, offset) => {
                            let reference_packfile = self.open_pack(packfile_id)?;
                            self.unpack_delta(
                                &reference_packfile,
                                offset,
                                reference_type,
                                &reference_data,
                            )?
                        }
                    }
                }
            }
            _ => bail!("expected delta type"),
        };

        trace!("reference data type: {}", reference_type.name());

        Ok((
            reference_type,
            DeltaObject::from(data)
                .context("reading delta data")?
                .rebuild(reference_data)
                .context("rebuilding delta")?,
        ))
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
            CommandObjectType::Blob => GitObject::Blob(BlobObject::from(data)),
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
        Pack::new(reader)?.read_all()
    }

    pub fn ls_tree(&self, reference: &str, recurse: bool, path: &Path) -> Result<()> {
        trace!("finding object {}", reference);
        let sha1 = self.find_object(reference)?;
        trace!("reading object {}", sha1.encode_hex::<String>());

        let mut data = Vec::new();
        let object = match self.read_object_data(sha1, &mut data)? {
            Tree => TreeObject::new(&data)?,
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

    pub fn log_iter(&self, sha1: [u8; 20]) -> Result<LogIterator> {
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

#[derive(PartialEq, Copy, Clone)]
enum ObjectLocation {
    ObjectFile,
    PackFile([u8; 20], u64),
}
