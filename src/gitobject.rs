use crate::kvlm::{kvlm_parse, kvlm_serialize};
use crate::pack::BinaryObject;
use crate::util::{parse_variable_length, read_byte};
use anyhow::Context;
use bytes::Buf;
use hex::{decode, ToHex};
use log::debug;
use ordered_hash_map::OrderedHashMap;
use std::io::{BufReader, ErrorKind, Read};
use std::ops::Deref;
use std::{fmt::{Debug, Display}, io, path::PathBuf, str::from_utf8};

#[derive(Debug)]
pub enum GitObject {
    Blob(BlobObject),
    Commit(CommitObject),
    Tree(TreeObject),
    Tag(TagObject),
    OffsetDelta(OffsetDeltaObject),
    RefDelta(RefDeltaObject),
}

impl Display for GitObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            GitObject::Blob(blob) => f.write_fmt(format_args!(
                "blob {}",
                from_utf8(&blob.data).unwrap_or("<<BINARY>>")
            )),
            GitObject::Commit(commit_object) => f.write_fmt(format_args!(
                "commit {}",
                from_utf8(&commit_object.serialize()).unwrap_or("<<BINARY>>")
            )),
            GitObject::Tree(_) => f.write_str("tree {...}"),
            _ => todo!(),
        }
    }
}

impl GitObject {
    pub fn name(&self) -> &'static [u8] {
        match &self {
            GitObject::Blob(_) => b"blob",
            GitObject::Commit(_) => b"commit",
            GitObject::Tree(_) => b"tree",
            GitObject::Tag(_) => b"tag",
            _ => unimplemented!(),
        }
    }

    pub fn to_binary_object(&self) -> BinaryObject {
        match self {
            GitObject::Blob(_) => BinaryObject::Blob,
            GitObject::Commit(_) => BinaryObject::Commit,
            GitObject::Tree(_) => BinaryObject::Tree,
            GitObject::Tag(_) => BinaryObject::Tag,
            GitObject::OffsetDelta(OffsetDeltaObject { offset, delta: _ }) => {
                BinaryObject::OffsetDelta(*offset)
            }
            GitObject::RefDelta(RefDeltaObject {
                                    reference,
                                    delta: _,
                                }) => BinaryObject::RefDelta(*reference),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match &self {
            GitObject::Blob(blob) => blob.serialize(),
            GitObject::Commit(commit) => commit.serialize(),
            GitObject::Tag(tag) => tag.serialize(),
            GitObject::Tree(tree) => tree.serialize(),
            GitObject::OffsetDelta(delta) => format!("OffsetDelta({:?})", delta).into_bytes(),
            GitObject::RefDelta(delta) => format!("RefDelta({:?})", delta).into_bytes(),
        }
    }
}

#[derive(Debug)]
pub struct BlobObject {
    data: Vec<u8>,
}

impl BlobObject {
    pub fn from(data: Vec<u8>) -> Self {
        Self { data }
    }

    fn serialize(&self) -> Vec<u8> {
        self.data.clone()
    }
}

impl Display for BlobObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(from_utf8(&self.data).unwrap_or("<<BINARY>>"))
    }
}

#[derive(Debug)]
pub struct CommitObject {
    kvlm: OrderedHashMap<String, Vec<Vec<u8>>>,
}

impl CommitObject {
    fn get(&self, name: &str) -> impl Iterator<Item=String> {
        self.kvlm.get(name).into_iter().flat_map(|a| {
            a.first()
                .into_iter()
                .flat_map(|v| from_utf8(v).map(|v| v.to_string()))
        })
    }

    pub fn author(&self) -> Vec<String> {
        self.get("author").collect()
    }

    pub fn message(&self) -> Option<String> {
        self.get("").next()
    }

    pub fn parents(&self) -> Vec<[u8; 20]> {
        self.get("parent")
            .flat_map(|s| decode(s).ok())
            .flat_map(|v| v.deref().try_into().ok())
            .collect()
    }

    pub fn from(data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            kvlm: kvlm_parse(data).context("Failed to parse commit kvlm")?,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}

#[derive(Debug)]
pub struct TagObject {
    kvlm: OrderedHashMap<String, Vec<Vec<u8>>>,
}

impl TagObject {
    pub fn from(data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            kvlm: kvlm_parse(data).context("parsing tag object")?,
        })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}

#[derive(Debug)]
pub struct TreeObject {
    leaves: Vec<TreeLeaf>,
}

impl TreeObject {
    pub fn new(data: &[u8]) -> anyhow::Result<TreeObject> {
        debug!("reading tree len: {}", data.len());
        let mut leaves = Vec::new();

        let mut rem = data;
        while !rem.is_empty() {
            let (leaf, len) = TreeLeaf::parse_one(rem).context("parsing tree leaf")?;
            debug!("treeleef read: {}, len: {len}", leaf.path.to_string_lossy());
            leaves.push(leaf);
            rem = &rem[len..];
        }
        Ok(Self {
            leaves,
        })
    }

    pub fn leaf_iter(&self) -> impl Iterator<Item=&TreeLeaf> {
        self.leaves.iter()
    }

    fn serialize(&self) -> Vec<u8> {
        // todo ensure leaves sorted by path
        // see git tree.c write_index_as_tree for sorting rules
        self.leaves
            .iter()
            .flat_map(|l| l.serialize())
            .collect::<Vec<u8>>()
    }
}

#[derive(PartialEq, Eq, Clone)]
pub struct TreeLeaf {
    pub mode: String,
    pub path: PathBuf,
    pub sha1: Vec<u8>,
}

impl Ord for TreeLeaf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}
impl PartialOrd for TreeLeaf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Debug for TreeLeaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TreeLeaf {{ mode = {}, path = {}, sha1 = {} }}",
            self.mode,
            self.path.to_string_lossy(),
            self.sha1.encode_hex::<String>()
        ))
    }
}

impl TreeLeaf {
    fn parse_one(data: &[u8]) -> anyhow::Result<(Self, usize)> {
        let x = data
            .iter()
            .position(|&b| b == b' ')
            .context("tree leaf does not contain space")?;
        anyhow::ensure!(x == 5 || x == 6, "tree leaf mode length incorrect");

        let mut mode = from_utf8(&data[..x])
            .context("converting mode to utf-8")?
            .to_string();
        if mode.len() == 5 {
            mode.insert(0, '0');
        }

        let y = x + data
            .iter()
            .skip(x)
            .position(|&b| b == b'\0')
            .context("tree leaf does not contain null")?;
        let path = PathBuf::from(from_utf8(&data[x + 1..y]).context("leaf path is not utf8")?);
        anyhow::ensure!(data.len() >= y + 21, "tree leaf truncated in sha1");
        let sha1 = data[y + 1..y + 21].to_vec();

        Ok((TreeLeaf { mode, path, sha1 }, y + 21))
    }

    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        let mode = if self.mode.len() == 6 && self.mode.starts_with("0") {
            &self.mode.chars().skip(1).collect()
        } else {
            &self.mode
        };

        res.extend_from_slice(mode.as_bytes());
        res.push(b' ');
        res.extend_from_slice(self.path.to_string_lossy().as_bytes());
        res.push(b'\0');
        res.extend_from_slice(&self.sha1);
        res
    }
}

#[derive(Debug)]
pub struct DeltaObject {
    base_size: usize,
    expanded_size: usize,
    instructions: Vec<DeltaInstruction>,
}

#[derive(Debug)]
pub struct OffsetDeltaObject {
    pub offset: u64,
    pub delta: DeltaObject,
}

#[derive(Debug)]
pub struct RefDeltaObject {
    pub reference: [u8; 20],
    pub delta: DeltaObject,
}

impl DeltaObject {
    pub fn from(data: &Vec<u8>) -> anyhow::Result<Self> {
        parse_delta_data(data)
    }

    pub fn rebuild(&self, data: Vec<u8>) -> Vec<u8> {
        let mut result = Vec::new();
        for instr in self.instructions.iter() {
            match instr {
                DeltaInstruction::Copy(offset, size) => {
                    result.extend_from_slice(&data[*offset..offset + size]);
                }
                DeltaInstruction::Insert(insert) => {
                    result.extend_from_slice(&insert);
                }
            };
        }
        result
    }
}

impl OffsetDeltaObject {
    pub fn new(offset: u64, data: &Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self {
            offset,
            delta: DeltaObject::from(data).context("parsing offset delta object")?,
        })
    }
}

impl RefDeltaObject {
    pub fn new(reference: [u8; 20], data: &Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self {
            reference,
            delta: parse_delta_data(data).context("parsing ref delta object")?,
        })
    }
}

fn parse_copy_instruction<T: Read>(
    opcode: u8,
    reader: &mut BufReader<T>,
) -> io::Result<DeltaInstruction> {
    let cp_off: usize = {
        let mut cp_off: usize = 0;
        for i in 0..4 {
            if opcode & (1 << i) != 0 {
                let x = read_byte(reader)?;
                cp_off |= (x as usize) << (i * 8);
            }
        }
        cp_off
    };
    let cp_size = {
        let mut cp_size: usize = 0;
        for i in 0..3 {
            if opcode & (1 << (4 + i)) != 0 {
                let x = read_byte(reader)?;
                cp_size |= (x as usize) << (i * 8);
            }
        }
        if cp_size == 0 {
            cp_size = 0x10000;
        }
        cp_size
    };

    Ok(DeltaInstruction::Copy(cp_off, cp_size))
}

#[derive(Debug)]
enum DeltaInstruction {
    Copy(usize, usize),
    Insert(Vec<u8>),
}

impl Display for DeltaInstruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeltaInstruction::Copy(offset, size) => {
                f.write_fmt(format_args!("Copy({}, {})", offset, size))
            }
            DeltaInstruction::Insert(data) => {
                let s = from_utf8(data);
                f.write_fmt(format_args!(
                    "Insert({})",
                    s.map(|s| s.to_string()).unwrap_or_else(|e| {
                        format!(
                            "e({}, rem:{})",
                            from_utf8(&data[0..e.valid_up_to()]).unwrap_or("<<REALLY_FAILED>>"),
                            data.len() - e.valid_up_to()
                        )
                    })
                ))
            }
        }
    }
}

fn parse_delta_data(reader: &[u8]) -> anyhow::Result<DeltaObject> {
    let mut reader = BufReader::new(reader.reader());
    let base_size = parse_variable_length(&mut reader).context("reading base size")?;
    let expanded_size = parse_variable_length(&mut reader).context("reading expanded size")?;

    let mut instructions = Vec::new();
    loop {
        let opcode = read_byte(&mut reader);
        match opcode {
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                anyhow::bail!("unexpected read error: {}", err);
            }
            Ok(opcode) => {
                anyhow::ensure!(opcode != 0, "invalid delta opcode 0");
                let instr = if opcode & 0x80 == 0 {
                    let data = {
                        let mut buf = vec![0; opcode as usize];
                        reader.read_exact(&mut buf).context("reading insert data")?;
                        buf
                    };
                    DeltaInstruction::Insert(data)
                } else {
                    parse_copy_instruction(opcode, &mut reader)
                        .context("reading copy instruction")?
                };

                instructions.push(instr);
            }
        }
    }
    Ok(DeltaObject {
        base_size,
        expanded_size,
        instructions,
    })
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Read, path::PathBuf};

    use hex::FromHex;

    use crate::gitobject::TreeLeaf;

    use super::TreeObject;

    #[test]
    fn deserialize_tree() {
        let mut f = File::open("test/tree").unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        let skip = buf.iter().position(|&b| b == b'\0').unwrap_or(0) + 1;
        let tree = TreeObject::new(&buf[skip..]).unwrap();

        assert_eq!(
            tree.leaves[0],
            TreeLeaf {
                path: PathBuf::from(".github"),
                mode: "040000".to_string(),
                sha1: <[u8; 20]>::from_hex("a0ef2d9bb064800d8faceb96832b3ed26eb57412")
                    .unwrap()
                    .to_vec()
            }
        );

        assert_eq!(tree.serialize(), buf[skip..].to_vec());
    }
}
