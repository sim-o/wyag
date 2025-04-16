use crate::{
    hex::hex,
    kvlm::{kvlm_parse, kvlm_serialize},
};
use std::{
    cell::RefCell,
    collections::HashMap,
    error::Error,
    fmt::{Debug, Display},
    path::PathBuf,
    str::from_utf8,
};

pub enum GitObject {
    Blob(BlobObject),
    Commit(CommitObject),
    Tree(TreeObject),
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
        }
    }
}

impl GitObject {
    pub fn name(&self) -> &'static [u8] {
        match &self {
            GitObject::Blob(_) => b"blob",
            GitObject::Commit(_) => b"commit",
            GitObject::Tree(_) => b"tree",
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match &self {
            GitObject::Blob(blob) => blob.serialize(),
            GitObject::Commit(commit) => commit.serialize(),
            GitObject::Tree(tree) => tree.serialize(),
        }
    }
}

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

pub struct CommitObject {
    kvlm: HashMap<String, Vec<Vec<u8>>>,
}

impl CommitObject {
    pub fn from(data: &[u8]) -> Result<CommitObject, Box<dyn Error>> {
        Ok(CommitObject {
            kvlm: kvlm_parse(data)?,
        })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}

#[derive(Debug)]
pub struct TreeObject {
    leaves: RefCell<Vec<TreeLeaf>>,
}

impl TreeObject {
    pub fn from(data: &[u8]) -> Result<TreeObject, Box<dyn Error>> {
        let mut leaves = Vec::new();
        // let skip = data
        //     .iter()
        //     .position(|&b| b == b'\0')
        //     .ok_or("tree object did not contain null")?;
        //
        // println!(
        //     "parsing size {}",
        //     from_utf8(&data[..skip]).unwrap_or("could not parse utf8")
        // );
        // let size = from_utf8(&data[..skip])?.parse::<usize>()?;

        let mut rem = data;

        while !rem.is_empty() {
            let (leaf, len) = TreeLeaf::parse_one(rem)?;
            leaves.push(leaf);
            rem = &rem[len..];
        }
        Ok(Self {
            leaves: RefCell::new(leaves),
        })
    }

    pub fn for_each_leaf(&self, f: impl Fn(&TreeLeaf)) {
        self.leaves.borrow().iter().for_each(f);
    }

    fn serialize(&self) -> Vec<u8> {
        {
            self.leaves.borrow_mut().sort();
        }
        let data = self
            .leaves
            .borrow()
            .iter()
            .flat_map(|l| l.serialize())
            .collect::<Vec<u8>>();

        data.len()
            .to_string()
            .bytes()
            .chain(b"\0".iter().copied())
            .chain(data.iter().copied())
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
            hex(&self.sha1)
        ))
    }
}

impl TreeLeaf {
    fn parse_one(data: &[u8]) -> Result<(Self, usize), Box<dyn Error>> {
        println!("parsing TreeLeaf...");
        let x = data
            .iter()
            .position(|&b| b == b' ')
            .ok_or("tree leaf does not contain space")?;
        assert!(x == 5 || x == 6, "tree leaf mode length incorrect");

        let mut mode = from_utf8(&data[..x])?.to_string();
        if mode.len() == 5 {
            mode.insert(0, '0');
        }

        let y = x + data[x..]
            .iter()
            .position(|&b| b == b'\0')
            .ok_or("tree leaf does not contain null")?;
        let path = PathBuf::from(from_utf8(&data[x + 1..y])?);
        if data.len() < y + 21 {
            Err("tree leaf truncated in sha1")?;
        }
        let sha1 = data[y + 1..y + 21].to_vec();

        println!("\tparsed TreeLeaf {}", path.to_string_lossy());
        Ok((TreeLeaf { mode, path, sha1 }, y + 21))
    }

    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        let mode = if self.mode.len() == 6 && self.mode.starts_with("0") {
            &self.mode.chars().skip(1).collect()
        } else {
            &self.mode
        };

        res.append(&mut mode.as_bytes().to_vec());
        res.push(b' ');
        res.append(&mut self.path.to_string_lossy().as_bytes().to_vec());
        res.push(b'\0');
        res.append(&mut self.sha1.clone());
        res
    }
}

#[cfg(test)]
mod test {
    use crate::{gitobject::TreeLeaf, hex::to_bytes};

    use super::TreeObject;
    use std::{fs::File, io::Read, path::PathBuf};

    #[test]
    fn deserialize_tree() {
        let mut f = File::open("test/tree").unwrap();
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).unwrap();
        let skip = buf.iter().position(|&b| b == b' ').unwrap_or(0) + 1;
        let tree = TreeObject::from(&buf[skip..]).unwrap();

        assert_eq!(
            tree.leaves.borrow()[0],
            TreeLeaf {
                path: PathBuf::from(".github"),
                mode: "040000".to_string(),
                sha1: to_bytes("a0ef2d9bb064800d8faceb96832b3ed26eb57412").unwrap()
            }
        );

        assert_eq!(tree.serialize(), buf[skip..].to_vec());
    }
}
