use crate::{
    hex::{hex, to_bytes},
    kvlm::{kvlm_parse, kvlm_serialize},
};
use std::{collections::HashMap, error::Error, fmt::Display, path::PathBuf, str::from_utf8};

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
    pub fn from(data: &Vec<u8>) -> Result<CommitObject, Box<dyn Error>> {
        Ok(CommitObject {
            kvlm: kvlm_parse(data)?,
        })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}

pub struct TreeObject {
    leaves: Vec<TreeLeaf>,
}

impl TreeObject {
    pub fn from(data: &[u8]) -> Result<TreeObject, Box<dyn Error>> {
        let mut leaves = Vec::new();
        let skip = data
            .iter()
            .position(|&b| b == b'\0')
            .ok_or("tree object did not contain null")?
            + 1;
        let mut rem = &data[skip..];
        while !rem.is_empty() {
            let (leaf, len) = TreeLeaf::parse_one(rem)?;
            println!("read leaf {}", leaf.path.to_string_lossy());
            leaves.push(leaf);
            rem = &rem[len..];
        }
        Ok(Self { leaves })
    }

    fn serialize(&self) -> Vec<u8> {
        todo!()
    }
}

#[derive(PartialEq, Debug)]
pub struct TreeLeaf {
    mode: String,
    path: PathBuf,
    sha1: String,
}

impl TreeLeaf {
    fn parse_one(data: &[u8]) -> Result<(Self, usize), Box<dyn Error>> {
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
        let sha1 = hex(&data[y + 1..y + 21]);

        Ok((TreeLeaf { mode, path, sha1 }, y + 21))
    }

    fn serialize(&self) -> Vec<u8> {
        let mut res = Vec::new();
        res.append(&mut self.mode.as_bytes().to_vec());
        res.push(b' ');
        res.append(&mut self.path.to_string_lossy().as_bytes().to_vec());
        res.push(b'\0');
        res.append(&mut to_bytes(&self.sha1).to_vec());
        res
    }
}

#[cfg(test)]
mod test {
    use crate::gitobject::TreeLeaf;

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
            tree.leaves,
            vec![
                TreeLeaf {
                    path: PathBuf::from(".github"),
                    mode: "040000".to_string(),
                    sha1: "a0ef2d9bb06480d8faceb96832b3ed26eb57412".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from(".gitignore"),
                    mode: "100644".to_string(),
                    sha1: "20717a631f8661cb909e6ef3462965ad8b56fba6".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from(".husky"),
                    mode: "040000".to_string(),
                    sha1: "522546b7b66d4cba2aab71dc1499282855e19fad".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from(".prettierrc.yaml"),
                    mode: "100644".to_string(),
                    sha1: "b8ebc29292668a38d4434ab6e3c9be6df817fe".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("README.md"),
                    mode: "100644".to_string(),
                    sha1: "a96b3e89693ae60d11a8629f8747fb7db52d6d2".to_string(),
                },
                TreeLeaf {
                    path: PathBuf::from("assets"),
                    mode: "040000".to_string(),
                    sha1: "74f5d9674588fb5d84ed1d2805c9febfedfc05".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("eslint.config.mjs"),
                    mode: "100644".to_string(),
                    sha1: "f6f97552d26aaabe78d3543748735b9943437".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("github.png"),
                    mode: "100644".to_string(),
                    sha1: "dfc1ca2489c5d2bbb4642e923514f92c75a7fde".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("jest.config.ts"),
                    mode: "100644".to_string(),
                    sha1: "448f53c9d4f27a56ae9a21f5bbcff5e88b9dee5".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("linkedin.png"),
                    mode: "100644".to_string(),
                    sha1: "dd5b2e77baf34226b94a5ed5ccb2e6ac78b3dba".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("package-lock.json"),
                    mode: "100644".to_string(),
                    sha1: "a93a84cb1ae5ce638037c5aef4071dbf1f60".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("package.json"),
                    mode: "100644".to_string(),
                    sha1: "e124e21333b86f4f4729f21f1cd9619ce631a9d".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("portrait.png"),
                    mode: "100644".to_string(),
                    sha1: "776246317496c7cac59e6eea5b815f9b9accb8".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("resume.yaml"),
                    mode: "100644".to_string(),
                    sha1: "fc3d35c0c60a4b3cd039f82cf9bf8549b5cfe".to_string()
                },
                TreeLeaf {
                    path: PathBuf::from("src"),
                    mode: "040000".to_string(),
                    sha1: "2ffe9e9c1e894c1594525397bd26f8bcc73e11b".to_string()
                },
            ]
        );
    }
}
