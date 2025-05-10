use anyhow::Context;
use hex::ToHex;
use log::debug;
use std::fmt::Debug;
use std::path::PathBuf;
use std::str::from_utf8;

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
        Ok(Self { leaves })
    }

    pub fn leaf_iter(&self) -> impl Iterator<Item=&TreeLeaf> {
        self.leaves.iter()
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
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

#[cfg(test)]
mod test {
    use crate::gitobject::tree::TreeLeaf;
    use crate::gitobject::tree::TreeObject;
    use hex::FromHex;
    use std::{fs::File, io::Read, path::PathBuf};

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
