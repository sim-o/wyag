use crate::kvlm::{kvlm_parse, kvlm_serialize};
use std::{collections::HashMap, error::Error, fmt::Display, str::from_utf8};

pub enum GitObject {
    Blob(BlobObject),
    Commit(CommitObject),
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
        }
    }
}

impl GitObject {
    pub fn name(&self) -> &'static [u8] {
        match &self {
            GitObject::Blob(_) => b"blob",
            GitObject::Commit(_) => b"commit",
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match &self {
            GitObject::Blob(blob) => blob.serialize(),
            GitObject::Commit(commit) => commit.serialize(),
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
    pub fn from(data: Vec<u8>) -> Result<CommitObject, Box<dyn Error>> {
        Ok(CommitObject {
            kvlm: kvlm_parse(&data)?,
        })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}
