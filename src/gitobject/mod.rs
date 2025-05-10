use crate::gitobject::blob::BlobObject;
use crate::gitobject::commit::CommitObject;
use crate::gitobject::delta::{OffsetDeltaObject, RefDeltaObject};
use crate::gitobject::tag::TagObject;
use crate::gitobject::tree::TreeObject;
use crate::pack::BinaryObject;
use anyhow::*;
use std::fmt::Display;
use std::str::from_utf8;

pub mod blob;
pub mod commit;
pub mod delta;
pub mod tag;
pub mod tree;

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
    pub fn new(object_type: BinaryObject, data: Vec<u8>) -> Result<Box<Self>> {
        let object = match object_type {
            BinaryObject::Commit => {
                GitObject::Commit(CommitObject::from(data).context("parsing commit")?)
            }
            BinaryObject::Tree => GitObject::Tree(TreeObject::new(&data).context("parsing tree")?),
            BinaryObject::Blob => GitObject::Blob(BlobObject::from(data)),
            BinaryObject::Tag => GitObject::Tag(TagObject::from(data).context("parsing tag")?),
            BinaryObject::OffsetDelta(offset) => GitObject::OffsetDelta(
                OffsetDeltaObject::new(offset, &data).context("parsing offset delta")?,
            ),
            BinaryObject::RefDelta(sha1) => {
                GitObject::RefDelta(RefDeltaObject::new(sha1, &data).context("parsing ref delta")?)
            }
        };

        Ok(Box::new(object))
    }

    pub fn name(&self) -> &'static [u8] {
        match &self {
            GitObject::Blob(_) => b"blob",
            GitObject::Commit(_) => b"commit",
            GitObject::Tree(_) => b"tree",
            GitObject::Tag(_) => b"tag",
            _ => unimplemented!(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match &self {
            GitObject::Blob(blob) => blob.serialize().to_vec(),
            GitObject::Commit(commit) => commit.serialize(),
            GitObject::Tag(tag) => tag.serialize(),
            GitObject::Tree(tree) => tree.serialize(),
            GitObject::OffsetDelta(delta) => format!("OffsetDelta({:?})", delta).into_bytes(),
            GitObject::RefDelta(delta) => format!("RefDelta({:?})", delta).into_bytes(),
        }
    }
}
