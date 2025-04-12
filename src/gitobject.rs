use std::{fmt::Debug, fmt::Display, str::from_utf8};

pub enum GitObject {
    Blob(BlobObject),
}

impl Display for GitObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            GitObject::Blob(blob) => f.write_fmt(format_args!(
                "blob {}",
                from_utf8(&blob.data).unwrap_or("<<BINARY>>")
            )),
        }
    }
}

impl GitObject {
    pub fn name(&self) -> &'static [u8] {
        match &self {
            GitObject::Blob(_) => b"blob",
        }
    }

    pub fn serialize(&self) -> &Vec<u8> {
        use GitObject::Blob;
        match &self {
            Blob(blob) => blob.serialize(),
        }
    }
}

pub struct BlobObject {
    data: Vec<u8>,
}

impl BlobObject {
    pub fn new() -> Self {
        BlobObject { data: Vec::new() }
    }

    pub fn from(data: Vec<u8>) -> Self {
        Self { data }
    }

    fn serialize(&self) -> &Vec<u8> {
        &self.data
    }

    fn deserialize(&self) -> BlobObject {
        todo!()
    }
}

impl Display for BlobObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(from_utf8(&self.data).unwrap_or("<<BINARY>>"))
    }
}
