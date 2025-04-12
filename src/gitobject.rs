use std::{fmt::Display, str::from_utf8};

pub trait GitObject: Display {
    fn init(&self);
    fn serialize(&self) -> &[u8];
    fn deserialize(&self) -> Box<dyn GitObject>;
}

pub struct BlobObject {
    data: Vec<u8>,
}

impl BlobObject {
    pub fn new() -> Self {
        let ret = BlobObject { data: Vec::new() };
        ret.init();
        ret
    }

    pub fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl GitObject for BlobObject {
    fn init(&self) {}

    fn serialize(&self) -> &[u8] {
        todo!()
    }

    fn deserialize(&self) -> Box<dyn GitObject> {
        todo!()
    }
}

impl Display for BlobObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(from_utf8(&self.data).unwrap_or("<<BINARY>>"))
    }
}
