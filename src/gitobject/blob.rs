use std::fmt::Display;
use std::str::from_utf8;

#[derive(Debug)]
pub struct BlobObject {
    pub data: Vec<u8>,
}

impl BlobObject {
    pub fn from(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn serialize(&self) -> &[u8] {
        self.data.as_slice()
    }
}

impl Display for BlobObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(from_utf8(&self.data).unwrap_or("<<BINARY>>"))
    }
}
