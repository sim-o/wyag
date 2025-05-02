use std::fmt::Display;
use std::str::from_utf8;

#[derive(Debug)]
pub struct BlobObject<'a> {
    pub data: &'a [u8],
}

impl<'a> BlobObject<'a> {
    pub fn from(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn serialize(&self) -> &'a [u8] {
        self.data
    }
}

impl<'a> Display for BlobObject<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(from_utf8(self.data).unwrap_or("<<BINARY>>"))
    }
}