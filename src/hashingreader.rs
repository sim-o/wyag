use sha1::digest::core_api::CoreWrapper;
use sha1::{Digest, Sha1, Sha1Core};
use std::io;
use std::io::Read;

pub struct HashingReader<T: Read> {
    hasher: CoreWrapper<Sha1Core>,
    inner: T,
}

impl<T: Read> Read for HashingReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let res = self.inner.read(buf);
        if let Ok(size) = res {
            self.hasher.update(&buf[..size]);
        }
        res
    }
}

impl<T: Read> HashingReader<T> {
    pub fn new(inner: T) -> Self {
        Self {
            hasher: Sha1::new(),
            inner,
        }
    }

    pub fn finalize(&mut self) -> [u8; 20] {
        self.hasher.finalize_reset().into()
    }
}
