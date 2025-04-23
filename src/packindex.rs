use std::cmp::Ordering;
use std::io;
use std::io::{BufReader, Error, ErrorKind, Read, Seek, SeekFrom};

use hex::ToHex;

pub struct PackIndex<T: Read + Seek> {
    id: String,
    reader: BufReader<T>,
}

const INDEX_HEADER: &[u8; 4] = b"\xff\x74\x4f\x63";

impl<T: Read + Seek> PackIndex<T> {
    pub fn new(id: String, reader: BufReader<T>) -> PackIndex<T> {
        PackIndex { id, reader }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn find(&mut self, sha1: &[u8]) -> io::Result<Option<u64>> {
        assert_eq!(sha1.len(), 20);
        self.reader.seek(SeekFrom::Start(0))?;
        {
            let mut header = [0; 4];
            self.reader.read_exact(&mut header)?;
            if !header
                .iter()
                .zip(INDEX_HEADER)
                .map(|(&a, &b)| a == b)
                .reduce(|acc, v| acc && v)
                .unwrap_or(false)
            {
                println!("header {}", header.encode_hex::<String>());
                return Err(Error::from(ErrorKind::InvalidData));
            }

            self.reader.read_exact(&mut header)?;
            let version = u32::from_be_bytes(header);
            if version != 2 {
                println!("invalid version {}", version);
                return Err(Error::from(ErrorKind::InvalidData));
            }
        }
        println!("read header");

        let fanout = self.read_n_u32be(256)?;

        let hashes = {
            let mut hashes = vec![0; 20 * fanout[255] as usize];
            self.reader.read_exact(&mut hashes)?;
            hashes
                .chunks_exact(20)
                .into_iter()
                .map(|b| b.to_vec())
                .collect::<Vec<_>>()
        };

        let crc32 = self.read_n_u32be(fanout[255] as usize)?;
        let offsets = self.read_n_u32be(fanout[255] as usize)?;

        let index = match Self::search_hash(hashes, fanout, sha1) {
            Some(value) => value,
            None => return Ok(None),
        };

        Ok(Some(offsets[index] as u64))
    }

    fn search_hash(hashes: Vec<Vec<u8>>, fanout: Vec<u32>, sha1: &[u8]) -> Option<usize> {
        assert_eq!(sha1.len(), 20);
        let mut left = if sha1[0] == 0 {
            0
        } else {
            fanout[sha1[0] as usize - 1] as usize
        };
        let mut right = fanout[sha1[0] as usize] as usize;
        while left <= right {
            let i = (right - left) / 2 + left;
            match hashes[i].as_slice().cmp(sha1) {
                Ordering::Less => left = i + 1,
                Ordering::Greater => right = i - 1,
                Ordering::Equal => return Some(i),
            }
        }
        return None;
    }

    fn read_n_u32be(&mut self, n: usize) -> Result<Vec<u32>, Error> {
        let mut crc32 = vec![0; 4 * n];
        self.reader.read_exact(&mut crc32)?;
        Ok(crc32
            .chunks_exact(4)
            .into_iter()
            .map(|b| {
                let b: [u8; 4] = b.try_into().unwrap();
                u32::from_be_bytes(b)
            })
            .collect::<Vec<u32>>())
    }
}
