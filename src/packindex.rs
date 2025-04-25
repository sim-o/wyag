use std::cmp::Ordering;
use std::io;
use std::io::{BufReader, Error, ErrorKind, Read};

use hex::ToHex;
use log::{debug, error, trace};

pub struct PackIndex {
    id: [u8; 20],
    fanout: [u32; 256],
    hashes: Vec<[u8; 20]>,
    crc32: Vec<u32>,
    offsets: Vec<u32>,
    offsets64: Vec<u64>,
    pack_sha1: [u8; 20],
    index_sha1: [u8; 20],
}

const INDEX_HEADER: &[u8; 4] = b"\xff\x74\x4f\x63";

impl PackIndex {
    pub fn new<T: Read>(
        id: [u8; 20],
        mut reader: BufReader<T>,
    ) -> Result<PackIndex, Box<dyn std::error::Error>> {
        check_header(&mut reader)?;
        let fanout: [u32; 256] = read_n_u32be(&mut reader, 256)?.try_into().unwrap();
        let hashes = read_hashes(&mut reader, fanout[255] as usize)?;
        let crc32 = read_n_u32be(&mut reader, fanout[255] as usize)?;
        let offsets = read_n_u32be(&mut reader, fanout[255] as usize)?;
        let offsets64 = read_n_u64be(
            &mut reader,
            offsets.iter().filter(|&n| n & 0x8000_0000 != 0).count(),
        )?;
        Ok(PackIndex {
            id,
            fanout,
            hashes,
            crc32,
            offsets,
            offsets64,
            pack_sha1: read_hash(&mut reader)?,
            index_sha1: read_hash(&mut reader)?,
        })
    }

    pub fn id(&self) -> [u8; 20] {
        self.id
    }

    pub fn find(&self, sha1: [u8; 20]) -> Option<u64> {
        let index = match self.search_hash(sha1) {
            Some(value) => value,
            None => return None,
        };

        let offset = self.offsets[index];
        if offset & 0x8000_0000 != 0 {
            let i: usize = (offset & 0x7fff_ffff) as usize;
            return Some(self.offsets64[i]);
        }

        Some(offset as u64)
    }

    fn search_hash(&self, sha1: [u8; 20]) -> Option<usize> {
        assert_eq!(sha1.len(), 20);
        let mut left = if sha1[0] == 0 {
            0
        } else {
            self.fanout[sha1[0] as usize - 1] as usize
        };
        let mut right = self.fanout[sha1[0] as usize] as usize;
        while left <= right {
            let i = (right - left) / 2 + left;
            match self.hashes[i].as_slice().cmp(&sha1) {
                Ordering::Less => left = i + 1,
                Ordering::Greater => right = i - 1,
                Ordering::Equal => return Some(i),
            }
        }
        None
    }
}

fn check_header<T: Read>(reader: &mut BufReader<T>) -> io::Result<()> {
    {
        let mut header = [0; 4];
        reader.read_exact(&mut header)?;
        if !header
            .iter()
            .zip(INDEX_HEADER)
            .map(|(&a, &b)| a == b)
            .reduce(|acc, v| acc && v)
            .unwrap_or(false)
        {
            debug!("header {}", header.encode_hex::<String>());
            return Err(Error::from(ErrorKind::InvalidData));
        }

        reader.read_exact(&mut header)?;
        let version = u32::from_be_bytes(header);
        if version != 2 {
            error!("invalid version {}", version);
            return Err(Error::from(ErrorKind::InvalidData));
        }
    }
    trace!("read header");
    Ok(())
}

fn read_hash<T: Read>(reader: &mut BufReader<T>) -> Result<[u8; 20], Error> {
    let mut hash = vec![0; 20];
    reader.read_exact(&mut hash)?;
    Ok(hash.try_into().unwrap())
}

fn read_hashes<T: Read>(reader: &mut BufReader<T>, items: usize) -> Result<Vec<[u8; 20]>, Error> {
    let hashes = {
        let mut hashes = vec![0; 20 * items];
        reader.read_exact(&mut hashes)?;
        hashes
            .chunks_exact(20)
            .into_iter()
            .map(|b| b.try_into().unwrap())
            .collect::<Vec<_>>()
    };
    Ok(hashes)
}

fn read_n_u32be<T: Read>(reader: &mut BufReader<T>, n: usize) -> Result<Vec<u32>, Error> {
    let mut buf = vec![0; size_of::<u32>() * n];
    reader.read_exact(&mut buf)?;
    Ok(buf
        .chunks_exact(size_of::<u32>())
        .into_iter()
        .map(|b| {
            let b: [u8; size_of::<u32>()] = b.try_into().unwrap();
            u32::from_be_bytes(b)
        })
        .collect::<Vec<u32>>())
}

fn read_n_u64be<T: Read>(reader: &mut BufReader<T>, n: usize) -> Result<Vec<u64>, Error> {
    let mut buf = vec![0; size_of::<u64>() * n];
    reader.read_exact(&mut buf)?;
    Ok(buf
        .chunks_exact(size_of::<u64>())
        .into_iter()
        .map(|b| {
            let b: [u8; size_of::<u64>()] = b.try_into().unwrap();
            u64::from_be_bytes(b)
        })
        .collect::<Vec<u64>>())
}
