use crate::hashingreader::HashingReader;
use crate::pack::Pack;
use anyhow::{bail, ensure, Context};
use hex::ToHex;
use log::{debug, info, trace};
use std::cmp::Ordering;
use std::io;
use std::io::{BufReader, Read};

pub struct PackIndex {
    fanout: [u32; 256],
    hashes: Vec<[u8; 20]>,
    crc32: Vec<u32>,
    offsets: Vec<u32>,
    offsets64: Vec<u64>,
    pack_sha1: [u8; 20],
    index_sha1: [u8; 20],
}

impl PackIndex {
    pub fn new<T: Read>(reader: BufReader<T>) -> anyhow::Result<PackIndex> {
        let mut reader = HashingReader::new(reader);

        check_header(&mut reader).context("check header")?;
        let fanout: [u32; 256] = read_n_u32be(&mut reader, 256)
            .context("reading fanout table")?
            .try_into()
            .unwrap();
        let hashes = read_hashes(&mut reader, fanout[255] as usize).context("reading hashes")?;
        let crc32 =
            read_n_u32be(&mut reader, fanout[255] as usize).context("reading crc32 table")?;
        let offsets =
            read_n_u32be(&mut reader, fanout[255] as usize).context("reading offsets table")?;
        let offsets64 = read_n_u64be(
            &mut reader,
            offsets.iter().filter(|&n| n & 0x8000_0000 != 0).count(),
        )
            .context("reading 64 bit offsets table")?;
        let pack_sha1 = read_hash(&mut reader).context("reading pack sha1")?;
        let actual_index_sha1 = reader.finalize();
        let index_sha1 = read_hash(&mut reader).context("reading index sha1")?;

        info!(
            "pack sha: {}, index sha: {}, actual index sha: {}",
            pack_sha1.encode_hex::<String>(),
            index_sha1.encode_hex::<String>(),
            actual_index_sha1.encode_hex::<String>()
        );
        assert_eq!(actual_index_sha1, index_sha1);

        Ok(PackIndex {
            fanout,
            hashes,
            crc32,
            offsets,
            offsets64,
            pack_sha1,
            index_sha1,
        })
    }

    pub fn id(&self) -> [u8; 20] {
        self.pack_sha1
    }

    pub fn find(&self, sha1: [u8; 20]) -> Option<u64> {
        let index = self.search_hash(sha1)?;

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
            self.fanout[sha1[0] as usize - 1] as usize + 1
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

    pub fn iter(&self) -> PackIndexIterator {
        PackIndexIterator {
            index: self,
            item: 0,
        }
    }
}

fn check_header<T: Read>(reader: &mut HashingReader<T>) -> anyhow::Result<()> {
    {
        let mut header = [0; 4];
        reader.read_exact(&mut header).context("reading header")?;
        if header != *b"\xff\x74\x4f\x63" {
            debug!("header {}", header.encode_hex::<String>());
            bail!("invalid header");
        }

        reader
            .read_exact(&mut header)
            .context("reading header version")?;
        let version = u32::from_be_bytes(header);
        ensure!(
            version == 2,
            "only version 2 supported, pack index is {version}"
        );
    }
    trace!("read header");
    Ok(())
}

fn read_hash<T: Read>(reader: &mut HashingReader<T>) -> io::Result<[u8; 20]> {
    let mut hash = vec![0; 20];
    reader.read_exact(&mut hash)?;
    Ok(hash.try_into().unwrap())
}

fn read_hashes<T: Read>(reader: &mut HashingReader<T>, items: usize) -> io::Result<Vec<[u8; 20]>> {
    let hashes = {
        let mut hashes = vec![0; 20 * items];
        reader.read_exact(&mut hashes)?;
        hashes
            .chunks_exact(20)
            .map(|b| b.try_into().unwrap())
            .collect::<Vec<_>>()
    };
    Ok(hashes)
}

fn read_n_u32be<T: Read>(reader: &mut HashingReader<T>, n: usize) -> io::Result<Vec<u32>> {
    let mut buf = vec![0; size_of::<u32>() * n];
    reader.read_exact(&mut buf)?;
    Ok(buf
        .chunks_exact(size_of::<u32>())
        .map(|b| {
            let b: [u8; size_of::<u32>()] = b.try_into().unwrap();
            u32::from_be_bytes(b)
        })
        .collect::<Vec<u32>>())
}

fn read_n_u64be<T: Read>(reader: &mut HashingReader<T>, n: usize) -> io::Result<Vec<u64>> {
    let mut buf = vec![0; size_of::<u64>() * n];
    reader.read_exact(&mut buf)?;
    Ok(buf
        .chunks_exact(size_of::<u64>())
        .map(|b| {
            let b: [u8; size_of::<u64>()] = b.try_into().unwrap();
            u64::from_be_bytes(b)
        })
        .collect::<Vec<u64>>())
}

pub struct PackIndexIterator<'a> {
    index: &'a PackIndex,
    item: usize,
}

impl Iterator for PackIndexIterator<'_> {
    type Item = PackIndexItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.item >= self.index.hashes.len() {
            return None;
        }

        let offset = if self.index.offsets[self.item] & (1u32 << 31) == 0 {
            self.index.offsets[self.item] as u64
        } else {
            self.index.offsets64[(self.index.offsets[self.item] ^ (1u32 << 31)) as usize]
        };
        let hash = self.index.hashes[self.item];
        self.item += 1;
        Some(PackIndexItem(hash, offset))
    }
}

pub struct PackIndexItem(pub [u8; 20], pub u64);
