extern crate sha1;

use GitObject::{Tag, Tree};
use anyhow::{Context, Result};
use flate2::bufread::ZlibDecoder;
use log::{debug, warn};
use std::io::{BufReader, Read};
use std::io::{Seek, SeekFrom};

use crate::gitobject::GitObject::{Blob, Commit, OffsetDelta, RefDelta};
use crate::gitobject::{BlobObject, CommitObject, GitObject, TagObject, TreeObject};
use crate::gitobject::{OffsetDeltaObject, RefDeltaObject};
use crate::util::parse_offset_delta;

pub struct Pack<T: Read + Seek> {
    reader: BufReader<T>,
}

impl<T: Read + Seek> Pack<T> {
    pub fn new(reader: BufReader<T>) -> Result<Pack<T>> {
        let mut pack = Pack { reader };
        pack.check_header().context("check header")?;
        Ok(pack)
    }

    pub fn read_object_at(&mut self, offset: u64) -> Result<GitObject> {
        self.reader
            .seek(SeekFrom::Start(offset))
            .context("seek offset in packfile")?;
        self.read_object().context("reading object")
    }

    pub fn read(&mut self) -> Result<Vec<GitObject>> {
        self.reader
            .seek(SeekFrom::Start(0))
            .context("read from start of pack")?;
        let entries = self.check_header().context("check header")?;
        debug!("packfile has {} entries", entries);

        let mut result = Vec::with_capacity(entries);

        for n in 0..entries {
            debug!("reading entry {}", n);
            if let Ok(data) = self.read_object() {
                result.push(data);
            } else {
                warn!("failed to read entry");
            }
        }

        Ok(result)
    }

    pub fn read_object_data_at(&mut self, offset: u64) -> Result<(BinaryObject, Vec<u8>)> {
        self.reader
            .seek(SeekFrom::Start(offset))
            .with_context(|| format!("reading object at offset {}", offset))?;
        read_data(&mut self.reader)
    }

    fn read_object(&mut self) -> Result<GitObject> {
        read_object(&mut self.reader).context("reading object")
    }

    fn check_header(&mut self) -> Result<usize> {
        {
            let mut header = [0; 4];
            self.reader
                .read_exact(&mut header)
                .context("reading magic string")?;
            anyhow::ensure!(&header == b"PACK", "packfile corrupted, bad header");
        }

        {
            let mut version = [0; 4];
            self.reader
                .read_exact(&mut version)
                .context("reading version")?;
            anyhow::ensure!(
                u32::from_be_bytes(version) == 2,
                "Packfile version not supported: {}",
                u32::from_le_bytes(version)
            );
        };

        let entries = {
            let mut entries = [0; 4];
            self.reader
                .read_exact(&mut entries)
                .context("reading entries count")?;
            u32::from_be_bytes(entries) as usize
        };
        Ok(entries)
    }
}

fn read_compressed<T: Read>(reader: &mut BufReader<T>, size: usize) -> Result<Vec<u8>> {
    debug!("reading compressed: {size}");
    let mut bytes = vec![b'\0'; size];
    let mut z = ZlibDecoder::new(reader);
    z.read_exact(&mut bytes)
        .context("reading compressed bytes")?;
    debug!("\treading done");
    Ok(bytes)
}

#[derive(Copy, Clone)]
pub enum BinaryObject {
    Blob,
    Commit,
    Tag,
    Tree,
    OffsetDelta(u64),
    RefDelta([u8; 20]),
}

impl BinaryObject {
    pub fn name(&self) -> String {
        match self {
            BinaryObject::Blob => "blob",
            BinaryObject::Commit => "commit",
            BinaryObject::Tag => "tag",
            BinaryObject::Tree => "tree",
            BinaryObject::OffsetDelta(_) => "offsetdelta",
            BinaryObject::RefDelta(_) => "refdelta",
        }
        .to_string()
    }
}

pub fn read_data<T: Read>(reader: &mut BufReader<T>) -> Result<(BinaryObject, Vec<u8>)> {
    debug!("reading object");
    let mut read = [0; 1];
    reader
        .read_exact(&mut read)
        .context("reading object type")?;
    let type_id = (read[0] >> 4) & 0x7;

    let size = {
        let mut size = read[0] as usize & 0xf;

        let mut shift = 4;
        while (read[0] & 0b1000_0000) != 0 {
            reader
                .read_exact(&mut read)
                .context("reading object size")?;
            size |= (read[0] as usize & 0x7f) << shift;
            shift += 7;
        }
        size
    };

    let object_type = match type_id {
        0b001 => BinaryObject::Commit,
        0b010 => BinaryObject::Tree,
        0b011 => BinaryObject::Blob,
        0b100 => BinaryObject::Tag,
        0b110 => BinaryObject::OffsetDelta(
            parse_offset_delta(reader).context("reading offset delta offset")?,
        ),
        0b111 => {
            BinaryObject::RefDelta(read_sha1(reader).context("reading ref delta reference sha1")?)
        }
        _ => anyhow::bail!("unexpected object type {}", type_id),
    };

    debug!("read object {}, size: {}", object_type.name(), size);
    Ok((
        object_type,
        read_compressed(reader, size).with_context(|| {
            format!(
                "reading compressed object data for type: {}",
                object_type.name()
            )
        })?,
    ))
}

pub fn read_object<T: Read>(reader: &mut BufReader<T>) -> Result<GitObject> {
    let (object_type, data) = read_data(reader).context("reading object")?;
    parse_object_data(object_type, data).context("reading object")
}

pub fn parse_object_data(object_type: BinaryObject, data: Vec<u8>) -> Result<GitObject> {
    let object = match object_type {
        BinaryObject::Commit => Commit(CommitObject::from(&data).context("parsing commit")?),
        BinaryObject::Tree => Tree(TreeObject::new(&data).context("parsing tree")?),
        BinaryObject::Blob => Blob(BlobObject::from(data)),
        BinaryObject::Tag => Tag(TagObject::from(&data).context("parsing tag")?),
        BinaryObject::OffsetDelta(offset) => {
            OffsetDelta(OffsetDeltaObject::new(offset, &data).context("parsing offset delta")?)
        }
        BinaryObject::RefDelta(sha1) => {
            RefDelta(RefDeltaObject::new(sha1, &data).context("parsing ref delta")?)
        }
    };

    Ok(object)
}

fn read_sha1<T: Read>(reader: &mut BufReader<T>) -> Result<[u8; 20]> {
    let mut sha1ref = [0; 20];
    reader.read_exact(&mut sha1ref).context("reading sha1")?;
    Ok(sha1ref)
}
