extern crate sha1;

use crate::util::parse_offset_delta;
use anyhow::{Context, Result};
use flate2::bufread::ZlibDecoder;
use log::debug;
use std::cell::RefCell;
use std::io;
use std::io::{BufReader, Read};
use std::io::{Seek, SeekFrom};

pub struct Pack<T: Read + Seek> {
    reader: RefCell<BufReader<T>>,
}

impl<T: Read + Seek> Pack<T> {
    pub fn new(reader: BufReader<T>) -> Result<Pack<T>> {
        let pack = Pack { reader: RefCell::new(reader) };
        pack.check_header().context("check header")?;
        Ok(pack)
    }

    pub fn read_all(&self) -> Result<Vec<(BinaryObject, Vec<u8>)>> {
        {
            self.reader
                .borrow_mut()
                .seek(SeekFrom::Start(0))
                .context("read from start of pack")?;
        }
        let entries = self.check_header().context("check header")?;
        debug!("packfile has {} entries", entries);

        let mut result = Vec::with_capacity(entries);

        for n in 0..entries {
            debug!("reading entry {}", n);
            let mut data = Vec::new();
            let object_type = read_data(&mut self.reader.borrow_mut(), &mut data)?;
            result.push((object_type, data));
        }

        Ok(result)
    }

    pub fn read_object_data_at(&self, offset: u64, data: &mut Vec<u8>) -> Result<BinaryObject> {
        let mut reader = self.reader.borrow_mut();
        reader
            .seek(SeekFrom::Start(offset))
            .with_context(|| format!("reading object at offset {}", offset))?;
        read_data(&mut reader, data)
    }

    fn check_header(&self) -> Result<usize> {
        let mut reader = self.reader.borrow_mut();
        {
            let mut header = [0; 4];
            reader
                .read_exact(&mut header)
                .context("reading magic string")?;
            anyhow::ensure!(&header == b"PACK", "packfile corrupted, bad header");
        }

        {
            let mut version = [0; 4];
            reader
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
            reader
                .read_exact(&mut entries)
                .context("reading entries count")?;
            u32::from_be_bytes(entries) as usize
        };
        Ok(entries)
    }
}

fn read_compressed<T: Read>(
    reader: &mut BufReader<T>,
    size: usize,
    bytes: &mut Vec<u8>,
) -> io::Result<()> {
    debug!("reading compressed: {}", bytes.len());
    bytes.resize(size, 0);
    let mut z = ZlibDecoder::new(reader);
    z.read_exact(bytes)
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum BinaryObject {
    Blob,
    Commit,
    Tag,
    Tree,
    OffsetDelta(u64),
    RefDelta([u8; 20]),
}

impl BinaryObject {
    pub fn is_delta(&self) -> bool {
        matches!(self, BinaryObject::OffsetDelta(_) | BinaryObject::RefDelta(_))
    }
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

pub fn read_data<T: Read>(reader: &mut BufReader<T>, data: &mut Vec<u8>) -> Result<BinaryObject> {
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

    read_compressed(reader, size, data).with_context(|| {
        format!(
            "reading compressed object data for type: {}",
            object_type.name()
        )
    })?;
    Ok(object_type)
}

fn read_sha1<T: Read>(reader: &mut BufReader<T>) -> Result<[u8; 20]> {
    let mut sha1ref = [0; 20];
    reader.read_exact(&mut sha1ref).context("reading sha1")?;
    Ok(sha1ref)
}
