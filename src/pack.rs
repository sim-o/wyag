extern crate libflate;
extern crate sha1;

use std::{error::Error, io::{BufReader, Read}};
use std::io::{Seek, SeekFrom};

use flate2::bufread::ZlibDecoder;

use crate::gitobject::{BlobObject, CommitObject, GitObject, TreeObject};
use crate::gitobject::{OffsetDeltaObject, RefDeltaObject};
use crate::gitobject::GitObject::{OffsetDelta, RefDelta};
use crate::util::parse_offset_delta;

pub struct Pack<T: Read + Seek> {
    reader: BufReader<T>,
}

impl<T: Read + Seek> Pack<T> {
    pub fn new(reader: BufReader<T>) -> Pack<T> {
        Pack { reader }
    }

    pub fn read_object_at(&mut self, offset: usize) -> Result<GitObject, Box<dyn Error>> {
        self.check_header()?;
        self.reader.seek(SeekFrom::Start(offset as u64))?;
        Ok(self.read_object()?)
    }

    pub fn read(&mut self) -> Result<Vec<GitObject>, Box<dyn Error>> {
        let entries = self.check_header()?;
        println!("packfile has {} entries", entries);

        let mut result = Vec::with_capacity(entries);

        for n in 0..entries {
            println!("reading entry {}", n);
            if let Ok(data) = self.read_object() {
                result.push(data);
            } else {
                println!("failed to read entry");
            }
        }

        Ok(result)
    }

    fn read_object(&mut self) -> Result<GitObject, Box<dyn Error>> {
        let mut read = [0; 1];
        self.reader.read_exact(&mut read)?;
        let _type = (read[0] >> 4) & 0x7;

        let size = {
            let mut size = read[0] as usize & 0xf;

            let mut shift = 4;
            while (read[0] & 0b1000_0000) != 0 {
                self.reader.read_exact(&mut read)?;
                size |= (read[0] as usize & 0x7f) << shift;
                shift += 7;
            }
            size
        };

        println!("reading type: 0b{:b} size = {}", _type, size);
        if (1..=0b100).contains(&_type) {
            let bytes = read_compressed(&mut self.reader, size)?;

            let object = match _type {
                0b001 => GitObject::Commit(CommitObject::from(&bytes)?),
                0b010 => GitObject::Tree(TreeObject::from(&bytes)?),
                0b011 => GitObject::Blob(BlobObject::from(bytes.clone())),
                0b100 => todo!("Tag object not implemented"),
                _ => unreachable!(),
            };

            Ok(object)
        } else if _type == 0b110 {
            let offset_delta = parse_offset_delta(&mut self.reader)?;
            let data = read_compressed(&mut self.reader, size)?;
            Ok(OffsetDelta(OffsetDeltaObject::new(offset_delta, &data)?))
        } else if _type == 0b111 {
            let mut sha1ref = [0; 20];
            self.reader.read_exact(&mut sha1ref)?;
            let data = read_compressed(&mut self.reader, size)?;
            Ok(RefDelta(RefDeltaObject::new(sha1ref, &data)?))
        } else {
            return Err("type 0 not implemented")?;
        }
    }

    fn check_header(&mut self) -> Result<usize, Box<dyn Error>> {
        {
            let mut header = [0; 4];
            self.reader.read_exact(&mut header)?;
            if &header != b"PACK" {
                Err("packfile corrupted, bad header")?;
            }
        }

        {
            let mut version = [0; 4];
            self.reader.read_exact(&mut version)?;
            if u32::from_be_bytes(version) != 2 {
                Err(format!(
                    "Packfile version not supported: {}",
                    u32::from_le_bytes(version)
                ))?;
            }
        };

        let entries = {
            let mut entries = [0; 4];
            self.reader.read_exact(&mut entries)?;
            u32::from_be_bytes(entries) as usize
        };
        Ok(entries)
    }
}

fn read_compressed<T: Read>(reader: &mut BufReader<T>, size: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![b'\0'; size];
    let mut z = ZlibDecoder::new(reader);
    z.read_exact(&mut bytes)?;
    Ok(bytes)
}
