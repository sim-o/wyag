use crate::pack::BinaryObject;
use hex::ToHex;
use log::{debug, trace};
use sha1::digest::Update;
use sha1::{Digest, Sha1};
use std::io;
use std::io::{BufReader, Read};
use std::str::from_utf8;

pub fn read_byte<T: Read>(reader: &mut T) -> io::Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn get_delta_hdr_size<T: Read>(reader: &mut T) -> io::Result<usize> {
    let mut size: usize = 0;
    let mut i = 0;
    loop {
        let cmd = read_byte(reader)?;
        size |= (cmd as usize & 0x7f) << i;
        i += 7;
        if cmd & 0x80 == 0 {
            break;
        }
    }
    Ok(size)
}

pub fn parse_offset_delta<T: Read>(reader: &mut BufReader<T>) -> io::Result<u64> {
    let mut b = read_byte(reader)?;
    let mut offset = b as u64 & 0x7f;

    while b & 0x80 > 0 {
        offset += 1;
        offset <<= 7;
        b = read_byte(reader)?;
        offset += b as u64 & 0x7f;
    }

    Ok(offset)
}

pub fn get_sha1(object_type: BinaryObject, data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    Update::update(&mut hasher, object_type.name().as_bytes());
    Update::update(&mut hasher, b" ");
    Update::update(&mut hasher, data.len().to_string().as_bytes());
    Update::update(&mut hasher, b"\0");
    Update::update(&mut hasher, data);
    hasher.finalize().into()
}

pub fn validate_sha1(sha1: [u8; 20], object_type: BinaryObject, data: &[u8]) -> anyhow::Result<()> {
    debug!("validating {} and len {}", object_type.name(), data.len());
    let result = get_sha1(object_type, data);
    trace!(
        "validating object [[{}]]",
        from_utf8(data).unwrap_or("<<bad utf8>>")
    );
    anyhow::ensure!(
        result == sha1,
        "sha1 did not validate for object {} with type {}, received {}",
        sha1.encode_hex::<String>(),
        object_type.name(),
        result.encode_hex::<String>(),
    );
    Ok(())
}
