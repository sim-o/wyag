use std::error::Error;
use std::io;
use std::io::{BufReader, Read};

use hex::ToHex;
use log::debug;
use sha1::digest::Update;
use sha1::{Digest, Sha1};

use crate::pack::BinaryObject;

pub fn read_byte<T: Read>(reader: &mut BufReader<T>) -> io::Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn parse_variable_length<T: Read>(reader: &mut BufReader<T>) -> Result<usize, Box<dyn Error>> {
    let mut expanded: usize = 0;
    let mut shift = 0;
    loop {
        let byte = read_byte(reader)?;
        expanded |= byte as usize & 0x7f << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok(expanded)
}

pub fn parse_offset_delta<T: Read>(reader: &mut BufReader<T>) -> Result<u64, Box<dyn Error>> {
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

pub fn get_sha1(object_type: &BinaryObject, data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    Update::update(&mut hasher, object_type.name().as_bytes());
    Update::update(&mut hasher, b" ");
    Update::update(&mut hasher, data.len().to_string().as_bytes());
    Update::update(&mut hasher, b"\0");
    Update::update(&mut hasher, &data);
    hasher.finalize().encode_hex()
}

pub fn validate_sha1(sha1: &[u8; 20], object_type: &BinaryObject, data: &[u8]) {
    debug!("validating {} and len {}", object_type.name(), data.len());
    let result = get_sha1(object_type, data);
    assert_eq!(result, sha1.encode_hex::<String>());
}
