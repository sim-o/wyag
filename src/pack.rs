extern crate libflate;
extern crate sha1;

use std::{error::Error, fmt::Display, fs::File, io::{BufReader, Read}, io};
use std::io::ErrorKind;
use std::str::from_utf8;

use bytes::Buf;
use flate2::bufread::ZlibDecoder;
use sha1::Digest;

use crate::{
    gitobject::{BlobObject, CommitObject, GitObject, TreeObject},
    hex::hex,
};

pub struct Pack<T: Read> {
    reader: BufReader<T>,
}

impl <T: Read> Pack<T> {
    pub fn new(reader: T) -> Pack<T> {
        Pack { reader }
    }

    pub fn read(&mut self) -> Result<Vec<GitObject>, Box<dyn Error>> {
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
        }

        let entries = {
            let mut entries = [0; 4];
            (&self.reader).read_exact(&mut entries)?;
            u32::from_be_bytes(entries) as usize
        };
        println!("packfile has {} entries", entries);

        let mut result = Vec::with_capacity(entries);

        for n in 0..entries {
            println!("reading entry {}", n);
            let data = {
                let mut read = [0; 1];
                (&self.reader).read_exact(&mut read)?;
                let _type = (read[0] >> 4) & 0x7;

                let size = {
                    let mut size = read[0] as usize & 0xf;

                    let mut shift = 4;
                    while (read[0] & 0b1000_0000) != 0 {
                        (&self.reader).read_exact(&mut read)?;
                        size |= (read[0] as usize & 0x7f) << shift;
                        shift += 7;
                    }
                    size
                };

                println!("reading type: 0b{:b} size = {}", _type, size);

                if (1..=0b100).contains(&_type) {
                    let bytes = read_compressed(&mut (&self.reader), size)?;

                    println!("inflated data {}", bytes.len());

                    match _type {
                        0b001 => GitObject::Commit(CommitObject::from(&bytes)?),
                        0b010 => GitObject::Tree(TreeObject::from(&bytes)?),
                        0b011 => GitObject::Blob(BlobObject::from(bytes.clone())),
                        0b100 => todo!("Tag object not implemented"),
                        _ => unreachable!(),
                    }
                } else if _type == 0b110 || _type == 0b111 {
                    // Deltas
                    if _type == 0b110 {
                        let offset_delta = parse_offset_delta(&mut (&self.reader))?;
                        println!("offset_delta: {}", offset_delta);
                    } else {
                        let mut sha1ref = [0; 20];
                        (&self.reader).read_exact(&mut sha1ref)?;
                        println!("sha1ref: {}", hex(&sha1ref));
                    }

                    let data = read_compressed(&mut (&self.reader), size)?;
                    let instructions = parse_delta_data(&data);
                    if let Ok(instructions) = instructions {
                        println!(
                            "instructions: {}",
                            instructions
                                .iter()
                                .map(|i| (i).to_string())
                                .collect::<String>()
                        );
                    }

                    continue;
                } else {
                    return Err("type 0 not implemented")?;
                }
            };

            result.push(data);
        }

        Ok(result)
    }
}

fn read_compressed(reader: &mut BufReader<File>, size: usize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = vec![b'\0'; size];
    let mut z = ZlibDecoder::new(reader);
    z.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn parse_delta_data(reader: &[u8]) -> Result<Vec<DeltaInstruction>, Box<dyn Error>> {
    let mut reader = BufReader::new(reader.reader());
    let base_size = parse_variable_length(&mut reader)?;
    let result_size = parse_variable_length(&mut reader)?;

    let mut instructions = Vec::new();
    loop {
        let opcode = read_byte(&mut reader);
        match opcode {
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                panic!("unexpected read error: {}", err);
            }
            Ok(opcode) => {
                if opcode == 0 {
                    panic!("invalid delta opcode 0");
                }

                let instr = if opcode & 0x80 == 0 {
                    let data = {
                        let mut buf = vec![0; opcode as usize];
                        reader.read_exact(&mut buf)?;
                        buf
                    };
                    DeltaInstruction::Insert(data)
                } else {
                    parse_copy_instruction(opcode, &mut reader)?
                };

                instructions.push(instr);
            }
        }
    }
    Ok(instructions)
}

fn read_byte<T: Read>(reader: &mut BufReader<T>) -> io::Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn parse_copy_instruction<T: Read>(
    opcode: u8,
    reader: &mut BufReader<T>,
) -> Result<DeltaInstruction, Box<dyn Error>> {
    let cp_off: usize = {
        let mut cp_off: usize = 0;
        for i in 0..4 {
            if opcode & (1 << i) != 0 {
                let x = read_byte(reader)?;
                cp_off |= (x as usize) << (i * 8);
            }
        }
        cp_off
    };
    let cp_size = {
        let mut cp_size: usize = 0;
        for i in 0..3 {
            if opcode & (1 << (4 + i)) != 0 {
                let x = read_byte(reader)?;
                cp_size |= (x as usize) << (i * 8);
            }
        }
        if cp_size == 0 {
            cp_size = 0x10000;
        }
        cp_size
    };

    Ok(DeltaInstruction::Copy(cp_off, cp_size))
}

#[derive(Debug)]
enum DeltaInstruction {
    Copy(usize, usize),
    Insert(Vec<u8>),
}

impl Display for DeltaInstruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeltaInstruction::Copy(offset, size) => {
                f.write_fmt(format_args!("Copy({}, {})", offset, size))
            }
            DeltaInstruction::Insert(data) => {
                let s = from_utf8(data);
                f.write_fmt(format_args!(
                    "Insert({})",
                    s.map(|s| s.to_string()).unwrap_or_else(|e| {
                        format!(
                            "e({}, rem:{})",
                            from_utf8(&data[0..e.valid_up_to()]).unwrap_or("<<REALLY_FAILED>>"),
                            data.len() - e.valid_up_to()
                        )
                    })
                ))
            }
        }
    }
}

fn parse_offset_delta(reader: &mut BufReader<File>) -> Result<usize, Box<dyn Error>> {
    let mut b = read_byte(reader)?;
    let mut offset = b as usize & 0x7f;

    while b & 0x80 > 0 {
        offset += 1;
        offset <<= 7;
        b = read_byte(reader)?;
        offset += b as usize & 0x7f;
    }

    Ok(offset)
}

fn parse_variable_length<T: Read>(reader: &mut BufReader<T>) -> Result<usize, Box<dyn Error>> {
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