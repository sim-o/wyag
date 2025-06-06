use crate::util::{get_delta_hdr_size, read_byte};
use anyhow::{Context, ensure};
use bytes::Buf;
use log::trace;
use std::fmt::Display;
use std::io;
use std::io::{ErrorKind, Read};
use std::str::from_utf8;

#[derive(Debug)]
pub struct DeltaObject {
    base_size: usize,
    expanded_size: usize,
    instructions: Vec<DeltaInstruction>,
}

#[derive(Debug)]
pub struct OffsetDeltaObject {
    #[allow(dead_code)]
    pub offset: u64,
    #[allow(dead_code)]
    pub delta: DeltaObject,
}

#[derive(Debug)]
pub struct RefDeltaObject {
    #[allow(dead_code)]
    pub reference: [u8; 20],
    #[allow(dead_code)]
    pub delta: DeltaObject,
}

impl DeltaObject {
    pub fn from(delta_data: &[u8]) -> anyhow::Result<Self> {
        parse_delta_data(delta_data).context("new delta")
    }

    pub fn rebuild(&self, reference_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        trace!(
            "rebuilding from {} [[{}]]",
            reference_data.len(),
            from_utf8(&reference_data)
                .map(String::from)
                .unwrap_or_else(|e| from_utf8(&reference_data[..e.valid_up_to()])
                    .unwrap()
                    .to_string()
                    + "...<<bad-utf8>>")
        );
        let mut result = Vec::new();
        for instr in self.instructions.iter() {
            match instr {
                DeltaInstruction::Copy(offset, size) => {
                    trace!("copy @{} +{}", offset, size);
                    result.extend_from_slice(&reference_data[*offset..offset + size]);
                }
                DeltaInstruction::Insert(insert) => {
                    trace!("insert +{}", insert.len());
                    result.extend_from_slice(insert);
                }
            };
        }
        ensure!(
            result.len() == self.expanded_size,
            "unpacked file size incorrect {} expected to be {}, base size {}",
            reference_data.len(),
            self.expanded_size,
            self.base_size,
        );
        Ok(result)
    }
}

impl OffsetDeltaObject {
    pub fn new(offset: u64, data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            offset,
            delta: DeltaObject::from(data).context("parsing offset delta object")?,
        })
    }
}

impl RefDeltaObject {
    pub fn new(reference: [u8; 20], data: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            reference,
            delta: parse_delta_data(data).context("parsing ref delta object")?,
        })
    }
}

fn parse_copy_instruction<T: Read>(opcode: u8, reader: &mut T) -> io::Result<DeltaInstruction> {
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

fn parse_delta_data(bytes: &[u8]) -> anyhow::Result<DeltaObject> {
    let mut reader = bytes.reader();
    let base_size = get_delta_hdr_size(&mut reader).context("reading base size")?;
    let expanded_size = get_delta_hdr_size(&mut reader).context("reading expanded size")?;

    let mut instructions = Vec::new();
    loop {
        let opcode = read_byte(&mut reader);
        match opcode {
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                anyhow::bail!("unexpected read error: {}", err);
            }
            Ok(opcode) => {
                anyhow::ensure!(opcode != 0, "invalid delta opcode 0");
                let instr = if opcode & 0x80 == 0 {
                    let data = {
                        let mut buf = vec![0; opcode as usize];
                        reader.read_exact(&mut buf).context("reading insert data")?;
                        buf
                    };
                    DeltaInstruction::Insert(data)
                } else {
                    parse_copy_instruction(opcode, &mut reader)
                        .context("reading copy instruction")?
                };

                instructions.push(instr);
            }
        }
    }
    Ok(DeltaObject {
        base_size,
        expanded_size,
        instructions,
    })
}
