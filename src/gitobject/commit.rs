use crate::kvlm::{kvlm_parse, kvlm_serialize};
use anyhow::Context;
use hex::decode;
use ordered_hash_map::OrderedHashMap;
use std::ops::{Deref, Range};
use std::str::from_utf8;

#[derive(Debug)]
pub struct CommitObject {
    data: Vec<u8>,
    kvlm: OrderedHashMap<Vec<u8>, Vec<Range<usize>>>,
}

impl CommitObject {
    fn get(&self, name: &[u8]) -> impl Iterator<Item = String> {
        self.kvlm.get(name).into_iter().flat_map(|a| {
            a.iter().map(|v| {
                from_utf8(&self.data[v.start..v.end])
                    .map(|v| v.to_string())
                    .unwrap_or("<<bad-utf8>>".to_string())
            })
        })
    }

    pub fn author(&self) -> Vec<String> {
        self.get(b"author").collect()
    }

    pub fn committer_timestamp(&self) -> u32 {
        self.get(b"committer")
            .next()
            .map(|s| s.parse().unwrap_or(0))
            .unwrap_or(0)
    }

    pub fn message(&self) -> Option<String> {
        self.get(b"").next()
    }

    pub fn parents(&self) -> Vec<[u8; 20]> {
        self.get(b"parent")
            .flat_map(|s| decode(s).ok())
            .flat_map(|v| v.deref().try_into().ok())
            .collect()
    }

    pub fn from(data: Vec<u8>) -> anyhow::Result<Self> {
        let (data, kvlm) = kvlm_parse(data).context("Failed to parse commit kvlm")?;
        Ok(Self { data, kvlm })
    }

    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.data, &self.kvlm)
    }
}
