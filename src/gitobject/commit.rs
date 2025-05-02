use crate::kvlm::{kvlm_parse, kvlm_serialize};
use anyhow::Context;
use hex::decode;
use ordered_hash_map::OrderedHashMap;
use std::ops::Deref;
use std::str::from_utf8;

#[derive(Debug)]
pub struct CommitObject<'a> {
    kvlm: OrderedHashMap<&'a [u8], Vec<&'a [u8]>>,
}

impl<'a> CommitObject<'a> {
    fn get(&self, name: &[u8]) -> impl Iterator<Item=String> {
        self.kvlm.get(name).into_iter().flat_map(|a| {
            a.first()
                .into_iter()
                .flat_map(|&v| from_utf8(v).map(|v| v.to_string()))
        })
    }

    pub fn author(&self) -> Vec<String> {
        self.get(b"author").collect()
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

    pub fn from(data: &'a mut [u8]) -> anyhow::Result<Self> {
        Ok(Self {
            kvlm: kvlm_parse(data).context("Failed to parse commit kvlm")?,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}