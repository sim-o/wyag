use crate::kvlm::{kvlm_parse, kvlm_serialize};
use anyhow::Context;
use ordered_hash_map::OrderedHashMap;

#[derive(Debug)]
pub struct TagObject<'a> {
    kvlm: OrderedHashMap<&'a [u8], Vec<&'a [u8]>>,
}

impl<'a> TagObject<'a> {
    pub fn from(data: &'a mut [u8]) -> anyhow::Result<Self> {
        Ok(Self {
            kvlm: kvlm_parse(data).context("parsing tag object")?,
        })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.kvlm)
    }
}