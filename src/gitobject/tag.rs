use crate::kvlm::{kvlm_parse, kvlm_serialize};
use anyhow::Context;
use ordered_hash_map::OrderedHashMap;
use std::ops::Range;

#[derive(Debug)]
pub struct TagObject {
    kvlm: OrderedHashMap<Vec<u8>, Vec<Range<usize>>>,
    pub data: Vec<u8>,
}

impl TagObject {
    pub fn from(data: Vec<u8>) -> anyhow::Result<Self> {
        let (data, kvlm) = kvlm_parse(data).context("parsing tag object")?;
        Ok(Self { data, kvlm })
    }
    pub fn serialize(&self) -> Vec<u8> {
        kvlm_serialize(&self.data, &self.kvlm)
    }
}
