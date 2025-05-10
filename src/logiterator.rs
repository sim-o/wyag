use crate::gitobject::commit::CommitObject;
use crate::pack::BinaryObject;
use crate::repository::Repository;
use anyhow::{Context, Result, ensure};
use hex::ToHex;
use log::error;
use std::cmp::Ordering;
use std::cmp::Ordering::Equal;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::rc::Rc;

struct HeapItem(u32, [u8; 20]);

impl Eq for HeapItem {}

impl PartialEq<Self> for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Equal
    }
}

impl PartialOrd<Self> for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

pub struct LogIterator<'a> {
    repository: &'a Repository,
    current: BinaryHeap<HeapItem>,
    seen: HashSet<[u8; 20]>,
    cache: HashMap<[u8; 20], Rc<CommitObject>>,
}

impl LogIterator<'_> {
    fn read_commit(&mut self, sha1: [u8; 20]) -> Result<Rc<CommitObject>> {
        if let Some(cached) = self.cache.get(&sha1) {
            return Ok(cached.clone());
        }

        let mut data = Vec::new();
        let object_type = self
            .repository
            .read_object_data(sha1, &mut data)
            .with_context(|| format!("iterating log {}", sha1.encode_hex::<String>()))?;
        ensure!(
            object_type == BinaryObject::Commit,
            "expected commit, received {}",
            object_type.name()
        );
        let rc = Rc::new(CommitObject::from(data)?);
        self.cache.insert(sha1, rc.clone());
        Ok(rc)
    }
}

impl<'a> LogIterator<'a> {
    pub fn new(repository: &'a Repository, sha1: [u8; 20]) -> Result<Self> {
        let mut res = Self {
            repository,
            current: BinaryHeap::new(),
            seen: HashSet::new(),
            cache: HashMap::new(),
        };

        let commit = res.read_commit(sha1)?;
        res.current
            .push(HeapItem(commit.committer_timestamp(), sha1));
        Ok(res)
    }
}

impl Iterator for LogIterator<'_> {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current;
        loop {
            HeapItem(_, current) = self.current.pop()?;
            if self.seen.insert(current) {
                break;
            }
        }

        let res = {
            let commit = match self.read_commit(current) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };

            let line = format!(
                "{} {}: {}",
                current.encode_hex::<String>(),
                commit
                    .author()
                    .first()
                    .unwrap_or(&"<<no author>>".to_string()),
                commit.message().unwrap_or("".to_string())
            )
            .replace("\n", " ");

            if commit.parents().len() > 1 {
                error!(
                    "commit with many parents! {} {}",
                    current.encode_hex::<String>(),
                    commit.parents().len()
                );
            }

            for next_sha1 in commit.parents() {
                if let Ok(next_commit) = self.read_commit(next_sha1) {
                    self.current
                        .push(HeapItem(next_commit.committer_timestamp(), next_sha1));
                }
            }

            line
        };
        Some(Ok(res))
    }
}
