use crate::gitobject::commit::CommitObject;
use crate::pack::BinaryObject;
use crate::repository::Repository;
use anyhow::{Context, Result, ensure};
use hex::ToHex;
use log::{debug, trace};
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::str::from_utf8;

pub struct LogIterator<'a> {
    repository: &'a Repository,
    current: [u8; 20],
    seen: HashSet<[u8; 20]>,
}

impl<'a> LogIterator<'a> {
    fn read_commit(&self, sha1: [u8; 20]) -> Result<Rc<RefCell<Vec<u8>>>> {
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
        Ok(Rc::new(RefCell::new(data)))
    }
}

impl<'a> LogIterator<'a> {
    pub fn new(repository: &'a Repository, sha1: [u8; 20]) -> Self {
        Self {
            repository,
            current: sha1,
            seen: HashSet::new(),
        }
    }
}

impl Iterator for LogIterator<'_> {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.seen.insert(self.current) {
            return None;
        }

        let next;
        let res = {
            let result = match self.read_commit(self.current) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };
            let rc = result.clone();
            let mut ref_mut = rc.borrow_mut();
            trace!(
                "reading object data '{}'",
                from_utf8(ref_mut.as_slice()).unwrap_or("<<bad utf8>>")
            );
            let commit = match CommitObject::from(ref_mut.as_mut_slice()) {
                Ok(commit) => commit,
                Err(e) => return Some(Err(e)),
            };

            let line = format!(
                "{} {}: {}",
                self.current.encode_hex::<String>(),
                commit
                    .author()
                    .first()
                    .unwrap_or(&"<<no author>>".to_string()),
                commit.message().unwrap_or("".to_string())
            )
            .replace("\n", " ");

            if let Some(&next_sha1) = commit.parents().first() {
                trace!("ascending {}", next_sha1.encode_hex::<String>());
                next = next_sha1;
            } else {
                debug!("no parents, breaking");
                return None;
            }

            line
        };

        self.current = next;
        Some(Ok(res))
    }
}
