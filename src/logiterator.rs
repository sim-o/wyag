use crate::gitobject::CommitObject;
use crate::pack::BinaryObject;
use crate::repository::Repository;
use anyhow::{Context, anyhow};
use hex::ToHex;
use log::{debug, trace};
use std::collections::HashSet;

pub struct LogIterator<'a> {
    repository: &'a Repository,
    current: [u8; 20],
    seen: HashSet<[u8; 20]>,
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
    type Item = anyhow::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.seen.insert(self.current) {
            return None;
        }

        let object_data = self
            .repository
            .read_object_data(self.current)
            .context("iterating logs");
        let commit = match object_data {
            Ok((BinaryObject::Commit, ref data)) => {
                match CommitObject::from(data).with_context(|| {
                    format!("parsing commit {}", self.current.encode_hex::<String>())
                }) {
                    Ok(commit) => commit,
                    Err(e) => return Some(Err(e)),
                }
            }
            Ok((object_type, _)) => {
                let e = anyhow!(
                    "expected object with type commit, received {}",
                    object_type.name()
                );
                return Some(Err(e));
            }
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
            self.current = next_sha1;
        } else {
            debug!("no parents, breaking");
            return None;
        }

        Some(Ok(line))
    }
}
