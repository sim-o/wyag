use crate::gitobject::GitObject;
use crate::repository::Repository;
use hex::ToHex;
use log::{debug, error, trace};
use std::collections::HashSet;
use std::error::Error;

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

impl<'a> Iterator for LogIterator<'a> {
    type Item = Result<String, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.seen.insert(self.current) {
            return None;
        }

        let object = match self.repository.read_object(self.current) {
            Ok(object) => object,
            Err(e) => return Some(Err(e)),
        };

        match object {
            GitObject::Commit(commit) => {
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
            _ => {
                error!("commit not at {}", self.current.encode_hex::<String>());
                Some(Err("expected commit".into()))
            }
        }
    }
}