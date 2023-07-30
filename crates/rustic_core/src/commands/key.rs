//! `key` subcommand
use derive_setters::Setters;

use crate::{
    backend::{FileType, WriteBackend},
    crypto::hasher::hash,
    error::CommandErrorKind,
    repofile::KeyFile,
    repository::Open,
    Id, Key, Repository, RusticResult,
};

#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[derive(Debug, Clone, Default, Setters)]
#[setters(into)]
/// Options for the `key` command. These are used when creating a new key.
pub struct KeyOptions {
    /// Set 'hostname' in public key information
    #[cfg_attr(feature = "clap", clap(long))]
    pub hostname: Option<String>,

    /// Set 'username' in public key information
    #[cfg_attr(feature = "clap", clap(long))]
    pub username: Option<String>,

    /// Add 'created' date in public key information
    #[cfg_attr(feature = "clap", clap(long))]
    pub with_created: bool,
}

impl KeyOptions {
    pub(crate) fn add_key<P, S: Open>(
        &self,
        repo: &Repository<P, S>,
        pass: &str,
    ) -> RusticResult<Id> {
        let key = repo.key();
        self.add(repo, pass, *key)
    }

    pub(crate) fn init_key<P, S>(
        &self,
        repo: &Repository<P, S>,
        pass: &str,
    ) -> RusticResult<(Key, Id)> {
        // generate key
        let key = Key::new();
        Ok((key, self.add(repo, pass, key)?))
    }

    fn add<P, S>(&self, repo: &Repository<P, S>, pass: &str, key: Key) -> RusticResult<Id> {
        let ko = self.clone();
        let keyfile = KeyFile::generate(key, &pass, ko.hostname, ko.username, ko.with_created)?;

        let data = serde_json::to_vec(&keyfile).map_err(CommandErrorKind::FromJsonError)?;
        let id = hash(&data);
        repo.be
            .write_bytes(FileType::Key, &id, false, data.into())?;
        Ok(id)
    }
}
