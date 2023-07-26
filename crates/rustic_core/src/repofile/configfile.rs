use serde::{Deserialize, Serialize};

use crate::{
    backend::FileType, blob::BlobType, error::ConfigFileErrorKind, id::Id, repofile::RepoFile,
    RusticResult,
};

pub(super) mod constants {

    pub(super) const KB: u32 = 1024;
    pub(super) const MB: u32 = 1024 * KB;
    // default pack size
    pub(super) const DEFAULT_TREE_SIZE: u32 = 4 * MB;
    pub(super) const DEFAULT_DATA_SIZE: u32 = 32 * MB;
    // the default factor used for repo-size dependent pack size.
    // 32 * sqrt(reposize in bytes) = 1 MB * sqrt(reposize in GB)
    pub(super) const DEFAULT_GROW_FACTOR: u32 = 32;
    pub(super) const DEFAULT_SIZE_LIMIT: u32 = u32::MAX;
    pub(super) const DEFAULT_MIN_PERCENTAGE: u32 = 30;
}

#[serde_with::apply(Option => #[serde(default, skip_serializing_if = "Option::is_none")])]
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
/// The config file describes all repository-wide information.
///
/// It is usually saved in the repository as `config`
pub struct ConfigFile {
    /// Repository version. Currently 1 and 2 are supported
    pub version: u32,
    /// The [`Id`] identifying the repsitors
    pub id: Id,
    /// The chunker polynomial used to chunk data
    pub chunker_polynomial: String,
    /// (optional) Marker if this is a hot repository. If not set, this is no hot repository
    ///
    /// Note: When using hot/cold repositories, this is only set within the hot part of the repository.
    pub is_hot: Option<bool>,
    /// (optional) compression level
    ///
    /// Note: that `Some(0)` means no compression. If not set, use the default compression:
    /// - for repository version 1, use no compression (as not supported)
    /// - for repository version 2, use the zstd default compression
    pub compression: Option<i32>,
    /// (optional) size of tree packs. This will be enhanced by the `treepack_growfactor` depending on the repository size
    ///
    /// If not set, defaults to 4 MiB
    pub treepack_size: Option<u32>,
    /// (optional) grow factor to increase size of tree packs depending on the repository size
    ///
    /// If not set, defaults to 32
    pub treepack_growfactor: Option<u32>,
    /// (optional) maximum targeted tree pack size.
    pub treepack_size_limit: Option<u32>,
    /// (optional) size of data packs. This will be enhanced by the `datapack_growfactor` depending on the repository size
    ///
    /// If not set, defaults to 32 MiB
    pub datapack_size: Option<u32>,
    /// (optional) grow factor to increase size of data packs depending on the repository size
    ///
    /// If not set, defaults to 32
    pub datapack_growfactor: Option<u32>,
    /// (optional) maximum targeted data pack size.
    pub datapack_size_limit: Option<u32>,
    /// (optional) tolerate pack sizes which are larger than given percentage of targeted pack size
    ///
    /// If not set, defaults to 30
    pub min_packsize_tolerate_percent: Option<u32>,
    /// (optional) tolerate pack sizes which are smaller than given percentage of targeted pack size
    ///
    /// If not set or set to 0 this is unlimited.
    pub max_packsize_tolerate_percent: Option<u32>,
}

impl RepoFile for ConfigFile {
    const TYPE: FileType = FileType::Config;
}

impl ConfigFile {
    #[must_use]
    /// Creates a new `ConfigFile` using the given `version`, `id` and chunker polynomial
    pub fn new(version: u32, id: Id, poly: u64) -> Self {
        Self {
            version,
            id,
            chunker_polynomial: format!("{poly:x}"),
            ..Self::default()
        }
    }

    /// Get the chunker polynomial
    pub fn poly(&self) -> RusticResult<u64> {
        Ok(u64::from_str_radix(&self.chunker_polynomial, 16)
            .map_err(ConfigFileErrorKind::ParsingFailedForPolynomial)?)
    }

    /// Get the compression level
    pub fn zstd(&self) -> RusticResult<Option<i32>> {
        match (self.version, self.compression) {
            (1, _) | (2, Some(0)) => Ok(None),
            (2, None) => Ok(Some(0)), // use default (=0) zstd compression
            (2, Some(c)) => Ok(Some(c)),
            _ => Err(ConfigFileErrorKind::ConfigVersionNotSupported.into()),
        }
    }

    #[must_use]
    /// Get pack size parameter for the given `BlobType`
    pub fn packsize(&self, blob: BlobType) -> (u32, u32, u32) {
        match blob {
            BlobType::Tree => (
                self.treepack_size.unwrap_or(constants::DEFAULT_TREE_SIZE),
                self.treepack_growfactor
                    .unwrap_or(constants::DEFAULT_GROW_FACTOR),
                self.treepack_size_limit
                    .unwrap_or(constants::DEFAULT_SIZE_LIMIT),
            ),
            BlobType::Data => (
                self.datapack_size.unwrap_or(constants::DEFAULT_DATA_SIZE),
                self.datapack_growfactor
                    .unwrap_or(constants::DEFAULT_GROW_FACTOR),
                self.datapack_size_limit
                    .unwrap_or(constants::DEFAULT_SIZE_LIMIT),
            ),
        }
    }

    #[must_use]
    /// Get pack size toleration limits
    pub fn packsize_ok_percents(&self) -> (u32, u32) {
        (
            self.min_packsize_tolerate_percent
                .unwrap_or(constants::DEFAULT_MIN_PERCENTAGE),
            match self.max_packsize_tolerate_percent {
                None | Some(0) => u32::MAX,
                Some(percent) => percent,
            },
        )
    }
}
