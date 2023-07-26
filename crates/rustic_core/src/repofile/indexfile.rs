use std::{cmp::Ordering, num::NonZeroU32};

use chrono::{DateTime, Local};

use serde::{Deserialize, Serialize};

use crate::{
    backend::FileType, blob::BlobType, id::Id, repofile::packfile::PackHeaderRef,
    repofile::RepoFile,
};

#[derive(Serialize, Deserialize, Debug, Default)]
/// Index files describe index information about multiple `pack` files.
///
/// They are usually stored in the repository under `/index/<ID>`
pub struct IndexFile {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// which other index files are superseded by this (not actively used)
    pub supersedes: Option<Vec<Id>>,
    /// Index information about used packs
    pub packs: Vec<IndexPack>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Index information about unused packs which are already marked for deletion
    pub packs_to_delete: Vec<IndexPack>,
}

impl RepoFile for IndexFile {
    const TYPE: FileType = FileType::Index;
}

impl IndexFile {
    pub(crate) fn add(&mut self, p: IndexPack, delete: bool) {
        if delete {
            self.packs_to_delete.push(p);
        } else {
            self.packs.push(p);
        }
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
/// Index information about a `pack`
pub struct IndexPack {
    /// pack Id
    pub id: Id,
    /// Index information about contained blobs
    pub blobs: Vec<IndexBlob>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The pack creation time or time when the pack was marked for deletion
    pub time: Option<DateTime<Local>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// The pack size
    pub size: Option<u32>,
}

impl IndexPack {
    pub(crate) fn add(
        &mut self,
        id: Id,
        tpe: BlobType,
        offset: u32,
        length: u32,
        uncompressed_length: Option<NonZeroU32>,
    ) {
        self.blobs.push(IndexBlob {
            id,
            tpe,
            offset,
            length,
            uncompressed_length,
        });
    }

    // calculate the pack size from the contained blobs
    #[must_use]
    pub(crate) fn pack_size(&self) -> u32 {
        self.size
            .unwrap_or_else(|| PackHeaderRef::from_index_pack(self).pack_size())
    }

    /// returns the blob type of the pack. Note that only packs with
    /// identical blob types are allowed
    #[must_use]
    pub fn blob_type(&self) -> BlobType {
        // TODO: This is a hack to support packs without blobs (e.g. when deleting unreferenced files)
        if self.blobs.is_empty() {
            BlobType::Data
        } else {
            self.blobs[0].tpe
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Copy)]
/// Index information about a `blob`
pub struct IndexBlob {
    /// blob Id
    pub id: Id,
    #[serde(rename = "type")]
    /// type of the blob
    pub tpe: BlobType,
    /// offset of the blob within the `pack` file
    pub offset: u32,
    /// length of the blob as stored within the `pack` file
    pub length: u32,
    /// data length of the blob. This is only set if the blob is compressed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_length: Option<NonZeroU32>,
}

impl PartialOrd<Self> for IndexBlob {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.offset.partial_cmp(&other.offset)
    }
}

impl Ord for IndexBlob {
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset.cmp(&other.offset)
    }
}
