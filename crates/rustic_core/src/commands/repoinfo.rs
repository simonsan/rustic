use serde::{Deserialize, Serialize};

use crate::{
    backend::{decrypt::DecryptReadBackend, FileType, ReadBackend, ALL_FILE_TYPES},
    blob::{BlobType, BlobTypeMap},
    index::IndexEntry,
    repofile::indexfile::{IndexFile, IndexPack},
    repository::Open,
    Progress, ProgressBars, Repository, RusticResult,
};

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
/// Index information from `repoinfo`
pub struct IndexInfos {
    /// Infos about blobs
    pub blobs: Vec<BlobInfo>,
    /// Infos about blobs in packs marked for deletion
    pub blobs_delete: Vec<BlobInfo>,
    /// Infos about packs
    pub packs: Vec<PackInfo>,
    /// Infos about packs marked for deletion
    pub packs_delete: Vec<PackInfo>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
/// Information about blobs within `repoinfo`
pub struct BlobInfo {
    /// blob type
    pub blob_type: BlobType,
    /// # of blobs of the type
    pub count: u64,
    /// Total size saved in the repository of all blobs of the type.
    ///
    /// This is the size of the blobs after compression and encryption.
    pub size: u64,
    /// Total data size of all blobs of the type.
    ///
    /// This is the raw size of the blobs without compression or encryption.
    pub data_size: u64,
}

impl BlobInfo {
    pub(crate) fn add(&mut self, ie: IndexEntry) {
        self.count += 1;
        self.size += u64::from(ie.length);
        self.data_size += u64::from(ie.data_length());
    }
}

#[serde_with::apply(Option => #[serde(default, skip_serializing_if = "Option::is_none")])]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
/// Information about packs within `repoinfo`
pub struct PackInfo {
    /// packs of the given blob type
    pub blob_type: BlobType,
    /// # of packs of the type
    pub count: u64,
    /// minimal pack size for packs of the type, None, if there is no pack.
    pub min_size: Option<u64>,
    /// maximal pack size for packs of the type, None, if there is no pack.
    pub max_size: Option<u64>,
}

impl PackInfo {
    pub(crate) fn add(&mut self, ip: &IndexPack) {
        self.count += 1;
        let size = u64::from(ip.pack_size());
        self.min_size = self
            .min_size
            .map_or(Some(size), |min_size| Some(min_size.min(size)));
        self.max_size = self
            .max_size
            .map_or(Some(size), |max_size| Some(max_size.max(size)));
    }
}

pub(crate) fn collect_index_infos<P: ProgressBars, S: Open>(
    repo: &Repository<P, S>,
) -> RusticResult<IndexInfos> {
    let mut blob_info = BlobTypeMap::<()>::default().map(|blob_type, _| BlobInfo {
        blob_type,
        count: 0,
        size: 0,
        data_size: 0,
    });
    let mut blob_info_delete = blob_info;
    let mut pack_info = BlobTypeMap::<()>::default().map(|blob_type, _| PackInfo {
        blob_type,
        count: 0,
        min_size: None,
        max_size: None,
    });
    let mut pack_info_delete = pack_info;

    let p = repo.pb.progress_counter("scanning index...");
    for index in repo.dbe().stream_all::<IndexFile>(&p)? {
        let index = index?.1;
        for pack in &index.packs {
            let tpe = pack.blob_type();
            pack_info[tpe].add(pack);

            for blob in &pack.blobs {
                let ie = IndexEntry::from_index_blob(blob, pack.id);
                blob_info[tpe].add(ie);
            }
        }

        for pack in &index.packs_to_delete {
            let tpe = pack.blob_type();
            pack_info_delete[tpe].add(pack);
            for blob in &pack.blobs {
                let ie = IndexEntry::from_index_blob(blob, pack.id);
                blob_info_delete[tpe].add(ie);
            }
        }
    }
    p.finish();

    let info = IndexInfos {
        blobs: blob_info.into_values().collect(),
        blobs_delete: blob_info_delete.into_values().collect(),
        packs: pack_info.into_values().collect(),
        packs_delete: pack_info_delete.into_values().collect(),
    };

    Ok(info)
}

#[serde_with::apply(Option => #[serde(default, skip_serializing_if = "Option::is_none")])]
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
/// Information about repository files
pub struct RepoFileInfos {
    /// Repository files
    pub repo: Vec<RepoFileInfo>,
    /// Hot repository files, if we have a hot/cold repository
    pub repo_hot: Option<Vec<RepoFileInfo>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
/// Information about a repository files of a given [`FileType`]
pub struct RepoFileInfo {
    /// The type of the files
    pub tpe: FileType,
    /// The total # of files
    pub count: u64,
    /// The total size of all files
    pub size: u64,
}

pub(crate) fn collect_file_info(be: &impl ReadBackend) -> RusticResult<Vec<RepoFileInfo>> {
    let mut files = Vec::with_capacity(ALL_FILE_TYPES.len());
    for tpe in ALL_FILE_TYPES {
        let list = be.list_with_size(tpe)?;
        let count = list.len() as u64;
        let size = list.iter().map(|f| u64::from(f.1)).sum();
        files.push(RepoFileInfo { tpe, count, size });
    }
    Ok(files)
}

pub(crate) fn collect_file_infos<P: ProgressBars, S>(
    repo: &Repository<P, S>,
) -> RusticResult<RepoFileInfos> {
    let p = repo.pb.progress_spinner("scanning files...");
    let files = collect_file_info(&repo.be)?;
    let files_hot = repo.be_hot.as_ref().map(collect_file_info).transpose()?;
    p.finish();

    Ok(RepoFileInfos {
        repo: files,
        repo_hot: files_hot,
    })
}
