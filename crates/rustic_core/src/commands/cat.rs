use std::path::Path;

use bytes::Bytes;

use crate::{
    backend::{decrypt::DecryptReadBackend, FileType, ReadBackend},
    blob::{tree::Tree, BlobType},
    error::CommandErrorKind,
    error::RusticResult,
    id::Id,
    index::IndexedBackend,
    progress::ProgressBars,
    repofile::SnapshotFile,
    repository::{IndexedFull, IndexedTree, Open, Repository},
};

/// Prints the contents of a file.
///
/// # Arguments
///
/// * `repo` - The repository to read from.
/// * `tpe` - The type of the file.
/// * `id` - The id of the file.
///
/// # Errors
///
/// If the file could not be read.
///
/// # Returns
///
/// The data read.
pub(crate) fn cat_file<P, S: Open>(
    repo: &Repository<P, S>,
    tpe: FileType,
    id: &str,
) -> RusticResult<Bytes> {
    let id = repo.dbe().find_id(tpe, id)?;
    let data = repo.dbe().read_encrypted_full(tpe, &id)?;
    Ok(data)
}

pub(crate) fn cat_blob<P, S: IndexedFull>(
    repo: &Repository<P, S>,
    tpe: BlobType,
    id: &str,
) -> RusticResult<Bytes> {
    let id = Id::from_hex(id)?;
    let data = repo.index().blob_from_backend(tpe, &id)?;

    Ok(data)
}

/// Prints the contents of a tree.
///
/// # Arguments
///
/// * `repo` - The repository to read from.
/// * `snap` - The snapshot to read from.
/// * `sn_filter` - The filter to apply to the snapshot.
///
/// # Errors
///
/// If the file could not be read.
///
/// # Returns
///
/// The data read.
pub(crate) fn cat_tree<P: ProgressBars, S: IndexedTree>(
    repo: &Repository<P, S>,
    snap: &str,
    sn_filter: impl FnMut(&SnapshotFile) -> bool + Send + Sync,
) -> RusticResult<Bytes> {
    let (id, path) = snap.split_once(':').unwrap_or((snap, ""));
    let snap = SnapshotFile::from_str(
        repo.dbe(),
        id,
        sn_filter,
        &repo.pb.progress_counter("getting snapshot..."),
    )?;
    let node = Tree::node_from_path(repo.index(), snap.tree, Path::new(path))?;
    let id = node
        .subtree
        .ok_or_else(|| CommandErrorKind::PathIsNoDir(path.to_string()))?;
    let data = repo.index().blob_from_backend(BlobType::Tree, &id)?;
    Ok(data)
}
