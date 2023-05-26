pub(crate) mod file_archiver;
pub(crate) mod parent;
pub(crate) mod tree;
pub(crate) mod tree_archiver;

use std::path::{Path, PathBuf};

use chrono::Local;
use indicatif::ProgressBar;
use log::warn;
use pariter::{scope, IteratorExt};

use crate::{
    archiver::{
        file_archiver::FileArchiver, parent::Parent, tree::TreeIterator,
        tree_archiver::TreeArchiver,
    },
    backend::{decrypt::DecryptWriteBackend, ReadSource, ReadSourceEntry},
    blob::BlobType,
    id::Id,
    index::{indexer::Indexer, indexer::SharedIndexer, IndexedBackend},
    repofile::{configfile::ConfigFile, snapshotfile::SnapshotFile},
    RusticResult,
};
#[allow(missing_debug_implementations)]
pub struct Archiver<BE: DecryptWriteBackend, I: IndexedBackend> {
    file_archiver: FileArchiver<BE, I>,
    tree_archiver: TreeArchiver<BE, I>,
    parent_tree: Option<Id>,
    parent: Parent<I>,
    indexer: SharedIndexer<BE>,
    be: BE,
    snap: SnapshotFile,
}

impl<BE: DecryptWriteBackend, I: IndexedBackend> Archiver<BE, I> {
    pub fn new(
        be: BE,
        index: I,
        config: &ConfigFile,
        parent_tree: impl Into<Option<Id>> + Copy,
        ignore_ctime: bool,
        ignore_inode: bool,
        mut snap: SnapshotFile,
    ) -> RusticResult<Self> {
        let indexer = Indexer::new(be.clone()).into_shared();
        let mut summary = snap.summary.take().unwrap();
        summary.backup_start = Local::now();

        let parent = Parent::new(&index, parent_tree, ignore_ctime, ignore_inode);
        let file_archiver = FileArchiver::new(be.clone(), index.clone(), indexer.clone(), config)?;
        let tree_archiver = TreeArchiver::new(be.clone(), index, indexer.clone(), config, summary)?;
        Ok(Self {
            file_archiver,
            tree_archiver,
            parent_tree: parent_tree.into(),
            parent,
            indexer,
            be,
            snap,
        })
    }

    pub fn archive<R>(
        mut self,
        src: R,
        backup_path: &Path,
        as_path: Option<&PathBuf>,
        p: &ProgressBar,
    ) -> RusticResult<SnapshotFile>
    where
        R: ReadSource + 'static,
        <R as ReadSource>::Open: Send,
        <R as ReadSource>::Iter: Send,
    {
        if !p.is_hidden() {
            if let Some(size) = src.size()? {
                p.set_length(size);
            }
        };
        p.set_prefix("backing up...");

        // filter out errors and handle as_path
        let iter = src.entries().filter_map(|item| match item {
            Err(e) => {
                warn!("ignoring error {e}\n");
                None
            }
            Ok(ReadSourceEntry { path, node, open }) => {
                let snapshot_path = if let Some(as_path) = as_path {
                    as_path
                        .clone()
                        .join(path.strip_prefix(backup_path).unwrap())
                } else {
                    path
                };
                Some(if node.is_dir() {
                    (snapshot_path, node, open)
                } else {
                    (
                        snapshot_path
                            .parent()
                            .expect("file path should have a parent!")
                            .to_path_buf(),
                        node,
                        open,
                    )
                })
            }
        });
        // handle beginning and ending of trees
        let iter = TreeIterator::new(iter);

        scope(|scope| -> RusticResult<_> {
            // use parent snapshot
            iter.filter_map(|item| match self.parent.process(item) {
                Ok(item) => Some(item),
                Err(err) => {
                    warn!("ignoring error reading parent snapshot: {err:?}");
                    None
                }
            })
            // archive files in parallel
            .parallel_map_scoped(scope, |item| self.file_archiver.process(item, p))
            .readahead_scoped(scope)
            .filter_map(|item| match item {
                Ok(item) => Some(item),
                Err(err) => {
                    warn!("ignoring error: {err:?}");
                    None
                }
            })
            .try_for_each(|item| self.tree_archiver.add(item))
        })
        .unwrap()?;

        let stats = self.file_archiver.finalize()?;
        let (id, mut summary) = self.tree_archiver.finalize(self.parent_tree)?;
        stats.apply(&mut summary, BlobType::Data);
        self.snap.tree = id;

        self.indexer.write().unwrap().finalize()?;

        summary.finalize(self.snap.time)?;
        self.snap.summary = Some(summary);

        let id = self.be.save_file(&self.snap)?;
        self.snap.id = id;

        p.finish_with_message("done");
        Ok(self.snap)
    }
}
