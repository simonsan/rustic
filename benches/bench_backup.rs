#![allow(unused)]
fn main() {
    use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
    use rustic_core::{
        BackupOpts, NoProgressBars, PathList, Repository, RepositoryOptions, SnapshotFile,
    };
    use tempfile::tempdir;

    pub fn setup_repository() -> (
        rustic_core::Repository<
            rustic_core::NoProgressBars,
            rustic_core::repository::IndexedStatus<
                rustic_core::repository::IdIndex,
                rustic_core::OpenStatus,
            >,
        >,
        rustic_core::BackupOpts,
        rustic_core::PathList,
        bool,
    ) {
        let tempdir = tempdir().unwrap();

        // Open repository
        let repo_opts = RepositoryOptions::default()
            .repository(tempdir.path().to_str().unwrap())
            .password("test");
        let repo = Repository::new(&repo_opts)
            .unwrap()
            .open()
            .unwrap()
            .to_indexed_ids()
            .unwrap();

        let backup_opts = BackupOpts::default();
        let source = PathList::from_string(".", true).unwrap(); // true: sanitize the given string
        let dry_run = false;

        (repo, backup_opts, source, dry_run)
    }

    pub fn backup(
        inputs: (
            rustic_core::Repository<
                rustic_core::NoProgressBars,
                rustic_core::repository::IndexedStatus<
                    rustic_core::repository::IdIndex,
                    rustic_core::OpenStatus,
                >,
            >,
            rustic_core::BackupOpts,
            rustic_core::PathList,
            bool,
        ),
    ) {
        let (repo, backup_opts, source, dry_run) = inputs;
        let _snap = repo
            .backup(&backup_opts, source, SnapshotFile::default(), dry_run)
            .unwrap();
    }

    pub fn criterion_benchmark(c: &mut Criterion) {
        let setup_repository = setup_repository();
        c.bench_with_input(
            BenchmarkId::new("backup", "backup"),
            &setup_repository.clone(),
            |b, s| {
                b.iter(|| backup(black_box(s.clone())));
            },
        );
    }

    criterion_group!(benches, criterion_benchmark);
    criterion_main!(benches);
}
