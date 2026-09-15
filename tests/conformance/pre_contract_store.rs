//! Pre-contract stores are historical evidence, never a target compatibility path.
mod support;
use std::{fs, path::Path};

use actionqueue_storage::{
    recovery::bootstrap::load_projection_from_storage,
    store::*,
    wal::{fs_writer::WalFsWriter, repair::RepairPolicy},
};
fn tree(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn visit(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in fs::read_dir(dir).unwrap() {
            let p = entry.unwrap().path();
            out.push((
                p.strip_prefix(root).unwrap().to_string_lossy().to_string(),
                if p.is_file() { fs::read(&p).unwrap() } else { vec![] },
            ));
            if p.is_dir() {
                visit(root, &p, out);
            }
        }
    }
    let mut v = Vec::new();
    visit(root, root, &mut v);
    v.sort();
    v
}
#[test]
fn target_store_rejects_pre_contract_data() {
    let archive = support::repo_root().join("archive/pre-aq-cont-1");
    for wal in ["lifecycle-wal-v5.wal", "lifecycle-wal-v5-truncated-tail.wal"] {
        for snapshot in [
            None,
            Some("lifecycle-snapshot-schema8.bin"),
            Some("lifecycle-snapshot-schema8-crc-mismatch.bin"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            fs::create_dir(dir.path().join("wal")).unwrap();
            fs::create_dir(dir.path().join("snapshots")).unwrap();
            fs::copy(
                archive.join("selected-wal-fixtures").join(wal),
                dir.path().join("wal/actionqueue.wal"),
            )
            .unwrap();
            if let Some(snapshot) = snapshot {
                fs::copy(
                    archive.join("selected-snapshot-fixtures").join(snapshot),
                    dir.path().join("snapshots/snapshot.bin"),
                )
                .unwrap();
            }
            let before = tree(dir.path());
            assert!(matches!(
                open_store(dir.path(), OpenOptions::ReadWrite),
                Err(StoreError::MissingTargetManifest)
            ));
            assert!(load_projection_from_storage(dir.path())
                .unwrap_err_string()
                .contains("MissingTargetManifest"));
            assert!(inspect_store(dir.path()).is_err());
            assert!(backup_store(dir.path(), &dir.path().with_extension("backup")).is_err());
            assert_eq!(tree(dir.path()), before);
        }
    }
}
trait ErrorString {
    fn unwrap_err_string(self) -> String;
}
impl<T, E: std::fmt::Display> ErrorString for Result<T, E> {
    fn unwrap_err_string(self) -> String {
        match self {
            Ok(_) => panic!("expected rejection"),
            Err(e) => e.to_string(),
        }
    }
}
#[test]
fn target_manifest_cannot_disguise_old_wal_or_snapshot() {
    let archive = support::repo_root().join("archive/pre-aq-cont-1");
    for old_wal in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        drop(open_store(dir.path(), OpenOptions::Initialize { features: vec![] }).unwrap());
        if old_wal {
            fs::copy(
                archive.join("selected-wal-fixtures/lifecycle-wal-v5.wal"),
                dir.path().join("wal/actionqueue.wal"),
            )
            .unwrap();
        } else {
            fs::copy(
                archive.join("selected-snapshot-fixtures/lifecycle-snapshot-schema8.bin"),
                dir.path().join("snapshots/snapshot.bin"),
            )
            .unwrap();
        }
        let before = tree(dir.path());
        let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
        assert!(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).is_err());
        assert_eq!(tree(dir.path()), before);
    }
}
