//! AQ-03 process-crash ownership gate.
//!
//! This separate test executable keeps process spawning out of the parallel
//! persistence suite. A child can briefly inherit unrelated store-lock descriptors
//! before exec, delaying release after another test drops its final session.
//! Keep close/reopen persistence tests in `target_persistence.rs`.

use actionqueue_storage::{
    store::{inspect_store, open_store, OpenOptions, StoreError},
    wal::{
        event::{WalEvent, WalEventType as E},
        fs_writer::WalFsWriter,
        writer::WalWriter,
    },
};
use std::{fs, path::Path};

#[test]
fn process_lock_holder() {
    let Ok(root) = std::env::var("AQ_TEST_LOCK_HOLDER") else {
        return;
    };
    let path = Path::new(&root);
    let session = open_store(path, OpenOptions::Initialize { features: vec![] }).unwrap();
    let mut writer = WalFsWriter::new(session).unwrap();
    writer.append(&WalEvent::new(2, E::EnginePaused { timestamp: 1 })).unwrap();
    writer.flush().unwrap();
    fs::write(path.with_extension("ready"), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn operating_system_releases_writer_lock_after_process_kill() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_lock_holder", "--nocapture"])
        .env("AQ_TEST_LOCK_HOLDER", &root)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..1000 {
        if root.with_extension("ready").exists() {
            break;
        }
        assert!(child.try_wait().unwrap().is_none(), "child exited before owning store");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let ready = root.with_extension("ready").exists();
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!("child did not acquire store");
    }
    assert!(matches!(open_store(&root, OpenOptions::ReadOnly), Err(StoreError::StoreInUse)));
    child.kill().unwrap();
    child.wait().unwrap();
    let inspection = inspect_store(&root).unwrap();
    assert_eq!(inspection.sequence, 2);
}
