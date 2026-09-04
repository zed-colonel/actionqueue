//! Clean-break store scaffold for `AQ-CONT-1`.
//!
//! At `AQ-01` these tests characterize the archived pre-contract fixtures with the
//! **baseline** reader: they prove the archived bytes are genuine WAL v5 / snapshot
//! schema 8 artifacts and record how the baseline treats them. Every fixture is copied
//! into a scratch directory first; the archive itself is never opened for writing.
//!
//! `AQ-03` replaces the characterization tests with rejection tests and un-ignores
//! `target_store_rejects_pre_contract_data`.

mod support;

use std::fs;
use std::path::{Path, PathBuf};

use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
use serde_json::Value;
use support::{repo_root, scratch_dir};

const ARCHIVE: &str = "archive/pre-aq-cont-1";
const WAL_FIXTURE: &str = "selected-wal-fixtures/lifecycle-wal-v5.wal";
const WAL_TRUNCATED: &str = "selected-wal-fixtures/lifecycle-wal-v5-truncated-tail.wal";
const SNAPSHOT_FIXTURE: &str = "selected-snapshot-fixtures/lifecycle-snapshot-schema8.bin";
const SNAPSHOT_CORRUPT: &str =
    "selected-snapshot-fixtures/lifecycle-snapshot-schema8-crc-mismatch.bin";

fn archive_path(rel: &str) -> PathBuf {
    repo_root().join(ARCHIVE).join(rel)
}

fn expected_scenario() -> Value {
    let text = fs::read_to_string(archive_path("selected-wal-fixtures/expected.json"))
        .expect("expected.json readable");
    serde_json::from_str::<Value>(&text).expect("expected.json parses")["scenario"].clone()
}

/// Copies archived fixtures into a fresh baseline data-directory layout.
fn stage_data_dir(label: &str, wal: &str, snapshot: Option<&str>) -> PathBuf {
    let dir = scratch_dir(label);
    fs::create_dir_all(dir.join("wal")).expect("wal dir");
    fs::create_dir_all(dir.join("snapshots")).expect("snapshot dir");
    fs::copy(archive_path(wal), dir.join("wal").join("actionqueue.wal")).expect("copy wal");
    if let Some(snapshot) = snapshot {
        fs::copy(archive_path(snapshot), dir.join("snapshots").join("snapshot.bin"))
            .expect("copy snapshot");
    }
    dir
}

fn run_state(
    projection: &actionqueue_storage::recovery::reducer::ReplayReducer,
    run_id: &str,
) -> String {
    use std::str::FromStr;
    let id = actionqueue_core::ids::RunId::from_str(run_id).expect("run id");
    projection.get_run_state(&id).map(|s| s.to_string()).unwrap_or_else(|| "missing".into())
}

fn assert_lifecycle_facts(data_dir: &Path, expected: &Value, expected_sequence: u64) {
    let recovery = load_projection_from_storage(data_dir).expect("baseline bootstrap");
    let projection = &recovery.projection;
    assert_eq!(projection.task_count() as u64, expected["task_count"].as_u64().unwrap());
    assert_eq!(projection.run_count() as u64, expected["run_count"].as_u64().unwrap());
    assert_eq!(projection.latest_sequence(), expected_sequence);
    for run in expected["runs"].as_array().expect("runs") {
        let run_id = run["run_id"].as_str().expect("run_id");
        assert_eq!(
            run_state(projection, run_id),
            run["state"].as_str().expect("state"),
            "{run_id}"
        );
    }
}

#[test]
fn archived_wal_is_a_genuine_pre_contract_store_readable_by_the_baseline_reader() {
    let expected = expected_scenario();
    let dir = stage_data_dir("wal-only", WAL_FIXTURE, None);
    assert_lifecycle_facts(&dir, &expected, expected["latest_sequence"].as_u64().unwrap());
    let recovery = load_projection_from_storage(&dir).expect("baseline bootstrap");
    assert!(!recovery.snapshot_loaded);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn archived_truncated_wal_is_refused_strictly_and_repairable_explicitly_at_baseline() {
    use actionqueue_storage::wal::fs_writer::WalFsWriter;
    use actionqueue_storage::wal::repair::RepairPolicy;

    let expected = expected_scenario();
    let dir = stage_data_dir("wal-truncated", WAL_TRUNCATED, None);
    let wal_path = dir.join("wal").join("actionqueue.wal");

    // Baseline bootstrap opens the WAL under RepairPolicy::Strict and refuses the tail.
    let refused = load_projection_from_storage(&dir).err().expect("strict bootstrap must refuse");
    let message = refused.to_string();
    assert!(message.contains("incomplete_header"), "unexpected refusal: {message}");

    // Explicit repair truncates only the severed final record (the EngineResume).
    let writer = WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
        .expect("explicit repair should succeed");
    drop(writer);
    let full_sequence = expected["latest_sequence"].as_u64().unwrap();
    assert_lifecycle_facts(&dir, &expected, full_sequence - 1);
    let recovery = load_projection_from_storage(&dir).expect("baseline bootstrap after repair");
    assert!(recovery.projection.is_engine_paused(), "lost EngineResume leaves the engine paused");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn archived_snapshot_is_loaded_by_the_baseline_reader() {
    let expected = expected_scenario();
    let dir = stage_data_dir("snapshot", WAL_FIXTURE, Some(SNAPSHOT_FIXTURE));
    let sequence = expected["latest_sequence"].as_u64().unwrap();
    assert_lifecycle_facts(&dir, &expected, sequence);
    let recovery = load_projection_from_storage(&dir).expect("baseline bootstrap");
    assert!(recovery.snapshot_loaded, "schema-8 snapshot should be accepted by the baseline");
    assert_eq!(recovery.snapshot_sequence, sequence);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn archived_corrupt_snapshot_falls_back_to_wal_replay_under_the_baseline_reader() {
    let expected = expected_scenario();
    let dir = stage_data_dir("snapshot-corrupt", WAL_FIXTURE, Some(SNAPSHOT_CORRUPT));
    assert_lifecycle_facts(&dir, &expected, expected["latest_sequence"].as_u64().unwrap());
    let recovery = load_projection_from_storage(&dir).expect("baseline bootstrap");
    assert!(!recovery.snapshot_loaded, "CRC mismatch must discard the snapshot");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn archived_fixtures_are_never_modified_by_characterization() {
    for rel in [WAL_FIXTURE, WAL_TRUNCATED, SNAPSHOT_FIXTURE, SNAPSHOT_CORRUPT] {
        let path = archive_path(rel);
        let listing = fs::read_to_string(archive_path("SHA256SUMS")).expect("SHA256SUMS");
        let pinned = support::parse_sha256sums(&listing)
            .into_iter()
            .find(|(_, p)| p == rel)
            .map(|(h, _)| h)
            .unwrap_or_else(|| panic!("{rel} pinned"));
        assert_eq!(support::sha256_file(&path), pinned, "{rel} changed on disk");
    }
}

/// Activated by `AQ-03`. The target store must refuse to open a nonempty data
/// directory that lacks an `AQ-CONT-1` manifest, must not modify it, and must
/// report `UnsupportedStoreFormat` / `MissingTargetManifest` (see `AQ-ADR-001`).
#[test]
#[ignore = "AQ-03: target store must reject pre-contract data without modifying it"]
fn target_store_rejects_pre_contract_data() {
    let dir = stage_data_dir("reject-old", WAL_FIXTURE, Some(SNAPSHOT_FIXTURE));
    let before = support::sha256_file(&dir.join("wal").join("actionqueue.wal"));
    // AQ-03 replaces the next line with the target bootstrap entry point and asserts
    // the documented rejection error instead of a successful load.
    let outcome = load_projection_from_storage(&dir);
    let after = support::sha256_file(&dir.join("wal").join("actionqueue.wal"));
    assert_eq!(before, after, "rejected store must be left untouched");
    assert!(
        outcome.is_err(),
        "pre-contract data must not be silently accepted by the target store"
    );
    let _ = fs::remove_dir_all(&dir);
}
