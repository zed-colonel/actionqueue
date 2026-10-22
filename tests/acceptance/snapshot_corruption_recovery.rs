//! P6-010 Snapshot corruption recovery acceptance proof.
//!
//! Verifies that a corrupt or missing snapshot file does NOT prevent recovery.
//! The system falls back to WAL-only replay as the charter mandates: snapshots
//! are derived acceleration artifacts, not authoritative state. A corrupt
//! snapshot must never block bootstrap.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
use actionqueue_storage::snapshot::build::build_snapshot_from_projection;
use actionqueue_storage::snapshot::writer::{SnapshotFsWriter, SnapshotWriter};

/// Writes a valid snapshot from the current projection at the given data_dir.
///
/// This helper bootstraps from storage, builds a snapshot from the recovered
/// projection, and writes it to the canonical snapshot path.
fn write_valid_snapshot(data_dir: &std::path::Path) {
    let recovery = load_projection_from_storage(data_dir)
        .expect("bootstrap should succeed for snapshot write helper");
    let snapshot = build_snapshot_from_projection(&recovery.projection, 1000)
        .expect("snapshot build should succeed from valid projection");
    let snapshot_path = data_dir.join("snapshots").join("snapshot.bin");
    std::fs::create_dir_all(snapshot_path.parent().expect("snapshot dir parent should exist"))
        .expect("snapshot directory should be creatable");
    let mut writer = SnapshotFsWriter::new(recovery.wal_writer.inner().session().unwrap())
        .expect("snapshot writer creation should succeed");
    writer.write(&snapshot).expect("snapshot write should succeed");
    writer.flush().expect("snapshot flush should succeed");
    writer.close().expect("snapshot close should succeed");
}

// ---------------------------------------------------------------------------
// Test SC-A: A corrupt snapshot (truncated to 5 bytes -- incomplete header)
// falls back to WAL-only replay and recovers all durable state.
// ---------------------------------------------------------------------------
#[test]
fn sc_a_corrupt_snapshot_falls_back_to_wal_replay() {
    let data_dir = support::unique_data_dir("snapshot-corruption-sc-a");
    let task_id_str = "a6010000-0000-0000-0000-000000000001";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Step 1: Submit a Once task and complete it via authority (writes WAL events).
    let submit = support::submit_once_task_via_cli(task_id_str, &data_dir);
    assert_eq!(submit["runs_created"], 1);
    let completion = support::complete_once_run_via_authority(&data_dir, task_id);
    assert!(completion.final_sequence > 0, "completion should durably advance WAL sequence");

    // Step 2: Build and write a valid snapshot from the current projection.
    write_valid_snapshot(&data_dir);

    // Verify: Snapshot file exists and is non-trivial.
    let snapshot_path = data_dir.join("snapshots").join("snapshot.bin");
    let original_len =
        std::fs::metadata(&snapshot_path).expect("snapshot file should exist after write").len();
    assert!(
        original_len > 12,
        "snapshot file should contain header + payload (got {original_len} bytes)"
    );

    // Step 3: Corrupt the snapshot by truncating it to 5 bytes (incomplete header).
    {
        let bytes = std::fs::read(&snapshot_path).expect("snapshot file should be readable");
        std::fs::write(&snapshot_path, &bytes[..5]).expect("truncated snapshot should be writable");
    }
    let corrupted_len =
        std::fs::metadata(&snapshot_path).expect("corrupted snapshot file should exist").len();
    assert_eq!(corrupted_len, 5, "snapshot should be truncated to exactly 5 bytes");

    // Step 4: Bootstrap from storage -- must succeed via WAL-only fallback.
    let recovery = load_projection_from_storage(&data_dir)
        .expect("recovery should succeed despite corrupt snapshot");

    // Step 5: Assert snapshot was NOT loaded (fallback to WAL-only replay).
    assert!(!recovery.snapshot_loaded, "snapshot_loaded must be false when snapshot is corrupt");
    assert_eq!(
        recovery.snapshot_sequence, 0,
        "snapshot_sequence must be 0 when snapshot is skipped"
    );

    // Step 6: Assert the completed run is present with correct state.
    assert_eq!(recovery.projection.task_count(), 1, "task must survive WAL-only recovery");
    assert_eq!(recovery.projection.run_count(), 1, "run must survive WAL-only recovery");

    let recovered_state = recovery
        .projection
        .get_run_state(&completion.run_id)
        .expect("run must be present in projection after recovery");
    assert_eq!(
        *recovered_state,
        RunState::Completed,
        "run must be in Completed state after WAL-only recovery"
    );

    // Step 7: Assert WAL replay applied all events (no snapshot contribution).
    assert_eq!(
        recovery.recovery_observations.snapshot_events_applied, 0,
        "snapshot events must be 0 when snapshot is skipped"
    );
    assert!(
        recovery.recovery_observations.wal_replay_events_applied > 0,
        "WAL replay must apply events when snapshot is corrupt"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test SC-B: A snapshot with a flipped byte in the CRC region (bytes 8-12)
// falls back to WAL-only replay and recovers all durable state.
// ---------------------------------------------------------------------------
#[test]
fn sc_b_truncated_snapshot_falls_back_to_wal_replay() {
    let data_dir = support::unique_data_dir("snapshot-corruption-sc-b");
    let task_id_str = "a6010000-0000-0000-0000-000000000002";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Step 1: Submit a Once task and complete it via authority.
    let submit = support::submit_once_task_via_cli(task_id_str, &data_dir);
    assert_eq!(submit["runs_created"], 1);
    let completion = support::complete_once_run_via_authority(&data_dir, task_id);
    assert!(completion.final_sequence > 0);

    // Step 2: Write a valid snapshot.
    write_valid_snapshot(&data_dir);

    // Step 3: Corrupt the snapshot by flipping a byte in the CRC region (bytes 8..12).
    let snapshot_path = data_dir.join("snapshots").join("snapshot.bin");
    {
        let mut bytes = std::fs::read(&snapshot_path).expect("snapshot file should be readable");
        assert!(bytes.len() > 12, "snapshot must have at least a full header");
        // Flip a byte in the CRC-32 region (byte index 8 is the first CRC byte).
        bytes[16] ^= 0xFF;
        std::fs::write(&snapshot_path, &bytes).expect("corrupted snapshot should be writable");
    }

    // Step 4: Bootstrap from storage -- must succeed via WAL-only fallback.
    let recovery = load_projection_from_storage(&data_dir)
        .expect("recovery should succeed despite CRC-corrupted snapshot");

    // Step 5: Assert recovery succeeds and the run state is Completed.
    assert_eq!(recovery.projection.task_count(), 1, "task must survive recovery");
    assert_eq!(recovery.projection.run_count(), 1, "run must survive recovery");

    let recovered_state = recovery
        .projection
        .get_run_state(&completion.run_id)
        .expect("run must be present in projection after recovery");
    assert_eq!(
        *recovered_state,
        RunState::Completed,
        "run must be in Completed state after WAL-only recovery"
    );

    // The snapshot should have been skipped due to CRC mismatch.
    assert!(!recovery.snapshot_loaded, "snapshot_loaded must be false when CRC is corrupted");
    assert_eq!(
        recovery.recovery_observations.snapshot_events_applied, 0,
        "snapshot events must be 0 when snapshot CRC is corrupted"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test SC-C: A missing snapshot file (never written) results in clean
// WAL-only replay with no errors.
// ---------------------------------------------------------------------------
#[test]
fn sc_c_missing_snapshot_uses_wal_only() {
    let data_dir = support::unique_data_dir("snapshot-corruption-sc-c");
    let task_id_str = "a6010000-0000-0000-0000-000000000003";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Step 1: Submit a Once task and complete it via authority.
    let submit = support::submit_once_task_via_cli(task_id_str, &data_dir);
    assert_eq!(submit["runs_created"], 1);
    let completion = support::complete_once_run_via_authority(&data_dir, task_id);
    assert!(completion.final_sequence > 0);

    // Step 2: Do NOT write any snapshot. Verify it does not exist.
    let snapshot_path = data_dir.join("snapshots").join("snapshot.bin");
    assert!(!snapshot_path.exists(), "snapshot file must not exist for this scenario");

    // Step 3: Bootstrap from storage -- must succeed via WAL-only replay.
    let recovery =
        load_projection_from_storage(&data_dir).expect("recovery should succeed without snapshot");

    // Step 4: Assert snapshot_loaded is false.
    assert!(
        !recovery.snapshot_loaded,
        "snapshot_loaded must be false when no snapshot file exists"
    );
    assert_eq!(
        recovery.snapshot_sequence, 0,
        "snapshot_sequence must be 0 when no snapshot exists"
    );

    // Step 5: Assert the run state is Completed.
    assert_eq!(recovery.projection.task_count(), 1, "task must survive WAL-only recovery");
    assert_eq!(recovery.projection.run_count(), 1, "run must survive WAL-only recovery");

    let recovered_state = recovery
        .projection
        .get_run_state(&completion.run_id)
        .expect("run must be present in projection after recovery");
    assert_eq!(
        *recovered_state,
        RunState::Completed,
        "run must be in Completed state after WAL-only recovery"
    );

    // Step 6: Confirm all state came from WAL replay.
    assert_eq!(
        recovery.recovery_observations.snapshot_events_applied, 0,
        "snapshot events must be 0 when no snapshot exists"
    );
    assert!(
        recovery.recovery_observations.wal_replay_events_applied > 0,
        "WAL replay must apply events when no snapshot exists"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test SC-F: A checkpoint the runtime publishes always loads at startup, even
// when the wall clock steps backwards between a required child's cancellation
// and its parent's completion. Live operation accepts that completion; startup
// must accept the checkpoint that contains it.
// ---------------------------------------------------------------------------
mod backward_clock {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{mpsc, Arc, Mutex};

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{ChildLifecyclePolicy, TaskPayload, TaskSpec};
    use actionqueue_executor_local::handler::{
        AttemptDisposition, ExecutorContext, ExecutorHandler,
    };
    use actionqueue_runtime::config::RuntimeConfig;
    use actionqueue_runtime::engine::ActionQueueEngine;
    use actionqueue_storage::recovery::bootstrap::load_existing_projection;
    use actionqueue_storage::store::{open_store, verify_store, OpenOptions};

    #[derive(Clone)]
    struct SharedClock(Arc<AtomicU64>);
    impl actionqueue_core::time::clock::Clock for SharedClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// Holds the parent's attempt open until the test releases it.
    #[derive(Clone)]
    struct ParentGate(Arc<Mutex<mpsc::Receiver<()>>>);
    impl ExecutorHandler for ParentGate {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            if ctx.input.payload == b"parent" {
                self.0.lock().unwrap().recv().unwrap();
            }
            AttemptDisposition::complete(None)
        }
    }

    fn spec(payload: &[u8]) -> TaskSpec {
        TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(payload.to_vec()),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn sc_f_checkpoint_after_backward_clock_step_loads_at_startup() {
        let data_dir = super::support::unique_data_dir("snapshot-corruption-sc-f");
        let clock = SharedClock(Arc::new(AtomicU64::new(1000)));
        let (release, gate) = mpsc::channel();
        // Publish a checkpoint whenever any durable progress is pending.
        let config = RuntimeConfig {
            data_dir: data_dir.clone(),
            snapshot_event_threshold: Some(1),
            ..RuntimeConfig::default()
        };
        let mut engine = ActionQueueEngine::new(config, ParentGate(Arc::new(Mutex::new(gate))))
            .bootstrap_with_clock(clock.clone())
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("sc-f").unwrap(),
                ),
            });
        let state =
            |e: &actionqueue_runtime::engine::BootstrappedEngine<ParentGate, SharedClock>,
             task: TaskId| {
                e.projection().run_instances().find(|r| r.task_id() == task).map(|r| r.state())
            };
        let parent = spec(b"parent");
        let parent_id = parent.id();
        engine.submit_task(parent).unwrap();
        for _ in 0..20 {
            let _ = engine.tick().await.unwrap();
            if state(&engine, parent_id) == Some(RunState::Running) {
                break;
            }
        }
        assert_eq!(state(&engine, parent_id), Some(RunState::Running));
        let child = spec(b"child").with_parent_policy(parent_id, ChildLifecyclePolicy::Required);
        let child_id = child.id();
        engine.submit_task(child).unwrap();
        clock.0.store(1050, Ordering::SeqCst);
        engine.cancel_task(child_id).unwrap();
        assert_eq!(state(&engine, child_id), Some(RunState::Canceled));
        // The clock steps back ten seconds before the parent's result commits.
        clock.0.store(1040, Ordering::SeqCst);
        release.send(()).unwrap();
        for _ in 0..100 {
            let _ = engine.tick().await.unwrap();
            if state(&engine, parent_id) == Some(RunState::Completed) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(state(&engine, parent_id), Some(RunState::Completed));
        let _ = engine.tick().await.unwrap();
        let latest = engine.projection().latest_sequence();
        let digest = engine.projection().projection_digest().unwrap();
        engine.shutdown().unwrap();

        // Writable startup, snapshot startup and full verification all load it.
        drop(open_store(&data_dir, OpenOptions::ReadWrite).unwrap().into_authority().unwrap());
        let recovered = load_existing_projection(&data_dir).unwrap();
        assert!(recovered.snapshot_loaded);
        assert_eq!(recovered.snapshot_sequence, latest);
        assert_eq!(recovered.projection.projection_digest().unwrap(), digest);
        drop(recovered);
        let verified = verify_store(&data_dir).unwrap();
        assert_eq!(verified.snapshot_sequence, latest);
        assert_eq!(verified.projection_digest, digest);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
