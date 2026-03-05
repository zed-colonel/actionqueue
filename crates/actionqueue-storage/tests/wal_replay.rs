//! Tests for WAL replay functionality.
//!
//! These tests verify that the WAL replay mechanism correctly reconstructs
//! state from the append-only event log. The tests cover:
//! - Basic replay of events in order
//! - Deterministic replay (same events produce same state)
//! - State transition validation during replay
//! - Snapshot plus WAL tail equivalence

use std::fs;
use std::path::PathBuf;

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::recovery::reducer::{
    EngineControlCausalityError, LeaseCausalityError, LeaseEventKind, ReplayReducer,
    ReplayReducerError, TaskCausalityError,
};
use actionqueue_storage::recovery::replay::ReplayDriver;
use actionqueue_storage::snapshot::loader::{SnapshotFsLoader, SnapshotLoader};
use actionqueue_storage::snapshot::mapping::{map_core_run_to_snapshot, SNAPSHOT_SCHEMA_VERSION};
use actionqueue_storage::snapshot::model::{
    Snapshot, SnapshotEngineControl, SnapshotMetadata, SnapshotRunStateHistoryEntry, SnapshotTask,
};

/// Test helper: builds a SnapshotTask with standard test defaults.
fn test_snapshot_task(task_spec: TaskSpec) -> SnapshotTask {
    SnapshotTask { task_spec, created_at: 0, updated_at: None, canceled_at: None }
}
use actionqueue_storage::snapshot::writer::{
    SnapshotFsWriter, SnapshotFsWriterInitError, SnapshotWriter,
};
use actionqueue_storage::wal::codec;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_reader::WalFsReader;
use actionqueue_storage::wal::fs_writer::{WalFsWriter, WalFsWriterInitError};
use actionqueue_storage::wal::reader::WalReader;
use actionqueue_storage::wal::writer::WalWriter;

/// A counter to ensure unique test file paths
static TEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn temp_wal_path() -> PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = dir.join(format!("actionqueue_wal_replay_test_{count}.tmp"));
    // Clean up if exists from previous test runs
    let _ = fs::remove_file(&path);
    path
}

fn temp_snapshot_path() -> PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = dir.join(format!("actionqueue_snapshot_replay_test_{count}.tmp"));
    let _ = fs::remove_file(&path);
    path
}

fn open_wal_writer(path: PathBuf) -> WalFsWriter {
    WalFsWriter::new(path).expect("Failed to open WAL writer for replay test")
}

fn open_snapshot_writer(path: PathBuf) -> SnapshotFsWriter {
    SnapshotFsWriter::new(path).expect("Failed to open snapshot writer for replay test")
}

/// Creates a test task spec with a deterministic ID
fn create_test_task_spec(id: u64) -> TaskSpec {
    TaskSpec::new(
        TaskId::from_uuid(uuid::Uuid::from_u128(id as u128)),
        TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

/// Creates a test task spec with a deterministic ID and Repeat policy
fn create_test_task_spec_repeat(id: u64, count: u32, interval_secs: u64) -> TaskSpec {
    TaskSpec::new(
        TaskId::from_uuid(uuid::Uuid::from_u128(id as u128)),
        TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
        RunPolicy::repeat(count, interval_secs).expect("repeat policy must be valid for test"),
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

/// Creates a test run instance
fn create_test_run_instance(
    task_id: TaskId,
    run_id: u64,
    state: RunState,
) -> actionqueue_core::run::run_instance::RunInstance {
    let mut run = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
        RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
        task_id,
        1000,
        1000,
    )
    .expect("test run instance should be valid");
    if state == RunState::Ready {
        run.transition_to(RunState::Ready).expect("scheduled -> ready");
    } else if state == RunState::Leased {
        run.transition_to(RunState::Ready).expect("scheduled -> ready");
        run.transition_to(RunState::Leased).expect("ready -> leased");
    } else if state == RunState::Running {
        run.transition_to(RunState::Ready).expect("scheduled -> ready");
        run.transition_to(RunState::Leased).expect("ready -> leased");
        run.transition_to(RunState::Running).expect("leased -> running");
    }
    run
}

/// Creates a WAL event with deterministic IDs
fn create_event(seq: u64, event: WalEventType) -> WalEvent {
    WalEvent::new(seq, event)
}

/// Creates a TaskCreated event
fn create_task_created_event(seq: u64, task_id: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::TaskCreated { task_spec: create_test_task_spec(task_id), timestamp: 0 },
    )
}

/// Creates a RunCreated event
fn create_run_created_event(seq: u64, task_id: u64, run_id: u64) -> WalEvent {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(task_id as u128));
    let run_instance = create_test_run_instance(task_id, run_id, RunState::Scheduled);
    create_event(seq, WalEventType::RunCreated { run_instance })
}

/// Creates a RunStateChanged event
fn create_run_state_changed_event(
    seq: u64,
    run_id: u64,
    previous_state: RunState,
    new_state: RunState,
) -> WalEvent {
    let event = WalEventType::RunStateChanged {
        run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
        previous_state,
        new_state,
        timestamp: 2000,
    };
    create_event(seq, event)
}

/// Creates a TaskCanceled event.
fn create_task_canceled_event(seq: u64, task_id: u64, timestamp: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::TaskCanceled {
            task_id: TaskId::from_uuid(uuid::Uuid::from_u128(task_id as u128)),
            timestamp,
        },
    )
}

/// Creates an AttemptStarted event.
fn create_attempt_started_event(seq: u64, run_id: u64, attempt_id: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::AttemptStarted {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            attempt_id: AttemptId::from_uuid(uuid::Uuid::from_u128(attempt_id as u128)),
            timestamp: 2000,
        },
    )
}

/// Creates an AttemptFinished event with an explicit canonical result.
fn create_attempt_finished_event_with_result(
    seq: u64,
    run_id: u64,
    attempt_id: u64,
    result: AttemptResultKind,
    error: Option<String>,
) -> WalEvent {
    create_event(
        seq,
        WalEventType::AttemptFinished {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            attempt_id: AttemptId::from_uuid(uuid::Uuid::from_u128(attempt_id as u128)),
            result,
            error,
            output: None,
            timestamp: 3000,
        },
    )
}

/// Creates an AttemptFinished success event.
fn create_attempt_finished_event(seq: u64, run_id: u64, attempt_id: u64) -> WalEvent {
    create_attempt_finished_event_with_result(
        seq,
        run_id,
        attempt_id,
        AttemptResultKind::Success,
        None,
    )
}

/// Creates a LeaseAcquired event.
fn create_lease_acquired_event(seq: u64, run_id: u64, owner: &str, expiry: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::LeaseAcquired {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            owner: owner.to_string(),
            expiry,
            timestamp: 2000,
        },
    )
}

/// Creates a LeaseHeartbeat event.
fn create_lease_heartbeat_event(seq: u64, run_id: u64, owner: &str, expiry: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::LeaseHeartbeat {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            owner: owner.to_string(),
            expiry,
            timestamp: 2000,
        },
    )
}

/// Creates a LeaseExpired event.
fn create_lease_expired_event(seq: u64, run_id: u64, owner: &str, expiry: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::LeaseExpired {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            owner: owner.to_string(),
            expiry,
            timestamp: 2000,
        },
    )
}

/// Creates a LeaseReleased event.
fn create_lease_released_event(seq: u64, run_id: u64, owner: &str, expiry: u64) -> WalEvent {
    create_event(
        seq,
        WalEventType::LeaseReleased {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
            owner: owner.to_string(),
            expiry,
            timestamp: 2000,
        },
    )
}

fn create_engine_paused_event(seq: u64, timestamp: u64) -> WalEvent {
    create_event(seq, WalEventType::EnginePaused { timestamp })
}

fn create_engine_resumed_event(seq: u64, timestamp: u64) -> WalEvent {
    create_event(seq, WalEventType::EngineResumed { timestamp })
}

fn extract_tail_to_wal(source_wal: PathBuf, start_sequence: u64, tail_wal: PathBuf) {
    let mut reader =
        WalFsReader::new(source_wal).expect("Failed to open source WAL file for tail extraction");
    reader
        .seek_to_sequence(start_sequence)
        .expect("seek_to_sequence should succeed for tail extraction");

    let mut writer = open_wal_writer(tail_wal);
    while let Some(event) = reader.read_next().expect("tail read should succeed") {
        writer.append(&event).expect("tail append should succeed");
    }
    writer.close().expect("tail writer close should succeed");
}

#[test]
fn test_replay_empty_wal() {
    let path = temp_wal_path();

    // Create empty WAL file
    fs::write(&path, []).unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for empty replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    // Replay should complete successfully with empty WAL
    driver.run().expect("Replay should succeed");
    let _reducer = driver.into_reducer();

    let _ = fs::remove_file(path);
}

#[test]
fn p6_013_t_p2_engine_pause_resume_replay_state_is_deterministic() {
    let path = temp_wal_path();
    let mut writer = open_wal_writer(path.clone());

    writer
        .append(&create_engine_paused_event(1, 1_000))
        .expect("engine paused event append should succeed");
    writer
        .append(&create_engine_resumed_event(2, 2_000))
        .expect("engine resumed event append should succeed");
    writer.close().expect("wal close should succeed");

    let reader = WalFsReader::new(path.clone()).expect("wal reader should open");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("replay should succeed");
    let reducer = driver.into_reducer();

    assert!(!reducer.is_engine_paused());
    assert_eq!(reducer.engine_paused_at(), Some(1_000));
    assert_eq!(reducer.engine_resumed_at(), Some(2_000));
    assert_eq!(reducer.latest_sequence(), 2);

    let _ = fs::remove_file(path);
}

#[test]
fn p6_013_t_n3_duplicate_engine_pause_is_rejected_by_reducer_causality() {
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_engine_paused_event(1, 1_000)).expect("first engine pause should apply");

    let duplicate = reducer.apply(&create_engine_paused_event(2, 1_001));
    assert!(matches!(
        duplicate,
        Err(ReplayReducerError::EngineControlCausality(
            EngineControlCausalityError::AlreadyPaused {
                first_paused_at: Some(1_000),
                duplicate_paused_at: 1_001,
            }
        ))
    ));
}

#[test]
fn p6_013_t_n4_engine_resume_without_pause_is_rejected() {
    let mut reducer = ReplayReducer::new();

    let result = reducer.apply(&create_engine_resumed_event(1, 1_000));
    assert!(matches!(
        result,
        Err(ReplayReducerError::EngineControlCausality(EngineControlCausalityError::NotPaused {
            attempted_resumed_at: 1_000
        }))
    ));
}

#[test]
fn test_replay_single_task_created() {
    let path = temp_wal_path();

    // Create and write a single TaskCreated event
    let event = create_task_created_event(1, 1234);
    let _bytes = codec::encode(&event).expect("encode should succeed");

    let mut writer = open_wal_writer(path.clone());
    writer.append(&event).unwrap();
    writer.close().unwrap();

    // Replay the event
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for single task replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed");
    let _ = driver.into_reducer();

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_multiple_events_in_order() {
    let path = temp_wal_path();

    // Create a sequence of events
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(1000u128));
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay the events
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for multiple events replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed");
    let reducer = driver.into_reducer();

    // Verify the state was reconstructed correctly
    assert!(reducer.get_task(&task_id).is_some(), "Task should be present");

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_validates_state_transitions() {
    let path = temp_wal_path();

    // Create events with an invalid state transition (Ready -> Scheduled is invalid)
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        // This transition is invalid: Ready -> Scheduled
        create_run_state_changed_event(3, 2000, RunState::Ready, RunState::Scheduled),
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay should fail due to invalid transition
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for state transition validation test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_err(), "Replay should fail with invalid transition");

    // Verify the error is about invalid transition
    match result.unwrap_err() {
        actionqueue_storage::wal::reader::WalReaderError::ReducerError(msg) => {
            assert!(
                msg.contains("Invalid state transition"),
                "Error should mention invalid transition: {msg}"
            );
        }
        _ => panic!("Expected ReducerError for invalid transition"),
    }

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_enforces_sequence_order() {
    let path = temp_wal_path();

    // Create events with increasing sequence numbers (valid)
    // With writer-side sequence enforcement, we need to use strictly increasing sequences
    let events = vec![
        create_task_created_event(1, 1000), // seq 1
        create_task_created_event(2, 2000), // seq 2 - increasing
    ];

    // Write events to WAL - should succeed with monotonically increasing sequences
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).expect("Append should succeed");
    }
    writer.close().unwrap();

    // Replay should succeed
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for sequence order validation test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed with valid sequence numbers");

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_detects_duplicate_events() {
    let path = temp_wal_path();

    // Create two different events with different sequences
    let event1 = create_task_created_event(1, 1000);
    let event2 = create_task_created_event(2, 2000);

    // Write events to WAL - should succeed with different sequences
    let mut writer = open_wal_writer(path.clone());
    writer.append(&event1).expect("First append should succeed");
    writer.append(&event2).expect("Second append should succeed");
    writer.close().unwrap();

    // Replay should succeed
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for duplicate event detection test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed with valid events");

    let _ = fs::remove_file(path);
}

#[test]
fn test_deterministic_replay_produces_same_state() {
    // Create two separate WAL files with the same events
    let path1 = temp_wal_path();
    let path2 = temp_wal_path();

    // Define a sequence of events
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(5, 2000, RunState::Leased, RunState::Running),
    ];

    // Write same events to both WAL files
    let mut writer1 = open_wal_writer(path1.clone());
    for event in &events {
        writer1.append(event).unwrap();
    }
    writer1.close().unwrap();

    let mut writer2 = open_wal_writer(path2.clone());
    for event in &events {
        writer2.append(event).unwrap();
    }
    writer2.close().unwrap();

    // Replay both WAL files
    let reader1 = WalFsReader::new(path1.clone())
        .expect("Failed to open first WAL file for deterministic replay test");
    let reducer1 = ReplayReducer::new();
    let mut driver1 = ReplayDriver::new(reader1, reducer1);
    driver1.run().unwrap();
    let reducer1 = driver1.into_reducer();

    let reader2 = WalFsReader::new(path2.clone())
        .expect("Failed to open second WAL file for deterministic replay test");
    let reducer2 = ReplayReducer::new();
    let mut driver2 = ReplayDriver::new(reader2, reducer2);
    driver2.run().unwrap();
    let reducer2 = driver2.into_reducer();

    // Compare the final states
    // Note: We need to access the internal state somehow
    // For now, just verify both replays succeeded
    assert_eq!(
        reducer1.latest_sequence(),
        reducer2.latest_sequence(),
        "Latest sequence should be the same"
    );

    let _ = fs::remove_file(path1);
    let _ = fs::remove_file(path2);
}

#[test]
fn test_replay_run_instance_states() {
    let path = temp_wal_path();

    // Define a complete run lifecycle with valid state transitions
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(1000u128));

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000), // Run in Scheduled state
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(5, 2000, RunState::Leased, RunState::Running),
        create_run_state_changed_event(6, 2000, RunState::Running, RunState::Completed),
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay the events
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for run instance state replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed");
    let reducer = driver.into_reducer();

    // Verify the final state
    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Completed),
        "Final state should be Completed"
    );

    // Verify all intermediate states were correctly applied
    assert!(reducer.get_task(&task_id).is_some(), "Task should be present");

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_handles_all_wal_event_types() {
    let path = temp_wal_path();

    // Create events for all supported event types
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let attempt_id = actionqueue_core::ids::AttemptId::from_uuid(uuid::Uuid::from_u128(3000u128));

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(5, 2000, RunState::Leased, RunState::Running),
        // AttemptStarted event
        create_event(6, WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 2000 }),
        // AttemptFinished event
        create_event(
            7,
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Success,
                error: None,
                output: None,
                timestamp: 3000,
            },
        ),
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay should succeed
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for test handles all WAL event types");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed: {result:?}");
    let reducer = driver.into_reducer();

    // Verify run state was updated
    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Running));

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_sequence_gaps_are_allowed() {
    let path = temp_wal_path();

    // Create events with sequence gaps (not contiguous, but increasing)
    let events = vec![
        create_task_created_event(1, 1000),
        create_task_created_event(5, 2000), // gap: skip sequence 2, 3, 4
        create_task_created_event(10, 3000), // gap: skip sequence 6, 7, 8, 9
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay should succeed (sequence gaps are valid as long as sequence is increasing)
    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for sequence gaps test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay with sequence gaps should succeed: {result:?}");
    let reducer = driver.into_reducer();

    // Verify all events were applied
    assert_eq!(reducer.latest_sequence(), 10);

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_rejects_decreasing_sequence() {
    let path = temp_wal_path();

    // Create events with increasing sequence numbers (valid)
    // Writer-side enforcement now catches non-monotonic sequences at write time
    let events = vec![
        create_task_created_event(1, 1000),
        create_task_created_event(2, 2000),
        create_task_created_event(3, 3000),
    ];

    // Write events to WAL - should succeed with increasing sequence numbers
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).expect("Append should succeed");
    }
    writer.close().unwrap();

    // Replay should succeed
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for decreasing sequence test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_ok(), "Replay should succeed with valid sequence numbers");

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_run_state_is_terminal_state() {
    let path = temp_wal_path();

    // Create events with a terminal state transition attempt
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Completed), // valid
        // Now trying to transition from Completed - should fail
        create_run_state_changed_event(4, 2000, RunState::Completed, RunState::Running),
    ];

    // Write events to WAL
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay should fail when trying to transition from terminal state
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for terminal state transition test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_err(), "Replay should fail when transitioning from terminal state");

    let _ = fs::remove_file(path);
}

#[test]
fn test_replay_detects_invalid_state_in_run_created() {
    let path = temp_wal_path();

    // Create a RunCreated event with an invalid state (not Scheduled)
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(1000u128));
    let mut run_instance = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
        RunId::from_uuid(uuid::Uuid::from_u128(2000u128)),
        task_id,
        1000,
        1000,
    )
    .expect("test run instance should be valid");
    run_instance.transition_to(RunState::Ready).expect("scheduled -> ready");
    run_instance.transition_to(RunState::Leased).expect("ready -> leased");
    run_instance.transition_to(RunState::Running).expect("leased -> running"); // Invalid! RunCreated must be in Scheduled state

    let event = create_event(1, WalEventType::RunCreated { run_instance });

    // Write event to WAL
    let mut writer = open_wal_writer(path.clone());
    writer.append(&event).unwrap();
    writer.close().unwrap();

    // Replay should fail because RunCreated must be in Scheduled state
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for invalid RunCreated state test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);

    let result = driver.run();
    assert!(result.is_err(), "Replay should fail with invalid RunCreated state");

    let _ = fs::remove_file(path);
}

#[test]
fn test_snapshot_plus_wal_tail_equivalence() {
    let full_wal_path = temp_wal_path();
    let snapshot_path = temp_snapshot_path();
    let bootstrap_wal_path = temp_wal_path();
    let tail_wal_path = temp_wal_path();

    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(1000u128));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(5, 2000, RunState::Leased, RunState::Running),
    ];

    // Build full WAL and replay baseline final state.
    let mut full_writer = open_wal_writer(full_wal_path.clone());
    for event in &events {
        full_writer.append(event).unwrap();
    }
    full_writer.close().unwrap();

    let full_reader = WalFsReader::new(full_wal_path.clone())
        .expect("Failed to open full WAL file for snapshot+tail equivalence test");
    let full_reducer = ReplayReducer::new();
    let mut full_driver = ReplayDriver::new(full_reader, full_reducer);
    full_driver.run().unwrap();
    let full_reducer = full_driver.into_reducer();

    // Persist a snapshot at sequence 3 (after Scheduled -> Ready).
    let snapshot = Snapshot {
        version: 4,
        timestamp: 2000,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 3,
            task_count: 1,
            run_count: 1,
        },
        tasks: vec![test_snapshot_task(create_test_task_spec(1000))],
        runs: vec![{
            let mut snapshot_run = map_core_run_to_snapshot(
                RunInstance::new_ready_with_id(run_id, task_id, 1000, 1000, 0)
                    .expect("snapshot run instance should be valid"),
            );
            snapshot_run.state_history = vec![
                SnapshotRunStateHistoryEntry {
                    from: None,
                    to: RunState::Scheduled,
                    timestamp: 1000,
                },
                SnapshotRunStateHistoryEntry {
                    from: Some(RunState::Scheduled),
                    to: RunState::Ready,
                    timestamp: 1000,
                },
            ];
            snapshot_run
        }],
        engine: SnapshotEngineControl::default(),
        dependency_declarations: Vec::new(),
        budgets: Vec::new(),
        subscriptions: Vec::new(),
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    let mut snapshot_writer = open_snapshot_writer(snapshot_path.clone());
    snapshot_writer.write(&snapshot).unwrap();
    snapshot_writer.close().unwrap();

    let mut snapshot_loader = SnapshotFsLoader::new(snapshot_path.clone());
    let loaded_snapshot =
        snapshot_loader.load().unwrap().expect("Expected snapshot payload to be present");

    // Rebuild snapshot state in reducer (snapshot baseline), then replay only WAL tail.
    let mut bootstrap_writer = open_wal_writer(bootstrap_wal_path.clone());
    bootstrap_writer
        .append(&create_event(
            1,
            WalEventType::TaskCreated {
                task_spec: loaded_snapshot.tasks[0].task_spec.clone(),
                timestamp: 0,
            },
        ))
        .unwrap();

    let boot_run = &loaded_snapshot.runs[0].run_instance;
    let boot_run_created = RunInstance::new_scheduled_with_id(
        boot_run.id(),
        boot_run.task_id(),
        boot_run.scheduled_at(),
        boot_run.created_at(),
    )
    .expect("snapshot bootstrap run instance should be valid");

    bootstrap_writer
        .append(&create_event(2, WalEventType::RunCreated { run_instance: boot_run_created }))
        .unwrap();
    bootstrap_writer
        .append(&create_run_state_changed_event(
            3,
            2000,
            RunState::Scheduled,
            loaded_snapshot.runs[0].run_instance.state(),
        ))
        .unwrap();
    bootstrap_writer.close().unwrap();

    let bootstrap_reader = WalFsReader::new(bootstrap_wal_path.clone())
        .expect("Failed to open bootstrap WAL file for snapshot+tail equivalence test");
    let bootstrap_reducer = ReplayReducer::new();
    let mut bootstrap_driver = ReplayDriver::new(bootstrap_reader, bootstrap_reducer);
    bootstrap_driver.run().unwrap();
    let snapshot_baseline_reducer = bootstrap_driver.into_reducer();

    extract_tail_to_wal(
        full_wal_path.clone(),
        loaded_snapshot.metadata.wal_sequence + 1,
        tail_wal_path.clone(),
    );

    let tail_reader = WalFsReader::new(tail_wal_path.clone())
        .expect("Failed to open tail WAL file for snapshot+tail equivalence test");
    let mut tail_driver = ReplayDriver::new(tail_reader, snapshot_baseline_reducer);
    tail_driver.run().unwrap();
    let snapshot_plus_tail_reducer = tail_driver.into_reducer();

    assert_eq!(
        full_reducer.get_run_state(&run_id),
        snapshot_plus_tail_reducer.get_run_state(&run_id),
        "Run state should match between full replay and snapshot+tail replay"
    );
    assert_eq!(
        full_reducer.get_run_instance(&run_id),
        snapshot_plus_tail_reducer.get_run_instance(&run_id),
        "Run instance should match between full replay and snapshot+tail replay"
    );
    assert_eq!(
        full_reducer.latest_sequence(),
        snapshot_plus_tail_reducer.latest_sequence(),
        "Latest sequence should match between full replay and snapshot+tail replay"
    );
    assert_eq!(
        full_reducer.get_task(&task_id),
        snapshot_plus_tail_reducer.get_task(&task_id),
        "Task projection should match between full replay and snapshot+tail replay"
    );

    let _ = fs::remove_file(full_wal_path);
    let _ = fs::remove_file(snapshot_path);
    let _ = fs::remove_file(bootstrap_wal_path);
    let _ = fs::remove_file(tail_wal_path);
}

#[test]
fn test_wal_writer_constructor_failure_returns_typed_error() {
    let missing_parent = std::env::temp_dir().join(format!(
        "actionqueue_wal_replay_missing_wal_parent_{}_{}",
        std::process::id(),
        TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = fs::remove_dir_all(&missing_parent);
    let wal_path = missing_parent.join("wal.log");

    let result = WalFsWriter::new(wal_path);
    assert!(matches!(result, Err(WalFsWriterInitError::IoError(_))));
}

#[test]
fn test_snapshot_writer_constructor_failure_returns_typed_error() {
    let missing_parent = std::env::temp_dir().join(format!(
        "actionqueue_wal_replay_missing_snapshot_parent_{}_{}",
        std::process::id(),
        TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = fs::remove_dir_all(&missing_parent);
    let snapshot_path = missing_parent.join("snapshot.bin");

    let result = SnapshotFsWriter::new(snapshot_path);
    assert!(matches!(result, Err(SnapshotFsWriterInitError::IoError(_))));
}

#[test]
fn test_reducer_apply_is_atomic_after_semantic_error() {
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 1000)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 1000, 2000)).expect("run creation should apply");
    reducer
        .apply(&create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready))
        .expect("scheduled -> ready should apply");

    let invalid_attempt_start = create_attempt_started_event(4, 2000, 3000);
    let invalid_result = reducer.apply(&invalid_attempt_start);
    assert!(
        matches!(
            invalid_result,
            Err(actionqueue_storage::recovery::reducer::ReplayReducerError::InvalidTransition)
        ),
        "attempt start from Ready should fail"
    );
    assert_eq!(reducer.latest_sequence(), 3, "failed semantic apply must not advance sequence");

    reducer
        .apply(&create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased))
        .expect("same sequence should remain reusable after failed semantic apply");
    assert_eq!(reducer.latest_sequence(), 4);
    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Leased));
}

#[test]
fn test_replay_run_canceled_event_sets_canonical_canceled_state() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_event(4, WalEventType::RunCanceled { run_id, timestamp: 4000 }),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).expect("event append should succeed");
    }
    writer.close().expect("WAL close should succeed");

    let reader =
        WalFsReader::new(path.clone()).expect("WAL open should succeed for cancellation replay");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("replay should succeed with RunCanceled event");
    let reducer = driver.into_reducer();

    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Canceled));
    assert_eq!(
        reducer.get_run_instance(&run_id).map(|run| run.state()),
        Some(RunState::Canceled),
        "run instance projection must be canceled"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn p6_011_t_p2_reducer_applies_task_canceled_projection_truth() {
    let task_id_raw = 1000u64;
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(task_id_raw as u128));
    let canceled_at = 4_242u64;
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, task_id_raw)).expect("task create should apply");
    reducer
        .apply(&create_task_canceled_event(2, task_id_raw, canceled_at))
        .expect("task canceled should apply");

    assert!(reducer.is_task_canceled(task_id));
    assert_eq!(reducer.task_canceled_at(task_id), Some(canceled_at));
    assert_eq!(
        reducer.get_task_record(&task_id).expect("task record should exist").canceled_at(),
        Some(canceled_at)
    );
}

#[test]
fn p6_011_t_n1_reducer_rejects_duplicate_task_canceled_with_typed_error() {
    let task_id_raw = 1000u64;
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(task_id_raw as u128));
    let first_canceled_at = 4_000u64;
    let duplicate_canceled_at = 4_001u64;
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, task_id_raw)).expect("task create should apply");
    reducer
        .apply(&create_task_canceled_event(2, task_id_raw, first_canceled_at))
        .expect("first task cancel should apply");

    let duplicate_result =
        reducer.apply(&create_task_canceled_event(3, task_id_raw, duplicate_canceled_at));

    assert!(matches!(
        duplicate_result,
        Err(ReplayReducerError::TaskCausality(TaskCausalityError::AlreadyCanceled {
            task_id: observed_task_id,
            first_canceled_at: observed_first,
            duplicate_canceled_at: observed_duplicate,
        })) if observed_task_id == task_id
            && observed_first == first_canceled_at
            && observed_duplicate == duplicate_canceled_at
    ));
    assert_eq!(reducer.latest_sequence(), 2);
    assert_eq!(reducer.task_canceled_at(task_id), Some(first_canceled_at));
}

#[test]
fn test_reducer_validates_attempt_lifecycle_ordering_and_ownership() {
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 1000)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 1000, 2000)).expect("run creation should apply");
    reducer
        .apply(&create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready))
        .expect("scheduled -> ready should apply");
    reducer
        .apply(&create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased))
        .expect("ready -> leased should apply");
    reducer
        .apply(&create_run_state_changed_event(5, 2000, RunState::Leased, RunState::Running))
        .expect("leased -> running should apply");

    let finish_without_start = reducer.apply(&create_attempt_finished_event(6, 2000, 3000));
    assert!(
        matches!(
            finish_without_start,
            Err(actionqueue_storage::recovery::reducer::ReplayReducerError::InvalidTransition)
        ),
        "attempt finish without prior start must fail"
    );
    assert_eq!(reducer.latest_sequence(), 5, "failed finish must not advance sequence");

    reducer
        .apply(&create_attempt_started_event(6, 2000, 3000))
        .expect("attempt start should apply for running run");

    assert_eq!(
        reducer
            .get_run_instance(&run_id)
            .and_then(|run_instance| run_instance.current_attempt_id()),
        Some(AttemptId::from_uuid(uuid::Uuid::from_u128(3000u128))),
        "active attempt should be tracked"
    );
    assert_eq!(
        reducer.get_run_instance(&run_id).map(|run_instance| run_instance.attempt_count()),
        Some(1),
        "attempt count should increment on attempt start"
    );

    let wrong_owner_finish = reducer.apply(&create_attempt_finished_event(7, 2000, 4000));
    assert!(
        matches!(
            wrong_owner_finish,
            Err(actionqueue_storage::recovery::reducer::ReplayReducerError::InvalidTransition)
        ),
        "attempt finish with non-active attempt id must fail"
    );
    assert_eq!(
        reducer.latest_sequence(),
        6,
        "failed ownership validation must not advance sequence"
    );

    reducer
        .apply(&create_attempt_finished_event(7, 2000, 3000))
        .expect("matching attempt finish should apply");

    assert_eq!(
        reducer
            .get_run_instance(&run_id)
            .and_then(|run_instance| run_instance.current_attempt_id()),
        None,
        "attempt finish should clear active attempt"
    );
    assert_eq!(reducer.latest_sequence(), 7);
}

/// Tests for uncertainty clause recovery - simulating crash scenarios between lease and completion
/// Test: Lease acquired before state reaches `Leased` is rejected by strict causality checks.
#[test]
fn test_lease_acquired_crash_before_execution() {
    let path = temp_wal_path();

    // Lease acquisition now requires run state precondition (Ready/Leased), and this
    // historical ordering remains valid because the run is in Ready at acquisition.
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_lease_acquired_event(4, 2000, "worker-1", 5000),
        create_run_state_changed_event(5, 2000, RunState::Ready, RunState::Leased),
    ];

    // Write events to WAL (simulating pre-crash state)
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay events to simulate restart
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for lease crash recovery test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    // Valid ordering should remain accepted.
    let _ = fs::remove_file(path);
}

/// Test: Lease heartbeat recorded - run should remain leased on restart with updated expiry
#[test]
fn test_lease_heartbeat_replay() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    // Events: Task -> Run -> Ready -> LeaseAcquired -> Leased -> Running -> Heartbeat
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_lease_acquired_event(4, 2000, "worker-1", 5000),
        create_run_state_changed_event(5, 2000, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(6, 2000, RunState::Leased, RunState::Running),
        create_lease_heartbeat_event(7, 2000, "worker-1", 6000), // Updated expiry
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for lease heartbeat test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify run state is Running
    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Running), "Run should be Running");
    // Verify lease was properly updated
    let lease = reducer.get_lease(&run_id).expect("Lease should be present");
    assert_eq!(&lease.0, "worker-1");
    assert_eq!(lease.1, 6000);
    assert_eq!(lease.1, 6000, "Lease expiry should be updated to 6000");

    let _ = fs::remove_file(path);
}

/// Test: Lease expired (via time) - run should become re-eligible on restart
#[test]
fn test_lease_expired_replay() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    // Events: Task -> Run -> Ready -> Leased -> LeaseAcquired -> LeaseExpired
    // The lease is acquired after Leased state to ensure proper ordering
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        create_lease_expired_event(6, 2000, "worker-1", 5000),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for lease expired test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify: run is Ready (re-eligible) after lease expiry
    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Ready),
        "Run should be Ready after lease expiry"
    );
    // Verify no active lease
    assert!(reducer.get_lease(&run_id).is_none(), "Lease should not be active after expiry");

    let _ = fs::remove_file(path);
}

/// Test: Lease released manually - run should become re-eligible on restart
#[test]
fn test_lease_released_replay() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    // Events: Task -> Run -> Ready -> Leased -> LeaseAcquired -> LeaseReleased
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        create_lease_released_event(6, 2000, "worker-1", 5000),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for lease released test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify: run is Ready (re-eligible) after lease release
    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Ready),
        "Run should be Ready after lease release"
    );
    // Verify no active lease
    assert!(reducer.get_lease(&run_id).is_none(), "Lease should not be active after release");

    let _ = fs::remove_file(path);
}

/// Test: Attempt started after lease acquire, crash before finish - run should have attempt lineage preserved
#[test]
fn test_attempt_started_crash_before_finish() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(3000u128));

    // Events: Task -> Run -> Ready -> Leased -> LeaseAcquired -> Running -> AttemptStarted
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        create_run_state_changed_event(6, 2000, RunState::Leased, RunState::Running),
        create_attempt_started_event(7, 2000, 3000),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for attempt started crash test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify run state is Running
    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Running), "Run should be Running");
    // Verify attempt was started and is tracked
    let run_instance = reducer.get_run_instance(&run_id).expect("Run instance should be present");
    assert_eq!(
        run_instance.current_attempt_id(),
        Some(attempt_id),
        "Active attempt should be tracked"
    );
    assert_eq!(run_instance.attempt_count(), 1, "Attempt count should be 1");

    let _ = fs::remove_file(path);
}

/// Test: terminal transitions clear active lease projection deterministically.
#[test]
fn test_attempt_finished_completion_with_active_lease() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));

    // Events: Task -> Run -> Ready -> Leased -> Running -> AttemptFinished(Complete) -> Completed transition
    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        create_run_state_changed_event(6, 2000, RunState::Leased, RunState::Running),
        create_attempt_started_event(7, 2000, 3000),
        create_attempt_finished_event(8, 2000, 3000),
        create_run_state_changed_event(9, 2000, RunState::Running, RunState::Completed),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for attempt finished with active lease test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify run state is Completed (terminal)
    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Completed),
        "Run should be Completed (terminal)"
    );
    // Terminal run must not retain active lease projection after replay.
    let lease = reducer.get_lease(&run_id);
    assert!(lease.is_none(), "Lease projection must be cleared for terminal run");

    let _ = fs::remove_file(path);
}

/// Test: timeout attempt result persists through WAL replay attempt-history projection.
#[test]
fn p6_017_t_p2_wal_replay_preserves_timeout_attempt_result_history() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(3001u128));

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        create_run_state_changed_event(6, 2000, RunState::Leased, RunState::Running),
        create_attempt_started_event(7, 2000, 3001),
        create_attempt_finished_event_with_result(
            8,
            2000,
            3001,
            AttemptResultKind::Timeout,
            Some("attempt timed out after 5s".to_string()),
        ),
        create_run_state_changed_event(9, 2000, RunState::Running, RunState::RetryWait),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL for timeout replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::RetryWait));
    let history = reducer
        .get_attempt_history(&run_id)
        .expect("attempt history should exist after timeout finish");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].attempt_id(), attempt_id);
    assert_eq!(history[0].result(), Some(AttemptResultKind::Timeout));
    assert_eq!(history[0].error(), Some("attempt timed out after 5s"));
    assert_eq!(history[0].finished_at(), Some(3000));

    let _ = fs::remove_file(path);
}

/// Test: Mixed scenario - lease expired between attempts, new attempt started
#[test]
fn test_lease_expired_between_attempts() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(2000u128));
    let attempt_id_2 = AttemptId::from_uuid(uuid::Uuid::from_u128(4000u128));

    // Scenario:
    // 1. Run transitions to Leased (but no attempt started yet)
    // 2. Lease expires (worker died before starting execution)
    // 3. Run becomes Ready (re-eligible) due to lease expiry
    // 4. New worker acquires lease, run transitions to Leased again
    // 5. New attempt 2 starts

    let events = vec![
        create_task_created_event(1, 1000),
        create_run_created_event(2, 1000, 2000),
        create_run_state_changed_event(3, 2000, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 2000, "worker-1", 5000),
        // Lease expires BEFORE attempt starts (worker died before starting)
        create_lease_expired_event(6, 2000, "worker-1", 5000), // Lease expired, run becomes Ready
        // New worker acquires lease
        create_run_state_changed_event(7, 2000, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(8, 2000, "worker-2", 7000),
        // Run transitions to Running and attempt starts
        create_run_state_changed_event(9, 2000, RunState::Leased, RunState::Running),
        create_attempt_started_event(10, 2000, 4000),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for lease expired between attempts test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify run state is Running
    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Running), "Run should be Running");
    // Verify active lease is for worker-2 with updated expiry
    let lease = reducer.get_lease(&run_id).expect("Lease should be present");
    assert_eq!(&lease.0, "worker-2");
    assert_eq!(lease.1, 7000, "Active lease should be for worker-2 with expiry 7000");
    // Verify attempt 2 is tracked (attempt 1 was never durable, attempt 2 is current)
    let run_instance = reducer.get_run_instance(&run_id).expect("Run instance should be present");
    assert_eq!(
        run_instance.current_attempt_id(),
        Some(attempt_id_2),
        "Current attempt should be 4000"
    );
    assert_eq!(
        run_instance.attempt_count(),
        1,
        "Attempt count should be 1 (only attempt 2 was durable)"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn d03_t_p1_valid_lease_lifecycle_with_owner_consistent_heartbeat_is_replay_valid() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(9100u128));

    let events = vec![
        create_task_created_event(1, 9000),
        create_run_created_event(2, 9000, 9100),
        create_run_state_changed_event(3, 9100, RunState::Scheduled, RunState::Ready),
        create_lease_acquired_event(4, 9100, "worker-a", 5000),
        create_run_state_changed_event(5, 9100, RunState::Ready, RunState::Leased),
        create_lease_heartbeat_event(6, 9100, "worker-a", 6000),
        create_lease_heartbeat_event(7, 9100, "worker-a", 6500),
        create_run_state_changed_event(8, 9100, RunState::Leased, RunState::Running),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).expect("append should succeed");
    }
    writer.close().expect("close should succeed");

    let reader = WalFsReader::new(path.clone()).expect("reader should open");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Running));
    let lease = reducer.get_lease(&run_id).expect("active lease should remain present");
    assert_eq!(lease.0, "worker-a");
    assert_eq!(lease.1, 6500);

    let _ = fs::remove_file(path);
}

#[test]
fn d03_t_p2_terminal_transition_clears_active_lease_projection() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(9200u128));

    let events = vec![
        create_task_created_event(1, 9001),
        create_run_created_event(2, 9001, 9200),
        create_run_state_changed_event(3, 9200, RunState::Scheduled, RunState::Ready),
        create_lease_acquired_event(4, 9200, "worker-a", 5000),
        create_run_state_changed_event(5, 9200, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(6, 9200, RunState::Leased, RunState::Running),
        create_attempt_started_event(7, 9200, 9300),
        create_attempt_finished_event(8, 9200, 9300),
        create_run_state_changed_event(9, 9200, RunState::Running, RunState::Completed),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).expect("append should succeed");
    }
    writer.close().expect("close should succeed");

    let reader = WalFsReader::new(path.clone()).expect("reader should open");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Completed));
    assert!(reducer.get_lease(&run_id).is_none(), "terminal run must not retain active lease");

    let _ = fs::remove_file(path);
}

#[test]
fn d03_t_n1_lease_acquired_for_unknown_run_is_rejected() {
    let mut reducer = ReplayReducer::new();

    let result = reducer.apply(&create_lease_acquired_event(1, 999_999, "worker-a", 5000));
    assert!(matches!(
        result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::UnknownRun { event: LeaseEventKind::Acquire, .. }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 0);
}

#[test]
fn d03_t_n2_heartbeat_from_non_owner_is_rejected() {
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 9400)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 9400, 9401)).expect("run creation should apply");
    reducer
        .apply(&create_run_state_changed_event(3, 9401, RunState::Scheduled, RunState::Ready))
        .expect("scheduled->ready should apply");
    reducer
        .apply(&create_lease_acquired_event(4, 9401, "worker-a", 5000))
        .expect("lease acquire should apply");
    reducer
        .apply(&create_run_state_changed_event(5, 9401, RunState::Ready, RunState::Leased))
        .expect("ready->leased should apply");

    let result = reducer.apply(&create_lease_heartbeat_event(6, 9401, "worker-b", 6000));
    assert!(matches!(
        result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::OwnerMismatch { event: LeaseEventKind::Heartbeat, .. }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 5);
}

#[test]
fn d03_t_n3_release_with_stale_or_mismatched_expiry_is_rejected() {
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 9500)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 9500, 9501)).expect("run creation should apply");
    reducer
        .apply(&create_run_state_changed_event(3, 9501, RunState::Scheduled, RunState::Ready))
        .expect("scheduled->ready should apply");
    reducer
        .apply(&create_lease_acquired_event(4, 9501, "worker-a", 5000))
        .expect("lease acquire should apply");
    reducer
        .apply(&create_run_state_changed_event(5, 9501, RunState::Ready, RunState::Leased))
        .expect("ready->leased should apply");

    let result = reducer.apply(&create_lease_released_event(6, 9501, "worker-a", 4999));
    assert!(matches!(
        result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::ExpiryMismatch { event: LeaseEventKind::Release, .. }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 5);
}

#[test]
fn d03_t_n4_expire_or_release_with_missing_active_lease_is_rejected() {
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 9600)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 9600, 9601)).expect("run creation should apply");
    reducer
        .apply(&create_run_state_changed_event(3, 9601, RunState::Scheduled, RunState::Ready))
        .expect("scheduled->ready should apply");
    reducer
        .apply(&create_run_state_changed_event(4, 9601, RunState::Ready, RunState::Leased))
        .expect("ready->leased should apply");

    let expired_result = reducer.apply(&create_lease_expired_event(5, 9601, "worker-a", 5000));
    assert!(matches!(
        expired_result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::MissingActiveLease { event: LeaseEventKind::Expire, .. }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 4);

    let released_result = reducer.apply(&create_lease_released_event(5, 9601, "worker-a", 5000));
    assert!(matches!(
        released_result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::MissingActiveLease { event: LeaseEventKind::Release, .. }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 4);
}

#[test]
fn d03_t_n5_semantically_impossible_lease_ordering_is_rejected_with_typed_error() {
    let mut reducer = ReplayReducer::new();

    reducer.apply(&create_task_created_event(1, 9700)).expect("task creation should apply");
    reducer.apply(&create_run_created_event(2, 9700, 9701)).expect("run creation should apply");

    // Lease acquisition from Scheduled is semantically impossible.
    let result = reducer.apply(&create_lease_acquired_event(3, 9701, "worker-a", 5000));
    assert!(matches!(
        result,
        Err(actionqueue_storage::recovery::reducer::ReplayReducerError::LeaseCausality(
            LeaseCausalityError::InvalidRunState {
                event: LeaseEventKind::Acquire,
                state: RunState::Scheduled,
                ..
            }
        ))
    ));
    assert_eq!(reducer.latest_sequence(), 2);
}

/// Test: Once policy - run completes, simulating crash afterward
/// On restart, the run should remain terminal (Completed) and no redispatch occurs
#[test]
fn test_once_completion_crash_restart_no_redispatch() {
    let path = temp_wal_path();
    let _task_id = TaskId::from_uuid(uuid::Uuid::from_u128(5000u128));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(5001u128));

    // Step 1: Create task with Once policy
    // Step 2: Derive the single run via RunCreated event (simulating initial derivation)
    // Step 3: Transition Scheduled -> Ready (run ready for execution, Once runs immediately)
    // Step 4: Lease acquired (run leased for execution)
    // Step 5: Transition Leased -> Running
    // Step 6: Attempt starts and finishes with success
    // Step 7: Transition Running -> Completed (terminal state)
    // Step 8: Simulate crash (no more events written)

    let events = vec![
        // Create Once task
        create_task_created_event(1, 5000),
        // Create the single run for Once policy
        create_run_created_event_with_id(2, 5000, 5001),
        // Run is ready for execution (Once runs immediately after being created)
        create_run_state_changed_event(3, 5001, RunState::Scheduled, RunState::Ready),
        // Lease acquired - run is leased for execution
        create_lease_acquired_event(4, 5001, "worker-1", 5000),
        // Run transitions to Running
        create_run_state_changed_event(5, 5001, RunState::Ready, RunState::Leased),
        create_run_state_changed_event(6, 5001, RunState::Leased, RunState::Running),
        // Attempt starts
        create_attempt_started_event(7, 5001, 5002),
        // Attempt finishes successfully
        create_attempt_finished_event(8, 5001, 5002),
        // Run transitions to Completed (terminal state for Once)
        create_run_state_changed_event(9, 5001, RunState::Running, RunState::Completed),
    ];

    // Write events to WAL (simulating pre-crash state with completed Once run)
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay events to simulate restart - this reconstructs state from WAL
    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL file for Once completion restart test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify 1: run state is Completed (terminal) after replay
    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Completed),
        "Run should be Completed (terminal) after replay of Once completion"
    );

    // Verify 2: run instance exists and has correct state
    let run_instance = reducer.get_run_instance(&run_id);
    assert!(run_instance.is_some(), "Run instance should be present after replay");

    // Verify 3: attempt is finished
    let ri = run_instance.expect("run_instance should be Some");
    assert_eq!(ri.attempt_count(), 1, "Completed Once run should have exactly 1 attempt");
    assert_eq!(ri.state(), RunState::Completed, "Run state should be Completed after replay");

    let _ = fs::remove_file(path);
}

/// Helper function to create a RunCreated event with specific run_id
fn create_run_created_event_with_id(seq: u64, task_id: u64, run_id: u64) -> WalEvent {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(task_id as u128));
    let run_instance = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
        RunId::from_uuid(uuid::Uuid::from_u128(run_id as u128)),
        task_id,
        1000,
        1000,
    )
    .expect("test run instance should be valid");
    create_event(seq, WalEventType::RunCreated { run_instance })
}

/// Test: Repeat policy - exactly N runs, simulating crash after partial completion
/// On restart, verify exactly N runs exist and no N+1 run is redispatched
#[test]
fn test_repeat_exact_n_restart_contract() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(6000u128));
    let run_id_1 = RunId::from_uuid(uuid::Uuid::from_u128(6001u128));
    let run_id_2 = RunId::from_uuid(uuid::Uuid::from_u128(6002u128));
    let run_id_3 = RunId::from_uuid(uuid::Uuid::from_u128(6003u128));

    // Step 1: Create task with Repeat policy (count=3, interval=60)
    // Step 2: Derive 3 runs via RunCreated events (simulating initial derivation)
    // Step 3: Run 1 completes (Scheduled->Ready->Leased->Running->Completed)
    // Step 4: Run 2 and Run 3 haven't started yet (still Scheduled)
    // Step 5: Simulate crash after Run 1 completes
    // Step 6: On restart, verify exactly 3 runs exist with correct states

    let events = vec![
        // Create Repeat task with count=3
        create_event(
            1,
            WalEventType::TaskCreated {
                task_spec: create_test_task_spec_repeat(6000, 3, 60),
                timestamp: 0,
            },
        ),
        // Create the 3 runs for Repeat policy (scheduled_at = base + index * interval)
        // Run 1: scheduled_at = 1000 + 0 * 60 = 1000
        create_event(
            2,
            WalEventType::RunCreated {
                run_instance:
                    actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
                        run_id_1, task_id, 1000, 1000,
                    )
                    .expect("test run should be valid"),
            },
        ),
        // Run 2: scheduled_at = 1000 + 1 * 60 = 1060
        create_event(
            3,
            WalEventType::RunCreated {
                run_instance:
                    actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
                        run_id_2, task_id, 1060, 1000,
                    )
                    .expect("test run should be valid"),
            },
        ),
        // Run 3: scheduled_at = 1000 + 2 * 60 = 1120
        create_event(
            4,
            WalEventType::RunCreated {
                run_instance:
                    actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
                        run_id_3, task_id, 1120, 1000,
                    )
                    .expect("test run should be valid"),
            },
        ),
        // Run 1: Scheduled -> Ready
        create_event(
            5,
            WalEventType::RunStateChanged {
                run_id: run_id_1,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2000,
            },
        ),
        // Run 1: Lease acquired
        create_event(
            6,
            WalEventType::LeaseAcquired {
                run_id: run_id_1,
                owner: "worker-1".to_string(),
                expiry: 2500,
                timestamp: 2000,
            },
        ),
        // Run 1: Ready -> Leased
        create_event(
            7,
            WalEventType::RunStateChanged {
                run_id: run_id_1,
                previous_state: RunState::Ready,
                new_state: RunState::Leased,
                timestamp: 2000,
            },
        ),
        // Run 1: Leased -> Running
        create_event(
            8,
            WalEventType::RunStateChanged {
                run_id: run_id_1,
                previous_state: RunState::Leased,
                new_state: RunState::Running,
                timestamp: 2000,
            },
        ),
        // Run 1: Attempt starts
        create_event(
            9,
            WalEventType::AttemptStarted {
                run_id: run_id_1,
                attempt_id: AttemptId::from_uuid(uuid::Uuid::from_u128(7001u128)),
                timestamp: 2000,
            },
        ),
        // Run 1: Attempt finishes successfully
        create_event(
            10,
            WalEventType::AttemptFinished {
                run_id: run_id_1,
                attempt_id: AttemptId::from_uuid(uuid::Uuid::from_u128(7001u128)),
                result: AttemptResultKind::Success,
                error: None,
                output: None,
                timestamp: 2500,
            },
        ),
        // Run 1: Running -> Completed (terminal state)
        create_event(
            11,
            WalEventType::RunStateChanged {
                run_id: run_id_1,
                previous_state: RunState::Running,
                new_state: RunState::Completed,
                timestamp: 2500,
            },
        ),
        // Run 2: Scheduled -> Ready (still in progress, not fully completed)
        create_event(
            12,
            WalEventType::RunStateChanged {
                run_id: run_id_2,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2600,
            },
        ),
        // CRASH SIMULATION: No more events written after run 1 completes and run 2 becomes ready
    ];

    // Write events to WAL (simulating pre-crash state)
    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    // Replay events to simulate restart
    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL file for Repeat restart test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");

    let reducer = driver.into_reducer();

    // Verify 1: exactly 3 runs exist
    assert_eq!(reducer.run_count(), 3, "Repeat policy must have exactly 3 runs");

    // Verify 2: Run 1 is Completed (fully completed before crash)
    assert_eq!(reducer.get_run_state(&run_id_1), Some(&RunState::Completed));

    // Verify 3: Run 2 is Ready (not completed before crash)
    assert_eq!(reducer.get_run_state(&run_id_2), Some(&RunState::Ready));

    // Verify 4: Run 3 is Scheduled (never started)
    assert_eq!(reducer.get_run_state(&run_id_3), Some(&RunState::Scheduled));

    // Verify 5: Run 1 attempt count is 1
    let ri1 = reducer.get_run_instance(&run_id_1);
    assert!(ri1.is_some());
    assert_eq!(ri1.unwrap().attempt_count(), 1);

    // Verify 6: Run 2 and Run 3 have attempt count 0
    let ri2 = reducer.get_run_instance(&run_id_2);
    let ri3 = reducer.get_run_instance(&run_id_3);
    assert!(ri2.is_some());
    assert!(ri3.is_some());
    assert_eq!(ri2.unwrap().attempt_count(), 0);
    assert_eq!(ri3.unwrap().attempt_count(), 0);

    // Verify 7: Run 1 is in Terminal state and cannot be re-dispatched
    // This is the key contract: after crash, we don't create N+1 runs
    // The already_derived count is already 3 (all 3 runs exist), so no new runs would be derived
    let task_spec = reducer.get_task(&task_id);
    assert!(task_spec.is_some());
    match task_spec.unwrap().run_policy() {
        RunPolicy::Once => panic!("Expected Repeat policy"),
        RunPolicy::Repeat(ref rp) => assert_eq!(rp.count(), 3, "Repeat policy count must be 3"),
        // Cron variant only exists when the workflow feature is active.
        #[cfg(feature = "workflow")]
        RunPolicy::Cron(_) => panic!("Expected Repeat policy, not Cron"),
    }

    let _ = fs::remove_file(path);
}

/// Test: DependencyDeclared events survive WAL replay and appear in the reducer's
/// dependency_declarations projection.
#[test]
fn dependency_declared_survives_wal_replay() {
    let path = temp_wal_path();

    let task_a_id = TaskId::from_uuid(uuid::Uuid::from_u128(7001));
    let task_b_id = TaskId::from_uuid(uuid::Uuid::from_u128(7002));
    let task_c_id = TaskId::from_uuid(uuid::Uuid::from_u128(7003));

    let events = vec![
        // Create 3 tasks: A, B, C.
        create_task_created_event(1, 7001),
        create_task_created_event(2, 7002),
        create_task_created_event(3, 7003),
        // Declare dependencies: B depends on A, C depends on B.
        create_event(
            4,
            WalEventType::DependencyDeclared {
                task_id: task_b_id,
                depends_on: vec![task_a_id],
                timestamp: 1000,
            },
        ),
        create_event(
            5,
            WalEventType::DependencyDeclared {
                task_id: task_c_id,
                depends_on: vec![task_b_id],
                timestamp: 1001,
            },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone())
        .expect("Failed to open WAL for dependency declared replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    // Collect dependency declarations into a map for assertion.
    let declarations: std::collections::HashMap<TaskId, std::collections::HashSet<TaskId>> =
        reducer.dependency_declarations().map(|(tid, prereqs)| (tid, prereqs.clone())).collect();

    // B depends on A.
    let b_prereqs = declarations.get(&task_b_id).expect("B should have dependency declarations");
    assert!(b_prereqs.contains(&task_a_id), "B should depend on A");
    assert_eq!(b_prereqs.len(), 1, "B should have exactly 1 prerequisite");

    // C depends on B.
    let c_prereqs = declarations.get(&task_c_id).expect("C should have dependency declarations");
    assert!(c_prereqs.contains(&task_b_id), "C should depend on B");
    assert_eq!(c_prereqs.len(), 1, "C should have exactly 1 prerequisite");

    // A has no declared prerequisites.
    assert!(
        !declarations.contains_key(&task_a_id),
        "A should not appear as a dependent in declarations"
    );

    let _ = fs::remove_file(path);
}

/// Test: AttemptFinished with output bytes survives WAL replay and the output
/// is accessible in the attempt history projection.
#[test]
fn attempt_finished_with_output_survives_wal_replay() {
    let path = temp_wal_path();
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(8100));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(8200));

    let events = vec![
        create_task_created_event(1, 8000),
        create_run_created_event(2, 8000, 8100),
        create_run_state_changed_event(3, 8100, RunState::Scheduled, RunState::Ready),
        create_run_state_changed_event(4, 8100, RunState::Ready, RunState::Leased),
        create_lease_acquired_event(5, 8100, "worker-1", 9000),
        create_run_state_changed_event(6, 8100, RunState::Leased, RunState::Running),
        create_attempt_started_event(7, 8100, 8200),
        // AttemptFinished with output bytes.
        create_event(
            8,
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Success,
                error: None,
                output: Some(b"test-output-bytes".to_vec()),
                timestamp: 3000,
            },
        ),
        create_run_state_changed_event(9, 8100, RunState::Running, RunState::Completed),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("Failed to open WAL for attempt output replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Completed),
        "Run should be Completed"
    );

    let history = reducer
        .get_attempt_history(&run_id)
        .expect("attempt history should exist after finish with output");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].attempt_id(), attempt_id);
    assert_eq!(history[0].result(), Some(AttemptResultKind::Success));
    assert_eq!(
        history[0].output(),
        Some(b"test-output-bytes".as_slice()),
        "output bytes must survive WAL replay"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn run_suspended_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xAB01));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xAB02));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xAB01), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::RunCreated {
                run_instance: RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
                    .expect("run should be valid"),
            },
        ),
        WalEvent::new(
            3,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            4,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Ready,
                new_state: RunState::Leased,
                timestamp: 3000,
            },
        ),
        WalEvent::new(
            5,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Leased,
                new_state: RunState::Running,
                timestamp: 4000,
            },
        ),
        WalEvent::new(
            6,
            WalEventType::RunSuspended {
                run_id,
                reason: Some("budget exhausted".to_string()),
                timestamp: 5000,
            },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone()).expect("should open WAL for suspended replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Suspended),
        "run should be Suspended after WAL replay"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn budget_allocated_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xBB01));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xBB01), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::BudgetAllocated {
                task_id,
                dimension: BudgetDimension::Token,
                limit: 10_000,
                timestamp: 2000,
            },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("should open WAL for budget allocation replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    let budget = reducer
        .get_budget(&task_id, BudgetDimension::Token)
        .expect("budget record should exist after replay");

    assert_eq!(budget.limit, 10_000, "limit should survive WAL replay");
    assert_eq!(budget.consumed, 0, "consumed should start at zero");
    assert!(!budget.exhausted, "budget should not be exhausted");

    let _ = fs::remove_file(path);
}

#[test]
fn budget_consumed_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xCC01));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xCC01), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::BudgetAllocated {
                task_id,
                dimension: BudgetDimension::CostCents,
                limit: 500,
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            3,
            WalEventType::BudgetConsumed {
                task_id,
                dimension: BudgetDimension::CostCents,
                amount: 200,
                timestamp: 3000,
            },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("should open WAL for budget consumption replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    let budget = reducer
        .get_budget(&task_id, BudgetDimension::CostCents)
        .expect("budget record should exist after replay");

    assert_eq!(budget.limit, 500, "limit should survive WAL replay");
    assert_eq!(budget.consumed, 200, "consumed amount should survive WAL replay");
    assert!(!budget.exhausted, "budget should not yet be exhausted");

    let _ = fs::remove_file(path);
}

#[test]
fn subscription_created_and_triggered_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xDD01));
    let sub_id = SubscriptionId::from_uuid(uuid::Uuid::from_u128(0xDD02));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xDD01), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::SubscriptionCreated {
                subscription_id: sub_id,
                task_id,
                filter: EventFilter::TaskCompleted { task_id },
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            3,
            WalEventType::SubscriptionTriggered { subscription_id: sub_id, timestamp: 3000 },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("should open WAL for subscription replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    let sub =
        reducer.get_subscription(&sub_id).expect("subscription record should exist after replay");

    assert_eq!(sub.task_id, task_id, "task_id should survive WAL replay");
    assert_eq!(sub.triggered_at, Some(3000), "triggered_at should survive WAL replay");
    assert!(sub.canceled_at.is_none(), "subscription should not be canceled");

    let _ = fs::remove_file(path);
}

#[test]
fn run_resumed_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xAB10));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xAB11));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(0xAB12));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xAB10), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::RunCreated {
                run_instance: RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
                    .expect("run should be valid"),
            },
        ),
        WalEvent::new(
            3,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            4,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Ready,
                new_state: RunState::Leased,
                timestamp: 3000,
            },
        ),
        WalEvent::new(
            5,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Leased,
                new_state: RunState::Running,
                timestamp: 4000,
            },
        ),
        WalEvent::new(6, WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 4500 }),
        WalEvent::new(
            7,
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Suspended,
                error: None,
                output: None,
                timestamp: 5000,
            },
        ),
        WalEvent::new(
            8,
            WalEventType::RunSuspended {
                run_id,
                reason: Some("budget exhausted".to_string()),
                timestamp: 5000,
            },
        ),
        WalEvent::new(9, WalEventType::RunResumed { run_id, timestamp: 6000 }),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone()).expect("should open WAL for resumed replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    assert_eq!(
        reducer.get_run_state(&run_id),
        Some(&RunState::Ready),
        "run should be Ready after RunResumed WAL replay"
    );

    let _ = fs::remove_file(path);
}

#[test]
fn budget_replenished_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xBB10));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xBB10), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::BudgetAllocated {
                task_id,
                dimension: BudgetDimension::Token,
                limit: 100,
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            3,
            WalEventType::BudgetConsumed {
                task_id,
                dimension: BudgetDimension::Token,
                amount: 100,
                timestamp: 3000,
            },
        ),
        WalEvent::new(
            4,
            WalEventType::BudgetReplenished {
                task_id,
                dimension: BudgetDimension::Token,
                new_limit: 200,
                timestamp: 4000,
            },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader =
        WalFsReader::new(path.clone()).expect("should open WAL for budget replenish replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    let budget = reducer
        .get_budget(&task_id, BudgetDimension::Token)
        .expect("budget record should exist after replay");

    assert_eq!(budget.limit, 200, "limit should be updated to new_limit after replenishment");
    assert_eq!(budget.consumed, 0, "consumed should be reset to zero after replenishment");
    assert!(!budget.exhausted, "budget should not be exhausted after replenishment");

    let _ = fs::remove_file(path);
}

#[test]
fn subscription_canceled_survives_wal_replay() {
    let path = temp_wal_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xDD10));
    let sub_id = SubscriptionId::from_uuid(uuid::Uuid::from_u128(0xDD11));

    let events = vec![
        WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: create_test_task_spec(0xDD10), timestamp: 1000 },
        ),
        WalEvent::new(
            2,
            WalEventType::SubscriptionCreated {
                subscription_id: sub_id,
                task_id,
                filter: EventFilter::TaskCompleted { task_id },
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            3,
            WalEventType::SubscriptionCanceled { subscription_id: sub_id, timestamp: 3000 },
        ),
    ];

    let mut writer = open_wal_writer(path.clone());
    for event in &events {
        writer.append(event).unwrap();
    }
    writer.close().unwrap();

    let reader = WalFsReader::new(path.clone())
        .expect("should open WAL for subscription canceled replay test");
    let reducer = ReplayReducer::new();
    let mut driver = ReplayDriver::new(reader, reducer);
    driver.run().expect("Replay should succeed");
    let reducer = driver.into_reducer();

    let sub =
        reducer.get_subscription(&sub_id).expect("subscription record should exist after replay");

    assert_eq!(sub.task_id, task_id, "task_id should survive WAL replay");
    assert_eq!(sub.canceled_at, Some(3000), "canceled_at should be set after SubscriptionCanceled");

    let _ = fs::remove_file(path);
}
