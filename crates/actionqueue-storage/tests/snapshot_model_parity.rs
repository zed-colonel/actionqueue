//! D-06 snapshot model parity tests.
//!
//! These tests harden the explicit snapshot mapping boundary so snapshot payloads
//! remain strict projections of canonical core semantics.

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
use actionqueue_storage::snapshot::loader::{
    SnapshotFsLoader, SnapshotLoader, SnapshotLoaderError,
};
use actionqueue_storage::snapshot::mapping::{
    map_core_run_to_snapshot, map_snapshot_run_to_core, validate_snapshot, SnapshotMappingError,
    SNAPSHOT_SCHEMA_VERSION,
};
use actionqueue_storage::snapshot::model::{
    Snapshot, SnapshotAttemptHistoryEntry, SnapshotBudget, SnapshotDependencyDeclaration,
    SnapshotEngineControl, SnapshotMetadata, SnapshotRunStateHistoryEntry, SnapshotSubscription,
    SnapshotTask,
};

/// Test helper: builds a SnapshotTask with standard test defaults.
/// When new fields are added to SnapshotTask, only this function needs updating.
fn test_snapshot_task(task_spec: TaskSpec) -> SnapshotTask {
    SnapshotTask { task_spec, created_at: 0, updated_at: None, canceled_at: None }
}

/// Test helper: builds a minimal in-progress SnapshotAttemptHistoryEntry.
fn test_attempt_entry_in_progress(
    attempt_id: actionqueue_core::ids::AttemptId,
    started_at: u64,
) -> SnapshotAttemptHistoryEntry {
    SnapshotAttemptHistoryEntry {
        disposition: None,
        accepted_start: None,
        finish_origin: Default::default(),
        attempt_id,
        started_at,
        finished_at: None,
        result: None,
        error: None,
        output: None,
    }
}

static TEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn temp_snapshot_path() -> PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = dir.join(format!(
        "actionqueue_snapshot_model_parity_test_{}_{}.tmp",
        std::process::id(),
        count
    ));
    let _ = fs::remove_file(&path);
    path
}

fn task_spec_with_id(task_id: TaskId) -> TaskSpec {
    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(b"d06-parity".to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

fn run_with_state(task_id: TaskId, run_id_raw: u128, state: RunState) -> RunInstance {
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(run_id_raw));
    let mut run = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000)
        .expect("run should build");

    match state {
        RunState::Scheduled => {}
        RunState::Ready => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass")
        }
        RunState::Leased => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
        }
        RunState::Running => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
            run.transition_to(RunState::Running).expect("leased->running should pass");
        }
        RunState::RetryWait => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
            run.transition_to(RunState::Running).expect("leased->running should pass");
            run.transition_to(RunState::RetryWait).expect("running->retrywait should pass");
        }
        RunState::Completed => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
            run.transition_to(RunState::Running).expect("leased->running should pass");
            run.transition_to(RunState::Completed).expect("running->completed should pass");
        }
        RunState::Failed => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
            run.transition_to(RunState::Running).expect("leased->running should pass");
            run.transition_to(RunState::Failed).expect("running->failed should pass");
        }
        RunState::Canceled => {
            run.transition_to(RunState::Canceled).expect("scheduled->canceled should pass")
        }
        RunState::Suspended | RunState::Awaiting => {
            run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
            run.transition_to(RunState::Leased).expect("ready->leased should pass");
            run.transition_to(RunState::Running).expect("leased->running should pass");
            run.transition_to(state).expect("running->waiting should pass");
        }
    }

    run
}

fn run_with_active_attempt(task_id: TaskId, run_id_raw: u128, attempt_id_raw: u128) -> RunInstance {
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(attempt_id_raw));
    let mut run = run_with_state(task_id, run_id_raw, RunState::Running);
    run.start_attempt(attempt_id).expect("attempt start should pass for running run");
    run
}

fn snapshot_attempt_history_for_run(run: &RunInstance) -> Vec<SnapshotAttemptHistoryEntry> {
    if let Some(attempt_id) = run.current_attempt_id() {
        return vec![test_attempt_entry_in_progress(
            attempt_id,
            run.created_at().saturating_add(1),
        )];
    }

    Vec::new()
}

fn snapshot_state_history_for_run(run: &RunInstance) -> Vec<SnapshotRunStateHistoryEntry> {
    let created_at = run.created_at();
    let mut entries = vec![SnapshotRunStateHistoryEntry {
        from: None,
        to: RunState::Scheduled,
        timestamp: created_at,
    }];

    let mut next_timestamp = created_at.saturating_add(1);
    let mut push_transition = |from: RunState, to: RunState| {
        entries.push(SnapshotRunStateHistoryEntry {
            from: Some(from),
            to,
            timestamp: next_timestamp,
        });
        next_timestamp = next_timestamp.saturating_add(1);
    };

    match run.state() {
        RunState::Scheduled => {}
        RunState::Ready => {
            push_transition(RunState::Scheduled, RunState::Ready);
        }
        RunState::Leased => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
        }
        RunState::Running => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
            push_transition(RunState::Leased, RunState::Running);
        }
        RunState::RetryWait => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
            push_transition(RunState::Leased, RunState::Running);
            push_transition(RunState::Running, RunState::RetryWait);
        }
        RunState::Completed => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
            push_transition(RunState::Leased, RunState::Running);
            push_transition(RunState::Running, RunState::Completed);
        }
        RunState::Failed => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
            push_transition(RunState::Leased, RunState::Running);
            push_transition(RunState::Running, RunState::Failed);
        }
        RunState::Canceled => {
            push_transition(RunState::Scheduled, RunState::Canceled);
        }
        RunState::Suspended | RunState::Awaiting => {
            push_transition(RunState::Scheduled, RunState::Ready);
            push_transition(RunState::Ready, RunState::Leased);
            push_transition(RunState::Leased, RunState::Running);
            push_transition(RunState::Running, run.state());
        }
    }

    entries
}

fn write_framed_snapshot(path: &std::path::Path, _version: u32, snapshot: &Snapshot) {
    let envelope = serde_json::json!({
        "store_id": uuid::Uuid::nil(), "snapshot_schema": 9, "projection_version": 9,
        "wal_sequence": snapshot.metadata.wal_sequence,
        "digest": {"algorithm":"sha256", "version":5,"hex":"unused for invalid schema"},
        "reserved": {"waits":[],"checkpoints":[],"resume_assignments":[],"causal_control":[]},
        "projection":snapshot
    });
    if snapshot.metadata.schema_version == SNAPSHOT_SCHEMA_VERSION {
        use actionqueue_storage::snapshot::writer::{SnapshotFsWriter, SnapshotWriter};
        let mut writer = SnapshotFsWriter::new_raw_for_test(path.to_path_buf()).unwrap();
        writer.write(snapshot).unwrap();
        writer.close().unwrap();
    } else {
        let payload = serde_json::to_vec(&envelope).unwrap();
        let mut bytes = b"AQCONT1S".to_vec();
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);
        fs::write(path, bytes).unwrap();
    }
}

#[test]
fn d06_t_p1_snapshot_payload_roundtrips_core_run_instance_without_semantic_loss() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA001));

    let runs = vec![
        run_with_state(task_id, 0xB001, RunState::Scheduled),
        run_with_state(task_id, 0xB002, RunState::Ready),
        run_with_state(task_id, 0xB003, RunState::Leased),
        run_with_state(task_id, 0xB004, RunState::Running),
        run_with_state(task_id, 0xB005, RunState::RetryWait),
        run_with_state(task_id, 0xB006, RunState::Completed),
        run_with_state(task_id, 0xB007, RunState::Failed),
        run_with_state(task_id, 0xB008, RunState::Canceled),
        run_with_active_attempt(task_id, 0xB009, 0xC001),
    ];

    for run in runs {
        let mut snapshot_run = map_core_run_to_snapshot(run.clone());
        snapshot_run.state_history = snapshot_state_history_for_run(&run);
        snapshot_run.attempts = snapshot_attempt_history_for_run(&run);
        let roundtripped = map_snapshot_run_to_core(&snapshot_run)
            .expect("snapshot->core roundtrip should pass for canonical run payload");
        assert_eq!(roundtripped, run, "roundtrip must preserve full run semantics");
    }
}

#[test]
fn d06_t_n1_mapping_boundary_rejects_duplicate_run_ids() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA100));
    let duplicate_run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB100));

    let run_a = RunInstance::new_scheduled_with_id(duplicate_run_id, task_id, 1_000, 1_000)
        .expect("run should build");
    let run_b = RunInstance::new_scheduled_with_id(duplicate_run_id, task_id, 2_000, 2_000)
        .expect("run should build");

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 1,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 10,
            task_count: 1,
            run_count: 2,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: vec![
            {
                let mut snapshot_run = map_core_run_to_snapshot(run_a);
                snapshot_run.state_history = vec![SnapshotRunStateHistoryEntry {
                    from: None,
                    to: RunState::Scheduled,
                    timestamp: 1_000,
                }];
                snapshot_run
            },
            {
                let mut snapshot_run = map_core_run_to_snapshot(run_b);
                snapshot_run.state_history = vec![SnapshotRunStateHistoryEntry {
                    from: None,
                    to: RunState::Scheduled,
                    timestamp: 2_000,
                }];
                snapshot_run
            },
        ],
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

    let result = validate_snapshot(&snapshot);
    assert!(matches!(
        result,
        Err(SnapshotMappingError::DuplicateRunId { run_id }) if run_id == duplicate_run_id
    ));
}

#[test]
fn d06_t_n2_loader_rejects_snapshot_payload_that_fails_mapping_invariants() {
    use actionqueue_storage::{
        recovery::reducer::ReplayReducer,
        snapshot::build::build_snapshot_from_projection,
        wal::event::{WalEvent, WalEventType},
    };
    let path = temp_snapshot_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA200));
    let mut projection = ReplayReducer::new();
    projection
        .apply(&WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: task_spec_with_id(task_id), timestamp: 1000 },
        ))
        .unwrap();
    projection
        .apply(&WalEvent::new(
            2,
            WalEventType::RunCreated {
                run_instance: run_with_state(task_id, 0xB200, RunState::Scheduled),
            },
        ))
        .unwrap();
    let snapshot = build_snapshot_from_projection(&projection, 1000).unwrap();
    let mut snapshot_json = serde_json::to_value(snapshot).unwrap();
    // An active attempt outside Running is invalid, independently of scheduling.
    let run_json = &mut snapshot_json["runs"][0]["run_instance"];
    run_json["current_attempt_id"] = serde_json::json!(AttemptId::new());
    let error = serde_json::from_value::<RunInstance>(run_json.clone()).unwrap_err();
    assert!(error.to_string().contains("active attempt_id is only valid in Running state"));
    let envelope = serde_json::json!({
        "store_id": uuid::Uuid::nil(), "snapshot_schema": 9, "projection_version": 9,
        "wal_sequence": 2,
        "digest": {"algorithm":"sha256", "version":5,"hex":"domain validation must fail first"},
        "reserved": {"waits":[],"checkpoints":[],"resume_assignments":[],"causal_control":[]},
        "projection": snapshot_json
    });
    let payload = serde_json::to_vec(&envelope).unwrap();
    let mut bytes = b"AQCONT1S".to_vec();
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    fs::write(&path, &bytes).unwrap();
    let error = SnapshotFsLoader::new(path.clone()).load().unwrap_err();
    assert!(
        matches!(error, SnapshotLoaderError::DecodeError(ref message)
        if message.contains("active attempt_id is only valid in Running state")),
        "{error}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    fs::remove_file(path).unwrap();
}

#[test]
fn d06_t_n3_schema_migration_guard_rejects_unknown_schema_version() {
    let path = temp_snapshot_path();
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA300));
    let run = run_with_state(task_id, 0xB300, RunState::Scheduled);

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 3,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION + 1,
            wal_sequence: 30,
            task_count: 1,
            run_count: 1,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: vec![{
            let mut snapshot_run = map_core_run_to_snapshot(run);
            snapshot_run.state_history = vec![SnapshotRunStateHistoryEntry {
                from: None,
                to: RunState::Scheduled,
                timestamp: 1_000,
            }];
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

    write_framed_snapshot(&path, 4, &snapshot);

    let mut loader = SnapshotFsLoader::new(path.clone());
    let load_result = loader.load();

    assert!(matches!(
        load_result,
        Err(SnapshotLoaderError::MappingError(SnapshotMappingError::UnsupportedSchemaVersion {
            expected,
            found,
        })) if expected == SNAPSHOT_SCHEMA_VERSION && found == SNAPSHOT_SCHEMA_VERSION + 1
    ));

    let _ = fs::remove_file(path);
}

#[test]
fn d06_t_n4_run_state_parity_guard_rejects_terminal_attempt_lineage_divergence() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA400));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB400));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(0xC400));

    // This payload is intentionally invalid: Completed run cannot carry active attempt lineage.
    // The RunInstance serde deserializer now rejects this at the deserialization boundary,
    // providing defense-in-depth before the snapshot mapping layer can see it.
    let result: Result<RunInstance, _> = serde_json::from_value(serde_json::json!({
        "id": run_id,
        "task_id": task_id,
        "state": "Completed",
        "current_attempt_id": attempt_id,
        "attempt_count": 1,
        "created_at": 1_000,
        "scheduled_at": 1_000,
        "effective_priority": 0
    }));

    assert!(
        result.is_err(),
        "deserialization must reject terminal state with active attempt lineage"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("attempt_id"),
        "error message should reference attempt_id constraint, got: {err_msg}"
    );
}

#[test]
fn d06_t_n5_mapping_rejects_missing_initial_state_history_entry() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA500));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB500));

    let run = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000)
        .expect("run should build");
    let snapshot_run = map_core_run_to_snapshot(run);

    let map_result = map_snapshot_run_to_core(&snapshot_run);

    assert!(matches!(
        map_result,
        Err(SnapshotMappingError::MissingInitialRunStateHistory { run_id: err_run_id })
            if err_run_id == run_id
    ));
}

#[test]
fn d06_t_n6_mapping_rejects_attempt_history_count_mismatch() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA600));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB600));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(0xC600));

    let mut run = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000)
        .expect("run should build");
    run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
    run.transition_to(RunState::Leased).expect("ready->leased should pass");
    run.transition_to(RunState::Running).expect("leased->running should pass");
    run.start_attempt(attempt_id).expect("attempt start should pass for running run");

    let mut snapshot_run = map_core_run_to_snapshot(run);
    snapshot_run.state_history = snapshot_state_history_for_run(&snapshot_run.run_instance);

    let map_result = map_snapshot_run_to_core(&snapshot_run);

    assert!(matches!(
        map_result,
        Err(SnapshotMappingError::InvalidAttemptHistoryCount {
            run_id: err_run_id,
            attempt_count: 1,
            history_len: 0,
        }) if err_run_id == run_id
    ));
}

#[test]
fn p6_011_t_n3_mapping_rejects_task_canceled_at_before_created_at() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA700));
    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 7,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 1,
            task_count: 1,
            run_count: 0,
        },
        tasks: vec![SnapshotTask {
            created_at: 100,
            canceled_at: Some(99),
            ..test_snapshot_task(task_spec_with_id(task_id))
        }],
        runs: Vec::new(),
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

    let result = validate_snapshot(&snapshot);
    assert!(matches!(
        result,
        Err(SnapshotMappingError::InvalidTaskCanceledAtCausality {
            task_id: err_task_id,
            created_at: 100,
            canceled_at: 99,
        }) if err_task_id == task_id
    ));
}

#[test]
fn p6_013_t_n5_mapping_rejects_engine_paused_without_paused_at() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA800));
    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 8,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 1,
            task_count: 1,
            run_count: 0,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: Vec::new(),
        engine: SnapshotEngineControl { paused: true, paused_at: None, resumed_at: None },
        dependency_declarations: Vec::new(),
        budgets: Vec::new(),
        subscriptions: Vec::new(),
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    let result = validate_snapshot(&snapshot);
    assert!(matches!(result, Err(SnapshotMappingError::InvalidEnginePausedCausality)));
}

#[test]
fn p6_013_t_n6_mapping_rejects_engine_pause_resume_ordering() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA801));
    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 9,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 1,
            task_count: 1,
            run_count: 0,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: Vec::new(),
        engine: SnapshotEngineControl {
            paused: false,
            paused_at: Some(200),
            resumed_at: Some(199),
        },
        dependency_declarations: Vec::new(),
        budgets: Vec::new(),
        subscriptions: Vec::new(),
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    let result = validate_snapshot(&snapshot);
    assert!(matches!(
        result,
        Err(SnapshotMappingError::InvalidEnginePauseResumeOrdering {
            paused_at: 200,
            resumed_at: 199,
        })
    ));
}

#[test]
fn p6_017_t_p3_snapshot_mapping_preserves_timeout_attempt_result_and_error() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xA900));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB900));
    let attempt_id = AttemptId::from_uuid(uuid::Uuid::from_u128(0xC900));

    let mut run = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000)
        .expect("run should build");
    run.transition_to(RunState::Ready).expect("scheduled->ready should pass");
    run.transition_to(RunState::Leased).expect("ready->leased should pass");
    run.transition_to(RunState::Running).expect("leased->running should pass");
    run.start_attempt(attempt_id).expect("attempt start should pass");
    run.finish_attempt(attempt_id).expect("attempt finish should pass");
    run.transition_to(RunState::RetryWait).expect("running->retrywait should pass");

    let mut snapshot_run = map_core_run_to_snapshot(run);
    snapshot_run.state_history = snapshot_state_history_for_run(&snapshot_run.run_instance);
    snapshot_run.attempts = vec![SnapshotAttemptHistoryEntry {
        disposition: None,
        accepted_start: None,
        finish_origin: Default::default(),
        attempt_id,
        started_at: 1_001,
        finished_at: Some(1_002),
        result: Some(AttemptResultKind::Timeout),
        error: Some("attempt timed out after 5s".to_string()),
        output: None,
    }];

    let roundtripped =
        map_snapshot_run_to_core(&snapshot_run).expect("snapshot->core mapping should succeed");
    assert_eq!(roundtripped.id(), run_id);
    assert_eq!(roundtripped.state(), RunState::RetryWait);

    let reducer_history =
        actionqueue_storage::snapshot::mapping::map_snapshot_attempt_history(snapshot_run.attempts);
    assert_eq!(reducer_history.len(), 1);
    assert_eq!(reducer_history[0].attempt_id(), attempt_id);
    assert_eq!(reducer_history[0].result(), Some(AttemptResultKind::Timeout));
    assert_eq!(reducer_history[0].error(), Some("attempt timed out after 5s"));
    assert_eq!(reducer_history[0].finished_at(), Some(1_002));
}

#[test]
fn dependency_declarations_survive_snapshot_roundtrip() {
    let task_a = TaskId::from_uuid(uuid::Uuid::from_u128(0xDA01));
    let task_b = TaskId::from_uuid(uuid::Uuid::from_u128(0xDA02));
    let task_c = TaskId::from_uuid(uuid::Uuid::from_u128(0xDA03));

    let path = temp_snapshot_path();

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 100,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 50,
            task_count: 3,
            run_count: 0,
        },
        tasks: vec![
            test_snapshot_task(task_spec_with_id(task_a)),
            test_snapshot_task(task_spec_with_id(task_b)),
            test_snapshot_task(task_spec_with_id(task_c)),
        ],
        runs: Vec::new(),
        engine: SnapshotEngineControl::default(),
        dependency_declarations: vec![
            SnapshotDependencyDeclaration {
                task_id: task_b,
                depends_on: vec![task_a],
                declared_at: 10,
            },
            SnapshotDependencyDeclaration {
                task_id: task_c,
                depends_on: vec![task_a, task_b],
                declared_at: 20,
            },
        ],
        budgets: Vec::new(),
        subscriptions: Vec::new(),
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    write_framed_snapshot(&path, 4, &snapshot);

    let mut loader = SnapshotFsLoader::new(path.clone());
    let loaded = loader
        .load()
        .expect("snapshot load must succeed")
        .expect("snapshot file must contain a snapshot");

    assert_eq!(loaded.dependency_declarations.len(), 2);
    assert_eq!(loaded.dependency_declarations[0].task_id, task_b);
    assert_eq!(loaded.dependency_declarations[0].depends_on, vec![task_a]);
    assert_eq!(loaded.dependency_declarations[0].declared_at, 10);
    assert_eq!(loaded.dependency_declarations[1].task_id, task_c);
    assert_eq!(loaded.dependency_declarations[1].depends_on, vec![task_a, task_b]);
    assert_eq!(loaded.dependency_declarations[1].declared_at, 20);

    let _ = fs::remove_file(path);
}

#[test]
fn budget_entries_roundtrip_through_snapshot() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xE001));
    let path = temp_snapshot_path();

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 100,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 10,
            task_count: 1,
            run_count: 0,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: Vec::new(),
        engine: SnapshotEngineControl::default(),
        dependency_declarations: Vec::new(),
        budgets: vec![SnapshotBudget {
            task_id,
            dimension: BudgetDimension::Token,
            limit: 5_000,
            consumed: 1_000,
            allocated_at: 50,
            exhausted: false,
        }],
        subscriptions: Vec::new(),
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    validate_snapshot(&snapshot).expect("snapshot should be valid");

    write_framed_snapshot(&path, 4, &snapshot);

    let mut loader = SnapshotFsLoader::new(path.clone());
    let loaded = loader
        .load()
        .expect("snapshot load must succeed")
        .expect("snapshot file must contain a snapshot");

    assert_eq!(loaded.budgets.len(), 1);
    let budget = &loaded.budgets[0];
    assert_eq!(budget.task_id, task_id);
    assert_eq!(budget.dimension, BudgetDimension::Token);
    assert_eq!(budget.limit, 5_000);
    assert_eq!(budget.consumed, 1_000);
    assert_eq!(budget.allocated_at, 50);
    assert!(!budget.exhausted);

    let _ = fs::remove_file(path);
}

#[test]
fn subscription_entries_roundtrip_through_snapshot() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xE002));
    let sub_id = SubscriptionId::from_uuid(uuid::Uuid::from_u128(0xE003));
    let path = temp_snapshot_path();

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 100,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 10,
            task_count: 1,
            run_count: 0,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: Vec::new(),
        engine: SnapshotEngineControl::default(),
        dependency_declarations: Vec::new(),
        budgets: Vec::new(),
        subscriptions: vec![SnapshotSubscription {
            matched_sequence: None,
            subscription_id: sub_id,
            task_id,
            filter: EventFilter::TaskCompleted { task_id },
            created_at: 30,
            triggered_at: Some(60),
            canceled_at: None,
        }],
        actors: Vec::new(),
        tenants: Vec::new(),
        role_assignments: Vec::new(),
        capability_grants: Vec::new(),
        ledger_entries: Vec::new(),
    };

    validate_snapshot(&snapshot).expect("snapshot should be valid");

    write_framed_snapshot(&path, 4, &snapshot);

    let mut loader = SnapshotFsLoader::new(path.clone());
    let loaded = loader
        .load()
        .expect("snapshot load must succeed")
        .expect("snapshot file must contain a snapshot");

    assert_eq!(loaded.subscriptions.len(), 1);
    let sub = &loaded.subscriptions[0];
    assert_eq!(sub.subscription_id, sub_id);
    assert_eq!(sub.task_id, task_id);
    assert_eq!(sub.created_at, 30);
    assert_eq!(sub.triggered_at, Some(60));
    assert!(sub.canceled_at.is_none());

    let _ = fs::remove_file(path);
}

#[test]
fn suspended_run_state_in_snapshot_history() {
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(0xE004));
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xE005));

    let run = run_with_state(task_id, 0xE005, RunState::Suspended);
    assert_eq!(run.state(), RunState::Suspended, "run should be in Suspended state");

    let snapshot_run = {
        let mut sr = map_core_run_to_snapshot(run.clone());
        sr.state_history = vec![
            SnapshotRunStateHistoryEntry { from: None, to: RunState::Scheduled, timestamp: 1_000 },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Scheduled),
                to: RunState::Ready,
                timestamp: 2_000,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Ready),
                to: RunState::Leased,
                timestamp: 3_000,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Leased),
                to: RunState::Running,
                timestamp: 4_000,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Running),
                to: RunState::Suspended,
                timestamp: 5_000,
            },
        ];
        sr
    };

    let snapshot = Snapshot {
        control_history: vec![],
        dispatch_sequences: Vec::new(),
        administrative_wakes: Vec::new(),
        administrative_pending: Vec::new(),
        waits: vec![],
        cancellations: vec![],
        pending_resumes: vec![],
        key_reservations: vec![],
        signals: vec![],
        last_signal_sequence: 0,
        admissions: vec![],
        version: 9,
        timestamp: 100,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: 10,
            task_count: 1,
            run_count: 1,
        },
        tasks: vec![test_snapshot_task(task_spec_with_id(task_id))],
        runs: vec![snapshot_run],
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

    validate_snapshot(&snapshot).expect("snapshot with Suspended run should be valid");

    let last_entry = snapshot.runs[0].state_history.last().expect("history must not be empty");
    assert_eq!(last_entry.to, RunState::Suspended);
    assert_eq!(last_entry.from, Some(RunState::Running));
    let _ = run_id;
}
