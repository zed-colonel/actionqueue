//! Offline capture tool for the frozen pre-`AQ-CONT-1` evidence archive.
//!
//! This example is evidence tooling, not runtime code. It drives the baseline
//! storage authority through a deterministic lifecycle scenario, copies the
//! resulting WAL v5 and snapshot schema v8 artifacts into the archive, derives
//! corruption variants, records the expected projection facts, and measures a
//! small reproducible WAL append/replay baseline.
//!
//! Usage (from the repository root):
//!
//! ```text
//! cargo run --example capture_pre_aq_cont_1_fixtures -- archive/pre-aq-cont-1
//! ```
//!
//! Target crates must never depend on this file or on the artifacts it writes.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AttemptFinishCommand, AttemptOutcome, AttemptResultKind, AttemptStartCommand, DurabilityPolicy,
    EnginePauseCommand, EngineResumeCommand, LeaseAcquireCommand, LeaseReleaseCommand,
    MutationAuthority, MutationCommand, RunCreateCommand, RunStateTransitionCommand,
    TaskCancelCommand, TaskCreateCommand,
};
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::snapshot::build::build_snapshot_from_projection;
use actionqueue_storage::snapshot::writer::{SnapshotFsWriter, SnapshotWriter};
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::writer::WalWriter;
use actionqueue_storage::wal::InstrumentedWalWriter;
use serde_json::{json, Value};

/// Lease owner recorded on every captured lease.
const LEASE_OWNER: &str = "archive-executor";

/// Fixed identifiers so the captured lineage is reproducible.
const TASK_A: &str = "0a000000-0000-4000-8000-00000000000a";
const TASK_B: &str = "0b000000-0000-4000-8000-00000000000b";
const TASK_C: &str = "0c000000-0000-4000-8000-00000000000c";
const RUN_A: &str = "1a000000-0000-4000-8000-00000000001a";
const RUN_B: &str = "1b000000-0000-4000-8000-00000000001b";
const RUN_C: &str = "1c000000-0000-4000-8000-00000000001c";
const ATTEMPT_A1: &str = "2a000000-0000-4000-8000-0000000000a1";
const ATTEMPT_A2: &str = "2a000000-0000-4000-8000-0000000000a2";

/// Number of immediate-durability appends measured for the performance baseline.
const IMMEDIATE_APPENDS: u64 = 5_000;
/// Number of deferred-durability appends measured for the performance baseline.
const DEFERRED_APPENDS: u64 = 50_000;

type Authority<W> = StorageMutationAuthority<W, ReplayReducer>;

fn main() {
    let archive_root = std::env::args().nth(1).map(PathBuf::from).unwrap_or_else(|| {
        eprintln!("usage: capture_fixtures <archive-root>");
        std::process::exit(2);
    });
    let scratch = scratch_dir("lifecycle");

    let facts = run_lifecycle_scenario(&scratch);
    let wal_dir = archive_root.join("selected-wal-fixtures");
    let snapshot_dir = archive_root.join("selected-snapshot-fixtures");
    std::fs::create_dir_all(&wal_dir).expect("wal fixture dir");
    std::fs::create_dir_all(&snapshot_dir).expect("snapshot fixture dir");

    let wal_source = scratch.join("wal").join("actionqueue.wal");
    let wal_target = wal_dir.join("lifecycle-wal-v5.wal");
    std::fs::copy(&wal_source, &wal_target).expect("copy WAL fixture");
    write_truncated_variant(&wal_target, &wal_dir.join("lifecycle-wal-v5-truncated-tail.wal"));

    let snapshot_target = snapshot_dir.join("lifecycle-snapshot-schema8.bin");
    write_snapshot(&scratch, &snapshot_target);
    write_bitflip_variant(
        &snapshot_target,
        &snapshot_dir.join("lifecycle-snapshot-schema8-crc-mismatch.bin"),
    );

    let expected = json!({
        "generator": "archive/pre-aq-cont-1/tools/capture_fixtures.rs",
        "baseline_commit": "97c9dc26c19c697dbfb204ed503e82c5f053394f",
        "wal_format_version": 5,
        "snapshot_schema_version": 8,
        "scenario": facts,
    });
    write_json(&archive_root.join("selected-wal-fixtures").join("expected.json"), &expected);

    let perf = measure_append_and_replay();
    let perf_dir = archive_root.join("performance-baseline");
    std::fs::create_dir_all(&perf_dir).expect("performance dir");
    write_json(&perf_dir.join("wal-append-replay.json"), &perf);

    let _ = std::fs::remove_dir_all(&scratch);
    println!("captured fixtures under {}", archive_root.display());
}

fn scratch_dir(label: &str) -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("aq-pre-aq-cont-1-capture-{label}-{unique}"));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn open_authority(data_dir: &Path) -> Authority<InstrumentedWalWriter<WalFsWriter>> {
    let recovery = load_projection_from_storage(data_dir).expect("bootstrap should succeed");
    StorageMutationAuthority::new(recovery.wal_writer, recovery.projection)
}

fn next_seq<W: WalWriter>(authority: &Authority<W>) -> u64 {
    authority.projection().latest_sequence() + 1
}

fn submit<W: WalWriter>(authority: &mut Authority<W>, command: MutationCommand) -> u64 {
    let sequence = match &command {
        MutationCommand::TaskCreate(c) => c.sequence(),
        MutationCommand::RunCreate(c) => c.sequence(),
        MutationCommand::RunStateTransition(c) => c.sequence(),
        MutationCommand::AttemptStart(c) => c.sequence(),
        MutationCommand::AttemptFinish(c) => c.sequence(),
        MutationCommand::LeaseAcquire(c) => c.sequence(),
        MutationCommand::LeaseRelease(c) => c.sequence(),
        MutationCommand::EnginePause(c) => c.sequence(),
        MutationCommand::EngineResume(c) => c.sequence(),
        MutationCommand::TaskCancel(c) => c.sequence(),
        other => panic!("capture tool does not submit {other:?}"),
    };
    let _ = authority
        .submit_command(command, DurabilityPolicy::Immediate)
        .expect("baseline authority should accept the scenario command");
    sequence
}

fn task_spec(id: &str, payload: &[u8], constraints: TaskConstraints) -> TaskSpec {
    TaskSpec::new(
        TaskId::from_str(id).expect("fixed task id"),
        TaskPayload::with_content_type(payload.to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        constraints,
        TaskMetadata::new(vec!["archive".to_string()], 0, Some("pre-aq-cont-1".to_string())),
    )
    .expect("task spec")
}

fn transition<W: WalWriter>(
    authority: &mut Authority<W>,
    run_id: RunId,
    from: RunState,
    to: RunState,
) -> u64 {
    let seq = next_seq(authority);
    submit(
        authority,
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            seq, run_id, from, to, seq,
        )),
    )
}

fn create_task_and_run<W: WalWriter>(
    authority: &mut Authority<W>,
    spec: TaskSpec,
    run_id: &str,
) -> RunId {
    let task_id = spec.id();
    let seq = next_seq(authority);
    submit(authority, MutationCommand::TaskCreate(TaskCreateCommand::new(seq, spec, seq)));
    let run = RunInstance::new_scheduled_with_id(
        RunId::from_str(run_id).expect("fixed run id"),
        task_id,
        seq,
        seq,
    )
    .expect("run instance");
    let run_id = run.id();
    let seq = next_seq(authority);
    submit(authority, MutationCommand::RunCreate(RunCreateCommand::new(seq, run)));
    run_id
}

fn lease_and_start_attempt<W: WalWriter>(
    authority: &mut Authority<W>,
    run_id: RunId,
    attempt: &str,
) -> (AttemptId, u64) {
    transition(authority, run_id, RunState::Ready, RunState::Leased);
    let seq = next_seq(authority);
    let expiry = seq + 60;
    submit(
        authority,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
            seq,
            run_id,
            LEASE_OWNER,
            expiry,
            seq,
        )),
    );
    transition(authority, run_id, RunState::Leased, RunState::Running);
    let attempt_id = AttemptId::from_str(attempt).expect("fixed attempt id");
    let seq = next_seq(authority);
    submit(
        authority,
        MutationCommand::AttemptStart(AttemptStartCommand::new(seq, run_id, attempt_id, seq)),
    );
    (attempt_id, expiry)
}

fn release_lease<W: WalWriter>(authority: &mut Authority<W>, run_id: RunId, expiry: u64) {
    let seq = next_seq(authority);
    submit(
        authority,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
            seq,
            run_id,
            LEASE_OWNER,
            expiry,
            seq,
        )),
    );
}

fn finish_attempt<W: WalWriter>(
    authority: &mut Authority<W>,
    run_id: RunId,
    attempt_id: AttemptId,
    outcome: AttemptOutcome,
) {
    let seq = next_seq(authority);
    submit(
        authority,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq, run_id, attempt_id, outcome, seq,
        )),
    );
}

/// Drives the deterministic lifecycle scenario and returns the expected facts.
fn run_lifecycle_scenario(data_dir: &Path) -> Value {
    let mut authority = open_authority(data_dir);

    // Task A: retryable failure, RetryWait, second attempt succeeds with output.
    let constraints_a = TaskConstraints::new(3, Some(30), None).expect("constraints");
    let run_a =
        create_task_and_run(&mut authority, task_spec(TASK_A, b"task-a", constraints_a), RUN_A);
    transition(&mut authority, run_a, RunState::Scheduled, RunState::Ready);
    let (attempt_a1, lease_a1) = lease_and_start_attempt(&mut authority, run_a, ATTEMPT_A1);
    let failure = AttemptOutcome::from_raw_parts(
        AttemptResultKind::Failure,
        Some("transient failure".to_string()),
        None,
    )
    .expect("failure outcome");
    finish_attempt(&mut authority, run_a, attempt_a1, failure);
    release_lease(&mut authority, run_a, lease_a1);
    transition(&mut authority, run_a, RunState::Running, RunState::RetryWait);
    transition(&mut authority, run_a, RunState::RetryWait, RunState::Ready);
    let (attempt_a2, lease_a2) = lease_and_start_attempt(&mut authority, run_a, ATTEMPT_A2);
    let success =
        AttemptOutcome::from_raw_parts(AttemptResultKind::Success, None, Some(b"ok".to_vec()))
            .expect("success outcome");
    finish_attempt(&mut authority, run_a, attempt_a2, success);
    release_lease(&mut authority, run_a, lease_a2);
    transition(&mut authority, run_a, RunState::Running, RunState::Completed);

    // Task B: concurrency key, promoted to Ready, then the task is canceled.
    let constraints_b =
        TaskConstraints::new(1, None, Some("archive-key".to_string())).expect("constraints");
    let run_b =
        create_task_and_run(&mut authority, task_spec(TASK_B, b"task-b", constraints_b), RUN_B);
    transition(&mut authority, run_b, RunState::Scheduled, RunState::Ready);
    let seq = next_seq(&authority);
    submit(
        &mut authority,
        MutationCommand::TaskCancel(TaskCancelCommand::new(
            seq,
            TaskId::from_str(TASK_B).expect("fixed task id"),
            seq,
        )),
    );

    // Task C: left Scheduled to represent in-flight work at the freeze point.
    let run_c = create_task_and_run(
        &mut authority,
        task_spec(TASK_C, b"task-c", TaskConstraints::default()),
        RUN_C,
    );

    // Engine control records.
    let seq = next_seq(&authority);
    submit(&mut authority, MutationCommand::EnginePause(EnginePauseCommand::new(seq, seq)));
    let seq = next_seq(&authority);
    let final_sequence =
        submit(&mut authority, MutationCommand::EngineResume(EngineResumeCommand::new(seq, seq)));

    projection_facts(authority.projection(), &[run_a, run_b, run_c], final_sequence)
}

fn projection_facts(projection: &ReplayReducer, runs: &[RunId], final_sequence: u64) -> Value {
    let run_states: Vec<Value> = runs
        .iter()
        .map(|run_id| {
            let attempts: Vec<Value> = projection
                .get_attempt_history(run_id)
                .unwrap_or(&[])
                .iter()
                .map(|entry| {
                    json!({
                        "attempt_id": entry.attempt_id().to_string(),
                        "result": entry.result().map(|r| format!("{r:?}")),
                        "error": entry.error(),
                        "output_len": entry.output().map(<[u8]>::len),
                    })
                })
                .collect();
            json!({
                "run_id": run_id.to_string(),
                "state": projection.get_run_state(run_id).map(|s| s.to_string()),
                "attempts": attempts,
            })
        })
        .collect();
    json!({
        "task_ids": [TASK_A, TASK_B, TASK_C],
        "canceled_task_ids": [TASK_B],
        "task_count": projection.task_count(),
        "run_count": projection.run_count(),
        "latest_sequence": final_sequence,
        "engine_paused": projection.is_engine_paused(),
        "runs": run_states,
    })
}

fn write_snapshot(data_dir: &Path, target: &Path) {
    let recovery = load_projection_from_storage(data_dir).expect("bootstrap for snapshot");
    let snapshot = build_snapshot_from_projection(&recovery.projection, 1_700_000_000)
        .expect("snapshot build");
    let mut writer = SnapshotFsWriter::new(target.to_path_buf()).expect("snapshot writer");
    writer.write(&snapshot).expect("snapshot write");
    writer.flush().expect("snapshot flush");
    writer.close().expect("snapshot close");
}

fn write_truncated_variant(source: &Path, target: &Path) {
    let mut bytes = std::fs::read(source).expect("read WAL fixture");
    let keep = bytes.len().saturating_sub(7);
    bytes.truncate(keep);
    std::fs::write(target, bytes).expect("write truncated WAL fixture");
}

fn write_bitflip_variant(source: &Path, target: &Path) {
    let mut bytes = std::fs::read(source).expect("read snapshot fixture");
    // Flip a byte deep inside the JSON payload, past the 12-byte framing header.
    let index = 12 + (bytes.len() - 12) / 2;
    bytes[index] ^= 0x01;
    std::fs::write(target, bytes).expect("write corrupt snapshot fixture");
}

fn write_json(path: &Path, value: &Value) {
    let mut text = serde_json::to_string_pretty(value).expect("serialize json");
    text.push('\n');
    std::fs::write(path, text).expect("write json");
}

fn append_batch<W: WalWriter>(authority: &mut Authority<W>, count: u64, policy: DurabilityPolicy) {
    for _ in 0..count {
        let seq = next_seq(authority);
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"perf".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("perf task spec");
        let _ = authority
            .submit_command(
                MutationCommand::TaskCreate(TaskCreateCommand::new(seq, spec, seq)),
                policy,
            )
            .expect("perf append");
    }
}

/// Measures WAL append throughput and cold replay time. The numbers are
/// environment-specific evidence, not target thresholds.
fn measure_append_and_replay() -> Value {
    let data_dir = scratch_dir("perf");
    let mut authority = open_authority(&data_dir);

    let started = Instant::now();
    append_batch(&mut authority, IMMEDIATE_APPENDS, DurabilityPolicy::Immediate);
    let immediate = started.elapsed();

    let started = Instant::now();
    append_batch(&mut authority, DEFERRED_APPENDS, DurabilityPolicy::Deferred);
    let deferred = started.elapsed();
    drop(authority);

    let wal_bytes = std::fs::metadata(data_dir.join("wal").join("actionqueue.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    let started = Instant::now();
    let recovery = load_projection_from_storage(&data_dir).expect("replay bootstrap");
    let replay = started.elapsed();
    let replayed_tasks = recovery.projection.task_count();
    drop(recovery);
    let _ = std::fs::remove_dir_all(&data_dir);

    json!({
        "note": "environment-specific evidence captured at the pre-AQ-CONT-1 freeze; not a target threshold",
        "cargo_profile": if cfg!(debug_assertions) { "dev" } else { "release" },
        "immediate_durability": {
            "appends": IMMEDIATE_APPENDS,
            "elapsed_ms": immediate.as_millis(),
            "appends_per_second": per_second(IMMEDIATE_APPENDS, immediate),
        },
        "deferred_durability": {
            "appends": DEFERRED_APPENDS,
            "elapsed_ms": deferred.as_millis(),
            "appends_per_second": per_second(DEFERRED_APPENDS, deferred),
        },
        "wal_only_replay": {
            "records": IMMEDIATE_APPENDS + DEFERRED_APPENDS,
            "replayed_tasks": replayed_tasks,
            "wal_bytes": wal_bytes,
            "elapsed_ms": replay.as_millis(),
            "records_per_second": per_second(IMMEDIATE_APPENDS + DEFERRED_APPENDS, replay),
        },
    })
}

fn per_second(count: u64, elapsed: std::time::Duration) -> u64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0;
    }
    (count as f64 / secs).round() as u64
}
