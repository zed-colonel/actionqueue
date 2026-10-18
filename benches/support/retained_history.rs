//! Shared synchronous fixture: retained canceled Once runs, without attempts or waits.
use std::time::Instant;

use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::OpaqueRef,
    causal::{CausalContext, ControlMutationContext},
    control::{ControlScope, HostControlContext},
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
    mutation::{
        AppliedMutation, CancelCommand, CancelTarget, DurabilityPolicy, MutationAuthority,
        MutationCommand, MutationOutcome,
    },
    run::RunState,
    task::{
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
    time::clock::MockClock,
};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::{
        bootstrap::load_existing_projection,
        reducer::ReplayReducer,
        work::{self, ProjectionWork},
    },
    snapshot::{
        build::build_snapshot_from_projection,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
    store::{open_store, OpenOptions},
    wal::{fs_writer::WalFsWriter, writer::WalWriter},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const BATCH: usize = 100;
type Authority = StorageMutationAuthority<WalFsWriter, ReplayReducer>;

fn task_id(n: usize) -> TaskId {
    format!("00000000-0000-0000-0000-{n:012x}").parse().unwrap()
}

fn request(n: usize) -> EnsureTaskRequest {
    EnsureTaskRequest::new(
        AdmissionKey::new(format!("retained/{n:012}")).unwrap(),
        TaskSpec::new(
            task_id(n),
            TaskPayload::new(vec![0, 1, 255]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
        vec![],
        CausalContext::new(
            TraceId::new("retained-history").unwrap(),
            CorrelationId::new("retained-history").unwrap(),
        ),
        None,
    )
    .unwrap()
}

fn open(path: &std::path::Path) -> Authority {
    open_store(path, OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap()
        .with_host(HostControlContext {
            actor_id: None,
            scope: ControlScope::SingleTenant,
            attribution: ControlMutationContext::new(OpaqueRef::new("retained-history").unwrap()),
        })
}

fn admit(a: &mut Authority, q: EnsureTaskRequest) -> EnsureTaskOutcome {
    actionqueue_runtime::admission::ensure_task(a, q, &MockClock::new(1000)).unwrap()
}

fn cancel(a: &mut Authority, task: TaskId) -> MutationOutcome {
    a.submit_command(
        MutationCommand::Cancel(CancelCommand {
            expected_sequence: a.projection().latest_sequence() + 1,
            target: CancelTarget::Task(task),
            tenant_id: None,
            control_context: None,
            timestamp: 1001,
        }),
        DurabilityPolicy::Immediate,
    )
    .unwrap()
}

struct Measurement {
    elapsed_ns: u64,
    work: ProjectionWork,
}

impl Measurement {
    fn json(&self, operations: usize) -> Value {
        json!({
            "elapsed_ns": self.elapsed_ns,
            "operations": operations,
            "mean_ns_per_operation": self.elapsed_ns as f64 / operations as f64,
            "full_projection_clones": self.work.full_projection_clones,
            "cloned_run_entries": self.work.cloned_run_entries,
            "shallow_projection_clones": self.work.shallow_projection_clones,
            "record_copies": self.work.record_copies,
            "subscription_candidates": self.work.subscription_candidates,
            "reducer_event_visits": self.work.reducer_event_visits,
        })
    }
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, Measurement) {
    work::reset_projection_work();
    let started = Instant::now();
    let result = operation();
    let elapsed = started.elapsed();
    let work = work::projection_work();
    (result, Measurement { elapsed_ns: elapsed.as_nanos().try_into().unwrap(), work })
}

/// AQ-P2 structural expectations. The recorded 0.2.0 baseline remains in docs/performance.
/// Timing is informational; no elapsed-time assertion runs in CI.
fn assert_shared(
    n: usize,
    admission: &Measurement,
    cancel: &Measurement,
    wal: &Measurement,
    snapshot: &Measurement,
) {
    for m in [admission, cancel, wal, snapshot] {
        assert_eq!(m.work.full_projection_clones, 0);
        assert_eq!(m.work.cloned_run_entries, 0);
        assert_eq!(m.work.subscription_candidates, 0);
    }
    assert_eq!(admission.work.shallow_projection_clones, 2 * BATCH);
    assert_eq!(cancel.work.shallow_projection_clones, 2 * BATCH);
    assert_eq!(admission.work.reducer_event_visits, BATCH);
    assert_eq!(cancel.work.reducer_event_visits, BATCH);
    // Path copying only edits the addressed task/run/bucket, independent of N.
    assert!(admission.work.record_copies <= 10 * BATCH);
    assert!(cancel.work.record_copies <= 20 * BATCH);
    let events = 1 + 2 * (n + BATCH);
    assert_eq!(wal.work.reducer_event_visits, events);
    assert_eq!(snapshot.work.reducer_event_visits, events + 2 * BATCH);
}

fn check_projection(p: &ReplayReducer, tasks: usize, sequence: u64) {
    assert_eq!(p.latest_sequence(), sequence);
    assert_eq!(p.task_count(), tasks);
    assert_eq!(p.run_count(), tasks);
    assert_eq!(p.admissions().count(), tasks);
    assert!(p.task_records().all(|t| t.task_spec().tenant_id().is_none()));
    assert!(p.run_instances().all(|r| r.state() == RunState::Canceled));
    assert!(p.run_instances().all(|r| p.get_attempt_history(&r.id()).is_none_or(|h| h.is_empty())));
    assert_eq!(p.waits().records().count(), 0);
    assert!(p.task_records().all(|t| p.is_task_canceled(t.task_spec().id())));
}

fn wal_digest(path: &std::path::Path) -> String {
    format!("{:x}", Sha256::digest(std::fs::read(path).unwrap()))
}

pub fn run(n: usize) -> Value {
    eprintln!("retained_history: N={n}, building canceled history");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut a = open(&root);
    for id in 1..=n {
        assert!(admit(&mut a, request(id)).is_created());
        let outcome = cancel(&mut a, task_id(id));
        assert!(matches!(outcome.applied(), AppliedMutation::TaskCancel { .. }));
        if id % 1000 == 0 {
            eprintln!("retained_history: N={n}, prepared {id} canceled runs");
        }
    }
    let prefix_sequence = 1 + 2 * n as u64;
    check_projection(a.projection(), n, prefix_sequence);
    let prefix = build_snapshot_from_projection(a.projection(), 1002).unwrap();
    let wal_path = a.store_session().unwrap().wal_path();
    let snapshot_path = a.store_session().unwrap().snapshot_path();
    assert!(!snapshot_path.exists());
    // Build inputs and allocate result storage outside the measured regions.
    let requests: Vec<_> = (n + 1..=n + BATCH).map(request).collect();
    let cancel_tasks: Vec<_> = (n + 1..=n + BATCH).map(task_id).collect();
    let mut admissions = Vec::with_capacity(BATCH);
    let mut cancellations = Vec::with_capacity(BATCH);
    eprintln!("retained_history: N={n}, measuring admissions and cancellations");
    let ((), admission) = measure(|| {
        for q in requests {
            admissions.push(admit(&mut a, q));
        }
    });
    for (index, outcome) in admissions.iter().enumerate() {
        assert!(outcome.is_created());
        assert_eq!(outcome.sequence(), prefix_sequence + index as u64 + 1);
    }
    let ((), cancellation) = measure(|| {
        for task in cancel_tasks {
            cancellations.push(cancel(&mut a, task));
        }
    });
    for (index, outcome) in cancellations.iter().enumerate() {
        assert!(
            matches!(outcome.applied(), AppliedMutation::TaskCancel { task_id: id } if *id == task_id(n + index + 1))
        );
        assert_eq!(outcome.sequence(), prefix_sequence + BATCH as u64 + index as u64 + 1);
    }
    let final_sequence = prefix_sequence + 2 * BATCH as u64;
    check_projection(a.projection(), n + BATCH, final_sequence);
    let expected_digest = a.projection().projection_digest().unwrap();
    let (writer, projection) = a.into_parts();
    writer.close().unwrap();
    drop(projection);
    let wal_bytes = std::fs::metadata(&wal_path).unwrap().len();
    let wal_sha256 = wal_digest(&wal_path);

    eprintln!("retained_history: N={n}, measuring WAL-only bootstrap");
    let (wal, wal_measurement) = measure(|| load_existing_projection(&root).unwrap());
    assert!(!wal.snapshot_loaded);
    assert_eq!(wal.snapshot_sequence, 0);
    check_projection(&wal.projection, n + BATCH, final_sequence);
    assert_eq!(wal.projection.projection_digest().unwrap(), expected_digest);
    assert_eq!(wal_digest(&wal_path), wal_sha256);

    eprintln!("retained_history: N={n}, publishing saved prefix (unmeasured)");
    let mut snapshot_writer =
        SnapshotFsWriter::new(wal.wal_writer.store_session().unwrap()).unwrap();
    snapshot_writer.write(&prefix).unwrap();
    snapshot_writer.close().unwrap();
    wal.wal_writer.close().unwrap();
    drop(wal.projection);
    drop(prefix);
    let snapshot_bytes = std::fs::metadata(&snapshot_path).unwrap().len();

    eprintln!("retained_history: N={n}, measuring snapshot-plus-tail bootstrap");
    let (snapshot, snapshot_measurement) = measure(|| load_existing_projection(&root).unwrap());
    assert!(snapshot.snapshot_loaded);
    assert_eq!(snapshot.snapshot_sequence, prefix_sequence);
    check_projection(&snapshot.projection, n + BATCH, final_sequence);
    assert_eq!(snapshot.projection.projection_digest().unwrap(), expected_digest);
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_bytes);
    assert_eq!(
        wal_digest(&wal_path),
        wal_sha256,
        "both recoveries must use identical WAL contents"
    );
    snapshot.wal_writer.close().unwrap();
    assert_shared(n, &admission, &cancellation, &wal_measurement, &snapshot_measurement);

    json!({
        "retained_terminal_runs": n,
        "batch_size": BATCH,
        "final_tasks": n + BATCH,
        "final_runs": n + BATCH,
        "final_admissions": n + BATCH,
        "prefix_events": prefix_sequence,
        "tail_events": 2 * BATCH,
        "final_sequence": final_sequence,
        "wal_bytes": wal_bytes,
        "wal_sha256": wal_sha256,
        "snapshot_bytes": snapshot_bytes,
        "projection_digest": expected_digest,
        "admission": admission.json(BATCH),
        "cancellation": cancellation.json(BATCH),
        "wal_only_bootstrap": wal_measurement.json(1),
        "snapshot_plus_tail_bootstrap": snapshot_measurement.json(1),
    })
}

#[cfg(test)]
#[test]
fn populated_clone_is_equivalent_and_mutates_independently() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    assert!(admit(&mut a, request(1)).is_created());
    assert!(admit(&mut a, request(2)).is_created());
    assert!(matches!(cancel(&mut a, task_id(1)).applied(), AppliedMutation::TaskCancel { .. }));
    let p = a.projection();
    work::reset_projection_work();
    let before = p.projection_digest().unwrap();
    let _ = build_snapshot_from_projection(p, 1002).unwrap();
    assert_eq!(work::projection_work(), ProjectionWork::default());
    let mut cloned = p.clone();
    assert_eq!(
        work::projection_work(),
        ProjectionWork { shallow_projection_clones: 1, ..ProjectionWork::default() }
    );
    // Debug includes derived indexes that the canonical digest omits.
    assert_eq!(format!("{p:?}"), format!("{cloned:?}"));
    assert_eq!(cloned.projection_digest().unwrap(), before);
    work::reset_projection_work();
    let event = actionqueue_storage::wal::event::WalEvent::new(
        p.latest_sequence() + 1,
        actionqueue_storage::wal::event::WalEventType::TaskCancellationCommitted {
            record: actionqueue_storage::mutation::wait::CancelRecord {
                target: CancelTarget::Task(task_id(2)),
                tenant_id: None,
                control_context: None,
                sequence: p.latest_sequence() + 1,
                timestamp: 1003,
            },
        },
    );
    cloned.apply(&event).unwrap();
    assert!(cloned.apply(&event).is_err());
    assert_eq!(work::projection_work().reducer_event_visits, 2);
    assert!(cloned.is_task_canceled(task_id(2)));
    assert!(!p.is_task_canceled(task_id(2)));
    assert_eq!(cloned.runs_for_task(task_id(2)).next().unwrap().state(), RunState::Canceled);
    assert_eq!(p.runs_for_task(task_id(2)).next().unwrap().state(), RunState::Scheduled);
    assert_eq!(p.projection_digest().unwrap(), before);
    assert_ne!(cloned.projection_digest().unwrap(), before);
    let (writer, _) = a.into_parts();
    writer.close().unwrap();
}
