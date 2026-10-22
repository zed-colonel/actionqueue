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
    snapshot::checkpoint::publish_checkpoint,
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

fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("retained-history").unwrap()),
    }
}
fn open_with(path: &std::path::Path, options: OpenOptions) -> Authority {
    open_store(path, options).unwrap().into_authority().unwrap().with_host(host())
}
fn open(path: &std::path::Path) -> Authority {
    open_with(path, OpenOptions::Initialize { features: vec![] })
}
/// Opens a copied store, which must already exist.
fn reopen(path: &std::path::Path) -> Authority {
    open_with(path, OpenOptions::ReadWrite)
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
    wal_frames: u64,
    wal_bytes: u64,
}

impl Measurement {
    fn json(&self, operations: usize) -> Value {
        json!({
            "elapsed_ns": self.elapsed_ns,
            "wal_frames_read":self.wal_frames,"wal_bytes_read":self.wal_bytes,
            "operations": operations,
            "mean_ns_per_operation": self.elapsed_ns as f64 / operations as f64,
            "shallow_projection_clones": self.work.shallow_projection_clones,
            "record_copies": self.work.record_copies,
            "subscription_candidates": self.work.subscription_candidates,
            "cancellation_run_candidates": self.work.cancellation_run_candidates,
            "reducer_event_visits": self.work.reducer_event_visits,
        })
    }
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, Measurement) {
    work::reset_projection_work();
    work::reset_wal_io();
    let started = Instant::now();
    let result = operation();
    let elapsed = started.elapsed();
    let work = work::projection_work();
    let (wal_frames, wal_bytes) = work::wal_io();
    (
        result,
        Measurement {
            elapsed_ns: elapsed.as_nanos().try_into().unwrap(),
            work,
            wal_frames,
            wal_bytes,
        },
    )
}

/// AQ-P2 mutation and AQ-P3 recovery work bounds. Historical evidence stays in docs/performance.
/// Hardware timing gates run in the bench driver; ordinary CI asserts structural work.
fn assert_shared(
    n: usize,
    admission: &Measurement,
    cancel: &Measurement,
    wal: &Measurement,
    snapshot: &Measurement,
) {
    for m in [admission, cancel, wal, snapshot] {
        assert_eq!(m.work.subscription_candidates, 0);
    }
    assert_eq!(admission.work.shallow_projection_clones, 2 * BATCH);
    assert_eq!(cancel.work.shallow_projection_clones, 2 * BATCH);
    assert_eq!(cancel.work.cancellation_run_candidates, BATCH);
    assert_eq!(admission.work.reducer_event_visits, BATCH);
    assert_eq!(cancel.work.reducer_event_visits, BATCH);
    // Path copying only edits the addressed task/run/bucket, independent of N.
    assert!(admission.work.record_copies <= 10 * BATCH);
    assert!(cancel.work.record_copies <= 20 * BATCH);
    let events = 1 + 2 * (n + BATCH);
    assert_eq!(wal.work.reducer_event_visits, events);
    assert_eq!(snapshot.work.reducer_event_visits, 2 * BATCH);
    assert_eq!(wal.wal_frames, events as u64);
    assert_eq!(snapshot.wal_frames, (2 * BATCH + 2) as u64);
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

thread_local! { static FIXTURE_CACHE:std::cell::RefCell<Option<std::path::PathBuf>>=const { std::cell::RefCell::new(None) }; }
#[allow(dead_code)]
pub fn set_fixture_cache(path: std::path::PathBuf) {
    FIXTURE_CACHE.with(|p| *p.borrow_mut() = Some(path));
}
/// Copies a closed store's manifest, lock file and WAL. A durable copy also
/// synchronizes those files, the store's directories and `dest`'s parent.
fn copy_store(source: &std::path::Path, dest: &std::path::Path, durable: bool) {
    std::fs::create_dir_all(dest.join("wal")).unwrap();
    std::fs::create_dir_all(dest.join("snapshots")).unwrap();
    for name in ["manifest.json", "store.lock", "wal/actionqueue.wal"] {
        std::fs::copy(source.join(name), dest.join(name)).unwrap();
        if durable {
            std::fs::File::open(dest.join(name)).unwrap().sync_all().unwrap();
        }
    }
    if durable {
        let parent = dest.parent().unwrap().to_owned();
        for dir in [dest.join("wal"), dest.join("snapshots"), dest.to_owned(), parent] {
            std::fs::File::open(dir).unwrap().sync_all().unwrap();
        }
    }
}
fn seed(n: usize) -> (tempfile::TempDir, Authority) {
    eprintln!("retained_history: N={n}, building canceled history");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let cache = FIXTURE_CACHE.with(|p| p.borrow().as_ref().map(|p| p.join(n.to_string())));
    if let Some(cache) = &cache {
        if cache.exists() {
            copy_store(cache, &root, false);
            let a = reopen(&root);
            check_projection(a.projection(), n, 1 + 2 * n as u64);
            return (dir, a);
        }
    }
    let mut a = open(&root);
    for id in 1..=n {
        assert!(admit(&mut a, request(id)).is_created());
        let outcome = cancel(&mut a, task_id(id));
        assert!(matches!(outcome.applied(), AppliedMutation::TaskCancel { .. }));
        if id % 1000 == 0 {
            eprintln!("retained_history: N={n}, prepared {id} canceled runs");
        }
    }
    if let Some(cache) = cache {
        copy_store(&root, &cache, false);
    }
    (dir, a)
}
/// Where a sample's retained prefix comes from. It fixes the bootstrap order and
/// the labels each result reports, so the two cannot disagree.
enum Prefix {
    /// The authority that built the prefix, WAL-first: the AQ-P1 and 0.2.0 boundaries.
    LiveSeed,
    /// Durable copies of the closed seed, opened by their own writer session.
    Copied { repetition: usize, wal_sha256: String },
}
impl Prefix {
    fn snapshot_first(&self) -> bool {
        matches!(self, Prefix::Copied { repetition, .. } if repetition % 2 == 0)
    }
    fn label(&self, result: &mut Value) {
        match self {
            Prefix::LiveSeed => {
                result["cache_conditions"] =
                    "warm OS cache; no cache eviction; WAL-first order; live seed authority".into();
            }
            Prefix::Copied { repetition, wal_sha256 } => {
                result["cache_conditions"] = "warm OS cache; no cache eviction; alternating order; independent copied files per repetition".into();
                result["repetition"] = (*repetition).into();
                result["prefix_wal_sha256"] = wal_sha256.as_str().into();
            }
        }
    }
}
/// One sample measures the live seed authority in WAL-first order. Multiple
/// samples reuse the closed, durable, unmeasured prefix: each receives fresh
/// files, its own writer/session and exactly N retained runs, alternating the
/// bootstrap order. Timed phases and durability/recovery assertions are identical.
pub fn run_repeated(n: usize, repetitions: usize) -> Vec<Value> {
    assert!(repetitions > 0);
    let (seed_dir, a) = seed(n);
    if repetitions == 1 {
        return vec![measure_state(n, seed_dir, a, Prefix::LiveSeed)];
    }
    let seed_root = seed_dir.path().join("store");
    let seed_wal = a.store_session().unwrap().wal_path();
    let (writer, projection) = a.into_parts();
    writer.close().unwrap();
    drop(projection);
    let prefix_hash = wal_digest(&seed_wal);
    let mut results = Vec::new();
    for repetition in 1..=repetitions {
        eprintln!("retained_history: N={n}, repetition {repetition}/{repetitions}");
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("store");
        copy_store(&seed_root, &root, true);
        assert_eq!(wal_digest(&root.join("wal/actionqueue.wal")), prefix_hash);
        let prefix = Prefix::Copied { repetition, wal_sha256: prefix_hash.clone() };
        let result = measure_state(n, dir, reopen(&root), prefix);
        eprintln!("retained_history sample: {}", serde_json::to_string(&result).unwrap());
        results.push(result);
    }
    assert_eq!(wal_digest(&seed_wal), prefix_hash);
    results
}
fn measure_state(n: usize, dir: tempfile::TempDir, mut a: Authority, prefix: Prefix) -> Value {
    let snapshot_first = prefix.snapshot_first();
    let root = dir.path().join("store");
    let prefix_sequence = 1 + 2 * n as u64;
    check_projection(a.projection(), n, prefix_sequence);
    let prefix_wal_bytes = std::fs::metadata(a.store_session().unwrap().wal_path()).unwrap().len();
    let candidate = a.capture_checkpoint(1002).unwrap();
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
    let expected_controls: Vec<_> = a
        .projection()
        .task_records()
        .map(|t| {
            let id = t.task_spec().id();
            (
                id,
                a.projection()
                    .control_sequences(
                        actionqueue_storage::recovery::inspection::ControlTarget::Task(id),
                    )
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let (publication, publication_measurement) = measure(|| publish_checkpoint(candidate).unwrap());
    assert_eq!(publication_measurement.work.reducer_event_visits, 0);
    assert_eq!(publication_measurement.wal_frames, 0);
    let (writer, projection) = a.into_parts();
    writer.close().unwrap();
    drop(projection);
    let wal_bytes = std::fs::metadata(&wal_path).unwrap().len();
    let wal_sha256 = wal_digest(&wal_path);
    let snapshot_bytes = std::fs::metadata(&snapshot_path).unwrap().len();
    assert!(snapshot_bytes < 256 * 1024 * 1024);
    let saved = dir.path().join("saved-checkpoint.bin");
    let do_wal = || {
        std::fs::rename(&snapshot_path, &saved).unwrap();
        let (wal, m) = measure(|| load_existing_projection(&root).unwrap());
        assert!(!wal.snapshot_loaded);
        check_projection(&wal.projection, n + BATCH, final_sequence);
        assert_eq!(wal.projection.projection_digest().unwrap(), expected_digest);
        wal.wal_writer.close().unwrap();
        drop(wal.projection);
        std::fs::rename(&saved, &snapshot_path).unwrap();
        m
    };
    let do_snapshot = || {
        let (snapshot, m) = measure(|| load_existing_projection(&root).unwrap());
        assert!(snapshot.snapshot_loaded);
        assert_eq!(snapshot.snapshot_sequence, prefix_sequence);
        check_projection(&snapshot.projection, n + BATCH, final_sequence);
        assert!(snapshot.projection.control_targets_available());
        for (id, sequences) in &expected_controls {
            assert_eq!(
                snapshot
                    .projection
                    .control_sequences(
                        actionqueue_storage::recovery::inspection::ControlTarget::Task(*id)
                    )
                    .collect::<Vec<_>>(),
                *sequences
            );
        }
        assert_eq!(snapshot.projection.projection_digest().unwrap(), expected_digest);
        let phases = snapshot.phases;
        snapshot.wal_writer.close().unwrap();
        drop(snapshot.projection);
        (m, phases)
    };
    let (wal_measurement, snapshot_measurement, phases) = if snapshot_first {
        let (sm, p) = do_snapshot();
        (do_wal(), sm, p)
    } else {
        let wm = do_wal();
        let (sm, p) = do_snapshot();
        (wm, sm, p)
    };
    assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), wal_bytes);
    assert_eq!(wal_digest(&wal_path), wal_sha256, "paired recoveries use identical WAL contents");
    assert_shared(n, &admission, &cancellation, &wal_measurement, &snapshot_measurement);
    // Same checkpoint, variable tails, including no tail. Prepare variants and
    // parity expectations outside complete writable startup timers.
    let bytes = std::fs::read(&wal_path).unwrap();
    let mut tail_variants = Vec::new();
    for tail in [0usize, 20] {
        let variant = tempfile::tempdir().unwrap();
        let variant_root = variant.path().join("store");
        copy_store(&root, &variant_root, false);
        std::fs::copy(&snapshot_path, variant_root.join("snapshots/snapshot.bin")).unwrap();
        let mut end = prefix_wal_bytes as usize;
        for _ in 0..tail {
            end += actionqueue_storage::wal::tail_validation::parse_record_at(&bytes, end)
                .unwrap()
                .unwrap()
                .record_len;
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(variant_root.join("wal/actionqueue.wal"))
            .unwrap()
            .set_len(end as u64)
            .unwrap();
        let (boot, m) = measure(|| load_existing_projection(&variant_root).unwrap());
        assert_eq!(m.work.reducer_event_visits, tail);
        assert_eq!(m.wal_frames, tail as u64 + 2);
        assert_eq!(boot.projection.latest_sequence(), prefix_sequence + tail as u64);
        tail_variants.push(json!({"tail_events":tail,"startup":m.json(1),"phases":boot.phases}));
        boot.wal_writer.close().unwrap();
        drop(boot.projection);
    }

    let mut result = json!({
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
        "tail_variants":tail_variants,
        "snapshot_bytes": snapshot_bytes,
        "publication": publication,
        "snapshot_phases":phases,
        "hydration_seconds":phases.snapshot_load_seconds+phases.hydration_seconds,
        "paired_startup_ratio":snapshot_measurement.elapsed_ns as f64 / wal_measurement.elapsed_ns as f64,
        "path_order":if snapshot_first { "snapshot,wal" } else { "wal,snapshot" },
        "projection_digest": expected_digest,
        "admission": admission.json(BATCH),
        "cancellation": cancellation.json(BATCH),
        "wal_only_bootstrap": wal_measurement.json(1),
        "snapshot_plus_tail_bootstrap": snapshot_measurement.json(1),
    });
    prefix.label(&mut result);
    result
}
