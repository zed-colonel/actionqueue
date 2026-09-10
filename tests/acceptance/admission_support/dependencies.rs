//! F-001: admission and recovery must reconstruct dependency eligibility from
//! durable completion, including tasks whose completion notification was GC'd.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use actionqueue_core::{
    admission::EnsureTaskRequest,
    run::state::RunState,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_engine::time::clock::Clock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use actionqueue_storage::{
    recovery::bootstrap::load_projection_from_storage,
    snapshot::{
        build::build_snapshot_from_projection,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
};

use super::admission_support::{id, open};

#[derive(Clone)]
struct TestClock(Arc<AtomicU64>);

impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct SuccessHandler;

impl ExecutorHandler for SuccessHandler {
    fn execute(&self, _: ExecutorContext) -> HandlerOutput {
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

type Engine = BootstrappedEngine<SuccessHandler, TestClock>;

#[derive(Debug, Clone, Copy)]
enum Recovery {
    Live,
    Wal,
    Snapshot,
}

const RECOVERY_MODES: [Recovery; 3] = [Recovery::Live, Recovery::Wal, Recovery::Snapshot];

fn start(config: &RuntimeConfig, clock: &TestClock) -> Engine {
    ActionQueueEngine::new(config.clone(), SuccessHandler)
        .bootstrap_with_clock(clock.clone())
        .unwrap()
}

fn recover(engine: Engine, mode: Recovery, config: &RuntimeConfig, clock: &TestClock) -> Engine {
    if matches!(mode, Recovery::Live) {
        return engine;
    }
    let digest = engine.projection().projection_digest().unwrap();
    engine.shutdown().unwrap();
    if matches!(mode, Recovery::Snapshot) {
        let authority = open(&config.data_dir);
        let snapshot = build_snapshot_from_projection(authority.projection(), clock.now()).unwrap();
        let mut writer = SnapshotFsWriter::new(authority.store_session().unwrap()).unwrap();
        writer.write(&snapshot).unwrap();
        writer.close().unwrap();
    }
    {
        let recovery = load_projection_from_storage(&config.data_dir).unwrap();
        assert_eq!(recovery.snapshot_loaded, matches!(mode, Recovery::Snapshot));
        assert_eq!(recovery.projection.projection_digest().unwrap(), digest);
    }
    let engine = start(config, clock);
    assert_eq!(engine.projection().projection_digest().unwrap(), digest);
    engine
}

fn request(n: u64, policy: RunPolicy, prerequisites: &[u64]) -> EnsureTaskRequest {
    let spec = TaskSpec::new(
        id(n),
        TaskPayload::new(vec![]),
        policy,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap();
    EnsureTaskRequest::for_task(spec, prerequisites.iter().copied().map(id).collect()).unwrap()
}

fn assert_states(engine: &Engine, n: u64, expected: &[RunState]) {
    let mut runs: Vec<_> = engine.projection().runs_for_task(id(n)).collect();
    runs.sort_by_key(|run| run.scheduled_at());
    assert_eq!(runs.iter().map(|run| run.state()).collect::<Vec<_>>(), expected, "task {n}");
}

#[tokio::test]
async fn admission_after_prerequisite_completion_executes_live_and_after_recovery() {
    for mode in RECOVERY_MODES {
        let dir = tempfile::tempdir().unwrap();
        let config = RuntimeConfig {
            data_dir: dir.path().to_path_buf(),
            snapshot_event_threshold: None,
            ..Default::default()
        };
        let clock = TestClock(Arc::new(AtomicU64::new(1000)));
        let mut engine = start(&config, &clock);
        engine.ensure_task(request(1, RunPolicy::Once, &[])).unwrap();
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 1);
        assert_states(&engine, 1, &[RunState::Completed]);

        let dependent = request(2, RunPolicy::Once, &[1]);
        let created = engine.ensure_task(dependent.clone()).unwrap();
        assert!(created.is_created());
        assert_states(&engine, 2, &[RunState::Scheduled]);
        let mut engine = recover(engine, mode, &config, &clock);
        let duplicate = engine.ensure_task(dependent).unwrap();
        assert!(!duplicate.is_created());
        assert_eq!(duplicate.sequence(), created.sequence());
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 1, "{mode:?}");
        assert_states(&engine, 2, &[RunState::Completed]);
        engine.shutdown().unwrap();
    }
}

#[tokio::test]
async fn unrelated_admission_preserves_existing_eligibility_live_and_after_recovery() {
    for mode in RECOVERY_MODES {
        let dir = tempfile::tempdir().unwrap();
        let config = RuntimeConfig {
            data_dir: dir.path().to_path_buf(),
            snapshot_event_threshold: None,
            ..Default::default()
        };
        let clock = TestClock(Arc::new(AtomicU64::new(1000)));
        let mut engine = start(&config, &clock);
        engine.ensure_task(request(1, RunPolicy::Once, &[])).unwrap();
        engine.ensure_task(request(2, RunPolicy::repeat(2, 10).unwrap(), &[1])).unwrap();
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 2);
        assert_states(&engine, 1, &[RunState::Completed]);
        // The first run proves eligibility was already established by A's
        // completion notification; B's remaining run is waiting only on time.
        assert_states(&engine, 2, &[RunState::Completed, RunState::Scheduled]);

        let unrelated = request(3, RunPolicy::Once, &[]);
        assert!(engine.submit_task(unrelated.task_spec().clone()).unwrap().is_created());
        let mut engine = recover(engine, mode, &config, &clock);
        clock.0.store(1010, Ordering::SeqCst);
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 2, "{mode:?}");
        assert_states(&engine, 2, &[RunState::Completed, RunState::Completed]);
        assert_states(&engine, 3, &[RunState::Completed]);
        engine.shutdown().unwrap();
    }
}

#[tokio::test]
async fn restored_eligibility_does_not_satisfy_unfinished_prerequisites() {
    for mode in RECOVERY_MODES {
        let dir = tempfile::tempdir().unwrap();
        let config = RuntimeConfig {
            data_dir: dir.path().to_path_buf(),
            snapshot_event_threshold: None,
            ..Default::default()
        };
        let clock = TestClock(Arc::new(AtomicU64::new(1000)));
        let mut engine = start(&config, &clock);
        engine.ensure_task(request(1, RunPolicy::Once, &[])).unwrap();
        let _ = engine.run_until_idle().await.unwrap();
        engine.ensure_task(request(2, RunPolicy::repeat(2, 10).unwrap(), &[1])).unwrap();
        engine.ensure_task(request(3, RunPolicy::Once, &[1, 2])).unwrap();
        let mut engine = recover(engine, mode, &config, &clock);
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 1, "{mode:?}");
        assert_states(&engine, 2, &[RunState::Completed, RunState::Scheduled]);
        assert_states(&engine, 3, &[RunState::Scheduled]);

        // Rebuild again with one completed run of B: all of B's runs must be
        // terminal before its dependents may execute.
        engine.ensure_task(request(4, RunPolicy::Once, &[])).unwrap();
        let mut engine = recover(engine, mode, &config, &clock);
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 1, "{mode:?}");
        assert_states(&engine, 3, &[RunState::Scheduled]);
        clock.0.store(1010, Ordering::SeqCst);
        assert_eq!(engine.run_until_idle().await.unwrap().total_dispatched, 2, "{mode:?}");
        assert_states(&engine, 2, &[RunState::Completed, RunState::Completed]);
        assert_states(&engine, 3, &[RunState::Completed]);
        engine.shutdown().unwrap();
    }
}
