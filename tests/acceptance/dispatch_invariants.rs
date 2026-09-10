//! P6-007 Property-based dispatch invariant tests.
//!
//! Uses `proptest` to verify that for random combinations of task parameters,
//! the total number of dispatched runs never exceeds the policy-implied maximum:
//!
//! - For `RunPolicy::Once`: max dispatches = `num_tasks * max_attempts`
//! - For `RunPolicy::Repeat(N, interval)`: max dispatches = `num_tasks * N * max_attempts`
//!
//! These tests exercise the full runtime dispatch loop with randomized inputs
//! to ensure no over-dispatch can occur under any parameter combination.
//!
//! Note: `MockClock` is immutable once owned by `DispatchLoop`, so repeat tasks
//! require a two-phase approach: submit at time T, then re-bootstrap with a
//! far-future clock so all runs are immediately promotable.

mod admission_support;

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;
use proptest::prelude::*;

static PROPTEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn proptest_data_dir(label: &str) -> PathBuf {
    let count = PROPTEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("p6-007-proptest-{label}-{}-{count}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("proptest data dir should be creatable");
    dir
}

/// A handler that always succeeds, simulating perfect execution.
#[derive(Debug)]
struct AlwaysSuccessHandler;

impl ExecutorHandler for AlwaysSuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

/// A handler that always returns a retryable failure, forcing maximum retries.
#[derive(Debug)]
struct AlwaysRetryHandler;

impl ExecutorHandler for AlwaysRetryHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::RetryableFailure { error: "always-retry".to_string(), consumption: vec![] }
    }
}

/// Builds a RuntimeConfig for proptest scenarios with fast backoff.
fn proptest_config(data_dir: &Path) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: data_dir.to_path_buf(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(1) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(10),
        ..RuntimeConfig::default()
    }
}

/// Submit tasks at a given clock time, returning task IDs.
/// The engine is dropped after submission so the WAL is durable.
fn submit_tasks_at_time<H: ExecutorHandler + 'static>(
    data_dir: &Path,
    handler: H,
    clock_time: u64,
    specs: Vec<TaskSpec>,
) {
    let config = proptest_config(data_dir);
    let clock = MockClock::new(clock_time);
    let engine = ActionQueueEngine::new(config, handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    for spec in specs {
        boot.submit_task(spec).expect("submit");
    }
    // drop boot to flush WAL
}

/// Re-bootstrap at a given clock time and run until idle, returning the summary.
async fn run_until_idle_at_time<H: ExecutorHandler + 'static>(
    data_dir: &Path,
    handler: H,
    clock_time: u64,
) -> actionqueue_runtime::dispatch::RunSummary {
    let config = proptest_config(data_dir);
    let clock = MockClock::new(clock_time);
    let engine = ActionQueueEngine::new(config, handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");
    boot.run_until_idle().await.expect("run_until_idle")
}

// ---------------------------------------------------------------------------
// Once policy tests
// ---------------------------------------------------------------------------

/// Verifies dispatch count for Once tasks with the success handler.
/// Total dispatched must never exceed num_tasks (since max_attempts=1 and handler succeeds).
#[tokio::test]
async fn once_tasks_dispatch_count_bounded_by_task_count() {
    for num_tasks in [1, 3, 5, 10, 15] {
        let data_dir = proptest_data_dir(&format!("once-success-{num_tasks}"));
        let config = proptest_config(&data_dir);

        let clock = MockClock::new(1_000_000);
        let engine = ActionQueueEngine::new(config, AlwaysSuccessHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

        for _ in 0..num_tasks {
            let spec = TaskSpec::new(
                TaskId::new(),
                TaskPayload::new(b"payload".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(), // max_attempts=1
                TaskMetadata::default(),
            )
            .expect("valid spec");
            boot.submit_task(spec).expect("submit");
        }

        let summary = boot.run_until_idle().await.expect("run_until_idle");

        // For Once with max_attempts=1 and success handler: each task dispatches exactly once.
        let max_allowed = num_tasks;
        assert!(
            summary.total_dispatched <= max_allowed,
            "Once tasks dispatched {} runs but max allowed is {} (num_tasks={num_tasks})",
            summary.total_dispatched,
            max_allowed
        );

        // Verify all runs completed.
        assert_eq!(summary.total_completed, num_tasks);

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

/// Verifies dispatch count for Once tasks with retries.
/// Total dispatched must never exceed num_tasks * max_attempts.
#[tokio::test]
async fn once_tasks_retry_dispatch_count_bounded() {
    for (num_tasks, max_attempts) in [(1, 3), (3, 2), (5, 5), (2, 1)] {
        let data_dir = proptest_data_dir(&format!("once-retry-{num_tasks}-{max_attempts}"));
        let config = proptest_config(&data_dir);

        let clock = MockClock::new(1_000_000);
        let engine = ActionQueueEngine::new(config, AlwaysRetryHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

        for _ in 0..num_tasks {
            let constraints =
                TaskConstraints::new(max_attempts, None, None).expect("valid constraints");
            let spec = TaskSpec::new(
                TaskId::new(),
                TaskPayload::new(b"payload".to_vec()),
                RunPolicy::Once,
                constraints,
                TaskMetadata::default(),
            )
            .expect("valid spec");
            boot.submit_task(spec).expect("submit");
        }

        let summary = boot.run_until_idle().await.expect("run_until_idle");

        let max_allowed: usize = (num_tasks as usize) * (max_attempts as usize);
        assert!(
            summary.total_dispatched <= max_allowed,
            "Once retry tasks dispatched {} but max allowed is {} (num_tasks={num_tasks}, \
             max_attempts={max_attempts})",
            summary.total_dispatched,
            max_allowed,
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

// ---------------------------------------------------------------------------
// Repeat policy tests (two-phase: submit at T, run at T+large_offset)
// ---------------------------------------------------------------------------

/// Verifies dispatch count for Repeat tasks with the success handler.
/// Total dispatched must never exceed num_tasks * repeat_count.
#[tokio::test]
async fn repeat_tasks_dispatch_count_bounded_by_policy() {
    for (num_tasks, repeat_count) in [(1, 5), (3, 3), (2, 4)] {
        let data_dir = proptest_data_dir(&format!("repeat-success-{num_tasks}-{repeat_count}"));

        // Phase 1: Submit at t=1000.
        let mut specs = Vec::new();
        for _ in 0..num_tasks {
            specs.push(
                TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::new(b"payload".to_vec()),
                    RunPolicy::repeat(repeat_count, 1).expect("valid policy"),
                    TaskConstraints::default(), // max_attempts=1
                    TaskMetadata::default(),
                )
                .expect("valid spec"),
            );
        }
        submit_tasks_at_time(&data_dir, AlwaysSuccessHandler, 1000, specs);

        // Phase 2: Run at t=far_future so all repeat runs are due.
        let summary = run_until_idle_at_time(&data_dir, AlwaysSuccessHandler, 1_000_000).await;

        let max_allowed: usize = (num_tasks as usize) * (repeat_count as usize);
        assert!(
            summary.total_dispatched <= max_allowed,
            "Repeat tasks dispatched {} but max allowed is {} (num_tasks={num_tasks}, \
             repeat_count={repeat_count})",
            summary.total_dispatched,
            max_allowed,
        );

        // All runs should have completed.
        assert_eq!(summary.total_completed, max_allowed);

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

/// Verifies dispatch count for Repeat tasks with retries.
/// Total dispatched must never exceed num_tasks * repeat_count * max_attempts.
#[tokio::test]
async fn repeat_tasks_retry_dispatch_count_bounded() {
    for (num_tasks, repeat_count, max_attempts) in [(1, 3, 2), (2, 2, 3), (1, 5, 1)] {
        let data_dir =
            proptest_data_dir(&format!("repeat-retry-{num_tasks}-{repeat_count}-{max_attempts}"));

        // Phase 1: Submit at t=1000.
        let mut specs = Vec::new();
        for _ in 0..num_tasks {
            let constraints =
                TaskConstraints::new(max_attempts, None, None).expect("valid constraints");
            specs.push(
                TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::new(b"payload".to_vec()),
                    RunPolicy::repeat(repeat_count, 1).expect("valid policy"),
                    constraints,
                    TaskMetadata::default(),
                )
                .expect("valid spec"),
            );
        }
        submit_tasks_at_time(&data_dir, AlwaysRetryHandler, 1000, specs);

        // Phase 2: Run at t=far_future so all repeat runs are due.
        let summary = run_until_idle_at_time(&data_dir, AlwaysRetryHandler, 1_000_000).await;

        let max_allowed: usize =
            (num_tasks as usize) * (repeat_count as usize) * (max_attempts as usize);
        assert!(
            summary.total_dispatched <= max_allowed,
            "Repeat retry tasks dispatched {} but max allowed is {} (num_tasks={num_tasks}, \
             repeat_count={repeat_count}, max_attempts={max_attempts})",
            summary.total_dispatched,
            max_allowed,
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}

// ---------------------------------------------------------------------------
// proptest-based randomized verification
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: for Once policy, dispatch count <= num_tasks * max_attempts.
    #[test]
    fn prop_once_dispatch_never_exceeds_bound(
        num_tasks in 1u32..=10,
        max_attempts in 1u32..=5,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        rt.block_on(async {
            let data_dir = proptest_data_dir(&format!(
                "prop-once-{num_tasks}-{max_attempts}"
            ));
            let config = proptest_config(&data_dir);

            let clock = MockClock::new(1_000_000);
            let engine = ActionQueueEngine::new(config, AlwaysRetryHandler);
            let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

            for _ in 0..num_tasks {
                let constraints = TaskConstraints::new(max_attempts, None, None)
                    .expect("valid constraints");
                let spec = TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::new(b"payload".to_vec()),
                    RunPolicy::Once,
                    constraints,
                    TaskMetadata::default())
                .expect("valid spec");
                boot.submit_task(spec).expect("submit");
            }

            let summary = boot.run_until_idle().await.expect("run_until_idle");
            let max_allowed = (num_tasks as usize) * (max_attempts as usize);

            prop_assert!(
                summary.total_dispatched <= max_allowed,
                "Once: dispatched {} > max {} (num_tasks={}, max_attempts={})",
                summary.total_dispatched,
                max_allowed,
                num_tasks,
                max_attempts,
            );

            let _ = std::fs::remove_dir_all(&data_dir);
            Ok(())
        })?;
    }

    /// Property: for Repeat(N, interval), dispatch count <= num_tasks * N * max_attempts.
    /// Uses two-phase approach to ensure all repeat runs are due at execution time.
    #[test]
    fn prop_repeat_dispatch_never_exceeds_bound(
        num_tasks in 1u32..=5,
        repeat_count in 1u32..=5,
        max_attempts in 1u32..=3,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");

        rt.block_on(async {
            let data_dir = proptest_data_dir(&format!(
                "prop-repeat-{num_tasks}-{repeat_count}-{max_attempts}"
            ));

            // Phase 1: Submit at t=1000.
            let mut specs = Vec::new();
            for _ in 0..num_tasks {
                let constraints = TaskConstraints::new(max_attempts, None, None)
                    .expect("valid constraints");
                specs.push(
                    TaskSpec::new(
                        TaskId::new(),
                        TaskPayload::new(b"payload".to_vec()),
                        RunPolicy::repeat(repeat_count, 1).expect("valid policy"),
                        constraints,
                        TaskMetadata::default())
                    .expect("valid spec"),
                );
            }
            submit_tasks_at_time(&data_dir, AlwaysRetryHandler, 1000, specs);

            // Phase 2: Run at far-future so all repeat runs are due.
            let summary =
                run_until_idle_at_time(&data_dir, AlwaysRetryHandler, 1_000_000).await;
            let max_allowed =
                (num_tasks as usize) * (repeat_count as usize) * (max_attempts as usize);

            prop_assert!(
                summary.total_dispatched <= max_allowed,
                "Repeat: dispatched {} > max {} (num_tasks={}, repeat_count={}, max_attempts={})",
                summary.total_dispatched,
                max_allowed,
                num_tasks,
                repeat_count,
                max_attempts,
            );

            let _ = std::fs::remove_dir_all(&data_dir);
            Ok(())
        })?;
    }
}

/// Verifies that all runs are in terminal states after run_until_idle.
/// Uses two-phase approach for repeat tasks.
#[tokio::test]
async fn all_runs_terminal_after_idle() {
    let data_dir = proptest_data_dir("terminal-after-idle");

    // Phase 1: Submit a mix of Once and Repeat tasks at t=1000.
    let mut specs = Vec::new();
    for _ in 0..5 {
        specs.push(
            TaskSpec::new(
                TaskId::new(),
                TaskPayload::new(b"payload".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec"),
        );
    }
    for _ in 0..3 {
        specs.push(
            TaskSpec::new(
                TaskId::new(),
                TaskPayload::new(b"payload".to_vec()),
                RunPolicy::repeat(3, 1).expect("valid policy"),
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec"),
        );
    }
    submit_tasks_at_time(&data_dir, AlwaysSuccessHandler, 1000, specs);

    // Phase 2: Run at far-future so all repeat runs are also due.
    let config = proptest_config(&data_dir);
    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, AlwaysSuccessHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let _summary = boot.run_until_idle().await.expect("run_until_idle");

    // All runs should be in terminal states (Completed or Failed).
    let projection = boot.projection();
    for run in projection.run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal after idle, but is {:?}",
            run.id(),
            run.state()
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

mod admission_dependencies {
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
    use super::AlwaysSuccessHandler as SuccessHandler;

    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl Clock for TestClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
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

    fn recover(
        engine: Engine,
        mode: Recovery,
        config: &RuntimeConfig,
        clock: &TestClock,
    ) -> Engine {
        if matches!(mode, Recovery::Live) {
            return engine;
        }
        let digest = engine.projection().projection_digest().unwrap();
        engine.shutdown().unwrap();
        if matches!(mode, Recovery::Snapshot) {
            let authority = open(&config.data_dir);
            let snapshot =
                build_snapshot_from_projection(authority.projection(), clock.now()).unwrap();
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
}
