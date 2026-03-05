//! P6-008 Concurrent dispatch stress test.
//!
//! Verifies that the dispatch loop handles concurrent execution under load:
//! 1. Submit 100+ tasks (Once policy) with a handler that sleeps 1-10ms
//! 2. Use 4 concurrent workers
//! 3. Run until idle
//! 4. Verify all tasks completed (all runs in terminal state)
//! 5. Verify no state violations (no runs stuck in Running/Leased)

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static STRESS_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn stress_data_dir(label: &str) -> PathBuf {
    let count = STRESS_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("p6-008-stress-{label}-{}-{count}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("stress data dir should be creatable");
    dir
}

/// A handler that sleeps for a pseudo-random duration (1-10ms) then succeeds.
/// Uses AtomicU64 as a simple PRNG seeded from the run_id hash to vary sleep times
/// across invocations without requiring external randomness.
#[derive(Debug)]
struct RandomSleepHandler {
    invocation_counter: AtomicU64,
}

impl RandomSleepHandler {
    fn new() -> Self {
        Self { invocation_counter: AtomicU64::new(0) }
    }
}

impl ExecutorHandler for RandomSleepHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        // Simple deterministic variation: use invocation counter to vary sleep.
        let n = self.invocation_counter.fetch_add(1, Ordering::Relaxed);
        let sleep_ms = 1 + (n % 10); // 1-10ms
        std::thread::sleep(Duration::from_millis(sleep_ms));
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

/// A handler that always succeeds without sleeping (for baseline comparison).
#[derive(Debug)]
struct InstantHandler;

impl ExecutorHandler for InstantHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

/// Builds the standard stress test RuntimeConfig.
/// Uses zero backoff so retries are immediately re-eligible with MockClock
/// (since MockClock is frozen, any non-zero backoff would never elapse).
fn stress_config(data_dir: PathBuf, concurrency: usize) -> RuntimeConfig {
    RuntimeConfig {
        data_dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(concurrency).expect("concurrency > 0"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(10),
        ..RuntimeConfig::default()
    }
}

/// Core stress test: 100 Once tasks, 4 concurrent workers, random sleep handler.
#[tokio::test]
async fn stress_100_tasks_4_workers_random_sleep() {
    let num_tasks = 100usize;
    let concurrency = 4usize;
    let data_dir = stress_data_dir("100-tasks-4w");
    let config = stress_config(data_dir.clone(), concurrency);

    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, RandomSleepHandler::new());
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    // Submit 100 tasks.
    let mut task_ids = Vec::with_capacity(num_tasks);
    for _ in 0..num_tasks {
        let task_id = TaskId::new();
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"stress-payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(), // max_attempts=1
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit should succeed");
        task_ids.push(task_id);
    }

    // Verify all tasks and runs were created.
    assert_eq!(boot.projection().task_count(), num_tasks);
    assert_eq!(boot.projection().run_count(), num_tasks);

    // Run until idle.
    let summary = boot.run_until_idle().await.expect("run_until_idle should succeed");

    // Assertion 1: All runs dispatched.
    assert_eq!(
        summary.total_dispatched, num_tasks,
        "all {num_tasks} runs should have been dispatched"
    );

    // Assertion 2: All runs completed.
    assert_eq!(summary.total_completed, num_tasks, "all {num_tasks} runs should have completed");

    // Assertion 3: No runs stuck in non-terminal states.
    let projection = boot.projection();
    let mut completed_count = 0usize;
    let mut stuck_runs = Vec::new();

    for task_id in &task_ids {
        let run_ids = projection.run_ids_for_task(*task_id);
        assert_eq!(run_ids.len(), 1, "each Once task should have exactly 1 run");

        let run_state =
            projection.get_run_state(&run_ids[0]).expect("run should exist in projection");

        if *run_state == RunState::Completed {
            completed_count += 1;
        } else {
            stuck_runs.push((run_ids[0], *run_state));
        }
    }

    assert_eq!(completed_count, num_tasks, "all runs should be in Completed state");
    assert!(stuck_runs.is_empty(), "no runs should be stuck: {stuck_runs:?}");

    // Assertion 4: No runs in Running or Leased state — all must be terminal.
    for run in projection.run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal after idle, but is {:?}",
            run.id(),
            run.state()
        );
        assert_ne!(
            run.state(),
            RunState::Running,
            "run {} should not be stuck in Running",
            run.id()
        );
        assert_ne!(run.state(), RunState::Leased, "run {} should not be stuck in Leased", run.id());
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Stress test with higher task count: 200 tasks, 4 workers, instant handler.
#[tokio::test]
async fn stress_200_tasks_4_workers_instant() {
    let num_tasks = 200usize;
    let concurrency = 4usize;
    let data_dir = stress_data_dir("200-tasks-4w-instant");
    let config = stress_config(data_dir.clone(), concurrency);

    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, InstantHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    for _ in 0..num_tasks {
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"fast-payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");
    }

    let summary = boot.run_until_idle().await.expect("run_until_idle");

    assert_eq!(summary.total_dispatched, num_tasks);
    assert_eq!(summary.total_completed, num_tasks);

    // All runs in terminal state.
    for run in boot.projection().run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal, but is {:?}",
            run.id(),
            run.state()
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Stress test with single worker: serialized execution of 100 tasks.
#[tokio::test]
async fn stress_100_tasks_1_worker_serialized() {
    let num_tasks = 100usize;
    let concurrency = 1usize;
    let data_dir = stress_data_dir("100-tasks-1w");
    let config = stress_config(data_dir.clone(), concurrency);

    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, RandomSleepHandler::new());
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    for _ in 0..num_tasks {
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"serial-payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");
    }

    let summary = boot.run_until_idle().await.expect("run_until_idle");

    assert_eq!(summary.total_dispatched, num_tasks);
    assert_eq!(summary.total_completed, num_tasks);

    for run in boot.projection().run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal, but is {:?}",
            run.id(),
            run.state()
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Stress test with high concurrency: 150 tasks, 8 workers.
#[tokio::test]
async fn stress_150_tasks_8_workers() {
    let num_tasks = 150usize;
    let concurrency = 8usize;
    let data_dir = stress_data_dir("150-tasks-8w");
    let config = stress_config(data_dir.clone(), concurrency);

    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, RandomSleepHandler::new());
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    for _ in 0..num_tasks {
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"high-concurrency-payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");
    }

    let summary = boot.run_until_idle().await.expect("run_until_idle");

    assert_eq!(summary.total_dispatched, num_tasks);
    assert_eq!(summary.total_completed, num_tasks);

    // Verify no state violations — all runs must be terminal.
    let mut running_count = 0usize;
    let mut leased_count = 0usize;
    for run in boot.projection().run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal after idle, but is {:?}",
            run.id(),
            run.state()
        );
        match run.state() {
            RunState::Running => running_count += 1,
            RunState::Leased => leased_count += 1,
            _ => {}
        }
    }
    assert_eq!(running_count, 0, "no runs should remain in Running after idle");
    assert_eq!(leased_count, 0, "no runs should remain in Leased after idle");

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Stress test with mixed outcomes: 100 tasks, 4 workers.
/// Half succeed, half terminally fail. All should reach terminal state.
#[tokio::test]
async fn stress_100_tasks_mixed_outcomes() {
    let num_tasks = 100usize;
    let concurrency = 4usize;
    let data_dir = stress_data_dir("100-tasks-mixed");
    let config = stress_config(data_dir.clone(), concurrency);

    let clock = MockClock::new(1_000_000);

    /// A handler that succeeds for even-indexed invocations and terminally fails for odd.
    #[derive(Debug)]
    struct MixedOutcomeHandler {
        counter: AtomicUsize,
    }

    impl ExecutorHandler for MixedOutcomeHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let _input = ctx.input;
            let n = self.counter.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
            if n % 2 == 0 {
                HandlerOutput::Success { output: None, consumption: vec![] }
            } else {
                HandlerOutput::TerminalFailure {
                    error: "terminal failure".to_string(),
                    consumption: vec![],
                }
            }
        }
    }

    let handler = MixedOutcomeHandler { counter: AtomicUsize::new(0) };
    let engine = ActionQueueEngine::new(config, handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    for _ in 0..num_tasks {
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"mixed-stress-payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(), // max_attempts=1
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");
    }

    let summary = boot.run_until_idle().await.expect("run_until_idle");

    // All tasks dispatched exactly once.
    assert_eq!(summary.total_dispatched, num_tasks, "all tasks should be dispatched exactly once");

    // All runs should have reached terminal (either Completed or Failed).
    assert_eq!(summary.total_completed, num_tasks, "all tasks should reach terminal state");

    // No state violations.
    for run in boot.projection().run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal after idle, but is {:?}",
            run.id(),
            run.state()
        );
        assert_ne!(
            run.state(),
            RunState::Running,
            "run {} should not be stuck in Running",
            run.id()
        );
        assert_ne!(run.state(), RunState::Leased, "run {} should not be stuck in Leased", run.id());
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}
