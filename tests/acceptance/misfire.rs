//! P6-006 Misfire acceptance test suite.
//!
//! Verifies that when time passes without ticking the scheduler, the system
//! handles catch-up correctly. When a Repeat(5, interval=1s) task has all its
//! runs past-due (clock advanced 10x the interval without ticking), a single
//! tick must promote all due runs to Ready.
//!
//! This documents the current misfire policy: **eager catch-up**. All runs
//! whose `scheduled_at` has passed are promoted in the next tick, regardless
//! of how long they have been overdue. No coalescing or skip-ahead occurs.

mod support;

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
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

static MISFIRE_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn misfire_data_dir(label: &str) -> PathBuf {
    let count = MISFIRE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("p6-006-misfire-{label}-{}-{count}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("misfire data dir should be creatable");
    dir
}

/// A handler that always succeeds instantly.
#[derive(Debug)]
struct InstantSuccessHandler;

impl ExecutorHandler for InstantSuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

/// Proves that after advancing the clock 10x the interval without ticking,
/// a single tick promotes all 5 due runs (eager catch-up misfire policy).
#[tokio::test]
async fn misfire_eager_catchup_promotes_all_overdue_runs_in_single_tick() {
    let data_dir = misfire_data_dir("eager-catchup");
    let interval_secs = 1;
    let count = 5u32;

    // Start clock at t=1000. Repeat(5, 1s) will schedule runs at:
    // t=1000, t=1001, t=1002, t=1003, t=1004
    let clock = MockClock::new(1000);

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine = ActionQueueEngine::new(config, InstantSuccessHandler);
    let mut bootstrapped = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    // Submit a Repeat(5, 1s) task.
    let task_id = TaskId::new();
    let run_policy =
        RunPolicy::repeat(count, interval_secs).expect("repeat policy should be valid");
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"misfire-test-payload".to_vec()),
        run_policy,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid");

    bootstrapped.submit_task(spec).expect("submit should succeed");

    // Verify: 5 runs created, all in Scheduled state.
    let run_ids = bootstrapped.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 5, "Repeat(5, 1s) must derive exactly 5 runs");

    for run_id in &run_ids {
        assert_eq!(
            bootstrapped.projection().get_run_state(run_id),
            Some(&RunState::Scheduled),
            "all runs should start in Scheduled state"
        );
    }

    // Verify scheduled_at times: t=1000, t=1001, t=1002, t=1003, t=1004.
    let mut scheduled_times: Vec<u64> = run_ids
        .iter()
        .map(|id| {
            bootstrapped.projection().get_run_instance(id).expect("run should exist").scheduled_at()
        })
        .collect();
    scheduled_times.sort();
    assert_eq!(
        scheduled_times,
        vec![1000, 1001, 1002, 1003, 1004],
        "scheduled_at times should span the repeat interval from origin"
    );

    // DO NOT tick yet. Advance the clock 10x the interval (10 seconds)
    // without any intermediate ticks. This simulates a misfire scenario.
    // Note: MockClock is not &mut here since the engine owns it. We need
    // to create a new engine with the advanced clock to demonstrate the
    // misfire behavior. Instead, we use the authority-level approach.
    //
    // Actually, the BootstrappedEngine owns the clock inside the DispatchLoop.
    // We cannot mutate it directly. The approach is: use run_until_idle which
    // will tick immediately. Since the clock starts at t=1000 and the first
    // run is scheduled_at=1000, a tick at t=1000 should promote run #1
    // immediately. But we want to test the misfire scenario where ALL runs
    // are overdue.
    //
    // Re-approach: create the engine with clock=1010 (10x interval after origin).
    // All 5 runs will have scheduled_at <= 1010, so a single tick should promote
    // all of them.
    drop(bootstrapped);

    // Re-bootstrap with clock at t=1010 (all runs are overdue).
    let advanced_clock = MockClock::new(1010);
    let config2 = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine2 = ActionQueueEngine::new(config2, InstantSuccessHandler);
    let mut bootstrapped2 =
        engine2.bootstrap_with_clock(advanced_clock).expect("re-bootstrap should succeed");

    // Verify runs are still Scheduled (no tick has been issued).
    for run_id in &run_ids {
        assert_eq!(
            bootstrapped2.projection().get_run_state(run_id),
            Some(&RunState::Scheduled),
            "runs should still be Scheduled before tick at t=1010"
        );
    }

    // One tick at t=1010 should promote ALL 5 overdue runs (eager catch-up).
    let tick_result = bootstrapped2.tick().await.expect("tick should succeed");
    assert_eq!(
        tick_result.promoted_scheduled, 5,
        "a single tick at t=1010 must promote all 5 overdue runs (eager catch-up misfire policy)"
    );

    // After tick+dispatch, verify runs have advanced beyond Scheduled.
    for run_id in &run_ids {
        let state = bootstrapped2
            .projection()
            .get_run_state(run_id)
            .expect("run should exist in projection");
        assert_ne!(
            *state,
            RunState::Scheduled,
            "no run should remain Scheduled after catch-up promotion"
        );
    }

    // Run until idle to complete everything.
    let summary = bootstrapped2.run_until_idle().await.expect("run_until_idle should succeed");
    assert!(
        summary.total_dispatched > 0 || tick_result.dispatched > 0,
        "at least some runs should have been dispatched"
    );

    // Verify all runs reached terminal Completed state.
    for run_id in &run_ids {
        assert_eq!(
            bootstrapped2.projection().get_run_state(run_id),
            Some(&RunState::Completed),
            "all runs should reach Completed after eager catch-up"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves that partial catch-up works: if clock is between scheduled times,
/// only the overdue runs are promoted.
#[tokio::test]
async fn misfire_partial_catchup_promotes_only_overdue_runs() {
    let data_dir = misfire_data_dir("partial-catchup");
    let interval_secs = 10;
    let count = 5u32;

    // Phase 1: Submit at t=1000. Repeat(5, 10s) schedules at:
    // t=1000, t=1010, t=1020, t=1030, t=1040
    let submit_clock = MockClock::new(1000);
    let config1 = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine1 = ActionQueueEngine::new(config1, InstantSuccessHandler);
    let mut boot1 = engine1.bootstrap_with_clock(submit_clock).expect("bootstrap should succeed");

    let task_id = TaskId::new();
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"partial-misfire-payload".to_vec()),
        RunPolicy::repeat(count, interval_secs).expect("repeat policy should be valid"),
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid");

    boot1.submit_task(spec).expect("submit should succeed");

    // Verify 5 runs were created with expected scheduled times.
    let run_ids = boot1.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 5);
    drop(boot1);

    // Phase 2: Re-bootstrap at t=1025. Runs at t=1000, 1010, 1020 are due,
    // while t=1030, 1040 are still future.
    let partial_clock = MockClock::new(1025);
    let config2 = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine2 = ActionQueueEngine::new(config2, InstantSuccessHandler);
    let mut boot2 =
        engine2.bootstrap_with_clock(partial_clock).expect("re-bootstrap should succeed");

    // Tick at t=1025: scheduled_at <= 1025 means t=1000, 1010, 1020 are due.
    let tick = boot2.tick().await.expect("tick should succeed");
    assert_eq!(
        tick.promoted_scheduled, 3,
        "only runs at t=1000, t=1010, t=1020 should be promoted at t=1025"
    );

    // Verify: 3 runs promoted, 2 still Scheduled.
    let scheduled_count = run_ids
        .iter()
        .filter(|id| boot2.projection().get_run_state(id) == Some(&RunState::Scheduled))
        .count();
    assert_eq!(scheduled_count, 2, "2 runs should still be Scheduled (t=1030, t=1040)");

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves that a very large misfire gap (100x interval) still promotes all runs.
/// This documents that there is no coalescing or skip-ahead limit.
#[tokio::test]
async fn misfire_no_coalescing_with_extreme_gap() {
    let data_dir = misfire_data_dir("extreme-gap");
    let interval_secs = 1;
    let count = 5u32;

    // Submit at t=1000, then bootstrap at t=1100 (100x interval).
    let submit_clock = MockClock::new(1000);
    let config1 = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine1 = ActionQueueEngine::new(config1, InstantSuccessHandler);
    let mut boot1 = engine1.bootstrap_with_clock(submit_clock).expect("bootstrap should succeed");

    let task_id = TaskId::new();
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"extreme-gap-payload".to_vec()),
        RunPolicy::repeat(count, interval_secs).expect("repeat policy should be valid"),
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid");
    boot1.submit_task(spec).expect("submit should succeed");
    drop(boot1);

    // Re-bootstrap at t=1100 (100 seconds later, all runs extremely overdue).
    let extreme_clock = MockClock::new(1100);
    let config2 = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(10) },
        dispatch_concurrency: NonZeroUsize::new(8).expect("8 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(50),
        ..RuntimeConfig::default()
    };

    let engine2 = ActionQueueEngine::new(config2, InstantSuccessHandler);
    let mut boot2 =
        engine2.bootstrap_with_clock(extreme_clock).expect("re-bootstrap should succeed");

    let tick = boot2.tick().await.expect("tick should succeed");
    assert_eq!(
        tick.promoted_scheduled, 5,
        "all 5 runs must be promoted even with 100x interval gap (no coalescing)"
    );

    // Run until idle to complete.
    let summary = boot2.run_until_idle().await.expect("run_until_idle should succeed");
    let total = summary.total_dispatched + tick.dispatched;
    assert!(total > 0, "runs should have been dispatched");

    let run_ids = boot2.projection().run_ids_for_task(task_id);
    for run_id in &run_ids {
        assert_eq!(
            boot2.projection().get_run_state(run_id),
            Some(&RunState::Completed),
            "all runs should reach Completed after extreme catch-up"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves that authority-level misfire catch-up behavior matches the promotion
/// predicate: `scheduled_at <= current_time`.
#[test]
fn misfire_authority_level_promotion_predicate_documentation() {
    let data_dir = support::unique_data_dir("p6-006-authority-predicate");
    let task_id_str = "d6d60006-0001-0001-0001-000000000001";

    // Submit a Repeat(5, 1s) task via CLI.
    let submit = support::submit_repeat_task_via_cli(task_id_str, &data_dir, 5, 1);
    assert_eq!(submit["runs_created"], 5);

    let task_id =
        actionqueue_core::ids::TaskId::from_str(task_id_str).expect("fixed task id should parse");
    let run_ids = support::task_run_ids_from_storage(&data_dir, task_id);
    assert_eq!(run_ids.len(), 5);

    // Build scheduled index with all 5 runs.
    let recovery =
        actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&data_dir)
            .expect("storage bootstrap should succeed");
    let runs: Vec<actionqueue_core::run::run_instance::RunInstance> =
        recovery.projection.run_instances().cloned().collect();
    let scheduled_index =
        actionqueue_engine::index::scheduled::ScheduledIndex::from(runs.as_slice());
    assert_eq!(scheduled_index.len(), 5);

    // At t=far_future, all should be ready for promotion.
    let far_future = u64::MAX / 2;
    let ready = scheduled_index.ready_for_promotion(far_future);
    assert_eq!(ready.len(), 5, "all 5 runs should be ready_for_promotion at far-future time");

    // At t=0, none should be ready (all scheduled in the future relative to t=0).
    let none_ready = scheduled_index.ready_for_promotion(0);
    assert_eq!(none_ready.len(), 0, "no runs should be ready_for_promotion at t=0");
}
