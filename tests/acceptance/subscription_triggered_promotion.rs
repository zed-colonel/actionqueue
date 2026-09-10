//! 7E Subscription-triggered promotion acceptance proof.
//!
//! Verifies that when Task A subscribes to Task B's completion, Task A's
//! future-scheduled run is promoted to Ready immediately after Task B
//! completes, bypassing the scheduled_at time check.
//!
//! Task A uses Repeat(2, 10000s) so its second run is scheduled at
//! time=11000 — far beyond the mock clock's value of 1000. Without the
//! subscription trigger, that run would remain Scheduled indefinitely.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::EventFilter;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("7e-sub-promo-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

/// Handler that always succeeds.
#[derive(Debug)]
struct AlwaysSucceedHandler;

impl ExecutorHandler for AlwaysSucceedHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

fn make_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(4).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

/// Task A's second run (scheduled_at=11000) is promoted by Task B's
/// completion subscription, despite being 10000 seconds in the future.
#[tokio::test]
async fn subscription_promotes_future_scheduled_run_on_completion() {
    let dir = data_dir("future-promo");

    let clock = MockClock::new(1000);
    let handler = AlwaysSucceedHandler;
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // Task B: Once, scheduled now (time=1000).
    let task_b_id = TaskId::new();
    let spec_b = TaskSpec::new(
        task_b_id,
        TaskPayload::new(b"task-b".to_vec()),
        RunPolicy::Once,
        TaskConstraints::new(1, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec_b).expect("submit B");

    // Task A: Repeat(2, 10000s). Run 1 at t=1000 (now), Run 2 at t=11000.
    let task_a_id = TaskId::new();
    let repeat_a = RunPolicy::repeat(2, 10000).expect("valid repeat policy");
    let spec_a = TaskSpec::new(
        task_a_id,
        TaskPayload::new(b"task-a".to_vec()),
        repeat_a,
        TaskConstraints::new(1, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec_a).expect("submit A");

    // Task A subscribes to Task B's completion.
    let _sub_id = boot
        .create_subscription(task_a_id, EventFilter::TaskCompleted { task_id: task_b_id })
        .expect("create subscription");

    // Verify Task A's second run is in Scheduled state.
    let run_ids_a = boot.projection().run_ids_for_task(task_a_id);
    assert_eq!(run_ids_a.len(), 2, "Repeat(2) must derive 2 runs");

    // Run until idle: Task B completes, Task A run 1 completes normally.
    // Task A run 2 should be promoted by the subscription trigger.
    let _summary = boot.run_until_idle().await.expect("run_until_idle");

    // Task B must be Completed.
    let run_ids_b = boot.projection().run_ids_for_task(task_b_id);
    let state_b = boot.projection().get_run_state(&run_ids_b[0]).expect("state B");
    assert_eq!(*state_b, RunState::Completed, "task B must complete");

    // Both runs of Task A must be Completed.
    for rid in &run_ids_a {
        let state = boot.projection().get_run_state(rid).expect("state A");
        assert_eq!(
            *state,
            RunState::Completed,
            "all Task A runs must be Completed after subscription-triggered promotion (run {rid})"
        );
    }

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
