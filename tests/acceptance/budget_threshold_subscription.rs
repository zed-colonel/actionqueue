//! 7F Budget-threshold subscription acceptance proof.
//!
//! Verifies that a budget threshold crossing fires an event that triggers a
//! subscription, which promotes a waiting task.
//!
//! Flow:
//! 1. Task A: Repeat(2, 10000) — run 1 at now, run 2 far in the future.
//! 2. Task A subscribes to a BudgetThreshold event on Task B (80% of Token).
//! 3. Task B runs and reports 400 tokens out of a 500 budget (80%).
//! 4. Budget threshold event fires → subscription triggered → run 2 promoted.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
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
        .join(format!("7f-budget-thresh-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

/// Handler: Task B reports 400 token consumption; everything else succeeds.
#[derive(Debug)]
struct ThresholdHandler;

impl ExecutorHandler for ThresholdHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        // Look up the task_id from the run's payload to decide behavior.
        if ctx.input.payload == b"task-b" {
            HandlerOutput::Success {
                output: None,
                consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 400)],
            }
        } else {
            HandlerOutput::Success { output: None, consumption: vec![] }
        }
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

/// Budget consumption crossing threshold fires subscription → promotes task.
#[tokio::test]
async fn budget_threshold_triggers_subscription_promotion() {
    let dir = data_dir("thresh-promo");

    let clock = MockClock::new(1000);
    let task_b_id = TaskId::new();
    let handler = ThresholdHandler;
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // Task B: Once, budget 500 tokens.
    let spec_b = TaskSpec::new(
        task_b_id,
        TaskPayload::new(b"task-b".to_vec()),
        RunPolicy::Once,
        TaskConstraints::new(1, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec_b).expect("submit B");
    boot.allocate_budget(task_b_id, BudgetDimension::Token, 500).expect("allocate B");

    // Task A: Repeat(2, 10000). Run 1 at now, run 2 at t=11000.
    let task_a_id = TaskId::new();
    let repeat_a = RunPolicy::repeat(2, 10000).expect("valid repeat");
    let spec_a = TaskSpec::new(
        task_a_id,
        TaskPayload::new(b"task-a".to_vec()),
        repeat_a,
        TaskConstraints::new(1, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec_a).expect("submit A");

    // Task A subscribes to Task B's 80% token budget threshold.
    let _sub_id = boot
        .create_subscription(
            task_a_id,
            EventFilter::BudgetThreshold {
                task_id: task_b_id,
                dimension: BudgetDimension::Token,
                threshold_pct: 80,
            },
        )
        .expect("create subscription");

    // Run until idle.
    let _summary = boot.run_until_idle().await.expect("run_until_idle");

    // Task B must be Completed.
    let run_ids_b = boot.projection().run_ids_for_task(task_b_id);
    let state_b = boot.projection().get_run_state(&run_ids_b[0]).expect("state B");
    assert_eq!(*state_b, RunState::Completed, "task B must complete");

    // Budget should reflect 400 consumed of 500.
    let budget =
        boot.projection().get_budget(&task_b_id, BudgetDimension::Token).expect("budget record");
    assert_eq!(budget.consumed, 400);

    // Both runs of Task A must be Completed (run 2 promoted by threshold).
    let run_ids_a = boot.projection().run_ids_for_task(task_a_id);
    assert_eq!(run_ids_a.len(), 2);
    for rid in &run_ids_a {
        let state = boot.projection().get_run_state(rid).expect("state A");
        assert_eq!(
            *state,
            RunState::Completed,
            "task A run {rid} must be Completed via budget threshold subscription"
        );
    }

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
