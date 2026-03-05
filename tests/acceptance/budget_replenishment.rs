//! 7B Budget-replenishment acceptance proof.
//!
//! Verifies that after exhausting a budget, replenishing it allows the dispatch
//! loop to resume dispatching the blocked task, which then completes.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
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

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("7b-budget-replenish-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir should be creatable");
    dir
}

/// First two invocations: retryable failure consuming 500 tokens each.
/// All subsequent invocations: success with no consumption.
#[derive(Debug)]
struct ExhaustThenSucceedHandler {
    call_count: Arc<AtomicUsize>,
}

impl ExecutorHandler for ExhaustThenSucceedHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            HandlerOutput::RetryableFailure {
                error: "consuming budget".to_string(),
                consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 500)],
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
        dispatch_concurrency: NonZeroUsize::new(1).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

/// Exhaust a 1000-token budget (2 × 500). Verify run is blocked. Then replenish
/// with a fresh 2000-token budget. Verify the run completes.
#[tokio::test]
async fn budget_replenishment_unblocks_dispatch() {
    let dir = data_dir("unblocks");

    let clock = MockClock::new(1000);
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler = ExhaustThenSucceedHandler { call_count: Arc::clone(&call_count) };
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let task_id = TaskId::new();
    let constraints = TaskConstraints::new(10, None, None).expect("valid constraints");
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"replenish-test".to_vec()),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec).expect("submit");

    // Allocate 1000 tokens — exactly 2 attempts before exhaustion.
    boot.allocate_budget(task_id, BudgetDimension::Token, 1000).expect("allocate budget");

    // Phase 1: run until idle — dispatches twice, budget exhausted, stuck in RetryWait.
    let _phase1 = boot.run_until_idle().await.expect("phase 1 idle");

    let run_ids = boot.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1);
    let state_after_exhaust = boot.projection().get_run_state(&run_ids[0]).expect("run state");
    // With zero-backoff, RetryWait → Ready is immediate; the run may be in
    // either state when the budget gate blocks further dispatch.
    assert!(
        *state_after_exhaust == RunState::RetryWait || *state_after_exhaust == RunState::Ready,
        "run must be in RetryWait or Ready after budget exhaustion (got {state_after_exhaust:?})"
    );
    assert_ne!(
        *state_after_exhaust,
        RunState::Completed,
        "run must NOT be Completed after budget exhaustion"
    );

    let budget_after =
        boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
    assert!(budget_after.exhausted, "budget must be exhausted after phase 1");

    // Phase 2: replenish with a fresh 2000-token budget.
    boot.replenish_budget(task_id, BudgetDimension::Token, 2000).expect("replenish budget");

    // Budget gate is now lifted; verify in-memory state cleared.
    let budget_replenished =
        boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
    assert!(!budget_replenished.exhausted, "budget must not be exhausted after replenishment");
    assert_eq!(budget_replenished.consumed, 0, "consumed must be reset to 0 after replenishment");

    // Phase 3: run until idle — budget gate lifted, run promoted and completes.
    let _phase3 = boot.run_until_idle().await.expect("phase 3 idle");

    let final_state = boot.projection().get_run_state(&run_ids[0]).expect("final state");
    assert_eq!(*final_state, RunState::Completed, "run must complete after budget replenishment");
    assert!(
        call_count.load(Ordering::SeqCst) >= 3,
        "handler must have been called at least 3 times (2 exhausting + 1 completing)"
    );

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
