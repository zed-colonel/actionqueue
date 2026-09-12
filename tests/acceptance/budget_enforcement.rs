//! 7A Budget-enforcement acceptance proof.
//!
//! Verifies that once a task's token budget is exhausted by handler consumption,
//! the dispatch loop stops dispatching that task. The run stays in RetryWait
//! (non-terminal) rather than being dispatched further.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

fn data_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(label).tempdir().expect("test data dir")
}

/// Handler that always issues a retryable failure and reports token consumption.
#[derive(Debug)]
struct TokenConsumingHandler {
    tokens_per_attempt: u64,
}

impl ExecutorHandler for TokenConsumingHandler {
    fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
        actionqueue_core::disposition::AttemptDisposition::retryable_failure(
            actionqueue_core::bounded::BoundedError::new("always-retry".to_string()).unwrap(),
        )
        .with_consumption(vec![BudgetConsumption::new(
            BudgetDimension::Token,
            self.tokens_per_attempt,
        )])
        .unwrap()
    }
}

/// Zero-delay backoff so RetryWait → Ready promotion is immediate (same clock tick).
fn make_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: dir,
        // Zero interval: delay_secs = 0, retry_ready_at = now → instant re-promotion.
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(1).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

/// After two attempts consuming 500 tokens each, the 1000-token budget is
/// exhausted. The budget gate blocks further dispatch and the run stays in
/// RetryWait (non-terminal).
#[tokio::test]
async fn budget_exhaustion_blocks_dispatch_after_cap_reached() {
    let dir = data_dir("cap-reached");

    let clock = MockClock::new(1000);
    let handler = TokenConsumingHandler { tokens_per_attempt: 500 };
    let engine = ActionQueueEngine::new(make_config(dir.path().to_path_buf()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // max_attempts=5 so budget exhaustion blocks before the retry cap fires.
    let task_id = TaskId::new();
    let constraints = TaskConstraints::new(5, None, None).expect("valid constraints");
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"budget-test".to_vec()),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec).expect("submit");

    // Allocate exactly 1000 tokens — 2 × 500 = exhausted after two attempts.
    boot.allocate_budget(task_id, BudgetDimension::Token, 1000).expect("allocate budget");

    // run_until_idle: with zero backoff, RetryWait → Ready is immediate.
    // After 2 dispatches the budget is exhausted; the budget gate blocks the
    // third promotion and the loop becomes idle with the run in RetryWait.
    let _summary = boot.run_until_idle().await.expect("run_until_idle");

    // Budget record in the projection must show exhaustion.
    let budget = boot
        .projection()
        .get_budget(&task_id, BudgetDimension::Token)
        .expect("budget record must exist");
    assert!(budget.exhausted, "token budget must be marked exhausted after 2 × 500 token attempts");
    assert!(budget.consumed >= 1000, "consumed must be >= 1000 tokens, got {}", budget.consumed);

    // The single run for Once policy must be in a non-terminal, non-Running state.
    // With zero-backoff, RetryWait is immediately promoted to Ready, so the run
    // ends up in Ready state — blocked from dispatch by the budget gate.
    let run_ids = boot.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "Once policy must derive exactly 1 run");
    let state = boot.projection().get_run_state(&run_ids[0]).expect("run state must exist");
    assert!(
        *state == RunState::RetryWait || *state == RunState::Ready,
        "run must be in RetryWait or Ready after budget exhaustion (got {state:?})"
    );
    assert_ne!(*state, RunState::Completed, "run must NOT complete after budget exhaustion");
    assert_ne!(*state, RunState::Failed, "run must NOT fail after budget exhaustion");

    boot.shutdown().expect("shutdown");
}
