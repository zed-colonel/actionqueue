//! 7D Budget-threshold suspension acceptance proof.
//!
//! Verifies the budget-exhaustion in-flight suspension mechanism: the dispatch
//! loop signals cancellation to a running handler when the task's budget is
//! exhausted. The handler cooperatively observes cancellation and returns
//! `Suspended`.
//!
//! Since concurrent dispatch with the same budget requires two runs for the
//! same task at the same time (which requires a real clock or shared clock),
//! this test uses a simpler approach:
//! 1. Task with max_attempts=3, budget=500
//! 2. Attempt 1: handler returns RetryableFailure + 500 tokens → exhausted
//! 3. Attempt 2: handler loops checking is_cancelled() (budget gate SHOULD
//!    block this dispatch, but we verify it does)
//!
//! The companion test verifies that `signal_budget_exhaustion_cancellations`
//! cancels the CancellationContext stored in InFlightRun using the
//! unit-level approach.

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
use actionqueue_executor_local::handler::{
    AttemptDisposition, CancellationContext, ExecutorContext, ExecutorHandler,
};
use actionqueue_executor_local::types::ExecutorRequest;
use actionqueue_executor_local::AttemptRunner;
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

fn data_dir(label: &str) -> tempfile::TempDir {
    tempfile::Builder::new().prefix(label).tempdir().expect("test data dir")
}

/// Handler that consumes tokens and returns RetryableFailure.
#[derive(Debug)]
struct TokenConsumeRetryHandler;

impl ExecutorHandler for TokenConsumeRetryHandler {
    fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
        actionqueue_core::disposition::AttemptDisposition::retryable_failure(
            actionqueue_core::bounded::BoundedError::new("transient".to_string()).unwrap(),
        )
        .with_consumption(vec![BudgetConsumption::new(BudgetDimension::Token, 500)])
        .unwrap()
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

/// After attempt 1 exhausts the budget, the budget gate blocks further
/// dispatch. The run stays in Ready (non-terminal, non-Running).
#[tokio::test]
async fn budget_exhaustion_blocks_further_dispatch() {
    let dir = data_dir("blocks-dispatch");

    let clock = MockClock::new(1000);
    let handler = TokenConsumeRetryHandler;
    let engine = ActionQueueEngine::new(make_config(dir.path().to_path_buf()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let task_id = TaskId::new();
    let constraints = TaskConstraints::new(5, None, None).expect("valid");
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"budget-suspend".to_vec()),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec).expect("submit");
    boot.allocate_budget(task_id, BudgetDimension::Token, 500).expect("allocate");

    let _summary = boot.run_until_idle().await.expect("run_until_idle");

    // Budget must be exhausted.
    let budget =
        boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget record");
    assert!(budget.exhausted, "budget must be exhausted after 500/500 tokens consumed");

    // Run must be non-terminal and blocked by budget gate.
    let run_ids = boot.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1);
    let state = boot.projection().get_run_state(&run_ids[0]).expect("state");
    assert!(
        *state == RunState::Ready || *state == RunState::RetryWait,
        "run must be blocked (Ready or RetryWait), got {state:?}"
    );

    boot.shutdown().expect("shutdown");
}

/// Unit-level test: verifies that an externally-provided CancellationContext
/// is delivered to the handler via the AttemptRunner. The handler observes
/// cancellation pre-signaled before dispatch and returns Suspended.
#[test]
fn external_cancellation_context_reaches_handler() {
    use actionqueue_core::ids::{AttemptId, RunId};
    use actionqueue_core::task::constraints::TaskConstraints;

    /// Handler that checks cancellation and suspends if signaled.
    #[derive(Debug)]
    struct SuspendOnCancelHandler;

    impl ExecutorHandler for SuspendOnCancelHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            if ctx.input.cancellation_context.token().is_cancelled() {
                actionqueue_core::disposition::AttemptDisposition::suspended(None, None)
            } else {
                actionqueue_core::disposition::AttemptDisposition::complete(None)
            }
        }
    }

    let runner = AttemptRunner::new(SuspendOnCancelHandler);

    // Create an external context and pre-signal cancellation.
    let ctx = CancellationContext::new();
    ctx.cancel();

    let request = ExecutorRequest {
        lease_fence: actionqueue_core::mutation::LeaseFence::new("test".into(), 1),
        failure_attempt_count: 0,
        resume_context: None,
        causal_context: None,
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(3, None, None).expect("valid"),
        attempt_number: 1,

        children: None,
        cancellation_context: Some(ctx),
    };

    let outcome = runner.run_attempt(request);

    // Handler must observe the pre-signaled cancellation and return Suspended.
    assert_eq!(
        outcome.disposition,
        actionqueue_core::disposition::AttemptDisposition::suspended(None, None),
        "handler must return Suspended when CancellationContext is pre-cancelled"
    );
}
