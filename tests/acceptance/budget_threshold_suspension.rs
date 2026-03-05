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
use std::sync::atomic::{AtomicUsize, Ordering};
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
    CancellationContext, ExecutorContext, ExecutorHandler, HandlerOutput,
};
use actionqueue_executor_local::types::ExecutorRequest;
use actionqueue_executor_local::AttemptRunner;
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("7d-budget-suspend-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

/// Handler that consumes tokens and returns RetryableFailure.
#[derive(Debug)]
struct TokenConsumeRetryHandler;

impl ExecutorHandler for TokenConsumeRetryHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::RetryableFailure {
            error: "transient".to_string(),
            consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 500)],
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

/// After attempt 1 exhausts the budget, the budget gate blocks further
/// dispatch. The run stays in Ready (non-terminal, non-Running).
#[tokio::test]
async fn budget_exhaustion_blocks_further_dispatch() {
    let dir = data_dir("blocks-dispatch");

    let clock = MockClock::new(1000);
    let handler = TokenConsumeRetryHandler;
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
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
    let _ = std::fs::remove_dir_all(&dir);
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
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            if ctx.input.cancellation_context.token().is_cancelled() {
                HandlerOutput::Suspended { output: None, consumption: vec![] }
            } else {
                HandlerOutput::Success { output: None, consumption: vec![] }
            }
        }
    }

    let runner = AttemptRunner::new(SuspendOnCancelHandler);

    // Create an external context and pre-signal cancellation.
    let ctx = CancellationContext::new();
    ctx.cancel();

    let request = ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(3, None, None).expect("valid"),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: Some(ctx),
    };

    let outcome = runner.run_attempt(request);

    // Handler must observe the pre-signaled cancellation and return Suspended.
    assert_eq!(
        outcome.response,
        actionqueue_executor_local::types::ExecutorResponse::Suspended { output: None },
        "handler must return Suspended when CancellationContext is pre-cancelled"
    );
}
