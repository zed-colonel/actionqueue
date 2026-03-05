//! 7H Cascading budget acceptance proof (workflow + budget features).
//!
//! Verifies that a coordinator task's budget governs its lifecycle:
//! 1. Coordinator submits child tasks on first dispatch.
//! 2. Coordinator reports token consumption that exhausts its budget.
//! 3. After suspension, the coordinator cannot be re-dispatched because
//!    the budget gate blocks it — even though children may still be pending.
//! 4. Budget replenishment allows the coordinator to resume and complete.

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
        .join(format!("7h-cascade-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

/// Coordinator handler:
/// - Call 0: submits a child task and returns Suspended (reporting 500 tokens).
/// - Call 1+: returns Success (coordinator completes).
///
/// Child handler always succeeds.
#[derive(Debug)]
struct CascadeHandler {
    parent_id: TaskId,
    call_count: Arc<AtomicUsize>,
}

impl ExecutorHandler for CascadeHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);

        if ctx.input.payload == b"coordinator" && n == 0 {
            // First coordinator dispatch: submit a child task.
            if let Some(ref sub) = ctx.submission {
                let child = TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::new(b"child".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::new(1, None, None).expect("valid"),
                    TaskMetadata::default(),
                )
                .expect("valid spec")
                .with_parent(self.parent_id);
                sub.submit(child, vec![]);
            }
            // Coordinator suspends after consuming 500 tokens (exhausting budget).
            HandlerOutput::Suspended {
                output: None,
                consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 500)],
            }
        } else if ctx.input.payload == b"child" {
            HandlerOutput::Success { output: None, consumption: vec![] }
        } else {
            // Coordinator resumed: succeed.
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

/// Coordinator budget exhaustion blocks its resume; replenishment allows it.
#[tokio::test]
async fn coordinator_budget_exhaustion_blocks_resume_dispatch() {
    let dir = data_dir("coord-block");

    let clock = MockClock::new(1000);
    let parent_id = TaskId::new();
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler = CascadeHandler { parent_id, call_count: Arc::clone(&call_count) };
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // Submit coordinator task with 500-token budget.
    let spec = TaskSpec::new(
        parent_id,
        TaskPayload::new(b"coordinator".to_vec()),
        RunPolicy::Once,
        TaskConstraints::new(3, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec).expect("submit");
    boot.allocate_budget(parent_id, BudgetDimension::Token, 500).expect("allocate");

    // Phase 1: coordinator dispatches → submits child → suspends (500 tokens).
    // Child dispatches → completes. Budget exhausted.
    let _s1 = boot.run_until_idle().await.expect("idle 1");

    // Coordinator must be in Suspended state.
    let runs_coord = boot.projection().run_ids_for_task(parent_id);
    assert_eq!(runs_coord.len(), 1);
    let coord_run_id = runs_coord[0];
    let coord_state = boot.projection().get_run_state(&coord_run_id).expect("state");
    assert_eq!(*coord_state, RunState::Suspended, "coordinator must be Suspended");

    // Budget must be exhausted.
    let budget = boot.projection().get_budget(&parent_id, BudgetDimension::Token).expect("budget");
    assert!(budget.exhausted, "budget must be exhausted");

    // Resume coordinator — but budget is exhausted, so it should stay in Ready
    // (budget gate blocks dispatch).
    boot.resume_run(coord_run_id).expect("resume");
    let _s2 = boot.run_until_idle().await.expect("idle 2");

    let coord_state2 = boot.projection().get_run_state(&coord_run_id).expect("state after resume");
    assert!(
        *coord_state2 == RunState::Ready,
        "coordinator must be stuck in Ready (budget gate blocks dispatch), got {coord_state2:?}"
    );

    // Phase 3: replenish budget → coordinator can dispatch → completes.
    boot.replenish_budget(parent_id, BudgetDimension::Token, 1000).expect("replenish");
    let _s3 = boot.run_until_idle().await.expect("idle 3");

    let coord_state3 = boot.projection().get_run_state(&coord_run_id).expect("final state");
    assert_eq!(
        *coord_state3,
        RunState::Completed,
        "coordinator must complete after budget replenishment"
    );

    // Child must also be Completed.
    let child_tasks: Vec<_> = boot
        .projection()
        .task_records()
        .filter(|tr| tr.task_spec().parent_task_id() == Some(parent_id))
        .collect();
    assert_eq!(child_tasks.len(), 1, "one child must exist");
    let child_id = child_tasks[0].task_spec().id();
    let child_runs = boot.projection().run_ids_for_task(child_id);
    let child_state = boot.projection().get_run_state(&child_runs[0]).expect("child state");
    assert_eq!(*child_state, RunState::Completed, "child must be Completed");

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
