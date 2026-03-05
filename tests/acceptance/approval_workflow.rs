//! 8G Approval workflow acceptance proof.
//!
//! Verifies that DAG-based approval workflows work correctly:
//! - A rejected review blocks execution.
//! - An approved review allows execution.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::Clock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;
use actionqueue_workflow::dag::DependencyGate;

/// Clock backed by a shared atomic counter, advanceable from outside the engine.
#[derive(Clone)]
struct AdvancableClock(Arc<AtomicU64>);

impl AdvancableClock {
    fn new(t: u64) -> Self {
        Self(Arc::new(AtomicU64::new(t)))
    }

    fn advance(&self, by: u64) {
        self.0.fetch_add(by, Ordering::Release);
    }
}

impl Clock for AdvancableClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8g-approval-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

struct SucceedHandler;

impl ExecutorHandler for SucceedHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::success()
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

fn simple_spec(id: TaskId, label: &str) -> TaskSpec {
    TaskSpec::new(
        id,
        TaskPayload::new(label.as_bytes().to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec")
}

/// DAG: plan → review → execution. Rejected review prevents execution via cascade.
#[test]
fn rejected_prerequisite_cascades_to_dependent() {
    let mut gate = DependencyGate::new();
    let plan_id = TaskId::new();
    let review_id = TaskId::new();
    let execution_id = TaskId::new();

    // review depends on plan; execution depends on review.
    gate.declare(review_id, vec![plan_id]).expect("no cycle");
    gate.declare(execution_id, vec![review_id]).expect("no cycle");

    // Plan completes.
    let eligible = gate.notify_completed(plan_id);
    assert!(eligible.contains(&review_id), "review becomes eligible after plan completes");

    // Review is "rejected" (permanently failed).
    let blocked = gate.notify_failed(review_id);
    assert!(blocked.contains(&execution_id), "execution blocked by rejected review");

    assert!(gate.is_dependency_failed(execution_id), "execution never runs");
    assert!(!gate.is_eligible(execution_id), "execution is NOT eligible");
}

/// DAG: plan → review → execution. Approved review allows execution.
#[test]
fn approved_prerequisite_allows_execution() {
    let mut gate = DependencyGate::new();
    let plan_id = TaskId::new();
    let review_id = TaskId::new();
    let execution_id = TaskId::new();

    gate.declare(review_id, vec![plan_id]).expect("no cycle");
    gate.declare(execution_id, vec![review_id]).expect("no cycle");

    // Plan completes.
    let _ = gate.notify_completed(plan_id);
    assert!(!gate.is_eligible(execution_id), "execution not yet eligible (review pending)");

    // Review completes (approved).
    let eligible = gate.notify_completed(review_id);
    assert!(eligible.contains(&execution_id), "execution becomes eligible after review approval");
    assert!(gate.is_eligible(execution_id), "execution must be eligible after approval");
}

/// Full acceptance: plan task completes, execution task can be scheduled.
#[tokio::test]
async fn plan_completes_then_execution_eligible() {
    let dir = data_dir("workflow");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), SucceedHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap");

    let plan_id = TaskId::new();
    let execution_id = TaskId::new();

    // Submit plan task.
    boot.submit_task(simple_spec(plan_id, "plan")).expect("submit plan");
    // Submit execution task with plan as prerequisite.
    boot.submit_task(simple_spec(execution_id, "execution")).expect("submit execution");
    boot.declare_dependency(execution_id, vec![plan_id]).expect("declare dependency");

    // Run until plan completes.
    clock.advance(1);
    let _ = boot.run_until_idle().await.expect("run");

    // Verify plan completed.
    let plan_runs = boot.projection().run_ids_for_task(plan_id);
    assert_eq!(plan_runs.len(), 1);
    let plan_state = boot.projection().get_run_state(&plan_runs[0]).expect("plan state");
    assert_eq!(*plan_state, RunState::Completed, "plan must complete");

    // Execution task should now be eligible and run.
    let _ = boot.run_until_idle().await.expect("run 2");
    let exec_runs = boot.projection().run_ids_for_task(execution_id);
    assert_eq!(exec_runs.len(), 1);
    let exec_state = boot.projection().get_run_state(&exec_runs[0]).expect("exec state");
    assert_eq!(*exec_state, RunState::Completed, "execution must complete after plan");
}
