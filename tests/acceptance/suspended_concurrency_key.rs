//! 7I Suspended concurrency key acceptance proof.
//!
//! Verifies that a suspended run follows the task's concurrency key hold policy:
//!
//! - `HoldDuringRetry` (default): the key is held while suspended, blocking a
//!   second task with the same key from being dispatched.
//! - `ReleaseOnRetry`: the key is released when the run suspends, allowing the
//!   second task to be dispatched immediately.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::{ConcurrencyKeyHoldPolicy, TaskConstraints};
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
        .join(format!("7i-suspended-ck-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir should be creatable");
    dir
}

/// Suspends on call 0; succeeds on all subsequent calls.
#[derive(Debug)]
struct SuspendOnFirstCallHandler {
    call_count: Arc<AtomicUsize>,
}

impl ExecutorHandler for SuspendOnFirstCallHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            HandlerOutput::Suspended { output: None, consumption: vec![] }
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

fn spec_with_key_and_policy(
    task_id: TaskId,
    key: &str,
    policy: ConcurrencyKeyHoldPolicy,
) -> TaskSpec {
    let mut constraints =
        TaskConstraints::new(3, None, Some(key.to_string())).expect("valid constraints");
    constraints.set_concurrency_key_hold_policy(policy);
    TaskSpec::new(
        task_id,
        TaskPayload::new(b"ck-test".to_vec()),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .expect("valid spec")
}

/// With HoldDuringRetry (default), a suspended run keeps the concurrency key.
/// Task A suspends first (holding the key). Task B is submitted after and must
/// remain in Ready — the key is still held by the suspended Task A.
#[tokio::test]
async fn suspended_run_holds_concurrency_key_with_hold_during_retry_policy() {
    let dir = data_dir("hold-policy");

    let clock = MockClock::new(1000);
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler = SuspendOnFirstCallHandler { call_count: Arc::clone(&call_count) };
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let shared_key = "exclusive-resource";

    // Submit Task A first (holds key while suspended).
    let task_a = TaskId::new();
    boot.submit_task(spec_with_key_and_policy(
        task_a,
        shared_key,
        ConcurrencyKeyHoldPolicy::HoldDuringRetry,
    ))
    .expect("submit task A");

    // Phase 1: dispatch Task A — it suspends and holds the key.
    let _phase1 = boot.run_until_idle().await.expect("phase 1");

    let run_ids_a = boot.projection().run_ids_for_task(task_a);
    assert_eq!(run_ids_a.len(), 1);
    let state_a = *boot.projection().get_run_state(&run_ids_a[0]).expect("state A");
    assert_eq!(state_a, RunState::Suspended, "task A must be Suspended after phase 1");

    // Submit Task B AFTER Task A is suspended — key still held.
    let task_b = TaskId::new();
    boot.submit_task(spec_with_key_and_policy(
        task_b,
        shared_key,
        ConcurrencyKeyHoldPolicy::HoldDuringRetry,
    ))
    .expect("submit task B");

    // Phase 2: one tick — Task B is in Ready but cannot dispatch (key held).
    let _tick = boot.tick().await.expect("tick after submit B");

    let run_ids_b = boot.projection().run_ids_for_task(task_b);
    assert_eq!(run_ids_b.len(), 1);
    let state_b = *boot.projection().get_run_state(&run_ids_b[0]).expect("state B");
    assert_eq!(
        state_b,
        RunState::Ready,
        "task B run must remain Ready — key held by suspended task A"
    );

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

/// With ReleaseOnRetry, a suspended run releases the concurrency key.
/// Task A suspends and releases the key. Task B is submitted after and can
/// dispatch immediately.
#[tokio::test]
async fn suspended_run_releases_concurrency_key_with_release_on_retry_policy() {
    let dir = data_dir("release-policy");

    let clock = MockClock::new(1000);
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler = SuspendOnFirstCallHandler { call_count: Arc::clone(&call_count) };
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let shared_key = "shared-resource";

    // Submit Task A with ReleaseOnRetry — releases key when suspended.
    let task_a = TaskId::new();
    boot.submit_task(spec_with_key_and_policy(
        task_a,
        shared_key,
        ConcurrencyKeyHoldPolicy::ReleaseOnRetry,
    ))
    .expect("submit task A");

    // Phase 1: dispatch Task A — it suspends and releases the key.
    let _phase1 = boot.run_until_idle().await.expect("phase 1");

    let run_ids_a = boot.projection().run_ids_for_task(task_a);
    assert_eq!(run_ids_a.len(), 1);
    let state_a = *boot.projection().get_run_state(&run_ids_a[0]).expect("state A");
    assert_eq!(state_a, RunState::Suspended, "task A must be Suspended after phase 1");

    // Submit Task B AFTER key was released by Task A's suspension.
    let task_b = TaskId::new();
    boot.submit_task(spec_with_key_and_policy(
        task_b,
        shared_key,
        ConcurrencyKeyHoldPolicy::ReleaseOnRetry,
    ))
    .expect("submit task B");

    // Phase 2: run until idle — Task B can acquire the key and complete.
    let _phase2 = boot.run_until_idle().await.expect("phase 2");

    let run_ids_b = boot.projection().run_ids_for_task(task_b);
    assert_eq!(run_ids_b.len(), 1);
    let state_b = *boot.projection().get_run_state(&run_ids_b[0]).expect("state B");
    assert_eq!(
        state_b,
        RunState::Completed,
        "task B must complete — key released by suspended task A (ReleaseOnRetry)"
    );

    // Task A remains Suspended (not resumed).
    let state_a_final = *boot.projection().get_run_state(&run_ids_a[0]).expect("state A final");
    assert_eq!(state_a_final, RunState::Suspended, "task A must still be Suspended (not resumed)");

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
