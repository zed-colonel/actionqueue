//! 7C Suspend/resume lifecycle acceptance proof.
//!
//! Verifies the full suspend/resume lifecycle:
//! 1. Handler returns Suspended on the first call.
//! 2. Run transitions Running → Suspended (not terminal, not a counted attempt).
//! 3. External resume via `resume_run()` transitions Suspended → Ready.
//! 4. Handler returns Success on the second call.
//! 5. Run completes. Attempt count reflects two total dispatches.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
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

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("7c-suspend-resume-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir should be creatable");
    dir
}

/// Suspends on the first call; succeeds on all subsequent calls.
#[derive(Debug)]
struct SuspendThenSucceedHandler {
    call_count: Arc<AtomicUsize>,
}

impl ExecutorHandler for SuspendThenSucceedHandler {
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

/// Full suspend/resume lifecycle: suspend → external resume → complete.
/// The task uses max_attempts=1 to verify suspended attempts do NOT count
/// toward the retry cap (the run must still complete after suspension).
#[tokio::test]
async fn suspend_resume_full_lifecycle() {
    let dir = data_dir("full-lifecycle");

    let clock = MockClock::new(1000);
    let call_count = Arc::new(AtomicUsize::new(0));
    let handler = SuspendThenSucceedHandler { call_count: Arc::clone(&call_count) };
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // max_attempts=1: if suspension counted as an attempt, the run would fail here.
    // It must NOT count, so the run can still complete after one suspend + one success.
    let task_id = TaskId::new();
    let constraints = TaskConstraints::new(1, None, None).expect("valid constraints");
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::new(b"suspend-resume-test".to_vec()),
        RunPolicy::Once,
        constraints,
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec).expect("submit");

    // Phase 1: dispatch until idle — run should land in Suspended after 1 dispatch.
    let _phase1 = boot.run_until_idle().await.expect("phase 1 idle");

    let run_ids = boot.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "Once policy must produce exactly 1 run");
    let run_id = run_ids[0];

    let state_suspended = boot.projection().get_run_state(&run_id).expect("run state");
    assert_eq!(
        *state_suspended,
        RunState::Suspended,
        "run must be in Suspended state after handler returns Suspended"
    );
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "handler must have been called exactly once (the suspend call)"
    );

    // Phase 2: externally resume the suspended run.
    boot.resume_run(run_id).expect("resume run");

    let state_after_resume =
        boot.projection().get_run_state(&run_id).expect("run state after resume");
    assert_eq!(
        *state_after_resume,
        RunState::Ready,
        "run must transition to Ready after resume_run()"
    );

    // Phase 3: dispatch until idle — run should complete.
    let _phase3 = boot.run_until_idle().await.expect("phase 3 idle");

    let final_state = boot.projection().get_run_state(&run_id).expect("final state");
    assert_eq!(
        *final_state,
        RunState::Completed,
        "run must complete after suspend → resume → success"
    );
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        2,
        "handler must have been called exactly twice (suspend + success)"
    );

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
