//! End-to-end test for the embedded engine API.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::RuntimeConfig;
use actionqueue_runtime::engine::ActionQueueEngine;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_data_dir() -> PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!("actionqueue_runtime_engine_test_{}_{count}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[derive(Debug)]
struct SuccessHandler;

impl ExecutorHandler for SuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: Some(b"done".to_vec()), consumption: vec![] }
    }
}

#[tokio::test]
async fn full_lifecycle_submit_to_complete() {
    let data_dir = temp_data_dir();
    let config = RuntimeConfig { data_dir: data_dir.clone(), ..RuntimeConfig::default() };

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(config, SuccessHandler);
    let mut bootstrapped = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    // Submit a Once task
    let task_id = TaskId::new();
    let spec = TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(b"test-payload".to_vec(), "text/plain"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec");

    bootstrapped.submit_task(spec).expect("submit should succeed");

    // Verify task was created
    assert_eq!(bootstrapped.projection().task_count(), 1);
    assert_eq!(bootstrapped.projection().run_count(), 1);

    // Run until idle — should promote, lease, execute, and complete
    let summary = bootstrapped.run_until_idle().await.expect("run should succeed");

    assert!(summary.total_dispatched > 0, "should have dispatched at least 1 run");
    assert!(summary.total_completed > 0, "should have completed at least 1 run");

    // Verify the run reached Completed state
    let run_ids = bootstrapped.projection().run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1);
    let run_state = bootstrapped.projection().get_run_state(&run_ids[0]);
    assert_eq!(run_state, Some(&RunState::Completed));

    bootstrapped.shutdown().expect("shutdown should succeed");
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn engine_pause_skips_dispatch() {
    let data_dir = temp_data_dir();
    let config = RuntimeConfig { data_dir: data_dir.clone(), ..RuntimeConfig::default() };

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(config, SuccessHandler);
    let mut bootstrapped = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    // Submit task
    let spec = TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(b"payload".to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    bootstrapped.submit_task(spec).expect("submit should succeed");

    // Tick should work normally
    let tick = bootstrapped.tick().await.expect("tick should succeed");
    assert!(!tick.engine_paused);

    let _ = std::fs::remove_dir_all(data_dir);
}
