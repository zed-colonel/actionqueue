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
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
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
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let _input = ctx.input;
        actionqueue_core::disposition::AttemptDisposition::complete(
            (Some(b"done".to_vec()))
                .map(|v| actionqueue_core::data_ref::DataRef::from_bytes(v).unwrap()),
        )
    }
}

#[tokio::test]
async fn full_lifecycle_submit_to_complete() {
    let data_dir = temp_data_dir();
    let config = RuntimeConfig { data_dir: data_dir.clone(), ..RuntimeConfig::default() };

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(config, SuccessHandler);
    let mut bootstrapped = engine
        .bootstrap_with_clock(clock)
        .expect("bootstrap should succeed")
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });

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

    let request =
        actionqueue_core::admission::EnsureTaskRequest::for_task(spec.clone(), vec![]).unwrap();
    let created = bootstrapped.submit_task(spec).expect("submit should succeed");

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

    let before = bootstrapped.projection().projection_digest().unwrap();
    let duplicate = bootstrapped.ensure_task(request.clone()).unwrap();
    assert!(!duplicate.is_created());
    assert_eq!(duplicate.sequence(), created.sequence());
    assert_eq!(bootstrapped.projection().projection_digest().unwrap(), before);
    bootstrapped.shutdown().expect("shutdown should succeed");
    let mut recovered = ActionQueueEngine::new(
        RuntimeConfig { data_dir: data_dir.clone(), ..Default::default() },
        SuccessHandler,
    )
    .bootstrap_with_clock(MockClock::new(5000))
    .unwrap()
    .with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    });
    assert!(!recovered.ensure_task(request).unwrap().is_created());
    assert_eq!(recovered.projection().projection_digest().unwrap(), before);
    recovered.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn engine_pause_skips_dispatch() {
    let data_dir = temp_data_dir();
    let config = RuntimeConfig { data_dir: data_dir.clone(), ..RuntimeConfig::default() };

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(config, SuccessHandler);
    let mut bootstrapped = engine
        .bootstrap_with_clock(clock)
        .expect("bootstrap should succeed")
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });

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

#[tokio::test]
async fn child_retry_survives_parent_completion_cache_cleanup_and_restart() {
    use actionqueue_core::admission::{AdmissionRejection, EnsureTaskRequest};
    use actionqueue_runtime::admission::AdmissionError;
    let data_dir = temp_data_dir();
    let config = RuntimeConfig { data_dir: data_dir.clone(), ..Default::default() };
    let mut engine = ActionQueueEngine::new(config.clone(), SuccessHandler)
        .bootstrap_with_clock(MockClock::new(1000))
        .unwrap()
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
    let make_spec = |id| {
        TaskSpec::new(
            id,
            TaskPayload::new(vec![]),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap()
    };
    let parent = TaskId::new();
    let child = TaskId::new();
    engine.submit_task(make_spec(parent)).unwrap();
    let request = EnsureTaskRequest::for_task(
        make_spec(child).with_parent_policy(
            parent,
            actionqueue_core::task::task_spec::ChildLifecyclePolicy::Detached,
        ),
        vec![],
    )
    .unwrap();
    engine.ensure_task(request.clone()).unwrap();
    let _ = engine.run_until_idle().await.unwrap();
    assert!(engine.projection().runs_for_task(parent).all(|r| r.state() == RunState::Completed));
    assert!(engine.projection().runs_for_task(child).all(|r| r.state() == RunState::Completed));
    let digest = engine.projection().projection_digest().unwrap();
    assert!(!engine.ensure_task(request.clone()).unwrap().is_created());
    assert!(matches!(
        engine.submit_task(make_spec(TaskId::new()).with_parent(parent)),
        Err(AdmissionError::Rejected(AdmissionRejection::TerminalParent))
    ));
    engine.shutdown().unwrap();
    let mut recovered = ActionQueueEngine::new(config, SuccessHandler)
        .bootstrap_with_clock(MockClock::new(5000))
        .unwrap()
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
    assert!(!recovered.ensure_task(request).unwrap().is_created());
    assert_eq!(recovered.projection().projection_digest().unwrap(), digest);
    recovered.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(data_dir);
}
