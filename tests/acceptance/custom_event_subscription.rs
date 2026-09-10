//! 7J Custom event subscription acceptance proof.
//!
//! Verifies that the `fire_custom_event` API triggers matching subscriptions,
//! which promotes waiting tasks' Scheduled runs to Ready.
//!
//! Flow:
//! 1. Task A: Repeat(2, 10000) — run 1 at now, run 2 far in the future.
//! 2. Task A subscribes to a custom event with key "caelum.thread.resume".
//! 3. Run 1 dispatches and completes normally.
//! 4. External caller fires custom event "caelum.thread.resume".
//! 5. Run 2 (scheduled at t=11000) is promoted via subscription trigger.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::EventFilter;
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
        .join(format!("7j-custom-evt-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

/// Handler that always succeeds.
#[derive(Debug)]
struct AlwaysSucceedHandler;

impl ExecutorHandler for AlwaysSucceedHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

fn make_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(2).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

/// Custom event triggers subscription promotion for a future-scheduled run.
#[tokio::test]
async fn custom_event_triggers_subscription_promotion() {
    let dir = data_dir("custom-trigger");

    let clock = MockClock::new(1000);
    let handler = AlwaysSucceedHandler;
    let engine = ActionQueueEngine::new(make_config(dir.clone()), handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // Task A: Repeat(2, 10000). Run 1 at t=1000, run 2 at t=11000.
    let task_a_id = TaskId::new();
    let repeat_a = RunPolicy::repeat(2, 10000).expect("valid repeat");
    let spec_a = TaskSpec::new(
        task_a_id,
        TaskPayload::new(b"task-a".to_vec()),
        repeat_a,
        TaskConstraints::new(1, None, None).expect("valid"),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    boot.submit_task(spec_a).expect("submit A");

    // Subscribe to a custom event.
    let _sub_id = boot
        .create_subscription(
            task_a_id,
            EventFilter::Custom { key: "caelum.thread.resume".to_string() },
        )
        .expect("create subscription");

    // Phase 1: run until idle. Run 1 completes; run 2 stays Scheduled.
    let _s1 = boot.run_until_idle().await.expect("idle 1");

    let run_ids = boot.projection().run_ids_for_task(task_a_id);
    assert_eq!(run_ids.len(), 2, "Repeat(2) must derive 2 runs");

    // One run must be Completed, one Scheduled.
    let completed = run_ids
        .iter()
        .filter(|rid| *boot.projection().get_run_state(rid).expect("state") == RunState::Completed)
        .count();
    let scheduled = run_ids
        .iter()
        .filter(|rid| *boot.projection().get_run_state(rid).expect("state") == RunState::Scheduled)
        .count();
    assert_eq!(completed, 1, "one run must be Completed");
    assert_eq!(scheduled, 1, "one run must be Scheduled (far future)");

    // Phase 2: fire custom event → subscription triggers → run 2 promoted.
    boot.fire_custom_event("caelum.thread.resume".to_string()).expect("fire custom event");
    let _s2 = boot.run_until_idle().await.expect("idle 2");

    // Both runs must now be Completed.
    for rid in &run_ids {
        let state = boot.projection().get_run_state(rid).expect("state");
        assert_eq!(
            *state,
            RunState::Completed,
            "run {rid} must be Completed after custom event trigger"
        );
    }

    boot.shutdown().expect("shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A budget gate keeps promoted runs Ready so the automatic snapshot captures
/// the early-promotion state before any attempt can start.
#[tokio::test]
async fn subscription_promoted_ready_run_survives_snapshot_restart_and_backup() {
    use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
    use actionqueue_storage::{
        recovery::bootstrap::recover_read_only,
        store::{backup_store, inspect_store, open_store, restore_store, OpenOptions},
        wal::repair::RepairPolicy,
    };
    #[derive(Debug)]
    struct ConsumeBudget;
    impl ExecutorHandler for ConsumeBudget {
        fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
            HandlerOutput::Success {
                output: None,
                consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 1)],
            }
        }
    }
    let base = tempfile::tempdir().unwrap();
    let source = base.path().join("source");
    let mut config = make_config(source.clone());
    config.snapshot_event_threshold = Some(1);
    let engine = ActionQueueEngine::new(config.clone(), ConsumeBudget);
    let mut boot = engine.bootstrap_with_clock(MockClock::new(1000)).unwrap();
    let task_id = TaskId::new();
    boot.submit_task(
        TaskSpec::new(
            task_id,
            TaskPayload::new(b"early-ready".to_vec()),
            RunPolicy::repeat(2, 10000).unwrap(),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap(),
    )
    .unwrap();
    boot.allocate_budget(task_id, BudgetDimension::Token, 1).unwrap();
    boot.create_subscription(task_id, EventFilter::Custom { key: "promote".into() }).unwrap();
    let future_run = boot
        .projection()
        .runs_for_task(task_id)
        .find(|run| run.scheduled_at() == 11000)
        .unwrap()
        .id();
    boot.run_until_idle().await.unwrap();
    assert!(boot.is_budget_exhausted(task_id, BudgetDimension::Token));
    boot.fire_custom_event("promote".into()).unwrap();
    assert_eq!(boot.tick().await.unwrap().dispatched, 0);
    assert_eq!(boot.projection().get_run_state(&future_run), Some(&RunState::Ready));
    let expected = boot.projection().projection_digest().unwrap();
    boot.shutdown().unwrap();
    let session = open_store(&source, OpenOptions::ReadOnly).unwrap();
    let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert!(recovered.snapshot_loaded);
    assert_eq!(recovered.projection.projection_digest().unwrap(), expected);
    drop(session);
    let backup = base.path().join("backup");
    let restored = base.path().join("restored");
    assert_eq!(backup_store(&source, &backup).unwrap().projection_digest, expected);
    restore_store(&backup, &restored).unwrap();
    assert_eq!(inspect_store(&restored).unwrap().projection_digest, expected);
    for path in [source, restored] {
        config.data_dir = path;
        let engine = ActionQueueEngine::new(config.clone(), AlwaysSucceedHandler);
        let boot = engine.bootstrap_with_clock(MockClock::new(1000)).unwrap();
        assert_eq!(boot.projection().get_run_state(&future_run), Some(&RunState::Ready));
        assert_eq!(boot.projection().projection_digest().unwrap(), expected);
        boot.shutdown().unwrap();
    }
}
