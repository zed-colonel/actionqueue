//! Attempt lineage: RunId is stable across retries, AttemptIds are unique,
//! and attempt history records failures and eventual success with output.
//!
//! Proves that a task with max_attempts=3 where the handler fails on the first
//! two attempts and succeeds on the third produces a single RunId with three
//! distinct AttemptIds, correct error/output payloads, and survives WAL recovery.

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::mutation::AttemptResultKind;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_engine::time::clock::MockClock;
    use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
    use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
    use actionqueue_runtime::engine::ActionQueueEngine;
    use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;

    /// Handler that fails on the first N calls and succeeds with output thereafter.
    #[derive(Debug)]
    struct FailThenSucceedHandler {
        call_count: Arc<AtomicUsize>,
        fail_count: usize,
    }

    impl ExecutorHandler for FailThenSucceedHandler {
        fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
            let n = self.call_count.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_count {
                HandlerOutput::RetryableFailure {
                    error: format!("attempt {n} failed"),
                    consumption: vec![],
                }
            } else {
                HandlerOutput::Success {
                    output: Some(b"final-output".to_vec()),
                    consumption: vec![],
                }
            }
        }
    }

    fn engine_config(data_dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            snapshot_event_threshold: None,
            backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_millis(0) },
            ..RuntimeConfig::default()
        }
    }

    #[tokio::test]
    async fn attempt_lineage_across_retries_and_recovery() {
        let data_dir = super::support::unique_data_dir("wf-attempt-lineage");
        let call_count = Arc::new(AtomicUsize::new(0));

        let task_id: TaskId = "08010101-0001-0001-0001-000000000001".parse().expect("valid uuid");

        // Submit task with max_attempts=3 and fail first 2 attempts.
        {
            let handler =
                FailThenSucceedHandler { call_count: Arc::clone(&call_count), fail_count: 2 };
            let constraints = TaskConstraints::new(3, None, None).expect("valid constraints");
            let spec = TaskSpec::new(
                task_id,
                TaskPayload::new(b"lineage-test".to_vec()),
                RunPolicy::Once,
                constraints,
                TaskMetadata::default(),
            )
            .expect("valid spec");

            let engine = ActionQueueEngine::new(engine_config(&data_dir), handler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");
            eng.submit_task(spec).expect("submit task");
            let _summary = eng.run_until_idle().await.expect("run must complete");

            // Verify single run reached Completed.
            let run_ids = eng.projection().run_ids_for_task(task_id);
            assert_eq!(run_ids.len(), 1, "must have exactly one run");
            let run_id = run_ids[0];
            assert_eq!(
                eng.projection().get_run_state(&run_id),
                Some(&RunState::Completed),
                "run must be Completed"
            );

            // Verify attempt history.
            let history =
                eng.projection().get_attempt_history(&run_id).expect("attempt history must exist");
            assert_eq!(history.len(), 3, "must have exactly 3 attempt entries");

            // All AttemptIds must be unique.
            let attempt_ids: HashSet<_> = history.iter().map(|e| e.attempt_id()).collect();
            assert_eq!(attempt_ids.len(), 3, "all 3 AttemptIds must be unique");

            // Verify result taxonomy.
            assert_eq!(history[0].result(), Some(AttemptResultKind::Failure));
            assert_eq!(history[0].error(), Some("attempt 0 failed"));
            assert!(history[0].output().is_none());

            assert_eq!(history[1].result(), Some(AttemptResultKind::Failure));
            assert_eq!(history[1].error(), Some("attempt 1 failed"));
            assert!(history[1].output().is_none());

            assert_eq!(history[2].result(), Some(AttemptResultKind::Success));
            assert!(history[2].error().is_none());
            assert_eq!(history[2].output(), Some(b"final-output".as_ref()));

            eng.shutdown().expect("shutdown must succeed");
        }

        // Phase 2: restart from WAL and verify lineage survives.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery must succeed");

            let run_ids = recovery.projection.run_ids_for_task(task_id);
            assert_eq!(run_ids.len(), 1, "must have one run after recovery");
            let run_id = run_ids[0];

            assert_eq!(
                recovery.projection.get_run_state(&run_id),
                Some(&RunState::Completed),
                "run must be Completed after recovery"
            );

            let history = recovery
                .projection
                .get_attempt_history(&run_id)
                .expect("attempt history must survive");
            assert_eq!(history.len(), 3, "must have 3 attempts after recovery");

            let attempt_ids: HashSet<_> = history.iter().map(|e| e.attempt_id()).collect();
            assert_eq!(attempt_ids.len(), 3, "AttemptIds must remain unique after recovery");

            assert_eq!(history[2].result(), Some(AttemptResultKind::Success));
            assert_eq!(history[2].output(), Some(b"final-output".as_ref()));
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
