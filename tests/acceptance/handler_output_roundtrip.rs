//! Output bytes survive WAL → recovery → projection.
//!
//! Proves that when a handler returns `output` bytes, they are threaded through
//! the WAL append path and are queryable via the projection after a restart.

mod support;

#[cfg(feature = "workflow")]
mod wf {
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

    #[derive(Debug)]
    struct OutputHandler;

    impl ExecutorHandler for OutputHandler {
        fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
            HandlerOutput::Success { output: Some(b"hello-output".to_vec()), consumption: vec![] }
        }
    }

    fn engine_config(data_dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            snapshot_event_threshold: None,
            ..RuntimeConfig::default()
        }
    }

    #[tokio::test]
    async fn output_bytes_survive_wal_and_restart() {
        let data_dir = super::support::unique_data_dir("wf-output-roundtrip");
        let task_id = TaskId::new();

        // Phase 1: run the task, verify output in-memory.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), OutputHandler);
            let mut eng = engine
                .bootstrap_with_clock(MockClock::new(1000))
                .expect("bootstrap should succeed");

            let spec = TaskSpec::new(
                task_id,
                TaskPayload::new(b"payload".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid task spec");

            eng.submit_task(spec).expect("submit should succeed");
            let _ = eng.run_until_idle().await.expect("run should complete");

            let run_ids = eng.projection().run_ids_for_task(task_id);
            assert_eq!(run_ids.len(), 1, "Once policy must produce exactly one run");
            let run_id = run_ids[0];
            assert_eq!(
                eng.projection().get_run_state(&run_id),
                Some(&RunState::Completed),
                "run must be Completed after handler returns Success"
            );

            let history = eng
                .projection()
                .get_attempt_history(&run_id)
                .expect("attempt history must exist for completed run");
            assert_eq!(history.len(), 1, "Once run must have exactly one attempt");
            assert_eq!(
                history[0].output(),
                Some(b"hello-output".as_ref()),
                "output bytes must be present in attempt history"
            );

            eng.shutdown().expect("shutdown should succeed");
        }

        // Phase 2: restart from WAL and verify output survived.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), OutputHandler);
            let eng = engine
                .bootstrap_with_clock(MockClock::new(2000))
                .expect("bootstrap after restart should succeed");

            let run_ids = eng.projection().run_ids_for_task(task_id);
            assert_eq!(run_ids.len(), 1, "run count must be stable across restart");
            let run_id = run_ids[0];
            assert_eq!(
                eng.projection().get_run_state(&run_id),
                Some(&RunState::Completed),
                "run must remain Completed after WAL recovery"
            );

            let history = eng
                .projection()
                .get_attempt_history(&run_id)
                .expect("attempt history must survive WAL recovery");
            assert_eq!(history.len(), 1, "attempt count must be stable across restart");
            assert_eq!(
                history[0].output(),
                Some(b"hello-output".as_ref()),
                "output bytes must survive WAL recovery"
            );

            eng.shutdown().expect("shutdown should succeed");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
