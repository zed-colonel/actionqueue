//! DAG dependency ordering: tasks execute in declared dependency order.
//!
//! Proves that when task B declares a dependency on task A, task B is not
//! promoted to Ready until A reaches terminal success.

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use std::sync::{Arc, Mutex};

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::mutation::{
        DependencyDeclareCommand, DurabilityPolicy, MutationAuthority, MutationCommand,
    };
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_engine::time::clock::MockClock;
    use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
    use actionqueue_runtime::config::RuntimeConfig;
    use actionqueue_runtime::engine::ActionQueueEngine;
    use actionqueue_storage::mutation::authority::StorageMutationAuthority;
    use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;

    #[derive(Debug, Clone)]
    struct TrackingHandler {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl ExecutorHandler for TrackingHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let name =
                String::from_utf8(ctx.input.payload.clone()).unwrap_or_else(|_| "?".to_string());
            self.log.lock().unwrap().push(name);
            HandlerOutput::Success { output: None, consumption: vec![] }
        }
    }

    fn engine_config(data_dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            snapshot_event_threshold: None,
            ..RuntimeConfig::default()
        }
    }

    fn make_once_spec(id: TaskId, name: &[u8]) -> TaskSpec {
        TaskSpec::new(
            id,
            TaskPayload::new(name.to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("task spec must be valid")
    }

    #[tokio::test]
    async fn three_step_linear_dag_executes_in_order() {
        let data_dir = super::support::unique_data_dir("wf-dag-ordering");
        let execution_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(vec![]));

        // Use fixed UUIDs so the handler can identify steps by payload name.
        let step1_id: TaskId = "01010101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let step2_id: TaskId = "01010101-0001-0001-0001-000000000002".parse().expect("valid uuid");
        let step3_id: TaskId = "01010101-0001-0001-0001-000000000003".parse().expect("valid uuid");

        // Phase 1: submit all three tasks.
        {
            let log = Arc::clone(&execution_log);
            let engine = ActionQueueEngine::new(engine_config(&data_dir), TrackingHandler { log });
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");
            eng.submit_task(make_once_spec(step1_id, b"step1")).expect("submit step1");
            eng.submit_task(make_once_spec(step2_id, b"step2")).expect("submit step2");
            eng.submit_task(make_once_spec(step3_id, b"step3")).expect("submit step3");
            eng.shutdown().expect("shutdown must succeed");
        }

        // Phase 2: declare dependencies via authority lane.
        // step2 depends on step1; step3 depends on step2.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery must succeed");
            let mut authority =
                StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

            let seq = authority.projection().latest_sequence() + 1;
            let _ = authority
                .submit_command(
                    MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(
                        seq,
                        step2_id,
                        vec![step1_id],
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("declare step2→step1 dependency must succeed");

            let seq = authority.projection().latest_sequence() + 1;
            let _ = authority
                .submit_command(
                    MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(
                        seq,
                        step3_id,
                        vec![step2_id],
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("declare step3→step2 dependency must succeed");
        }

        // Phase 3: bootstrap new engine — gate is rebuilt from projection — and run.
        {
            let log = Arc::clone(&execution_log);
            let engine = ActionQueueEngine::new(engine_config(&data_dir), TrackingHandler { log });
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");
            let _ = eng.run_until_idle().await.expect("run must complete");

            // Verify all three runs reached Completed.
            for (task_id, name) in [(step1_id, "step1"), (step2_id, "step2"), (step3_id, "step3")] {
                let run_ids = eng.projection().run_ids_for_task(task_id);
                assert_eq!(run_ids.len(), 1, "{name} must have exactly one run");
                assert_eq!(
                    eng.projection().get_run_state(&run_ids[0]),
                    Some(&RunState::Completed),
                    "{name} run must be Completed"
                );
            }

            eng.shutdown().expect("shutdown must succeed");
        }

        // Phase 4: verify execution order.
        let log = execution_log.lock().unwrap();
        assert_eq!(
            *log,
            vec!["step1".to_string(), "step2".to_string(), "step3".to_string()],
            "steps must execute in DAG order: step1 → step2 → step3"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
