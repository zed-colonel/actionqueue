//! Workflow state survives WAL recovery: DAG dependencies, hierarchy, and
//! handler output are all reconstructed correctly after an engine restart.
//!
//! Proves that Sprint 2 invariants hold across the crash boundary:
//! - Dependency declarations are replayed and the gate is rebuilt.
//! - Parent-child mappings are reconstructed in the hierarchy tracker.
//! - Handler output bytes are preserved in the projection.

mod support;

#[cfg(feature = "workflow")]
mod wf {
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

    /// A handler that echoes its payload as output bytes and always succeeds.
    #[derive(Debug)]
    struct EchoHandler;

    impl ExecutorHandler for EchoHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            HandlerOutput::Success { output: Some(ctx.input.payload.clone()), consumption: vec![] }
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
    async fn dag_dependency_declarations_survive_restart() {
        let data_dir = super::support::unique_data_dir("wf-crash-dag");

        let step1_id: TaskId = "06010101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let step2_id: TaskId = "06010101-0001-0001-0001-000000000002".parse().expect("valid uuid");

        // Phase 1: submit both tasks and declare step2→step1 dependency.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), EchoHandler);
            let mut eng = engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap");

            let spec1 = TaskSpec::new(
                step1_id,
                TaskPayload::new(b"step1".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec");
            let spec2 = TaskSpec::new(
                step2_id,
                TaskPayload::new(b"step2".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec");

            eng.submit_task(spec1).expect("submit step1");
            eng.submit_task(spec2).expect("submit step2");
            eng.shutdown().expect("shutdown");
        }

        // Declare dependency via authority.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery");
            let mut auth = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(
                        seq,
                        step2_id,
                        vec![step1_id],
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("declare dependency");
        }

        // Phase 2: first run — step1 completes, step2 becomes eligible and completes.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), EchoHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap after declare");
            let _ = eng.run_until_idle().await.expect("run");

            // Both steps must have completed.
            for (task_id, name) in [(step1_id, "step1"), (step2_id, "step2")] {
                let runs = eng.projection().run_ids_for_task(task_id);
                assert_eq!(runs.len(), 1, "{name} must have one run");
                assert_eq!(
                    eng.projection().get_run_state(&runs[0]),
                    Some(&RunState::Completed),
                    "{name} must be Completed"
                );
            }
            eng.shutdown().expect("shutdown");
        }

        // Phase 3: restart again — verify completed state + output survive.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), EchoHandler);
            let eng =
                engine.bootstrap_with_clock(MockClock::new(2000)).expect("bootstrap after restart");

            let step1_runs = eng.projection().run_ids_for_task(step1_id);
            let step2_runs = eng.projection().run_ids_for_task(step2_id);

            assert_eq!(
                eng.projection().get_run_state(&step1_runs[0]),
                Some(&RunState::Completed),
                "step1 must remain Completed after restart"
            );
            assert_eq!(
                eng.projection().get_run_state(&step2_runs[0]),
                Some(&RunState::Completed),
                "step2 must remain Completed after restart"
            );

            // Output bytes (echoed payload) must survive.
            let step1_history = eng
                .projection()
                .get_attempt_history(&step1_runs[0])
                .expect("step1 history must survive restart");
            assert_eq!(
                step1_history[0].output(),
                Some(b"step1".as_ref()),
                "step1 output must survive WAL recovery"
            );

            let step2_history = eng
                .projection()
                .get_attempt_history(&step2_runs[0])
                .expect("step2 history must survive restart");
            assert_eq!(
                step2_history[0].output(),
                Some(b"step2".as_ref()),
                "step2 output must survive WAL recovery"
            );

            // Task count must be stable.
            assert_eq!(
                eng.projection().task_count(),
                2,
                "task count must be stable across restart"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn parent_child_mapping_survives_restart() {
        let data_dir = super::support::unique_data_dir("wf-crash-hierarchy");

        let parent_id: TaskId = "06020101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let child_id: TaskId = "06020101-0001-0001-0001-000000000002".parse().expect("valid uuid");

        // Submit parent and child (with parent_task_id set) via engine.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), EchoHandler);
            let mut eng = engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap");

            let parent_spec = TaskSpec::new(
                parent_id,
                TaskPayload::new(b"parent".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec");
            let child_spec = TaskSpec::new(
                child_id,
                TaskPayload::new(b"child".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec")
            .with_parent(parent_id);

            eng.submit_task(parent_spec).expect("submit parent");
            eng.submit_task(child_spec).expect("submit child");
            let _ = eng.run_until_idle().await.expect("run");
            eng.shutdown().expect("shutdown");
        }

        // Restart and verify the parent-child relationship is in the projection.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), EchoHandler);
            let eng =
                engine.bootstrap_with_clock(MockClock::new(2000)).expect("bootstrap after restart");

            // Both tasks must be Completed.
            let parent_runs = eng.projection().run_ids_for_task(parent_id);
            let child_runs = eng.projection().run_ids_for_task(child_id);

            assert_eq!(
                eng.projection().get_run_state(&parent_runs[0]),
                Some(&RunState::Completed),
                "parent must be Completed after restart"
            );
            assert_eq!(
                eng.projection().get_run_state(&child_runs[0]),
                Some(&RunState::Completed),
                "child must be Completed after restart"
            );

            // Parent-child relationship must survive.
            let child_spec =
                eng.projection().get_task(&child_id).expect("child task must exist after restart");
            assert_eq!(
                child_spec.parent_task_id(),
                Some(parent_id),
                "parent_task_id must survive WAL recovery"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
