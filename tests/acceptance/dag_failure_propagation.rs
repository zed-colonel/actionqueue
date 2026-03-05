//! Dependency failure propagation: failed prerequisite cancels dependents.
//!
//! Proves that when a prerequisite task's run reaches Failed state, the
//! dependency gate cascades failure to all transitive dependents, canceling
//! their non-terminal runs.

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

    /// A handler that fails when payload is "fail", succeeds otherwise.
    #[derive(Debug)]
    struct SelectiveHandler;

    impl ExecutorHandler for SelectiveHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            if ctx.input.payload == b"fail" {
                HandlerOutput::TerminalFailure {
                    error: "intentional terminal failure".to_string(),
                    consumption: vec![],
                }
            } else {
                HandlerOutput::Success { output: None, consumption: vec![] }
            }
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
    async fn failed_prerequisite_cascades_to_cancel_dependent_runs() {
        let data_dir = super::support::unique_data_dir("wf-dag-failure-propagation");

        let step1_id: TaskId = "02010101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let step2_id: TaskId = "02010101-0001-0001-0001-000000000002".parse().expect("valid uuid");

        // Phase 1: submit step1 (will fail) and step2 (depends on step1).
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), SelectiveHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");

            let spec1 = TaskSpec::new(
                step1_id,
                TaskPayload::new(b"fail".to_vec()),
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

        // Phase 2: declare dependency — step2 depends on step1.
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
                .expect("declare step2→step1 must succeed");
        }

        // Phase 3: run engine — step1 fails, step2 must be canceled.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), SelectiveHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");
            let _ = eng.run_until_idle().await.expect("run must complete");

            let step1_runs = eng.projection().run_ids_for_task(step1_id);
            assert_eq!(step1_runs.len(), 1, "step1 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&step1_runs[0]),
                Some(&RunState::Failed),
                "step1 run must be Failed (handler returns TerminalFailure)"
            );

            let step2_runs = eng.projection().run_ids_for_task(step2_id);
            assert_eq!(step2_runs.len(), 1, "step2 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&step2_runs[0]),
                Some(&RunState::Canceled),
                "step2 run must be Canceled when its prerequisite (step1) fails"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Transitive failure propagation through a 3-level dependency chain.
    ///
    /// Declares: step1 → step2 → step3 (step2 depends on step1, step3 depends
    /// on step2). When step1 fails, both step2 AND step3 must be canceled,
    /// exercising `DependencyGate::notify_failed()` BFS cascading beyond 2 levels.
    #[tokio::test]
    async fn transitive_failure_propagation_cancels_3_level_chain() {
        let data_dir = super::support::unique_data_dir("wf-dag-failure-3level");

        let step1_id: TaskId = "02020101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let step2_id: TaskId = "02020101-0001-0001-0001-000000000002".parse().expect("valid uuid");
        let step3_id: TaskId = "02020101-0001-0001-0001-000000000003".parse().expect("valid uuid");

        // Phase 1: submit all three tasks.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), SelectiveHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");

            let spec1 = TaskSpec::new(
                step1_id,
                TaskPayload::new(b"fail".to_vec()),
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

            let spec3 = TaskSpec::new(
                step3_id,
                TaskPayload::new(b"step3".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec");

            eng.submit_task(spec1).expect("submit step1");
            eng.submit_task(spec2).expect("submit step2");
            eng.submit_task(spec3).expect("submit step3");
            eng.shutdown().expect("shutdown");
        }

        // Phase 2: declare chain: step2→step1, step3→step2.
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
                .expect("declare step2→step1 must succeed");

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
                .expect("declare step3→step2 must succeed");
        }

        // Phase 3: run engine — step1 fails, step2 AND step3 must be canceled.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), SelectiveHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1000)).expect("bootstrap must succeed");
            let _ = eng.run_until_idle().await.expect("run must complete");

            let step1_runs = eng.projection().run_ids_for_task(step1_id);
            assert_eq!(step1_runs.len(), 1, "step1 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&step1_runs[0]),
                Some(&RunState::Failed),
                "step1 run must be Failed"
            );

            let step2_runs = eng.projection().run_ids_for_task(step2_id);
            assert_eq!(step2_runs.len(), 1, "step2 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&step2_runs[0]),
                Some(&RunState::Canceled),
                "step2 run must be Canceled (direct dependent of failed step1)"
            );

            let step3_runs = eng.projection().run_ids_for_task(step3_id);
            assert_eq!(step3_runs.len(), 1, "step3 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&step3_runs[0]),
                Some(&RunState::Canceled),
                "step3 run must be Canceled (transitive dependent of failed step1 via step2)"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
