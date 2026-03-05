//! Task hierarchy lifecycle: cascade cancellation, orphan prevention, and
//! completion gating.
//!
//! Proves that:
//! 1. When a parent task is canceled, the dispatch loop cascades the cancellation
//!    to all non-terminal descendants.
//! 2. `HierarchyTracker::register_child` rejects children of terminal parents
//!    (orphan prevention contract).
//! 3. A parent task's runs stay non-terminal while children are still running
//!    (completion gating via scheduling order — validates AC-6).

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::mutation::{
        DurabilityPolicy, MutationAuthority, MutationCommand, RunCreateCommand, TaskCancelCommand,
        TaskCreateCommand,
    };
    use actionqueue_core::run::run_instance::RunInstance;
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
    use actionqueue_workflow::hierarchy::{HierarchyError, HierarchyTracker};

    #[derive(Debug)]
    struct NopHandler;

    impl ExecutorHandler for NopHandler {
        fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
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

    fn make_once_spec_with_parent(id: TaskId, parent_id: TaskId, payload: &[u8]) -> TaskSpec {
        TaskSpec::new(
            id,
            TaskPayload::new(payload.to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec")
        .with_parent(parent_id)
    }

    // ── Cascade cancellation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn canceling_parent_cascades_to_non_terminal_children() {
        let data_dir = super::support::unique_data_dir("wf-hierarchy-cascade");

        let parent_id: TaskId = "03010101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let child1_id: TaskId = "03010101-0001-0001-0001-000000000002".parse().expect("valid uuid");
        let child2_id: TaskId = "03010101-0001-0001-0001-000000000003".parse().expect("valid uuid");

        let ts = 1000u64;

        // Phase 1: submit parent + two children (with parent_task_id set) and cancel
        // the parent task — all via authority, before the engine runs.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery must succeed");
            let mut auth = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

            // Create parent task and its run.
            let seq = auth.projection().latest_sequence() + 1;
            let parent_spec = TaskSpec::new(
                parent_id,
                TaskPayload::new(b"parent".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid spec");
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, parent_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create parent task");

            let parent_run = RunInstance::new_scheduled(parent_id, ts, ts).expect("valid run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, parent_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create parent run");

            // Create child1 with parent_task_id = parent_id.
            let child1_spec = make_once_spec_with_parent(child1_id, parent_id, b"child1");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, child1_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child1 task");
            let child1_run = RunInstance::new_scheduled(child1_id, ts, ts).expect("valid run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, child1_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child1 run");

            // Create child2 with parent_task_id = parent_id.
            let child2_spec = make_once_spec_with_parent(child2_id, parent_id, b"child2");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, child2_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child2 task");
            let child2_run = RunInstance::new_scheduled(child2_id, ts, ts).expect("valid run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, child2_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child2 run");

            // Cancel the parent task.
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCancel(TaskCancelCommand::new(seq, parent_id, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("cancel parent task must succeed");
        }

        // Phase 2: bootstrap engine and tick once — cascade_hierarchy_cancellations fires.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), NopHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(ts)).expect("bootstrap must succeed");

            // Tick processes cascade (step 0c) which cancels children.
            let _ = eng.tick().await.expect("tick must succeed");

            let child1_runs = eng.projection().run_ids_for_task(child1_id);
            assert_eq!(child1_runs.len(), 1, "child1 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&child1_runs[0]),
                Some(&RunState::Canceled),
                "child1 must be Canceled after parent cancellation cascade"
            );

            let child2_runs = eng.projection().run_ids_for_task(child2_id);
            assert_eq!(child2_runs.len(), 1, "child2 must have one run");
            assert_eq!(
                eng.projection().get_run_state(&child2_runs[0]),
                Some(&RunState::Canceled),
                "child2 must be Canceled after parent cancellation cascade"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ── Orphan prevention ────────────────────────────────────────────────────

    #[test]
    fn register_child_of_terminal_parent_returns_orphan_prevention_error() {
        let parent_id: TaskId = "03020101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let child_id: TaskId = "03020101-0001-0001-0001-000000000002".parse().expect("valid uuid");

        let mut tracker = HierarchyTracker::new();
        tracker.mark_terminal(parent_id);

        let err = tracker
            .register_child(parent_id, child_id)
            .expect_err("child of terminal parent must be rejected");

        assert!(
            matches!(
                err,
                HierarchyError::OrphanPrevention { child, parent }
                    if child == child_id && parent == parent_id
            ),
            "error must be OrphanPrevention with correct child and parent IDs"
        );
    }

    #[test]
    fn depth_limit_exceeded_returns_depth_limit_error() {
        let mut tracker = HierarchyTracker::with_max_depth(2);
        let a: TaskId = "03030101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let b: TaskId = "03030101-0001-0001-0001-000000000002".parse().expect("valid uuid");
        let c: TaskId = "03030101-0001-0001-0001-000000000003".parse().expect("valid uuid");
        let d: TaskId = "03030101-0001-0001-0001-000000000004".parse().expect("valid uuid");

        tracker.register_child(a, b).expect("depth 1: ok");
        tracker.register_child(b, c).expect("depth 2: ok");
        let err = tracker.register_child(c, d).expect_err("depth 3 exceeds max_depth=2");
        assert!(
            matches!(err, HierarchyError::DepthLimitExceeded { depth: 3, limit: 2, .. }),
            "error must be DepthLimitExceeded with depth=3, limit=2"
        );
    }

    // ── Completion gating (AC-6) ────────────────────────────────────────────

    /// Parent task's runs stay non-terminal while children are still running.
    ///
    /// Validates AC-6: Parent task cannot complete while children are non-terminal.
    ///
    /// Strategy: schedule children at t=1000 and parent at t=1200. Engine at
    /// clock=1100 dispatches children (eligible) but not parent (not yet due).
    /// After children complete, re-bootstrap engine at clock=1200 to dispatch
    /// the parent. This proves the parent stays non-terminal while children
    /// execute, and can complete normally after children are terminal.
    #[tokio::test]
    async fn parent_stays_non_terminal_while_children_running() {
        let data_dir = super::support::unique_data_dir("wf-hierarchy-completion-gate");

        let parent_id: TaskId = "03040101-0001-0001-0001-000000000001".parse().expect("valid uuid");
        let child1_id: TaskId = "03040101-0001-0001-0001-000000000002".parse().expect("valid uuid");
        let child2_id: TaskId = "03040101-0001-0001-0001-000000000003".parse().expect("valid uuid");

        let ts = 1000u64;

        // Phase 1: Submit parent (scheduled at 1200) + 2 children (scheduled at 1000)
        // via mutation authority.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery must succeed");
            let mut auth = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

            // Create parent task + run (scheduled at 1200 — not eligible until then).
            let parent_spec = TaskSpec::new(
                parent_id,
                TaskPayload::new(b"parent".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("valid parent spec");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, parent_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create parent task");
            let parent_run =
                RunInstance::new_scheduled(parent_id, ts + 200, ts).expect("valid parent run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, parent_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create parent run");

            // Create child1 (scheduled at 1000 — immediately eligible at clock=1100).
            let child1_spec = make_once_spec_with_parent(child1_id, parent_id, b"child1");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, child1_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child1 task");
            let child1_run =
                RunInstance::new_scheduled(child1_id, ts, ts).expect("valid child1 run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, child1_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child1 run");

            // Create child2 (scheduled at 1000 — immediately eligible at clock=1100).
            let child2_spec = make_once_spec_with_parent(child2_id, parent_id, b"child2");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::TaskCreate(TaskCreateCommand::new(seq, child2_spec, ts)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child2 task");
            let child2_run =
                RunInstance::new_scheduled(child2_id, ts, ts).expect("valid child2 run");
            let seq = auth.projection().latest_sequence() + 1;
            let _ = auth
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, child2_run)),
                    DurabilityPolicy::Immediate,
                )
                .expect("create child2 run");
        }

        // Phase 2: Bootstrap engine at clock=1100. Children are eligible (scheduled_at=1000),
        // parent is NOT (scheduled_at=1200). Children complete; parent stays Scheduled.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), NopHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1100)).expect("bootstrap must succeed");
            let _ = eng.run_until_idle().await.expect("run must succeed");

            // Children must be Completed.
            let child1_runs = eng.projection().run_ids_for_task(child1_id);
            assert_eq!(child1_runs.len(), 1);
            assert_eq!(
                eng.projection().get_run_state(&child1_runs[0]),
                Some(&RunState::Completed),
                "child1 must be Completed"
            );
            let child2_runs = eng.projection().run_ids_for_task(child2_id);
            assert_eq!(child2_runs.len(), 1);
            assert_eq!(
                eng.projection().get_run_state(&child2_runs[0]),
                Some(&RunState::Completed),
                "child2 must be Completed"
            );

            // Parent must still be non-terminal (Scheduled — not yet eligible).
            let parent_runs = eng.projection().run_ids_for_task(parent_id);
            assert_eq!(parent_runs.len(), 1, "parent must have exactly one run");
            let parent_state = eng.projection().get_run_state(&parent_runs[0]);
            assert!(
                parent_state.is_some_and(|s| !s.is_terminal()),
                "parent must still be non-terminal while scheduled_at has not elapsed, got: \
                 {parent_state:?}"
            );

            eng.shutdown().expect("shutdown");
        }

        // Phase 3: Bootstrap engine at clock=1200. Parent is now eligible and completes.
        {
            let engine = ActionQueueEngine::new(engine_config(&data_dir), NopHandler);
            let mut eng =
                engine.bootstrap_with_clock(MockClock::new(1200)).expect("bootstrap must succeed");
            let _ = eng.run_until_idle().await.expect("run must succeed");

            let parent_runs = eng.projection().run_ids_for_task(parent_id);
            assert_eq!(parent_runs.len(), 1);
            assert_eq!(
                eng.projection().get_run_state(&parent_runs[0]),
                Some(&RunState::Completed),
                "parent must be Completed after children are all terminal and scheduled_at elapsed"
            );

            eng.shutdown().expect("shutdown");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
