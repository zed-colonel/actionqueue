//! Task hierarchy lifecycle: cascade cancellation, orphan prevention, and
//! completion gating.
//!
//! Proves that:
//! 1. When a parent task is canceled, the dispatch loop cascades the cancellation
//!    to all non-terminal descendants.
//! 2. `HierarchyTracker::register_child` rejects children of terminal parents
//!    (orphan prevention contract).
//! 3. A parent task's runs stay non-terminal while children are still running
//!    (storage rejects premature Complete — validates AC-6).

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::mutation::{
        DurabilityPolicy, MutationAuthority, MutationCommand, TaskCancelCommand,
    };
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_engine::time::clock::MockClock;
    use actionqueue_executor_local::handler::{
        AttemptDisposition, ExecutorContext, ExecutorHandler,
    };
    use actionqueue_runtime::config::RuntimeConfig;
    use actionqueue_runtime::engine::ActionQueueEngine;
    use actionqueue_storage::mutation::authority::StorageMutationAuthority;
    use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
    use actionqueue_workflow::hierarchy::{HierarchyError, HierarchyTracker};

    #[derive(Debug)]
    struct NopHandler;

    impl ExecutorHandler for NopHandler {
        fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
            actionqueue_core::disposition::AttemptDisposition::complete(None)
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
            let mut auth = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection)
                .with_host(actionqueue_core::control::HostControlContext {
                    actor_id: None,
                    scope: actionqueue_core::control::ControlScope::SingleTenant,
                    attribution: actionqueue_core::causal::ControlMutationContext::new(
                        actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                    ),
                });

            // Admit each task and its initial run atomically, parent first.
            let parent_spec = TaskSpec::new(
                parent_id,
                TaskPayload::new(b"parent".to_vec()),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .unwrap();
            for spec in [
                parent_spec,
                make_once_spec_with_parent(child1_id, parent_id, b"child1"),
                make_once_spec_with_parent(child2_id, parent_id, b"child2"),
            ] {
                actionqueue_runtime::admission::ensure_task(
                    &mut auth,
                    actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
                    &MockClock::new(ts),
                )
                .unwrap();
            }

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
            let mut eng = engine
                .bootstrap_with_clock(MockClock::new(ts))
                .expect("bootstrap must succeed")
                .with_host(actionqueue_core::control::HostControlContext {
                    actor_id: None,
                    scope: actionqueue_core::control::ControlScope::SingleTenant,
                    attribution: actionqueue_core::causal::ControlMutationContext::new(
                        actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                    ),
                });

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

    // Premature Complete is exercised at the actual mutation boundary.
    mod completion_gate {
        #![allow(dead_code, unused_imports)]
        include!("child_support.rs");
        #[test]
        fn premature_complete_is_rejected_without_output() {
            let dir = tempfile::tempdir().unwrap();
            let mut a = s::open(dir.path());
            let r = running(&mut a, 1, None, false);
            let p = parent(&a, r);
            let spec = child(2, vec![], ChildLifecyclePolicy::Required, p).task_spec().clone();
            admission_support::ensure(
                &mut a,
                actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
                20,
            )
            .unwrap();
            let expected = command(&a, r, spec_for_wait()).expected;
            actionqueue_runtime::disposition::commit(
                &mut a,
                expected,
                AttemptDisposition::complete(Some(DataRef::from_bytes(vec![1]).unwrap())),
                21,
            )
            .unwrap();
            assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Failed));
            let last = a.projection().get_attempt_history(&r).unwrap().last().unwrap();
            assert!(last.output_ref().is_none());
            assert_eq!(last.error(), Some(actionqueue_runtime::config::CHILDREN_NONTERMINAL));
        }
        fn spec_for_wait() -> WaitSpec {
            spec(WaitId::new(), None)
        }
    }
}
