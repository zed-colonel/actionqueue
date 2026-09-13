//! Dynamic task submission: handler creates child tasks via compound child admission.
//!
//! Proves that a Coordinator handler can propose new tasks during execution
//! via `AttemptDisposition`, and that those tasks are durably created
//! by the dispatch loop on the following tick.

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
    use actionqueue_executor_local::handler::{
        AttemptDisposition, ExecutorContext, ExecutorHandler,
    };
    use actionqueue_runtime::config::RuntimeConfig;
    use actionqueue_runtime::engine::ActionQueueEngine;

    /// Fixed child task IDs known to both the coordinator handler and the test.
    const CHILD_A_UUID: &str = "04010101-0001-0001-0001-000000000002";
    const CHILD_B_UUID: &str = "04010101-0001-0001-0001-000000000003";
    const COORDINATOR_UUID: &str = "04010101-0001-0001-0001-000000000001";

    /// A coordinator that submits two child tasks on its first (and only) execution.
    struct CoordinatorHandler {
        coordinator_id: TaskId,
        child_a_id: TaskId,
        child_b_id: TaskId,
    }

    impl ExecutorHandler for CoordinatorHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            if ctx.input.resume_context.is_some() {
                // Already submitted — this attempt should not happen (Once policy).
                return AttemptDisposition::complete(None);
            }

            {
                let child_a = TaskSpec::new(
                    self.child_a_id,
                    TaskPayload::new(b"child_a".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .expect("valid child spec")
                .with_parent(self.coordinator_id);

                let child_b = TaskSpec::new(
                    self.child_b_id,
                    TaskPayload::new(b"child_b".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .expect("valid child spec")
                .with_parent(self.coordinator_id);

                super::support::admit_children(vec![child_a, child_b])
            }
        }
    }

    /// A simple handler that always succeeds (used for child tasks).
    struct ChildHandler;

    impl ExecutorHandler for ChildHandler {
        fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
            actionqueue_core::disposition::AttemptDisposition::complete(None)
        }
    }

    /// Routes execution to either CoordinatorHandler or ChildHandler based on payload.
    struct RoutingHandler {
        coordinator: CoordinatorHandler,
        child: ChildHandler,
    }

    impl ExecutorHandler for RoutingHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            let payload = ctx.input.payload.clone();
            if payload == b"coordinator" {
                self.coordinator.execute(ctx)
            } else {
                self.child.execute(ctx)
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
    async fn coordinator_submits_two_children_which_complete_successfully() {
        let data_dir = super::support::unique_data_dir("wf-dynamic-submission");

        let coordinator_id: TaskId = COORDINATOR_UUID.parse().expect("valid uuid");
        let child_a_id: TaskId = CHILD_A_UUID.parse().expect("valid uuid");
        let child_b_id: TaskId = CHILD_B_UUID.parse().expect("valid uuid");

        let engine = ActionQueueEngine::new(
            engine_config(&data_dir),
            RoutingHandler {
                coordinator: CoordinatorHandler { coordinator_id, child_a_id, child_b_id },
                child: ChildHandler,
            },
        );
        let mut eng = engine
            .bootstrap_with_clock(MockClock::new(1000))
            .expect("bootstrap must succeed")
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });

        let coordinator_spec = TaskSpec::new(
            coordinator_id,
            TaskPayload::new(b"coordinator".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid coordinator spec");

        eng.submit_task(coordinator_spec).expect("submit coordinator");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), eng.run_until_idle())
            .await
            .expect("handler must return a valid disposition")
            .expect("run must complete");

        // Coordinator must be Completed.
        let coordinator_runs = eng.projection().run_ids_for_task(coordinator_id);
        assert_eq!(coordinator_runs.len(), 1, "coordinator must have exactly one run");
        assert_eq!(
            eng.projection().get_run_state(&coordinator_runs[0]),
            Some(&RunState::Completed),
            "coordinator run must be Completed"
        );

        // Child A must be created and Completed.
        let child_a_runs = eng.projection().run_ids_for_task(child_a_id);
        assert_eq!(child_a_runs.len(), 1, "child_a must have been created with one run");
        assert_eq!(
            eng.projection().get_run_state(&child_a_runs[0]),
            Some(&RunState::Completed),
            "child_a run must be Completed"
        );

        // Child B must be created and Completed.
        let child_b_runs = eng.projection().run_ids_for_task(child_b_id);
        assert_eq!(child_b_runs.len(), 1, "child_b must have been created with one run");
        assert_eq!(
            eng.projection().get_run_state(&child_b_runs[0]),
            Some(&RunState::Completed),
            "child_b run must be Completed"
        );

        // Both children must have coordinator_id as parent.
        let child_a_spec =
            eng.projection().get_task(&child_a_id).expect("child_a task must exist in projection");
        assert_eq!(
            child_a_spec.parent_task_id(),
            Some(coordinator_id),
            "child_a must have coordinator as parent"
        );

        let child_b_spec =
            eng.projection().get_task(&child_b_id).expect("child_b task must exist in projection");
        assert_eq!(
            child_b_spec.parent_task_id(),
            Some(coordinator_id),
            "child_b must have coordinator as parent"
        );

        // Verify total task count: coordinator + 2 children = 3.
        assert_eq!(
            eng.projection().task_count(),
            3,
            "projection must contain exactly 3 tasks: coordinator + 2 children"
        );

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
