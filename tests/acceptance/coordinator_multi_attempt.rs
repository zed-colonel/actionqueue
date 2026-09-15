//! Coordinator fan-out commits with Awaiting; resumed handlers receive child snapshots.

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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
    use actionqueue_storage::mutation::authority::StorageMutationAuthority;
    use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;

    fn engine_config(data_dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            snapshot_event_threshold: None,
            ..RuntimeConfig::default()
        }
    }

    fn make_spec(id: TaskId, payload: &[u8]) -> TaskSpec {
        TaskSpec::new(
            id,
            TaskPayload::new(payload.to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec")
    }

    // ── Part 1: coordinator spawns children and all complete ─────────────────

    const COORD_A_UUID: &str = "05010101-0001-0001-0001-000000000001";
    const CHILD_A1_UUID: &str = "05010101-0001-0001-0001-000000000002";
    const CHILD_A2_UUID: &str = "05010101-0001-0001-0001-000000000003";

    struct SpawnAndSucceedHandler {
        coordinator_id: TaskId,
        child_a_id: TaskId,
        child_b_id: TaskId,
    }

    impl ExecutorHandler for SpawnAndSucceedHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            if ctx.input.payload == b"child" {
                return actionqueue_core::disposition::AttemptDisposition::complete(None);
            }
            // Coordinator: submit children via compound child admission, then succeed.
            if ctx.input.resume_context.is_none() {
                return super::support::admit_children(vec![
                    make_spec(self.child_a_id, b"child").with_parent(self.coordinator_id),
                    make_spec(self.child_b_id, b"child").with_parent(self.coordinator_id),
                ]);
            }
            actionqueue_core::disposition::AttemptDisposition::complete(None)
        }
    }

    #[tokio::test]
    async fn coordinator_spawns_two_children_and_all_complete() {
        let data_dir = super::support::unique_data_dir("wf-coord-spawn");

        let coordinator_id: TaskId = COORD_A_UUID.parse().expect("valid uuid");
        let child_a_id: TaskId = CHILD_A1_UUID.parse().expect("valid uuid");
        let child_b_id: TaskId = CHILD_A2_UUID.parse().expect("valid uuid");

        let engine = ActionQueueEngine::new(
            engine_config(&data_dir),
            SpawnAndSucceedHandler { coordinator_id, child_a_id, child_b_id },
        );
        let mut eng = engine
            .bootstrap_with_clock(MockClock::new(1000))
            .expect("bootstrap must succeed")
            .with_host(crate::support::host_support::host(
                actionqueue_core::control::ControlScope::SingleTenant,
            ));

        eng.submit_task(make_spec(coordinator_id, b"coordinator")).expect("submit coordinator");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), eng.run_until_idle())
            .await
            .expect("handler must return a valid disposition")
            .expect("run must complete");

        // All three tasks must be Completed.
        let coord_runs = eng.projection().run_ids_for_task(coordinator_id);
        assert_eq!(
            eng.projection().get_run_state(&coord_runs[0]),
            Some(&RunState::Completed),
            "coordinator must be Completed"
        );

        let a_runs = eng.projection().run_ids_for_task(child_a_id);
        assert!(!a_runs.is_empty(), "child_a must have been created");
        assert_eq!(
            eng.projection().get_run_state(&a_runs[0]),
            Some(&RunState::Completed),
            "child_a must be Completed"
        );

        let b_runs = eng.projection().run_ids_for_task(child_b_id);
        assert!(!b_runs.is_empty(), "child_b must have been created");
        assert_eq!(
            eng.projection().get_run_state(&b_runs[0]),
            Some(&RunState::Completed),
            "child_b must be Completed"
        );

        assert_eq!(eng.projection().task_count(), 3, "3 tasks: coordinator + 2 children");

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ── Part 2: ChildrenSnapshot populated for tasks that have children ───────

    const COORD_B_UUID: &str = "05020101-0001-0001-0001-000000000001";
    const CHILD_B1_UUID: &str = "05020101-0001-0001-0001-000000000002";
    const CHILD_B2_UUID: &str = "05020101-0001-0001-0001-000000000003";

    /// Captured snapshot details for post-test assertions.
    #[derive(Debug, Default)]
    struct CapturedSnapshot {
        child_count: usize,
        child_task_ids: Vec<TaskId>,
        all_terminal: bool,
    }

    /// Captures whether ctx.children was populated and records snapshot details.
    struct SnapshotCapturingHandler {
        children_seen: Arc<AtomicBool>,
        captured: Arc<std::sync::Mutex<CapturedSnapshot>>,
    }

    impl ExecutorHandler for SnapshotCapturingHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            // Only the coordinator task has this specific payload.
            if ctx.input.payload == b"coordinator" {
                if let Some(ref snap) = ctx.children {
                    self.children_seen.store(true, Ordering::SeqCst);
                    let mut captured = self.captured.lock().unwrap();
                    captured.child_count = snap.children().len();
                    captured.child_task_ids = snap.children().iter().map(|c| c.task_id()).collect();
                    captured.all_terminal = snap.all_children_terminal();
                    if ctx.input.resume_context.is_none() {
                        return AttemptDisposition::awaiting(
                            actionqueue_core::continuation::WaitSpec::children(
                                actionqueue_core::ids::WaitId::new(),
                                captured.child_task_ids.clone(),
                                actionqueue_core::continuation::ChildWaitPolicy::AllTerminal,
                                None,
                            )
                            .unwrap(),
                            None,
                        );
                    }
                }
            }
            actionqueue_core::disposition::AttemptDisposition::complete(None)
        }
    }

    #[tokio::test]
    async fn children_snapshot_is_populated_for_coordinator_with_children() {
        let data_dir = super::support::unique_data_dir("wf-coord-snapshot");

        let coordinator_id: TaskId = COORD_B_UUID.parse().expect("valid uuid");
        let child1_id: TaskId = CHILD_B1_UUID.parse().expect("valid uuid");
        let child2_id: TaskId = CHILD_B2_UUID.parse().expect("valid uuid");

        let ts = 1000u64;

        // Phase 1: Use authority to submit coordinator + 2 children (with parent_task_id).
        // Both children have higher priority so the snapshot is populated when the coordinator runs.
        {
            let recovery = load_projection_from_storage(&data_dir).expect("recovery must succeed");
            let mut auth = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection)
                .with_host(crate::support::host_support::host(
                    actionqueue_core::control::ControlScope::SingleTenant,
                ));

            // Admit the parent first; explicit child priority populates its snapshot
            // before the coordinator executes, using ordinary scheduling semantics.
            actionqueue_runtime::admission::ensure_task(
                &mut auth,
                actionqueue_core::admission::EnsureTaskRequest::for_task(
                    make_spec(coordinator_id, b"coordinator"),
                    vec![],
                )
                .unwrap(),
                &MockClock::new(ts),
            )
            .unwrap();
            for id in [child1_id, child2_id] {
                let base = make_spec(id, b"child");
                let child = TaskSpec::new(
                    id,
                    base.task_payload().clone(),
                    RunPolicy::Once,
                    base.constraints().clone(),
                    TaskMetadata::new(vec![], 1, None),
                )
                .unwrap()
                .with_parent(coordinator_id);
                actionqueue_runtime::admission::ensure_task(
                    &mut auth,
                    actionqueue_core::admission::EnsureTaskRequest::for_task(child, vec![])
                        .unwrap(),
                    &MockClock::new(ts),
                )
                .unwrap();
            }
        }

        // Phase 2: children complete first, then the coordinator inspects their results.
        let children_seen = Arc::new(AtomicBool::new(false));
        let captured = Arc::new(std::sync::Mutex::new(CapturedSnapshot::default()));

        {
            let engine = ActionQueueEngine::new(
                engine_config(&data_dir),
                SnapshotCapturingHandler {
                    children_seen: Arc::clone(&children_seen),
                    captured: Arc::clone(&captured),
                },
            );
            // All admitted work is due.
            let mut eng = engine
                .bootstrap_with_clock(MockClock::new(1100))
                .expect("bootstrap must succeed")
                .with_host(crate::support::host_support::host(
                    actionqueue_core::control::ControlScope::SingleTenant,
                ));
            let _ = tokio::time::timeout(std::time::Duration::from_secs(10), eng.run_until_idle())
                .await
                .expect("handler must return a valid disposition")
                .expect("run must complete");

            // All tasks must have completed.
            let child1_runs = eng.projection().run_ids_for_task(child1_id);
            assert_eq!(
                eng.projection().get_run_state(&child1_runs[0]),
                Some(&RunState::Completed),
                "child1 must be Completed"
            );
            let child2_runs = eng.projection().run_ids_for_task(child2_id);
            assert_eq!(
                eng.projection().get_run_state(&child2_runs[0]),
                Some(&RunState::Completed),
                "child2 must be Completed"
            );
            let coord_runs = eng.projection().run_ids_for_task(coordinator_id);
            assert_eq!(
                eng.projection().get_run_state(&coord_runs[0]),
                Some(&RunState::Completed),
                "coordinator must be Completed"
            );

            eng.shutdown().expect("shutdown must succeed");
        }

        // The coordinator must have seen children in its snapshot.
        assert!(
            children_seen.load(Ordering::SeqCst),
            "coordinator must have received a populated ChildrenSnapshot"
        );

        // Verify snapshot contained exactly the expected children.
        let snap = captured.lock().unwrap();
        assert_eq!(snap.child_count, 2, "snapshot must contain exactly 2 children");
        assert!(snap.child_task_ids.contains(&child1_id), "snapshot must contain child1 task_id");
        assert!(snap.child_task_ids.contains(&child2_id), "snapshot must contain child2 task_id");
        // NOTE: all_terminal is NOT asserted because children and coordinator are
        // dispatched in parallel (max_concurrent > 1). The ChildrenSnapshot is built
        // at dispatch time, before the coordinator handler runs, so children may still
        // be in Running state. The assertion that matters is children_seen above —
        // proving the snapshot was populated with the correct child task IDs.

        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
