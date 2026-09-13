mod admission_support;
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::OpaqueRef,
    causal::ControlMutationContext,
    limits::AdmissionLimits,
    mutation::{
        AppliedMutation, DurabilityPolicy, MutationAuthority, MutationCommand, TaskCancelCommand,
    },
};
use actionqueue_storage::{
    snapshot::{
        build::build_snapshot_from_projection,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
    store::{backup_store, restore_store},
};
use admission_support::*;

#[test]
fn created_retry_wal_snapshot_tail_and_backup_preserve_complete_admission() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut a = open(&root);
    let q = request(1);
    let first = ensure(&mut a, q.clone(), 42).unwrap();
    assert!(first.is_created());
    assert_eq!(first.sequence(), 2);
    let original = image(&a);
    assert_eq!(original.runs.len(), 3);
    assert_eq!(original.admissions.len(), 1);
    assert!(matches!(
        ensure(&mut a, q.clone(), 999).unwrap(),
        EnsureTaskOutcome::AlreadyExists { sequence: 2, .. }
    ));
    assert_eq!(image(&a), original);
    drop(a);
    let mut a = open(&root);
    assert!(!ensure(&mut a, q.clone(), 1000).unwrap().is_created());
    assert_eq!(image(&a), original);
    let s = build_snapshot_from_projection(a.projection(), 3000).unwrap();
    let mut w = SnapshotFsWriter::new(a.store_session().unwrap()).unwrap();
    w.write(&s).unwrap();
    w.close().unwrap();
    ensure(&mut a, request(2), 50).unwrap();
    let expected = image(&a);
    let digest = a.projection().projection_digest().unwrap();
    drop(a);
    let mut a = open(&root);
    assert_eq!(image(&a), expected);
    assert!(!ensure(&mut a, q.clone(), 5000).unwrap().is_created());
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    drop(a);
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");
    backup_store(&root, &backup).unwrap();
    restore_store(&backup, &restored).unwrap();
    let mut a = open(&restored);
    assert_eq!(image(&a), expected);
    assert!(!ensure(&mut a, q, 9000).unwrap().is_created());
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
}
#[test]
fn first_control_context_and_identity_survive_lower_limits_and_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let base = request(1);
    let q = EnsureTaskRequest::new(
        base.admission_key().clone(),
        base.task_spec().clone(),
        vec![],
        base.causal_context().clone(),
        Some(ControlMutationContext::new(OpaqueRef::new("caller/first").unwrap())),
    )
    .unwrap();
    ensure(&mut a, q.clone(), 42).unwrap();
    let _ = a
        .submit_command(
            MutationCommand::TaskCancel(TaskCancelCommand::new(3, id(1), 43)),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    a.set_admission_limits(AdmissionLimits {
        payload_bytes: 0,
        initial_runs: 0,
        dependencies: 0,
        content_type_bytes: 0,
        record_bytes: 0,
    });
    assert!(!ensure(&mut a, base, 100).unwrap().is_created());
    assert_eq!(a.projection().task_admission(id(1)).unwrap().request(), &q);
    assert_eq!(a.projection().latest_sequence(), 3);
    drop(a);
    let mut a = open(dir.path());
    assert!(!ensure(&mut a, q, 1000).unwrap().is_created());
}
#[test]
fn identical_and_conflicting_barrier_proposals_resolve_before_stale_sequence() {
    for conflicting in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let a = std::sync::Arc::new(std::sync::Mutex::new(open(dir.path())));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(12));
        let threads: Vec<_> = (0..12)
            .map(|n| {
                let a = a.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let q =
                        if conflicting { with_spec(&request(1), spec(n + 1)) } else { request(1) };
                    // All proposals use the same stale sequence but separately generated run IDs.
                    let c = command(q, 2, 42 + n);
                    barrier.wait();
                    a.lock().unwrap().submit_command(
                        MutationCommand::AdmissionCommit(c),
                        DurabilityPolicy::Immediate,
                    )
                })
            })
            .collect();
        let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| matches!(r, Ok(o) if matches!(o.applied(), AppliedMutation::Admission(EnsureTaskOutcome::Created { .. })))).count(), 1);
        assert_eq!(results.iter().filter(|r| r.is_err()).count(), if conflicting { 11 } else { 0 });
        for error in results.iter().filter_map(|r| r.as_ref().err()) {
            assert!(matches!(
                error,
                actionqueue_storage::mutation::MutationAuthorityError::Admission(
                    actionqueue_core::admission::AdmissionRejection::Conflict { .. }
                )
            ));
        }
        let a = a.lock().unwrap();
        assert_eq!(a.projection().latest_sequence(), 2);
        assert_eq!(a.projection().run_count(), 3);
        assert_eq!(a.projection().admissions().count(), 1);
        let record = a.projection().admissions().next().unwrap();
        assert!(a
            .projection()
            .runs_for_task(record.task_id())
            .all(|r| r.created_at() == record.timestamp()));
    }
}
#[test]
fn dependency_sets_parent_terminal_retry_and_current_dependencies_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    ensure(&mut a, request(1), 42).unwrap();
    ensure(&mut a, request(2), 42).unwrap();
    let q = with_dependencies(
        &with_spec(
            &request(3),
            spec(3).with_parent_policy(
                id(1),
                actionqueue_core::task::task_spec::ChildLifecyclePolicy::Detached,
            ),
        ),
        vec![id(2), id(1), id(2)],
    );
    ensure(&mut a, q.clone(), 42).unwrap();
    let retry = with_dependencies(&q, vec![id(1), id(2)]);
    let _ = a
        .submit_command(
            MutationCommand::TaskCancel(TaskCancelCommand::new(5, id(1), 43)),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    assert!(!ensure(&mut a, retry, 1000).unwrap().is_created());
    let changed = with_spec(&request(4), spec(4).with_parent(id(1)));
    assert!(matches!(
        ensure(&mut a, changed, 1000),
        Err(actionqueue_runtime::admission::AdmissionError::Rejected(
            actionqueue_core::admission::AdmissionRejection::TerminalParent
        ))
    ));
    let _ = a
        .submit_command(
            MutationCommand::DependencyDeclare(
                actionqueue_core::mutation::DependencyDeclareCommand::new(
                    6,
                    id(3),
                    vec![id(1)],
                    44,
                ),
            ),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    assert!(!ensure(&mut a, q.clone(), 5000).unwrap().is_created());
    assert_eq!(
        a.projection().task_admission(id(3)).unwrap().request().dependencies(),
        q.dependencies()
    );
}

// F-018: even a host-bound storage caller cannot persist partial admission facts.
#[test]
fn standalone_creation_cannot_bypass_admission_or_limits() {
    use actionqueue_core::{
        mutation::{RunCreateCommand, TaskCreateCommand},
        run::RunInstance,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    use actionqueue_storage::mutation::{
        authority::MutationValidationError, MutationAuthorityError,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    a.set_admission_limits(AdmissionLimits { payload_bytes: 1, ..Default::default() });
    for size in [1, 2, 65_537] {
        let task = TaskSpec::new(
            id(10),
            TaskPayload::new(vec![0; size]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let before = a.projection().projection_digest().unwrap();
        let result = a.submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(
                a.projection().latest_sequence() + 1,
                task,
                10,
            )),
            DurabilityPolicy::Immediate,
        );
        assert!(matches!(
            result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::TaskCreateRequiresAdmission
            ))
        ));
        assert_eq!(before, a.projection().projection_digest().unwrap());
        assert_eq!(a.projection().task_count(), 0);
        assert_eq!(a.projection().run_count(), 0);
        assert_eq!(a.projection().admissions().count(), 0);
    }
    assert!(ensure(&mut a, request(1), 10).is_err());
    drop(a);
    let mut a = open(dir.path());
    assert_eq!(a.projection().task_count(), 0);
    ensure(&mut a, request(1), 10).unwrap();
    let before = image(&a);
    let extra = RunInstance::new_scheduled(id(1), 31, 30).unwrap();
    assert!(matches!(
        a.submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(
                a.projection().latest_sequence() + 1,
                extra
            )),
            DurabilityPolicy::Immediate
        ),
        Err(MutationAuthorityError::Validation(
            MutationValidationError::RunCreateRequiresCronReplenishment
        ))
    ));
    assert_eq!(image(&a), before);
    drop(a);
    assert_eq!(image(&open(dir.path())), before);
}

#[cfg(feature = "workflow")]
#[test]
fn standalone_run_creation_only_replenishes_next_bounded_cron_occurrence() {
    use actionqueue_core::{
        mutation::{RunCreateCommand, RunStateTransitionCommand},
        run::{RunInstance, RunState},
        task::{
            run_policy::{CronPolicy, RunPolicy},
            task_spec::TaskSpec,
        },
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let base = request(1);
    let task = base.task_spec();
    let policy = CronPolicy::new("* * * * * * *").unwrap().with_max_occurrences(6).unwrap();
    let spec = TaskSpec::new(
        task.id(),
        task.task_payload().clone(),
        RunPolicy::Cron(policy),
        task.constraints().clone(),
        task.metadata().clone(),
    )
    .unwrap();
    ensure(&mut a, with_spec(&base, spec), 10).unwrap();
    let mut runs: Vec<_> = a.projection().runs_for_task(id(1)).cloned().collect();
    runs.sort_by_key(|r| r.scheduled_at());
    assert_eq!(runs.len(), 5);
    let next = runs.last().unwrap().scheduled_at() + 1;
    let create = |a: &mut Authority, scheduled_at| {
        a.submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(
                a.projection().latest_sequence() + 1,
                RunInstance::new_scheduled(id(1), scheduled_at, 10).unwrap(),
            )),
            DurabilityPolicy::Immediate,
        )
    };
    let before = image(&a);
    assert!(create(&mut a, next).is_err(), "full rolling window");
    assert_eq!(image(&a), before);
    let cancel = |a: &mut Authority, run: actionqueue_core::ids::RunId| {
        let _ = a
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    a.projection().latest_sequence() + 1,
                    run,
                    RunState::Scheduled,
                    RunState::Canceled,
                    11,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
    };
    cancel(&mut a, runs[0].id());
    for invalid in [next - 1, next + 1] {
        let before = image(&a);
        assert!(create(&mut a, invalid).is_err(), "duplicate or skipped occurrence");
        assert_eq!(image(&a), before);
    }
    let _ = create(&mut a, next).unwrap();
    cancel(&mut a, runs[1].id());
    let before = image(&a);
    assert!(create(&mut a, next + 1).is_err(), "total occurrence cap");
    assert_eq!(image(&a), before);
    drop(a);
    assert_eq!(image(&open(dir.path())), before);
}
