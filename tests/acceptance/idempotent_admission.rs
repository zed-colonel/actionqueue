mod admission_support;
#[path = "admission_support/dependencies.rs"]
mod dependencies;
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
        &with_spec(&request(3), spec(3).with_parent(id(1))),
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
