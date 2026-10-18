mod admission_support;
use actionqueue_core::mutation::{
    CancelCommand, CancelTarget, DurabilityPolicy, MutationAuthority, MutationCommand,
};
use actionqueue_storage::{
    recovery::{bootstrap::load_existing_projection, work},
    store::fault,
};
use admission_support::*;

#[test]
fn after_publication_fault_restores_the_complete_pre_mutation_image() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open(dir.path());
        ensure(&mut a, request(1), 42).unwrap();
        let before = image(&a);
        let root = a.projection().clone();
        fault::fail_once("authority_after_publish");
        let result = if cancel {
            a.submit_command(
                MutationCommand::Cancel(CancelCommand {
                    expected_sequence: a.projection().latest_sequence() + 1,
                    target: CancelTarget::Task(id(1)),
                    tenant_id: None,
                    control_context: None,
                    timestamp: 43,
                }),
                DurabilityPolicy::Immediate,
            )
            .map(|_| ())
        } else {
            a.submit_command(
                MutationCommand::AdmissionCommit(command(
                    request(2),
                    a.projection().latest_sequence() + 1,
                    43,
                )),
                DurabilityPolicy::Immediate,
            )
            .map(|_| ())
        };
        assert!(result.is_err());
        assert!(a.recovery_required());
        assert_eq!(image(&a), before);
        assert!(root.shares_revision_with(a.projection()));
        let sequence = a.projection().latest_sequence();
        drop(a);
        let recovered = load_existing_projection(dir.path()).unwrap();
        assert_eq!(recovered.projection.latest_sequence(), sequence + 1);
        assert_eq!(recovered.projection.is_task_canceled(id(1)), cancel);
        assert_eq!(recovered.projection.task_count(), if cancel { 1 } else { 2 });
    }
}

#[test]
fn change_cursors_preserve_multi_frame_updates_without_reconsuming_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let initial = a.projection().latest_sequence();
    ensure(&mut a, request(1), 42).unwrap();
    ensure(&mut a, request(2), 42).unwrap();
    let cursor = a.projection().latest_sequence();
    assert_eq!(a.projection().changed_tasks_since(initial).count(), 2);
    assert_eq!(a.projection().changed_runs_since(initial).count(), 6);
    let root = a.projection().clone();
    work::reset_projection_work();
    assert!(!ensure(&mut a, request(2), 43).unwrap().is_created());
    assert_eq!(work::projection_work(), work::ProjectionWork::default());
    assert!(root.shares_revision_with(a.projection()));
    assert_eq!(a.projection().changed_tasks_since(cursor).count(), 0);
    let _ = a
        .submit_command(
            MutationCommand::Cancel(CancelCommand {
                expected_sequence: cursor + 1,
                target: CancelTarget::Task(id(1)),
                tenant_id: None,
                control_context: None,
                timestamp: 43,
            }),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    assert!(!root.shares_revision_with(a.projection()));
    assert_eq!(a.projection().changed_tasks_since(cursor).collect::<Vec<_>>(), vec![id(1)]);
    assert_eq!(a.projection().changed_runs_since(cursor).count(), 3);
    assert!(!root.is_task_canceled(id(1)));
    let mut published = a.projection().clone();
    published.trim_terminal_history();
    assert!(!published.shares_revision_with(a.projection()));
}
