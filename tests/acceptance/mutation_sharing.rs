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

#[test]
fn snapshot_hydration_restores_derived_coordination_and_change_indexes() {
    use actionqueue_storage::{
        snapshot::{
            build::build_snapshot_from_projection,
            writer::{SnapshotFsWriter, SnapshotWriter},
        },
        wal::writer::WalWriter,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    ensure(&mut a, request(1), 42).unwrap();
    ensure(&mut a, with_dependencies(&request(2), vec![id(1)]), 42).unwrap();
    let snapshot = build_snapshot_from_projection(a.projection(), 43).unwrap();
    let digest = a.projection().projection_digest().unwrap();
    let mut writer = SnapshotFsWriter::new(a.store_session().unwrap()).unwrap();
    writer.write(&snapshot).unwrap();
    writer.close().unwrap();
    let (writer, _) = a.into_parts();
    writer.close().unwrap();
    let recovered = load_existing_projection(dir.path()).unwrap();
    assert!(recovered.snapshot_loaded);
    let p = &recovered.projection;
    assert_eq!(p.projection_digest().unwrap(), digest);
    assert_eq!(p.active_run_ids_for_task(id(1)).count(), 3);
    assert_eq!(p.active_run_ids_for_task(id(2)).count(), 3);
    assert_eq!(p.changed_tasks_since(0).count(), 2);
    assert_eq!(p.changed_runs_since(0).count(), 6);
    assert_eq!(p.prerequisites_of(id(2)).collect::<Vec<_>>(), vec![id(1)]);
    assert_eq!(p.task_terminal_status(id(1)), None);
    recovered.wal_writer.close().unwrap();
}
