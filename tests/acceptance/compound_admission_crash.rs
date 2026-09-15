mod admission_support;
#[path = "../conformance/harness/process.rs"]
mod process;
use std::sync::Mutex;

use actionqueue_core::mutation::{DurabilityPolicy, MutationAuthority, MutationCommand};
use actionqueue_storage::{
    mutation::MutationAuthorityError,
    recovery::bootstrap::recover_read_only,
    store::{fault, open_store, OpenOptions},
    wal::{fs_writer::WalFsWriter, repair::RepairPolicy},
};
use admission_support::*;
static SERIAL: Mutex<()> = Mutex::new(());
fn reopen(path: &std::path::Path) -> Authority {
    let session = open_store(path, OpenOptions::ReadWrite).unwrap();
    let projection = recover_read_only(&session, RepairPolicy::TruncatePartial).unwrap().projection;
    let writer = WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).unwrap();
    Authority::new(writer, projection).with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    })
}
const POINTS: &[&str] = &[
    "wal_before_append",
    "wal_partial_frame",
    "wal_before_sync",
    "authority_before_publish",
    "authority_after_publish",
];
#[test]
fn injected_failures_preserve_atomicity_and_fence_cached_duplicates() {
    let _guard = SERIAL.lock().unwrap();
    for point in POINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open(dir.path());
        ensure(&mut a, request(1), 42).unwrap();
        let original = image(&a);
        let c = command(with_dependencies(&request(2), vec![id(1)]), 3, 42);
        let mut prepared = a.projection().clone();
        let record = actionqueue_storage::mutation::admission::AdmissionRecord::new(
            attributed(with_dependencies(&request(2), vec![id(1)])),
            c.plan().digest().clone(),
            42,
            3,
        )
        .unwrap();
        let event = actionqueue_storage::wal::event::WalEvent::new(
            3,
            actionqueue_storage::wal::event::WalEventType::AdmissionCommitted {
                record,
                runs: c.plan().runs().to_vec(),
            },
        )
        .with_control((&fixture_host()).into());
        prepared.apply(&event).unwrap();
        let expected_digest = prepared.projection_digest().unwrap();
        fault::fail_once(point);
        let error = a
            .submit_command(
                MutationCommand::AdmissionCommit(c.clone()),
                DurabilityPolicy::Immediate,
            )
            .unwrap_err();
        assert!(a.recovery_required(), "{point}");
        if *point != "authority_after_publish" {
            assert_eq!(image(&a), original);
        }
        assert!(matches!(
            a.lookup_admission(&request(1)),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        assert!(matches!(
            a.submit_command(MutationCommand::AdmissionCommit(c), DurabilityPolicy::Immediate),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        let (mut writer, projection) = a.into_parts();
        use actionqueue_storage::wal::writer::WalWriter;
        assert!(writer.recovery_required());
        assert!(matches!(
            writer.append(&event),
            Err(actionqueue_storage::wal::writer::WalWriterError::Poisoned)
        ));
        let rewrapped = Authority::new(writer, projection).with_host(
            actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            },
        );
        assert!(matches!(
            rewrapped.lookup_admission(&request(1)),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        if matches!(*point, "authority_before_publish" | "authority_after_publish") {
            assert!(matches!(error, MutationAuthorityError::Publication { synced: true, .. }));
        }
        drop(rewrapped);
        let mut a = reopen(dir.path());
        match *point {
            "wal_before_append" | "wal_partial_frame" => assert_eq!(image(&a), original),
            "authority_before_publish" | "authority_after_publish" => {
                assert_eq!(a.projection().projection_digest().unwrap(), expected_digest)
            }
            _ => {
                assert!(matches!(error, MutationAuthorityError::PartialDurability { .. }));
                assert!(
                    [original.metadata.wal_sequence, 3].contains(&a.projection().latest_sequence())
                );
            }
        }
        if a.projection().latest_sequence() == 3 {
            assert_eq!(a.projection().projection_digest().unwrap(), expected_digest);
        }
        let exists = a.projection().latest_sequence() == 3;
        assert_eq!(
            ensure(&mut a, with_dependencies(&request(2), vec![id(1)]), 999).unwrap().is_created(),
            !exists
        );
        assert_eq!(a.projection().latest_sequence(), 3);
        assert_eq!(a.projection().run_count(), 6);
    }
}
#[test]
#[ignore = "invoked only by subprocess crash parent"]
fn crash_child() {
    let path = std::path::PathBuf::from(std::env::var("AQ_ADMISSION_CRASH_ROOT").unwrap());
    let point = std::env::var("AQ_ADMISSION_CRASH_POINT").unwrap();
    let mut a = open(&path.join("store"));
    ensure(&mut a, request(1), 42).unwrap();
    let q = with_dependencies(
        &with_spec(
            &request(2),
            spec(2).with_parent_policy(
                id(1),
                actionqueue_core::task::task_spec::ChildLifecyclePolicy::Detached,
            ),
        ),
        vec![id(1)],
    );
    let c = command(q.clone(), 3, 42);
    let mut prepared = a.projection().clone();
    let record = actionqueue_storage::mutation::admission::AdmissionRecord::new(
        attributed(q),
        c.plan().digest().clone(),
        42,
        3,
    )
    .unwrap();
    prepared
        .apply(
            &actionqueue_storage::wal::event::WalEvent::new(
                3,
                actionqueue_storage::wal::event::WalEventType::AdmissionCommitted {
                    record,
                    runs: c.plan().runs().to_vec(),
                },
            )
            .with_control((&fixture_host()).into()),
        )
        .unwrap();
    std::fs::write(
        path.join("expected.json"),
        serde_json::to_vec(
            &actionqueue_storage::snapshot::build::build_snapshot_from_projection(&prepared, 0)
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    std::fs::write(path.join("original.json"), serde_json::to_vec(&image(&a)).unwrap()).unwrap();
    fault::pause_once(&point);
    let _ =
        a.submit_command(MutationCommand::AdmissionCommit(c), DurabilityPolicy::Immediate).unwrap();
    panic!("did not reach crash boundary");
}
#[test]
fn subprocess_kill_at_each_boundary_recovers_complete_admission_or_none() {
    let _guard = SERIAL.lock().unwrap();
    for point in POINTS {
        let dir = tempfile::tempdir().unwrap();
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "crash_child", "--ignored", "--nocapture"])
            .env("AQ_ADMISSION_CRASH_ROOT", dir.path())
            .env("AQ_ADMISSION_CRASH_POINT", point);
        process::kill_at_prefix(cmd, &format!("AQ_CRASH_BOUNDARY {point} "));
        let mut a = reopen(&dir.path().join("store"));
        let current = image(&a);
        let mut expected: actionqueue_storage::snapshot::model::Snapshot = serde_json::from_slice(
            &std::fs::read(dir.path().join(if current.metadata.wal_sequence == 3 {
                "expected.json"
            } else {
                "original.json"
            }))
            .unwrap(),
        )
        .unwrap();
        expected.tasks.sort_by_key(|t| *t.task_spec.id().as_uuid());
        expected.runs.sort_by_key(|r| r.run_instance.id());
        expected.admissions.sort_by_key(|r| *r.task_id().as_uuid());
        assert_eq!(current, expected, "{point}");
        if matches!(*point, "authority_before_publish" | "authority_after_publish") {
            assert_eq!(current.metadata.wal_sequence, 3);
        }
        if matches!(*point, "wal_before_append" | "wal_partial_frame") {
            assert_eq!(current.metadata.wal_sequence, 2);
        }
        let q = with_dependencies(
            &with_spec(
                &request(2),
                spec(2).with_parent_policy(
                    id(1),
                    actionqueue_core::task::task_spec::ChildLifecyclePolicy::Detached,
                ),
            ),
            vec![id(1)],
        );
        let outcome = ensure(&mut a, q, 999).unwrap();
        assert_eq!(outcome.is_created(), current.metadata.wal_sequence == 2);
        assert_eq!(a.projection().latest_sequence(), 3);
        assert_eq!(a.projection().admissions().count(), 2);
    }
}

fn attributed(
    q: actionqueue_core::admission::EnsureTaskRequest,
) -> actionqueue_core::admission::EnsureTaskRequest {
    actionqueue_core::admission::EnsureTaskRequest::new(
        q.admission_key().clone(),
        q.task_spec().clone(),
        q.dependencies().to_vec(),
        q.causal_context().clone(),
        Some(fixture_host().attribution),
    )
    .unwrap()
}
fn fixture_host() -> actionqueue_core::control::HostControlContext {
    actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    }
}
