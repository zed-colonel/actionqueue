//! Protocol tests through production capture, recovery, verification and upgrade paths.
use super::{
    build::build_snapshot_from_projection, checkpoint::*, envelope, loader::load_checkpoint,
    wire_v10::*,
};
use crate::{
    mutation::StorageMutationAuthority,
    recovery::{bootstrap::*, projection::ProjectionImageV8, reducer::ReplayReducer, work},
    store::*,
    wal::{
        codec,
        event::{WalEvent, WalEventType as E},
        fs_reader::WalFsReader,
        fs_writer::WalFsWriter,
        reader::WalReader,
        repair::RepairPolicy,
    },
};
use actionqueue_core::{
    bounded::OpaqueRef, causal::ControlMutationContext, control::*, mutation::*,
};
use std::{fs, io::Write};
type Authority = StorageMutationAuthority<WalFsWriter, ReplayReducer>;
fn authority(path: &std::path::Path) -> Authority {
    open_store(path, OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap()
        .with_host(HostControlContext {
            actor_id: None,
            scope: ControlScope::Store,
            attribution: ControlMutationContext::new(OpaqueRef::new("checkpoint-test").unwrap()),
        })
}
fn toggle(a: &mut Authority) {
    let sequence = a.projection().latest_sequence() + 1;
    let command = if a.projection().is_engine_paused() {
        MutationCommand::EngineResume(EngineResumeCommand::new(sequence, sequence))
    } else {
        MutationCommand::EnginePause(EnginePauseCommand::new(sequence, sequence))
    };
    let _ = a.submit_command(command, DurabilityPolicy::Immediate).unwrap();
}
fn admit(a: &mut Authority) -> actionqueue_core::ids::TaskId {
    use actionqueue_core::{
        admission::{AdmissionPlan, EnsureTaskRequest},
        causal::CausalContext,
        ids::*,
        run::RunInstance,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    let task = TaskId::new();
    let q = EnsureTaskRequest::new(
        AdmissionKey::new("upgrade/task").unwrap(),
        TaskSpec::new(
            task,
            TaskPayload::new(vec![0, 255]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
        vec![],
        CausalContext::new(
            TraceId::new("upgrade").unwrap(),
            CorrelationId::new("upgrade").unwrap(),
        ),
        None,
    )
    .unwrap();
    let plan = AdmissionPlan::new(
        q.clone(),
        vec![RunInstance::new_scheduled(task, 10, 10).unwrap()],
        q.digest().unwrap(),
    )
    .unwrap();
    let seq = a.projection().latest_sequence() + 1;
    let host = HostControlContext {
        scope: ControlScope::SingleTenant,
        ..a.control_context().unwrap().clone()
    };
    let _ = a
        .with_control_context(&host, |a| {
            a.submit_command(
                MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(seq, plan, None, 10)),
                DurabilityPolicy::Immediate,
            )
        })
        .unwrap();
    task
}
fn publish(a: &mut Authority) {
    let c = a.capture_checkpoint(10).unwrap();
    publish_checkpoint(c).unwrap();
}
fn rewrite(session: &StoreSession, c: &Checkpoint) {
    fs::write(session.snapshot_path(), envelope::encode_checkpoint(c).unwrap()).unwrap();
}
#[test]
fn every_checkpoint_cut_replays_only_tail_and_next_append_continues_cursor() {
    for cut in 0..=8 {
        let dir = tempfile::tempdir().unwrap();
        let mut a = authority(dir.path());
        for _ in 0..cut {
            toggle(&mut a);
        }
        let candidate = a.capture_checkpoint(10).unwrap();
        for _ in cut..8 {
            toggle(&mut a);
        }
        work::reset_projection_work();
        work::reset_wal_io();
        publish_checkpoint(candidate).unwrap();
        assert_eq!(work::projection_work().reducer_event_visits, 0);
        assert_eq!(work::wal_io(), (0, 0));
        let expected = a.projection().projection_digest().unwrap();
        drop(a);
        work::reset_projection_work();
        work::reset_wal_io();
        let recovered = load_existing_projection(dir.path()).unwrap();
        assert_eq!(work::projection_work().reducer_event_visits, 8 - cut);
        assert_eq!(work::wal_io().0, (8 - cut + 2) as u64);
        assert_eq!(recovered.projection.projection_digest().unwrap(), expected);
        assert!(recovered.projection.control_targets_available());
        assert_eq!(recovered.recovery_observations.wal_replay_events_applied, (8 - cut) as u64);
        let mut a = StorageMutationAuthority::new(recovered.wal_writer, recovered.projection)
            .with_host(HostControlContext {
                actor_id: None,
                scope: ControlScope::Store,
                attribution: ControlMutationContext::new(
                    OpaqueRef::new("checkpoint-test").unwrap(),
                ),
            });
        let sequence = a.projection().latest_sequence();
        // This authority uses the instrumentation wrapper as production startup does.
        let _ = a
            .submit_command(
                MutationCommand::EnginePause(EnginePauseCommand::new(sequence + 1, 20)),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
        let c = a.capture_checkpoint(20).unwrap();
        publish_checkpoint(c).unwrap();
        drop(a);
        assert_eq!(verify_store(dir.path()).unwrap().sequence, sequence + 1);
    }
}
#[test]
fn skipped_prefix_changes_are_detected_by_verify_backup_and_before_repair() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let mut a = authority(&root);
    for _ in 0..4 {
        toggle(&mut a);
    }
    publish(&mut a);
    drop(a);
    let session = open_store(&root, OpenOptions::ReadOnly).unwrap();
    let mut reader = WalFsReader::for_session(&session).unwrap();
    reader.read_next().unwrap();
    let start = reader.cursor().end;
    let event = reader.read_next().unwrap().unwrap();
    let end = reader.cursor().end;
    let mut changed = WalEvent::new(event.sequence(), E::EnginePaused { timestamp: 1 });
    if let Some(c) = event.control() {
        changed = changed.with_control(c.clone());
    }
    let bytes = codec::encode_for_store(&changed, session.manifest().store_id).unwrap();
    assert_eq!(bytes.len() as u64, end - start);
    let path = session.wal_path();
    let mut wal = fs::read(&path).unwrap();
    wal[start as usize..end as usize].copy_from_slice(&bytes);
    fs::write(&path, &wal).unwrap();
    drop(session);
    // The changed pause is overwritten later, so final logical projections agree.
    let recovered = load_existing_projection(&root).unwrap();
    assert!(recovered.snapshot_loaded);
    drop(recovered);
    let before = fs::read(&path).unwrap();
    assert!(verify_store(&root).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(backup_store(&root, &dir.path().join("backup")).is_err());
    let session = open_store(&root, OpenOptions::ReadWrite).unwrap();
    let next = WalEvent::new(6, E::EnginePaused { timestamp: 6 });
    let partial = codec::encode_for_store(&next, session.manifest().store_id).unwrap();
    fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&partial[..20]).unwrap();
    let before = fs::read(&path).unwrap();
    assert!(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).is_err());
    assert_eq!(fs::read(path).unwrap(), before, "repair must not truncate an unverified prefix");
}
#[test]
fn publication_failure_stale_candidates_and_unproven_pairs_do_not_acknowledge() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    publish(&mut a);
    toggle(&mut a);
    let stale = a.capture_checkpoint(20).unwrap();
    toggle(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap().clone();
    let before = fs::read(session.snapshot_path()).unwrap();
    assert!(publish_checkpoint(stale).is_err());
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), before);
    toggle(&mut a);
    for point in ["snapshot_before_rename", "snapshot_after_rename"] {
        let candidate = a.capture_checkpoint(30).unwrap();
        fault::fail_once(point);
        assert!(publish_checkpoint(candidate).is_err());
        assert_eq!(session.published_sequence(), 3);
        assert!(!a.recovery_required());
    }
    publish(&mut a);
    assert_eq!(session.published_sequence(), 4);
    let arbitrary = ReplayReducer::from_projection_image(ProjectionImageV8(
        build_snapshot_from_projection(a.projection(), 30).unwrap(),
    ))
    .unwrap();
    let (writer, _) = a.into_parts();
    let mut unproven = StorageMutationAuthority::new(writer, arbitrary);
    assert!(unproven.capture_checkpoint(30).is_err());
    drop(unproven);
    let mut a = session.clone().into_authority().unwrap();
    fault::fail_once("wal_before_sync");
    assert!(a.capture_checkpoint(30).is_err());
    assert!(a.recovery_required());
    assert_eq!(session.published_sequence(), 4);
}
#[test]
fn invalid_bindings_ordinals_indexes_and_feature_sections_fail_without_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    toggle(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap().clone();
    let original = fs::read(session.snapshot_path()).unwrap();
    for kind in [
        "manifest",
        "offset",
        "hash",
        "commitment-version",
        "index-sequence",
        "index-duplicate",
        "attempt-owner",
        "feature",
    ] {
        fs::write(session.snapshot_path(), &original).unwrap();
        let mut c = load_checkpoint(&session).unwrap().unwrap();
        match kind {
            "manifest" => c.binding.manifest[0] ^= 1,
            "offset" => c.binding.cursor.end += 1,
            "hash" => c.binding.cursor.last_hash[0] ^= 1,
            "commitment-version" => c.binding.commitment_version = 99,
            "index-sequence" => c.inspection.controls.push((0, [1; 16], 0, vec![1])),
            "index-duplicate" => {
                c.inspection.controls.push((0, [1; 16], 0, vec![2]));
                c.inspection.controls.push((0, [1; 16], 0, vec![2]));
            }
            "attempt-owner" => c.inspection.attempts.push(([1; 16], [2; 16])),
            _ => c.snapshot.tenants.push(super::model::SnapshotTenant {
                tenant_id: actionqueue_core::ids::TenantId::new(),
                name: "unsupported".into(),
                created_at: 1,
            }),
        }
        rewrite(&session, &c);
        assert!(recover_read_only(&session, RepairPolicy::Strict).is_err(), "{kind}");
    }
    // Lengths/counts are bounded before allocation; unknown tags/trailing bytes are rejected.
    let mut c = load_checkpoint_after_restore(&session, &original);
    c.binding.cursor.sequence = 0;
    rewrite(&session, &c);
    assert!(recover_read_only(&session, RepairPolicy::Strict).is_err());
    fs::write(session.snapshot_path(), &original).unwrap();
    drop(a);
    drop(session);
    assert!(verify_store(dir.path()).is_ok());
}
fn load_checkpoint_after_restore(s: &StoreSession, bytes: &[u8]) -> Checkpoint {
    fs::write(s.snapshot_path(), bytes).unwrap();
    load_checkpoint(s).unwrap().unwrap()
}
#[test]
fn schema9_upgrade_preserves_source_identity_history_and_requires_explicit_destination() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let dest = dir.path().join("upgraded");
    let mut a = authority(&source);
    for _ in 0..4 {
        toggle(&mut a);
    }
    let task = admit(&mut a);
    let seq = a.projection().latest_sequence() + 1;
    let host = HostControlContext {
        scope: ControlScope::SingleTenant,
        ..a.control_context().unwrap().clone()
    };
    let _ = a
        .with_control_context(&host, |a| {
            a.submit_command(
                MutationCommand::Cancel(CancelCommand {
                    expected_sequence: seq,
                    target: CancelTarget::Task(task),
                    tenant_id: None,
                    control_context: None,
                    timestamp: 11,
                }),
                DurabilityPolicy::Immediate,
            )
        })
        .unwrap();
    let controls: Vec<_> = a
        .projection()
        .control_sequences(crate::recovery::inspection::ControlTarget::Task(task))
        .collect();
    let image = build_snapshot_from_projection(a.projection(), 10).unwrap();
    let expected = a.projection().projection_digest().unwrap();
    let mut manifest = a.store_session().unwrap().manifest().clone();
    let wal_path = a.store_session().unwrap().wal_path();
    drop(a);
    // Construct the released v9 format with its original manifest binding.
    manifest.snapshot_schema = 9;
    fs::write(source.join("manifest.json"), serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    let old = fs::read(&wal_path).unwrap();
    let first_len =
        crate::wal::tail_validation::parse_record_at(&old, 0).unwrap().unwrap().record_len;
    let first = codec::encode_for_store(
        &WalEvent::new(1, E::StoreInitialized { manifest_digest: manifest.digest() }),
        manifest.store_id,
    )
    .unwrap();
    let mut wal = first;
    wal.extend_from_slice(&old[first_len..]);
    fs::write(&wal_path, &wal).unwrap();
    let snapshot = envelope::encode(&image, manifest.store_id).unwrap();
    fs::write(source.join("snapshots/snapshot.bin"), &snapshot).unwrap();
    let before_manifest = fs::read(source.join("manifest.json")).unwrap();
    assert!(open_store(&source, OpenOptions::ReadWrite).is_err());
    assert!(!dest.exists());
    let verified = upgrade_store(&source, &dest).unwrap();
    assert_eq!(verified.store_id, manifest.store_id);
    assert_eq!(verified.projection_digest, expected);
    assert_eq!(fs::read(source.join("manifest.json")).unwrap(), before_manifest);
    assert_eq!(fs::read(&wal_path).unwrap(), wal);
    assert_eq!(fs::read(source.join("snapshots/snapshot.bin")).unwrap(), snapshot);
    let upgraded = fs::read(dest.join("wal/actionqueue.wal")).unwrap();
    let new_len =
        crate::wal::tail_validation::parse_record_at(&upgraded, 0).unwrap().unwrap().record_len;
    assert_eq!(&upgraded[new_len..], &wal[first_len..]);
    let recovered = load_existing_projection(&dest).unwrap();
    assert_eq!(
        recovered
            .projection
            .control_sequences(crate::recovery::inspection::ControlTarget::Task(task))
            .collect::<Vec<_>>(),
        controls
    );
    assert!(recovered
        .projection
        .tasks_by_reference(None, crate::recovery::inspection::ReferenceField::Trace, "upgrade")
        .any(|id| id == task));
    drop(recovered);
    assert_eq!(verify_store(&dest).unwrap().projection_digest, expected);
    assert!(upgrade_store(&source, &dest).is_err());
    fs::remove_file(source.join("snapshots/snapshot.bin")).unwrap();
    assert_eq!(
        upgrade_store(&source, &dir.path().join("wal-only")).unwrap().projection_digest,
        expected
    );
    // A damaged source snapshot is never silently omitted by the upgrade reader.
    fs::write(source.join("snapshots/snapshot.bin"), &snapshot[..21]).unwrap();
    assert!(upgrade_store(&source, &dir.path().join("bad")).is_err());
    assert!(!dir.path().join("bad").exists());
}

fn frozen_checkpoint() -> Checkpoint {
    use actionqueue_core::{
        admission::EnsureTaskRequest,
        causal::CausalContext,
        ids::*,
        run::RunInstance,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    let task = TaskId::from_uuid(uuid::Uuid::from_bytes([5; 16]));
    let run = RunId::from_uuid(uuid::Uuid::from_bytes([6; 16]));
    let q = EnsureTaskRequest::new(
        AdmissionKey::new("frozen").unwrap(),
        TaskSpec::new(
            task,
            TaskPayload::new(vec![0, 1, 255]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
        vec![],
        CausalContext::new(
            TraceId::new("trace").unwrap(),
            CorrelationId::new("correlation").unwrap(),
        ),
        None,
    )
    .unwrap();
    let record =
        crate::mutation::admission::AdmissionRecord::new(q.clone(), q.digest().unwrap(), 1, 2)
            .unwrap();
    let attribution = ControlAttribution::from(&HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("fixture").unwrap()),
    });
    let mut p = ReplayReducer::new();
    p.apply(&WalEvent::new(1, E::StoreInitialized { manifest_digest: [2; 32] })).unwrap();
    p.apply(
        &WalEvent::new(
            2,
            E::AdmissionCommitted {
                record,
                runs: vec![RunInstance::new_scheduled_with_id(run, task, 1, 1).unwrap()],
            },
        )
        .with_control(attribution.clone()),
    )
    .unwrap();
    p.apply(
        &WalEvent::new(
            3,
            E::TaskCancellationCommitted {
                record: crate::mutation::wait::CancelRecord {
                    target: CancelTarget::Task(task),
                    tenant_id: None,
                    control_context: None,
                    sequence: 3,
                    timestamp: 2,
                },
            },
        )
        .with_control(attribution),
    )
    .unwrap();
    Checkpoint {
        binding: Binding {
            store: [1; 16],
            manifest: [2; 32],
            commitment_version: 1,
            cursor: crate::wal::checkpoint::WalCursor {
                sequence: 3,
                end: 123,
                last_start: 100,
                last_hash: [3; 32],
                commitment: [4; 32],
            },
        },
        snapshot: build_snapshot_from_projection(&p, 10).unwrap(),
        inspection: p.inspection_index.export(),
    }
}
#[test]
fn snapshot_wire_fixtures_freeze_schema9_schema10_and_canonical_v9() {
    fn unhex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }
    let c = frozen_checkpoint();
    let v10 = envelope::encode_checkpoint(&c).unwrap();
    let v9 = envelope::encode(&c.snapshot, uuid::Uuid::from_bytes(c.binding.store)).unwrap();
    assert_eq!(v10, unhex(include_str!("../../tests/fixtures/checkpoint-v10.hex")));
    assert_eq!(v9, unhex(include_str!("../../tests/fixtures/checkpoint-v9.hex")));
    assert_eq!(
        crate::recovery::projection::snapshot_digest(&c.snapshot).unwrap().hex,
        "d33749d7335f23e5266f91c2a40289a7645c80e23da76355452466e49d92c607"
    );
    let decoded = envelope::decode_checkpoint(&v10[20..], None).unwrap();
    assert_eq!(decoded.snapshot, c.snapshot);
    assert_eq!(decoded.inspection, c.inspection);
    assert_eq!(
        envelope::decode(&v9[20..], Some(uuid::Uuid::from_bytes(c.binding.store))).unwrap(),
        c.snapshot
    );
}

#[test]
fn recovery_proofs_reject_foreign_sessions_and_replaced_projections() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    drop(authority(&first));
    drop(authority(&second));
    let session = open_store(&first, OpenOptions::ReadWrite).unwrap();
    let foreign = open_store(&second, OpenOptions::ReadWrite).unwrap();
    let mut recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert!(WalFsWriter::from_recovered(foreign, &recovered).is_err());
    let snapshot = build_snapshot_from_projection(&recovered.projection, 0).unwrap();
    recovered.projection =
        ReplayReducer::from_projection_image(ProjectionImageV8(snapshot)).unwrap();
    assert!(WalFsWriter::from_recovered(session.clone(), &recovered).is_err());
    assert!(session.into_authority().is_ok());
}
#[test]
fn binary_record_bounds_ordinals_and_trailing_fields_are_checked() {
    let c = frozen_checkpoint();
    let frame = envelope::encode_checkpoint(&c).unwrap();
    let mut bytes = frame[20..frame.len() - 32].to_vec();
    fn skip_record(bytes: &[u8], p: &mut usize) {
        let n = u32::from_le_bytes(bytes[*p..*p + 4].try_into().unwrap()) as usize;
        *p += 4 + n;
    }
    let mut p = 0;
    skip_record(&bytes, &mut p);
    // First collection's hostile declared size is rejected before allocating rows.
    let mut hostile = bytes.clone();
    hostile[p..p + 4].copy_from_slice(&1_000_001u32.to_le_bytes());
    assert!(super::wire_v10::decode(&hostile).is_err());
    // Skip task and admission sections to the run's checked task ordinal.
    for _ in 0..2 {
        let n = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
        p += 4;
        for _ in 0..n {
            skip_record(&bytes, &mut p);
        }
    }
    let run_count = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
    assert_eq!(run_count, 1);
    p += 4;
    bytes[p + 4 + 16] = 127;
    assert!(super::wire_v10::decode(&bytes).err().unwrap().contains("ordinal"));
    // postcard itself permits trailing bytes; the checkpoint record boundary does not.
    let mut bytes = frame[20..frame.len() - 32].to_vec();
    let n = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    bytes.insert(4 + n as usize, 0);
    bytes[..4].copy_from_slice(&(n + 1).to_le_bytes());
    assert!(super::wire_v10::decode(&bytes).err().unwrap().contains("trailing checkpoint record"));
}

#[test]
fn arbitrary_snapshot_writers_cannot_replace_a_newer_unrecovered_checkpoint() {
    use super::writer::{SnapshotFsWriter, SnapshotWriter};
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    let older = build_snapshot_from_projection(a.projection(), 1).unwrap();
    toggle(&mut a);
    toggle(&mut a);
    publish(&mut a);
    drop(a);
    let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
    let before = fs::read(session.snapshot_path()).unwrap();
    let mut writer = SnapshotFsWriter::new(&session).unwrap();
    writer.write(&older).unwrap();
    assert!(writer.close().is_err());
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), before);
}
