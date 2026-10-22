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

fn tree(root: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        at: &std::path::Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in fs::read_dir(at).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(path.strip_prefix(root).unwrap().to_owned(), fs::read(path).unwrap());
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn assert_no_staging(root: &std::path::Path) {
    assert!(!fs::read_dir(root)
        .unwrap()
        .any(|entry| { entry.unwrap().file_name().to_string_lossy().starts_with(".aq-stage-") }));
}

#[test]
fn oversized_state_history_cannot_replace_a_published_checkpoint() {
    use super::model::SnapshotRunStateHistoryEntry;
    use actionqueue_core::run::RunState;
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    admit(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap();
    let before = fs::read(session.snapshot_path()).unwrap();
    let mut c = load_checkpoint(session).unwrap().unwrap();
    let run = &mut c.snapshot.runs[0];
    run.run_instance.transition_to(RunState::Ready).unwrap();
    let timestamp = run.run_instance.created_at();
    run.state_history.push(SnapshotRunStateHistoryEntry {
        from: Some(RunState::Scheduled),
        to: RunState::Ready,
        timestamp,
    });
    // Repeated lease expiry is a valid history without attempt records.
    for _ in 0..500_000 {
        for (from, to) in [(RunState::Ready, RunState::Leased), (RunState::Leased, RunState::Ready)]
        {
            run.state_history.push(SnapshotRunStateHistoryEntry {
                from: Some(from),
                to,
                timestamp,
            });
        }
    }
    let projection = ReplayReducer::from_projection_image(ProjectionImageV8(c.snapshot)).unwrap();
    let candidate = CheckpointCandidate::new(session.clone(), projection, c.binding.cursor, 20);
    let error = publish_checkpoint(candidate).unwrap_err();
    assert!(error.to_string().contains("checkpoint collection bound exceeded"), "{error}");
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), before);
    assert_eq!(session.published_sequence(), c.binding.cursor.sequence);
    assert_eq!(fs::read_dir(session.snapshot_path().parent().unwrap()).unwrap().count(), 1);
    recover_read_only(session, RepairPolicy::Strict).unwrap();
}

#[test]
fn nested_checkpoint_collections_share_encode_and_decode_bounds() {
    let mut c = frozen_checkpoint();
    let history = c.snapshot.runs[0].state_history[0].clone();
    c.snapshot.runs[0].state_history.resize(1_000_000, history.clone());
    c.inspection.controls[0].3.resize(1_000_000, 2);
    let bytes = envelope::encode_checkpoint(&c).unwrap();
    let decoded = envelope::decode_checkpoint(&bytes[20..], None).unwrap();
    assert_eq!(decoded.snapshot.runs[0].state_history.len(), 1_000_000);
    assert_eq!(decoded.inspection.controls[0].3.len(), 1_000_000);
    c.snapshot.runs[0].state_history.push(history);
    assert!(envelope::encode_checkpoint(&c).unwrap_err().contains("collection bound exceeded"));
    c.snapshot.runs[0].state_history.pop();
    c.inspection.controls[0].3.push(2);
    assert!(envelope::encode_checkpoint(&c).unwrap_err().contains("collection bound exceeded"));
}

#[test]
fn hydration_preserves_empty_attempt_histories() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    let task = admit(&mut a);
    let run = a.projection().run_ids_for_task(task)[0];
    assert!(a.projection().get_attempt_history(&run).unwrap().is_empty());
    publish(&mut a);
    let recovered = recover_read_only(a.store_session().unwrap(), RepairPolicy::Strict).unwrap();
    assert!(recovered.projection.get_attempt_history(&run).unwrap().is_empty());
}

#[test]
fn corrupted_oversized_length_falls_back_but_complete_oversized_frame_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    admit(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap();
    let expected = a.projection().projection_digest().unwrap();
    let mut bytes = fs::read(session.snapshot_path()).unwrap();
    bytes[15] ^= 0x80;
    fs::write(session.snapshot_path(), &bytes).unwrap();
    let recovered = recover_read_only(session, RepairPolicy::Strict).unwrap();
    assert!(!recovered.snapshot_loaded);
    assert_eq!(recovered.projection.projection_digest().unwrap(), expected);
    assert!(super::loader::load_checkpoint(session).err().unwrap().physical_damage());

    let path = dir.path().join("oversized.bin");
    let len = envelope::MAX_SNAPSHOT_BYTES as u32;
    let mut header = envelope::MAGIC.to_vec();
    header.extend(2u32.to_le_bytes());
    header.extend(len.to_le_bytes());
    header.extend(0u32.to_le_bytes());
    fs::write(&path, header).unwrap();
    fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(u64::from(len) + 20).unwrap();
    let error = super::loader::read_frame(&path, 2).unwrap_err();
    assert!(!error.physical_damage());
    assert!(error.to_string().contains("size bound exceeded"));
}

#[test]
fn interleaved_recoveries_claim_before_checking_and_release_on_stale_cursor() {
    use crate::wal::fs_writer::WalFsWriterInitError;
    let dir = tempfile::tempdir().unwrap();
    let session = open_store(dir.path(), OpenOptions::Initialize { features: vec![] }).unwrap();
    let earlier = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    let later = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    let mut writer = WalFsWriter::from_recovered(session.clone(), &later).unwrap();
    use crate::wal::writer::WalWriter;
    writer.append(&WalEvent::new(2, E::EnginePaused { timestamp: 2 })).unwrap();
    writer.flush().unwrap();
    // The ownership error must win even though the earlier cursor is stale.
    assert!(matches!(
        WalFsWriter::from_recovered(session.clone(), &earlier),
        Err(WalFsWriterInitError::Store(StoreError::StoreInUse))
    ));
    drop(writer);
    assert!(matches!(
        WalFsWriter::from_recovered(session.clone(), &earlier),
        Err(WalFsWriterInitError::Store(StoreError::InvalidStore(_)))
    ));
    // A failed cursor check releases the claim, allowing fresh recovery.
    let fresh = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    let mut writer = WalFsWriter::from_recovered(session.clone(), &fresh).unwrap();
    writer.append(&WalEvent::new(3, E::EngineResumed { timestamp: 3 })).unwrap();
    writer.close().unwrap();
    drop(session);
    assert_eq!(verify_store(dir.path()).unwrap().sequence, 3);
}

#[test]
fn installed_checkpoint_rejects_older_candidate_after_failed_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    publish(&mut a);
    toggle(&mut a);
    let older = a.capture_checkpoint(20).unwrap();
    toggle(&mut a);
    let newer = a.capture_checkpoint(30).unwrap();
    let session = a.store_session().unwrap().clone();
    fault::fail_once("snapshot_after_rename");
    assert!(publish_checkpoint(newer).is_err());
    assert_eq!(session.published_sequence(), 1);
    let installed = fs::read(session.snapshot_path()).unwrap();
    // A lost publication race is reported as staleness, not as store damage.
    let error = publish_checkpoint(older).unwrap_err();
    assert_eq!(error, StoreError::StaleCheckpoint { sequence: 2, installed: 3 });
    assert!(error.to_string().contains("sequence 2 is older than installed checkpoint 3"));
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), installed);
    publish(&mut a);
    assert_eq!(session.published_sequence(), 3);
}

#[test]
fn publication_wal_sync_failure_fences_live_authority_until_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    publish(&mut a);
    toggle(&mut a);
    let candidate = a.capture_checkpoint(20).unwrap();
    // The publication descriptor's sync can cover frames appended after capture.
    toggle(&mut a);
    let session = a.store_session().unwrap().clone();
    let before = fs::read(session.snapshot_path()).unwrap();
    fault::fail_once("snapshot_wal_before_sync");
    assert!(matches!(publish_checkpoint(candidate), Err(StoreError::Io(_))));
    assert!(a.recovery_required());
    assert_eq!(session.published_sequence(), 1);
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), before);
    let before = fs::read(session.wal_path()).unwrap();
    let error = a
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(4, 40)),
            DurabilityPolicy::Immediate,
        )
        .unwrap_err();
    assert!(matches!(error, crate::mutation::MutationAuthorityError::RecoveryRequired));
    assert!(a.capture_checkpoint(40).is_err());
    assert_eq!(fs::read(session.wal_path()).unwrap(), before);
    drop(a);
    let mut a = session.into_authority().unwrap();
    assert!(!a.recovery_required());
    publish(&mut a);
}

#[test]
fn backup_omits_only_physical_snapshot_damage_and_restores_from_wal() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let mut a = authority(&source);
    admit(&mut a);
    publish(&mut a);
    toggle(&mut a);
    let session = a.store_session().unwrap().clone();
    let bytes = fs::read(session.snapshot_path()).unwrap();
    let mut semantic = load_checkpoint(&session).unwrap().unwrap();
    semantic.binding.manifest[0] ^= 1;
    let digest = a.projection().projection_digest().unwrap();
    drop(a);
    drop(session);
    for damage in ["truncated", "crc", "semantic"] {
        let snapshot_path = source.join("snapshots/snapshot.bin");
        let mut damaged = bytes.clone();
        match damage {
            "truncated" => damaged.truncate(21),
            "crc" => damaged[16] ^= 1,
            _ => damaged = envelope::encode_checkpoint(&semantic).unwrap(),
        }
        fs::write(&snapshot_path, damaged).unwrap();
        let before = tree(&source);
        let output = dir.path().join(format!("backup-{damage}"));
        if damage == "semantic" {
            let error = backup_store(&source, &output).unwrap_err();
            assert!(error.to_string().contains("identity mismatch"));
            assert!(!output.exists());
        } else {
            // Full verification rejects the damaged file, naming it and the fallback.
            let error = verify_store(&source).unwrap_err().to_string();
            assert!(error.contains(snapshot_path.to_str().unwrap()), "{error}");
            assert!(error.contains("physically damaged") || error.contains("checksum"), "{error}");
            assert!(error.contains("recover from the complete WAL"), "{error}");
            let descriptor = backup_store(&source, &output).unwrap();
            assert!(!descriptor.files.iter().any(|f| f.path == "snapshots/snapshot.bin"));
            assert!(!output.join("snapshots/snapshot.bin").exists());
            let restored = dir.path().join(format!("restored-{damage}"));
            restore_store(&output, &restored).unwrap();
            assert_eq!(verify_store(&restored).unwrap().projection_digest, digest);
        }
        assert_eq!(tree(&source), before);
        assert_no_staging(dir.path());
    }
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
fn skipped_prefix_changes_are_detected_by_verify_backup_restore_and_before_repair() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let mut a = authority(&root);
    for _ in 0..4 {
        toggle(&mut a);
    }
    publish(&mut a);
    drop(a);
    let backup = dir.path().join("backup");
    let mut descriptor = backup_store(&root, &backup).unwrap();
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
    // Rehashing the descriptor bypasses only the file checksum gate. Restore
    // must still recompute the checkpoint's complete WAL-prefix commitment.
    fs::write(backup.join("wal/actionqueue.wal"), &wal).unwrap();
    use sha2::{Digest, Sha256};
    let wal_file = descriptor.files.iter_mut().find(|f| f.path == "wal/actionqueue.wal").unwrap();
    wal_file.length = wal.len() as u64;
    wal_file.sha256 = format!("{:x}", Sha256::digest(&wal));
    fs::write(backup.join("backup.json"), serde_json::to_vec_pretty(&descriptor).unwrap()).unwrap();
    let backup_before = tree(&backup);
    let restored = dir.path().join("restored");
    let error = restore_store(&backup, &restored).unwrap_err();
    assert!(error.to_string().contains("checkpoint differs from complete WAL prefix"), "{error}");
    assert!(!restored.exists());
    assert_eq!(tree(&backup), backup_before);
    assert_no_staging(dir.path());
    // The changed pause is overwritten later, so final logical projections agree.
    let recovered = load_existing_projection(&root).unwrap();
    assert!(recovered.snapshot_loaded);
    drop(recovered);
    let before = fs::read(&path).unwrap();
    assert!(verify_store(&root).is_err());
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(backup_store(&root, &dir.path().join("bad-backup")).is_err());
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
    assert!(matches!(publish_checkpoint(stale), Err(StoreError::StaleCheckpoint { .. })));
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), before);
    toggle(&mut a);
    for point in ["snapshot_before_rename", "snapshot_after_rename"] {
        let candidate = a.capture_checkpoint(30).unwrap();
        fault::fail_once(point);
        let error = publish_checkpoint(candidate).unwrap_err();
        assert!(matches!(&error, StoreError::Io(m) if m.contains(point)), "{error}");
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
        let error = recover_read_only(&session, RepairPolicy::Strict).unwrap_err();
        if matches!(kind, "offset" | "hash") {
            // A boundary disagreement names the checkpoint, not an I/O failure.
            let sequence = c.binding.cursor.sequence;
            assert_eq!(
                error,
                StoreError::InvalidStore(format!(
                    "snapshot checkpoint at sequence {sequence} does not match the WAL \
                     boundary; run actionqueue store verify"
                )),
                "{kind}"
            );
        }
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
    let image = build_snapshot_from_projection(a.projection(), 10).unwrap();
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
    toggle(&mut a);
    toggle(&mut a);
    let controls: Vec<_> = a
        .projection()
        .control_sequences(crate::recovery::inspection::ControlTarget::Task(task))
        .collect();
    let expected = a.projection().projection_digest().unwrap();
    assert!(image.metadata.wal_sequence < a.projection().latest_sequence());
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
    let error = open_store(&source, OpenOptions::ReadWrite).unwrap_err();
    assert!(error
        .to_string()
        .contains("actionqueue store upgrade --data-dir SOURCE --output DEST"));
    assert!(!dest.exists());
    let before = tree(&source);
    // Fail after staging has a complete destination WAL and checkpoint temp file.
    let failed = dir.path().join("failed-staging");
    fault::fail_once("snapshot_before_rename");
    let error = upgrade_store(&source, &failed).unwrap_err();
    assert!(error.to_string().contains("snapshot_before_rename"));
    assert!(!failed.exists());
    assert_eq!(tree(&source), before);
    assert_no_staging(dir.path());
    let verified = upgrade_store(&source, &dest).unwrap();
    assert_eq!(verified.verification.store_id, manifest.store_id);
    assert_eq!(verified.verification.projection_digest, expected);
    assert_eq!(verified.dropped_tail_bytes, 0);
    assert_eq!(verified.checkpoint_omitted, None);
    assert_eq!(fs::read(source.join("manifest.json")).unwrap(), before_manifest);
    assert_eq!(fs::read(&wal_path).unwrap(), wal);
    assert_eq!(fs::read(source.join("snapshots/snapshot.bin")).unwrap(), snapshot);
    assert_eq!(tree(&source), before);
    assert_no_staging(dir.path());
    let upgraded_session = open_store(&dest, OpenOptions::ReadOnly).unwrap();
    assert!(load_checkpoint(&upgraded_session).unwrap().unwrap().snapshot.timestamp > 0);
    drop(upgraded_session);
    assert!(upgrade_store(&dest, &dir.path().join("already-upgraded"))
        .unwrap_err()
        .to_string()
        .contains("already schema 10"));
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
        upgrade_store(&source, &dir.path().join("wal-only"))
            .unwrap()
            .verification
            .projection_digest,
        expected
    );
    // Damaged snapshots need actionable diagnostics; the source stays unchanged.
    for damage in ["truncated", "crc"] {
        let snapshot_path = source.join("snapshots/snapshot.bin");
        let mut damaged = snapshot.clone();
        if damage == "truncated" {
            damaged.truncate(21);
        } else {
            damaged[16] ^= 1;
        }
        fs::write(&snapshot_path, damaged).unwrap();
        let before = tree(&source);
        let error = upgrade_store(&source, &dir.path().join("bad")).unwrap_err().to_string();
        assert!(error.contains(snapshot_path.to_str().unwrap()), "{error}");
        assert!(error.contains("remove snapshots/snapshot.bin"), "{error}");
        assert!(error.contains("retry actionqueue store upgrade"), "{error}");
        assert!(!dir.path().join("bad").exists());
        assert_eq!(tree(&source), before);
        assert_no_staging(dir.path());
    }
    fs::remove_file(source.join("snapshots/snapshot.bin")).unwrap();
    assert_eq!(
        upgrade_store(&source, &dir.path().join("repaired-upgrade"))
            .unwrap()
            .verification
            .projection_digest,
        expected
    );
}

#[test]
fn repeated_checkpoint_write_replaces_and_truncates_temporary_bytes() {
    use super::writer::{SnapshotFsWriter, SnapshotWriter};
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(&dir.path().join("store"));
    publish(&mut a);
    let session = a.store_session().unwrap();
    let smaller = load_checkpoint(session).unwrap().unwrap();
    admit(&mut a);
    publish(&mut a);
    let larger = load_checkpoint(a.store_session().unwrap()).unwrap().unwrap();
    let path = dir.path().join("rewritten.bin");
    let mut writer = SnapshotFsWriter::new_raw_for_test(path.clone()).unwrap();
    writer.write_checkpoint(&larger).unwrap();
    writer.write_checkpoint(&smaller).unwrap();
    writer.close().unwrap();
    assert_eq!(fs::read(path).unwrap(), envelope::encode_checkpoint(&smaller).unwrap());
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
    let mut logical_v9 = c.snapshot.clone();
    logical_v9.terminal_sequences.clear();
    assert_eq!(
        envelope::decode(&v9[20..], Some(uuid::Uuid::from_bytes(c.binding.store))).unwrap(),
        logical_v9
    );
}

#[test]
fn terminal_sequence_evidence_is_validated_and_full_verification_checks_it_against_wal() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    let task = admit(&mut a);
    let run = a.projection().run_ids_for_task(task)[0];
    let sequence = a.projection().latest_sequence() + 1;
    let host = HostControlContext {
        scope: ControlScope::SingleTenant,
        ..a.control_context().unwrap().clone()
    };
    let _ = a
        .with_control_context(&host, |a| {
            a.submit_command(
                MutationCommand::Cancel(CancelCommand {
                    expected_sequence: sequence,
                    target: CancelTarget::Run(run),
                    tenant_id: None,
                    control_context: None,
                    timestamp: 20,
                }),
                DurabilityPolicy::Immediate,
            )
        })
        .unwrap();
    toggle(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap().clone();
    let original = load_checkpoint(&session).unwrap().unwrap();
    assert_eq!(original.snapshot.terminal_sequences, vec![(run, sequence)]);
    for records in
        [vec![], vec![(run, 0)], vec![(run, sequence), (run, sequence)], vec![(run, sequence + 10)]]
    {
        let mut bad = original.snapshot.clone();
        bad.terminal_sequences = records;
        assert!(ReplayReducer::from_projection_image(ProjectionImageV8(bad)).is_err());
    }
    // This is structurally valid and has the same v9 digest, but lies about the
    // frame that canceled the run. Full verification must compare the auxiliary map.
    let mut forged = original;
    forged.snapshot.terminal_sequences[0].1 += 1;
    assert_eq!(
        crate::recovery::projection::snapshot_digest(&forged.snapshot).unwrap(),
        a.projection().projection_digest().unwrap(),
    );
    rewrite(&session, &forged);
    drop(a);
    drop(session);
    assert!(verify_store(dir.path()).unwrap_err().to_string().contains("complete WAL prefix"));
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

#[test]
fn full_verification_and_arbitrary_writes_reject_run_reordering_with_an_equal_digest() {
    use super::writer::{SnapshotFsWriter, SnapshotWriter};
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
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    let task = TaskId::new();
    let q = EnsureTaskRequest::new(
        AdmissionKey::new("ordering").unwrap(),
        TaskSpec::new(
            task,
            TaskPayload::new(vec![]),
            RunPolicy::repeat(2, 1).unwrap(),
            Default::default(),
            Default::default(),
        )
        .unwrap(),
        vec![],
        CausalContext::new(
            TraceId::new("ordering").unwrap(),
            CorrelationId::new("ordering").unwrap(),
        ),
        None,
    )
    .unwrap();
    let plan = AdmissionPlan::new(
        q.clone(),
        vec![
            RunInstance::new_scheduled(task, 10, 10).unwrap(),
            RunInstance::new_scheduled(task, 11, 10).unwrap(),
        ],
        q.digest().unwrap(),
    )
    .unwrap();
    let host = HostControlContext {
        scope: ControlScope::SingleTenant,
        ..a.control_context().unwrap().clone()
    };
    let _ = a
        .with_control_context(&host, |a| {
            a.submit_command(
                MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(2, plan, None, 10)),
                DurabilityPolicy::Immediate,
            )
        })
        .unwrap();
    publish(&mut a);
    let session = a.store_session().unwrap();
    let mut c = load_checkpoint(session).unwrap().unwrap();
    let digest = crate::recovery::projection::snapshot_digest(&c.snapshot).unwrap();
    c.snapshot.runs.reverse();
    assert_eq!(crate::recovery::projection::snapshot_digest(&c.snapshot).unwrap(), digest);
    let mut writer = SnapshotFsWriter::new(session).unwrap();
    assert!(writer.write(&c.snapshot).is_err());
    drop(writer);
    rewrite(session, &c);
    assert!(crate::store::verify::verify_session(session, false).is_err());
}

/// Rewrites a closed schema-10 store as the released schema-9 format, binding its
/// first frame to the schema-9 manifest. Returns the manifest and WAL path.
fn into_schema9(root: &std::path::Path, a: Authority) -> (StoreManifest, std::path::PathBuf) {
    let mut manifest = a.store_session().unwrap().manifest().clone();
    let wal_path = a.store_session().unwrap().wal_path();
    drop(a);
    manifest.snapshot_schema = 9;
    fs::write(root.join("manifest.json"), serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    let old = fs::read(&wal_path).unwrap();
    let first_len =
        crate::wal::tail_validation::parse_record_at(&old, 0).unwrap().unwrap().record_len;
    let mut wal = codec::encode_for_store(
        &WalEvent::new(1, E::StoreInitialized { manifest_digest: manifest.digest() }),
        manifest.store_id,
    )
    .unwrap();
    wal.extend_from_slice(&old[first_len..]);
    fs::write(&wal_path, &wal).unwrap();
    (manifest, wal_path)
}
fn after_first_frame(wal: &[u8]) -> &[u8] {
    &wal[crate::wal::tail_validation::parse_record_at(wal, 0).unwrap().unwrap().record_len..]
}
/// Part of the frame a crash during the next append would leave.
fn torn_frame(sequence: u64, store_id: uuid::Uuid) -> Vec<u8> {
    let next = WalEvent::new(sequence, E::EnginePaused { timestamp: 50 });
    let mut bytes = codec::encode_for_store(&next, store_id).unwrap();
    bytes.truncate(bytes.len() / 2);
    bytes
}

#[test]
fn publication_refuses_bytes_startup_cannot_load_and_keeps_the_installed_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    let task = admit(&mut a);
    publish(&mut a);
    let session = a.store_session().unwrap().clone();
    let installed = fs::read(session.snapshot_path()).unwrap();
    let c = load_checkpoint(&session).unwrap().unwrap();
    let run = a.projection().run_ids_for_task(task)[0];
    // The encoder accepts this projection, but hydration rejects its image.
    let mut projection =
        ReplayReducer::from_projection_image(ProjectionImageV8(c.snapshot.clone())).unwrap();
    projection.dispatch_sequences.insert(run, 0);
    let candidate = CheckpointCandidate::new(session.clone(), projection, c.binding.cursor, 20);
    let error = publish_checkpoint(candidate).unwrap_err().to_string();
    assert!(error.contains("checkpoint failed its pre-publication load"), "{error}");
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), installed);
    assert_eq!(fs::read_dir(session.snapshot_path().parent().unwrap()).unwrap().count(), 1);
    assert_eq!(session.published_sequence(), c.binding.cursor.sequence);
    assert!(recover_read_only(&session, RepairPolicy::Strict).unwrap().snapshot_loaded);
    // A checkpoint installed by other means names its file and the remedy at startup.
    let mut unloadable = c;
    unloadable.snapshot.dispatch_sequences.push((run, 0));
    rewrite(&session, &unloadable);
    let error = recover_read_only(&session, RepairPolicy::Strict).unwrap_err().to_string();
    let path = session.snapshot_path();
    assert!(error.contains(&format!("snapshot checkpoint {} cannot be loaded", path.display())));
    assert!(error.contains("the WAL is complete"), "{error}");
    assert!(error.contains("recover from the WAL alone"), "{error}");
    fs::write(&path, &installed).unwrap();
    drop(a);
    drop(session);
    assert!(verify_store(dir.path()).is_ok());
}

#[test]
fn upgrade_without_a_publishable_checkpoint_completes_from_the_complete_wal() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let mut a = authority(&source);
    admit(&mut a);
    toggle(&mut a);
    let expected = a.projection().projection_digest().unwrap();
    let sequence = a.projection().latest_sequence();
    into_schema9(&source, a);
    let before = tree(&source);
    let dest = dir.path().join("upgraded");
    // This image is larger than the checkpoint frame bound in effect.
    envelope::TEST_ENCODE_LIMIT.with(|limit| limit.set(64));
    let upgraded = upgrade_store(&source, &dest);
    envelope::TEST_ENCODE_LIMIT.with(|limit| limit.set(envelope::MAX_SNAPSHOT_BYTES));
    let upgraded = upgraded.unwrap();
    let reason = upgraded.checkpoint_omitted.as_deref().unwrap();
    assert!(reason.contains("exceeds maximum size"), "{reason}");
    assert_eq!(upgraded.verification.sequence, sequence);
    assert_eq!(upgraded.verification.snapshot_sequence, 0);
    assert_eq!(upgraded.verification.projection_digest, expected);
    assert_eq!(upgraded.dropped_tail_bytes, 0);
    assert!(!dest.join("snapshots/snapshot.bin").exists());
    assert_eq!(tree(&source), before);
    assert_no_staging(dir.path());
    let recovered = load_existing_projection(&dest).unwrap();
    assert!(!recovered.snapshot_loaded);
    assert_eq!(recovered.projection.projection_digest().unwrap(), expected);
    drop(recovered);
    // The upgraded store publishes once its state fits again.
    let mut a = open_store(&dest, OpenOptions::ReadWrite).unwrap().into_authority().unwrap();
    publish(&mut a);
    drop(a);
    assert_eq!(verify_store(&dest).unwrap().snapshot_sequence, sequence);
}

#[test]
fn upgrade_leaves_out_and_reports_an_incomplete_final_source_frame() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let mut a = authority(&source);
    admit(&mut a);
    toggle(&mut a);
    let expected = a.projection().projection_digest().unwrap();
    let sequence = a.projection().latest_sequence();
    let (manifest, wal_path) = into_schema9(&source, a);
    let complete = fs::read(&wal_path).unwrap();
    let torn = torn_frame(sequence + 1, manifest.store_id);
    fs::OpenOptions::new().append(true).open(&wal_path).unwrap().write_all(&torn).unwrap();
    let before = tree(&source);
    let dest = dir.path().join("upgraded");
    let upgraded = upgrade_store(&source, &dest).unwrap();
    assert_eq!(upgraded.dropped_tail_bytes, torn.len() as u64);
    assert_eq!(upgraded.checkpoint_omitted, None);
    assert_eq!(upgraded.verification.sequence, sequence);
    assert_eq!(upgraded.verification.snapshot_sequence, sequence);
    assert_eq!(upgraded.verification.projection_digest, expected);
    // The source keeps its torn frame; the destination holds exactly the complete ones.
    assert_eq!(tree(&source), before);
    let upgraded_wal = fs::read(dest.join("wal/actionqueue.wal")).unwrap();
    assert_eq!(after_first_frame(&upgraded_wal), after_first_frame(&complete));
    assert_eq!(verify_store(&dest).unwrap().projection_digest, expected);
    // Damage inside a complete frame is corruption, not an interrupted append.
    let mut damaged = complete.clone();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    fs::write(&wal_path, &damaged).unwrap();
    let failed = dir.path().join("damaged");
    assert!(upgrade_store(&source, &failed).is_err());
    assert!(!failed.exists());
    assert_no_staging(dir.path());
}

#[test]
fn torn_final_frame_after_an_installed_checkpoint_is_repaired_and_the_checkpoint_reloads() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = authority(dir.path());
    for _ in 0..4 {
        toggle(&mut a);
    }
    publish(&mut a);
    let checkpoint = a.projection().latest_sequence();
    for _ in 0..3 {
        toggle(&mut a);
    }
    let sequence = a.projection().latest_sequence();
    let expected = a.projection().projection_digest().unwrap();
    let store_id = a.store_session().unwrap().manifest().store_id;
    let path = a.store_session().unwrap().wal_path();
    drop(a);
    let complete = fs::read(&path).unwrap();
    let torn = torn_frame(sequence + 1, store_id);
    fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&torn).unwrap();
    let damaged = fs::read(&path).unwrap();
    // Inspection reports the incomplete tail without repairing it.
    let inspection = inspect_store(dir.path()).unwrap();
    assert_eq!(inspection.tail_health, "incomplete_final_frame");
    assert_eq!(inspection.sequence, sequence);
    assert_eq!(inspection.projection_digest, expected);
    assert_eq!(fs::read(&path).unwrap(), damaged);
    let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
    assert!(recover_read_only(&session, RepairPolicy::Strict).is_err());
    assert_eq!(fs::read(&path).unwrap(), damaged);
    // Repair proves the checkpoint's prefix, then truncates exactly the torn bytes.
    drop(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).unwrap());
    assert_eq!(fs::read(&path).unwrap(), complete);
    let recovered = load_existing_projection(dir.path()).unwrap();
    assert!(recovered.snapshot_loaded);
    assert_eq!(recovered.snapshot_sequence, checkpoint);
    assert_eq!(recovered.projection.latest_sequence(), sequence);
    assert_eq!(recovered.projection.projection_digest().unwrap(), expected);
    drop(recovered);
    let verified = verify_store(dir.path()).unwrap();
    assert_eq!(verified.snapshot_sequence, checkpoint);
    assert_eq!(verified.sequence, sequence);
    assert_eq!(verified.projection_digest, expected);
    assert_eq!(inspect_store(dir.path()).unwrap().tail_health, "clean");
}
