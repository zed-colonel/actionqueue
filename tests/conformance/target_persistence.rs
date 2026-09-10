//! AQ-03 executable gates for identity, replay, corruption, and offline transfer.
use actionqueue_core::{
    ids::{AttemptId, RunId, TaskId},
    mutation::AttemptResultKind,
    run::{state::RunState, RunInstance},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_storage::{
    recovery::{bootstrap::recover_read_only, reducer::ReplayReducer},
    snapshot::{
        build::build_snapshot_from_projection,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
    store::*,
    wal::{
        codec,
        event::{WalEvent, WalEventType as E},
        fs_writer::WalFsWriter,
        repair::RepairPolicy,
        writer::WalWriter,
    },
};
use std::{collections::BTreeMap, fs, path::Path};
fn tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, v: &mut BTreeMap<String, Vec<u8>>) {
        for e in fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            v.insert(
                p.strip_prefix(root).unwrap().to_string_lossy().into(),
                if p.is_file() { fs::read(&p).unwrap() } else { vec![] },
            );
            if p.is_dir() {
                visit(root, &p, v);
            }
        }
    }
    let mut v = BTreeMap::new();
    visit(root, root, &mut v);
    v
}
fn init(path: &Path) -> StoreSession {
    open_store(path, OpenOptions::Initialize { features: vec![] }).unwrap()
}
fn task(id: TaskId) -> TaskSpec {
    TaskSpec::new(
        id,
        TaskPayload::new(vec![0, 1, 255]),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::new(vec!["tag".into()], 37, None),
    )
    .unwrap()
}
fn append(writer: &mut WalFsWriter, projection: &mut ReplayReducer, event: E) {
    let e = WalEvent::new(projection.latest_sequence() + 1, event);
    writer.append(&e).unwrap();
    writer.flush().unwrap();
    projection.apply(&e).unwrap();
}
fn snapshot(session: &StoreSession, projection: &ReplayReducer) {
    let s = build_snapshot_from_projection(projection, 987654).unwrap();
    let mut w = SnapshotFsWriter::new(session).unwrap();
    w.write(&s).unwrap();
    w.close().unwrap();
}
#[test]
fn initialization_identity_and_lifetime_locks() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let session = init(&root);
    assert_eq!(session.manifest().contract, "AQ-CONT-1");
    assert_eq!(
        recover_read_only(&session, RepairPolicy::Strict).unwrap().projection.latest_sequence(),
        1
    );
    let manifest = session.manifest().clone();
    let before = tree(&root);
    assert!(matches!(open_store(&root, OpenOptions::ReadWrite), Err(StoreError::StoreInUse)));
    assert!(matches!(inspect_store(&root), Err(StoreError::StoreInUse)));
    assert!(matches!(backup_store(&root, &dir.path().join("backup")), Err(StoreError::StoreInUse)));
    assert_eq!(tree(&root), before);
    drop(session);
    let a = open_store(&root, OpenOptions::ReadOnly).unwrap();
    let b = open_store(&root, OpenOptions::ReadOnly).unwrap();
    assert_eq!(a.manifest(), &manifest);
    drop((a, b));
    assert_eq!(init(&root).manifest(), &manifest);
}
#[test]
fn concurrent_initializers_publish_one_identity() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let root = root.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                match open_store(&root, OpenOptions::Initialize { features: vec![] }) {
                    Ok(s) => Some(s.manifest().store_id),
                    Err(StoreError::StoreInUse) => None,
                    Err(e) => panic!("{e}"),
                }
            })
        })
        .collect();
    let ids: Vec<_> = threads.into_iter().filter_map(|t| t.join().unwrap()).collect();
    assert!(!ids.is_empty());
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(inspect_store(&root).unwrap().sequence, 1);
}
#[test]
fn malformed_future_and_unsupported_manifests_never_write() {
    for mutation in ["schema", "feature", "extra", "malformed"] {
        let dir = tempfile::tempdir().unwrap();
        drop(init(dir.path()));
        let path = dir.path().join("manifest.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        match mutation {
            "schema" => value["wal_format"] = 99.into(),
            "feature" => value["features"] = serde_json::json!(["unknown"]),
            "extra" => value["metadata"] = serde_json::json!({}),
            _ => {}
        }
        fs::write(
            &path,
            if mutation == "malformed" {
                b"{no".to_vec()
            } else {
                serde_json::to_vec(&value).unwrap()
            },
        )
        .unwrap();
        let before = tree(dir.path());
        assert!(open_store(dir.path(), OpenOptions::Initialize { features: vec![] }).is_err());
        assert!(inspect_store(dir.path()).is_err());
        assert_eq!(tree(dir.path()), before);
    }
}
#[test]
fn exact_projection_survives_every_snapshot_cut_backup_restore_and_next_append() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let session = init(&source);
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut w = WalFsWriter::new(session.clone()).unwrap();
    let t = TaskId::new();
    let t2 = TaskId::new();
    let r = RunId::new();
    let a = AttemptId::new();
    let a2 = AttemptId::new();
    let events = vec![
        E::TaskCreated { task_spec: task(t), timestamp: 10 },
        E::TaskCreated { task_spec: task(t2), timestamp: 10 },
        E::DependencyDeclared { task_id: t2, depends_on: vec![t], timestamp: 11 },
        E::RunCreated { run_instance: RunInstance::new_scheduled_with_id(r, t, 20, 10).unwrap() },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 20,
        },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 21,
        },
        E::LeaseAcquired { run_id: r, owner: "worker".into(), expiry: 100, timestamp: 21 },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 22,
        },
        E::AttemptStarted { run_id: r, attempt_id: a, timestamp: 22 },
        E::AttemptFinished {
            run_id: r,
            attempt_id: a,
            result: AttemptResultKind::Failure,
            error: Some("retry".into()),
            output: None,
            timestamp: 23,
        },
        E::LeaseReleased { run_id: r, owner: "worker".into(), expiry: 100, timestamp: 23 },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Running,
            new_state: RunState::RetryWait,
            timestamp: 23,
        },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::RetryWait,
            new_state: RunState::Ready,
            timestamp: 24,
        },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 25,
        },
        E::LeaseAcquired { run_id: r, owner: "worker2".into(), expiry: 200, timestamp: 25 },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 26,
        },
        E::AttemptStarted { run_id: r, attempt_id: a2, timestamp: 26 },
        E::AttemptFinished {
            run_id: r,
            attempt_id: a2,
            result: AttemptResultKind::Success,
            error: None,
            output: Some(vec![0, 255, 42]),
            timestamp: 27,
        },
        E::RunStateChanged {
            run_id: r,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 27,
        },
        E::TaskCanceled { task_id: t2, timestamp: 28 },
        E::EnginePaused { timestamp: 29 },
    ];
    for e in events {
        append(&mut w, &mut p, e);
        snapshot(&session, &p);
        let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
        assert!(recovered.snapshot_loaded);
        assert_eq!(
            recovered.projection.projection_digest().unwrap(),
            p.projection_digest().unwrap()
        );
    }
    assert_eq!(p.get_run_state(&r), Some(&RunState::Completed));
    assert_eq!(p.get_task(&t).unwrap().metadata().priority(), 37);
    assert_eq!(p.get_run_instance(&r).unwrap().effective_priority(), 37);
    assert_eq!(p.get_attempt_history(&r).unwrap().len(), 2);
    assert_eq!(p.get_attempt_history(&r).unwrap()[1].output(), Some([0, 255, 42].as_slice()));
    assert!(p.is_task_canceled(t2));
    append(&mut w, &mut p, E::EngineResumed { timestamp: 30 });
    let expected = p.projection_digest().unwrap();
    drop((w, session));
    let before = tree(&source);
    let backup = dir.path().join("backup");
    let dest = dir.path().join("restored");
    let b = backup_store(&source, &backup).unwrap();
    let restored = restore_store(&backup, &dest).unwrap();
    assert_eq!(b.store_id, restored.store_id);
    assert_eq!(inspect_store(&dest).unwrap().projection_digest, expected);
    assert_eq!(tree(&source), before);
    let session = open_store(&dest, OpenOptions::ReadWrite).unwrap();
    let mut w = WalFsWriter::new(session).unwrap();
    let next = w.current_sequence() + 1;
    w.append(&WalEvent::new(next, E::EnginePaused { timestamp: 31 })).unwrap();
    w.flush().unwrap();
    drop(w);
    assert_eq!(inspect_store(&dest).unwrap().sequence, next);
}
#[test]
fn only_incomplete_final_target_frames_are_repairable() {
    let dir = tempfile::tempdir().unwrap();
    let session = init(dir.path());
    let manifest = session.manifest().clone();
    drop(session);
    let path = dir.path().join("wal/actionqueue.wal");
    let initial = fs::read(&path).unwrap();
    let frame = codec::encode_for_store(
        &WalEvent::new(2, E::EnginePaused { timestamp: 10 }),
        manifest.store_id,
    )
    .unwrap();
    for cut in 1..frame.len() {
        let mut bytes = initial.clone();
        bytes.extend_from_slice(&frame[..cut]);
        fs::write(&path, &bytes).unwrap();
        let before = tree(dir.path());
        let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
        assert!(recover_read_only(&session, RepairPolicy::Strict).is_err());
        assert_eq!(tree(dir.path()), before);
        drop(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).unwrap());
        assert_eq!(fs::read(&path).unwrap(), initial);
    }
    for field in [0, 8, 16, 32, 40, 44, 48, codec::HEADER_LEN] {
        let mut bad = frame.clone();
        bad[field] ^= 0xff;
        let mut bytes = initial.clone();
        bytes.extend_from_slice(&bad);
        bytes.extend_from_slice(&frame);
        fs::write(&path, &bytes).unwrap();
        let before = tree(dir.path());
        let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
        assert!(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).is_err());
        assert_eq!(tree(dir.path()), before);
    }
}
#[test]
fn invalid_snapshot_semantics_and_mixed_identity_halt_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    let sa = init(&a);
    let sb = init(&b);
    snapshot(&sa, &recover_read_only(&sa, RepairPolicy::Strict).unwrap().projection);
    drop((sa, sb));
    fs::copy(a.join("snapshots/snapshot.bin"), b.join("snapshots/snapshot.bin")).unwrap();
    let before = tree(&b);
    assert!(inspect_store(&b).is_err());
    assert_eq!(tree(&b), before);
    fs::remove_file(b.join("wal/actionqueue.wal")).unwrap();
    let before = tree(&b);
    assert!(open_store(&b, OpenOptions::ReadWrite).is_err());
    assert_eq!(tree(&b), before);
}
#[test]
fn backup_inventory_checksums_and_destination_refusal_preserve_sources() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    drop(init(&source));
    let backup = dir.path().join("backup");
    backup_store(&source, &backup).unwrap();
    assert!(restore_store(&backup, &source).is_err());
    assert!(restore_store(&backup, &backup.join("nested")).is_err());
    let wal = backup.join("wal/actionqueue.wal");
    let mut bytes = fs::read(&wal).unwrap();
    bytes[0] ^= 1;
    fs::write(&wal, bytes).unwrap();
    let before = tree(&backup);
    let dest = dir.path().join("dest");
    assert!(restore_store(&backup, &dest).is_err());
    assert!(!dest.exists());
    assert_eq!(tree(&backup), before);
}

#[test]
fn publication_failures_leave_sources_and_prior_snapshots_intact() {
    use actionqueue_storage::store::fault::fail_once;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    fail_once("initialize_before_publish");
    assert!(open_store(&root, OpenOptions::Initialize { features: vec![] }).is_err());
    assert!(!root.exists());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    let session = init(&root);
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut writer = WalFsWriter::new(session.clone()).unwrap();
    let initial = tree(&root);
    fail_once("wal_before_append");
    assert!(writer.append(&WalEvent::new(2, E::EnginePaused { timestamp: 1 })).is_err());
    assert_eq!(tree(&root), initial);
    append(&mut writer, &mut p, E::EnginePaused { timestamp: 1 });
    snapshot(&session, &p);
    let saved = fs::read(session.snapshot_path()).unwrap();
    append(&mut writer, &mut p, E::EngineResumed { timestamp: 2 });
    let mut sw = SnapshotFsWriter::new(&session).unwrap();
    sw.write(&build_snapshot_from_projection(&p, 2).unwrap()).unwrap();
    fail_once("snapshot_before_rename");
    assert!(sw.close().is_err());
    assert_eq!(fs::read(session.snapshot_path()).unwrap(), saved);
    drop((writer, session));
    let source = tree(&root);
    let backup = dir.path().join("backup");
    backup_store(&root, &backup).unwrap();
    let dest = dir.path().join("dest");
    fail_once("restore_before_publish");
    assert!(restore_store(&backup, &dest).is_err());
    assert!(!dest.exists());
    assert_eq!(tree(&root), source);
}
#[test]
fn sync_failure_fences_writer_and_recovery_resolves_uncertain_append() {
    let dir = tempfile::tempdir().unwrap();
    let session = init(dir.path());
    let mut writer = WalFsWriter::new(session).unwrap();
    writer.append(&WalEvent::new(2, E::EnginePaused { timestamp: 1 })).unwrap();
    actionqueue_storage::store::fault::fail_once("wal_before_sync");
    assert!(writer.flush().is_err());
    assert!(writer.append(&WalEvent::new(3, E::EngineResumed { timestamp: 2 })).is_err());
    drop(writer);
    let inspected = inspect_store(dir.path()).unwrap();
    assert_eq!(inspected.sequence, 2);
}
#[test]
fn incomplete_and_corrupt_snapshots_fall_back_but_future_schema_and_legacy_keys_halt() {
    let dir = tempfile::tempdir().unwrap();
    let session = init(dir.path());
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut w = WalFsWriter::new(session.clone()).unwrap();
    append(&mut w, &mut p, E::TaskCreated { task_spec: task(TaskId::new()), timestamp: 1 });
    snapshot(&session, &p);
    let original = fs::read(session.snapshot_path()).unwrap();
    for cut in [0, 1, 7, 8, 19, original.len() - 1] {
        fs::write(session.snapshot_path(), &original[..cut]).unwrap();
        let r = recover_read_only(&session, RepairPolicy::Strict).unwrap();
        assert!(!r.snapshot_loaded);
        assert_eq!(r.projection.projection_digest().unwrap(), p.projection_digest().unwrap());
    }
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    fs::write(session.snapshot_path(), corrupt).unwrap();
    assert!(!recover_read_only(&session, RepairPolicy::Strict).unwrap().snapshot_loaded);
    for field in ["snapshot_schema", "projection_version", "legacy_routing", "waits"] {
        let mut value: serde_json::Value = serde_json::from_slice(&original[20..]).unwrap();
        match field {
            "snapshot_schema" | "projection_version" => value[field] = 99.into(),
            "legacy_routing" => {
                value["projection"]["tasks"][0]["task_spec"]["constraints"]
                    [&["required", "capabilities"].join("_")] = serde_json::json!(["legacy"])
            }
            _ => value["reserved"]["waits"] = serde_json::json!([[1]]),
        }
        let payload = serde_json::to_vec(&value).unwrap();
        let mut framed = original[..20].to_vec();
        framed[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        framed[16..20].copy_from_slice(&crc32(&payload).to_le_bytes());
        framed.extend_from_slice(&payload);
        fs::write(session.snapshot_path(), framed).unwrap();
        let before = tree(dir.path());
        assert!(recover_read_only(&session, RepairPolicy::TruncatePartial).is_err());
        assert_eq!(tree(dir.path()), before);
    }
}
fn crc32(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in bytes {
        c ^= *b as u32;
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 == 1 { 0xedb88320 } else { 0 };
        }
    }
    !c
}
#[test]
fn map_insertion_order_and_snapshot_creation_time_do_not_change_digest() {
    let ids = [TaskId::new(), TaskId::new(), TaskId::new()];
    let mut a = ReplayReducer::new();
    let mut b = ReplayReducer::new();
    for (i, id) in ids.iter().enumerate() {
        a.apply(&WalEvent::new(
            i as u64 + 1,
            E::TaskCreated { task_spec: task(*id), timestamp: 42 },
        ))
        .unwrap();
    }
    for (i, id) in ids.iter().rev().enumerate() {
        b.apply(&WalEvent::new(
            i as u64 + 1,
            E::TaskCreated { task_spec: task(*id), timestamp: 42 },
        ))
        .unwrap();
    }
    assert_eq!(a.projection_digest().unwrap(), b.projection_digest().unwrap());
}
#[cfg(unix)]
#[test]
fn symlinks_and_unexpected_backup_entries_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    drop(init(&root));
    let backup = dir.path().join("backup");
    backup_store(&root, &backup).unwrap();
    std::os::unix::fs::symlink(root.join("manifest.json"), backup.join("extra")).unwrap();
    let dest = dir.path().join("dest");
    assert!(restore_store(&backup, &dest).is_err());
    assert!(!dest.exists());
    fs::remove_file(backup.join("extra")).unwrap();
    fs::write(backup.join("extra"), b"x").unwrap();
    assert!(restore_store(&backup, &dest).is_err());
    let alias = dir.path().join("alias");
    std::os::unix::fs::symlink(&root, &alias).unwrap();
    assert!(open_store(&alias, OpenOptions::ReadWrite).is_err());
}

#[test]
fn stores_never_acquire_features_from_a_richer_binary() {
    use actionqueue_core::budget::BudgetDimension;
    let dir = tempfile::tempdir().unwrap();
    let session = init(dir.path());
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut w = WalFsWriter::new(session.clone()).unwrap();
    let id = TaskId::new();
    append(&mut w, &mut p, E::TaskCreated { task_spec: task(id), timestamp: 1 });
    let before = tree(dir.path());
    assert!(w
        .append(&WalEvent::new(
            3,
            E::BudgetAllocated {
                task_id: id,
                dimension: BudgetDimension::Token,
                limit: 10,
                timestamp: 2
            }
        ))
        .is_err());
    assert_eq!(tree(dir.path()), before);
}
#[cfg(all(feature = "budget", feature = "actor", feature = "platform"))]
#[test]
fn feature_projections_preserve_budget_subscription_actor_and_platform_history() {
    use actionqueue_core::{
        budget::BudgetDimension,
        ids::{ActorId, LedgerEntryId, TenantId},
        platform::{Capability, Role},
        subscription::{EventFilter, SubscriptionId},
    };
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    let session = open_store(
        &root,
        OpenOptions::Initialize {
            features: vec!["actor".into(), "budget".into(), "platform".into()],
        },
    )
    .unwrap();
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut w = WalFsWriter::new(session.clone()).unwrap();
    let t = TaskId::new();
    let actor = ActorId::new();
    let tenant = TenantId::new();
    let sub = SubscriptionId::new();
    let ledger = LedgerEntryId::new();
    let events = vec![
        E::TenantCreated { tenant_id: tenant, name: "tenant".into(), timestamp: 1 },
        E::TaskCreated { task_spec: task(t).with_tenant(tenant), timestamp: 1 },
        E::BudgetAllocated {
            task_id: t,
            dimension: BudgetDimension::Token,
            limit: 10,
            timestamp: 2,
        },
        E::BudgetConsumed {
            task_id: t,
            dimension: BudgetDimension::Token,
            amount: 7,
            timestamp: 3,
        },
        E::BudgetReplenished {
            task_id: t,
            dimension: BudgetDimension::Token,
            new_limit: 20,
            timestamp: 4,
        },
        E::BudgetConsumed {
            task_id: t,
            dimension: BudgetDimension::Token,
            amount: 21,
            timestamp: 5,
        },
        E::SubscriptionCreated {
            subscription_id: sub,
            task_id: t,
            filter: EventFilter::TaskCompleted { task_id: t },
            timestamp: 6,
        },
        E::SubscriptionTriggered { subscription_id: sub, timestamp: 7 },
        E::SubscriptionCanceled { subscription_id: sub, timestamp: 8 },
        E::ActorRegistered {
            actor_id: actor,
            identity: "actor".into(),
            executor_traits: vec!["compute".into()],
            department: None,
            heartbeat_interval_secs: 10,
            tenant_id: Some(tenant),
            timestamp: 9,
        },
        E::ActorHeartbeat { actor_id: actor, timestamp: 10 },
        E::RoleAssigned { actor_id: actor, role: Role::Operator, tenant_id: tenant, timestamp: 11 },
        E::CapabilityGranted {
            actor_id: actor,
            capability: Capability::CanExecute,
            tenant_id: tenant,
            timestamp: 12,
        },
        E::LedgerEntryAppended {
            entry_id: ledger,
            tenant_id: tenant,
            ledger_key: "opaque".into(),
            actor_id: Some(actor),
            payload: vec![0, 255, 1],
            timestamp: 13,
        },
        E::CapabilityRevoked {
            actor_id: actor,
            capability: Capability::CanExecute,
            tenant_id: tenant,
            timestamp: 14,
        },
        E::ActorDeregistered { actor_id: actor, timestamp: 15 },
    ];
    for e in events {
        append(&mut w, &mut p, e);
        snapshot(&session, &p);
        assert_eq!(
            recover_read_only(&session, RepairPolicy::Strict)
                .unwrap()
                .projection
                .projection_digest()
                .unwrap(),
            p.projection_digest().unwrap()
        );
    }
    let budget = p.get_budget(&t, BudgetDimension::Token).unwrap();
    assert_eq!(budget.consumed, 21);
    assert_eq!(budget.limit, 20);
    assert!(budget.exhausted);
    assert_eq!(p.get_subscription(&sub).unwrap().triggered_at, Some(7));
    assert_eq!(p.get_actor(&actor).unwrap().last_heartbeat_at, Some(10));
    assert_eq!(p.ledger_entries().next().unwrap().payload, [0, 255, 1]);
    let digest = p.projection_digest().unwrap();
    drop((w, session));
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");
    backup_store(&root, &backup).unwrap();
    restore_store(&backup, &restored).unwrap();
    assert_eq!(inspect_store(&restored).unwrap().projection_digest, digest);
}
#[cfg(feature = "workflow")]
#[test]
fn cron_wire_payload_is_valid_with_and_without_occurrence_limit() {
    let dir = tempfile::tempdir().unwrap();
    let session =
        open_store(dir.path(), OpenOptions::Initialize { features: vec!["workflow".into()] })
            .unwrap();
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut w = WalFsWriter::new(session.clone()).unwrap();
    for max in [None, Some(3)] {
        let mut cron =
            actionqueue_core::task::run_policy::CronPolicy::new("0 * * * * * *").unwrap();
        if let Some(max) = max {
            cron = cron.with_max_occurrences(max).unwrap();
        }
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(vec![]),
            RunPolicy::Cron(cron),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap();
        append(&mut w, &mut p, E::TaskCreated { task_spec: spec, timestamp: 1 });
    }
    snapshot(&session, &p);
    assert_eq!(
        recover_read_only(&session, RepairPolicy::Strict)
            .unwrap()
            .projection
            .projection_digest()
            .unwrap(),
        p.projection_digest().unwrap()
    );
}

#[test]
fn process_lock_holder() {
    let Ok(root) = std::env::var("AQ_TEST_LOCK_HOLDER") else {
        return;
    };
    let path = Path::new(&root);
    let session = init(path);
    let mut writer = WalFsWriter::new(session).unwrap();
    writer.append(&WalEvent::new(2, E::EnginePaused { timestamp: 1 })).unwrap();
    writer.flush().unwrap();
    fs::write(path.with_extension("ready"), b"ready").unwrap();
    loop {
        std::thread::park();
    }
}
#[test]
fn operating_system_releases_writer_lock_after_process_kill() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_lock_holder", "--nocapture"])
        .env("AQ_TEST_LOCK_HOLDER", &root)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..1000 {
        if root.with_extension("ready").exists() {
            break;
        }
        assert!(child.try_wait().unwrap().is_none(), "child exited before owning store");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let ready = root.with_extension("ready").exists();
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!("child did not acquire store");
    }
    assert!(matches!(open_store(&root, OpenOptions::ReadOnly), Err(StoreError::StoreInUse)));
    child.kill().unwrap();
    child.wait().unwrap();
    let inspection = inspect_store(&root).unwrap();
    assert_eq!(inspection.sequence, 2);
}
#[test]
fn malformed_backup_descriptors_refuse_before_destination_creation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("source");
    drop(init(&root));
    let backup = dir.path().join("backup");
    backup_store(&root, &backup).unwrap();
    let descriptor = fs::read(backup.join("backup.json")).unwrap();
    for case in ["version", "traversal", "missing", "duplicate"] {
        let mut v: serde_json::Value = serde_json::from_slice(&descriptor).unwrap();
        match case {
            "version" => v["version"] = 9.into(),
            "traversal" => v["files"][0]["path"] = "../escape".into(),
            "missing" => {
                v["files"].as_array_mut().unwrap().pop();
            }
            _ => {
                let item = v["files"][0].clone();
                v["files"].as_array_mut().unwrap().push(item);
            }
        }
        fs::write(backup.join("backup.json"), serde_json::to_vec(&v).unwrap()).unwrap();
        let before = tree(&backup);
        let dest = dir.path().join(case);
        assert!(restore_store(&backup, &dest).is_err());
        assert!(!dest.exists());
        assert_eq!(tree(&backup), before);
    }
}

#[test]
fn canonical_projection_matches_independent_sha256_vector() {
    let vector: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/aq-cont-1/projection-v1-vector.json"))
            .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let session = init(dir.path());
    let mut p = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    let mut writer = WalFsWriter::new(session).unwrap();
    let spec: TaskSpec =
        serde_json::from_value(vector["projection"]["tasks"][0]["task_spec"].clone()).unwrap();
    append(&mut writer, &mut p, E::TaskCreated { task_spec: spec, timestamp: 42 });
    assert_eq!(p.projection_digest().unwrap().hex, vector["sha256"].as_str().unwrap());
    assert_eq!(
        serde_json::to_value(build_snapshot_from_projection(&p, 0).unwrap()).unwrap(),
        vector["projection"]
    );
}

#[test]
fn durable_append_recovers_after_failure_before_projection_publication() {
    use actionqueue_core::mutation::{
        DurabilityPolicy, EnginePauseCommand, MutationAuthority, MutationCommand,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut authority = init(dir.path()).into_authority().unwrap();
    actionqueue_storage::store::fault::fail_once("authority_before_publish");
    assert!(authority
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(2, 42)),
            DurabilityPolicy::Immediate
        )
        .is_err());
    assert!(!authority.projection().is_engine_paused());
    drop(authority);
    let session = open_store(dir.path(), OpenOptions::ReadOnly).unwrap();
    let recovered = recover_read_only(&session, RepairPolicy::Strict).unwrap();
    assert_eq!(recovered.projection.latest_sequence(), 2);
    assert!(recovered.projection.is_engine_paused());
}
