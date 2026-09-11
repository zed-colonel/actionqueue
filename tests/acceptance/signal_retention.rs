mod signal_support;
use actionqueue_core::{
    bounded::OpaqueRef, causal::ControlMutationContext, continuation::*, ids::*, limits::*,
    mutation::*, time::clock::MockClock,
};
use actionqueue_runtime::signals::{
    pin_signal, retire_signals, unpin_signal, SignalAdmissionError,
};
use actionqueue_storage::{
    mutation::MutationAuthorityError,
    snapshot::{
        build::build_snapshot_from_projection,
        mapping::validate_snapshot,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
    store::{backup_store, restore_store},
};
use signal_support::*;
fn ingress() -> SignalIngressContext {
    SignalIngressContext {
        tenant_id: None,
        control_context: Some(ControlMutationContext::new(OpaqueRef::new("operator").unwrap())),
    }
}
fn pin(name: &str) -> SignalPinId {
    SignalPinId::new(name).unwrap()
}
#[test]
fn independent_pins_retirement_boundaries_and_retired_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    a.set_signal_retention_policy(SignalRetentionPolicy {
        minimum_age_secs: 10,
        minimum_sequence_window: 1,
    });
    for n in 1..=3 {
        admit(&mut a, n, 100).unwrap();
    }
    for now in [0, 99, 100, 110] {
        assert!(a
            .projection()
            .signals()
            .retirement_candidates(
                None,
                SignalRetentionPolicy { minimum_age_secs: 10, minimum_sequence_window: 1 },
                now,
                100
            )
            .is_empty());
    }
    assert_eq!(
        a.projection().signals().retirement_candidates(
            None,
            SignalRetentionPolicy { minimum_age_secs: 10, minimum_sequence_window: 1 },
            111,
            100
        ),
        [SignalSequence::new(1)]
    );
    let clock = MockClock::new(111);
    assert_eq!(pin_signal(&mut a, id(1), pin("a"), ingress(), &clock).unwrap(), 1);
    let wal = a.projection().latest_sequence();
    assert_eq!(
        pin_signal(&mut a, id(1), pin("a"), Default::default(), &MockClock::new(999)).unwrap(),
        0
    );
    assert_eq!(a.projection().latest_sequence(), wal);
    pin_signal(&mut a, id(1), pin("b"), ingress(), &clock).unwrap();
    unpin_signal(&mut a, id(1), pin("a"), ingress(), &clock).unwrap();
    assert_eq!(unpin_signal(&mut a, id(1), pin("a"), ingress(), &clock).unwrap(), 0);
    assert!(matches!(
        retire_signals(&mut a, vec![SignalSequence::new(1)], ingress(), &clock),
        Err(SignalAdmissionError::Rejected(SignalRejection::Protected))
    ));
    assert_eq!(a.signal_statistics().pinned, 1);
    assert_eq!(a.signal_statistics().pins, 1);
    unpin_signal(&mut a, id(1), pin("b"), ingress(), &clock).unwrap();
    let before_bytes = a.signal_statistics().bytes;
    assert_eq!(retire_signals(&mut a, vec![SignalSequence::new(1)], ingress(), &clock).unwrap(), 1);
    assert_eq!(sequences(&a, &filter(), 0), vec![2, 3]);
    assert_eq!(a.signal_statistics().bytes, before_bytes);
    assert_eq!(a.signal_statistics().retired, 1);
    let original = a.projection().signals().get_signal(None, &id(1)).unwrap().clone();
    assert_eq!(original.retirement().unwrap().timestamp, 111);
    assert_eq!(original.retirement().unwrap().control_context, ingress().control_context);
    assert!(matches!(admit(&mut a, 1, 1000).unwrap(), AdmitSignalOutcome::AlreadyExists { .. }));
    assert_eq!(a.projection().signals().get_signal(None, &id(1)).unwrap(), &original);
    assert!(matches!(
        pin_signal(&mut a, id(1), pin("late"), ingress(), &clock),
        Err(SignalAdmissionError::Rejected(SignalRejection::Retired))
    ));
    a.set_signal_limits(SignalLimits { identities: 3, ..Default::default() });
    assert!(matches!(
        admit(&mut a, 4, 111),
        Err(SignalAdmissionError::Rejected(SignalRejection::Capacity))
    ));
    drop(a);
    let a = reopen(dir.path());
    assert_eq!(sequences(&a, &filter(), 0), vec![2, 3]);
    assert_eq!(a.projection().signals().get_signal(None, &id(1)).unwrap(), &original);
}
#[test]
fn stale_batch_rechecks_protection_and_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    a.set_signal_retention_policy(SignalRetentionPolicy {
        minimum_age_secs: 0,
        minimum_sequence_window: 0,
    });
    for n in 1..=4 {
        admit(&mut a, n, 10).unwrap();
    }
    let plan = a.projection().signals().retirement_candidates(
        None,
        SignalRetentionPolicy { minimum_age_secs: 0, minimum_sequence_window: 0 },
        11,
        2,
    );
    assert_eq!(plan, vec![SignalSequence::new(1), SignalSequence::new(2)]);
    pin_signal(&mut a, id(2), pin("late"), Default::default(), &MockClock::new(11)).unwrap();
    let before = a.projection().projection_digest().unwrap();
    assert!(matches!(
        retire_signals(&mut a, plan, Default::default(), &MockClock::new(11)),
        Err(SignalAdmissionError::Rejected(SignalRejection::Protected))
    ));
    assert_eq!(a.projection().projection_digest().unwrap(), before);
    a.set_signal_limits(SignalLimits {
        pins_per_signal: 1,
        pins: 1,
        retirement_batch: 1,
        ..Default::default()
    });
    assert!(matches!(
        pin_signal(&mut a, id(1), pin("more"), Default::default(), &MockClock::new(11)),
        Err(SignalAdmissionError::Rejected(SignalRejection::Capacity))
    ));
    assert!(matches!(
        retire_signals(
            &mut a,
            vec![SignalSequence::new(1), SignalSequence::new(3)],
            Default::default(),
            &MockClock::new(11)
        ),
        Err(SignalAdmissionError::Rejected(SignalRejection::TooLarge))
    ));
    assert!(a.projection().signals().list_signals(None, SignalSequence::new(0), 0).is_empty());
}
#[test]
fn snapshot_tail_wal_only_and_backup_restore_preserve_all_indexes_and_counters() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let mut a = open(&root);
    a.set_signal_retention_policy(SignalRetentionPolicy {
        minimum_age_secs: 0,
        minimum_sequence_window: 0,
    });
    for n in 1..=4 {
        admit(&mut a, n, 10).unwrap();
    }
    pin_signal(&mut a, id(2), pin("keep"), ingress(), &MockClock::new(11)).unwrap();
    retire_signals(&mut a, vec![SignalSequence::new(1)], ingress(), &MockClock::new(11)).unwrap();
    let s = build_snapshot_from_projection(a.projection(), 100).unwrap();
    let mut writer = SnapshotFsWriter::new(a.store_session().unwrap()).unwrap();
    writer.write(&s).unwrap();
    writer.close().unwrap();
    retire_signals(&mut a, vec![SignalSequence::new(3)], ingress(), &MockClock::new(12)).unwrap();
    admit(&mut a, 5, 0).unwrap();
    let digest = a.projection().projection_digest().unwrap();
    let stats = a.signal_statistics();
    drop(a);
    let mut a = reopen(&root);
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    assert_eq!(a.signal_statistics(), stats);
    assert_eq!(sequences(&a, &filter(), 0), [2, 4, 5]);
    assert_eq!(admit(&mut a, 5, 99).unwrap().sequence().get(), 5);
    drop(a);
    backup_store(&root, &dir.path().join("backup")).unwrap();
    restore_store(&dir.path().join("backup"), &dir.path().join("restored")).unwrap();
    let a = reopen(&dir.path().join("restored"));
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    assert_eq!(a.signal_statistics(), stats);
    drop(a);
    std::fs::remove_file(root.join("snapshots/snapshot.bin")).unwrap();
    let mut a = reopen(&root);
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    assert_eq!(a.signal_statistics(), stats);
    assert_eq!(admit(&mut a, 6, 1).unwrap().sequence().get(), 6);
}
#[test]
fn malformed_snapshot_and_replay_transitions_fail_closed() {
    use actionqueue_storage::{
        mutation::signal::SignalsRetiredRecord,
        wal::event::{WalEvent, WalEventType},
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    for n in 1..=3 {
        admit(&mut a, n, 10).unwrap();
    }
    let image = build_snapshot_from_projection(a.projection(), 0).unwrap();
    let mut bad = image.clone();
    bad.last_signal_sequence = 99;
    assert!(validate_snapshot(&bad).is_err());
    let mut bad = image.clone();
    bad.signals.reverse();
    assert!(validate_snapshot(&bad).is_err());
    let mut bad = image.clone();
    bad.signals[1] = bad.signals[0].clone();
    assert!(validate_snapshot(&bad).is_err());
    let mut value = serde_json::to_value(&image.signals[0]).unwrap();
    value["digest"][0] = 99.into();
    assert!(serde_json::from_value::<actionqueue_storage::mutation::signal::SignalRecord>(value)
        .is_err());
    let before = a.projection().projection_digest().unwrap();
    let event = WalEvent::new(
        5,
        WalEventType::SignalsRetired {
            record: SignalsRetiredRecord {
                tenant_id: None,
                sequences: vec![SignalSequence::new(1), SignalSequence::new(99)],
                control: actionqueue_storage::mutation::signal::SignalControlRecord {
                    wal_sequence: 5,
                    timestamp: 11,
                    control_context: None,
                },
            },
        },
    );
    assert!(a.projection_mut().apply(&event).is_err());
    assert_eq!(a.projection().projection_digest().unwrap(), before);
    let c = RetireSignalsCommand {
        expected_sequence: 5,
        tenant_id: None,
        sequences: vec![SignalSequence::new(2), SignalSequence::new(1)],
        timestamp: 11,
        control_context: None,
    };
    assert!(matches!(
        a.submit_command(MutationCommand::RetireSignals(c), DurabilityPolicy::Immediate),
        Err(MutationAuthorityError::Signal(SignalRejection::InvalidEnvelope))
    ));
}
#[test]
fn malformed_signal_wire_fields_versions_and_hard_frame_limits_are_rejected() {
    use actionqueue_storage::{
        mutation::signal::SignalRecord,
        wal::{
            codec,
            event::{WalEvent, WalEventType},
        },
    };
    let original = SignalRecord::new(envelope(1, 42), SignalSequence::new(1), 2).unwrap();
    let value = serde_json::to_value(&original).unwrap();
    for (field, bad) in [
        ("signal_id", serde_json::json!("")),
        ("namespace", serde_json::json!("Upper")),
        ("canonical_version", serde_json::json!(2)),
        ("hash_algorithm", serde_json::json!(9)),
        ("sequence", serde_json::json!(0)),
        ("wal_sequence", serde_json::json!(0)),
        ("pins", serde_json::json!([["",{"wal_sequence":3,"timestamp":43,"context":null}]])),
    ] {
        let mut v = value.clone();
        v[field] = bad;
        assert!(serde_json::from_value::<SignalRecord>(v).is_err(), "{field}");
    }
    let event = WalEvent::new(2, WalEventType::SignalAdmitted { record: original });
    let bytes = codec::encode(&event).unwrap();
    assert_eq!(u16::from_le_bytes(bytes[12..14].try_into().unwrap()), 288);
    // The header must reject oversized signal lengths before allocating/repairing a tail.
    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for b in bytes {
            crc ^= *b as u32;
            for _ in 0..8 {
                crc = (crc >> 1) ^ if crc & 1 == 1 { 0xedb88320 } else { 0 };
            }
        }
        !crc
    }
    for (offset, data) in [
        (14, 2u16.to_le_bytes().to_vec()),
        (40, (MAX_SIGNAL_RECORD_BYTES as u32 - 51).to_le_bytes().to_vec()),
    ] {
        let mut bad = bytes.clone();
        bad[offset..offset + data.len()].copy_from_slice(&data);
        let checksum = crc32(&bad[..48]);
        bad[48..52].copy_from_slice(&checksum.to_le_bytes());
        assert!(codec::decode(&bad).is_err());
    }
}
