mod signal_support;
use actionqueue_core::{
    bounded::*,
    causal::*,
    continuation::*,
    data_ref::*,
    ids::*,
    limits::*,
    mutation::*,
    time::clock::{Clock, MockClock},
};
use actionqueue_runtime::signals::SignalAdmissionError;
use actionqueue_storage::{
    mutation::MutationAuthorityError,
    store::{open_store, OpenOptions},
};
use signal_support::*;
#[test]
fn duplicates_preserve_first_receipt_control_and_sequences_under_lowered_limits() {
    struct NoClock;
    impl Clock for NoClock {
        fn now(&self) -> u64 {
            panic!("duplicate consulted clock")
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let ingress = SignalIngressContext {
        tenant_id: None,
        control_context: Some(ControlMutationContext::new(OpaqueRef::new("caller/1").unwrap())),
    };
    assert!(
        matches!(admit_signal(&mut a, request(1), ingress.clone(), &MockClock::new(42)).unwrap(), AdmitSignalOutcome::Admitted { sequence, .. } if sequence.get() == 1)
    );
    let digest = a.projection().projection_digest().unwrap();
    a.set_signal_limits(SignalLimits {
        identities: 0,
        bytes: 0,
        record_bytes: 0,
        inline_bytes: 0,
        ..Default::default()
    });
    assert!(
        matches!(admit_signal(&mut a, request(1), Default::default(), &NoClock).unwrap(), AdmitSignalOutcome::AlreadyExists { sequence, .. } if sequence.get() == 1)
    );
    let stale = SignalAdmitCommand::new(0, envelope(1, 900));
    assert!(matches!(
        a.submit_command(MutationCommand::SignalAdmit(stale), DurabilityPolicy::Immediate)
            .unwrap()
            .applied(),
        AppliedMutation::Signal(AdmitSignalOutcome::AlreadyExists { .. })
    ));
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    assert_eq!(
        a.projection().signals().get_signal(None, &id(1)).unwrap().envelope().control_context,
        ingress.control_context
    );
    assert!(matches!(
        admit(&mut a, 2, 43),
        Err(SignalAdmissionError::Rejected(SignalRejection::Capacity))
    ));
    assert_eq!(a.signal_statistics().capacity_rejections, 1);
    drop(a);
    let mut a = reopen(dir.path());
    assert!(matches!(admit(&mut a, 1, 1000).unwrap(), AdmitSignalOutcome::AlreadyExists { .. }));
    assert_eq!(
        a.projection().signals().get_signal(None, &id(1)).unwrap().envelope().received_at,
        42
    );
}
#[test]
fn every_producer_field_conflicts_and_global_sequence_ignores_other_wal_events() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    admit(&mut a, 1, 42).unwrap();
    let before = a.projection().projection_digest().unwrap();
    for field in 0..8 {
        let mut e = envelope(1, 99);
        match field {
            0 => e.namespace = SignalNamespace::new("another").unwrap(),
            1 => e.kind = SignalKind::new("failed").unwrap(),
            2 => e.correlation_id = None,
            3 => {
                e.causation = Some(
                    CausationLink::new(None, None, None, Some(OpaqueRef::new("external").unwrap()))
                        .unwrap(),
                )
            }
            4 => e.source_ref = Some(OpaqueRef::new("source").unwrap()),
            5 => {
                e.payload_hash = Some(ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap())
            }
            6 => e.occurred_at = None,
            _ => {
                e.payload = Some(DataRef::External(ExternalDataRef {
                    scheme: DataScheme::new("blob").unwrap(),
                    locator: OpaqueRef::new("opaque").unwrap(),
                    hash: ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap(),
                    size_bytes: Some(4),
                    content_type: None,
                }))
            }
        }
        assert!(
            matches!(
                submit(&mut a, e),
                Err(MutationAuthorityError::Signal(SignalRejection::Conflict))
            ),
            "field {field}"
        );
    }
    assert_eq!(a.projection().projection_digest().unwrap(), before);
    let _ = a
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(3, 42)).with_control(
                &crate::signal_support::host_support::host(
                    actionqueue_core::control::ControlScope::Store,
                ),
            ),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    assert_eq!(admit(&mut a, 2, 1).unwrap().sequence().get(), 2);
    assert_eq!(a.projection().signals().get_signal(None, &id(2)).unwrap().wal_sequence(), 4);
    assert_eq!(sequences(&a, &filter(), 1), vec![2]);
    assert!(sequences(&a, &filter(), u64::MAX).is_empty());
}
#[test]
fn payload_hash_validation_external_and_hash_only_observations() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let hash = ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap();
    let mut e = envelope(1, 42);
    e.payload = Some(DataRef::Inline(InlineData::new(None, vec![1, 2, 3], hash.clone()).unwrap()));
    assert_eq!(request_from(&e).unwrap_err(), SignalRejection::InvalidHash);
    assert!(matches!(
        submit(&mut a, e.clone()),
        Err(MutationAuthorityError::Signal(SignalRejection::InvalidHash))
    ));
    e.payload = Some(DataRef::External(ExternalDataRef {
        scheme: DataScheme::new("https").unwrap(),
        locator: OpaqueRef::new("https://never-fetched.invalid/object").unwrap(),
        hash: hash.clone(),
        size_bytes: Some(u64::MAX),
        content_type: Some(ContentType::new("application/octet-stream").unwrap()),
    }));
    let _ = submit(&mut a, e.clone()).unwrap();
    e.payload_hash = Some(hash.clone());
    assert!(matches!(
        submit(&mut a, e.clone()).unwrap().applied(),
        AppliedMutation::Signal(AdmitSignalOutcome::AlreadyExists { .. })
    ));
    e.payload_hash = Some(ContentHash::new(HashAlgorithm::Sha256, vec![1; 32]).unwrap());
    assert!(matches!(
        submit(&mut a, e),
        Err(MutationAuthorityError::Signal(SignalRejection::InvalidHash))
    ));
    let mut e = envelope(2, 42);
    e.payload_hash = Some(hash);
    let _ = submit(&mut a, e).unwrap();
    drop(a);
    let a = reopen(dir.path());
    assert_eq!(a.signal_statistics().retained, 2);
}
#[test]
fn indexed_candidates_match_reference_semantics_and_paginate_after_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    for n in 1..=40 {
        let mut e = envelope(n, 42);
        e.correlation_id =
            (n % 3 != 0).then(|| CorrelationId::new(format!("c/{}", n % 2)).unwrap());
        e.source_ref = (n % 5 != 0).then(|| OpaqueRef::new(format!("s/{}", n % 4)).unwrap());
        if n % 7 == 0 {
            e.kind = SignalKind::new("other").unwrap();
        }
        let _ = submit(&mut a, e).unwrap();
    }
    fn check(a: &Authority) {
        let all = a.projection().signals().list_signals(None, SignalSequence::new(0), 100);
        for c in [None, Some("c/0"), Some("c/1"), Some("absent")] {
            for source in [None, Some("s/0"), Some("s/1"), Some("absent")] {
                let mut f = filter();
                f.correlation_id = c.map(|s| CorrelationId::new(s).unwrap());
                f.source_ref = source.map(|s| OpaqueRef::new(s).unwrap());
                for after in [0, 1, 17, 40, u64::MAX] {
                    let expected: Vec<_> = all
                        .iter()
                        .filter(|r| {
                            r.is_retained() && r.sequence().get() > after && f.matches(r.envelope())
                        })
                        .map(|r| r.sequence().get())
                        .collect();
                    assert_eq!(sequences(a, &f, after), expected);
                    let mut page = Vec::new();
                    let mut cursor = SignalSequence::new(after);
                    loop {
                        let next = a.projection().signals().retained_candidates(&f, cursor, 2);
                        if next.is_empty() {
                            break;
                        }
                        for r in next {
                            cursor = r.sequence();
                            page.push(cursor.get());
                        }
                    }
                    assert_eq!(page, expected);
                }
            }
        }
        assert_eq!(
            a.projection()
                .signals()
                .list_signals(None, SignalSequence::new(10), 3)
                .iter()
                .map(|r| r.sequence().get())
                .collect::<Vec<_>>(),
            [11, 12, 13]
        );
    }
    check(&a);
    drop(a);
    let a = reopen(dir.path());
    check(&a);
}
#[test]
fn creation_size_and_resident_capacity_boundaries_do_not_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let host = a.control_context().unwrap().clone();
    let mut e = envelope(1, 42);
    e.control_context = Some(host.attribution.clone());
    let bytes =
        actionqueue_storage::mutation::signal::SignalRecord::new(e, SignalSequence::new(1), 2)
            .unwrap()
            .encoded_bytes_with_control(Some(&(&host).into()))
            .unwrap();
    a.set_signal_limits(SignalLimits { record_bytes: bytes - 1, ..Default::default() });
    assert!(matches!(
        admit(&mut a, 1, 42),
        Err(SignalAdmissionError::Rejected(SignalRejection::TooLarge))
    ));
    a.set_signal_limits(SignalLimits { record_bytes: bytes, bytes, ..Default::default() });
    admit(&mut a, 1, 42).unwrap();
    assert_eq!(a.signal_statistics().bytes, bytes);
    assert!(matches!(
        admit(&mut a, 2, 42),
        Err(SignalAdmissionError::Rejected(SignalRejection::Capacity))
    ));
    assert_eq!(a.projection().latest_sequence(), 2);
    let c = SignalAdmitCommand::new(3, envelope(2, 42));
    assert!(matches!(
        a.submit_command(MutationCommand::SignalAdmit(c), DurabilityPolicy::Deferred),
        Err(MutationAuthorityError::Signal(SignalRejection::ImmediateDurabilityRequired))
    ));
}
#[test]
fn scoped_signal_requires_platform_profile_and_existing_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let mut e = envelope(1, 42);
    e.tenant_id = Some(TenantId::new());
    let err = submit(&mut a, e).unwrap_err();
    assert!(matches!(
        err,
        MutationAuthorityError::Signal(
            SignalRejection::UnsupportedFeature | SignalRejection::TenantMismatch
        ) | MutationAuthorityError::Control(_)
    ));
    assert_eq!(a.projection().latest_sequence(), 1);
}
#[test]
fn local_causation_requires_real_consistent_ancestry() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let mut e = envelope(1, 42);
    e.causation = Some(CausationLink::new(Some(TaskId::new()), None, None, None).unwrap());
    assert!(matches!(
        submit(&mut a, e),
        Err(MutationAuthorityError::Signal(SignalRejection::InvalidCausation))
    ));
}
#[test]
fn prior_development_manifest_is_rejected_without_writes() {
    for version in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        drop(open(dir.path()));
        let manifest = dir.path().join("manifest.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        value["snapshot_schema"] = version.into();
        value["projection_version"] = version.into();
        let bytes = serde_json::to_vec(&value).unwrap();
        std::fs::write(&manifest, &bytes).unwrap();
        assert!(open_store(dir.path(), OpenOptions::ReadWrite).is_err());
        assert_eq!(std::fs::read(&manifest).unwrap(), bytes);
    }
}

#[cfg(feature = "platform")]
#[test]
fn same_identity_across_tenants_isolated_with_local_ancestry_and_profile_validation() {
    use actionqueue_core::platform::TenantRegistration;
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_platform(dir.path());
    let tenants = [TenantId::new(), TenantId::new()];
    for t in tenants {
        let seq = a.projection().latest_sequence() + 1;
        let _ = a
            .submit_command(
                MutationCommand::TenantCreate(TenantCreateCommand::new(
                    seq,
                    TenantRegistration::new(t, "tenant"),
                    1,
                ))
                .with_control(&crate::signal_support::host_support::host(
                    actionqueue_core::control::ControlScope::Store,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
    }
    for (n, tenant) in [Some(tenants[0]), Some(tenants[1])].into_iter().enumerate() {
        let mut e = envelope(1, 42);
        e.tenant_id = tenant;
        let _ = submit(&mut a, e.clone()).unwrap();
        assert_eq!(
            a.projection().signals().get_signal(tenant, &id(1)).unwrap().sequence().get(),
            n as u64 + 1
        );
        let _ = submit(&mut a, e).unwrap();
        let mut f = filter();
        f.tenant_id = tenant;
        assert_eq!(sequences(&a, &f, 0), [n as u64 + 1]);
    }
    let task = TaskId::new();
    let spec = actionqueue_core::task::task_spec::TaskSpec::new(
        task,
        actionqueue_core::task::task_spec::TaskPayload::new(vec![]),
        actionqueue_core::task::run_policy::RunPolicy::Once,
        Default::default(),
        Default::default(),
    )
    .unwrap()
    .with_tenant(tenants[0]);
    let host = signal_support::host_support::tenant(&mut a, tenants[0]).unwrap();
    a.set_control_context(Some(host));
    actionqueue_runtime::admission::ensure_task(
        &mut a,
        actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
        &MockClock::new(1),
    )
    .unwrap();
    let mut e = envelope(2, 42);
    e.causation = Some(CausationLink::new(Some(task), None, None, None).unwrap());
    for tenant in [None, Some(tenants[1])] {
        e.tenant_id = tenant;
        assert!(matches!(
            submit(&mut a, e.clone()),
            Err(MutationAuthorityError::Signal(SignalRejection::TenantMismatch)
                | MutationAuthorityError::Control(_))
        ));
    }
    e.tenant_id = Some(tenants[0]);
    let _ = submit(&mut a, e).unwrap();
    drop(a);
    let a = reopen(dir.path());
    assert_eq!(a.signal_statistics().retained, 3);
    // A platform-capable binary must still respect a store's smaller immutable profile.
    let root = dir.path().join("no-platform");
    let session = open_store(&root, OpenOptions::Initialize { features: vec![] }).unwrap();
    let mut a =
        session.into_authority().unwrap().with_host(crate::signal_support::host_support::host(
            actionqueue_core::control::ControlScope::SingleTenant,
        ));
    // Populate an in-memory tenant solely to separate profile validation from tenant validation.
    a.projection_mut()
        .apply(&actionqueue_storage::wal::event::WalEvent::new(
            2,
            actionqueue_storage::wal::event::WalEventType::TenantCreated {
                tenant_id: tenants[0],
                name: "tenant".into(),
                timestamp: 1,
            },
        ))
        .unwrap();
    let mut e = envelope(1, 42);
    e.tenant_id = Some(tenants[0]);
    assert!(matches!(
        submit(&mut a, e),
        Err(MutationAuthorityError::Signal(SignalRejection::UnsupportedFeature)
            | MutationAuthorityError::Control(_))
    ));
}

#[test]
fn max_inline_payload_replays_and_duplicates_ignore_lower_inline_limits() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    a.set_signal_limits(SignalLimits { inline_bytes: MAX_INLINE_DATA_BYTES, ..Default::default() });
    let bytes = vec![255; MAX_INLINE_DATA_BYTES];
    let hash = ContentHash::new(HashAlgorithm::Sha256, Sha256::digest(&bytes).to_vec()).unwrap();
    let mut e = envelope(1, 42);
    e.payload = Some(DataRef::Inline(
        InlineData::new(Some(ContentType::new("application/octet-stream").unwrap()), bytes, hash)
            .unwrap(),
    ));
    let _ = submit(&mut a, e.clone()).unwrap();
    a.set_signal_limits(SignalLimits { inline_bytes: 0, ..Default::default() });
    let _ = submit(&mut a, e.clone()).unwrap();
    e.signal_id = id(2);
    assert!(matches!(
        submit(&mut a, e),
        Err(MutationAuthorityError::Signal(SignalRejection::TooLarge))
    ));
    drop(a);
    let a = reopen(dir.path());
    assert_eq!(a.signal_statistics().retained, 1);
}
