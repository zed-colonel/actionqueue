mod admission_support;
use actionqueue_core::{
    admission::{AdmissionDigest, AdmissionPlan, AdmissionRejection as R, EnsureTaskRequest},
    bounded::{ContentHash, HashAlgorithm, OpaqueRef},
    causal::{CausalContext, CausationLink},
    ids::{CorrelationId, TraceId},
    limits::AdmissionLimits,
    mutation::{AdmissionCommitCommand, DurabilityPolicy, MutationAuthority, MutationCommand},
    run::RunInstance,
    task::{
        constraints::ConcurrencyKeyHoldPolicy, metadata::TaskMetadata, run_policy::RunPolicy,
        safety::SafetyLevel, task_spec::TaskPayload,
    },
};
use actionqueue_runtime::admission::AdmissionError;
use actionqueue_storage::mutation::MutationAuthorityError;
use admission_support::*;
fn conflict(
    a: &mut Authority,
    q: EnsureTaskRequest,
    original: &actionqueue_storage::snapshot::model::Snapshot,
) {
    let e = ensure(a, q, 1000).unwrap_err();
    assert!(
        matches!(&e, AdmissionError::Rejected(R::Conflict { existing_task_id, existing_digest, proposed_digest, .. }) if *existing_task_id == id(1) && existing_digest != proposed_digest),
        "{e}"
    );
    assert_eq!(&image(a), original);
}
#[test]
fn every_stored_meaning_field_conflicts_without_mutation_or_payload_disclosure() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let q = request(1);
    ensure(&mut a, q.clone(), 42).unwrap();
    let original = image(&a);
    let mut variants = vec![spec(2), spec(1).with_parent(id(2))];
    for payload in [
        TaskPayload::new(vec![9]),
        TaskPayload::with_content_type(vec![0, 1, 255], "application/octet-stream"),
        TaskPayload::with_content_type(vec![0, 1, 255], ""),
    ] {
        let mut s = spec(1);
        s.set_payload(payload);
        variants.push(s);
    }
    for policy in
        [RunPolicy::Once, RunPolicy::repeat(2, 7).unwrap(), RunPolicy::repeat(3, 8).unwrap()]
    {
        let mut s = spec(1);
        s.set_run_policy(policy).unwrap();
        variants.push(s);
    }
    for field in 0..6 {
        let mut s = spec(1);
        let mut c = s.constraints().clone();
        match field {
            0 => c.set_max_attempts(7).unwrap(),
            1 => c.set_timeout_secs(Some(9)).unwrap(),
            2 => c.set_concurrency_key(Some("lock".into())).unwrap(),
            3 => c.set_concurrency_key_hold_policy(ConcurrencyKeyHoldPolicy::ReleaseOnRetry),
            4 => c.set_safety_level(SafetyLevel::Transactional),
            _ => c.set_required_executor_traits(Some(vec!["cpu".into()])).unwrap(),
        }
        s.set_constraints(c).unwrap();
        variants.push(s);
    }
    for m in [
        TaskMetadata::new(vec!["a".into()], 0, None),
        TaskMetadata::new(vec![], 1, None),
        TaskMetadata::new(vec![], 0, Some("description".into())),
        TaskMetadata::new(vec![], 0, Some("".into())),
    ] {
        let mut s = spec(1);
        s.set_metadata(m);
        variants.push(s);
    }
    for s in variants {
        conflict(&mut a, with_spec(&q, s), &original);
    }
    conflict(&mut a, with_dependencies(&q, vec![id(2)]), &original);
    let c = q.causal_context();
    let opaque = || OpaqueRef::new("opaque/value").unwrap();
    let variants = vec![
        CausalContext::new(TraceId::new("trace2").unwrap(), c.correlation_id().clone()),
        CausalContext::new(c.trace_id().clone(), CorrelationId::new("correlation2").unwrap()),
        c.clone().with_submitting_principal_ref(opaque()),
        c.clone().with_requesting_actor_ref(opaque()),
        c.clone().with_purpose_ref(opaque()),
        c.clone().with_authorization_ref(opaque()),
        c.clone().with_identity_context_ref(opaque()),
        c.clone().with_signed_statement_ref(opaque()),
        c.clone().with_proof_context_ref(opaque()),
        c.clone().with_origin_ref(opaque()),
        c.clone().with_causation(CausationLink::new(None, None, None, Some(opaque())).unwrap()),
        c.clone().with_causation(CausationLink::new(Some(id(2)), None, None, None).unwrap()),
        c.clone().with_causation(
            CausationLink::new(Some(id(2)), Some(id(3).to_string().parse().unwrap()), None, None)
                .unwrap(),
        ),
        c.clone().with_causation(
            CausationLink::new(
                Some(id(2)),
                Some(id(3).to_string().parse().unwrap()),
                Some(id(4).to_string().parse().unwrap()),
                None,
            )
            .unwrap(),
        ),
    ];
    for c in variants {
        conflict(&mut a, with_causal(&q, c), &original);
    }
    // Causation IDs are independently digest-bearing when the other fields are fixed.
    let base = q.causal_context().clone().with_causation(
        CausationLink::new(
            Some(id(2)),
            Some(id(3).to_string().parse().unwrap()),
            Some(id(4).to_string().parse().unwrap()),
            Some(opaque()),
        )
        .unwrap(),
    );
    let digest = with_causal(&q, base).digest().unwrap();
    for (task, run, attempt, external) in [
        (5, 3, 4, "opaque/value"),
        (2, 5, 4, "opaque/value"),
        (2, 3, 5, "opaque/value"),
        (2, 3, 4, "other"),
    ] {
        let c = q.causal_context().clone().with_causation(
            CausationLink::new(
                Some(id(task)),
                Some(id(run).to_string().parse().unwrap()),
                Some(id(attempt).to_string().parse().unwrap()),
                Some(OpaqueRef::new(external).unwrap()),
            )
            .unwrap(),
        );
        assert_ne!(with_causal(&q, c).digest().unwrap(), digest);
    }
}
#[test]
fn tag_trait_permutations_are_equal_and_attribution_cannot_change_scheduling() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let mut s = spec(1);
    s.set_metadata(TaskMetadata::new(vec!["z".into(), "a".into(), "z".into()], -7, None));
    let mut c = s.constraints().clone();
    c.set_required_executor_traits(Some(vec!["gpu".into(), "cpu".into(), "gpu".into()])).unwrap();
    s.set_constraints(c).unwrap();
    let q = with_spec(&request(1), s.clone());
    ensure(&mut a, q.clone(), 42).unwrap();
    s.set_metadata(TaskMetadata::new(vec!["a".into(), "z".into()], -7, None));
    let mut c = s.constraints().clone();
    c.set_required_executor_traits(Some(vec!["cpu".into(), "gpu".into()])).unwrap();
    s.set_constraints(c).unwrap();
    assert_eq!(q.digest().unwrap(), with_spec(&q, s.clone()).digest().unwrap());
    assert!(!ensure(&mut a, with_spec(&q, s), 99).unwrap().is_created());
    let mut other = request(2);
    other = with_causal(
        &other,
        other
            .causal_context()
            .clone()
            .with_origin_ref(OpaqueRef::new("campaign/c1/arm/a/execution-unit/2").unwrap())
            .with_authorization_ref(OpaqueRef::new("unverified/reference").unwrap()),
    );
    ensure(&mut a, other, 42).unwrap();
    let times = |id| {
        a.projection()
            .runs_for_task(id)
            .map(|r| (r.scheduled_at(), r.created_at(), r.state()))
            .collect::<Vec<_>>()
    };
    assert_eq!(times(id(1)), times(id(2)));
}
#[test]
fn structural_parent_dependency_size_and_plan_failures_leave_wal_and_projection_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let original = image(&a);
    let wal = std::fs::read(a.store_session().unwrap().wal_path()).unwrap();
    for (q, expected) in [
        (with_spec(&request(1), spec(1).with_parent(id(99))), R::InvalidParent),
        (with_dependencies(&request(1), vec![id(99)]), R::UnknownDependency),
    ] {
        assert!(matches!(ensure(&mut a,q,42),Err(AdmissionError::Rejected(e)) if e == expected));
        assert_eq!(image(&a), original);
    }
    assert!(matches!(EnsureTaskRequest::for_task(spec(1), vec![id(1)]), Err(R::DependencyCycle)));
    let mut oversized = spec(1);
    oversized.set_run_policy(RunPolicy::repeat(u32::MAX, 1).unwrap()).unwrap();
    assert!(matches!(EnsureTaskRequest::for_task(oversized, vec![]), Err(R::TooLarge)));
    let mut oversized = spec(1);
    oversized.set_payload(TaskPayload::new(vec![0; 65537]));
    assert!(matches!(EnsureTaskRequest::for_task(oversized, vec![]), Err(R::TooLarge)));
    let mut oversized = spec(1);
    oversized.set_metadata(TaskMetadata::new(vec![], 0, Some("x".repeat(16 * 1024 * 1024))));
    assert!(matches!(EnsureTaskRequest::for_task(oversized, vec![]), Err(R::TooLarge)));
    let valid = command(request(1), 2, 42);
    let wrong = AdmissionDigest::new(ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap());
    let forged = AdmissionPlan::new(request(1), valid.plan().runs().to_vec(), wrong).unwrap();
    assert!(matches!(
        a.submit_command(
            MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(2, forged, None, 42)),
            DurabilityPolicy::Immediate
        ),
        Err(MutationAuthorityError::Admission(R::InvalidDigest))
    ));
    let bad_runs = [
        vec![],
        vec![RunInstance::new_scheduled(id(1), 42, 42).unwrap()],
        (0..3).map(|i| RunInstance::new_scheduled(id(1), 43 + i * 7, 42).unwrap()).collect(),
        (0..3).map(|i| RunInstance::new_ready(id(1), 42 + i * 7, 100, 0).unwrap()).collect(),
    ];
    for runs in bad_runs {
        let plan = plan_with_runs(request(1), runs);
        assert!(matches!(
            a.submit_command(
                MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(2, plan, None, 42)),
                DurabilityPolicy::Immediate
            ),
            Err(MutationAuthorityError::Admission(R::InvalidRuns))
        ));
    }
    assert!(matches!(
        a.submit_command(
            MutationCommand::AdmissionCommit(valid.clone()),
            DurabilityPolicy::Deferred
        ),
        Err(MutationAuthorityError::Admission(R::ImmediateDurabilityRequired))
    ));
    a.set_admission_limits(AdmissionLimits { record_bytes: 64, ..Default::default() });
    assert!(matches!(ensure(&mut a, request(1), 42), Err(AdmissionError::Rejected(R::TooLarge))));
    assert_eq!(image(&a), original);
    assert_eq!(std::fs::read(a.store_session().unwrap().wal_path()).unwrap(), wal);
    assert!(!a.recovery_required());
    a.set_admission_limits(AdmissionLimits::default());
    ensure(&mut a, request(1), 42).unwrap();
    for n in 2..=9 {
        ensure(&mut a, with_spec(&request(n), spec(n).with_parent(id(n - 1))), 42).unwrap();
    }
    let before = image(&a);
    assert!(matches!(
        ensure(&mut a, with_spec(&request(10), spec(10).with_parent(id(9))), 42),
        Err(AdmissionError::Rejected(R::HierarchyDepth))
    ));
    assert_eq!(image(&a), before);
}
#[test]
fn compound_reducer_validation_is_atomic() {
    let mut projection = actionqueue_storage::recovery::reducer::ReplayReducer::new();
    let c = command(request(1), 1, 42);
    let record = actionqueue_storage::mutation::admission::AdmissionRecord::new(
        request(1),
        request(1).digest().unwrap(),
        42,
        1,
    )
    .unwrap();
    let e = actionqueue_storage::wal::event::WalEvent::new(
        1,
        actionqueue_storage::wal::event::WalEventType::AdmissionCommitted {
            record,
            runs: c.plan().runs()[..2].to_vec(),
        },
    );
    let before = projection.projection_digest().unwrap();
    assert!(projection.apply(&e).is_err());
    assert_eq!(projection.projection_digest().unwrap(), before);
}
#[test]
fn canonical_bytes_and_digest_match_independent_python_vector() {
    let vector: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/aq-cont-1/admission-v1-vector.json"))
            .unwrap();
    let request: EnsureTaskRequest = serde_json::from_value(vector["request"].clone()).unwrap();
    let canonical =
        actionqueue_core::admission::canonical::CanonicalAdmissionV1::new(&request).unwrap();
    let hex = |b: &[u8]| b.iter().map(|b| format!("{b:02x}")).collect::<String>();
    assert_eq!(hex(canonical.bytes()), vector["canonical_hex"].as_str().unwrap());
    assert_eq!(hex(canonical.digest().unwrap().hash().bytes()), vector["sha256"].as_str().unwrap());
    assert!(AdmissionDigest::versioned(99, request.digest().unwrap().hash().clone()).is_err());
    let mut digest = serde_json::to_value(request.digest().unwrap()).unwrap();
    digest["hash"]["algorithm"] = "unknown".into();
    assert!(serde_json::from_value::<AdmissionDigest>(digest).is_err());
}
#[cfg(feature = "platform")]
#[test]
fn tenant_keys_are_independent_and_cross_tenant_references_or_uuid_collisions_leak_no_admission() {
    use actionqueue_core::{
        ids::TenantId, mutation::TenantCreateCommand, platform::TenantRegistration,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_platform(dir.path());
    let tenants: Vec<TenantId> = (10..=11).map(|n| id(n).to_string().parse().unwrap()).collect();
    for (n, t) in tenants.iter().enumerate() {
        let _ = a
            .submit_command(
                MutationCommand::TenantCreate(TenantCreateCommand::new(
                    n as u64 + 2,
                    TenantRegistration::new(*t, format!("tenant/{n}")),
                    40,
                ))
                .with_control(&actionqueue_core::control::HostControlContext {
                    actor_id: None,
                    scope: actionqueue_core::control::ControlScope::Store,
                    attribution: actionqueue_core::causal::ControlMutationContext::new(
                        actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                    ),
                }),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
    }
    let q1 = with_spec(&request(1), spec(1).with_tenant(tenants[0]));
    let q2 = with_spec(&request(1), spec(2).with_tenant(tenants[1]));
    ensure(&mut a, q1.clone(), 42).unwrap();
    ensure(&mut a, q2.clone(), 42).unwrap();
    assert!(!ensure(&mut a, q1.clone(), 99).unwrap().is_created());
    assert!(!ensure(&mut a, q2, 99).unwrap().is_created());
    let before = image(&a);
    assert!(matches!(
        ensure(
            &mut a,
            with_spec(&request(3), spec(3).with_tenant(tenants[1]).with_parent(id(1))),
            42
        ),
        Err(AdmissionError::Rejected(R::TenantMismatch))
    ));
    assert!(matches!(
        ensure(
            &mut a,
            with_dependencies(
                &with_spec(&request(3), spec(3).with_tenant(tenants[1])),
                vec![id(1)]
            ),
            42
        ),
        Err(AdmissionError::Rejected(R::TenantMismatch))
    ));
    assert!(matches!(
        ensure(&mut a, with_spec(&request(3), spec(1).with_tenant(tenants[1])), 42),
        Err(AdmissionError::Rejected(R::TaskIdCollision))
    ));
    assert_eq!(image(&a), before);
    // Tenant is digest-bearing, even though the namespace lookup is independent.
    assert_ne!(
        q1.digest().unwrap(),
        with_spec(&q1, spec(1).with_tenant(tenants[1])).digest().unwrap()
    );
}
#[cfg(feature = "workflow")]
#[test]
fn cron_initial_window_and_text_are_preserved_and_unsupported_profiles_reject() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    let mut s = spec(1);
    s.set_run_policy(RunPolicy::cron("0 * * * * * *").unwrap()).unwrap();
    let q = with_spec(&request(1), s.clone());
    ensure(&mut a, q.clone(), 120).unwrap();
    assert_eq!(
        a.projection().runs_for_task(id(1)).map(|r| r.scheduled_at()).collect::<Vec<_>>(),
        vec![120, 180, 240, 300, 360]
    );
    assert!(!ensure(&mut a, q.clone(), 999).unwrap().is_created());
    s.set_run_policy(RunPolicy::cron("0  * * * * * *").unwrap()).unwrap();
    assert_ne!(q.digest().unwrap(), with_spec(&q, s).digest().unwrap());
    let base = dir.path().join("base");
    let session = actionqueue_storage::store::open_store(
        &base,
        actionqueue_storage::store::OpenOptions::Initialize { features: vec![] },
    )
    .unwrap();
    let p = actionqueue_storage::recovery::bootstrap::recover_read_only(
        &session,
        actionqueue_storage::wal::repair::RepairPolicy::Strict,
    )
    .unwrap()
    .projection;
    let mut a =
        Authority::new(actionqueue_storage::wal::fs_writer::WalFsWriter::new(session).unwrap(), p)
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
    assert!(matches!(ensure(&mut a, q, 120), Err(AdmissionError::Rejected(R::UnsupportedFeature))));
    assert_eq!(a.projection().latest_sequence(), 1);
}
#[test]
fn snapshot_admission_validation_rejects_duplicates_orphans_and_corrupted_digest() {
    use actionqueue_storage::snapshot::{mapping::validate_snapshot, model::Snapshot};
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    ensure(&mut a, request(1), 42).unwrap();
    let s = image(&a);
    let mut duplicate = s.clone();
    duplicate.admissions.push(s.admissions[0].clone());
    assert!(validate_snapshot(&duplicate).is_err());
    let mut orphan = s.clone();
    orphan.tasks.clear();
    orphan.metadata.task_count = 0;
    assert!(validate_snapshot(&orphan).is_err());
    for field in ["hash_algorithm", "canonical_version", "hash"] {
        let mut v = serde_json::to_value(&s).unwrap();
        if field == "hash" {
            v["admissions"][0][field] = serde_json::json!(vec![0; 32]);
        } else {
            v["admissions"][0][field] = 99.into();
        }
        assert!(serde_json::from_value::<Snapshot>(v).is_err(), "{field}");
    }
}
#[test]
fn aq03_manifest_versions_are_rejected_untouched() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(dir.path()));
    let path = dir.path().join("manifest.json");
    let mut m: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    m["snapshot_schema"] = 1.into();
    m["projection_version"] = 1.into();
    let bytes = serde_json::to_vec(&m).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    assert!(actionqueue_storage::store::open_store(
        dir.path(),
        actionqueue_storage::store::OpenOptions::ReadWrite
    )
    .is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn identity_bounds_run_collisions_and_schedule_overflow_reject_before_append() {
    let mut nil_parent = serde_json::to_value(spec(1)).unwrap();
    nil_parent["parent_task_id"] = id(0).to_string().into();
    assert!(matches!(
        EnsureTaskRequest::for_task(serde_json::from_value(nil_parent).unwrap(), vec![]),
        Err(R::InvalidIdentity)
    ));
    assert!(matches!(EnsureTaskRequest::for_task(spec(1), vec![id(0)]), Err(R::InvalidIdentity)));
    let mut s = spec(1);
    s.set_payload(TaskPayload::with_content_type(vec![], "x".repeat(129)));
    assert!(matches!(EnsureTaskRequest::for_task(s, vec![]), Err(R::TooLarge)));
    assert!(matches!(EnsureTaskRequest::for_task(spec(1), vec![id(2); 65]), Err(R::TooLarge)));
    let dir = tempfile::tempdir().unwrap();
    let mut a = open(dir.path());
    ensure(&mut a, request(1), 42).unwrap();
    let before = image(&a);
    let run_id = a.projection().run_ids_for_task(id(1))[0];
    let mut runs = command(request(2), 3, 42).plan().runs().to_vec();
    runs[0] = RunInstance::new_scheduled_with_id(run_id, id(2), 42, 42).unwrap();
    let plan = plan_with_runs(request(2), runs);
    assert!(matches!(
        a.submit_command(
            MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(3, plan, None, 42)),
            DurabilityPolicy::Immediate
        ),
        Err(MutationAuthorityError::Admission(R::RunIdCollision))
    ));
    let mut s = spec(2);
    s.set_run_policy(RunPolicy::repeat(3, u64::MAX).unwrap()).unwrap();
    assert!(matches!(
        ensure(&mut a, with_spec(&request(2), s), 42),
        Err(AdmissionError::Derivation(_))
    ));
    assert_eq!(image(&a), before);
}
