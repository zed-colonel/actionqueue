//! AQ-11 host boundary, current permission lookup, scope and durable attribution.
#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_core::{control::*, platform::*};
use actionqueue_runtime::control::*;
fn host(scope: ControlScope, actor_id: Option<ActorId>) -> HostControlContext {
    HostControlContext {
        scope,
        actor_id,
        attribution: ControlMutationContext::new(OpaqueRef::new("attested/caller").unwrap())
            .with_host_session_ref(OpaqueRef::new("attested/session").unwrap())
            .with_request_id(OpaqueRef::new("attested/request").unwrap()),
    }
}
#[test]
fn administrative_attribution_is_in_the_mutation_frame_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    let h = host(ControlScope::Store, None);
    let before = seq(&a);
    let _ = execute_mutation(
        &mut a,
        &h,
        MutationCommand::EnginePause(EnginePauseCommand::new(before, 10)),
    )
    .unwrap();
    assert_eq!(seq(&a), before + 1);
    assert_eq!(a.projection().control_history().get(&before).unwrap().context, h.attribution);
    parity(&a);
    drop(a);
    let a = s::reopen(dir.path());
    assert_eq!(a.projection().control_history().get(&before).unwrap().context, h.attribution);
}
#[test]
fn body_attribution_cannot_replace_host_attribution_or_scope() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    let h = host(ControlScope::SingleTenant, None);
    let q = admission_support::request(1);
    let forged = actionqueue_core::admission::EnsureTaskRequest::new(
        q.admission_key().clone(),
        q.task_spec().clone(),
        vec![],
        q.causal_context().clone(),
        Some(ControlMutationContext::new(OpaqueRef::new("body/forged").unwrap())),
    )
    .unwrap();
    execute_control(&mut a, &h, ControlOperation::AdmitTask(forged), &MockClock::new(10)).unwrap();
    assert_eq!(
        a.projection().task_admission(q.task_spec().id()).unwrap().request().control_context(),
        Some(&h.attribution)
    );
    let before = seq(&a);
    assert!(execute_control(
        &mut a,
        &host(ControlScope::Tenant(TenantId::new()), None),
        ControlOperation::AdmitTask(q.clone()),
        &MockClock::new(11)
    )
    .is_err());
    assert_eq!(seq(&a), before);
    parity(&a);
}
#[cfg(feature = "platform")]
#[test]
fn aq_dd_003_references_do_not_grant_authority_and_revocation_precedes_duplicates() {
    use actionqueue_core::actor::{ActorRegistration, ExecutorTraits};
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open_platform(dir.path());
    let tenant = TenantId::new();
    let actor = ActorId::new();
    let admin = host(ControlScope::Store, None);
    let c = MutationCommand::TenantCreate(TenantCreateCommand::new(
        seq(&a),
        TenantRegistration::new(tenant, "tenant"),
        10,
    ));
    let _ = execute_mutation(&mut a, &admin, c).unwrap();
    // Provision the first actor through the supported host boundary.
    let c = MutationCommand::ActorRegister(ActorRegisterCommand::new(
        seq(&a),
        ActorRegistration::new(
            actor,
            "actor",
            ExecutorTraits::new(vec!["AdmitTask".into(), "admin".into()]).unwrap(),
            30,
        )
        .with_tenant(tenant),
        10,
    ));
    let _ =
        execute_mutation(&mut a, &host(ControlScope::ProvisionTenant(tenant), None), c).unwrap();
    let c = MutationCommand::RoleAssign(RoleAssignCommand::new(
        seq(&a),
        actor,
        Role::Operator,
        tenant,
        10,
    ));
    let _ = execute_mutation(&mut a, &admin, c).unwrap();
    let h = host(ControlScope::Tenant(tenant), Some(actor));
    for action in [
        QueueAction::AdmitTask,
        QueueAction::AdmitSignal,
        QueueAction::ClaimRun,
        QueueAction::CancelRun,
    ] {
        assert!(authorize(&a, &h, action).is_err());
    }
    for developmental in [false, true] {
        let q = admission_support::request(if developmental { 2 } else { 1 });
        let causal = if developmental {
            q.causal_context().clone()
        } else {
            CausalContext::new(
                TraceId::new("ordinary").unwrap(),
                CorrelationId::new("ordinary").unwrap(),
            )
        };
        let q = actionqueue_core::admission::EnsureTaskRequest::new(
            q.admission_key().clone(),
            q.task_spec().clone().with_tenant(tenant),
            vec![],
            causal,
            None,
        )
        .unwrap();
        let before = seq(&a);
        assert!(execute_control(
            &mut a,
            &h,
            ControlOperation::AdmitTask(q.clone()),
            &MockClock::new(11)
        )
        .is_err());
        assert_eq!(seq(&a), before);
        let c = MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
            seq(&a),
            actor,
            QueueAction::AdmitTask.permission(),
            tenant,
            11,
        ));
        let _ = execute_mutation(&mut a, &admin, c).unwrap();
        execute_control(&mut a, &h, ControlOperation::AdmitTask(q.clone()), &MockClock::new(12))
            .unwrap();
        let c = MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
            seq(&a),
            actor,
            QueueAction::AdmitTask.permission(),
            tenant,
            13,
        ));
        let _ = execute_mutation(&mut a, &admin, c).unwrap();
        let before = seq(&a);
        assert!(execute_control(&mut a, &h, ControlOperation::AdmitTask(q), &MockClock::new(14))
            .is_err());
        assert_eq!(seq(&a), before);
        assert!(authorize(&a, &h, QueueAction::PauseEngine).is_err());
    }
    parity(&a);
}

#[test]
fn administrative_suspend_closes_fence_and_resume_replays_context() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    let r = running(&mut a, 1, None, false);
    let h = host(ControlScope::SingleTenant, None);
    let c = MutationCommand::RunSuspend(RunSuspendCommand::new(seq(&a), r, None, 20));
    let _ = execute_mutation(&mut a, &h, c).unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Suspended));
    assert!(a.projection().get_lease(&r).is_none());
    assert!(a.projection().get_run_instance(&r).unwrap().current_attempt_id().is_none());
    let c = MutationCommand::RunResume(RunResumeCommand::new(seq(&a), r, 21));
    let _ = execute_mutation(&mut a, &h, c).unwrap();
    assert_eq!(
        a.projection().pending_resume(r).unwrap().wake,
        WakeReason::AdministrativeResume { control_context: Some(h.attribution.clone()) }
    );
    parity(&a);
}

#[tokio::test]
async fn http_requires_control_mode_and_a_host_hook() {
    use tower::ServiceExt;
    for (enabled, authenticated, expected) in
        [(false, false, 404), (true, false, 401), (true, true, 200)]
    {
        let dir = tempfile::tempdir().unwrap();
        let hook = authenticated.then(|| {
            std::sync::Arc::new(|_: &axum::http::HeaderMap, _: &axum::http::Uri| {
                Ok(host(ControlScope::Store, None))
            }) as actionqueue_daemon::http::auth::HostAuthenticator
        });
        let state = actionqueue_daemon::bootstrap::bootstrap_with_authenticator(
            actionqueue_daemon::config::DaemonConfig {
                data_dir: dir.path().into(),
                enable_control: enabled,
                ..Default::default()
            },
            hook,
        );
        if enabled && !authenticated {
            assert!(state.is_err());
            continue;
        }
        let state = state.unwrap();
        let response = state
            .http_router()
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v2/engine/pause")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
    }
}

#[test]
fn raw_controls_and_conflicting_envelopes_are_rejected_before_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    let h = host(ControlScope::SingleTenant, None);
    let q = admission_support::request(42);
    let c = MutationCommand::AdmissionCommit(admission_support::command(q.clone(), seq(&a), 10));
    let before = seq(&a);
    assert!(a.submit_command(c.clone(), DurabilityPolicy::Immediate).is_err());
    assert!(a.lookup_admission(&q).is_err());
    assert_eq!(before, seq(&a));
    let _ = a.submit_command(c.with_control(&h), DurabilityPolicy::Immediate).unwrap();
    let duplicate = MutationCommand::AdmissionCommit(admission_support::command(q, before, 10));
    let before = seq(&a);
    assert!(a.submit_command(duplicate, DurabilityPolicy::Immediate).is_err());
    let forged = MutationCommand::Cancel(CancelCommand {
        expected_sequence: before,
        target: CancelTarget::Task(admission_support::id(42)),
        tenant_id: None,
        timestamp: 11,
        control_context: Some(ControlMutationContext::new(OpaqueRef::new("forged").unwrap())),
    });
    assert!(a.submit_command(forged.with_control(&h), DurabilityPolicy::Immediate).is_err());
    assert_eq!(before, seq(&a));
    assert!(!a.recovery_required());
    parity(&a);
}

#[test]
fn attributed_creation_limits_cover_complete_frame_and_rejection_does_not_fence() {
    let h = host(ControlScope::SingleTenant, None);
    let q = admission_support::request(43);
    let reference = tempfile::tempdir().unwrap();
    let mut a = open_store(reference.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    execute_control(&mut a, &h, ControlOperation::AdmitTask(q.clone()), &MockClock::new(10))
        .unwrap();
    use actionqueue_storage::wal::{fs_reader::WalFsReader, reader::WalReader};
    let mut reader = WalFsReader::for_session(a.store_session().unwrap()).unwrap();
    let mut size = 0;
    while let Some(event) = reader.read_next().unwrap() {
        if matches!(
            event.event(),
            actionqueue_storage::wal::event::WalEventType::AdmissionCommitted { .. }
        ) {
            assert!(event.control().is_some());
            size = actionqueue_storage::wal::codec::encode(&event).unwrap().len();
        }
    }
    assert!(size > 0);
    let dir = tempfile::tempdir().unwrap();
    let mut b = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
        .unwrap()
        .into_authority()
        .unwrap();
    b.set_admission_limits(actionqueue_core::limits::AdmissionLimits {
        record_bytes: size - 1,
        ..Default::default()
    });
    let before = seq(&b);
    assert!(execute_control(
        &mut b,
        &h,
        ControlOperation::AdmitTask(q.clone()),
        &MockClock::new(10)
    )
    .is_err());
    assert_eq!(before, seq(&b));
    assert!(!b.recovery_required());
    b.set_admission_limits(actionqueue_core::limits::AdmissionLimits {
        record_bytes: size,
        ..Default::default()
    });
    execute_control(&mut b, &h, ControlOperation::AdmitTask(q.clone()), &MockClock::new(10))
        .unwrap();
    b.set_admission_limits(actionqueue_core::limits::AdmissionLimits {
        record_bytes: 0,
        ..Default::default()
    });
    let before = seq(&b);
    execute_control(&mut b, &h, ControlOperation::AdmitTask(q), &MockClock::new(11)).unwrap();
    assert_eq!(before, seq(&b));
    parity(&b);
}

#[test]
fn aq_dd_008_cancellation_preserves_host_attribution_without_granting_authority() {
    for developmental in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
            .unwrap()
            .into_authority()
            .unwrap();
        let mut h = host(ControlScope::SingleTenant, None);
        if developmental {
            h.attribution =
                h.attribution.with_request_id(OpaqueRef::new("campaign/arm/intervention").unwrap());
        }
        let q = admission_support::request(44);
        execute_control(&mut a, &h, ControlOperation::AdmitTask(q), &MockClock::new(10)).unwrap();
        let target = CancelTarget::Task(admission_support::id(44));
        let before = seq(&a);
        let wrong =
            HostControlContext { scope: ControlScope::Tenant(TenantId::new()), ..h.clone() };
        assert!(execute_control(
            &mut a,
            &wrong,
            ControlOperation::Cancel(target),
            &MockClock::new(11)
        )
        .is_err());
        assert_eq!(before, seq(&a));
        execute_control(&mut a, &h, ControlOperation::Cancel(target), &MockClock::new(12)).unwrap();
        assert_eq!(a.projection().control_history().get(&before).unwrap().context, h.attribution);
        assert!(a
            .projection()
            .runs_for_task(admission_support::id(44))
            .all(|r| r.state() == RunState::Canceled));
        parity(&a);
    }
}

fn attributed(a: &mut s::Authority, h: &HostControlContext, command: MutationCommand) {
    let before = a.projection().projection_digest().unwrap();
    let wrong = host(
        if h.scope == ControlScope::Store {
            ControlScope::SingleTenant
        } else {
            ControlScope::Store
        },
        None,
    );
    assert!(execute_mutation(a, &wrong, command.clone()).is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    let at = seq(a);
    let _ = execute_mutation(a, h, command).unwrap();
    assert_eq!(a.projection().control_history().get(&at).unwrap(), &ControlAttribution::from(h));
}
#[test]
fn authenticated_wait_inspection_resolution_and_cancellation_are_scoped_and_attributed() {
    for cancel in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let run = running(&mut a, 1, None, false);
        let wait = WaitId::new();
        let c = command(&a, run, spec(wait, None));
        establish(&mut a, c).unwrap();
        let h = host(ControlScope::SingleTenant, None);
        let before = a.projection().projection_digest().unwrap();
        assert!(inspect_wait(&a, &host(ControlScope::Store, None), wait).is_err());
        assert_eq!(inspect_wait(&a, &h, wait).unwrap().run_id, run);
        let operation = if cancel {
            ControlOperation::CancelWait { run_id: run, wait_id: wait }
        } else {
            ControlOperation::ResolveWait { run_id: run, wait_id: wait }
        };
        assert!(execute_control(
            &mut a,
            &host(ControlScope::Store, None),
            operation,
            &MockClock::new(30)
        )
        .is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
        let at = seq(&a);
        let operation = if cancel {
            ControlOperation::CancelWait { run_id: run, wait_id: wait }
        } else {
            ControlOperation::ResolveWait { run_id: run, wait_id: wait }
        };
        execute_control(&mut a, &h, operation, &MockClock::new(30)).unwrap();
        assert_eq!(
            a.projection().control_history().get(&at).unwrap(),
            &ControlAttribution::from(&h)
        );
        assert_eq!(
            a.projection().get_run_state(&run),
            Some(&if cancel { RunState::Canceled } else { RunState::Ready })
        );
        parity(&a);
    }
}
#[test]
fn authenticated_signal_inspection_and_all_retention_mutations_are_attributed() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let h = host(ControlScope::SingleTenant, None);
    a.set_signal_retention_policy(actionqueue_core::limits::SignalRetentionPolicy {
        minimum_age_secs: 0,
        minimum_sequence_window: 0,
    });
    for n in 1..=2 {
        execute_control(
            &mut a,
            &h,
            ControlOperation::AdmitSignal(s::request(n)),
            &MockClock::new(10),
        )
        .unwrap();
    }
    assert!(inspect_signal(&a, &host(ControlScope::Store, None), &s::id(1)).is_err());
    assert_eq!(inspect_signal(&a, &h, &s::id(1)).unwrap().sequence, SignalSequence::new(1));
    for add in [true, false] {
        let c = SignalPinCommand {
            expected_sequence: seq(&a),
            tenant_id: None,
            signal_id: s::id(1),
            pin_id: SignalPinId::new("host-pin").unwrap(),
            timestamp: 20,
            control_context: Some(h.attribution.clone()),
        };
        attributed(
            &mut a,
            &h,
            if add { MutationCommand::SignalPin(c) } else { MutationCommand::SignalUnpin(c) },
        );
    }
    let c = MutationCommand::RetireSignals(RetireSignalsCommand {
        expected_sequence: seq(&a),
        tenant_id: None,
        sequences: vec![SignalSequence::new(1)],
        timestamp: 20,
        control_context: Some(h.attribution.clone()),
    });
    attributed(&mut a, &h, c);
    parity(&a);
}
#[cfg(feature = "budget")]
#[test]
fn every_budget_and_subscription_control_is_attributed() {
    use actionqueue_core::{budget::BudgetDimension, subscription::*};
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let run = running(&mut a, 1, None, false);
    let task = a.projection().get_run_instance(&run).unwrap().task_id();
    let h = host(ControlScope::SingleTenant, None);
    let c = MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
        seq(&a),
        task,
        BudgetDimension::Token,
        100,
        20,
    ));
    attributed(&mut a, &h, c);
    let c = MutationCommand::BudgetConsume(BudgetConsumeCommand::new(
        seq(&a),
        task,
        BudgetDimension::Token,
        10,
        21,
    ));
    attributed(&mut a, &h, c);
    let c = MutationCommand::BudgetReplenish(BudgetReplenishCommand::new(
        seq(&a),
        task,
        BudgetDimension::Token,
        5,
        22,
    ));
    attributed(&mut a, &h, c);
    let id = SubscriptionId::new();
    let c = MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
        seq(&a),
        id,
        task,
        EventFilter::TaskCompleted { task_id: task },
        23,
    ));
    attributed(&mut a, &h, c);
    let c = MutationCommand::SubscriptionCancel(SubscriptionCancelCommand::new(seq(&a), id, 24));
    attributed(&mut a, &h, c);
    parity(&a);
}
#[cfg(feature = "actor")]
#[test]
fn actor_controls_and_engine_switches_carry_the_host_identity() {
    use actionqueue_core::actor::*;
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let id = ActorId::new();
    let h = host(ControlScope::SingleTenant, Some(id));
    let c = MutationCommand::ActorRegister(ActorRegisterCommand::new(
        seq(&a),
        ActorRegistration::new(
            id,
            "remote",
            ExecutorTraits::new(vec!["compute".into()]).unwrap(),
            30,
        ),
        10,
    ));
    attributed(&mut a, &h, c);
    let c = MutationCommand::ActorHeartbeat(ActorHeartbeatCommand::new(seq(&a), id, 11));
    attributed(&mut a, &h, c);
    let c = MutationCommand::ActorDeregister(ActorDeregisterCommand::new(seq(&a), id, 12));
    attributed(&mut a, &h, c);
    let h = host(ControlScope::Store, None);
    let c = MutationCommand::EnginePause(EnginePauseCommand::new(seq(&a), 13));
    attributed(&mut a, &h, c);
    let c = MutationCommand::EngineResume(EngineResumeCommand::new(seq(&a), 14));
    attributed(&mut a, &h, c);
    parity(&a);
}

#[cfg(feature = "platform")]
#[test]
fn every_tenant_control_permission_is_current_and_independent_of_opaque_references() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open_platform(dir.path());
    let tenant = TenantId::new();
    let admin = host(ControlScope::Store, None);
    let c = MutationCommand::TenantCreate(TenantCreateCommand::new(
        seq(&a),
        TenantRegistration::new(tenant, "fresh"),
        1,
    ));
    let _ = execute_mutation(&mut a, &admin, c).unwrap();
    let h = s::host_support::tenant(&mut a, tenant).unwrap();
    for action in [
        QueueAction::AdmitTask,
        QueueAction::InspectTask,
        QueueAction::AdmitSignal,
        QueueAction::InspectSignal,
        QueueAction::RetainSignal,
        QueueAction::CancelTask,
        QueueAction::CancelRun,
        QueueAction::SuspendRun,
        QueueAction::ResumeRun,
        QueueAction::InspectWait,
        QueueAction::ResolveWait,
        QueueAction::CancelWait,
        QueueAction::RegisterActor,
        QueueAction::DeregisterActor,
        QueueAction::HeartbeatActor,
        QueueAction::InspectClaimable,
        QueueAction::ClaimRun,
        QueueAction::RenewLease,
        QueueAction::SubmitResult,
        QueueAction::ManageBudget,
        QueueAction::ManageSubscription,
        QueueAction::AppendLedger,
        QueueAction::InspectLedger,
    ] {
        let c = MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
            seq(&a),
            h.actor_id.unwrap(),
            action.permission(),
            tenant,
            10,
        ));
        let _ = execute_mutation(&mut a, &admin, c).unwrap();
        assert_eq!(authorize(&a, &h, action).unwrap(), Some(tenant));
        let c = MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
            seq(&a),
            h.actor_id.unwrap(),
            action.permission(),
            tenant,
            11,
        ));
        let _ = execute_mutation(&mut a, &admin, c).unwrap();
        let before = a.projection().projection_digest().unwrap();
        assert!(authorize(&a, &h, action).is_err());
        let mut forged = h.clone();
        forged.attribution = host(ControlScope::Store, None).attribution;
        assert!(authorize(&a, &forged, action).is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
    parity(&a);
}

#[cfg(feature = "platform")]
fn persisted_image(
    a: &s::Authority,
) -> (actionqueue_storage::recovery::projection::ProjectionDigest, Vec<u8>) {
    (
        a.projection().projection_digest().unwrap(),
        std::fs::read(a.store_session().unwrap().wal_path()).unwrap(),
    )
}
#[cfg(feature = "platform")]
fn tenant_pair(a: &mut s::Authority) -> (HostControlContext, HostControlContext) {
    let mut hosts = Vec::new();
    for name in ["owner", "other"] {
        let tenant = TenantId::new();
        let c = MutationCommand::TenantCreate(TenantCreateCommand::new(
            seq(a),
            TenantRegistration::new(tenant, name),
            1,
        ));
        let _ = execute_mutation(a, &host(ControlScope::Store, None), c).unwrap();
        let mut h = s::host_support::tenant(a, tenant).unwrap();
        h.attribution = host(ControlScope::Store, None)
            .attribution
            .with_request_id(OpaqueRef::new("campaign/arm/no-authority").unwrap());
        hosts.push(h);
    }
    (hosts.remove(0), hosts.remove(0))
}
#[cfg(feature = "platform")]
fn permission(a: &mut s::Authority, h: &HostControlContext, action: QueueAction, grant: bool) {
    let ControlScope::Tenant(tenant) = h.scope else { panic!() };
    let c = if grant {
        MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
            seq(a),
            h.actor_id.unwrap(),
            action.permission(),
            tenant,
            30,
        ))
    } else {
        MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
            seq(a),
            h.actor_id.unwrap(),
            action.permission(),
            tenant,
            30,
        ))
    };
    let _ = execute_mutation(a, &host(ControlScope::Store, None), c).unwrap();
}

#[cfg(feature = "platform")]
#[test]
fn generic_control_transitions_enforce_host_scope_permissions_and_replayed_lineage() {
    for (from, to, action) in [
        (RunState::Scheduled, RunState::Canceled, QueueAction::CancelRun),
        (RunState::Running, RunState::Canceled, QueueAction::CancelRun),
        (RunState::Running, RunState::Suspended, QueueAction::SuspendRun),
        (RunState::Suspended, RunState::Ready, QueueAction::ResumeRun),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open_platform(dir.path());
        let (h, other) = tenant_pair(&mut a);
        let ControlScope::Tenant(tenant) = h.scope else { panic!() };
        let mut r = running_scoped(&mut a, 301, None, false, Some(tenant));
        if from == RunState::Scheduled {
            r = a
                .projection()
                .runs_for_task(admission_support::id(301))
                .find(|r| r.state() == RunState::Scheduled)
                .unwrap()
                .id();
        }
        for owner in [&h, &other] {
            permission(&mut a, owner, QueueAction::SuspendRun, true);
            permission(&mut a, owner, QueueAction::ResumeRun, true);
        }
        if from == RunState::Suspended {
            let c = MutationCommand::RunSuspend(RunSuspendCommand::new(seq(&a), r, None, 20));
            let _ = execute_mutation(&mut a, &h, c).unwrap();
        }
        a.set_control_context(None);
        let make = |a: &s::Authority| {
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                seq(a),
                r,
                from,
                to,
                31,
            ))
        };
        let before = persisted_image(&a);
        assert!(a.submit_command(make(&a), DurabilityPolicy::Immediate).is_err());
        // Rebuild the command at the authoritative sequence for every check.
        let c = make(&a);
        assert!(execute_mutation(&mut a, &other, c).is_err());
        let c = MutationCommand::RecoveryControl(Box::new(make(&a)));
        assert!(a.submit_command(c, DurabilityPolicy::Immediate).is_err());
        assert_eq!(before, persisted_image(&a));
        permission(&mut a, &h, action, false);
        let before = persisted_image(&a);
        let c = make(&a);
        assert!(execute_mutation(&mut a, &h, c).is_err());
        assert_eq!(before, persisted_image(&a));
        permission(&mut a, &h, action, true);
        let at = seq(&a);
        let c = make(&a);
        let _ = execute_mutation(&mut a, &h, c).unwrap();
        assert_eq!(a.projection().get_run_state(&r), Some(&to));
        assert_eq!(a.projection().control_history().get(&at), Some(&ControlAttribution::from(&h)));
        parity(&a);
        drop(a);
        let a = s::reopen(dir.path());
        assert_eq!(a.projection().control_history().get(&at), Some(&ControlAttribution::from(&h)));
        if to == RunState::Ready {
            assert_eq!(
                a.projection().pending_resume(r).unwrap().wake,
                WakeReason::AdministrativeResume { control_context: Some(h.attribution.clone()) }
            );
        }
    }
}

#[cfg(feature = "platform")]
#[test]
fn platform_wait_and_retention_operations_reject_missing_cross_tenant_and_revoked_hosts() {
    for operation in
        ["inspect_wait", "resolve", "cancel_wait", "inspect_signal", "pin", "unpin", "retire"]
    {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open_platform(dir.path());
        let (h, other) = tenant_pair(&mut a);
        let ControlScope::Tenant(tenant) = h.scope else { panic!() };
        let r = running_scoped(&mut a, 302, None, false, Some(tenant));
        let wait = WaitId::new();
        let mut filter = s::filter();
        filter.tenant_id = Some(tenant);
        filter.kind = SignalKind::new("wait-only").unwrap();
        let w = WaitSpec::new(
            wait,
            filter,
            WaitMatchPolicy::FirstMatch,
            SignalEligibility::After(SignalSequence::new(0)),
            None,
        )
        .unwrap();
        let c = command(&a, r, w);
        establish(&mut a, c).unwrap();
        a.set_signal_retention_policy(actionqueue_core::limits::SignalRetentionPolicy {
            minimum_age_secs: 0,
            minimum_sequence_window: 0,
        });
        execute_control(
            &mut a,
            &h,
            ControlOperation::AdmitSignal(s::request(501)),
            &MockClock::new(25),
        )
        .unwrap();
        execute_control(
            &mut a,
            &h,
            ControlOperation::AdmitSignal(s::request(502)),
            &MockClock::new(25),
        )
        .unwrap();
        let pin = |a: &s::Authority| SignalPinCommand {
            expected_sequence: seq(a),
            tenant_id: Some(tenant),
            signal_id: s::id(501),
            pin_id: SignalPinId::new("pin").unwrap(),
            timestamp: 31,
            control_context: None,
        };
        if operation == "unpin" {
            let c = MutationCommand::SignalPin(pin(&a));
            let _ = execute_mutation(&mut a, &h, c).unwrap();
        }
        let action = match operation {
            "inspect_wait" => QueueAction::InspectWait,
            "resolve" => QueueAction::ResolveWait,
            "cancel_wait" => QueueAction::CancelWait,
            "inspect_signal" => QueueAction::InspectSignal,
            _ => QueueAction::RetainSignal,
        };
        let invoke = |a: &mut s::Authority,
                      h: &HostControlContext|
         -> Result<(), actionqueue_runtime::control::ServiceError> {
            match operation {
                "inspect_wait" => inspect_wait(a, h, wait).map(|_| ()).map_err(Into::into),
                "inspect_signal" => {
                    inspect_signal(a, h, &s::id(501)).map(|_| ()).map_err(Into::into)
                }
                "resolve" => execute_control(
                    a,
                    h,
                    ControlOperation::ResolveWait { run_id: r, wait_id: wait },
                    &MockClock::new(31),
                )
                .map(|_| ()),
                "cancel_wait" => execute_control(
                    a,
                    h,
                    ControlOperation::CancelWait { run_id: r, wait_id: wait },
                    &MockClock::new(31),
                )
                .map(|_| ()),
                _ => {
                    let c = match operation {
                        "pin" => MutationCommand::SignalPin(pin(a)),
                        "unpin" => MutationCommand::SignalUnpin(pin(a)),
                        _ => MutationCommand::RetireSignals(RetireSignalsCommand {
                            expected_sequence: seq(a),
                            tenant_id: Some(tenant),
                            sequences: vec![SignalSequence::new(1)],
                            timestamp: 31,
                            control_context: None,
                        }),
                    };
                    execute_mutation(a, h, c).map(|_| ()).map_err(Into::into)
                }
            }
        };
        a.set_control_context(None);
        let missing = HostControlContext { actor_id: None, ..h.clone() };
        let before = persisted_image(&a);
        for rejected in [&missing, &other] {
            assert!(invoke(&mut a, rejected).is_err(), "{operation}");
            assert_eq!(before, persisted_image(&a), "{operation}");
        }
        permission(&mut a, &h, action, false);
        let before = persisted_image(&a);
        assert!(invoke(&mut a, &h).is_err(), "{operation}");
        assert_eq!(before, persisted_image(&a), "{operation}");
        permission(&mut a, &h, action, true);
        let at = seq(&a);
        invoke(&mut a, &h).unwrap();
        if !operation.starts_with("inspect") {
            assert_eq!(
                a.projection().control_history().get(&at),
                Some(&ControlAttribution::from(&h)),
                "{operation}"
            );
        }
        parity(&a);
    }
}

#[cfg(feature = "platform")]
#[test]
fn platform_mutation_operations_enforce_context_target_and_revocation_before_append() {
    use actionqueue_core::{actor::*, budget::BudgetDimension, subscription::*};
    for operation in [
        "cancel_task",
        "cancel_run",
        "allocate",
        "consume",
        "replenish",
        "subscribe",
        "unsubscribe",
        "register",
        "deregister",
        "heartbeat",
        "ledger",
    ] {
        if !cfg!(feature = "budget")
            && matches!(
                operation,
                "allocate" | "consume" | "replenish" | "subscribe" | "unsubscribe"
            )
        {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open_platform(dir.path());
        let (h, other) = tenant_pair(&mut a);
        let ControlScope::Tenant(tenant) = h.scope else { panic!() };
        let run = running_scoped(&mut a, 303, None, false, Some(tenant));
        let task = a.projection().get_run_instance(&run).unwrap().task_id();
        let subscription = SubscriptionId::new();
        let actor = ActorId::new();
        let action = match operation {
            "cancel_task" => QueueAction::CancelTask,
            "cancel_run" => QueueAction::CancelRun,
            "allocate" | "consume" | "replenish" => QueueAction::ManageBudget,
            "subscribe" | "unsubscribe" => QueueAction::ManageSubscription,
            "register" => QueueAction::RegisterActor,
            "deregister" => QueueAction::DeregisterActor,
            "heartbeat" => QueueAction::HeartbeatActor,
            _ => QueueAction::AppendLedger,
        };
        for owner in [&h, &other] {
            if matches!(
                action,
                QueueAction::RegisterActor
                    | QueueAction::DeregisterActor
                    | QueueAction::HeartbeatActor
            ) {
                permission(&mut a, owner, action, true);
            }
        }
        if matches!(operation, "consume" | "replenish") {
            let c = MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
                seq(&a),
                task,
                BudgetDimension::Token,
                100,
                20,
            ));
            let _ = execute_mutation(&mut a, &h, c).unwrap();
        }
        if operation == "unsubscribe" {
            let c = MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
                seq(&a),
                subscription,
                task,
                EventFilter::TaskCompleted { task_id: task },
                20,
            ));
            let _ = execute_mutation(&mut a, &h, c).unwrap();
        }
        let make =
            |a: &s::Authority| match operation {
                "cancel_task" | "cancel_run" => MutationCommand::Cancel(CancelCommand {
                    expected_sequence: seq(a),
                    target: if operation == "cancel_task" {
                        CancelTarget::Task(task)
                    } else {
                        CancelTarget::Run(run)
                    },
                    tenant_id: Some(tenant),
                    timestamp: 31,
                    control_context: None,
                }),
                "allocate" => MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
                    seq(a),
                    task,
                    BudgetDimension::Token,
                    100,
                    31,
                )),
                "consume" => MutationCommand::BudgetConsume(BudgetConsumeCommand::new(
                    seq(a),
                    task,
                    BudgetDimension::Token,
                    10,
                    31,
                )),
                "replenish" => MutationCommand::BudgetReplenish(BudgetReplenishCommand::new(
                    seq(a),
                    task,
                    BudgetDimension::Token,
                    10,
                    31,
                )),
                "subscribe" => MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
                    seq(a),
                    subscription,
                    task,
                    EventFilter::TaskCompleted { task_id: task },
                    31,
                )),
                "unsubscribe" => MutationCommand::SubscriptionCancel(
                    SubscriptionCancelCommand::new(seq(a), subscription, 31),
                ),
                "register" => MutationCommand::ActorRegister(ActorRegisterCommand::new(
                    seq(a),
                    ActorRegistration::new(
                        actor,
                        "actor",
                        ExecutorTraits::new(vec!["compute".into()]).unwrap(),
                        30,
                    )
                    .with_tenant(tenant),
                    31,
                )),
                "deregister" => MutationCommand::ActorDeregister(ActorDeregisterCommand::new(
                    seq(a),
                    h.actor_id.unwrap(),
                    31,
                )),
                "heartbeat" => MutationCommand::ActorHeartbeat(ActorHeartbeatCommand::new(
                    seq(a),
                    h.actor_id.unwrap(),
                    31,
                )),
                _ => MutationCommand::LedgerAppend(LedgerAppendCommand::new(
                    seq(a),
                    LedgerEntry::new(LedgerEntryId::new(), tenant, "opaque", vec![], 31)
                        .with_actor(h.actor_id.unwrap()),
                    31,
                )),
            };
        a.set_control_context(None);
        let before = persisted_image(&a);
        let bytes = std::fs::read(a.store_session().unwrap().wal_path()).unwrap();
        assert!(a.submit_command(make(&a), DurabilityPolicy::Immediate).is_err(), "{operation}");
        for rejected in [&HostControlContext { actor_id: None, ..h.clone() }, &other] {
            let c = make(&a);
            assert!(execute_mutation(&mut a, rejected, c).is_err(), "{operation}");
            assert_eq!(before, persisted_image(&a), "{operation}");
            assert_eq!(
                bytes,
                std::fs::read(a.store_session().unwrap().wal_path()).unwrap(),
                "{operation}"
            );
        }
        permission(&mut a, &h, action, false);
        let before = persisted_image(&a);
        let c = make(&a);
        assert!(execute_mutation(&mut a, &h, c).is_err(), "{operation}");
        assert_eq!(before, persisted_image(&a), "{operation}");
        permission(&mut a, &h, action, true);
        let at = seq(&a);
        let c = make(&a);
        let _ = execute_mutation(&mut a, &h, c).unwrap();
        assert_eq!(
            a.projection().control_history().get(&at),
            Some(&ControlAttribution::from(&h)),
            "{operation}"
        );
        parity(&a);
    }
}

#[cfg(feature = "platform")]
#[tokio::test]
async fn wait_only_inspection_lists_own_tenant_and_honors_revocation_in_embedded_and_http() {
    use actionqueue_runtime::inspection::{InspectionError, Inspector, Query};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    for granted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open_platform(dir.path());
        let (h, other) = tenant_pair(&mut a);
        let mut waits = Vec::new();
        for (n, owner) in [&h, &other].into_iter().enumerate() {
            let ControlScope::Tenant(tenant) = owner.scope else { panic!() };
            let run = running_scoped(&mut a, 401 + n as u64, None, false, Some(tenant));
            let mut filter = s::filter();
            filter.tenant_id = Some(tenant);
            let id = WaitId::new();
            let wait = WaitSpec::new(
                id,
                filter,
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::After(SignalSequence::new(0)),
                None,
            )
            .unwrap();
            establish_wait(&mut a, run, wait);
            waits.push(id);
        }
        permission(&mut a, &h, QueueAction::InspectTask, false);
        permission(&mut a, &h, QueueAction::InspectWait, granted);
        let i = Inspector::new(a.projection(), &h, true, Default::default(), false, 40).unwrap();
        let list = i.list_waits(&Query::default());
        let own = i.get_wait(waits[0]);
        let foreign = i.get_wait(waits[1]);
        if granted {
            assert_eq!(list.as_ref().unwrap().items.len(), 1);
            assert_eq!(list.as_ref().unwrap().items[0].wait_id, waits[0]);
            assert!(own.is_ok());
            assert_eq!(foreign.unwrap_err(), InspectionError::NotFound);
            assert_eq!(
                i.list_waits(&Query { origin_ref: Some("protected".into()), ..Default::default() })
                    .unwrap_err(),
                InspectionError::Unauthorized
            );
        } else {
            assert_eq!(list.as_ref().unwrap_err(), &InspectionError::Unauthorized);
            assert_eq!(own.as_ref().unwrap_err(), &InspectionError::Unauthorized);
        }
        let expected_list = list.ok().map(|v| serde_json::to_value(v).unwrap());
        let expected_own = own.ok().map(|v| serde_json::to_value(v).unwrap());
        drop(a);
        let state = actionqueue_daemon::bootstrap::bootstrap_with_authenticator(
            actionqueue_daemon::config::DaemonConfig {
                data_dir: dir.path().into(),
                ..Default::default()
            },
            Some(std::sync::Arc::new(move |_, _| Ok(h.clone()))),
        )
        .unwrap();
        for (path, expected, status) in [
            ("/api/v2/waits".into(), expected_list, if granted { 200 } else { 403 }),
            (format!("/api/v2/waits/{}", waits[0]), expected_own, if granted { 200 } else { 403 }),
            (format!("/api/v2/waits/{}", waits[1]), None, if granted { 404 } else { 403 }),
            ("/api/v2/waits?origin_ref=protected".into(), None, 403),
        ] {
            let response = state
                .http_router()
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(&path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status, "{path}");
            if let Some(expected) = expected {
                let value: serde_json::Value = serde_json::from_slice(
                    &response.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                assert_eq!(value, expected);
            }
        }
        state.shutdown().await;
    }
}
