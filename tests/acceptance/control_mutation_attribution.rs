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
fn aq_dd_003_and_008_references_do_not_grant_authority_and_revocation_precedes_duplicates() {
    use actionqueue_core::actor::{ActorRegistration, ExecutorTraits};
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let tenant = TenantId::new();
    let actor = ActorId::new();
    let admin = host(ControlScope::Store, None);
    let c = MutationCommand::TenantCreate(TenantCreateCommand::new(
        seq(&a),
        TenantRegistration::new(tenant, "tenant"),
        10,
    ));
    let _ = execute_mutation(&mut a, &admin, c).unwrap();
    // Bootstrap the actor with a direct trusted-host mutation before its grants exist.
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
    apply(&mut a, c.with_control(&host(ControlScope::Tenant(tenant), None)));
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
        )
        .unwrap();
        let response = state
            .http_router()
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/engine/pause")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
    }
}
