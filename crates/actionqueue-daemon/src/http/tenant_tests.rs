//! End-to-end host authentication, current grants, namespace filtering and remote maintenance.
use actionqueue_core::{
    actor::*, bounded::*, causal::*, control::*, ids::*, mutation::*, platform::*,
    time::clock::MockClock,
};
use actionqueue_runtime::control::{execute_control, execute_mutation, ControlOperation};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tower::ServiceExt;

use super::*;

fn host(scope: ControlScope, actor_id: Option<ActorId>) -> HostControlContext {
    HostControlContext {
        scope,
        actor_id,
        attribution: ControlMutationContext::new(OpaqueRef::new("http-host").unwrap()),
    }
}
fn fixture() -> (std::path::PathBuf, RouterState, HostControlContext, HostControlContext) {
    fixture_with_workflow(false)
}
fn fixture_with_workflow(
    workflow: bool,
) -> (std::path::PathBuf, RouterState, HostControlContext, HostControlContext) {
    let root = std::env::temp_dir().join(format!("aq-http-{}", TaskId::new()));
    let _ = actionqueue_storage::store::open_store(
        &root,
        actionqueue_storage::store::OpenOptions::Initialize {
            features: if workflow {
                vec!["actor".into(), "platform".into(), "workflow".into()]
            } else {
                vec!["actor".into(), "platform".into()]
            },
        },
    )
    .unwrap();
    let boot = crate::bootstrap::bootstrap_with_authenticator(
        crate::config::DaemonConfig {
            data_dir: root.clone(),
            enable_control: true,
            metrics_bind: None,
            ..Default::default()
        },
        Some(Arc::new(|_, _| Err(auth::AuthenticationError))),
    )
    .unwrap();
    // Bootstrap has not started a Tokio timer: this fixture is constructed outside
    // the async runtime. A deterministic clock is installed before serving.
    let (router, mut state) = boot.into_http();
    drop(router);
    let inner = Arc::get_mut(&mut state).unwrap();
    inner.clock = Arc::new(crate::time::clock::MockClock::new(10));
    let admin = host(ControlScope::Store, None);
    let mut actors = Vec::new();
    {
        let mut a = inner.control_authority.as_ref().unwrap().lock().unwrap();
        for name in ["one", "two"] {
            let tenant = TenantId::new();
            let actor = ActorId::new();
            let seq = a.projection().latest_sequence() + 1;
            let _ = execute_mutation(
                &mut a,
                &admin,
                MutationCommand::TenantCreate(TenantCreateCommand::new(
                    seq,
                    TenantRegistration::new(tenant, name),
                    10,
                )),
            )
            .unwrap();
            let seq = a.projection().latest_sequence() + 1;
            let _ = execute_mutation(
                &mut a,
                &host(ControlScope::ProvisionTenant(tenant), None),
                MutationCommand::ActorRegister(ActorRegisterCommand::new(
                    seq,
                    ActorRegistration::new(
                        actor,
                        name,
                        ExecutorTraits::new(vec!["compute".into()]).unwrap(),
                        30,
                    )
                    .with_tenant(tenant),
                    10,
                )),
            )
            .unwrap();
            let seq = a.projection().latest_sequence() + 1;
            let _ = execute_mutation(
                &mut a,
                &admin,
                MutationCommand::RoleAssign(RoleAssignCommand::new(
                    seq,
                    actor,
                    Role::Operator,
                    tenant,
                    10,
                )),
            )
            .unwrap();
            let h = host(ControlScope::Tenant(tenant), Some(actor));
            for action in [
                QueueAction::InspectTask,
                QueueAction::AdmitTask,
                QueueAction::ClaimRun,
                QueueAction::InspectClaimable,
                QueueAction::SubmitResult,
                QueueAction::RenewLease,
            ] {
                let seq = a.projection().latest_sequence() + 1;
                let _ = execute_mutation(
                    &mut a,
                    &admin,
                    MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
                        seq,
                        actor,
                        action.permission(),
                        tenant,
                        10,
                    )),
                )
                .unwrap();
            }
            let task = actionqueue_core::task::task_spec::TaskSpec::new(
                TaskId::new(),
                actionqueue_core::task::task_spec::TaskPayload::new(name.as_bytes().to_vec()),
                actionqueue_core::task::run_policy::RunPolicy::Once,
                actionqueue_core::task::constraints::TaskConstraints::new(3, None, None).unwrap(),
                Default::default(),
            )
            .unwrap()
            .with_tenant(tenant);
            execute_control(
                &mut a,
                &h,
                ControlOperation::AdmitTask(
                    actionqueue_core::admission::EnsureTaskRequest::for_task(task, vec![]).unwrap(),
                ),
                &MockClock::new(10),
            )
            .unwrap();
            actors.push(h);
        }
        *inner.shared_projection.write().unwrap() = a.projection().clone();
    }
    let one = actors[0].clone();
    let two = actors[1].clone();
    inner.host_authenticator = Some(Arc::new(move |headers, _| {
        match headers.get("authorization").and_then(|h| h.to_str().ok()) {
            Some("one") => Ok(one.clone()),
            Some("two") => Ok(two.clone()),
            Some("missing-scope") => Ok(host(ControlScope::SingleTenant, one.actor_id)),
            _ => Err(auth::AuthenticationError),
        }
    }));
    (root, state, actors[0].clone(), actors[1].clone())
}
async fn get(
    router: axum::Router,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = request.header("authorization", token);
    }
    let response = router.oneshot(request.body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}
#[test]
fn tenant_inspection_authentication_scope_filtering_and_current_revocation() {
    let (root, state, one, two) = fixture();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let router = build_router(state.clone());
        assert_eq!(get(router.clone(), "/api/v2/tasks", None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            get(router.clone(), "/api/v2/tasks", Some("missing-scope")).await.0,
            StatusCode::FORBIDDEN
        );
        let (status, body) = get(router.clone(), "/api/v2/tasks", Some("one")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
        let task = body["items"][0]["id"].as_str().unwrap();
        assert_eq!(
            get(router.clone(), &format!("/api/v2/tasks/{task}"), Some("two")).await.0,
            StatusCode::NOT_FOUND
        );
        let (_, body) = get(router.clone(), "/api/v2/runs", Some("one")).await;
        assert_eq!(body["items"].as_array().unwrap().len(), 1);
        let run = body["items"][0]["run_id"].as_str().unwrap();
        assert_eq!(
            get(router.clone(), &format!("/api/v2/runs/{run}"), Some("two")).await.0,
            StatusCode::NOT_FOUND
        );
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
            let seq = a.projection().latest_sequence() + 1;
            let _ = execute_mutation(
                &mut a,
                &host(ControlScope::Store, None),
                MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
                    seq,
                    one.actor_id.unwrap(),
                    QueueAction::InspectTask.permission(),
                    tenant,
                    11,
                )),
            )
            .unwrap();
            // Deliberately do not synchronize the read projection: inspection must
            // use the same current authority revision as grant validation.
        }
        assert_eq!(
            get(router.clone(), "/api/v2/tasks", Some("one")).await.0,
            StatusCode::FORBIDDEN
        );
        assert_eq!(get(router, "/api/v2/tasks", Some("two")).await.0, StatusCode::OK);
    });
    drop(two);
    runtime.block_on(maintenance::shutdown(&state));
    drop(state);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[derive(Clone)]
struct MovingClock(Arc<std::sync::atomic::AtomicU64>);
impl actionqueue_core::time::clock::Clock for MovingClock {
    fn now(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}
async fn post(
    router: axum::Router,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", token)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}
#[test]
fn http_remote_capacity_retry_expiry_and_revocation_before_retransmission() {
    let (root, mut state, one, two) = fixture();
    let clock = MovingClock(Arc::new(std::sync::atomic::AtomicU64::new(10)));
    let inner = Arc::get_mut(&mut state).unwrap();
    inner.clock = Arc::new(clock.clone());
    inner.remote_policy = actionqueue_runtime::remote::RemotePolicy {
        max_concurrent: 1,
        lease_timeout_secs: 3,
        retry_delay_secs: 2,
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let router = build_router(state.clone());
        let (_, body) = get(router.clone(), "/api/v2/runs", Some("one")).await;
        let run = body["items"][0]["run_id"].as_str().unwrap().to_owned();
        let (_, body) = get(router.clone(), "/api/v2/runs", Some("two")).await;
        let other = body["items"][0]["run_id"].as_str().unwrap().to_owned();
        let path = format!("/api/v2/actors/{}/claim", one.actor_id.unwrap());
        let other_path = format!("/api/v2/actors/{}/claim", two.actor_id.unwrap());
        let claim = |run: &str| serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":AttemptId::new()});
        // A real principal with grants still cannot claim another tenant's run.
        assert_eq!(post(router.clone(), &path, "one", claim(&other)).await.0, StatusCode::CONFLICT);
        let (status, work) = post(router.clone(), &path, "one", claim(&run)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(work["lease_expiry"], 13);
        assert_eq!(post(router.clone(), &other_path, "two", claim(&other)).await.0, StatusCode::CONFLICT);
        let result_path = format!("/api/v2/actors/{}/result",one.actor_id.unwrap());
        let disposition = actionqueue_core::disposition::AttemptDisposition::retryable_failure(BoundedError::new("retry").unwrap());
        let result = serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2",
            "run_id":run,"attempt_id":work["attempt_id"],"lease_fence":work["lease_fence"],
            "disposition_digest":actionqueue_core::disposition_digest::disposition_digest(&disposition),"disposition":disposition});
        assert_eq!(post(router.clone(), &result_path, "one", result.clone()).await.0, StatusCode::OK);
        assert_eq!(post(router.clone(), &path, "one", claim(&run)).await.0, StatusCode::CONFLICT);
        clock.0.store(12,std::sync::atomic::Ordering::Release);
        let (status, next) = post(router.clone(), &path, "one", claim(&run)).await;
        assert_eq!(status,StatusCode::OK);
        assert_eq!(next["failure_attempt_count"],1);
        assert_eq!(next["lease_expiry"],15);
        clock.0.store(15,std::sync::atomic::Ordering::Release);
        // An HTTP claim at expiry processes lease loss and frees global capacity.
        assert_eq!(post(router.clone(), &other_path, "two", claim(&other)).await.0, StatusCode::OK);
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
            let seq = a.projection().latest_sequence()+1;
            let _ = execute_mutation(&mut a,&host(ControlScope::Store,None),MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(seq,one.actor_id.unwrap(),QueueAction::SubmitResult.permission(),tenant,15))).unwrap();
        }
        let before = state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap();
        assert_eq!(post(router,&result_path,"one",result).await.0,StatusCode::CONFLICT);
        assert_eq!(before,state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap());
    });
    runtime.block_on(maintenance::shutdown(&state));
    drop(state);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn configured_bearer_hook_binds_scope_and_rejects_missing_or_failed_authentication() {
    let h = host(ControlScope::SingleTenant, None);
    let bytes = serde_json::to_vec(&serde_json::json!([{"token":"0123456789abcdef0123456789abcdef","actor_id":null,"scope":h.scope,"attribution":h.attribution}])).unwrap();
    let hook = auth::bearer_authenticator(&bytes).unwrap();
    let mut headers = axum::http::HeaderMap::new();
    let uri = "/api/v2/tasks".parse().unwrap();
    assert!(hook(&headers, &uri).is_err());
    headers.insert("authorization", "Bearer invalid".parse().unwrap());
    assert!(hook(&headers, &uri).is_err());
    headers.insert("authorization", "Bearer 0123456789abcdef0123456789abcdef".parse().unwrap());
    assert_eq!(hook(&headers, &uri).unwrap(), h);
    assert!(auth::bearer_authenticator(br#"[]"#).is_err());
}

#[test]
fn http_continuation_renewal_and_idle_timer_recovery() {
    use actionqueue_core::{continuation::*, disposition::AttemptDisposition};
    let (root, mut state, one, _) = fixture();
    let clock = MovingClock(Arc::new(std::sync::atomic::AtomicU64::new(10)));
    Arc::get_mut(&mut state).unwrap().clock = Arc::new(clock.clone());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let router = build_router(state.clone());
        let (_, body) = get(router.clone(),"/api/v2/runs",Some("one")).await;
        let run = body["items"][0]["run_id"].as_str().unwrap().to_owned();
        let path = format!("/api/v2/actors/{}",one.actor_id.unwrap());
        let claim = || serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":AttemptId::new()});
        let (status, work) = post(router.clone(),&format!("{path}/claim"),"one",claim()).await;
        assert_eq!(status,StatusCode::OK);
        let renewal = serde_json::json!({"run_id":run,"attempt_id":work["attempt_id"],"lease_fence":work["lease_fence"],"expiry":400});
        assert_eq!(post(router.clone(),&format!("{path}/renew"),"one",renewal.clone()).await.0,StatusCode::OK);
        let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
        let wait = WaitSpec::new(WaitId::new(),SignalFilter { tenant_id:Some(tenant),namespace:SignalNamespace::new("remote").unwrap(),kind:SignalKind::new("done").unwrap(),correlation_id:None,source_ref:None },WaitMatchPolicy::FirstMatch,SignalEligibility::After(SignalSequence::new(0)),Some(WaitDeadline { at:12,policy:WaitTimeoutPolicy::ResumeWithTimeout })).unwrap();
        let d = AttemptDisposition::awaiting(wait,None);
        let result = serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":work["attempt_id"],"lease_fence":work["lease_fence"],"disposition_digest":actionqueue_core::disposition_digest::disposition_digest(&d),"disposition":d});
        assert_eq!(post(router.clone(),&format!("{path}/result"),"one",result).await.0,StatusCode::OK);
        let before = state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap();
        assert_eq!(post(router.clone(),&format!("{path}/renew"),"one",renewal).await.0,StatusCode::CONFLICT);
        assert_eq!(before,state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap());
        clock.0.store(12,std::sync::atomic::Ordering::Release);
        // No request is required to resolve the deadline.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(state.control_authority.as_ref().unwrap().lock().unwrap().projection().get_run_state(&run.parse().unwrap()),Some(&actionqueue_core::run::RunState::Ready));
        let (status,next) = post(router.clone(),&format!("{path}/claim"),"one",claim()).await;
        assert_eq!(status,StatusCode::OK);
        assert_eq!(next["failure_attempt_count"],0);
        assert!(!next["resume_context"].is_null());
        clock.0.store(100,std::sync::atomic::Ordering::Release);
        // Actor timeout is independent of the longer renewed execution lease.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(state.control_authority.as_ref().unwrap().lock().unwrap().projection().get_actor(&one.actor_id.unwrap()).unwrap().deregistered_at.is_some());
    });
    runtime.block_on(maintenance::shutdown(&state));
    drop(state);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn http_compound_result_checks_effect_permissions_before_commit_and_retry() {
    let (root, state, one, _) = fixture();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let router = build_router(state.clone());
        let (_,body) = get(router.clone(),"/api/v2/runs",Some("one")).await;
        let run = body["items"][0]["run_id"].as_str().unwrap().to_owned();
        let path = format!("/api/v2/actors/{}",one.actor_id.unwrap());
        let (status,work) = post(router.clone(),&format!("{path}/claim"),"one",serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":AttemptId::new()})).await;
        assert_eq!(status,StatusCode::OK);
        let vector: serde_json::Value = serde_json::from_str(include_str!("../../../../conformance/aq-cont-1/disposition-v1-vector.json")).unwrap();
        let mut proposal = vector["output"]["disposition"].clone();
        proposal["consumption"] = serde_json::json!([]);
        let d: actionqueue_core::disposition::AttemptDisposition = serde_json::from_value(proposal).unwrap();
        let signal = d.emitted_signals()[0].signal_id.clone();
        let result = serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":work["attempt_id"],"lease_fence":work["lease_fence"],"disposition_digest":actionqueue_core::disposition_digest::disposition_digest(&d),"disposition":d});
        let before = state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap();
        assert_eq!(post(router.clone(),&format!("{path}/result"),"one",result.clone()).await.0,StatusCode::CONFLICT);
        assert_eq!(before,state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap());
        let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            let n = a.projection().latest_sequence()+1;
            let _ = execute_mutation(&mut a,&host(ControlScope::Store,None),MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(n,one.actor_id.unwrap(),QueueAction::AdmitSignal.permission(),tenant,10))).unwrap();
        }
        assert_eq!(post(router.clone(),&format!("{path}/result"),"one",result.clone()).await.0,StatusCode::OK);
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            assert!(a.projection().signals().get_signal(Some(tenant),&signal).is_some());
            assert!(a.projection().signals().get_signal(None,&signal).is_none());
            let n = a.projection().latest_sequence()+1;
            let _ = execute_mutation(&mut a,&host(ControlScope::Store,None),MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(n,one.actor_id.unwrap(),QueueAction::AdmitSignal.permission(),tenant,10))).unwrap();
        }
        let before = state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap();
        // SubmitResult itself is still granted; the revoked effect permission
        // must also be checked before acknowledging the exact accepted retry.
        assert_eq!(post(router,&format!("{path}/result"),"one",result).await.0,StatusCode::CONFLICT);
        assert_eq!(before,state.control_authority.as_ref().unwrap().lock().unwrap().projection().projection_digest().unwrap());
    });
    runtime.block_on(maintenance::shutdown(&state));
    drop(state);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "workflow")]
#[test]
fn http_remote_cron_claims_continue_past_five_occurrences() {
    use actionqueue_core::{
        disposition::AttemptDisposition,
        task::{run_policy::*, task_spec::*},
    };
    for bounded in [false, true] {
        let (root, mut state, one, _) = fixture_with_workflow(true);
        let clock = MovingClock(Arc::new(std::sync::atomic::AtomicU64::new(10)));
        Arc::get_mut(&mut state).unwrap().clock = Arc::new(clock.clone());
        let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
        let task = TaskId::new();
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            let policy = CronPolicy::new("* * * * * * *").unwrap();
            let policy = if bounded { policy.with_max_occurrences(8).unwrap() } else { policy };
            let spec = TaskSpec::new(
                task,
                TaskPayload::new(vec![]),
                RunPolicy::Cron(policy),
                Default::default(),
                Default::default(),
            )
            .unwrap()
            .with_tenant(tenant);
            execute_control(
                &mut a,
                &one,
                ControlOperation::AdmitTask(
                    actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
                ),
                &MockClock::new(10),
            )
            .unwrap();
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let router = build_router(state.clone());
            let path = format!("/api/v2/actors/{}", one.actor_id.unwrap());
            // First finish the fixture's once task, then eight cron occurrences.
            for n in 0..9 {
                clock.0.store(10+n, std::sync::atomic::Ordering::Release);
                let (status, eligible) = get(router.clone(), &format!("{path}/claimable"), Some("one")).await;
                assert_eq!(status, StatusCode::OK);
                let run = eligible["runs"][0].as_str().unwrap();
                let (status, work) = post(router.clone(), &format!("{path}/claim"), "one", serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":AttemptId::new()})).await;
                assert_eq!(status, StatusCode::OK, "{work}");
                let d = AttemptDisposition::complete(None);
                let result = serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2","run_id":run,"attempt_id":work["attempt_id"],"lease_fence":work["lease_fence"],"disposition_digest":actionqueue_core::disposition_digest::disposition_digest(&d),"disposition":d});
                assert_eq!(post(router.clone(), &format!("{path}/result"), "one", result).await.0, StatusCode::OK);
            }
            // Ingress runs the same maintenance even after the last result.
            assert_eq!(get(router, &format!("{path}/claimable"), Some("one")).await.0, StatusCode::OK);
            let a = state.control_authority.as_ref().unwrap().lock().unwrap();
            assert_eq!(a.projection().runs_for_task(task).filter(|r| r.state() == actionqueue_core::run::RunState::Completed).count(), 8);
            assert_eq!(a.projection().runs_for_task(task).filter(|r| !r.state().is_terminal()).count(), if bounded { 0 } else { 5 });
        });
        runtime.block_on(maintenance::shutdown(&state));
        drop(state);
        drop(runtime);
        std::fs::remove_dir_all(root).unwrap();
    }
}

// F-017: HTTP selection and claim use the same priority as local Ready promotion.
#[test]
fn http_remote_claims_higher_priority_scheduled_work_first() {
    let (root, state, one, _) = fixture();
    let ControlScope::Tenant(tenant) = one.scope else { unreachable!() };
    let mut runs = Vec::new();
    {
        let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
        for priority in [1, 100] {
            let task = actionqueue_core::task::task_spec::TaskSpec::new(
                TaskId::new(),
                actionqueue_core::task::task_spec::TaskPayload::new(vec![]),
                actionqueue_core::task::run_policy::RunPolicy::Once,
                Default::default(),
                actionqueue_core::task::metadata::TaskMetadata::new(vec![], priority, None),
            )
            .unwrap()
            .with_tenant(tenant);
            let task_id = task.id();
            execute_control(
                &mut a,
                &one,
                ControlOperation::AdmitTask(
                    actionqueue_core::admission::EnsureTaskRequest::for_task(task, vec![]).unwrap(),
                ),
                &MockClock::new(10),
            )
            .unwrap();
            runs.push(a.projection().runs_for_task(task_id).next().unwrap().id());
        }
    }
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let router = build_router(state.clone());
        let path = format!("/api/v2/actors/{}", one.actor_id.unwrap());
        let (status, body) = get(router.clone(), &format!("{path}/claimable"), Some("one")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["runs"][0], serde_json::json!(runs[1]));
        let claim = |run| {
            serde_json::json!({"protocol_version":1,"contract_revision":"AQ-CONT-1-r2",
            "run_id":run,"attempt_id":AttemptId::new()})
        };
        assert_eq!(
            post(router.clone(), &format!("{path}/claim"), "one", claim(runs[0])).await.0,
            StatusCode::CONFLICT
        );
        let (status, work) = post(router, &format!("{path}/claim"), "one", claim(runs[1])).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(work["run_id"], serde_json::json!(runs[1]));
    });
    runtime.block_on(maintenance::shutdown(&state));
    drop(state);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}
