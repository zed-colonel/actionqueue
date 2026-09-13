//! AQ-12 shared-service parity, authorization, bounded inspection and recursive redaction.
use std::sync::{Arc, Mutex, RwLock};

use actionqueue_core::{
    admission::*,
    bounded::OpaqueRef,
    causal::*,
    control::*,
    ids::*,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
    time::clock::MockClock,
};
use actionqueue_daemon::{
    bootstrap::{ReadyStatus, RouterConfig},
    http::*,
};
use actionqueue_runtime::{
    control::{execute_control, ControlOperation},
    inspection::{Inspector, Query},
};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::{bootstrap::RecoveryObservations, reducer::ReplayReducer},
    wal::{fs_writer::WalFsWriter, InstrumentedWalWriter, WalAppendTelemetry},
};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use tower::ServiceExt;
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("HOST_SECRET").unwrap()),
    }
}
fn request(priority: i32) -> EnsureTaskRequest {
    let t = TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(b"PAYLOAD_SECRET".to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::new(vec![], priority, Some("DESCRIPTION_SECRET".into())),
    )
    .unwrap();
    EnsureTaskRequest::new(
        AdmissionKey::new(format!("key/{}", t.id())).unwrap(),
        t,
        vec![],
        CausalContext::new(
            TraceId::new("trace-secret").unwrap(),
            CorrelationId::new("correlation-secret").unwrap(),
        )
        .with_origin_ref(OpaqueRef::new("origin/EXACT%20secret").unwrap()),
        None,
    )
    .unwrap()
}
fn fixture(auth: bool, control: bool) -> (axum::Router, ControlMutationAuthority, RouterState) {
    let path = std::env::temp_dir().join(format!("aq12-http-{}.wal", TaskId::new()));
    let telemetry = WalAppendTelemetry::new();
    let writer =
        InstrumentedWalWriter::new(WalFsWriter::new_raw_for_test(path).unwrap(), telemetry.clone());
    let a = Arc::new(Mutex::new(
        StorageMutationAuthority::new(writer, ReplayReducer::new()).with_host(host()),
    ));
    let state = RouterStateInner::with_control_authority(
        RouterConfig { control_enabled: control, metrics_enabled: true },
        Arc::new(RwLock::new(ReplayReducer::new())),
        RouterObservability {
            metrics: Arc::new(
                actionqueue_daemon::metrics::registry::MetricsRegistry::new(Some(
                    "127.0.0.1:0".parse().unwrap(),
                ))
                .unwrap(),
            ),
            wal_append_telemetry: telemetry,
            clock: Arc::new(MockClock::new(10)),
            recovery_observations: RecoveryObservations::zero(),
        },
        a.clone(),
        ReadyStatus::ready(),
    );
    let state = if auth {
        state.with_host_authenticator(Arc::new(|headers, _| {
            if headers.get("authorization").is_some_and(|v| v == "Bearer valid") {
                Ok(host())
            } else {
                Err(auth::AuthenticationError)
            }
        }))
    } else {
        state
    };
    let state = Arc::new(state);
    (build_router(state.clone()), a, state)
}
async fn send(
    router: &axum::Router,
    method: &str,
    path: &str,
    body: impl Into<Body>,
    auth: bool,
) -> (u16, serde_json::Value, String) {
    let mut builder =
        Request::builder().method(method).uri(path).header("content-type", "application/json");
    if auth {
        builder = builder.header("authorization", "Bearer valid");
    }
    let r = router.clone().oneshot(builder.body(body.into()).unwrap()).await.unwrap();
    let status = r.status().as_u16();
    let bytes = r.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    (status, serde_json::from_str(&text).unwrap_or_default(), text)
}
#[tokio::test]
async fn created_duplicate_conflict_and_embedded_http_parity() {
    let (router, a, _) = fixture(true, true);
    let q = request(2);
    let body = serde_json::to_vec(&q).unwrap();
    let (status, created, _) =
        send(&router, "POST", "/api/v2/admissions:ensure", body.clone(), true).await;
    assert_eq!(status, 201);
    assert_eq!(send(&router, "POST", "/api/v2/admissions:ensure", body, true).await.0, 200);
    let mut task = q.task_spec().clone();
    task.set_payload(TaskPayload::new(b"changed".to_vec()));
    let changed = EnsureTaskRequest::new(
        q.admission_key().clone(),
        task,
        vec![],
        q.causal_context().clone(),
        None,
    )
    .unwrap();
    let (status, body, text) = send(
        &router,
        "POST",
        "/api/v2/admissions:ensure",
        serde_json::to_vec(&changed).unwrap(),
        true,
    )
    .await;
    assert_eq!(status, 409);
    assert_eq!(body["error_code"], "conflict");
    assert!(!text.contains("changed"));
    let id = q.task_spec().id();
    let (status, actual, text) =
        send(&router, "GET", &format!("/api/v2/tasks/{id}"), Body::empty(), true).await;
    assert_eq!(status, 200);
    let p = a.lock().unwrap().projection().clone();
    let h = host();
    let i = Inspector::new(&p, &h, false, Default::default(), false, 10).unwrap();
    assert_eq!(actual, serde_json::to_value(i.get_task(id).unwrap()).unwrap());
    for canary in [
        "PAYLOAD_SECRET",
        "HOST_SECRET",
        "DESCRIPTION_SECRET",
        "trace-secret",
        "correlation-secret",
        "origin/EXACT",
    ] {
        assert!(!text.contains(canary), "{canary}");
    }
    assert_eq!(actual["payload"]["size_bytes"], 14);
    assert!(created["Created"]["task_id"].is_string());
    for _ in 0..2 {
        assert_eq!(
            send(&router, "POST", &format!("/api/v2/tasks/{id}:cancel"), Body::empty(), true)
                .await
                .0,
            200
        );
    }
}
#[tokio::test]
async fn all_object_routes_require_auth_and_removed_routes_are_absent() {
    for auth_config in [false, true] {
        let (r, _, _) = fixture(auth_config, true);
        for path in [
            "/api/v2/tasks",
            "/api/v2/runs",
            "/api/v2/signals",
            "/api/v2/waits",
            "/api/v2/inspect?origin_ref=x",
            "/api/v2/admissions?key=x",
        ] {
            assert_eq!(send(&r, "GET", path, Body::empty(), false).await.0, 401, "{path}");
        }
        assert_eq!(send(&r, "POST", "/api/v2/admissions:ensure", "invalid", false).await.0, 401);
        assert_eq!(send(&r, "GET", "/api/v1/tasks", Body::empty(), true).await.0, 404);
    }
    let (r, _, _) = fixture(true, false);
    assert_eq!(send(&r, "POST", "/api/v2/admissions:ensure", "{}", true).await.0, 404);
    assert_eq!(send(&r, "GET", "/healthz", Body::empty(), false).await.0, 200);
    assert_eq!(send(&r, "GET", "/ready", Body::empty(), false).await.0, 200);
}
#[tokio::test]
async fn exact_origin_filter_pagination_and_query_flag_cannot_grant_disclosure() {
    let (r, a, _) = fixture(true, true);
    for priority in [1, 9] {
        execute_control(
            &mut a.lock().unwrap(),
            &host(),
            ControlOperation::AdmitTask(request(priority)),
            &MockClock::new(10),
        )
        .unwrap();
    }
    let (status, page, _) = send(
        &r,
        "GET",
        "/api/v2/inspect?origin_ref=origin%2FEXACT%2520secret",
        Body::empty(),
        true,
    )
    .await;
    assert_eq!(status, 200);
    assert!(page["different_fields"].as_array().unwrap().contains(&serde_json::json!("priority")));
    assert!(page["notice"].as_str().unwrap().contains("attribution only"));
    let (_, wrong, _) =
        send(&r, "GET", "/api/v2/inspect?origin_ref=origin%2FEXACT%20secret", Body::empty(), true)
            .await;
    assert!(wrong["nodes"]["items"].as_array().unwrap().is_empty());
    assert_eq!(
        send(&r, "GET", "/api/v2/tasks?display_references=true", Body::empty(), true).await.0,
        403
    );
    // Work with a frozen projection to exercise cursor binding independently of scheduling ticks.
    let p = a.lock().unwrap().projection().clone();
    let h = host();
    let i = Inspector::new(&p, &h, false, Default::default(), false, 10).unwrap();
    let mut q = Query { limit: Some(1), ..Default::default() };
    let first = i.list_tasks(&q).unwrap();
    q.cursor = first.next_cursor;
    assert_eq!(i.list_tasks(&q).unwrap().items.len(), 1);
    q.origin_ref = Some("different".into());
    assert_eq!(
        i.list_tasks(&q).unwrap_err(),
        actionqueue_runtime::inspection::InspectionError::InvalidQuery
    );
    q.origin_ref = None;
    execute_control(
        &mut a.lock().unwrap(),
        &h,
        ControlOperation::AdmitTask(request(3)),
        &MockClock::new(10),
    )
    .unwrap();
    let newer = a.lock().unwrap().projection().clone();
    let i = Inspector::new(&newer, &h, false, Default::default(), false, 10).unwrap();
    assert_eq!(
        i.list_tasks(&q).unwrap_err(),
        actionqueue_runtime::inspection::InspectionError::StaleCursor
    );
}
#[tokio::test]
async fn unmatched_signals_and_metrics_remain_structural_and_redacted() {
    use actionqueue_core::{continuation::*, data_ref::DataRef};
    let (r, a, _) = fixture(true, true);
    let q = AdmitSignalRequest::new(
        SignalId::new("signal-1").unwrap(),
        SignalNamespace::new("secret-namespace").unwrap(),
        SignalKind::new("secret-kind").unwrap(),
        Some(CorrelationId::new("exact-secret").unwrap()),
        None,
        Some(OpaqueRef::new("SOURCE_SECRET").unwrap()),
        Some(DataRef::from_bytes(b"SIGNAL_SECRET".to_vec()).unwrap()),
        None,
        None,
    )
    .unwrap();
    for expected in [201, 200] {
        assert_eq!(
            send(&r, "POST", "/api/v2/signals", serde_json::to_vec(&q).unwrap(), true).await.0,
            expected
        );
    }
    let (status, v, text) =
        send(&r, "GET", "/api/v2/inspect?correlation_id=exact-secret", Body::empty(), true).await;
    assert_eq!(status, 200);
    assert_eq!(v["nodes"]["items"].as_array().unwrap().len(), 1);
    assert!(!text.contains("SOURCE_SECRET"));
    assert!(!text.contains("SIGNAL_SECRET"));
    let (_, _, first) = send(&r, "GET", "/metrics", Body::empty(), false).await;
    let (_, _, second) = send(&r, "GET", "/metrics", Body::empty(), false).await;
    assert_eq!(first, second);
    for canary in [
        "exact-secret",
        "SOURCE_SECRET",
        "SIGNAL_SECRET",
        "secret-namespace",
        "secret-kind",
        "signal-1",
    ] {
        assert!(!first.contains(canary));
    }
    assert!(first.contains("actionqueue_signal_duplicate_total 1"));
    assert_eq!(a.lock().unwrap().telemetry().snapshot().signal_duplicates, 1);
}
#[tokio::test]
async fn malformed_and_oversized_requests_have_bounded_errors() {
    let (r, _, _) = fixture(true, true);
    assert_eq!(send(&r, "POST", "/api/v2/admissions:ensure", "{", true).await.0, 400);
    assert_eq!(
        send(&r, "POST", "/api/v2/signals", vec![b' '; 2 * 1024 * 1024 + 1], true).await.0,
        413
    );
    for q in ["limit=0", "limit=1001", "offset=0"] {
        assert_eq!(
            send(&r, "GET", &format!("/api/v2/tasks?{q}"), Body::empty(), true).await.0,
            400
        );
    }
}

#[tokio::test]
async fn poisoned_authority_blocks_readiness_and_mutation() {
    let (r, a, _) = fixture(true, true);
    let _ = std::panic::catch_unwind(|| {
        let _guard = a.lock().unwrap();
        panic!("injected authority poison");
    });
    assert_eq!(send(&r, "GET", "/ready", Body::empty(), false).await.0, 503);
    assert_eq!(
        send(
            &r,
            "POST",
            "/api/v2/admissions:ensure",
            serde_json::to_vec(&request(1)).unwrap(),
            true
        )
        .await
        .0,
        503
    );
}

#[tokio::test]
async fn query_and_path_rejections_never_echo_sensitive_input() {
    let (r, _, _) = fixture(true, true);
    for path in [
        "/api/v2/tasks/INPUT_CANARY",
        "/api/v2/tasks?display_references=INPUT_CANARY",
        "/api/v2/tasks?INPUT_CANARY=1",
    ] {
        let (status, _, body) = send(&r, "GET", path, Body::empty(), true).await;
        assert_eq!(status, 400);
        assert!(!body.contains("INPUT_CANARY"));
    }
}

#[tokio::test]
async fn same_revision_projection_divergence_fails_closed_and_counts_once() {
    let (router, a, _) = fixture(true, true);
    let (_, other, _) = fixture(true, true);
    let q = request(0);
    assert_eq!(
        send(&router, "POST", "/api/v2/admissions:ensure", serde_json::to_vec(&q).unwrap(), true)
            .await
            .0,
        201
    );
    execute_control(
        &mut other.lock().unwrap(),
        &host(),
        ControlOperation::AdmitTask(request(1)),
        &MockClock::new(10),
    )
    .unwrap();
    let state = Arc::new(
        RouterStateInner::with_control_authority(
            RouterConfig { control_enabled: true, metrics_enabled: true },
            Arc::new(RwLock::new(other.lock().unwrap().projection().clone())),
            RouterObservability {
                metrics: Arc::new(
                    actionqueue_daemon::metrics::registry::MetricsRegistry::new(None).unwrap(),
                ),
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock: Arc::new(MockClock::new(10)),
                recovery_observations: RecoveryObservations::zero(),
            },
            a.clone(),
            ReadyStatus::ready(),
        )
        .with_host_authenticator(Arc::new(|_, _| Ok(host())))
        .without_background_maintenance(),
    );
    let router = build_router(state.clone());
    assert!(maintenance::tick(&state).is_err());
    assert_eq!(send(&router, "GET", "/ready", Body::empty(), false).await.0, 503);
    assert_eq!(
        send(&router, "POST", "/api/v2/admissions:ensure", serde_json::to_vec(&q).unwrap(), true)
            .await
            .0,
        503
    );
    assert_eq!(a.lock().unwrap().telemetry().snapshot().projection_mismatches, 1);
}
