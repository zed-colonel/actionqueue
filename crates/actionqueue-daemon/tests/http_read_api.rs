//! Phase 6 read API integration tests.
//!
//! These tests assert deterministic, read-only response shapes for the daemon
//! HTTP introspection endpoints. They use the in-process router from bootstrap
//! and seeded projection state to avoid IO beyond the configured data directory.

use std::str::FromStr;
use std::sync::Arc;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_daemon::time::clock::{MockClock, SharedDaemonClock};
use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};
use axum::body::Body;
use axum::http::{Method, StatusCode};
use http_body_util::BodyExt;

fn build_task_spec(task_id: TaskId, priority: i32, concurrency_key: Option<&str>) -> TaskSpec {
    let constraints = TaskConstraints::new(1, None, concurrency_key.map(|key| key.to_string()))
        .expect("constraints should be valid");
    let metadata = TaskMetadata::new(vec![], priority, None);
    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        constraints,
        metadata,
    )
    .expect("task spec should be valid")
}

fn apply_event(reducer: &mut ReplayReducer, sequence: u64, event: WalEventType) {
    let event = WalEvent::new(sequence, event);
    reducer.apply(&event).expect("event should apply");
}

fn run_instance_scheduled(
    run_id: RunId,
    task_id: TaskId,
    scheduled_at: u64,
    created_at: u64,
) -> RunInstance {
    RunInstance::new_scheduled_with_id(run_id, task_id, scheduled_at, created_at)
        .expect("run instance should be valid")
}

/// Builds deterministic projection state for read-route integration assertions.
///
/// The seeded state includes two tasks and two runs spanning `Ready` and
/// `Completed` terminal coverage, with one started attempt on the completed run
/// so aggregate stats include a non-zero attempt count.
fn seeded_projection_for_deterministic_reads() -> (ReplayReducer, TaskId, TaskId, RunId, RunId) {
    let mut reducer = ReplayReducer::new();
    let task_high = TaskId::from_str("12345678-1234-1234-1234-1234567890ab").unwrap();
    let task_low = TaskId::from_str("12345678-1234-1234-1234-1234567890ac").unwrap();
    let run_completed = RunId::from_str("aaaaaaaa-1111-2222-3333-444444444444").unwrap();
    let run_ready = RunId::from_str("bbbbbbbb-1111-2222-3333-444444444444").unwrap();
    let attempt_completed = AttemptId::from_str("cccccccc-1111-2222-3333-444444444444").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated {
            task_spec: build_task_spec(task_high, 9, Some("alpha")),
            timestamp: 1_000,
        },
    );
    apply_event(
        &mut reducer,
        2,
        WalEventType::TaskCreated {
            task_spec: build_task_spec(task_low, 3, Some("beta")),
            timestamp: 1_001,
        },
    );

    apply_event(
        &mut reducer,
        3,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_completed, task_high, 2_000, 2_000),
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 1_010,
        },
    );
    apply_event(
        &mut reducer,
        5,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 1_011,
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 1_012,
        },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::AttemptStarted {
            run_id: run_completed,
            attempt_id: attempt_completed,
            timestamp: 1_013,
        },
    );
    apply_event(
        &mut reducer,
        8,
        WalEventType::AttemptFinished {
            run_id: run_completed,
            attempt_id: attempt_completed,
            result: AttemptResultKind::Success,
            error: None,
            output: None,
            timestamp: 1_014,
        },
    );
    apply_event(
        &mut reducer,
        9,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 1_015,
        },
    );

    apply_event(
        &mut reducer,
        10,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_ready, task_low, 2_001, 2_001),
        },
    );
    apply_event(
        &mut reducer,
        11,
        WalEventType::RunStateChanged {
            run_id: run_ready,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 1_016,
        },
    );

    (reducer, task_high, task_low, run_completed, run_ready)
}

fn build_router_with_projection(projection: ReplayReducer) -> axum::Router<()> {
    build_router_with_feature_settings(projection, false, false)
}

fn build_router_with_control_setting(
    projection: ReplayReducer,
    control_enabled: bool,
) -> axum::Router<()> {
    build_router_with_feature_settings(projection, control_enabled, false)
}

fn build_router_with_feature_settings(
    projection: ReplayReducer,
    control_enabled: bool,
    metrics_enabled: bool,
) -> axum::Router<()> {
    let metrics_bind = if metrics_enabled {
        Some(std::net::SocketAddr::from(([127, 0, 0, 1], 9090)))
    } else {
        None
    };
    let metrics_registry = std::sync::Arc::new(
        actionqueue_daemon::metrics::registry::MetricsRegistry::new(metrics_bind)
            .expect("test metrics registry should initialize"),
    );
    let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));
    let wal_append_telemetry = WalAppendTelemetry::new();
    let recovery_observations = RecoveryObservations::zero();

    let shared_projection = std::sync::Arc::new(std::sync::RwLock::new(projection.clone()));
    let router_state_inner = if control_enabled {
        let unique = format!(
            "actionqueue-daemon-http-read-api-{}-{}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        );
        let wal_path = std::env::temp_dir().join(unique);
        let wal_writer = WalFsWriter::new(wal_path).expect("test wal writer should initialize");
        let wal_writer = InstrumentedWalWriter::new(wal_writer, wal_append_telemetry.clone());
        let authority = StorageMutationAuthority::new(wal_writer, projection);
        actionqueue_daemon::http::RouterStateInner::with_control_authority(
            actionqueue_daemon::bootstrap::RouterConfig { control_enabled, metrics_enabled },
            shared_projection,
            actionqueue_daemon::http::RouterObservability {
                metrics: metrics_registry,
                wal_append_telemetry: wal_append_telemetry.clone(),
                clock,
                recovery_observations,
            },
            std::sync::Arc::new(std::sync::Mutex::new(authority)),
            actionqueue_daemon::bootstrap::ReadyStatus::ready(),
        )
    } else {
        actionqueue_daemon::http::RouterStateInner::new(
            actionqueue_daemon::bootstrap::RouterConfig { control_enabled, metrics_enabled },
            shared_projection,
            actionqueue_daemon::http::RouterObservability {
                metrics: metrics_registry,
                wal_append_telemetry,
                clock,
                recovery_observations,
            },
            actionqueue_daemon::bootstrap::ReadyStatus::ready(),
        )
    };
    let router_state = std::sync::Arc::new(router_state_inner);
    actionqueue_daemon::http::build_router(router_state).with_state(())
}

async fn send_request(router: &mut axum::Router<()>, path: &str) -> axum::response::Response {
    use axum::http::Request;
    use tower::Service;

    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .expect("request should build");
    let mut service = router.as_service::<Body>();
    service.call(request).await.expect("request should succeed")
}

async fn send_post_request(router: &mut axum::Router<()>, path: &str) -> axum::response::Response {
    use axum::http::Request;
    use tower::Service;

    let request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .body(Body::empty())
        .expect("request should build");
    let mut service = router.as_service::<Body>();
    service.call(request).await.expect("request should succeed")
}

async fn response_body_string(response: axum::response::Response) -> String {
    let bytes = response.into_body().collect().await.expect("body should collect").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("response body should be utf-8")
}

#[tokio::test]
async fn health_ready_stats_routes_return_success() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_projection(reducer);

    let response = send_request(&mut router, "/healthz").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_body_string(response).await, r#"{"status":"ok"}"#);

    let response = send_request(&mut router, "/ready").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_body_string(response).await, r#"{"status":"ready"}"#);

    let response = send_request(&mut router, "/api/v1/stats").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("stats should be valid json");
    assert_eq!(
        value,
        serde_json::json!({
            "total_tasks": 0,
            "total_runs": 0,
            "attempts_total": 0,
            "runs_by_state": {
                "scheduled": 0,
                "ready": 0,
                "leased": 0,
                "running": 0,
                "retry_wait": 0,
                "completed": 0,
                "failed": 0,
                "canceled": 0
            },
            "latest_sequence": 0
        })
    );
}

#[tokio::test]
async fn stats_route_returns_deterministic_authoritative_counts_for_seeded_projection() {
    let (reducer, _, _, _, _) = seeded_projection_for_deterministic_reads();
    let mut router = build_router_with_projection(reducer);

    let first = send_request(&mut router, "/api/v1/stats").await;
    assert_eq!(first.status(), StatusCode::OK);
    let first_body = response_body_string(first).await;
    let first_value: serde_json::Value =
        serde_json::from_str(&first_body).expect("stats should be valid json");
    assert_eq!(
        first_value,
        serde_json::json!({
            "total_tasks": 2,
            "total_runs": 2,
            "attempts_total": 1,
            "runs_by_state": {
                "scheduled": 0,
                "ready": 1,
                "leased": 0,
                "running": 0,
                "retry_wait": 0,
                "completed": 1,
                "failed": 0,
                "canceled": 0
            },
            "latest_sequence": 11
        })
    );

    let second = send_request(&mut router, "/api/v1/stats").await;
    assert_eq!(second.status(), StatusCode::OK);
    let second_body = response_body_string(second).await;
    let second_value: serde_json::Value =
        serde_json::from_str(&second_body).expect("stats should be valid json");
    assert_eq!(second_value, first_value);
}

#[tokio::test]
async fn read_routes_are_repeatable_and_do_not_mutate_projection_truth() {
    let (reducer, task_high, _task_low, run_completed, _run_ready) =
        seeded_projection_for_deterministic_reads();
    let mut router = build_router_with_projection(reducer);

    let baseline_stats_response = send_request(&mut router, "/api/v1/stats").await;
    assert_eq!(baseline_stats_response.status(), StatusCode::OK);
    let baseline_stats_body = response_body_string(baseline_stats_response).await;
    let baseline_stats: serde_json::Value =
        serde_json::from_str(&baseline_stats_body).expect("stats should be valid json");

    let health_first = send_request(&mut router, "/healthz").await;
    assert_eq!(health_first.status(), StatusCode::OK);
    let health_first_body = response_body_string(health_first).await;
    let health_second = send_request(&mut router, "/healthz").await;
    assert_eq!(health_second.status(), StatusCode::OK);
    assert_eq!(response_body_string(health_second).await, health_first_body);

    let ready_first = send_request(&mut router, "/ready").await;
    assert_eq!(ready_first.status(), StatusCode::OK);
    let ready_first_body = response_body_string(ready_first).await;
    let ready_second = send_request(&mut router, "/ready").await;
    assert_eq!(ready_second.status(), StatusCode::OK);
    assert_eq!(response_body_string(ready_second).await, ready_first_body);

    let tasks_first = send_request(&mut router, "/api/v1/tasks").await;
    assert_eq!(tasks_first.status(), StatusCode::OK);
    let tasks_first_body = response_body_string(tasks_first).await;
    let tasks_first_value: serde_json::Value =
        serde_json::from_str(&tasks_first_body).expect("tasks list should be valid json");
    let tasks_second = send_request(&mut router, "/api/v1/tasks").await;
    assert_eq!(tasks_second.status(), StatusCode::OK);
    let tasks_second_body = response_body_string(tasks_second).await;
    let tasks_second_value: serde_json::Value =
        serde_json::from_str(&tasks_second_body).expect("tasks list should be valid json");
    assert_eq!(tasks_second_value, tasks_first_value);
    let task_ids: Vec<String> = tasks_second_value["tasks"]
        .as_array()
        .expect("tasks array should exist")
        .iter()
        .map(|entry| entry["id"].as_str().expect("task id should be string").to_string())
        .collect();
    assert_eq!(
        task_ids,
        vec![
            "12345678-1234-1234-1234-1234567890ab".to_string(),
            "12345678-1234-1234-1234-1234567890ac".to_string(),
        ]
    );

    let task_get_path = format!("/api/v1/tasks/{task_high}");
    let task_get_first = send_request(&mut router, &task_get_path).await;
    assert_eq!(task_get_first.status(), StatusCode::OK);
    let task_get_first_body = response_body_string(task_get_first).await;
    let task_get_second = send_request(&mut router, &task_get_path).await;
    assert_eq!(task_get_second.status(), StatusCode::OK);
    assert_eq!(response_body_string(task_get_second).await, task_get_first_body);

    let runs_first = send_request(&mut router, "/api/v1/runs").await;
    assert_eq!(runs_first.status(), StatusCode::OK);
    let runs_first_body = response_body_string(runs_first).await;
    let runs_first_value: serde_json::Value =
        serde_json::from_str(&runs_first_body).expect("runs list should be valid json");
    let runs_second = send_request(&mut router, "/api/v1/runs").await;
    assert_eq!(runs_second.status(), StatusCode::OK);
    let runs_second_body = response_body_string(runs_second).await;
    let runs_second_value: serde_json::Value =
        serde_json::from_str(&runs_second_body).expect("runs list should be valid json");
    assert_eq!(runs_second_value, runs_first_value);
    let run_ids: Vec<String> = runs_second_value["runs"]
        .as_array()
        .expect("runs array should exist")
        .iter()
        .map(|entry| entry["run_id"].as_str().expect("run id should be string").to_string())
        .collect();
    assert_eq!(
        run_ids,
        vec![
            "aaaaaaaa-1111-2222-3333-444444444444".to_string(),
            "bbbbbbbb-1111-2222-3333-444444444444".to_string(),
        ]
    );

    let run_get_path = format!("/api/v1/runs/{run_completed}");
    let run_get_first = send_request(&mut router, &run_get_path).await;
    assert_eq!(run_get_first.status(), StatusCode::OK);
    let run_get_first_body = response_body_string(run_get_first).await;
    let run_get_second = send_request(&mut router, &run_get_path).await;
    assert_eq!(run_get_second.status(), StatusCode::OK);
    assert_eq!(response_body_string(run_get_second).await, run_get_first_body);

    let final_stats_response = send_request(&mut router, "/api/v1/stats").await;
    assert_eq!(final_stats_response.status(), StatusCode::OK);
    let final_stats_body = response_body_string(final_stats_response).await;
    let final_stats: serde_json::Value =
        serde_json::from_str(&final_stats_body).expect("stats should be valid json");
    assert_eq!(final_stats, baseline_stats);
}

#[tokio::test]
async fn metrics_route_returns_200_and_prometheus_text_when_enabled() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_feature_settings(reducer, false, true);

    let response = send_request(&mut router, "/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .expect("content-type header should exist")
        .to_str()
        .expect("content-type should be valid ascii")
        .to_string();
    assert_eq!(content_type, "text/plain; version=0.0.4");
}

#[tokio::test]
async fn metrics_route_is_absent_when_metrics_are_disabled() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_feature_settings(reducer, false, false);

    let response = send_request(&mut router, "/metrics").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn control_routes_are_absent_when_control_feature_is_disabled() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_control_setting(reducer, false);
    let control_paths = [
        "/api/v1/tasks/00000000-0000-0000-0000-000000000123/cancel",
        "/api/v1/runs/00000000-0000-0000-0000-000000000456/cancel",
        "/api/v1/engine/pause",
        "/api/v1/engine/resume",
    ];

    for path in control_paths {
        let response = send_post_request(&mut router, path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn control_engine_resume_first_call_on_fresh_projection_is_already_resumed() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_control_setting(reducer, true);

    let response = send_post_request(&mut router, "/api/v1/engine/resume").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_body_string(response).await, r#"{"status":"already_resumed"}"#);
}

#[tokio::test]
async fn control_engine_pause_and_resume_calls_are_idempotent() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_control_setting(reducer, true);

    let first = send_post_request(&mut router, "/api/v1/engine/pause").await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(response_body_string(first).await, r#"{"status":"paused"}"#);

    let second = send_post_request(&mut router, "/api/v1/engine/pause").await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(response_body_string(second).await, r#"{"status":"already_paused"}"#);

    let resume_first = send_post_request(&mut router, "/api/v1/engine/resume").await;
    assert_eq!(resume_first.status(), StatusCode::OK);
    assert_eq!(response_body_string(resume_first).await, r#"{"status":"resumed"}"#);

    let resume_second = send_post_request(&mut router, "/api/v1/engine/resume").await;
    assert_eq!(resume_second.status(), StatusCode::OK);
    assert_eq!(response_body_string(resume_second).await, r#"{"status":"already_resumed"}"#);
}

#[tokio::test]
async fn control_run_cancel_first_second_and_terminal_paths_are_idempotent() {
    let mut reducer = ReplayReducer::new();

    let task_cancelable = TaskId::from_str("00000000-0000-0000-0000-000000000123").unwrap();
    let run_cancelable = RunId::from_str("00000000-0000-0000-0000-000000000456").unwrap();
    let task_terminal = TaskId::from_str("00000000-0000-0000-0000-000000000124").unwrap();
    let run_terminal = RunId::from_str("00000000-0000-0000-0000-000000000457").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated {
            task_spec: build_task_spec(task_cancelable, 1, None),
            timestamp: 10,
        },
    );
    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_cancelable, task_cancelable, 10, 10),
        },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::RunStateChanged {
            run_id: run_cancelable,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 11,
        },
    );

    apply_event(
        &mut reducer,
        4,
        WalEventType::TaskCreated {
            task_spec: build_task_spec(task_terminal, 1, None),
            timestamp: 12,
        },
    );
    apply_event(
        &mut reducer,
        5,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_terminal, task_terminal, 12, 12),
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::RunStateChanged {
            run_id: run_terminal,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 13,
        },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::RunStateChanged {
            run_id: run_terminal,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 14,
        },
    );
    apply_event(
        &mut reducer,
        8,
        WalEventType::RunStateChanged {
            run_id: run_terminal,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 15,
        },
    );
    apply_event(
        &mut reducer,
        9,
        WalEventType::RunStateChanged {
            run_id: run_terminal,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 16,
        },
    );

    let mut router = build_router_with_control_setting(reducer, true);

    let first_cancel =
        send_post_request(&mut router, "/api/v1/runs/00000000-0000-0000-0000-000000000456/cancel")
            .await;
    assert_eq!(first_cancel.status(), StatusCode::OK);
    assert_eq!(
        response_body_string(first_cancel).await,
        r#"{"run_id":"00000000-0000-0000-0000-000000000456","status":"canceled"}"#
    );

    let second_cancel =
        send_post_request(&mut router, "/api/v1/runs/00000000-0000-0000-0000-000000000456/cancel")
            .await;
    assert_eq!(second_cancel.status(), StatusCode::OK);
    assert_eq!(
        response_body_string(second_cancel).await,
        r#"{"run_id":"00000000-0000-0000-0000-000000000456","status":"already_canceled"}"#
    );

    let terminal_cancel =
        send_post_request(&mut router, "/api/v1/runs/00000000-0000-0000-0000-000000000457/cancel")
            .await;
    assert_eq!(terminal_cancel.status(), StatusCode::OK);
    assert_eq!(
        response_body_string(terminal_cancel).await,
        r#"{"run_id":"00000000-0000-0000-0000-000000000457","status":"already_terminal"}"#
    );
}

#[tokio::test]
async fn control_run_cancel_invalid_nil_and_unknown_errors_are_locked() {
    let reducer = ReplayReducer::new();
    let mut router = build_router_with_control_setting(reducer, true);

    let invalid = send_post_request(&mut router, "/api/v1/runs/not-a-uuid/cancel").await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(invalid).await,
        r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"not-a-uuid"}}"#
    );

    let nil =
        send_post_request(&mut router, "/api/v1/runs/00000000-0000-0000-0000-000000000000/cancel")
            .await;
    assert_eq!(nil.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(nil).await,
        r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"00000000-0000-0000-0000-000000000000"}}"#
    );

    let unknown =
        send_post_request(&mut router, "/api/v1/runs/11111111-1111-1111-1111-111111111111/cancel")
            .await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_body_string(unknown).await,
        r#"{"error":"run_not_found","message":"run not found","details":{"run_id":"11111111-1111-1111-1111-111111111111"}}"#
    );
}

#[tokio::test]
async fn control_task_cancel_first_and_second_call_are_idempotent_with_locked_shape() {
    let mut reducer = ReplayReducer::new();
    let task_id = TaskId::from_str("00000000-0000-0000-0000-000000000123").unwrap();
    let run_ready = RunId::from_str("00000000-0000-0000-0000-000000000001").unwrap();
    let run_completed = RunId::from_str("00000000-0000-0000-0000-000000000002").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated { task_spec: build_task_spec(task_id, 1, None), timestamp: 10 },
    );
    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_ready, task_id, 10, 10),
        },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::RunStateChanged {
            run_id: run_ready,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 11,
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_completed, task_id, 12, 12),
        },
    );
    apply_event(
        &mut reducer,
        5,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 13,
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 14,
        },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 15,
        },
    );
    apply_event(
        &mut reducer,
        8,
        WalEventType::RunStateChanged {
            run_id: run_completed,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 16,
        },
    );

    let mut router = build_router_with_control_setting(reducer, true);
    let path = "/api/v1/tasks/00000000-0000-0000-0000-000000000123/cancel";

    let first = send_post_request(&mut router, path).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        response_body_string(first).await,
        r#"{"task_id":"00000000-0000-0000-0000-000000000123","status":"canceled","runs_canceled":1,"runs_already_terminal":1,"runs_total":2}"#
    );

    let second = send_post_request(&mut router, path).await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        response_body_string(second).await,
        r#"{"task_id":"00000000-0000-0000-0000-000000000123","status":"already_canceled","runs_canceled":0,"runs_already_terminal":2,"runs_total":2}"#
    );
}

#[tokio::test]
async fn tasks_list_is_deterministic_and_validates_pagination() {
    let mut reducer = ReplayReducer::new();

    let task_high =
        build_task_spec(TaskId::from_str("00000000-0000-0000-0000-000000000004").unwrap(), 6, None);
    let task_mid_early =
        build_task_spec(TaskId::from_str("00000000-0000-0000-0000-000000000005").unwrap(), 5, None);
    let task_mid_a =
        build_task_spec(TaskId::from_str("00000000-0000-0000-0000-000000000001").unwrap(), 5, None);
    let task_mid_b =
        build_task_spec(TaskId::from_str("00000000-0000-0000-0000-000000000002").unwrap(), 5, None);
    let task_low =
        build_task_spec(TaskId::from_str("00000000-0000-0000-0000-000000000003").unwrap(), 4, None);

    apply_event(&mut reducer, 1, WalEventType::TaskCreated { task_spec: task_high, timestamp: 20 });
    apply_event(
        &mut reducer,
        2,
        WalEventType::TaskCreated { task_spec: task_mid_early, timestamp: 9 },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::TaskCreated { task_spec: task_mid_a, timestamp: 10 },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::TaskCreated { task_spec: task_mid_b, timestamp: 10 },
    );
    apply_event(&mut reducer, 5, WalEventType::TaskCreated { task_spec: task_low, timestamp: 5 });

    let mut router = build_router_with_projection(reducer);

    let response = send_request(&mut router, "/api/v1/tasks").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("tasks list json");
    let ids: Vec<String> = value["tasks"]
        .as_array()
        .expect("tasks array")
        .iter()
        .map(|entry| entry["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec![
            "00000000-0000-0000-0000-000000000004",
            "00000000-0000-0000-0000-000000000005",
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ]
    );

    let response = send_request(&mut router, "/api/v1/tasks?limit=2&offset=1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("tasks list json");
    let ids: Vec<String> = value["tasks"]
        .as_array()
        .expect("tasks array")
        .iter()
        .map(|entry| entry["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["00000000-0000-0000-0000-000000000005", "00000000-0000-0000-0000-000000000001",]
    );

    let response = send_request(&mut router, "/api/v1/tasks?limit=0").await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_pagination","message":"limit must be between 1 and 1000","details":{"field":"limit"}}"#
    );
}

#[tokio::test]
async fn task_get_returns_full_spec_and_errors() {
    let mut reducer = ReplayReducer::new();
    let task_id = TaskId::from_str("00000000-0000-0000-0000-000000000010").unwrap();
    let task_spec = build_task_spec(task_id, 1, None);
    apply_event(&mut reducer, 1, WalEventType::TaskCreated { task_spec, timestamp: 100 });
    let mut router = build_router_with_projection(reducer);

    let response =
        send_request(&mut router, "/api/v1/tasks/00000000-0000-0000-0000-000000000010").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("task get json");
    assert_eq!(value["id"], "00000000-0000-0000-0000-000000000010");
    assert_eq!(value["created_at"], 100);

    let response = send_request(&mut router, "/api/v1/tasks/not-a-uuid").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_task_id","message":"invalid task id","details":{"task_id":"not-a-uuid"}}"#
    );

    let response =
        send_request(&mut router, "/api/v1/tasks/00000000-0000-0000-0000-000000000000").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_task_id","message":"invalid task id","details":{"task_id":"00000000-0000-0000-0000-000000000000"}}"#
    );

    let response =
        send_request(&mut router, "/api/v1/tasks/00000000-0000-0000-0000-000000000999").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"task_not_found","message":"task not found","details":{"task_id":"00000000-0000-0000-0000-000000000999"}}"#
    );
}

#[tokio::test]
async fn runs_list_is_deterministic_and_validates_pagination() {
    let mut reducer = ReplayReducer::new();
    let task_id = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
    let task_spec = build_task_spec(task_id, 1, Some("key"));
    apply_event(&mut reducer, 1, WalEventType::TaskCreated { task_spec, timestamp: 100 });

    let run_id_1 = RunId::from_str("11111111-1111-1111-1111-111111111110").unwrap();
    let run_id_2 = RunId::from_str("22222222-2222-2222-2222-222222222220").unwrap();
    let run_id_3 = RunId::from_str("33333333-3333-3333-3333-333333333330").unwrap();

    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_id_1, task_id, 1000, 1500),
        },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_id_2, task_id, 1000, 1501),
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_id_3, task_id, 2000, 1502),
        },
    );

    let mut router = build_router_with_projection(reducer);

    let response = send_request(&mut router, "/api/v1/runs").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("runs list json");
    let ids: Vec<String> = value["runs"]
        .as_array()
        .expect("runs array")
        .iter()
        .map(|entry| entry["run_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        ids,
        vec![
            "11111111-1111-1111-1111-111111111110",
            "22222222-2222-2222-2222-222222222220",
            "33333333-3333-3333-3333-333333333330",
        ]
    );

    let response = send_request(&mut router, "/api/v1/runs?limit=2&offset=1").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("runs list json");
    assert_eq!(value["runs"].as_array().unwrap().len(), 2);

    let response = send_request(&mut router, "/api/v1/runs?offset=-1").await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_pagination","message":"offset must be a non-negative integer","details":{"field":"offset"}}"#
    );
}

#[tokio::test]
async fn run_get_returns_history_attempts_lease_and_errors() {
    let mut reducer = ReplayReducer::new();
    let task_id = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
    let run_id = RunId::from_str("22222222-2222-2222-2222-222222222222").unwrap();
    let attempt_id = AttemptId::from_str("33333333-3333-3333-3333-333333333333").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated { task_spec: build_task_spec(task_id, 1, None), timestamp: 900 },
    );
    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_id, task_id, 1000, 1000),
        },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 1001,
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::LeaseAcquired {
            run_id,
            owner: "worker-1".to_string(),
            expiry: 2000,
            timestamp: 1100,
        },
    );
    apply_event(
        &mut reducer,
        5,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 1101,
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 1200,
        },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 1201 },
    );

    let mut router = build_router_with_projection(reducer);

    let response =
        send_request(&mut router, "/api/v1/runs/22222222-2222-2222-2222-222222222222").await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("run get json");
    assert_eq!(value["run_id"], "22222222-2222-2222-2222-222222222222");
    assert_eq!(value["block_reason"], "running");
    assert_eq!(value["state_history"][0]["from"], serde_json::Value::Null);
    assert_eq!(value["state_history"][0]["to"], "Scheduled");

    let response = send_request(&mut router, "/api/v1/runs/not-a-uuid").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"not-a-uuid"}}"#
    );

    let response =
        send_request(&mut router, "/api/v1/runs/00000000-0000-0000-0000-000000000000").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"00000000-0000-0000-0000-000000000000"}}"#
    );

    let response =
        send_request(&mut router, "/api/v1/runs/11111111-1111-1111-1111-111111111111").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        response_body_string(response).await,
        r#"{"error":"run_not_found","message":"run not found","details":{"run_id":"11111111-1111-1111-1111-111111111111"}}"#
    );
}

#[tokio::test]
async fn task_get_returns_parent_task_id_when_present() {
    let mut reducer = ReplayReducer::new();
    let parent_id = TaskId::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let child_id = TaskId::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();

    let parent_spec = build_task_spec(parent_id, 1, None);
    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated { task_spec: parent_spec, timestamp: 100 },
    );
    let child_spec = TaskSpec::new(
        child_id,
        TaskPayload::with_content_type(b"child".to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("child task spec should be valid")
    .with_parent(parent_id);
    apply_event(
        &mut reducer,
        2,
        WalEventType::TaskCreated { task_spec: child_spec, timestamp: 101 },
    );

    let mut router = build_router_with_projection(reducer);

    let response = send_request(&mut router, &format!("/api/v1/tasks/{child_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("task get json");
    assert_eq!(value["parent_task_id"], parent_id.to_string());

    // Parent task should not have parent_task_id
    let response = send_request(&mut router, &format!("/api/v1/tasks/{parent_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("task get json");
    assert!(value.get("parent_task_id").is_none() || value["parent_task_id"].is_null());
}

#[tokio::test]
async fn run_get_returns_attempt_output_when_present() {
    let mut reducer = ReplayReducer::new();
    let task_id = TaskId::from_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let run_id = RunId::from_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let attempt_id = AttemptId::from_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated { task_spec: build_task_spec(task_id, 1, None), timestamp: 900 },
    );
    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(run_id, task_id, 1000, 1000),
        },
    );
    apply_event(
        &mut reducer,
        3,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 1001,
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 1002,
        },
    );
    apply_event(
        &mut reducer,
        5,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 1003,
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 1004 },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::AttemptFinished {
            run_id,
            attempt_id,
            result: AttemptResultKind::Success,
            error: None,
            output: Some(b"test-output".to_vec()),
            timestamp: 1005,
        },
    );
    apply_event(
        &mut reducer,
        8,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 1006,
        },
    );

    let mut router = build_router_with_projection(reducer);

    let response = send_request(&mut router, &format!("/api/v1/runs/{run_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_body_string(response).await;
    let value: serde_json::Value = serde_json::from_str(&body).expect("run get json");
    assert_eq!(value["attempts"][0]["output"], serde_json::json!(b"test-output"));
}
