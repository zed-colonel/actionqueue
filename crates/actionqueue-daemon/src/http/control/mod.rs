//! Feature-gated control route registration.
//!
//! This module centralizes control-surface route registration behind the
//! daemon `enable_control` feature flag.
//!
//! # Control behavior contract
//!
//! - When control is disabled, no control routes are registered.
//! - When control is enabled, task cancel is implemented in P6-011:
//!   - `POST /api/v1/tasks/:task_id/cancel`
//! - When control is enabled, run cancel is implemented in P6-012:
//!   - `POST /api/v1/runs/:run_id/cancel`
//! - Engine pause is implemented in P6-013:
//!   - `POST /api/v1/engine/pause`
//! - Engine resume is implemented in P6-014:
//!   - `POST /api/v1/engine/resume`
//!
//! This routing boundary is intentionally centralized so control feature gating
//! remains deterministic and testable.

mod engine_pause;
mod engine_resume;
mod run_cancel;
mod task_cancel;

/// Shared typed error response for control handlers.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct ErrorResponse {
    pub error: &'static str,
    pub message: String,
}

/// Returns a 500 Internal Server Error response with a typed error body.
pub(crate) fn internal_error_response(message: &str) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::Json;

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: "internal_error", message: message.to_string() }),
    )
        .into_response()
}

/// Returns a 500 Internal Server Error for sequence overflow.
pub(crate) fn sequence_overflow_response() -> axum::response::Response {
    internal_error_response("control sequence overflow")
}

/// Returns a 500 Internal Server Error for a mutation authority failure.
///
/// The error variant is classified into a human-readable category for the
/// response message, avoiding leaking internal error details to callers.
pub(crate) fn internal_authority_error(
    error: actionqueue_storage::mutation::authority::MutationAuthorityError<
        actionqueue_storage::recovery::reducer::ReplayReducerError,
    >,
) -> axum::response::Response {
    let message = match error {
        actionqueue_storage::mutation::authority::MutationAuthorityError::Validation(_) => {
            "authority validation failed"
        }
        actionqueue_storage::mutation::authority::MutationAuthorityError::Append(_) => {
            "authority append failed"
        }
        actionqueue_storage::mutation::authority::MutationAuthorityError::PartialDurability {
            ..
        } => "authority partial durability failed",
        actionqueue_storage::mutation::authority::MutationAuthorityError::Apply { .. } => {
            "authority apply failed"
        }
    };
    internal_error_response(message)
}

/// Registers control routes according to the `control_enabled` feature flag.
///
/// When disabled, the router is returned unchanged and control paths remain
/// unreachable (HTTP 404 by route absence). When enabled, all four control
/// paths are registered as `POST` routes with fully implemented handlers.
pub fn register_routes(
    router: axum::Router<super::RouterState>,
    control_enabled: bool,
) -> axum::Router<super::RouterState> {
    if !control_enabled {
        return router;
    }

    router
        .route("/api/v1/tasks/:task_id/cancel", axum::routing::post(task_cancel::handle))
        .route("/api/v1/runs/:run_id/cancel", axum::routing::post(run_cancel::handle))
        .route("/api/v1/engine/pause", axum::routing::post(engine_pause::handle))
        .route("/api/v1/engine/resume", axum::routing::post(engine_resume::handle))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_storage::mutation::authority::StorageMutationAuthority;
    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::fs_writer::WalFsWriter;
    use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::Service;

    use super::*;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    const CONTROL_PATHS: [&str; 4] = [
        "/api/v1/tasks/00000000-0000-0000-0000-000000000123/cancel",
        "/api/v1/runs/00000000-0000-0000-0000-000000000456/cancel",
        "/api/v1/engine/pause",
        "/api/v1/engine/resume",
    ];

    fn test_control_authority() -> super::super::ControlMutationAuthority {
        let unique = format!(
            "actionqueue-daemon-control-test-{}-{}.wal",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after epoch")
                .as_nanos()
        );
        let wal_path = std::env::temp_dir().join(unique);
        let wal_writer = WalFsWriter::new(wal_path).expect("test wal writer should initialize");
        let wal_writer = InstrumentedWalWriter::new(wal_writer, WalAppendTelemetry::new());
        let authority = StorageMutationAuthority::new(wal_writer, ReplayReducer::new());
        std::sync::Arc::new(std::sync::Mutex::new(authority))
    }

    fn test_metrics_registry(enabled: bool) -> Arc<crate::metrics::registry::MetricsRegistry> {
        let metrics_bind =
            if enabled { Some(std::net::SocketAddr::from(([127, 0, 0, 1], 9090))) } else { None };
        Arc::new(
            crate::metrics::registry::MetricsRegistry::new(metrics_bind)
                .expect("test metrics registry should initialize"),
        )
    }

    fn test_clock() -> SharedDaemonClock {
        Arc::new(MockClock::new(1_700_000_000))
    }

    fn test_router(control_enabled: bool) -> axum::Router<()> {
        let state = if control_enabled {
            std::sync::Arc::new(super::super::RouterStateInner::with_control_authority(
                crate::bootstrap::RouterConfig { control_enabled, metrics_enabled: false },
                Arc::new(std::sync::RwLock::new(ReplayReducer::new())),
                crate::http::RouterObservability {
                    metrics: test_metrics_registry(false),
                    wal_append_telemetry: WalAppendTelemetry::new(),
                    clock: test_clock(),
                    recovery_observations: RecoveryObservations::zero(),
                },
                test_control_authority(),
                crate::bootstrap::ReadyStatus::ready(),
            ))
        } else {
            std::sync::Arc::new(super::super::RouterStateInner::new(
                crate::bootstrap::RouterConfig { control_enabled, metrics_enabled: false },
                Arc::new(std::sync::RwLock::new(ReplayReducer::new())),
                crate::http::RouterObservability {
                    metrics: test_metrics_registry(false),
                    wal_append_telemetry: WalAppendTelemetry::new(),
                    clock: test_clock(),
                    recovery_observations: RecoveryObservations::zero(),
                },
                crate::bootstrap::ReadyStatus::ready(),
            ))
        };

        register_routes(axum::Router::new(), control_enabled).with_state(state).with_state(())
    }

    async fn send_post(router: &mut axum::Router<()>, path: &str) -> axum::response::Response {
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
    async fn disabled_gating_does_not_register_control_routes() {
        let mut router = test_router(false);

        for path in CONTROL_PATHS {
            let response = send_post(&mut router, path).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn enabled_gating_routes_engine_resume_to_real_handler() {
        let mut router = test_router(true);
        let response = send_post(&mut router, CONTROL_PATHS[3]).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body_string(response).await, r#"{"status":"already_resumed"}"#);
    }

    #[tokio::test]
    async fn enabled_gating_routes_engine_pause_to_real_handler() {
        let mut router = test_router(true);
        let response = send_post(&mut router, CONTROL_PATHS[2]).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response_body_string(response).await, r#"{"status":"paused"}"#);
    }

    #[tokio::test]
    async fn enabled_gating_routes_task_cancel_to_real_handler() {
        let mut router = test_router(true);
        let response = send_post(&mut router, CONTROL_PATHS[0]).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"task_not_found","message":"task not found","details":{"task_id":"00000000-0000-0000-0000-000000000123"}}"#
        );
    }

    #[tokio::test]
    async fn enabled_gating_routes_run_cancel_to_real_handler() {
        let mut router = test_router(true);
        let response = send_post(&mut router, CONTROL_PATHS[1]).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"run_not_found","message":"run not found","details":{"run_id":"00000000-0000-0000-0000-000000000456"}}"#
        );
    }
}
