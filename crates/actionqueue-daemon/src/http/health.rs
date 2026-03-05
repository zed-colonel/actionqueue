//! Health check route module.
//!
//! This module provides the liveness endpoint (`GET /healthz`) for the daemon.
//! The health endpoint is side-effect free and provides a deterministic response
//! indicating daemon liveness only. It does not imply readiness or storage availability.
//!
//! # Invariant boundaries
//!
//! The health handler performs no IO, reads no storage, and mutates no runtime state.
//! It is purely a static response intended for watchdogs and orchestration systems
//! that require reliable liveness signals.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Health check response payload.
///
/// This struct represents the stable schema for the health endpoint response.
/// Fields should not be modified without careful consideration of external
/// dependencies that may rely on this contract.
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    /// Daemon liveness status.
    ///
    /// This field indicates that the daemon process is running and responding
    /// to HTTP requests. It does not indicate that all services are ready
    /// or that storage is available.
    pub status: &'static str,
}

impl HealthResponse {
    /// Creates a new health response indicating liveness.
    pub const fn ok() -> Self {
        Self { status: "ok" }
    }
}

/// Health check handler.
///
/// This handler responds to `GET /healthz` requests with a deterministic,
/// side-effect-free payload indicating daemon liveness.
///
/// # Invariant boundaries
///
/// This handler performs no IO, reads no storage, and mutates no runtime state.
#[tracing::instrument(skip_all)]
pub async fn handle(_state: State<super::RouterState>) -> impl IntoResponse {
    Json(HealthResponse::ok())
}

/// Registers the health route in the router builder.
///
/// This function adds the `/healthz` endpoint to the router configuration.
/// The route is always available when HTTP is enabled and does not depend
/// on any feature flags.
///
/// # Arguments
///
/// * `router` - The axum router to register routes with
pub fn register_routes(
    router: axum::Router<super::RouterState>,
) -> axum::Router<super::RouterState> {
    router.route("/healthz", axum::routing::get(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_response_ok() {
        let response = HealthResponse::ok();
        assert_eq!(response.status, "ok");
    }

    #[test]
    fn test_health_response_serialization() {
        let response = HealthResponse::ok();
        let json = serde_json::to_string(&response).expect("serialization should succeed");
        assert_eq!(json, r#"{"status":"ok"}"#);
    }
}
