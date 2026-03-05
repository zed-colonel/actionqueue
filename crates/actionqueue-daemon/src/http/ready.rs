//! Ready route module.
//!
//! This module provides the readiness endpoint (`GET /ready`) for the daemon.
//! The ready endpoint is side-effect free and provides a deterministic response
//! indicating daemon readiness. It reflects whether the daemon has completed
//! bootstrap and is fully operational.
//!
//! # Invariant boundaries
//!
//! The ready handler performs no IO, reads no storage, and mutates no runtime state.
//! It reflects the bootstrap state and is constant-time.
//!
//! # Response schema
//!
//! When ready: `{"status": "ready"}`
//! When not ready: `{"status": "<reason>"}`
//!
//! The status is "ready" when the daemon has completed bootstrap. Otherwise,
//! it includes a reason string indicating why the daemon is not yet ready.
//!
//! # Readiness vocabulary (WP-2)
//!
//! The only allowed not-ready reasons in WP-2 are:
//! - `ReadyStatus::REASON_CONFIG_INVALID`: Configuration was invalid during bootstrap.
//! - `ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE`: Bootstrap process was incomplete.
//!
//! These reasons are defined as static constants on [`ReadyStatus`](crate::bootstrap::ReadyStatus).

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Ready check response payload.
///
/// This struct represents the stable schema for the ready endpoint response.
/// Fields should not be modified without careful consideration of external
/// dependencies that may rely on this contract.
///
/// # Readiness vocabulary (WP-2)
///
/// When not ready, the status should be one of the documented reasons from
/// [`ReadyStatus`](crate::bootstrap::ReadyStatus):
/// - `ReadyStatus::REASON_CONFIG_INVALID`
/// - `ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE`
#[derive(Debug, Clone, Serialize)]
pub struct ReadyResponse {
    /// Daemon readiness status.
    ///
    /// When the daemon is fully ready, this is "ready".
    /// When not ready, this contains a reason string.
    pub status: &'static str,
}

impl ReadyResponse {
    /// Creates a new ready response indicating the daemon is ready.
    pub const fn ready() -> Self {
        Self { status: "ready" }
    }

    /// Creates a new ready response indicating the daemon is not ready with a reason.
    pub const fn not_ready(reason: &'static str) -> Self {
        Self { status: reason }
    }
}

/// Ready check handler.
///
/// This handler responds to `GET /ready` requests with a deterministic,
/// side-effect-free payload indicating daemon readiness.
///
/// # Invariant boundaries
///
/// This handler performs no IO, reads no storage, and mutates no runtime state.
/// It reflects the bootstrap state contained in RouterState.
#[tracing::instrument(skip_all)]
pub async fn handle(state: State<super::RouterState>) -> impl IntoResponse {
    if state.ready_status.is_ready() {
        (StatusCode::OK, Json(ReadyResponse::ready()))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse::not_ready(state.ready_status.reason())),
        )
    }
}

/// Registers the ready route in the router builder.
///
/// This function adds the `/ready` endpoint to the router configuration.
/// The route is always available when HTTP is enabled and does not depend
/// on any feature flags.
///
/// # Arguments
///
/// * `router` - The axum router to register routes with
pub fn register_routes(
    router: axum::Router<super::RouterState>,
) -> axum::Router<super::RouterState> {
    router.route("/ready", axum::routing::get(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ready_response_ready() {
        let response = ReadyResponse::ready();
        assert_eq!(response.status, "ready");
    }

    #[test]
    fn test_ready_response_not_ready() {
        let response =
            ReadyResponse::not_ready(crate::bootstrap::ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE);
        assert_eq!(response.status, crate::bootstrap::ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE);
    }

    #[test]
    fn test_ready_response_serialization_ready() {
        let response = ReadyResponse::ready();
        let json = serde_json::to_string(&response).expect("serialization should succeed");
        assert_eq!(json, r#"{"status":"ready"}"#);
    }

    #[test]
    fn test_ready_response_serialization_not_ready() {
        let response =
            ReadyResponse::not_ready(crate::bootstrap::ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE);
        let json = serde_json::to_string(&response).expect("serialization should succeed");
        assert_eq!(json, r#"{"status":"bootstrap_incomplete"}"#);
    }

    #[test]
    fn test_ready_response_from_ready_status_ready() {
        let ready_status = crate::bootstrap::ReadyStatus::ready();
        let response = if ready_status.is_ready() {
            ReadyResponse::ready()
        } else {
            ReadyResponse::not_ready(ready_status.reason())
        };
        assert_eq!(response.status, "ready");
    }

    #[test]
    fn test_ready_response_from_ready_status_not_ready() {
        let ready_status = crate::bootstrap::ReadyStatus::not_ready(
            crate::bootstrap::ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE,
        );
        let response = if ready_status.is_ready() {
            ReadyResponse::ready()
        } else {
            ReadyResponse::not_ready(ready_status.reason())
        };
        assert_eq!(response.status, crate::bootstrap::ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE);
    }
}
