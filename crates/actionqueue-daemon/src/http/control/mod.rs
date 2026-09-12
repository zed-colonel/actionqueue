//! Feature-gated control route registration.
//!
//! This module centralizes control-surface route registration behind the
//! daemon `enable_control` feature flag.
//!
//! # Control behavior contract
//!
//! - When control is disabled, no control routes are registered.
//! - When control is enabled, task cancel is implemented in P6-011:
//!   - `POST /api/v2/tasks/:task_id/cancel`
//! - When control is enabled, run cancel is implemented in P6-012:
//!   - `POST /api/v2/runs/:run_id/cancel`
//! - Engine pause is implemented in P6-013:
//!   - `POST /api/v2/engine/pause`
//! - Engine resume is implemented in P6-014:
//!   - `POST /api/v2/engine/resume`
//!
//! This routing boundary is intentionally centralized so control feature gating
//! remains deterministic and testable.

mod engine_pause;
mod engine_resume;

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
        actionqueue_storage::mutation::MutationAuthorityError::Control(_) => {
            return axum::response::IntoResponse::into_response(axum::http::StatusCode::FORBIDDEN)
        }
        actionqueue_storage::mutation::MutationAuthorityError::Disposition(_) => {
            "disposition rejected"
        }
        actionqueue_storage::mutation::MutationAuthorityError::Wait(_) => {
            "continuation control rejected"
        }
        actionqueue_storage::mutation::MutationAuthorityError::Publication { .. } => {
            "authority publication failed"
        }
        actionqueue_storage::mutation::MutationAuthorityError::Admission(_) => "admission rejected",
        actionqueue_storage::mutation::MutationAuthorityError::Signal(_) => "signal rejected",
        actionqueue_storage::mutation::MutationAuthorityError::RecoveryRequired => {
            "mutation authority requires recovery"
        }
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
        .route("/api/v2/engine/pause", axum::routing::post(engine_pause::handle))
        .route("/api/v2/engine/resume", axum::routing::post(engine_resume::handle))
}
