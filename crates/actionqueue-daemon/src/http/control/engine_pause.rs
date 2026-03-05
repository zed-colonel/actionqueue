//! Engine pause control route handler.
//!
//! This module implements `POST /api/v1/engine/pause` through the mutation
//! authority lane. The handler is deterministic, idempotent, and never mutates
//! projection state directly.

use actionqueue_core::mutation::{
    DurabilityPolicy, EnginePauseCommand, MutationAuthority, MutationCommand,
};
use actionqueue_storage::mutation::authority::{MutationAuthorityError, MutationValidationError};
use actionqueue_storage::recovery::reducer::ReplayReducerError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Success payload for engine pause control endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnginePauseResponse {
    /// Pause status vocabulary (`paused` or `already_paused`).
    pub status: &'static str,
}

/// Handles engine pause control requests.
#[tracing::instrument(skip_all)]
pub async fn handle(state: State<crate::http::RouterState>) -> impl IntoResponse {
    let authority_handle = match &state.control_authority {
        Some(authority) => authority.clone(),
        None => {
            return super::internal_error_response("control authority unavailable");
        }
    };

    let mut authority = match authority_handle.lock() {
        Ok(guard) => guard,
        Err(_) => {
            tracing::error!(
                "control authority mutex poisoned — control plane is permanently degraded"
            );
            return super::internal_error_response("control authority lock poisoned");
        }
    };

    if authority.projection().is_engine_paused() {
        return (StatusCode::OK, Json(EnginePauseResponse { status: "already_paused" }))
            .into_response();
    }

    let sequence = match authority.projection().latest_sequence().checked_add(1) {
        Some(sequence) => sequence,
        None => return super::sequence_overflow_response(),
    };
    let command = MutationCommand::EnginePause(EnginePauseCommand::new(sequence, sequence));

    match authority.submit_command(command, DurabilityPolicy::Immediate) {
        Ok(_) => {
            match crate::http::write_projection(&state) {
                Ok(mut guard) => *guard = authority.projection().clone(),
                Err(response) => return *response,
            };
            (StatusCode::OK, Json(EnginePauseResponse { status: "paused" })).into_response()
        }
        Err(MutationAuthorityError::Validation(MutationValidationError::EngineAlreadyPaused)) => {
            (StatusCode::OK, Json(EnginePauseResponse { status: "already_paused" })).into_response()
        }
        Err(err @ MutationAuthorityError::PartialDurability { .. }) => {
            internal_authority_error(err)
        }
        Err(error) => internal_authority_error(error),
    }
}

fn internal_authority_error(
    error: MutationAuthorityError<ReplayReducerError>,
) -> axum::response::Response {
    super::internal_authority_error(error)
}

#[cfg(test)]
mod tests {
    use super::EnginePauseResponse;

    #[test]
    fn success_schema_serializes_with_locked_fields() {
        let payload = EnginePauseResponse { status: "paused" };
        let json = serde_json::to_string(&payload).expect("serialization should succeed");
        assert_eq!(json, r#"{"status":"paused"}"#);
    }

    #[test]
    fn status_vocabulary_is_locked() {
        let paused = EnginePauseResponse { status: "paused" };
        let already_paused = EnginePauseResponse { status: "already_paused" };

        assert_eq!(paused.status, "paused");
        assert_eq!(already_paused.status, "already_paused");
    }
}
