//! Engine resume control route handler.
//!
//! This module implements `POST /api/v1/engine/resume` through the mutation
//! authority lane. The handler is deterministic, idempotent, and never mutates
//! projection state directly.

use actionqueue_core::mutation::{
    DurabilityPolicy, EngineResumeCommand, MutationAuthority, MutationCommand,
};
use actionqueue_storage::mutation::authority::{MutationAuthorityError, MutationValidationError};
use actionqueue_storage::recovery::reducer::ReplayReducerError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Success payload for engine resume control endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EngineResumeResponse {
    /// Resume status vocabulary (`resumed` or `already_resumed`).
    pub status: &'static str,
}

/// Handles engine resume control requests.
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

    if !authority.projection().is_engine_paused() {
        return (StatusCode::OK, Json(EngineResumeResponse { status: "already_resumed" }))
            .into_response();
    }

    let sequence = match authority.projection().latest_sequence().checked_add(1) {
        Some(sequence) => sequence,
        None => return super::sequence_overflow_response(),
    };
    let command = MutationCommand::EngineResume(EngineResumeCommand::new(sequence, sequence));

    match authority.submit_command(command, DurabilityPolicy::Immediate) {
        Ok(_) => {
            match crate::http::write_projection(&state) {
                Ok(mut guard) => *guard = authority.projection().clone(),
                Err(response) => return *response,
            };
            (StatusCode::OK, Json(EngineResumeResponse { status: "resumed" })).into_response()
        }
        Err(error) => match classify_submit_error(error) {
            ResumeSubmitDisposition::AlreadyResumed => {
                (StatusCode::OK, Json(EngineResumeResponse { status: "already_resumed" }))
                    .into_response()
            }
            ResumeSubmitDisposition::Internal(response) => response,
        },
    }
}

enum ResumeSubmitDisposition {
    AlreadyResumed,
    Internal(axum::response::Response),
}

fn classify_submit_error(
    error: MutationAuthorityError<ReplayReducerError>,
) -> ResumeSubmitDisposition {
    match error {
        MutationAuthorityError::Validation(MutationValidationError::EngineNotPaused) => {
            ResumeSubmitDisposition::AlreadyResumed
        }
        other => ResumeSubmitDisposition::Internal(internal_authority_error(other)),
    }
}

fn internal_authority_error(
    error: MutationAuthorityError<ReplayReducerError>,
) -> axum::response::Response {
    super::internal_authority_error(error)
}

#[cfg(test)]
mod tests {
    use actionqueue_storage::mutation::authority::{
        MutationAuthorityError, MutationValidationError,
    };

    use super::{classify_submit_error, EngineResumeResponse, ResumeSubmitDisposition};

    #[test]
    fn success_schema_serializes_with_locked_fields() {
        let payload = EngineResumeResponse { status: "resumed" };
        let json = serde_json::to_string(&payload).expect("serialization should succeed");
        assert_eq!(json, r#"{"status":"resumed"}"#);
    }

    #[test]
    fn status_vocabulary_is_locked() {
        let resumed = EngineResumeResponse { status: "resumed" };
        let already_resumed = EngineResumeResponse { status: "already_resumed" };

        assert_eq!(resumed.status, "resumed");
        assert_eq!(already_resumed.status, "already_resumed");
    }

    #[test]
    fn typed_engine_not_paused_validation_maps_to_already_resumed() {
        let result = classify_submit_error(MutationAuthorityError::Validation(
            MutationValidationError::EngineNotPaused,
        ));
        assert!(matches!(result, ResumeSubmitDisposition::AlreadyResumed));
    }
}
