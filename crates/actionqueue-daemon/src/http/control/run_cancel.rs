//! Run cancel control route handler.
//!
//! This module implements `POST /api/v1/runs/:run_id/cancel` through the
//! mutation-authority run-state-transition lane only. The handler is
//! deterministic, idempotent, and does not directly mutate projection state or
//! append WAL records.

use std::str::FromStr;

use actionqueue_core::ids::RunId;
use actionqueue_core::mutation::{
    DurabilityPolicy, MutationAuthority, MutationCommand, RunStateTransitionCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_storage::mutation::authority::{MutationAuthorityError, MutationValidationError};
use actionqueue_storage::recovery::reducer::ReplayReducerError;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Success payload for run cancellation control endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunCancelResponse {
    /// Run identifier.
    pub run_id: String,
    /// Cancellation status vocabulary.
    ///
    /// Allowed values are:
    /// - `canceled`
    /// - `already_canceled`
    /// - `already_terminal`
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidRunIdResponse {
    error: &'static str,
    message: &'static str,
    details: RunIdDetails,
}

#[derive(Debug, Clone, Serialize)]
struct RunNotFoundResponse {
    error: &'static str,
    message: &'static str,
    details: RunIdDetails,
}

#[derive(Debug, Clone, Serialize)]
struct InternalErrorResponse {
    error: &'static str,
    message: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct RunIdDetails {
    run_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelDisposition {
    AlreadyCanceled,
    AlreadyTerminal,
    CancelEligible { previous_state: RunState },
    InvariantViolation,
}

/// Handles run cancellation requests.
#[tracing::instrument(skip(state))]
pub async fn handle(
    state: State<crate::http::RouterState>,
    Path(run_id_str): Path<String>,
) -> impl IntoResponse {
    let authority_handle = match &state.control_authority {
        Some(authority) => authority.clone(),
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(InternalErrorResponse {
                    error: "control_unavailable",
                    message: "control authority unavailable",
                }),
            )
                .into_response();
        }
    };

    let mut authority = match authority_handle.lock() {
        Ok(guard) => guard,
        Err(_) => {
            tracing::error!(
                "control authority mutex poisoned — control plane is permanently degraded"
            );
            return internal_error_response("control authority lock poisoned");
        }
    };

    let run_id = match parse_run_id(&run_id_str) {
        Some(run_id) => run_id,
        None => return invalid_run_id_response(&run_id_str),
    };

    let Some(current_state) = authority.projection().get_run_state(&run_id).copied() else {
        return run_not_found_response(&run_id_str);
    };

    match classify_cancel_disposition(current_state) {
        CancelDisposition::AlreadyCanceled => {
            return success_response(run_id, "already_canceled");
        }
        CancelDisposition::AlreadyTerminal => {
            return success_response(run_id, "already_terminal");
        }
        CancelDisposition::InvariantViolation => {
            return internal_error_response("run cancel transition invariant violation");
        }
        CancelDisposition::CancelEligible { previous_state } => {
            let sequence = match authority.projection().latest_sequence().checked_add(1) {
                Some(sequence) => sequence,
                None => return internal_error_response("control sequence overflow"),
            };
            let command = MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                sequence,
                run_id,
                previous_state,
                RunState::Canceled,
                sequence,
            ));

            match authority.submit_command(command, DurabilityPolicy::Immediate) {
                Ok(_) => {
                    match crate::http::write_projection(&state) {
                        Ok(mut guard) => *guard = authority.projection().clone(),
                        Err(response) => return *response,
                    };
                    return success_response(run_id, "canceled");
                }
                Err(MutationAuthorityError::Validation(
                    MutationValidationError::PreviousStateMismatch { .. },
                )) => {}
                Err(error) => return internal_authority_error(error),
            }
        }
    }

    let Some(refreshed_state) = authority.projection().get_run_state(&run_id).copied() else {
        return internal_error_response("run state missing during race resolution");
    };

    match classify_cancel_disposition(refreshed_state) {
        CancelDisposition::AlreadyCanceled => success_response(run_id, "already_canceled"),
        CancelDisposition::AlreadyTerminal => success_response(run_id, "already_terminal"),
        CancelDisposition::InvariantViolation => {
            internal_error_response("run cancel transition invariant violation")
        }
        CancelDisposition::CancelEligible { previous_state } => {
            let retry_sequence = match authority.projection().latest_sequence().checked_add(1) {
                Some(sequence) => sequence,
                None => return internal_error_response("control sequence overflow"),
            };
            let retry_command =
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    retry_sequence,
                    run_id,
                    previous_state,
                    RunState::Canceled,
                    retry_sequence,
                ));

            match authority.submit_command(retry_command, DurabilityPolicy::Immediate) {
                Ok(_) => {
                    match crate::http::write_projection(&state) {
                        Ok(mut guard) => *guard = authority.projection().clone(),
                        Err(response) => return *response,
                    };
                    success_response(run_id, "canceled")
                }
                Err(error) => internal_authority_error(error),
            }
        }
    }
}

fn parse_run_id(run_id_str: &str) -> Option<RunId> {
    let run_id = RunId::from_str(run_id_str).ok()?;
    if run_id.as_uuid().is_nil() {
        return None;
    }
    Some(run_id)
}

fn classify_cancel_disposition(state: RunState) -> CancelDisposition {
    if state == RunState::Canceled {
        return CancelDisposition::AlreadyCanceled;
    }

    if state.is_terminal() {
        return CancelDisposition::AlreadyTerminal;
    }

    if is_valid_transition(state, RunState::Canceled) {
        CancelDisposition::CancelEligible { previous_state: state }
    } else {
        CancelDisposition::InvariantViolation
    }
}

fn success_response(run_id: RunId, status: &'static str) -> axum::response::Response {
    (StatusCode::OK, Json(RunCancelResponse { run_id: run_id.to_string(), status })).into_response()
}

fn invalid_run_id_response(run_id: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(InvalidRunIdResponse {
            error: "invalid_run_id",
            message: "invalid run id",
            details: RunIdDetails { run_id: run_id.to_string() },
        }),
    )
        .into_response()
}

fn run_not_found_response(run_id: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(RunNotFoundResponse {
            error: "run_not_found",
            message: "run not found",
            details: RunIdDetails { run_id: run_id.to_string() },
        }),
    )
        .into_response()
}

fn internal_error_response(message: &'static str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(InternalErrorResponse { error: "internal_error", message }),
    )
        .into_response()
}

fn internal_authority_error(
    error: MutationAuthorityError<ReplayReducerError>,
) -> axum::response::Response {
    super::internal_authority_error(error)
}

#[cfg(test)]
mod tests {
    use super::RunCancelResponse;

    #[test]
    fn success_schema_serializes_with_locked_fields() {
        let payload = RunCancelResponse {
            run_id: "00000000-0000-0000-0000-000000000001".to_string(),
            status: "canceled",
        };

        let json = serde_json::to_string(&payload).expect("serialization should succeed");
        assert_eq!(
            json,
            r#"{"run_id":"00000000-0000-0000-0000-000000000001","status":"canceled"}"#
        );
    }

    #[test]
    fn status_vocabulary_is_locked() {
        let canceled = RunCancelResponse { run_id: "id".to_string(), status: "canceled" };
        let already_canceled =
            RunCancelResponse { run_id: "id".to_string(), status: "already_canceled" };
        let already_terminal =
            RunCancelResponse { run_id: "id".to_string(), status: "already_terminal" };

        assert_eq!(canceled.status, "canceled");
        assert_eq!(already_canceled.status, "already_canceled");
        assert_eq!(already_terminal.status, "already_terminal");
    }
}
