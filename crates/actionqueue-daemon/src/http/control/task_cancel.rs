//! Task cancel control route handler.
//!
//! This module implements `POST /api/v1/tasks/:task_id/cancel` with durable,
//! authority-lane-only semantics and idempotent behavior.

use std::str::FromStr;

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::mutation::{
    DurabilityPolicy, MutationAuthority, MutationCommand, RunStateTransitionCommand,
    TaskCancelCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_storage::mutation::authority::{MutationAuthorityError, MutationValidationError};
use actionqueue_storage::recovery::reducer::ReplayReducerError;
use axum::extract::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Success payload for task cancellation control endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskCancelResponse {
    /// Task identifier.
    pub task_id: String,
    /// Cancellation status vocabulary (`canceled` or `already_canceled`).
    pub status: &'static str,
    /// Number of runs transitioned to canceled during this request.
    pub runs_canceled: u64,
    /// Number of runs found already terminal.
    pub runs_already_terminal: u64,
    /// Total number of runs currently known for the task.
    pub runs_total: u64,
}

use super::ErrorResponse;

#[derive(Debug, Clone, Serialize)]
struct ErrorDetailsResponse {
    error: &'static str,
    message: &'static str,
    details: ErrorDetails,
}

#[derive(Debug, Clone, Serialize)]
struct ErrorDetails {
    task_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunCancelEligibility {
    Eligible { previous_state: RunState },
    AlreadyTerminal,
}

/// Classifies whether a run can be canceled via a canonical run-state transition.
fn classify_run_cancel_eligibility(state: RunState) -> RunCancelEligibility {
    if state.is_terminal() {
        RunCancelEligibility::AlreadyTerminal
    } else if is_valid_transition(state, RunState::Canceled) {
        RunCancelEligibility::Eligible { previous_state: state }
    } else {
        // Non-terminal states that cannot transition to canceled are treated as already-terminal
        // for response accounting purposes; there is no canonical cancel transition to apply.
        RunCancelEligibility::AlreadyTerminal
    }
}

/// Handles task cancellation requests.
#[tracing::instrument(skip(state))]
pub async fn handle(
    state: State<crate::http::RouterState>,
    Path(task_id_str): Path<String>,
) -> impl IntoResponse {
    let task_id = match parse_task_id(&task_id_str) {
        Some(task_id) => task_id,
        None => return invalid_task_id_response(&task_id_str),
    };

    let authority_handle = match &state.control_authority {
        Some(authority) => authority.clone(),
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "control_unavailable",
                    message: "control authority unavailable".to_string(),
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
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal_error",
                    message: "control authority lock poisoned".to_string(),
                }),
            )
                .into_response();
        }
    };

    if authority.projection().get_task(&task_id).is_none() {
        return task_not_found_response(&task_id_str);
    }

    let run_ids = authority.projection().run_ids_for_task(task_id);
    let runs_total = u64::try_from(run_ids.len()).unwrap_or(u64::MAX);

    let mut eligible_runs = Vec::<(RunId, RunState)>::new();
    let mut runs_already_terminal = 0u64;
    for run_id in &run_ids {
        let Some(current_state) = authority.projection().get_run_state(run_id).copied() else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "internal_error",
                    message: "run state missing from projection".to_string(),
                }),
            )
                .into_response();
        };

        match classify_run_cancel_eligibility(current_state) {
            RunCancelEligibility::Eligible { previous_state } => {
                eligible_runs.push((*run_id, previous_state));
            }
            RunCancelEligibility::AlreadyTerminal => {
                runs_already_terminal += 1;
            }
        }
    }

    let status = if authority.projection().is_task_canceled(task_id) {
        "already_canceled"
    } else {
        let sequence = match authority.projection().latest_sequence().checked_add(1) {
            Some(s) => s,
            None => return internal_error_response("control sequence overflow"),
        };
        let timestamp = sequence;
        let command =
            MutationCommand::TaskCancel(TaskCancelCommand::new(sequence, task_id, timestamp));
        match authority.submit_command(command, DurabilityPolicy::Immediate) {
            Ok(_) => "canceled",
            Err(MutationAuthorityError::Validation(
                MutationValidationError::TaskAlreadyCanceled { .. },
            )) => "already_canceled",
            Err(err @ MutationAuthorityError::PartialDurability { .. }) => {
                return internal_authority_error(err)
            }
            Err(error) => return internal_authority_error(error),
        }
    };

    let mut runs_canceled = 0u64;
    for (run_id, previous_state) in eligible_runs {
        let sequence = match authority.projection().latest_sequence().checked_add(1) {
            Some(s) => s,
            None => return internal_error_response("control sequence overflow"),
        };
        let timestamp = sequence;
        let command = MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            sequence,
            run_id,
            previous_state,
            RunState::Canceled,
            timestamp,
        ));

        match authority.submit_command(command, DurabilityPolicy::Immediate) {
            Ok(_) => {
                runs_canceled += 1;
            }
            Err(MutationAuthorityError::Validation(
                MutationValidationError::PreviousStateMismatch { run_id: mismatch_run, .. },
            )) => {
                if let Some(new_state) =
                    authority.projection().get_run_state(&mismatch_run).copied()
                {
                    if new_state.is_terminal() {
                        runs_already_terminal += 1;
                        continue;
                    }
                }

                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "internal_error",
                        message: "run transitioned concurrently to non-terminal state".to_string(),
                    }),
                )
                    .into_response();
            }
            Err(err @ MutationAuthorityError::PartialDurability { .. }) => {
                return internal_authority_error(err)
            }
            Err(error) => return internal_authority_error(error),
        }
    }

    // Sync shared projection after all mutations are complete.
    match crate::http::write_projection(&state) {
        Ok(mut guard) => *guard = authority.projection().clone(),
        Err(response) => return *response,
    };

    (
        StatusCode::OK,
        Json(TaskCancelResponse {
            task_id: task_id.to_string(),
            status,
            runs_canceled,
            runs_already_terminal,
            runs_total,
        }),
    )
        .into_response()
}

fn parse_task_id(task_id_str: &str) -> Option<TaskId> {
    let task_id = TaskId::from_str(task_id_str).ok()?;
    if task_id.is_nil() {
        return None;
    }
    Some(task_id)
}

fn invalid_task_id_response(task_id: &str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorDetailsResponse {
            error: "invalid_task_id",
            message: "invalid task id",
            details: ErrorDetails { task_id: task_id.to_string() },
        }),
    )
        .into_response()
}

fn task_not_found_response(task_id: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorDetailsResponse {
            error: "task_not_found",
            message: "task not found",
            details: ErrorDetails { task_id: task_id.to_string() },
        }),
    )
        .into_response()
}

fn internal_error_response(message: &'static str) -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: "internal_error", message: message.to_string() }),
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
    use super::TaskCancelResponse;

    #[test]
    fn success_schema_serializes_with_locked_fields() {
        let payload = TaskCancelResponse {
            task_id: "00000000-0000-0000-0000-000000000001".to_string(),
            status: "canceled",
            runs_canceled: 2,
            runs_already_terminal: 1,
            runs_total: 3,
        };
        let json = serde_json::to_string(&payload).expect("serialization should succeed");
        assert_eq!(
            json,
            r#"{"task_id":"00000000-0000-0000-0000-000000000001","status":"canceled","runs_canceled":2,"runs_already_terminal":1,"runs_total":3}"#
        );
    }

    #[test]
    fn status_vocabulary_is_locked() {
        let canceled = TaskCancelResponse {
            task_id: "id".to_string(),
            status: "canceled",
            runs_canceled: 0,
            runs_already_terminal: 0,
            runs_total: 0,
        };
        let already_canceled = TaskCancelResponse {
            task_id: "id".to_string(),
            status: "already_canceled",
            runs_canceled: 0,
            runs_already_terminal: 0,
            runs_total: 0,
        };

        assert_eq!(canceled.status, "canceled");
        assert_eq!(already_canceled.status, "already_canceled");
    }
}
