//! Single run retrieval route module.
//!
//! This module provides the single run endpoint (`GET /api/v1/runs/:run_id`)
//! for the daemon. The endpoint returns the run lifecycle and lineage details
//! derived from authoritative projection state. The response is read-only,
//! deterministic, and sourced strictly from the recovery projection.
//!
//! # Invariant boundaries
//!
//! The handler performs no IO, reads no mutation authority or WAL, and mutates
//! no runtime state. It only reads from the authoritative projection state.

use std::str::FromStr;

use actionqueue_core::ids::RunId;
use actionqueue_core::mutation::AttemptResultKind;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Run get response payload.
///
/// This struct represents the stable schema for the run get endpoint response.
#[derive(Debug, Clone, Serialize)]
pub struct RunGetResponse {
    /// Run identifier as a stable string.
    pub run_id: String,
    /// Task identifier as a stable string.
    pub task_id: String,
    /// Run state.
    pub state: actionqueue_core::run::state::RunState,
    /// Run creation timestamp derived from the WAL event.
    pub created_at: u64,
    /// Run scheduled timestamp derived from run instance.
    pub scheduled_at: u64,
    /// Number of attempts made for this run.
    pub attempt_count: u32,
    /// Current attempt identifier if any.
    pub current_attempt_id: Option<String>,
    /// Run state history entries derived from WAL.
    pub state_history: Vec<RunStateHistoryEntry>,
    /// Attempt lineage entries derived from WAL.
    pub attempts: Vec<RunAttemptEntry>,
    /// Lease metadata if a lease is active.
    pub lease: Option<RunLeaseEntry>,
    /// Block reason when the run is not Ready.
    pub block_reason: Option<&'static str>,
}

/// Run state history entry for the response payload.
#[derive(Debug, Clone, Serialize)]
pub struct RunStateHistoryEntry {
    /// Previous state, or null for initial Scheduled.
    pub from: Option<actionqueue_core::run::state::RunState>,
    /// New state.
    pub to: actionqueue_core::run::state::RunState,
    /// Timestamp associated with the transition.
    pub timestamp: u64,
}

/// Run attempt entry for the response payload.
#[derive(Debug, Clone, Serialize)]
pub struct RunAttemptEntry {
    /// Attempt identifier as a stable string.
    pub attempt_id: String,
    /// Attempt started timestamp.
    pub started_at: u64,
    /// Attempt finished timestamp if finished.
    pub finished_at: Option<u64>,
    /// Canonical attempt result taxonomy if finished.
    pub result: Option<AttemptResultKind>,
    /// Attempt error message if any.
    pub error: Option<String>,
    /// Opaque handler output bytes if the attempt produced output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<u8>>,
}

/// Run lease entry for the response payload.
#[derive(Debug, Clone, Serialize)]
pub struct RunLeaseEntry {
    /// Lease owner.
    pub owner: String,
    /// Lease expiry timestamp.
    pub expiry: u64,
    /// Lease acquisition timestamp.
    pub acquired_at: u64,
    /// Lease last update timestamp.
    pub updated_at: u64,
}

impl RunGetResponse {
    /// Builds a run get response from projection records.
    fn from_record(
        run_instance: &actionqueue_core::run::run_instance::RunInstance,
        history: &[actionqueue_storage::recovery::reducer::RunStateHistoryEntry],
        attempts: &[actionqueue_storage::recovery::reducer::AttemptHistoryEntry],
        lease: Option<&actionqueue_storage::recovery::reducer::LeaseMetadata>,
    ) -> Self {
        let state_history = history
            .iter()
            .map(|entry| RunStateHistoryEntry {
                from: entry.from(),
                to: entry.to(),
                timestamp: entry.timestamp(),
            })
            .collect();

        let attempts = attempts
            .iter()
            .map(|entry| RunAttemptEntry {
                attempt_id: entry.attempt_id().to_string(),
                started_at: entry.started_at(),
                finished_at: entry.finished_at(),
                result: entry.result(),
                error: entry.error().map(str::to_string),
                output: entry.output().map(|b| b.to_vec()),
            })
            .collect();

        let lease = lease.map(|metadata| RunLeaseEntry {
            owner: metadata.owner().to_string(),
            expiry: metadata.expiry(),
            acquired_at: metadata.acquired_at(),
            updated_at: metadata.updated_at(),
        });

        let block_reason = match run_instance.state() {
            actionqueue_core::run::state::RunState::Ready => None,
            actionqueue_core::run::state::RunState::Scheduled => Some("scheduled"),
            actionqueue_core::run::state::RunState::Leased => Some("leased"),
            actionqueue_core::run::state::RunState::Running => Some("running"),
            actionqueue_core::run::state::RunState::RetryWait => Some("retry_wait"),
            actionqueue_core::run::state::RunState::Suspended => Some("suspended"),
            actionqueue_core::run::state::RunState::Completed
            | actionqueue_core::run::state::RunState::Failed
            | actionqueue_core::run::state::RunState::Canceled => Some("terminal"),
        };

        Self {
            run_id: run_instance.id().to_string(),
            task_id: run_instance.task_id().to_string(),
            state: run_instance.state(),
            created_at: run_instance.created_at(),
            scheduled_at: run_instance.scheduled_at(),
            attempt_count: run_instance.attempt_count(),
            current_attempt_id: run_instance.current_attempt_id().map(|id| id.to_string()),
            state_history,
            attempts,
            lease,
            block_reason,
        }
    }
}

/// Invalid run ID error response.
#[derive(Debug, Clone, Serialize)]
struct InvalidRunIdResponse {
    error: &'static str,
    message: &'static str,
    details: InvalidRunIdDetails,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidRunIdDetails {
    run_id: String,
}

/// Run not found error response.
#[derive(Debug, Clone, Serialize)]
struct RunNotFoundResponse {
    error: &'static str,
    message: &'static str,
    details: RunNotFoundDetails,
}

#[derive(Debug, Clone, Serialize)]
struct RunNotFoundDetails {
    run_id: String,
}

/// Run get handler.
///
/// This handler responds to `GET /api/v1/runs/:run_id` with a deterministic,
/// side-effect-free payload containing the run details derived from
/// authoritative projection state.
#[tracing::instrument(skip(state))]
pub async fn handle(
    state: axum::extract::State<crate::http::RouterState>,
    Path(run_id_str): Path<String>,
) -> impl IntoResponse {
    let run_id = match RunId::from_str(&run_id_str) {
        Ok(id) => id,
        Err(_) => {
            return invalid_run_id_response(&run_id_str).into_response();
        }
    };

    if run_id.as_uuid().is_nil() {
        return invalid_run_id_response(&run_id_str).into_response();
    }

    let projection = match super::read_projection(&state) {
        Ok(guard) => guard,
        Err(response) => return (*response).into_response(),
    };
    let run_instance = match projection.get_run_instance(&run_id) {
        Some(instance) => instance,
        None => return run_not_found_response(&run_id_str).into_response(),
    };

    let history = projection.get_run_history(&run_id).unwrap_or(&[]);
    let attempts = projection.get_attempt_history(&run_id).unwrap_or(&[]);
    let lease = projection.get_lease_metadata(&run_id);

    let response = RunGetResponse::from_record(run_instance, history, attempts, lease);
    (StatusCode::OK, Json(response)).into_response()
}

/// Registers the run get route in the router builder.
pub fn register_routes(
    router: axum::Router<crate::http::RouterState>,
) -> axum::Router<crate::http::RouterState> {
    router.route("/api/v1/runs/:run_id", axum::routing::get(handle))
}

/// Creates an invalid run ID error response.
fn invalid_run_id_response(run_id: &str) -> impl IntoResponse {
    let response = InvalidRunIdResponse {
        error: "invalid_run_id",
        message: "invalid run id",
        details: InvalidRunIdDetails { run_id: run_id.to_string() },
    };
    (StatusCode::BAD_REQUEST, Json(response)).into_response()
}

/// Creates a run not found error response.
fn run_not_found_response(run_id: &str) -> impl IntoResponse {
    let response = RunNotFoundResponse {
        error: "run_not_found",
        message: "run not found",
        details: RunNotFoundDetails { run_id: run_id.to_string() },
    };
    (StatusCode::NOT_FOUND, Json(response)).into_response()
}

#[cfg(test)]
mod tests {
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
    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::event::{WalEvent, WalEventType};
    use actionqueue_storage::wal::WalAppendTelemetry;
    use axum::extract::{Path, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;

    use super::RunGetResponse;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    fn task_spec(task_id: TaskId) -> TaskSpec {
        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("task spec should be valid")
    }

    fn run_instance_scheduled(run_id: RunId, task_id: TaskId) -> RunInstance {
        RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
            .expect("run instance should be valid")
    }

    fn apply_event(reducer: &mut ReplayReducer, sequence: u64, event: WalEventType) {
        let event = WalEvent::new(sequence, event);
        reducer.apply(&event).expect("event should apply");
    }

    fn build_state(reducer: ReplayReducer) -> crate::http::RouterState {
        let metrics = std::sync::Arc::new(
            crate::metrics::registry::MetricsRegistry::new(None)
                .expect("test metrics registry should initialize"),
        );
        let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));
        let state = crate::http::RouterStateInner::new(
            crate::bootstrap::RouterConfig { control_enabled: false, metrics_enabled: false },
            std::sync::Arc::new(std::sync::RwLock::new(reducer)),
            crate::http::RouterObservability {
                metrics,
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock,
                recovery_observations: RecoveryObservations::zero(),
            },
            crate::bootstrap::ReadyStatus::ready(),
        );
        std::sync::Arc::new(state)
    }

    async fn response_body_string(response: axum::response::Response) -> String {
        let bytes = response.into_body().collect().await.expect("body should collect").to_bytes();
        String::from_utf8(bytes.to_vec()).expect("response body should be utf-8")
    }

    #[test]
    fn test_run_get_success_with_history_attempts_lease() {
        let mut reducer = ReplayReducer::new();

        let task_id = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let run_id = RunId::from_str("22222222-2222-2222-2222-222222222222").unwrap();
        let attempt_id = AttemptId::from_str("33333333-3333-3333-3333-333333333333").unwrap();
        let attempt_id_2 = AttemptId::from_str("44444444-4444-4444-4444-444444444444").unwrap();

        apply_event(
            &mut reducer,
            1,
            WalEventType::TaskCreated { task_spec: task_spec(task_id), timestamp: 900 },
        );
        apply_event(
            &mut reducer,
            2,
            WalEventType::RunCreated { run_instance: run_instance_scheduled(run_id, task_id) },
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
        apply_event(
            &mut reducer,
            8,
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Failure,
                error: Some("boom".to_string()),
                output: None,
                timestamp: 1300,
            },
        );
        apply_event(
            &mut reducer,
            9,
            WalEventType::AttemptStarted { run_id, attempt_id: attempt_id_2, timestamp: 1350 },
        );
        apply_event(
            &mut reducer,
            10,
            WalEventType::LeaseHeartbeat {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 2100,
                timestamp: 1400,
            },
        );

        let run_instance = reducer.get_run_instance(&run_id).expect("run should exist");
        let history = reducer.get_run_history(&run_id).expect("history should exist");
        let attempts = reducer.get_attempt_history(&run_id).expect("attempts should exist");
        let lease = reducer.get_lease_metadata(&run_id);

        let response = RunGetResponse::from_record(run_instance, history, attempts, lease);

        assert_eq!(response.run_id, run_id.to_string());
        assert_eq!(response.task_id, task_id.to_string());
        assert_eq!(response.state, RunState::Running);
        assert_eq!(response.state_history.first().unwrap().from, None);
        assert_eq!(response.state_history.first().unwrap().to, RunState::Scheduled);
        assert_eq!(response.state_history.first().unwrap().timestamp, 1000);
        assert_eq!(response.attempts.len(), 2);
        assert_eq!(response.attempts[0].attempt_id, attempt_id.to_string());
        assert_eq!(response.attempts[0].started_at, 1201);
        assert_eq!(response.attempts[0].finished_at, Some(1300));
        assert_eq!(response.attempts[0].result, Some(AttemptResultKind::Failure));
        assert_eq!(response.attempts[0].error, Some("boom".to_string()));
        assert_eq!(response.attempts[1].attempt_id, attempt_id_2.to_string());
        assert_eq!(response.attempts[1].started_at, 1350);
        assert_eq!(response.attempts[1].finished_at, None);
        assert_eq!(response.attempts[1].result, None);
        assert_eq!(response.attempts[1].error, None);
        assert_eq!(response.lease.as_ref().unwrap().updated_at, 1400);
        assert_eq!(response.block_reason, Some("running"));
    }

    #[test]
    fn test_run_get_invalid_id_returns_400() {
        let response = super::invalid_run_id_response("not-a-uuid").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response_json = serde_json::to_string(&super::InvalidRunIdResponse {
            error: "invalid_run_id",
            message: "invalid run id",
            details: super::InvalidRunIdDetails { run_id: "not-a-uuid".to_string() },
        })
        .expect("should serialize response");

        assert_eq!(
            response_json,
            r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"not-a-uuid"}}"#
        );
    }

    #[test]
    fn test_run_get_nil_id_returns_400() {
        let response =
            super::invalid_run_id_response("00000000-0000-0000-0000-000000000000").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response_json = serde_json::to_string(&super::InvalidRunIdResponse {
            error: "invalid_run_id",
            message: "invalid run id",
            details: super::InvalidRunIdDetails {
                run_id: "00000000-0000-0000-0000-000000000000".to_string(),
            },
        })
        .expect("should serialize response");

        assert_eq!(
            response_json,
            r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"00000000-0000-0000-0000-000000000000"}}"#
        );
    }

    #[test]
    fn test_run_get_not_found_returns_404() {
        let response =
            super::run_not_found_response("11111111-1111-1111-1111-111111111111").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response_json = serde_json::to_string(&super::RunNotFoundResponse {
            error: "run_not_found",
            message: "run not found",
            details: super::RunNotFoundDetails {
                run_id: "11111111-1111-1111-1111-111111111111".to_string(),
            },
        })
        .expect("should serialize response");

        assert_eq!(
            response_json,
            r#"{"error":"run_not_found","message":"run not found","details":{"run_id":"11111111-1111-1111-1111-111111111111"}}"#
        );
    }

    #[test]
    fn test_block_reason_ready_is_null() {
        let mut reducer = ReplayReducer::new();

        let task_id = TaskId::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        let run_id = RunId::from_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();

        apply_event(
            &mut reducer,
            1,
            WalEventType::TaskCreated { task_spec: task_spec(task_id), timestamp: 900 },
        );
        apply_event(
            &mut reducer,
            2,
            WalEventType::RunCreated { run_instance: run_instance_scheduled(run_id, task_id) },
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

        let run_instance = reducer.get_run_instance(&run_id).expect("run should exist");
        let history = reducer.get_run_history(&run_id).expect("history should exist");
        let attempts = reducer.get_attempt_history(&run_id).expect("attempts should exist");
        let lease = reducer.get_lease_metadata(&run_id);

        let response = RunGetResponse::from_record(run_instance, history, attempts, lease);
        assert_eq!(response.block_reason, None);
    }

    #[tokio::test]
    async fn test_run_get_handle_invalid_id_returns_400() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("not-a-uuid".to_string())).await.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"not-a-uuid"}}"#
        );
    }

    #[tokio::test]
    async fn test_run_get_handle_nil_id_returns_400() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("00000000-0000-0000-0000-000000000000".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"invalid_run_id","message":"invalid run id","details":{"run_id":"00000000-0000-0000-0000-000000000000"}}"#
        );
    }

    #[tokio::test]
    async fn test_run_get_handle_not_found_returns_404() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("11111111-1111-1111-1111-111111111111".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"run_not_found","message":"run not found","details":{"run_id":"11111111-1111-1111-1111-111111111111"}}"#
        );
    }
}
