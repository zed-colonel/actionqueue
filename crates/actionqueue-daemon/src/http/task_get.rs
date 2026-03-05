//! Single task retrieval route module.
//!
//! This module provides the single task endpoint (`GET /api/v1/tasks/:task_id`)
//! for the daemon. The endpoint returns a task's full specification (including
//! payload and content_type) plus authoritative timestamps. The response is
//! read-only, deterministic, and sourced strictly from the recovery projection.
//!
//! # Invariant boundaries
//!
//! The handler performs no IO, reads no mutation authority or WAL, and mutates
//! no runtime state. It only reads from the authoritative projection state.

use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Task get response payload.
///
/// This struct represents the stable schema for the task get endpoint response.
#[derive(Debug, Clone, Serialize)]
pub struct TaskGetResponse {
    /// Task identifier as a stable string.
    pub id: String,
    /// Task payload bytes (serde `Vec<u8>` JSON array of numbers).
    pub payload: Vec<u8>,
    /// Optional payload content-type hint.
    pub content_type: Option<String>,
    /// Task run policy.
    pub run_policy: actionqueue_core::task::run_policy::RunPolicy,
    /// Task constraints.
    pub constraints: actionqueue_core::task::constraints::TaskConstraints,
    /// Task metadata including priority.
    pub metadata: actionqueue_core::task::metadata::TaskMetadata,
    /// Parent task identifier, if this task is a child in a workflow hierarchy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    /// Task creation timestamp derived from the WAL event.
    pub created_at: u64,
    /// Task update timestamp (null until update events exist).
    pub updated_at: Option<u64>,
}

impl TaskGetResponse {
    /// Builds a task get response from a projection record.
    fn from_record(record: &actionqueue_storage::recovery::reducer::TaskRecord) -> Self {
        let task_spec = record.task_spec();
        Self {
            id: task_spec.id().to_string(),
            payload: task_spec.payload().to_vec(),
            content_type: task_spec.content_type().map(str::to_string),
            run_policy: task_spec.run_policy().clone(),
            constraints: task_spec.constraints().clone(),
            metadata: task_spec.metadata().clone(),
            parent_task_id: task_spec.parent_task_id().map(|id| id.to_string()),
            created_at: record.created_at(),
            updated_at: record.updated_at(),
        }
    }
}

/// Invalid task ID error response.
#[derive(Debug, Clone, Serialize)]
struct InvalidTaskIdResponse {
    error: &'static str,
    message: &'static str,
    details: InvalidTaskIdDetails,
}

#[derive(Debug, Clone, Serialize)]
struct InvalidTaskIdDetails {
    task_id: String,
}

/// Task not found error response.
#[derive(Debug, Clone, Serialize)]
struct TaskNotFoundResponse {
    error: &'static str,
    message: &'static str,
    details: TaskNotFoundDetails,
}

#[derive(Debug, Clone, Serialize)]
struct TaskNotFoundDetails {
    task_id: String,
}

/// Task get handler.
///
/// This handler responds to `GET /api/v1/tasks/:task_id` with a deterministic,
/// side-effect-free payload containing the task specification derived from
/// authoritative projection state.
#[tracing::instrument(skip(state))]
pub async fn handle(
    state: axum::extract::State<crate::http::RouterState>,
    Path(task_id_str): Path<String>,
) -> impl IntoResponse {
    // Parse the task ID
    let task_id = match TaskId::from_str(&task_id_str) {
        Ok(id) => id,
        Err(_) => {
            return invalid_task_id_response(&task_id_str).into_response();
        }
    };

    // Check for nil UUID
    if task_id.is_nil() {
        return invalid_task_id_response(&task_id_str).into_response();
    }

    // Lookup the task record in the projection
    let projection = match super::read_projection(&state) {
        Ok(guard) => guard,
        Err(response) => return (*response).into_response(),
    };
    let record = projection.get_task_record(&task_id);

    // Handle not found
    let Some(record) = record else {
        return task_not_found_response(&task_id_str).into_response();
    };

    // Build and return the response
    let response = TaskGetResponse::from_record(record);
    (StatusCode::OK, Json(response)).into_response()
}

/// Registers the task get route in the router builder.
pub fn register_routes(
    router: axum::Router<crate::http::RouterState>,
) -> axum::Router<crate::http::RouterState> {
    router.route("/api/v1/tasks/:task_id", axum::routing::get(handle))
}

/// Creates an invalid task ID error response.
fn invalid_task_id_response(task_id: &str) -> impl IntoResponse {
    let response = InvalidTaskIdResponse {
        error: "invalid_task_id",
        message: "invalid task id",
        details: InvalidTaskIdDetails { task_id: task_id.to_string() },
    };
    (StatusCode::BAD_REQUEST, Json(response)).into_response()
}

/// Creates a task not found error response.
fn task_not_found_response(task_id: &str) -> impl IntoResponse {
    let response = TaskNotFoundResponse {
        error: "task_not_found",
        message: "task not found",
        details: TaskNotFoundDetails { task_id: task_id.to_string() },
    };
    (StatusCode::NOT_FOUND, Json(response)).into_response()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

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

    use super::TaskGetResponse;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    fn task_spec_with_payload(task_id: actionqueue_core::ids::TaskId, payload: &[u8]) -> TaskSpec {
        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(payload.to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("task spec should be valid")
    }

    fn apply_task(
        reducer: &mut ReplayReducer,
        sequence: u64,
        task_spec: TaskSpec,
        created_at: u64,
    ) {
        let event =
            WalEvent::new(sequence, WalEventType::TaskCreated { task_spec, timestamp: created_at });
        reducer.apply(&event).expect("task created event should apply");
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
    fn test_task_get_response_from_record() {
        let mut reducer = ReplayReducer::new();

        let task_id =
            actionqueue_core::ids::TaskId::from_str("00000000-0000-0000-0000-000000000001")
                .unwrap();
        let task_spec = task_spec_with_payload(task_id, b"test payload");
        apply_task(&mut reducer, 1, task_spec, 100);

        let record = reducer.get_task_record(&task_id).expect("task should exist");
        let response = TaskGetResponse::from_record(record);

        assert_eq!(response.id, "00000000-0000-0000-0000-000000000001");
        assert_eq!(response.payload, b"test payload");
        assert_eq!(response.content_type, Some("application/octet-stream".to_string()));
        assert_eq!(response.created_at, 100);
        assert!(response.updated_at.is_none());
    }

    #[test]
    fn test_invalid_task_id_response_schema() {
        let response = super::invalid_task_id_response("test-id").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response_json = serde_json::to_string(&super::InvalidTaskIdResponse {
            error: "invalid_task_id",
            message: "invalid task id",
            details: super::InvalidTaskIdDetails { task_id: "test-id".to_string() },
        })
        .expect("should serialize response");

        assert_eq!(
            response_json,
            r#"{"error":"invalid_task_id","message":"invalid task id","details":{"task_id":"test-id"}}"#
        );
    }

    #[test]
    fn test_task_not_found_response_schema() {
        let response = super::task_not_found_response("test-id").into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response_json = serde_json::to_string(&super::TaskNotFoundResponse {
            error: "task_not_found",
            message: "task not found",
            details: super::TaskNotFoundDetails { task_id: "test-id".to_string() },
        })
        .expect("should serialize response");

        assert_eq!(
            response_json,
            r#"{"error":"task_not_found","message":"task not found","details":{"task_id":"test-id"}}"#
        );
    }

    #[tokio::test]
    async fn test_task_get_handle_invalid_id_returns_400() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("not-a-uuid".to_string())).await.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"invalid_task_id","message":"invalid task id","details":{"task_id":"not-a-uuid"}}"#
        );
    }

    #[tokio::test]
    async fn test_task_get_handle_nil_id_returns_400() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("00000000-0000-0000-0000-000000000000".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"invalid_task_id","message":"invalid task id","details":{"task_id":"00000000-0000-0000-0000-000000000000"}}"#
        );
    }

    #[tokio::test]
    async fn test_task_get_handle_not_found_returns_404() {
        let state = build_state(ReplayReducer::new());
        let response =
            super::handle(State(state), Path("00000000-0000-0000-0000-000000000999".to_string()))
                .await
                .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_body_string(response).await,
            r#"{"error":"task_not_found","message":"task not found","details":{"task_id":"00000000-0000-0000-0000-000000000999"}}"#
        );
    }
}
