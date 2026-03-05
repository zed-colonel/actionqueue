//! Tasks list route module.
//!
//! This module provides the task listing endpoint (`GET /api/v1/tasks`) for the daemon.
//! The list response is deterministic, read-only, and derived solely from the
//! authoritative projection state. Ordering and pagination follow the contract in
//! `plans/p6-006-tasks-list-implementation-plan.md`.

use std::cmp::Ordering;

use axum::extract::{RawQuery, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use super::pagination::{pagination_error, parse_pagination, Pagination};

/// Tasks list response payload.
///
/// This struct represents the stable schema for the tasks list endpoint response.
#[derive(Debug, Clone, Serialize)]
pub struct TaskListResponse {
    /// Task summaries returned for the current page.
    pub tasks: Vec<TaskSummary>,
    /// The limit applied to the result set.
    pub limit: usize,
    /// The offset applied to the result set.
    pub offset: usize,
}

/// A stable task summary returned by the tasks list endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct TaskSummary {
    /// Task identifier as a stable string.
    pub id: String,
    /// Task run policy.
    pub run_policy: actionqueue_core::task::run_policy::RunPolicy,
    /// Task constraints.
    pub constraints: actionqueue_core::task::constraints::TaskConstraints,
    /// Task metadata including priority.
    pub metadata: actionqueue_core::task::metadata::TaskMetadata,
    /// Task creation timestamp derived from the WAL event.
    pub created_at: u64,
    /// Task update timestamp (null until update events exist).
    pub updated_at: Option<u64>,
    /// Parent task identifier, if this task is a child in a hierarchy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
}

impl TaskSummary {
    /// Builds a task summary from a projection record.
    fn from_record(record: &actionqueue_storage::recovery::reducer::TaskRecord) -> Self {
        let task_spec = record.task_spec();
        Self {
            id: task_spec.id().to_string(),
            run_policy: task_spec.run_policy().clone(),
            constraints: task_spec.constraints().clone(),
            metadata: task_spec.metadata().clone(),
            created_at: record.created_at(),
            updated_at: record.updated_at(),
            parent_task_id: task_spec.parent_task_id().map(|id| id.to_string()),
        }
    }
}

/// Tasks list handler.
///
/// This handler responds to `GET /api/v1/tasks` with a deterministic, side-effect-free
/// payload containing task summaries derived from authoritative projection state.
#[tracing::instrument(skip_all)]
pub async fn handle(state: State<super::RouterState>, raw_query: RawQuery) -> impl IntoResponse {
    let pagination = match parse_pagination(raw_query.0.as_deref()) {
        Ok(pagination) => pagination,
        Err(error) => return pagination_error(error).into_response(),
    };

    let projection = match super::read_projection(&state) {
        Ok(guard) => guard,
        Err(response) => return *response,
    };
    let response = build_task_list_response(&projection, pagination);
    Json(response).into_response()
}

/// Registers the tasks list route in the router builder.
pub fn register_routes(
    router: axum::Router<super::RouterState>,
) -> axum::Router<super::RouterState> {
    router.route("/api/v1/tasks", axum::routing::get(handle))
}

fn build_task_list_response(
    projection: &actionqueue_storage::recovery::reducer::ReplayReducer,
    pagination: Pagination,
) -> TaskListResponse {
    let mut tasks: Vec<TaskSummary> =
        projection.task_records().map(TaskSummary::from_record).collect();
    tasks.sort_by(compare_task_summaries);

    let start = pagination.offset.min(tasks.len());
    let end = start.saturating_add(pagination.limit).min(tasks.len());
    let paged_tasks = tasks[start..end].to_vec();

    TaskListResponse { tasks: paged_tasks, limit: pagination.limit, offset: pagination.offset }
}

fn compare_task_summaries(left: &TaskSummary, right: &TaskSummary) -> Ordering {
    let priority_cmp = right.metadata.priority().cmp(&left.metadata.priority());
    if priority_cmp != Ordering::Equal {
        return priority_cmp;
    }

    let created_cmp = left.created_at.cmp(&right.created_at);
    if created_cmp != Ordering::Equal {
        return created_cmp;
    }

    left.id.cmp(&right.id)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::event::{WalEvent, WalEventType};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::*;
    use crate::http::pagination::{
        PaginationErrorDetails, PaginationErrorResponse, DEFAULT_LIMIT, MAX_LIMIT,
    };

    fn task_spec_with_priority(task_id: TaskId, priority: i32) -> TaskSpec {
        let metadata = TaskMetadata::new(vec![], priority, None);

        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            metadata,
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
        reducer.apply(&event).expect("task created event should apply")
    }

    #[test]
    fn test_empty_list_response() {
        let reducer = ReplayReducer::new();
        let pagination = Pagination { limit: DEFAULT_LIMIT, offset: 0 };
        let response = build_task_list_response(&reducer, pagination);

        assert!(response.tasks.is_empty());
        assert_eq!(response.limit, DEFAULT_LIMIT);
        assert_eq!(response.offset, 0);
    }

    #[test]
    fn test_ordering_by_priority_created_at_task_id() {
        let mut reducer = ReplayReducer::new();

        let task_high = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000004").unwrap(),
            6,
        );
        let task_mid_early = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000005").unwrap(),
            5,
        );
        let task_mid_a = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000001").unwrap(),
            5,
        );
        let task_mid_b = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000002").unwrap(),
            5,
        );
        let task_low = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000003").unwrap(),
            4,
        );

        apply_task(&mut reducer, 1, task_high, 20);
        apply_task(&mut reducer, 2, task_mid_early, 9);
        apply_task(&mut reducer, 3, task_mid_a, 10);
        apply_task(&mut reducer, 4, task_mid_b, 10);
        apply_task(&mut reducer, 5, task_low, 5);

        let response =
            build_task_list_response(&reducer, Pagination { limit: DEFAULT_LIMIT, offset: 0 });

        let ids: Vec<String> = response.tasks.iter().map(|task| task.id.clone()).collect();
        assert_eq!(
            ids,
            vec![
                "00000000-0000-0000-0000-000000000004",
                "00000000-0000-0000-0000-000000000005",
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000003",
            ]
        );
    }

    #[test]
    fn test_pagination_slicing() {
        let mut reducer = ReplayReducer::new();

        let task_high = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000004").unwrap(),
            6,
        );
        let task_mid_early = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000005").unwrap(),
            5,
        );
        let task_mid_a = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000001").unwrap(),
            5,
        );
        let task_mid_b = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000002").unwrap(),
            5,
        );
        let task_low = task_spec_with_priority(
            TaskId::from_str("00000000-0000-0000-0000-000000000003").unwrap(),
            4,
        );

        apply_task(&mut reducer, 1, task_high, 20);
        apply_task(&mut reducer, 2, task_mid_early, 9);
        apply_task(&mut reducer, 3, task_mid_a, 10);
        apply_task(&mut reducer, 4, task_mid_b, 10);
        apply_task(&mut reducer, 5, task_low, 5);

        let response = build_task_list_response(&reducer, Pagination { limit: 2, offset: 1 });

        let ids: Vec<String> = response.tasks.iter().map(|task| task.id.clone()).collect();
        assert_eq!(
            ids,
            vec!["00000000-0000-0000-0000-000000000005", "00000000-0000-0000-0000-000000000001",]
        );
        assert_eq!(response.limit, 2);
        assert_eq!(response.offset, 1);
    }

    #[test]
    fn test_invalid_pagination_limit_error_schema() {
        let error = parse_pagination(Some("limit=0")).expect_err("limit should be invalid");
        let response = pagination_error(error).into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let payload = PaginationErrorResponse {
            error: "invalid_pagination",
            message: format!("limit must be between 1 and {MAX_LIMIT}"),
            details: PaginationErrorDetails { field: "limit" },
        };
        let json = serde_json::to_string(&payload).expect("error payload should serialize");
        assert_eq!(
            json,
            format!(
                r#"{{"error":"invalid_pagination","message":"limit must be between 1 and {MAX_LIMIT}","details":{{"field":"limit"}}}}"#
            )
        );
    }

    #[test]
    fn test_invalid_pagination_offset_error_schema() {
        let error = parse_pagination(Some("offset=-1")).expect_err("offset should be invalid");
        let response = pagination_error(error).into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let payload = PaginationErrorResponse {
            error: "invalid_pagination",
            message: "offset must be a non-negative integer".to_string(),
            details: PaginationErrorDetails { field: "offset" },
        };
        let json = serde_json::to_string(&payload).expect("error payload should serialize");
        assert_eq!(
            json,
            r#"{"error":"invalid_pagination","message":"offset must be a non-negative integer","details":{"field":"offset"}}"#
        );
    }
}
