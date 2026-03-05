//! Runs list route module.
//!
//! This module provides the run listing endpoint (`GET /api/v1/runs`) for the daemon.
//! The list response is deterministic, read-only, and derived solely from the
//! authoritative projection state. Ordering and pagination follow the contract in
//! `plans/p6-008-runs-list-implementation-plan.md`.

use std::cmp::Ordering;

use axum::extract::{RawQuery, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use super::pagination::{pagination_error, parse_pagination, Pagination};

/// Runs list response payload.
///
/// This struct represents the stable schema for the runs list endpoint response.
#[derive(Debug, Clone, Serialize)]
pub struct RunListResponse {
    /// Run summaries returned for the current page.
    pub runs: Vec<RunSummary>,
    /// The limit applied to the result set.
    pub limit: usize,
    /// The offset applied to the result set.
    pub offset: usize,
}

/// A stable run summary returned by the runs list endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    /// Run identifier as a stable string.
    pub run_id: String,
    /// Task identifier as a stable string.
    pub task_id: String,
    /// Run state (serialized via serde).
    pub state: actionqueue_core::run::state::RunState,
    /// Run creation timestamp derived from the WAL event.
    pub created_at: u64,
    /// Run scheduled_at timestamp.
    pub scheduled_at: u64,
    /// Number of attempts made for this run.
    pub attempt_count: u32,
    /// Concurrency key from task constraints, or null if task is missing.
    pub concurrency_key: Option<String>,
}

impl RunSummary {
    /// Builds a run summary from a run instance and an optional task spec.
    fn from_run_instance(
        run_instance: &actionqueue_core::run::run_instance::RunInstance,
        task_constraints: Option<&actionqueue_core::task::constraints::TaskConstraints>,
    ) -> Self {
        let concurrency_key = task_constraints
            .and_then(|constraints| constraints.concurrency_key().map(str::to_string));
        Self {
            run_id: run_instance.id().to_string(),
            task_id: run_instance.task_id().to_string(),
            state: run_instance.state(),
            created_at: run_instance.created_at(),
            scheduled_at: run_instance.scheduled_at(),
            attempt_count: run_instance.attempt_count(),
            concurrency_key,
        }
    }
}

/// Runs list handler.
///
/// This handler responds to `GET /api/v1/runs` with a deterministic, side-effect-free
/// payload containing run summaries derived from authoritative projection state.
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
    let response = build_runs_list_response(&projection, pagination);
    Json(response).into_response()
}

/// Registers the runs list route in the router builder.
pub fn register_routes(
    router: axum::Router<super::RouterState>,
) -> axum::Router<super::RouterState> {
    router.route("/api/v1/runs", axum::routing::get(handle))
}

fn build_runs_list_response(
    projection: &actionqueue_storage::recovery::reducer::ReplayReducer,
    pagination: Pagination,
) -> RunListResponse {
    let mut summaries: Vec<RunSummary> = projection
        .run_instances()
        .map(|run_instance| {
            let task_id = run_instance.task_id();
            let task_spec = projection.get_task(&task_id);
            let task_constraints = task_spec.map(|spec| spec.constraints());
            RunSummary::from_run_instance(run_instance, task_constraints)
        })
        .collect();
    summaries.sort_by(compare_run_summaries);

    let start = pagination.offset.min(summaries.len());
    let end = start.saturating_add(pagination.limit).min(summaries.len());
    let paged_runs = summaries[start..end].to_vec();

    RunListResponse { runs: paged_runs, limit: pagination.limit, offset: pagination.offset }
}

fn compare_run_summaries(left: &RunSummary, right: &RunSummary) -> Ordering {
    let scheduled_at_cmp = left.scheduled_at.cmp(&right.scheduled_at);
    if scheduled_at_cmp != Ordering::Equal {
        return scheduled_at_cmp;
    }

    left.run_id.cmp(&right.run_id)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use actionqueue_core::ids::{RunId, TaskId};
    use actionqueue_core::run::run_instance::RunInstance;
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
        parse_limit, parse_offset, PaginationError, PaginationErrorDetails, PaginationErrorResponse,
    };

    fn task_spec_with_concurrency_key(task_id: TaskId, concurrency_key: Option<&str>) -> TaskSpec {
        let constraints = TaskConstraints::new(1, None, concurrency_key.map(|s| s.to_string()))
            .expect("constraints should be valid");
        let metadata = TaskMetadata::default();

        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            constraints,
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
        reducer.apply(&event).expect("task created event should apply");
    }

    fn apply_run(reducer: &mut ReplayReducer, sequence: u64, run_instance: RunInstance) {
        let event = WalEvent::new(sequence, WalEventType::RunCreated { run_instance });
        reducer.apply(&event).expect("run created event should apply");
    }

    fn run_instance_scheduled(
        run_id: RunId,
        task_id: TaskId,
        scheduled_at: u64,
        created_at: u64,
    ) -> RunInstance {
        RunInstance::new_scheduled_with_id(run_id, task_id, scheduled_at, created_at)
            .expect("run instance should be valid")
    }

    #[test]
    fn test_empty_runs_list_response() {
        let reducer = ReplayReducer::new();
        let pagination = Pagination { limit: 100, offset: 0 };
        let response = build_runs_list_response(&reducer, pagination);

        assert!(response.runs.is_empty());
        assert_eq!(response.limit, 100);
        assert_eq!(response.offset, 0);
    }

    #[test]
    fn test_ordering_by_scheduled_at_then_run_id() {
        let mut reducer = ReplayReducer::new();

        let task_id_1 = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let task_id_2 = TaskId::from_str("22222222-2222-2222-2222-222222222222").unwrap();
        let task_id_3 = TaskId::from_str("33333333-3333-3333-3333-333333333333").unwrap();

        let task_spec_1 = task_spec_with_concurrency_key(task_id_1, Some("key-1"));
        let task_spec_2 = task_spec_with_concurrency_key(task_id_2, Some("key-2"));
        let task_spec_3 = task_spec_with_concurrency_key(task_id_3, Some("key-3"));

        apply_task(&mut reducer, 1, task_spec_1.clone(), 1000);
        apply_task(&mut reducer, 2, task_spec_2.clone(), 1001);
        apply_task(&mut reducer, 3, task_spec_3.clone(), 1002);

        let run_id_1 = RunId::from_str("11111111-1111-1111-1111-111111111110").unwrap();
        let run_id_2 = RunId::from_str("22222222-2222-2222-2222-222222222220").unwrap();
        let run_id_3 = RunId::from_str("33333333-3333-3333-3333-333333333330").unwrap();

        // Create scheduled runs with different scheduled_at values (for ordering test)
        let run_1 = run_instance_scheduled(run_id_1, task_id_1, 1000, 1500);
        let run_2 = run_instance_scheduled(run_id_2, task_id_2, 1000, 1501);
        let run_3 = run_instance_scheduled(run_id_3, task_id_3, 2000, 1502);

        apply_run(&mut reducer, 4, run_1);
        apply_run(&mut reducer, 5, run_2);
        apply_run(&mut reducer, 6, run_3);

        let pagination = Pagination { limit: 100, offset: 0 };
        let response = build_runs_list_response(&reducer, pagination);

        assert_eq!(response.runs.len(), 3);
        // First two runs have same scheduled_at (1000), so sort by run_id
        assert_eq!(response.runs[0].run_id, run_id_1.to_string());
        assert_eq!(response.runs[1].run_id, run_id_2.to_string());
        // Third run has later scheduled_at (2000)
        assert_eq!(response.runs[2].run_id, run_id_3.to_string());
    }

    #[test]
    fn test_pagination_slicing() {
        let mut reducer = ReplayReducer::new();

        let task_id = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let task_spec = task_spec_with_concurrency_key(task_id, Some("key"));

        apply_task(&mut reducer, 1, task_spec, 1000);

        let run_ids: Vec<RunId> = (1..=5)
            .map(|i| {
                let uuid_str = format!("00000000-0000-0000-0000-0000000000{}{}", i / 10, i % 10);
                RunId::from_str(&uuid_str).unwrap()
            })
            .collect();

        for (i, run_id) in run_ids.iter().enumerate() {
            // Use scheduled_at=1000+i, created_at=2000+i
            let run = run_instance_scheduled(*run_id, task_id, 1000 + i as u64, 2000 + i as u64);
            apply_run(&mut reducer, 2 + i as u64, run);
        }

        // Test limit=2, offset=1
        let pagination = Pagination { limit: 2, offset: 1 };
        let response = build_runs_list_response(&reducer, pagination);

        assert_eq!(response.runs.len(), 2);
        assert_eq!(response.limit, 2);
        assert_eq!(response.offset, 1);
        assert_eq!(response.runs[0].run_id, run_ids[1].to_string());
        assert_eq!(response.runs[1].run_id, run_ids[2].to_string());

        // Test limit=3, offset=3
        let pagination = Pagination { limit: 3, offset: 3 };
        let response = build_runs_list_response(&reducer, pagination);

        assert_eq!(response.runs.len(), 2);
        assert_eq!(response.limit, 3);
        assert_eq!(response.offset, 3);
        assert_eq!(response.runs[0].run_id, run_ids[3].to_string());
        assert_eq!(response.runs[1].run_id, run_ids[4].to_string());
    }

    #[test]
    fn test_invalid_pagination_limit_error_schema() {
        // Test limit=0
        let result = parse_limit("0");
        assert!(matches!(result, Err(ref e) if e.field == "limit"));

        // Test limit=1001 (exceeds max)
        let result = parse_limit("1001");
        assert!(matches!(result, Err(ref e) if e.field == "limit"));

        // Verify error response structure
        let error = PaginationError::new("limit", "limit must be between 1 and 1000");
        let error_message = error.message.clone();
        let error_field = error.field;
        let response = pagination_error(error).into_response();
        let status = response.status();

        let response_json = serde_json::to_string(&PaginationErrorResponse {
            error: "invalid_pagination",
            message: error_message,
            details: PaginationErrorDetails { field: error_field },
        })
        .expect("should serialize response");

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json,
            r#"{"error":"invalid_pagination","message":"limit must be between 1 and 1000","details":{"field":"limit"}}"#
        );
    }

    #[test]
    fn test_invalid_pagination_offset_error_schema() {
        // Test offset=-1 (should fail since usize can't be negative)
        // In the implementation, parse_non_negative uses parse::<usize>()
        // which returns an error for negative numbers
        let result = parse_offset("-1");
        assert!(matches!(result, Err(ref e) if e.field == "offset"));

        // Verify error response structure
        let error = PaginationError::new("offset", "offset must be a non-negative integer");
        let error_message = error.message.clone();
        let error_field = error.field;
        let response = pagination_error(error).into_response();
        let status = response.status();

        let response_json = serde_json::to_string(&PaginationErrorResponse {
            error: "invalid_pagination",
            message: error_message,
            details: PaginationErrorDetails { field: error_field },
        })
        .expect("should serialize response");

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response_json,
            r#"{"error":"invalid_pagination","message":"offset must be a non-negative integer","details":{"field":"offset"}}"#
        );
    }

    #[test]
    fn test_concurrency_key_resolution_and_missing_task() {
        let mut reducer = ReplayReducer::new();

        // Task with concurrency key
        let task_id_with_key = TaskId::from_str("11111111-1111-1111-1111-111111111111").unwrap();
        let task_spec_with_key = task_spec_with_concurrency_key(task_id_with_key, Some("my-key"));

        // Task without concurrency key (None)
        let task_id_no_key = TaskId::from_str("22222222-2222-2222-2222-222222222222").unwrap();
        let task_spec_no_key = task_spec_with_concurrency_key(task_id_no_key, None);

        apply_task(&mut reducer, 1, task_spec_with_key, 1000);
        apply_task(&mut reducer, 2, task_spec_no_key, 1001);

        // Run with task that has concurrency key
        let run_id_with_key = RunId::from_str("11111111-1111-1111-1111-111111111110").unwrap();
        let run_with_key = run_instance_scheduled(run_id_with_key, task_id_with_key, 500, 1500);

        // Run with task that has no concurrency key
        let run_id_no_key = RunId::from_str("22222222-2222-2222-2222-222222222220").unwrap();
        let run_no_key = run_instance_scheduled(run_id_no_key, task_id_no_key, 600, 1501);

        // Run with missing task (task was never created)
        let missing_task_id = TaskId::from_str("33333333-3333-3333-3333-333333333333").unwrap();
        let run_id_missing_task = RunId::from_str("33333333-3333-3333-3333-333333333330").unwrap();
        let run_missing_task =
            run_instance_scheduled(run_id_missing_task, missing_task_id, 700, 1502);

        apply_run(&mut reducer, 3, run_with_key);
        apply_run(&mut reducer, 4, run_no_key);
        apply_run(&mut reducer, 5, run_missing_task);

        let pagination = Pagination { limit: 100, offset: 0 };
        let response = build_runs_list_response(&reducer, pagination);

        assert_eq!(response.runs.len(), 3);

        // Runs sorted by scheduled_at ascending
        // First run: has concurrency key (scheduled_at=500)
        assert_eq!(response.runs[0].run_id, run_id_with_key.to_string());
        assert_eq!(response.runs[0].concurrency_key, Some("my-key".to_string()));

        // Second run: task exists but concurrency_key is None (scheduled_at=600)
        assert_eq!(response.runs[1].run_id, run_id_no_key.to_string());
        assert_eq!(response.runs[1].concurrency_key, None);

        // Third run: task missing from reducer, should have null concurrency_key (scheduled_at=700)
        assert_eq!(response.runs[2].run_id, run_id_missing_task.to_string());
        assert_eq!(response.runs[2].concurrency_key, None);
    }
}
