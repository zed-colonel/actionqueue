//! Stats route module.
//!
//! This module provides the aggregate statistics endpoint (`GET /api/v1/stats`)
//! for the daemon. The stats endpoint returns read-only operational summaries
//! derived from authoritative state without speculation, consistent with
//! `actionqueue-charter.md`, `actionqueue-scope-appendix-v0.1.md`, and invariant
//! boundaries in `invariant-boundaries-v0.1.md`.
//!
//! # Overview
//!
//! Stats are derived from:
//! - Total task count from the task projection
//! - Runs by state (Scheduled, Ready, Leased, Running, RetryWait, Completed, Failed, Canceled)
//! - Total run count
//! - Total attempt count derived from run instances
//! - Latest sequence number from the WAL projection
//!
//! # Scheduling lag
//!
//! Scheduling lag is intentionally omitted in S1 because there is no authoritative
//! clock injection in the daemon. Do not derive scheduling lag from system time
//! or any non-authoritative source. If scheduling lag becomes necessary in a future
//! release, an authoritative time source must be added to the daemon bootstrap state.
//!
//! # Invariant boundaries
//!
//! The stats handler performs no IO, writes no WAL entries, and mutates no
//! runtime state. It reflects the authoritative projection state from bootstrap.
//!
//! # Response schema
//!
//! ```json
//! {
//!   "total_tasks": 5,
//!   "total_runs": 12,
//!   "attempts_total": 20,
//!   "runs_by_state": {
//!     "scheduled": 2,
//!     "ready": 3,
//!     "leased": 1,
//!     "running": 4,
//!     "retry_wait": 0,
//!     "completed": 1,
//!     "failed": 0,
//!     "canceled": 1
//!   },
//!   "latest_sequence": 42
//! }
//! ```

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

/// Stats response payload.
///
/// This struct represents the stable schema for the stats endpoint response.
/// Fields should not be modified without careful consideration of external
/// dependencies that may rely on this contract.
#[derive(Debug, Clone, Serialize)]
pub struct StatsResponse {
    /// Total number of tasks tracked.
    ///
    /// This is derived from the authoritative task projection in the storage
    /// reducer.
    pub total_tasks: usize,

    /// Total number of runs tracked.
    ///
    /// This is derived from the authoritative run instance projection in the
    /// storage reducer.
    pub total_runs: usize,

    /// Total number of attempts across all runs.
    ///
    /// This is computed as the sum of `RunInstance::attempt_count()` from all
    /// run instances in the authoritative projection. This provides a count
    /// of all attempts that have been started for all runs.
    pub attempts_total: u64,

    /// Counts of runs by state.
    ///
    /// Each state is counted from the authoritative run instance state
    /// projection. The map is sorted by state name for deterministic output.
    pub runs_by_state: StatsRunsByState,

    /// Latest sequence number processed from the WAL.
    ///
    /// This reflects the most recent event sequence number applied to the
    /// projection state.
    pub latest_sequence: u64,
}

/// Counts of runs by state.
///
/// This struct provides a stable schema for run state counts, with each field
/// corresponding to a canonical state in the run lifecycle.
#[derive(Debug, Clone, Serialize)]
pub struct StatsRunsByState {
    /// Number of runs in Scheduled state.
    pub scheduled: usize,
    /// Number of runs in Ready state.
    pub ready: usize,
    /// Number of runs in Leased state.
    pub leased: usize,
    /// Number of runs in Running state.
    pub running: usize,
    /// Number of runs in RetryWait state.
    pub retry_wait: usize,
    /// Number of runs in Completed state.
    pub completed: usize,
    /// Number of runs in Failed state.
    pub failed: usize,
    /// Number of runs in Canceled state.
    pub canceled: usize,
}

impl StatsResponse {
    /// Creates a new stats response from the projection state.
    ///
    /// This function derives stats from the authoritative ReplayReducer state,
    /// counting runs by their current state. Empty state results in zero counts.
    ///
    /// # Arguments
    ///
    /// * `projection` - The authoritative projection state from bootstrap
    pub fn from_projection(
        projection: &actionqueue_storage::recovery::reducer::ReplayReducer,
    ) -> Self {
        // Count runs by state from the run instances projection
        let mut scheduled = 0;
        let mut ready = 0;
        let mut leased = 0;
        let mut running = 0;
        let mut retry_wait = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut canceled = 0;
        let mut attempts_total: u64 = 0;

        for run_instance in projection.run_instances() {
            match run_instance.state() {
                actionqueue_core::run::state::RunState::Scheduled => scheduled += 1,
                actionqueue_core::run::state::RunState::Ready => ready += 1,
                actionqueue_core::run::state::RunState::Leased => leased += 1,
                actionqueue_core::run::state::RunState::Running => running += 1,
                actionqueue_core::run::state::RunState::RetryWait => retry_wait += 1,
                actionqueue_core::run::state::RunState::Suspended => {}
                actionqueue_core::run::state::RunState::Completed => completed += 1,
                actionqueue_core::run::state::RunState::Failed => failed += 1,
                actionqueue_core::run::state::RunState::Canceled => canceled += 1,
            }
            // Sum attempt counts from all run instances
            attempts_total = attempts_total.saturating_add(u64::from(run_instance.attempt_count()));
        }

        Self {
            total_tasks: projection.task_count(),
            total_runs: projection.run_count(),
            attempts_total,
            runs_by_state: StatsRunsByState {
                scheduled,
                ready,
                leased,
                running,
                retry_wait,
                completed,
                failed,
                canceled,
            },
            latest_sequence: projection.latest_sequence(),
        }
    }
}

/// Stats handler.
///
/// This handler responds to `GET /api/v1/stats` requests with a deterministic,
/// side-effect-free payload containing aggregate statistics derived from
/// authoritative state.
///
/// # Invariant boundaries
///
/// This handler performs no IO, writes no WAL entries, and mutates no runtime
/// state. It reflects the projection state from the BootstrapState.
#[tracing::instrument(skip_all)]
pub async fn handle(state: State<super::RouterState>) -> impl IntoResponse {
    let projection = match super::read_projection(&state) {
        Ok(guard) => guard,
        Err(response) => return *response,
    };
    Json(StatsResponse::from_projection(&projection)).into_response()
}

/// Registers the stats route in the router builder.
///
/// This function adds the `/api/v1/stats` endpoint to the router configuration.
/// The route is always available when HTTP is enabled and does not depend
/// on any feature flags.
///
/// # Arguments
///
/// * `router` - The axum router to register routes with
pub fn register_routes(
    router: axum::Router<super::RouterState>,
) -> axum::Router<super::RouterState> {
    router.route("/api/v1/stats", axum::routing::get(handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_response_serialization() {
        let response = StatsResponse {
            total_tasks: 5,
            total_runs: 12,
            attempts_total: 20,
            runs_by_state: StatsRunsByState {
                scheduled: 2,
                ready: 3,
                leased: 1,
                running: 4,
                retry_wait: 0,
                completed: 1,
                failed: 0,
                canceled: 1,
            },
            latest_sequence: 42,
        };

        let json = serde_json::to_string(&response).expect("serialization should succeed");
        assert!(json.contains("\"total_tasks\":5"));
        assert!(json.contains("\"total_runs\":12"));
        assert!(json.contains("\"attempts_total\":20"));
        assert!(json.contains("\"scheduled\":2"));
        assert!(json.contains("\"ready\":3"));
        assert!(json.contains("\"leased\":1"));
        assert!(json.contains("\"running\":4"));
        assert!(json.contains("\"retry_wait\":0"));
        assert!(json.contains("\"completed\":1"));
        assert!(json.contains("\"failed\":0"));
        assert!(json.contains("\"canceled\":1"));
        assert!(json.contains("\"latest_sequence\":42"));
    }

    #[test]
    fn test_stats_response_empty_state() {
        let response = StatsResponse {
            total_tasks: 0,
            total_runs: 0,
            attempts_total: 0,
            runs_by_state: StatsRunsByState {
                scheduled: 0,
                ready: 0,
                leased: 0,
                running: 0,
                retry_wait: 0,
                completed: 0,
                failed: 0,
                canceled: 0,
            },
            latest_sequence: 0,
        };

        assert_eq!(response.total_tasks, 0);
        assert_eq!(response.total_runs, 0);
        assert_eq!(response.attempts_total, 0);
        assert_eq!(response.runs_by_state.scheduled, 0);
    }

    #[test]
    fn test_stats_runs_by_state_serialization() {
        let state = StatsRunsByState {
            scheduled: 1,
            ready: 2,
            leased: 3,
            running: 4,
            retry_wait: 5,
            completed: 6,
            failed: 7,
            canceled: 8,
        };

        let json = serde_json::to_string(&state).expect("serialization should succeed");
        assert!(json.contains("\"scheduled\":1"));
        assert!(json.contains("\"ready\":2"));
        assert!(json.contains("\"leased\":3"));
        assert!(json.contains("\"running\":4"));
        assert!(json.contains("\"retry_wait\":5"));
        assert!(json.contains("\"completed\":6"));
        assert!(json.contains("\"failed\":7"));
        assert!(json.contains("\"canceled\":8"));
    }

    #[test]
    fn test_stats_response_from_projection() {
        let task_spec = actionqueue_core::task::task_spec::TaskSpec::new(
            actionqueue_core::ids::TaskId::new(),
            actionqueue_core::task::task_spec::TaskPayload::with_content_type(
                vec![1, 2, 3],
                "application/octet-stream",
            ),
            actionqueue_core::task::run_policy::RunPolicy::Once,
            actionqueue_core::task::constraints::TaskConstraints::default(),
            actionqueue_core::task::metadata::TaskMetadata::default(),
        )
        .expect("task spec should be valid");

        let run_instance = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
            actionqueue_core::ids::RunId::new(),
            task_spec.id(),
            10,
            10,
        )
        .expect("run instance should be valid");

        let mut reducer = actionqueue_storage::recovery::reducer::ReplayReducer::new();

        let task_event = actionqueue_storage::wal::event::WalEvent::new(
            1,
            actionqueue_storage::wal::event::WalEventType::TaskCreated { task_spec, timestamp: 0 },
        );
        reducer.apply(&task_event).expect("task event should apply");

        let run_event = actionqueue_storage::wal::event::WalEvent::new(
            2,
            actionqueue_storage::wal::event::WalEventType::RunCreated { run_instance },
        );
        reducer.apply(&run_event).expect("run event should apply");

        let response = StatsResponse::from_projection(&reducer);
        assert_eq!(response.total_tasks, 1);
        assert_eq!(response.total_runs, 1);
    }
}
