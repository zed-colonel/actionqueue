//! Worker communication types for the async dispatch loop.
//!
//! Defines the request/result messages exchanged between the dispatch loop
//! and spawned worker tasks via tokio channels.

use actionqueue_core::budget::BudgetConsumption;
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
#[cfg(feature = "budget")]
use actionqueue_executor_local::handler::CancellationContext;
use actionqueue_executor_local::types::ExecutorResponse;

/// The result sent back from a worker task after attempt completion.
#[derive(Debug)]
pub(crate) struct WorkerResult {
    /// The unique identifier for the run instance.
    pub run_id: RunId,
    /// The unique identifier for the attempt.
    pub attempt_id: AttemptId,
    /// The executor response classifying the attempt outcome.
    pub response: ExecutorResponse,
    /// Maximum attempts allowed for this run.
    pub max_attempts: u32,
    /// The attempt number that completed (1-indexed).
    pub attempt_number: u32,
    /// Resource consumption reported by the handler for this attempt.
    ///
    /// Populated by the executor for budget tracking. Processed by the budget integration
    /// in the dispatch loop under the `budget` feature flag.
    #[cfg_attr(not(feature = "budget"), allow(dead_code))]
    pub consumption: Vec<BudgetConsumption>,
}

/// Tracks an in-flight run being executed by a worker.
#[derive(Debug)]
pub(crate) struct InFlightRun {
    /// The run identifier.
    pub run_id: RunId,
    /// The attempt identifier for diagnostic/tracing use.
    pub attempt_id: AttemptId,
    /// The task identifier for concurrency key release.
    pub task_id: TaskId,
    /// Lease expiry timestamp.
    pub lease_expiry: u64,
    /// The attempt number (1-indexed) for diagnostic/tracing use.
    pub attempt_number: u32,
    /// Maximum attempts allowed for diagnostic/tracing use.
    pub max_attempts: u32,
    /// Cancellation context clone retained by the dispatch loop.
    ///
    /// Used to signal cooperative suspension (e.g. budget exhaustion)
    /// to a running handler via [`CancellationContext::cancel()`].
    #[cfg(feature = "budget")]
    pub cancellation_context: Option<CancellationContext>,
}
