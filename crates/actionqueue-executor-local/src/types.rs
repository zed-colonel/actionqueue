//! Executor request and response contracts for attempt-level execution.
//!
//! This module defines the contract messages exchanged between the executor
//! system and handler implementations. These contracts ensure deterministic
//! attempt outcomes and explicit timeout classification.

use std::sync::Arc;

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::constraints::TaskConstraints;

use crate::children::ChildrenSnapshot;
use crate::handler::cancellation::CancellationContext;
use crate::handler::TaskSubmissionPort;

/// Executor request sent to a handler for attempt execution.
///
/// This structure carries all information needed for a handler to execute
/// an attempt. The dispatch loop populates `submission` and `children` from
/// the workflow infrastructure when available; they are `None` in the base case.
pub struct ExecutorRequest {
    /// The unique identifier for the run instance.
    pub run_id: RunId,
    /// The unique identifier for this specific attempt within the run.
    pub attempt_id: AttemptId,
    /// The opaque payload bytes to execute.
    pub payload: Vec<u8>,
    /// Task constraints snapshot for this attempt (max_attempts, timeout, etc.).
    pub constraints: TaskConstraints,
    /// The 1-indexed attempt number (first attempt is 1, not 0).
    pub attempt_number: u32,
    /// Optional submission port for proposing child tasks during execution.
    ///
    /// Set by the dispatch loop when the workflow feature is active.
    /// The handler calls `submission.submit(spec, deps)` to propose new tasks.
    pub submission: Option<Arc<dyn TaskSubmissionPort>>,
    /// Optional snapshot of child task states at dispatch time.
    ///
    /// Present when the task being dispatched has children in the hierarchy.
    pub children: Option<ChildrenSnapshot>,
    /// Optional externally-provided cancellation context.
    ///
    /// When set, the attempt runner uses this context instead of creating a
    /// new one. This allows the dispatch loop to retain a clone and signal
    /// cancellation (e.g. for budget exhaustion) while the handler runs.
    pub cancellation_context: Option<CancellationContext>,
}

/// Response from a handler after attempt execution.
///
/// This structure represents the deterministic outcome of an attempt,
/// including classification of timeout failures vs other failure types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorResponse {
    /// Execution completed successfully within the timeout.
    Success {
        /// Optional output data produced by the execution.
        output: Option<Vec<u8>>,
    },
    /// Execution failed due to timeout expiration.
    Timeout {
        /// Timeout duration in seconds that was exceeded.
        timeout_secs: u64,
    },
    /// Execution failed but may succeed on retry (transient failure).
    RetryableFailure {
        /// Error message describing the failure cause.
        error: String,
    },
    /// Execution was voluntarily suspended (e.g. the handler observed budget
    /// exhaustion via the cancellation token). The run transitions to Suspended
    /// state and this attempt does not count toward the max_attempts retry cap.
    Suspended {
        /// Optional partial-state bytes preserved for use when the run resumes.
        output: Option<Vec<u8>>,
    },
    /// Execution failed permanently and should not be retried.
    TerminalFailure {
        /// Error message describing the failure cause.
        error: String,
    },
}

impl ExecutorResponse {
    /// Returns true if this response indicates a terminal state (no retries).
    pub fn is_terminal(&self) -> bool {
        matches!(self, ExecutorResponse::Success { .. } | ExecutorResponse::TerminalFailure { .. })
    }

    /// Returns true if this response indicates a retryable outcome.
    ///
    /// Note: `Timeout` returns `false` here because retry eligibility for
    /// timed-out attempts is determined by [`crate::retry::decide_retry_transition`],
    /// which considers the attempt cap and backoff policy holistically.
    /// `Suspended` also returns `false` — suspension is not a retry.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ExecutorResponse::RetryableFailure { .. })
    }
}
