//! Executor request and response contracts for attempt-level execution.
//!
//! This module defines the contract messages exchanged between the executor
//! system and handler implementations. These contracts ensure deterministic
//! attempt outcomes and explicit timeout classification.

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::constraints::TaskConstraints;

use crate::children::ChildrenSnapshot;
use crate::handler::cancellation::CancellationContext;

/// Executor request sent to a handler for attempt execution.
///
/// This structure carries all information needed for a handler to execute
/// an attempt, including the captured lease fence and optional child snapshot.
pub struct ExecutorRequest {
    /// Durable committed failures before this physical attempt.
    pub failure_attempt_count: u32,
    /// Lease fence captured at the accepted attempt start.
    pub lease_fence: actionqueue_core::mutation::LeaseFence,
    /// Durable continuation assigned to this physical attempt.
    pub resume_context: Option<actionqueue_core::continuation::ResumeContext>,
    /// Original immutable admission attribution, absent for legacy task creation.
    pub causal_context: Option<actionqueue_core::causal::CausalContext>,
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
