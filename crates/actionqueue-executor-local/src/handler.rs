//! Executor handler trait and related types for attempt-level execution.
//!
//! This module defines the [`ExecutorHandler`] trait that implementations must
//! fulfill to execute attempts. The handler receives an [`ExecutorContext`]
//! containing execution identity, payload, metadata, and optional workflow

pub mod cancellation;

pub use actionqueue_core::disposition::AttemptDisposition;
use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::safety::SafetyLevel;
pub use cancellation::{CancellationContext, CancellationToken};

use crate::children::ChildrenSnapshot;

/// Execution context provided to the handler for each attempt.
///
/// This structure contains all information needed to execute an attempt
/// including the run and attempt identifiers, the payload to execute,
/// and constraints snapshot for timeout and retry behavior.
///
/// # Cancellation contract
///
/// - If `metadata.timeout_secs` is `Some`, timeout enforcement may request
///   cancellation while execution is in progress.
/// - Long-running handlers must poll [`CancellationToken::is_cancelled()`]
///   at bounded intervals and exit promptly once cancellation is observed.
/// - Returning success after cancellation has been requested does not override
///   timeout truth; timeout classification remains authoritative.
#[derive(Debug, Clone)]
pub struct HandlerInput {
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
    /// The attempt-level execution metadata (timeout, retry policy, etc.).
    pub metadata: AttemptMetadata,
    /// The cancellation context for this execution. Handlers can check this
    /// to determine if execution should be terminated early.
    pub cancellation_context: CancellationContext,
}

/// Attempt-level execution metadata derived from task constraints.
#[derive(Debug, Clone)]
pub struct AttemptMetadata {
    /// Committed failures before this execution; yields do not spend retry allowance.
    pub failure_attempt_count: u32,
    /// Maximum number of attempts allowed for this run.
    pub max_attempts: u32,
    /// Current attempt number (1-indexed).
    pub attempt_number: u32,
    /// Execution timeout in seconds. If `None`, no timeout.
    pub timeout_secs: Option<u64>,
    /// Safety level classification of the task.
    pub safety_level: SafetyLevel,
}

/// Full execution context provided to a handler for each attempt.
///
/// Extends [`HandlerInput`] with optional workflow extensions:
/// - [`children`](Self::children): snapshot of child task states
///
/// # Cancellation contract
///
/// See [`HandlerInput`] for the timeout and cancellation contract.
pub struct ExecutorContext {
    /// Core execution input: identifiers, payload, metadata, cancellation.
    pub input: HandlerInput,
    /// Optional snapshot of child task states, taken at dispatch time.
    ///
    /// Present when the dispatched task has children in the hierarchy tracker.
    /// Coordinator handlers use this to check progress and decide what to submit.
    pub children: Option<ChildrenSnapshot>,
}

impl std::fmt::Debug for ExecutorContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorContext")
            .field("input", &self.input)
            .field("children", &self.children)
            .finish()
    }
}

/// Trait for executor implementations to fulfill for attempt execution.
///
/// The handler is invoked for each attempt with full context including
/// `RunId` and `AttemptId` for traceability. Implementations must return
/// a typed [`AttemptDisposition`] that indicates whether the attempt succeeded,
/// should be retried, or should be marked as a terminal failure.
///
/// # Invariants
///
/// - Handlers must not mutate attempt-counting or run-derivation accounting.
/// - Handlers must use the `RunId` and `AttemptId` from [`HandlerInput`] for
///   all logging and reporting.
/// - Long-running work must cooperate with timeout enforcement by polling
///   [`CancellationToken::is_cancelled()`] at a bounded cadence.
pub trait ExecutorHandler: Send + Sync {
    /// Executes the attempt with the provided context and returns the outcome.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The full execution context, including run/attempt identifiers,
    ///   payload, metadata, and optional workflow extensions.
    ///
    /// # Returns
    ///
    /// The complete outcome and bounded effects to commit atomically. Use
    /// [`AttemptDisposition::awaiting`] for a durable continuation.
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition;
}
