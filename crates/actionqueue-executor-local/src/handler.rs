//! Executor handler trait and related types for attempt-level execution.
//!
//! This module defines the [`ExecutorHandler`] trait that implementations must
//! fulfill to execute attempts. The handler receives an [`ExecutorContext`]
//! containing execution identity, payload, metadata, and optional workflow
//! extensions (submission channel and children snapshot).

pub mod cancellation;

use actionqueue_core::budget::BudgetConsumption;
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
    /// Maximum number of attempts allowed for this run.
    pub max_attempts: u32,
    /// Current attempt number (1-indexed).
    pub attempt_number: u32,
    /// Execution timeout in seconds. If `None`, no timeout.
    pub timeout_secs: Option<u64>,
    /// Safety level classification of the task.
    pub safety_level: SafetyLevel,
}

/// Outcome of an attempt execution.
///
/// The handler return type uses typed variants to distinguish between
/// successful completion, retryable failures (transient errors that may
/// succeed on retry), and terminal failures (permanent errors that should
/// not be retried). Handlers may also return `Suspended` to signal that
/// execution was voluntarily preempted (e.g. in response to budget exhaustion).
///
/// All variants carry a `consumption` list: zero or more [`BudgetConsumption`]
/// records reporting the resources consumed during this attempt. The dispatch
/// loop durably records these after each attempt completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerOutput {
    /// Execution completed successfully.
    Success {
        /// Optional output data produced by the execution.
        output: Option<Vec<u8>>,
        /// Resource consumption incurred during this attempt.
        consumption: Vec<BudgetConsumption>,
    },
    /// Execution failed but may succeed on retry (transient failure).
    RetryableFailure {
        /// Error message describing the failure cause.
        error: String,
        /// Resource consumption incurred before the failure.
        consumption: Vec<BudgetConsumption>,
    },
    /// Execution failed permanently and should not be retried.
    TerminalFailure {
        /// Error message describing the failure cause.
        error: String,
        /// Resource consumption incurred before the failure.
        consumption: Vec<BudgetConsumption>,
    },
    /// Execution was voluntarily suspended (e.g. budget exhaustion signalled via
    /// the cancellation token). The run transitions to `Suspended` and does not
    /// count this attempt toward the `max_attempts` retry cap.
    Suspended {
        /// Optional partial-state bytes the handler may persist for use on resume.
        output: Option<Vec<u8>>,
        /// Resource consumption incurred before suspension.
        consumption: Vec<BudgetConsumption>,
    },
}

impl HandlerOutput {
    /// Creates a success output with no output bytes and no consumption.
    pub fn success() -> Self {
        Self::Success { output: None, consumption: vec![] }
    }

    /// Creates a success output with output bytes and no consumption.
    pub fn success_with_output(output: Vec<u8>) -> Self {
        Self::Success { output: Some(output), consumption: vec![] }
    }

    /// Creates a retryable failure with no consumption.
    pub fn retryable_failure(error: impl Into<String>) -> Self {
        Self::RetryableFailure { error: error.into(), consumption: vec![] }
    }

    /// Creates a terminal failure with no consumption.
    pub fn terminal_failure(error: impl Into<String>) -> Self {
        Self::TerminalFailure { error: error.into(), consumption: vec![] }
    }

    /// Creates a suspended outcome with no partial state and no consumption.
    pub fn suspended() -> Self {
        Self::Suspended { output: None, consumption: vec![] }
    }

    /// Creates a suspended outcome with partial-state bytes and consumption.
    pub fn suspended_with_output(output: Vec<u8>, consumption: Vec<BudgetConsumption>) -> Self {
        Self::Suspended { output: Some(output), consumption }
    }

    /// Returns the resource consumption reported by this handler output.
    pub fn consumption(&self) -> &[BudgetConsumption] {
        match self {
            Self::Success { consumption, .. }
            | Self::RetryableFailure { consumption, .. }
            | Self::TerminalFailure { consumption, .. }
            | Self::Suspended { consumption, .. } => consumption,
        }
    }
}

/// Abstract port through which a handler can propose new child tasks.
///
/// The concrete implementation (backed by a tokio mpsc channel) lives in
/// `actionqueue-workflow` and is injected by the dispatch loop. Using a trait
/// object keeps `actionqueue-executor-local` free of tokio dependencies.
///
/// Submissions are fire-and-forget: if the channel is closed (e.g., the
/// dispatch loop shut down), the submission is silently dropped. Handlers
/// have no error path for submission failures.
pub trait TaskSubmissionPort: Send + Sync {
    /// Proposes a new task for creation.
    ///
    /// The dispatch loop validates and WAL-appends the task on the next tick.
    /// The submitted `task_spec` should have `parent_task_id` set to associate
    /// it with the Coordinator's task.
    ///
    /// `dependencies` is the list of `TaskId`s that must reach terminal success
    /// before this task's runs are promoted to Ready.
    fn submit(
        &self,
        task_spec: actionqueue_core::task::task_spec::TaskSpec,
        dependencies: Vec<actionqueue_core::ids::TaskId>,
    );
}

/// Full execution context provided to a handler for each attempt.
///
/// Extends [`HandlerInput`] with optional workflow extensions:
/// - [`submission`](Self::submission): channel for proposing child tasks
/// - [`children`](Self::children): snapshot of child task states
///
/// # Cancellation contract
///
/// See [`HandlerInput`] for the timeout and cancellation contract.
pub struct ExecutorContext {
    /// Core execution input: identifiers, payload, metadata, cancellation.
    pub input: HandlerInput,
    /// Optional channel for proposing new child tasks during execution.
    ///
    /// Present when the workflow feature is active. Handlers set
    /// `parent_task_id` on submitted `TaskSpec`s to establish hierarchy.
    pub submission: Option<std::sync::Arc<dyn TaskSubmissionPort>>,
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
            .field("submission", &self.submission.as_ref().map(|_| "<TaskSubmissionPort>"))
            .field("children", &self.children)
            .finish()
    }
}

/// Trait for executor implementations to fulfill for attempt execution.
///
/// The handler is invoked for each attempt with full context including
/// `RunId` and `AttemptId` for traceability. Implementations must return
/// a typed [`HandlerOutput`] that indicates whether the attempt succeeded,
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
    /// A [`HandlerOutput`] that indicates the attempt outcome:
    /// - [`HandlerOutput::Success`] if execution completed successfully
    /// - [`HandlerOutput::RetryableFailure`] if execution failed but may succeed on retry
    /// - [`HandlerOutput::TerminalFailure`] if execution failed permanently
    /// - [`HandlerOutput::Suspended`] if execution was voluntarily preempted
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput;
}
