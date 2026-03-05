//! Run state definitions for the task execution lifecycle.

/// Canonical states in the run lifecycle.
///
/// States progress forward through: Scheduled -> Ready -> Leased -> Running -> (RetryWait -> Ready)* or -> Terminal
/// A running attempt may also be preempted to Suspended (e.g. by budget exhaustion), then
/// resumed back to Ready when capacity is restored.
/// Cancellation is also allowed from Scheduled, Ready, Leased, Running, RetryWait, and Suspended -> Canceled.
/// Terminal states (Completed, Failed, Canceled) are immutable and cannot transition to any other state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RunState {
    /// The run is scheduled and waiting to become ready.
    /// Transitions: -> Ready (when scheduled_at has passed), -> Canceled
    Scheduled,

    /// The run is ready to be leased/ picked up by an executor.
    /// Transitions: -> Leased (when leased), -> Canceled
    Ready,

    /// The run has been leased to an executor for processing.
    /// Transitions: -> Running (when execution starts), -> Ready (lease expired), -> Canceled
    Leased,

    /// The run is currently being executed.
    /// Transitions: -> RetryWait (on failure, if retries remain), -> Suspended (preempted),
    /// -> Completed (on success), -> Failed (on failure, no retries), -> Canceled
    Running,

    /// The run failed and is waiting before retry.
    /// Transitions: -> Ready (when backoff completes), -> Failed (if no more retries remain), -> Canceled
    RetryWait,

    /// The run has been preempted (e.g. budget exhaustion) and is waiting for resumption.
    /// Non-terminal — will resume to Ready when capacity is restored.
    /// Suspended attempts do not count toward the max_attempts retry cap.
    /// Transitions: -> Ready (when budget replenished / explicit resume), -> Canceled
    Suspended,

    /// The run completed successfully.
    /// Terminal state - no further transitions allowed.
    Completed,

    /// The run failed after all retries were exhausted.
    /// Terminal state - no further transitions allowed.
    Failed,

    /// The run was canceled.
    /// Terminal state - no further transitions allowed.
    Canceled,
}

impl RunState {
    /// Returns true if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, RunState::Completed | RunState::Failed | RunState::Canceled)
    }
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            RunState::Scheduled => "scheduled",
            RunState::Ready => "ready",
            RunState::Leased => "leased",
            RunState::Running => "running",
            RunState::RetryWait => "retry_wait",
            RunState::Suspended => "suspended",
            RunState::Completed => "completed",
            RunState::Failed => "failed",
            RunState::Canceled => "canceled",
        };
        write!(f, "{name}")
    }
}
