//! Run state definitions for the task execution lifecycle.

/// Canonical states in the run lifecycle.
///
/// States progress forward through: Scheduled -> Ready -> Leased -> Running -> (RetryWait -> Ready)* or -> Terminal
/// A running attempt may also be preempted to Suspended (e.g. by budget exhaustion), then
/// resumed back to Ready when capacity is restored.
/// Running may yield to Awaiting, which resolves through Ready, Failed, or Canceled.
/// Cancellation is allowed from every non-terminal state.
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
    /// -> Awaiting (continuation), -> Completed (on success), -> Failed (no retries), -> Canceled
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

    /// Waiting for a durable continuation. Non-terminal; resolves to Ready, Failed, or Canceled.
    /// Appended to preserve WAL v5 postcard discriminants until AQ-03.
    Awaiting,
}

impl RunState {
    /// Every run state, in declaration order.
    ///
    /// Observability surfaces derive their bounded label sets and per-state
    /// counters from this list so that adding a state cannot leave one behind.
    pub const ALL: [RunState; 10] = [
        RunState::Scheduled,
        RunState::Ready,
        RunState::Leased,
        RunState::Running,
        RunState::RetryWait,
        RunState::Suspended,
        RunState::Completed,
        RunState::Failed,
        RunState::Canceled,
        RunState::Awaiting,
    ];

    /// Returns true if this is a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, RunState::Completed | RunState::Failed | RunState::Canceled)
    }

    /// Returns the stable snake_case label used by metrics, stats, and display.
    pub const fn label(self) -> &'static str {
        match self {
            RunState::Scheduled => "scheduled",
            RunState::Ready => "ready",
            RunState::Leased => "leased",
            RunState::Running => "running",
            RunState::RetryWait => "retry_wait",
            RunState::Suspended => "suspended",
            RunState::Completed => "completed",
            RunState::Failed => "failed",
            RunState::Canceled => "canceled",
            RunState::Awaiting => "awaiting",
        }
    }
}

impl std::fmt::Display for RunState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}
