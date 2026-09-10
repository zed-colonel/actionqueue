//! State transition table for run instances.
//!
//! This module defines the valid transitions between run states and
//! enforces the invariant that backward transitions are forbidden.

use crate::run::state::RunState;

/// Typed errors for lifecycle transition construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTransitionError {
    /// The `(from -> to)` transition is not allowed by the canonical transition table.
    InvalidTransition {
        /// Source lifecycle state.
        from: RunState,
        /// Target lifecycle state.
        to: RunState,
        /// Why the transition was rejected.
        reason: RunTransitionRejection,
    },
}

impl std::fmt::Display for RunTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition { from, to, reason } => {
                write!(f, "invalid run transition: {from:?} -> {to:?}: {reason:?}")
            }
        }
    }
}

impl std::error::Error for RunTransitionError {}

/// A transition represents a valid state change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct Transition {
    from: RunState,
    to: RunState,
}

impl Transition {
    /// Creates a new transition from `from` to `to`.
    /// Returns a typed error if the transition is not valid.
    pub fn new(from: RunState, to: RunState) -> Result<Self, RunTransitionError> {
        match transition_rejection(from, to) {
            None => Ok(Transition { from, to }),
            Some(reason) => Err(RunTransitionError::InvalidTransition { from, to, reason }),
        }
    }

    /// Returns the source state of this transition.
    pub fn from(&self) -> RunState {
        self.from
    }

    /// Returns the target state of this transition.
    pub fn to(&self) -> RunState {
        self.to
    }
}

/// Checks if a transition from `from` to `to` is valid.
///
/// Valid transitions:
/// - Scheduled -> Ready
/// - Scheduled -> Canceled
/// - Ready -> Leased
/// - Ready -> Canceled
/// - Leased -> Running
/// - Leased -> Ready (lease expired)
/// - Leased -> Canceled
/// - Running -> RetryWait (failure with retries remaining)
/// - Running -> Suspended (preempted by budget exhaustion)
/// - Running -> Completed (success)
/// - Running -> Failed (failure, no retries remaining)
/// - Running -> Canceled
/// - RetryWait -> Ready (backoff complete)
/// - RetryWait -> Failed (no more retries)
/// - RetryWait -> Canceled
/// - Suspended -> Ready (budget replenished / explicit resume)
/// - Suspended -> Canceled
/// - Running -> Awaiting (continuation registered)
/// - Awaiting -> Ready (continuation resolved)
/// - Awaiting -> Failed (deadline policy)
/// - Awaiting -> Canceled
///
/// Terminal states (Completed, Failed, Canceled) have no valid transitions.
pub fn is_valid_transition(from: RunState, to: RunState) -> bool {
    transition_rejection(from, to).is_none()
}

/// Classifies invalid transitions, with terminal finality taking precedence.
pub fn transition_rejection(from: RunState, to: RunState) -> Option<RunTransitionRejection> {
    // Terminal states cannot transition to any other state
    if from.is_terminal() {
        return Some(RunTransitionRejection::TerminalStateIsFinal);
    }

    if from == RunState::Awaiting
        && !matches!(to, RunState::Ready | RunState::Failed | RunState::Canceled)
    {
        return Some(RunTransitionRejection::AwaitingResolvesOnlyToReadyFailedCanceled);
    }
    if to == RunState::Awaiting && from != RunState::Running {
        return Some(RunTransitionRejection::AwaitingRequiresRunning);
    }
    let valid = matches!(
        (from, to),
        (RunState::Scheduled, RunState::Ready)
            | (RunState::Scheduled, RunState::Canceled)
            | (RunState::Ready, RunState::Leased)
            | (RunState::Ready, RunState::Canceled)
            | (RunState::Leased, RunState::Running)
            | (RunState::Leased, RunState::Ready)
            | (RunState::Leased, RunState::Canceled)
            | (RunState::Running, RunState::RetryWait)
            | (RunState::Running, RunState::Suspended)
            | (RunState::Running, RunState::Completed)
            | (RunState::Running, RunState::Failed)
            | (RunState::Running, RunState::Canceled)
            | (RunState::RetryWait, RunState::Ready)
            | (RunState::RetryWait, RunState::Failed)
            | (RunState::RetryWait, RunState::Canceled)
            | (RunState::Suspended, RunState::Ready)
            | (RunState::Suspended, RunState::Canceled)
            | (RunState::Running, RunState::Awaiting)
            | (RunState::Awaiting, RunState::Ready)
            | (RunState::Awaiting, RunState::Failed)
            | (RunState::Awaiting, RunState::Canceled)
    );
    if valid {
        None
    } else {
        Some(RunTransitionRejection::NotInTransitionTable)
    }
}

/// Returns all valid transitions from a given state.
pub fn valid_transitions(from: RunState) -> Vec<RunState> {
    let states = [
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

    states.into_iter().filter(|&to| is_valid_transition(from, to)).collect()
}

/// Reason a lifecycle edge is forbidden.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTransitionRejection {
    /// Completed, Failed and Canceled are final.
    TerminalStateIsFinal,
    /// Only Running can register a continuation.
    AwaitingRequiresRunning,
    /// A continuation resumes through Ready, or ends in Failed/Canceled.
    AwaitingResolvesOnlyToReadyFailedCanceled,
    /// No such edge exists in the canonical table.
    NotInTransitionTable,
}
