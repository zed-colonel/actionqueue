//! Retry transition helper for attempt outcomes.
//!
//! This module deterministically maps a completed attempt outcome to the next
//! run-state intent while enforcing the `max_attempts` hard cap.

use std::error::Error;
use std::fmt;

use actionqueue_core::run::state::RunState;

use crate::attempt_runner::{AttemptOutcomeKind, RetryDecisionInput};

/// Error returned when retry decision inputs violate retry invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryDecisionError {
    /// `max_attempts` is invalid and cannot be used for cap enforcement.
    InvalidMaxAttempts {
        /// Invalid configured attempt cap.
        max_attempts: u32,
    },
    /// `attempt_number` is invalid and cannot represent a completed attempt.
    InvalidAttemptNumber {
        /// Invalid attempt number.
        attempt_number: u32,
    },
    /// A completed attempt number exceeded the configured hard cap.
    ///
    /// This explicitly prevents any `N + 1` retry path.
    AttemptExceedsCap {
        /// Completed attempt number.
        attempt_number: u32,
        /// Configured hard attempt cap.
        max_attempts: u32,
    },
}

impl fmt::Display for RetryDecisionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMaxAttempts { max_attempts } => {
                write!(f, "invalid max_attempts value ({max_attempts}); expected >= 1")
            }
            Self::InvalidAttemptNumber { attempt_number } => {
                write!(f, "invalid attempt_number value ({attempt_number}); expected >= 1")
            }
            Self::AttemptExceedsCap { attempt_number, max_attempts } => {
                write!(f, "attempt_number ({attempt_number}) exceeds max_attempts ({max_attempts})",)
            }
        }
    }
}

impl Error for RetryDecisionError {}

/// Next run-state intent derived from one completed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum RetryDecision {
    /// Transition to `Completed`.
    Complete,
    /// Transition to `RetryWait`.
    Retry,
    /// Transition to `Failed`.
    Fail,
    /// Transition to `Suspended`. Does not count toward the max_attempts cap.
    Suspend,
}

impl RetryDecision {
    /// Returns the canonical target run state for this decision.
    pub fn target_state(self) -> RunState {
        match self {
            Self::Complete => RunState::Completed,
            Self::Retry => RunState::RetryWait,
            Self::Fail => RunState::Failed,
            Self::Suspend => RunState::Suspended,
        }
    }
}

/// Computes retry transition intent from attempt outcome and counters.
///
/// # Invariants
///
/// - `max_attempts` must be at least 1.
/// - `attempt_number` must be at least 1.
/// - `attempt_number` must not exceed `max_attempts` (except for Suspended outcomes,
///   which bypass cap validation and always return `Suspend`).
/// - Retryable outcomes only produce `Retry` when `attempt_number < max_attempts`.
pub fn decide_retry_transition(
    input: &RetryDecisionInput,
) -> Result<RetryDecision, RetryDecisionError> {
    // Suspended bypasses cap validation. Suspended attempts do not count toward
    // max_attempts. The dispatch loop tracks effective attempt count separately.
    if input.outcome_kind == AttemptOutcomeKind::Suspended {
        return Ok(RetryDecision::Suspend);
    }

    validate_retry_input(input)?;

    let decision = match input.outcome_kind {
        AttemptOutcomeKind::Success => RetryDecision::Complete,
        AttemptOutcomeKind::TerminalFailure => RetryDecision::Fail,
        AttemptOutcomeKind::RetryableFailure | AttemptOutcomeKind::Timeout => {
            if input.attempt_number < input.max_attempts {
                RetryDecision::Retry
            } else {
                RetryDecision::Fail
            }
        }
        // Handled above — unreachable here but exhaustive match required.
        AttemptOutcomeKind::Suspended => RetryDecision::Suspend,
    };

    Ok(decision)
}

/// Returns whether another attempt may be scheduled from this outcome.
pub fn can_retry(input: &RetryDecisionInput) -> Result<bool, RetryDecisionError> {
    Ok(matches!(decide_retry_transition(input)?, RetryDecision::Retry))
}

fn validate_retry_input(input: &RetryDecisionInput) -> Result<(), RetryDecisionError> {
    if input.max_attempts == 0 {
        return Err(RetryDecisionError::InvalidMaxAttempts { max_attempts: input.max_attempts });
    }

    if input.attempt_number == 0 {
        return Err(RetryDecisionError::InvalidAttemptNumber {
            attempt_number: input.attempt_number,
        });
    }

    if input.attempt_number > input.max_attempts {
        return Err(RetryDecisionError::AttemptExceedsCap {
            attempt_number: input.attempt_number,
            max_attempts: input.max_attempts,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{AttemptId, RunId};
    use actionqueue_core::run::state::RunState;

    use super::{can_retry, decide_retry_transition, RetryDecision, RetryDecisionError};
    use crate::attempt_runner::{AttemptOutcomeKind, RetryDecisionInput};

    fn make_input(
        outcome_kind: AttemptOutcomeKind,
        attempt_number: u32,
        max_attempts: u32,
    ) -> RetryDecisionInput {
        RetryDecisionInput {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            attempt_number,
            max_attempts,
            outcome_kind,
        }
    }

    #[test]
    fn success_always_completes() {
        let input = make_input(AttemptOutcomeKind::Success, 1, 3);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Complete);
        assert_eq!(decision.target_state(), RunState::Completed);
        assert!(!can_retry(&input).expect("validation should pass"));
    }

    #[test]
    fn terminal_failure_always_fails() {
        let input = make_input(AttemptOutcomeKind::TerminalFailure, 2, 5);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Fail);
        assert_eq!(decision.target_state(), RunState::Failed);
        assert!(!can_retry(&input).expect("validation should pass"));
    }

    #[test]
    fn retryable_failure_retries_under_cap() {
        let input = make_input(AttemptOutcomeKind::RetryableFailure, 2, 3);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Retry);
        assert_eq!(decision.target_state(), RunState::RetryWait);
        assert!(can_retry(&input).expect("validation should pass"));
    }

    #[test]
    fn retryable_failure_fails_at_cap_without_n_plus_one() {
        let input = make_input(AttemptOutcomeKind::RetryableFailure, 3, 3);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Fail);
        assert!(!can_retry(&input).expect("validation should pass"));
    }

    #[test]
    fn timeout_retries_under_cap_and_fails_at_cap() {
        let under_cap = make_input(AttemptOutcomeKind::Timeout, 1, 2);
        let at_cap = make_input(AttemptOutcomeKind::Timeout, 2, 2);

        assert_eq!(
            decide_retry_transition(&under_cap).expect("decision should succeed"),
            RetryDecision::Retry
        );
        assert_eq!(
            decide_retry_transition(&at_cap).expect("decision should succeed"),
            RetryDecision::Fail
        );
    }

    #[test]
    fn invalid_max_attempts_is_rejected() {
        let input = make_input(AttemptOutcomeKind::RetryableFailure, 1, 0);

        assert_eq!(
            decide_retry_transition(&input),
            Err(RetryDecisionError::InvalidMaxAttempts { max_attempts: 0 })
        );
    }

    #[test]
    fn invalid_attempt_number_is_rejected() {
        let input = make_input(AttemptOutcomeKind::RetryableFailure, 0, 1);

        assert_eq!(
            decide_retry_transition(&input),
            Err(RetryDecisionError::InvalidAttemptNumber { attempt_number: 0 })
        );
    }

    #[test]
    fn n_plus_one_attempt_path_is_rejected() {
        let input = make_input(AttemptOutcomeKind::RetryableFailure, 4, 3);

        assert_eq!(
            decide_retry_transition(&input),
            Err(RetryDecisionError::AttemptExceedsCap { attempt_number: 4, max_attempts: 3 })
        );
    }

    #[test]
    fn suspended_always_suspends() {
        let input = make_input(AttemptOutcomeKind::Suspended, 1, 3);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Suspend);
        assert_eq!(decision.target_state(), RunState::Suspended);
        assert!(!can_retry(&input).expect("validation should pass"));
    }

    #[test]
    fn suspended_bypasses_cap_validation() {
        // attempt_number > max_attempts would normally be rejected, but
        // Suspended bypasses cap validation entirely.
        let input = make_input(AttemptOutcomeKind::Suspended, 5, 3);
        let decision = decide_retry_transition(&input).expect("decision should succeed");

        assert_eq!(decision, RetryDecision::Suspend);
    }
}
