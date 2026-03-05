//! Property-based tests for retry cap enforcement.

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_executor_local::attempt_runner::{AttemptOutcomeKind, RetryDecisionInput};
use actionqueue_executor_local::retry::{can_retry, decide_retry_transition, RetryDecision};
use proptest::prelude::*;

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

fn config() -> ProptestConfig {
    ProptestConfig::with_cases(256)
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn attempt_exceeding_max_cannot_retry(
        max_attempts in 1u32..100,
        extra in 1u32..100,
    ) {
        let attempt_number = max_attempts + extra;
        let input = make_input(
            AttemptOutcomeKind::RetryableFailure,
            attempt_number,
            max_attempts,
        );
        // attempt_number > max_attempts is rejected by validation
        let result = can_retry(&input);
        prop_assert!(result.is_err(), "attempt_number > max_attempts must be rejected");
    }

    #[test]
    fn attempt_within_cap_can_retry_on_retryable_failure(
        max_attempts in 2u32..100,
        attempt_number in 1u32..100,
    ) {
        // Only test when attempt_number < max_attempts (strictly under cap)
        prop_assume!(attempt_number < max_attempts);

        let input = make_input(
            AttemptOutcomeKind::RetryableFailure,
            attempt_number,
            max_attempts,
        );

        let result = can_retry(&input).expect("validation must succeed");
        prop_assert!(result, "retryable failure under cap must allow retry");
    }

    #[test]
    fn at_cap_with_retryable_failure_cannot_retry(
        max_attempts in 1u32..100,
    ) {
        let input = make_input(
            AttemptOutcomeKind::RetryableFailure,
            max_attempts,
            max_attempts,
        );

        let decision = decide_retry_transition(&input)
            .expect("validation must succeed");
        prop_assert_eq!(
            decision,
            RetryDecision::Fail,
            "at-cap retryable failure must result in Fail"
        );

        let can = can_retry(&input).expect("validation must succeed");
        prop_assert!(!can, "at-cap retryable failure must not allow retry");
    }
}
