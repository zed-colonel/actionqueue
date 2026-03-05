//! Hard-cap retry boundary tests for local attempt execution.
//!
//! These tests verify the max-attempt cap invariance: no N+1 attempts are
//! permitted under any retry path.

use std::time::Duration;

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_executor_local::{
    can_retry, decide_retry_transition, AttemptOutcomeKind, AttemptRunner, AttemptTimer,
    ExecutorContext, ExecutorHandler, ExecutorRequest, HandlerOutput, RetryDecision,
    RetryDecisionError, RetryDecisionInput,
};

#[derive(Debug, Clone, Copy)]
struct FixedTimer {
    elapsed: Duration,
}

impl AttemptTimer for FixedTimer {
    type Mark = ();

    fn start(&self) -> Self::Mark {}

    fn elapsed_since(&self, _mark: Self::Mark) -> Duration {
        self.elapsed
    }
}

struct RetryableFailureHandler;

impl ExecutorHandler for RetryableFailureHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::RetryableFailure {
            error: "deterministic retryable failure for cap boundary tests".to_string(),
            consumption: vec![],
        }
    }
}

struct TerminalFailureHandler;

impl ExecutorHandler for TerminalFailureHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::TerminalFailure {
            error: "deterministic terminal failure for cap boundary tests".to_string(),
            consumption: vec![],
        }
    }
}

struct SuccessHandler;

impl ExecutorHandler for SuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: None, consumption: vec![] }
    }
}

fn make_request(
    run_id: RunId,
    attempt_id: AttemptId,
    attempt_number: u32,
    max_attempts: u32,
) -> ExecutorRequest {
    ExecutorRequest {
        run_id,
        attempt_id,
        payload: vec![],
        constraints: TaskConstraints::new(max_attempts, Some(60), None)
            .expect("test constraints should be valid"),
        attempt_number,
        submission: None,
        children: None,
        cancellation_context: None,
    }
}

fn make_retry_input(
    attempt_number: u32,
    max_attempts: u32,
    outcome_kind: AttemptOutcomeKind,
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
fn retry_helper_boundary_matrix_one_nominal_and_exhausted_values() {
    let boundary_one = make_retry_input(1, 1, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&boundary_one), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&boundary_one), Ok(false));

    let nominal_under_cap = make_retry_input(2, 3, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&nominal_under_cap), Ok(RetryDecision::Retry));
    assert_eq!(can_retry(&nominal_under_cap), Ok(true));

    let exhausted_at_cap = make_retry_input(3, 3, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&exhausted_at_cap), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&exhausted_at_cap), Ok(false));
}

#[test]
fn runner_never_emits_n_plus_one_retry_decision() {
    let run_id = RunId::new();
    let first_attempt = AttemptId::new();
    let second_attempt = AttemptId::new();
    let cap = 2;
    let runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        FixedTimer { elapsed: Duration::from_millis(4) },
    );

    let first_record = runner.run_attempt(make_request(run_id, first_attempt, 1, cap));
    assert_eq!(first_record.retry_decision, Ok(RetryDecision::Retry));

    let second_record = runner.run_attempt(make_request(run_id, second_attempt, 2, cap));
    assert_eq!(second_record.retry_decision, Ok(RetryDecision::Fail));

    assert!(matches!(first_record.retry_decision, Ok(RetryDecision::Retry)));
    assert!(matches!(second_record.retry_decision, Ok(RetryDecision::Fail)));
}

#[test]
fn max_attempts_one_boundary() {
    // With max_attempts=1, attempt 1 must fail (cannot retry)
    let input = make_retry_input(1, 1, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&input), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&input), Ok(false));
}

#[test]
fn max_attempts_two_boundary_retries_under_cap() {
    // With max_attempts=2, attempt 1 may retry (1 < 2)
    let under_cap = make_retry_input(1, 2, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&under_cap), Ok(RetryDecision::Retry));
    assert_eq!(can_retry(&under_cap), Ok(true));

    // Attempt 2 at cap must fail (2 == 2, not < 2)
    let at_cap = make_retry_input(2, 2, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&at_cap), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&at_cap), Ok(false));
}

#[test]
fn max_attempts_three_boundary_exhaustive() {
    // With max_attempts=3, attempts 1 and 2 may retry, attempt 3 must fail
    let attempt_1 = make_retry_input(1, 3, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&attempt_1), Ok(RetryDecision::Retry));
    assert_eq!(can_retry(&attempt_1), Ok(true));

    let attempt_2 = make_retry_input(2, 3, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&attempt_2), Ok(RetryDecision::Retry));
    assert_eq!(can_retry(&attempt_2), Ok(true));

    let attempt_3 = make_retry_input(3, 3, AttemptOutcomeKind::RetryableFailure);
    assert_eq!(decide_retry_transition(&attempt_3), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&attempt_3), Ok(false));
}

#[test]
fn success_always_completes_regardless_of_cap() {
    // Success at any attempt number should always complete
    let at_attempt_1 = make_retry_input(1, 3, AttemptOutcomeKind::Success);
    assert_eq!(decide_retry_transition(&at_attempt_1), Ok(RetryDecision::Complete));
    assert_eq!(can_retry(&at_attempt_1), Ok(false));

    let at_attempt_3 = make_retry_input(3, 3, AttemptOutcomeKind::Success);
    assert_eq!(decide_retry_transition(&at_attempt_3), Ok(RetryDecision::Complete));
    assert_eq!(can_retry(&at_attempt_3), Ok(false));
}

#[test]
fn terminal_failure_always_fails_regardless_of_cap() {
    // Terminal failure at any attempt number should always fail
    let at_attempt_1 = make_retry_input(1, 3, AttemptOutcomeKind::TerminalFailure);
    assert_eq!(decide_retry_transition(&at_attempt_1), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&at_attempt_1), Ok(false));

    let at_attempt_3 = make_retry_input(3, 3, AttemptOutcomeKind::TerminalFailure);
    assert_eq!(decide_retry_transition(&at_attempt_3), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&at_attempt_3), Ok(false));
}

#[test]
fn timeout_retries_under_cap_and_fails_at_cap() {
    // Timeout behaves like retryable failure for cap enforcement
    let under_cap = make_retry_input(1, 2, AttemptOutcomeKind::Timeout);
    assert_eq!(decide_retry_transition(&under_cap), Ok(RetryDecision::Retry));
    assert_eq!(can_retry(&under_cap), Ok(true));

    let at_cap = make_retry_input(2, 2, AttemptOutcomeKind::Timeout);
    assert_eq!(decide_retry_transition(&at_cap), Ok(RetryDecision::Fail));
    assert_eq!(can_retry(&at_cap), Ok(false));
}

#[test]
fn n_plus_one_attempt_path_returns_error() {
    // Attempt number exceeds cap - this is a hard invariant violation
    let input = make_retry_input(4, 3, AttemptOutcomeKind::RetryableFailure);

    assert_eq!(
        decide_retry_transition(&input),
        Err(RetryDecisionError::AttemptExceedsCap { attempt_number: 4, max_attempts: 3 })
    );
}

#[test]
fn runner_enforces_cap_with_terminal_failure() {
    // Even with terminal failure, no N+1 should occur
    let run_id = RunId::new();
    let attempt_1 = AttemptId::new();
    let attempt_2 = AttemptId::new();
    let cap = 2;
    let runner = AttemptRunner::with_timer(
        TerminalFailureHandler,
        FixedTimer { elapsed: Duration::from_millis(4) },
    );

    let record_1 = runner.run_attempt(make_request(run_id, attempt_1, 1, cap));
    assert_eq!(record_1.retry_decision, Ok(RetryDecision::Fail));
    assert_eq!(record_1.retry_decision_input.attempt_number, 1);

    // Attempt 2 would fail, but this is still within cap (2 <= 2)
    let record_2 = runner.run_attempt(make_request(run_id, attempt_2, 2, cap));
    assert_eq!(record_2.retry_decision, Ok(RetryDecision::Fail));
    assert_eq!(record_2.retry_decision_input.attempt_number, 2);

    // Attempt 3 would exceed cap (3 > 2)
    let attempt_3 = AttemptId::new();
    let record_3 = runner.run_attempt(make_request(run_id, attempt_3, 3, cap));
    assert_eq!(
        record_3.retry_decision,
        Err(RetryDecisionError::AttemptExceedsCap { attempt_number: 3, max_attempts: 2 })
    );
}

#[test]
fn runner_enforces_cap_with_success() {
    // Success terminates the run - no further attempts should be attempted
    let run_id = RunId::new();
    let attempt_1 = AttemptId::new();
    let cap = 2;
    let runner =
        AttemptRunner::with_timer(SuccessHandler, FixedTimer { elapsed: Duration::from_millis(4) });

    let record_1 = runner.run_attempt(make_request(run_id, attempt_1, 1, cap));
    assert_eq!(record_1.retry_decision, Ok(RetryDecision::Complete));
    assert_eq!(record_1.retry_decision_input.attempt_number, 1);

    // The run is complete, so no more attempts should be made
    // This test verifies the contract: even with max_attempts=2, the run
    // completes after attempt 1 succeeds
}

#[test]
fn all_retryable_failure_paths_respect_cap() {
    // Test all outcome kinds that could trigger retry under cap
    let outcome_kinds = [AttemptOutcomeKind::RetryableFailure, AttemptOutcomeKind::Timeout];

    for outcome in outcome_kinds {
        // At cap, should fail
        let at_cap = make_retry_input(3, 3, outcome);
        assert_eq!(
            decide_retry_transition(&at_cap),
            Ok(RetryDecision::Fail),
            "outcome {outcome:?} at cap should fail"
        );
        assert_eq!(
            can_retry(&at_cap),
            Ok(false),
            "outcome {outcome:?} at cap should not be retryable"
        );

        // Under cap, should retry
        let under_cap = make_retry_input(2, 3, outcome);
        assert_eq!(
            decide_retry_transition(&under_cap),
            Ok(RetryDecision::Retry),
            "outcome {outcome:?} under cap should retry"
        );
        assert_eq!(
            can_retry(&under_cap),
            Ok(true),
            "outcome {outcome:?} under cap should be retryable"
        );
    }
}

#[test]
fn attempt_runner_deterministic_output_at_boundary() {
    // Verify that the AttemptRunner produces deterministic outputs at boundary
    let run_id = RunId::new();
    let attempt_1 = AttemptId::new();
    let attempt_2 = AttemptId::new();
    let attempt_3 = AttemptId::new();
    let cap = 3;

    let runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        FixedTimer { elapsed: Duration::from_millis(4) },
    );

    let record_1 = runner.run_attempt(make_request(run_id, attempt_1, 1, cap));
    assert_eq!(record_1.retry_decision, Ok(RetryDecision::Retry));
    assert_eq!(record_1.retry_decision_input.max_attempts, 3);
    assert_eq!(record_1.retry_decision_input.attempt_number, 1);

    let record_2 = runner.run_attempt(make_request(run_id, attempt_2, 2, cap));
    assert_eq!(record_2.retry_decision, Ok(RetryDecision::Retry));
    assert_eq!(record_2.retry_decision_input.max_attempts, 3);
    assert_eq!(record_2.retry_decision_input.attempt_number, 2);

    let record_3 = runner.run_attempt(make_request(run_id, attempt_3, 3, cap));
    assert_eq!(record_3.retry_decision, Ok(RetryDecision::Fail));
    assert_eq!(record_3.retry_decision_input.max_attempts, 3);
    assert_eq!(record_3.retry_decision_input.attempt_number, 3);
}

#[test]
fn invalid_max_attempts_is_rejected() {
    // max_attempts=0 is invalid
    let input = make_retry_input(1, 0, AttemptOutcomeKind::RetryableFailure);

    assert_eq!(
        decide_retry_transition(&input),
        Err(RetryDecisionError::InvalidMaxAttempts { max_attempts: 0 })
    );
}

#[test]
fn invalid_attempt_number_is_rejected() {
    // attempt_number=0 is invalid
    let input = make_retry_input(0, 1, AttemptOutcomeKind::RetryableFailure);

    assert_eq!(
        decide_retry_transition(&input),
        Err(RetryDecisionError::InvalidAttemptNumber { attempt_number: 0 })
    );
}

#[test]
fn can_retry_matches_decide_retry_transition() {
    // Verify that can_retry returns true iff decide_retry_transition returns Retry
    let test_cases = [
        (1, 1, AttemptOutcomeKind::RetryableFailure, false),
        (1, 2, AttemptOutcomeKind::RetryableFailure, true),
        (2, 2, AttemptOutcomeKind::RetryableFailure, false),
        (1, 3, AttemptOutcomeKind::Timeout, true),
        (3, 3, AttemptOutcomeKind::Timeout, false),
        (1, 3, AttemptOutcomeKind::Success, false),
        (1, 3, AttemptOutcomeKind::TerminalFailure, false),
        (2, 3, AttemptOutcomeKind::TerminalFailure, false),
    ];

    for (attempt_num, max, outcome, expected_can_retry) in test_cases {
        let input = make_retry_input(attempt_num, max, outcome);

        let decision = decide_retry_transition(&input).expect("decision should succeed");
        let can_retry_result = can_retry(&input).expect("can_retry should succeed");

        // can_retry returns true only if decision is Retry
        let expected_decision = if expected_can_retry {
            RetryDecision::Retry
        } else if outcome == AttemptOutcomeKind::Success {
            RetryDecision::Complete
        } else {
            RetryDecision::Fail
        };

        assert_eq!(decision, expected_decision, "mismatch for attempt {attempt_num}/{max}");
        assert_eq!(
            can_retry_result, expected_can_retry,
            "can_retry mismatch for attempt {attempt_num}/{max}"
        );
    }
}

#[test]
fn cap_boundary_consistency_across_various_outcomes() {
    // Verify consistent cap boundary behavior across different outcome kinds
    let outcomes = [
        AttemptOutcomeKind::Success,
        AttemptOutcomeKind::RetryableFailure,
        AttemptOutcomeKind::TerminalFailure,
        AttemptOutcomeKind::Timeout,
    ];

    for outcome in outcomes {
        // At cap (attempt 3 of 3)
        let at_cap = make_retry_input(3, 3, outcome);
        let decision = decide_retry_transition(&at_cap).expect("decision should succeed");

        match outcome {
            AttemptOutcomeKind::Success => {
                assert_eq!(decision, RetryDecision::Complete);
                assert_eq!(can_retry(&at_cap), Ok(false));
            }
            AttemptOutcomeKind::RetryableFailure | AttemptOutcomeKind::Timeout => {
                assert_eq!(decision, RetryDecision::Fail);
                assert_eq!(can_retry(&at_cap), Ok(false));
            }
            AttemptOutcomeKind::TerminalFailure => {
                assert_eq!(decision, RetryDecision::Fail);
                assert_eq!(can_retry(&at_cap), Ok(false));
            }
            // Suspended bypasses cap validation — tested separately.
            AttemptOutcomeKind::Suspended => {}
        }
    }
}
