//! Hard-cap retry boundary tests for local attempt execution.
//!
//! The runner reports one disposition per physical attempt and decides no run
//! state. The durable authority accounts for that disposition through
//! `DispositionOutcome::accounting`, which is the only retry-cap policy; these
//! tests prove the two compose without an `N + 1` path.

use std::time::Duration;

use actionqueue_core::disposition::DispositionAccountingError;
use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::run::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_executor_local::{
    AttemptDisposition, AttemptOutcomeRecord, AttemptRunner, AttemptTimer, ExecutorContext,
    ExecutorHandler, ExecutorRequest,
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
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let _input = ctx.input;
        actionqueue_core::disposition::AttemptDisposition::retryable_failure(
            actionqueue_core::bounded::BoundedError::new(
                "deterministic retryable failure for cap boundary tests".to_string(),
            )
            .unwrap(),
        )
    }
}

struct TerminalFailureHandler;

impl ExecutorHandler for TerminalFailureHandler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let _input = ctx.input;
        actionqueue_core::disposition::AttemptDisposition::terminal_failure(
            actionqueue_core::bounded::BoundedError::new(
                "deterministic terminal failure for cap boundary tests".to_string(),
            )
            .unwrap(),
        )
    }
}

struct SuccessHandler;

impl ExecutorHandler for SuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        let _input = ctx.input;
        actionqueue_core::disposition::AttemptDisposition::complete(None)
    }
}

fn make_request(
    run_id: RunId,
    attempt_id: AttemptId,
    attempt_number: u32,
    max_attempts: u32,
) -> ExecutorRequest {
    ExecutorRequest {
        lease_fence: actionqueue_core::mutation::LeaseFence::new("test".into(), 1),
        failure_attempt_count: attempt_number.saturating_sub(1),
        resume_context: None,
        causal_context: None,
        run_id,
        attempt_id,
        payload: vec![],
        constraints: TaskConstraints::new(max_attempts, Some(60), None)
            .expect("test constraints should be valid"),
        attempt_number,

        children: None,
        cancellation_context: None,
    }
}

/// Runs one attempt whose `attempt_number - 1` failures already happened durably and
/// returns the state the authority would commit for that disposition.
fn run(handler: impl ExecutorHandler, attempt_number: u32, max_attempts: u32) -> RunState {
    let runner =
        AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_millis(4) });
    let record = runner.run_attempt(make_request(
        RunId::new(),
        AttemptId::new(),
        attempt_number,
        max_attempts,
    ));
    accounted(&record, attempt_number, max_attempts).unwrap()
}

fn accounted(
    record: &AttemptOutcomeRecord,
    attempt_number: u32,
    max_attempts: u32,
) -> Result<RunState, DispositionAccountingError> {
    record
        .disposition
        .outcome()
        .accounting(attempt_number.saturating_sub(1), max_attempts)
        .map(|accounting| accounting.target_state)
}

#[test]
fn retryable_failures_retry_under_the_cap_and_fail_exactly_at_it() {
    for max_attempts in 1..=4 {
        for attempt_number in 1..=max_attempts {
            let expected =
                if attempt_number < max_attempts { RunState::RetryWait } else { RunState::Failed };
            assert_eq!(run(RetryableFailureHandler, attempt_number, max_attempts), expected);
        }
    }
}

#[test]
fn no_attempt_beyond_the_cap_is_ever_accounted_as_a_retry() {
    for (attempt_number, max_attempts) in [(2, 1), (3, 2), (4, 3), (9, 3)] {
        assert_eq!(run(RetryableFailureHandler, attempt_number, max_attempts), RunState::Failed);
    }
    // A zero allowance is unrepresentable in constraints and rejected by accounting.
    let runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        FixedTimer { elapsed: Duration::from_millis(4) },
    );
    let record = runner.run_attempt(make_request(RunId::new(), AttemptId::new(), 1, 1));
    assert_eq!(accounted(&record, 1, 0), Err(DispositionAccountingError::InvalidMaxAttempts));
}

#[test]
fn success_and_terminal_failure_ignore_the_remaining_allowance() {
    for (attempt_number, max_attempts) in [(1, 1), (1, 3), (3, 3)] {
        assert_eq!(run(SuccessHandler, attempt_number, max_attempts), RunState::Completed);
        assert_eq!(run(TerminalFailureHandler, attempt_number, max_attempts), RunState::Failed);
    }
}

#[test]
fn runner_output_is_deterministic_at_the_boundary() {
    let run_id = RunId::new();
    let attempt_id = AttemptId::new();
    let runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        FixedTimer { elapsed: Duration::from_millis(4) },
    );
    let first = runner.run_attempt(make_request(run_id, attempt_id, 2, 2));
    let second = runner.run_attempt(make_request(run_id, attempt_id, 2, 2));
    assert_eq!(first, second);
    assert_eq!(accounted(&first, 2, 2), Ok(RunState::Failed));
}
