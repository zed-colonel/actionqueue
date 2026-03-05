//! ID propagation tests for attempt execution boundaries.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_executor_local::{
    AttemptRunner, AttemptTimer, ExecutorContext, ExecutorHandler, ExecutorRequest, HandlerOutput,
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

#[derive(Debug)]
struct CapturingHandler {
    captured: Arc<Mutex<Vec<(RunId, AttemptId)>>>,
}

impl CapturingHandler {
    fn new(captured: Arc<Mutex<Vec<(RunId, AttemptId)>>>) -> Self {
        Self { captured }
    }
}

impl ExecutorHandler for CapturingHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let input = ctx.input;
        self.captured.lock().unwrap().push((input.run_id, input.attempt_id));

        HandlerOutput::RetryableFailure {
            error: "force retry path for ID propagation assertions".to_string(),
            consumption: vec![],
        }
    }
}

#[test]
fn handler_receives_exact_run_and_attempt_ids_for_initial_attempt() {
    let run_id = RunId::new();
    let attempt_id = AttemptId::new();
    let captured_ids = Arc::new(Mutex::new(Vec::new()));
    let handler = CapturingHandler::new(Arc::clone(&captured_ids));
    let runner =
        AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_millis(2) });

    let request = ExecutorRequest {
        run_id,
        attempt_id,
        payload: vec![1, 2, 3],
        constraints: TaskConstraints::new(3, Some(30), None)
            .expect("test constraints should be valid"),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let record = runner.run_attempt(request);
    let captured = captured_ids.lock().unwrap();

    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0], (run_id, attempt_id));
    assert_eq!(record.run_id, run_id);
    assert_eq!(record.attempt_id, attempt_id);
    assert_eq!(record.retry_decision_input.run_id, run_id);
    assert_eq!(record.retry_decision_input.attempt_id, attempt_id);
}

#[test]
fn retried_attempt_keeps_run_id_and_uses_provided_attempt_id_without_regeneration() {
    let stable_run_id = RunId::new();
    let first_attempt = AttemptId::new();
    let second_attempt = AttemptId::new();
    let captured_ids = Arc::new(Mutex::new(Vec::new()));
    let handler = CapturingHandler::new(Arc::clone(&captured_ids));
    let runner =
        AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_millis(1) });

    let constraints =
        TaskConstraints::new(3, Some(30), None).expect("test constraints should be valid");

    let first_request = ExecutorRequest {
        run_id: stable_run_id,
        attempt_id: first_attempt,
        payload: vec![],
        constraints: constraints.clone(),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };
    let second_request = ExecutorRequest {
        run_id: stable_run_id,
        attempt_id: second_attempt,
        payload: vec![],
        constraints,
        attempt_number: 2,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let first_record = runner.run_attempt(first_request);
    let second_record = runner.run_attempt(second_request);

    let captured = captured_ids.lock().unwrap();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0], (stable_run_id, first_attempt));
    assert_eq!(captured[1], (stable_run_id, second_attempt));

    assert_eq!(first_record.run_id, stable_run_id);
    assert_eq!(second_record.run_id, stable_run_id);
    assert_eq!(first_record.attempt_id, first_attempt);
    assert_eq!(second_record.attempt_id, second_attempt);
}
