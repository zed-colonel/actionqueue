//! Timeout behavior tests for attempt outcomes and retry interactions.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::{panic::catch_unwind, panic::AssertUnwindSafe};

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_executor_local::{
    AttemptOutcomeKind, AttemptRunner, AttemptTimer, ExecutorContext, ExecutorHandler,
    ExecutorRequest, ExecutorResponse, HandlerOutput, RetryDecision, TimeoutCadencePolicy,
    TimeoutClassification, TimeoutCooperation, TimeoutCooperationMetrics,
};

#[derive(Debug, Clone)]
enum TimerMode {
    Fixed(Duration),
    Real,
}

#[derive(Debug, Clone)]
struct TestTimer {
    mode: TimerMode,
}

impl TestTimer {
    fn fixed(elapsed: Duration) -> Self {
        Self { mode: TimerMode::Fixed(elapsed) }
    }

    fn real() -> Self {
        Self { mode: TimerMode::Real }
    }
}

impl AttemptTimer for TestTimer {
    type Mark = Instant;

    fn start(&self) -> Self::Mark {
        Instant::now()
    }

    fn elapsed_since(&self, mark: Self::Mark) -> Duration {
        match self.mode {
            TimerMode::Fixed(elapsed) => elapsed,
            TimerMode::Real => mark.elapsed(),
        }
    }
}

struct SuccessHandler;

impl ExecutorHandler for SuccessHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::Success { output: Some(vec![7, 7]), consumption: vec![] }
    }
}

struct RetryableFailureHandler;

impl ExecutorHandler for RetryableFailureHandler {
    fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
        let _input = ctx.input;
        HandlerOutput::RetryableFailure {
            error: "retryable failure to combine with timeout semantics".to_string(),
            consumption: vec![],
        }
    }
}

fn make_request(max_attempts: u32, attempt_number: u32, timeout_secs: u64) -> ExecutorRequest {
    ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![1],
        constraints: TaskConstraints::new(max_attempts, Some(timeout_secs), None)
            .expect("test constraints should be valid"),
        attempt_number,
        submission: None,
        children: None,
        cancellation_context: None,
    }
}

#[test]
fn on_time_completion_stays_success_and_completed_in_time() {
    let runner =
        AttemptRunner::with_timer(SuccessHandler, TestTimer::fixed(Duration::from_secs(1)));

    let record = runner.run_attempt(make_request(3, 1, 5));

    assert!(matches!(
        record.response,
        actionqueue_executor_local::ExecutorResponse::Success {
            output: Some(ref bytes)
        } if bytes == &vec![7, 7]
    ));
    assert!(matches!(
        record.timeout_classification,
        TimeoutClassification::CompletedInTime {
            timeout_secs: 5,
            elapsed,
            ..
        } if elapsed == Duration::from_secs(1)
    ));
    assert_eq!(record.retry_decision, Ok(RetryDecision::Complete));
}

#[test]
fn timeout_overrides_handler_success_with_explicit_timeout_outcome() {
    let runner =
        AttemptRunner::with_timer(SuccessHandler, TestTimer::fixed(Duration::from_secs(6)));

    let record = runner.run_attempt(make_request(4, 1, 5));

    assert_eq!(
        record.response,
        actionqueue_executor_local::ExecutorResponse::Timeout { timeout_secs: 5 }
    );
    assert!(matches!(record.timeout_classification, TimeoutClassification::TimedOut(_)));
    assert_eq!(record.retry_decision_input.outcome_kind, AttemptOutcomeKind::Timeout);
    assert_eq!(record.retry_decision, Ok(RetryDecision::Retry));
}

#[test]
fn timeout_interacts_with_cap_under_cap_retries_at_cap_fails() {
    let under_cap_runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        TestTimer::fixed(Duration::from_secs(10)),
    );
    let under_cap = under_cap_runner.run_attempt(make_request(2, 1, 3));
    assert_eq!(
        under_cap.response,
        actionqueue_executor_local::ExecutorResponse::Timeout { timeout_secs: 3 }
    );
    assert_eq!(under_cap.retry_decision, Ok(RetryDecision::Retry));

    let at_cap_runner = AttemptRunner::with_timer(
        RetryableFailureHandler,
        TestTimer::fixed(Duration::from_secs(10)),
    );
    let at_cap = at_cap_runner.run_attempt(make_request(2, 2, 3));
    assert_eq!(
        at_cap.response,
        actionqueue_executor_local::ExecutorResponse::Timeout { timeout_secs: 3 }
    );
    assert_eq!(at_cap.retry_decision, Ok(RetryDecision::Fail));
}

#[test]
fn d01_t_p1_deadline_signal_occurs_during_active_handler() {
    struct CooperativeLoopHandler {
        observed_cancel: Arc<AtomicBool>,
    }

    impl ExecutorHandler for CooperativeLoopHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            for _ in 0..50_000 {
                if input.cancellation_context.token().is_cancelled() {
                    self.observed_cancel.store(true, Ordering::SeqCst);
                    return HandlerOutput::RetryableFailure {
                        error: "cooperative cancellation observed".to_string(),
                        consumption: vec![],
                    };
                }
                std::hint::spin_loop();
            }

            HandlerOutput::RetryableFailure {
                error: "loop finished before cancellation".to_string(),
                consumption: vec![],
            }
        }
    }

    let observed_cancel = Arc::new(AtomicBool::new(false));
    let runner = AttemptRunner::with_timer_metrics_and_cadence_policy(
        CooperativeLoopHandler { observed_cancel: Arc::clone(&observed_cancel) },
        TestTimer::real(),
        TimeoutCooperationMetrics::default(),
        TimeoutCadencePolicy::new(Duration::from_millis(50)),
    );

    let request = ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(3, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let record = runner.run_attempt(request);

    assert!(observed_cancel.load(Ordering::SeqCst));
    assert_eq!(record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::Cooperative);
    assert!(record.timeout_enforcement.cancellation_requested);
    assert!(record.timeout_enforcement.cancellation_observed);
    assert!(record.timeout_enforcement.cancellation_observation_latency.is_some());
    assert_eq!(record.timeout_enforcement.cadence_threshold, Duration::from_millis(50));
    assert!(record.timeout_enforcement.watchdog_joined);
}

#[test]
fn d01_t_p2_in_time_completion_has_no_cancellation_signal() {
    struct InTimeHandler;

    impl ExecutorHandler for InTimeHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            assert!(!input.cancellation_context.token().is_cancelled());
            HandlerOutput::Success { output: Some(vec![42]), consumption: vec![] }
        }
    }

    let runner =
        AttemptRunner::with_timer(InTimeHandler, TestTimer::fixed(Duration::from_millis(10)));

    let record = runner.run_attempt(make_request(3, 1, 1));

    assert_eq!(record.response, ExecutorResponse::Success { output: Some(vec![42]) });
    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
    assert!(!record.timeout_enforcement.cancellation_requested);
    assert!(!record.timeout_enforcement.cancellation_observed);
    assert!(record.timeout_enforcement.watchdog_joined);
}

#[test]
fn d01_t_p3_timeout_precedence_overrides_handler_success_payload() {
    struct LateSuccessHandler {
        saw_cancellation: Arc<AtomicBool>,
    }

    impl ExecutorHandler for LateSuccessHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            for _ in 0..20_000 {
                if input.cancellation_context.token().is_cancelled() {
                    self.saw_cancellation.store(true, Ordering::SeqCst);
                    break;
                }
                std::hint::spin_loop();
            }

            HandlerOutput::Success { output: Some(vec![9, 9, 9]), consumption: vec![] }
        }
    }

    let saw_cancellation = Arc::new(AtomicBool::new(false));
    let runner = AttemptRunner::with_timer(
        LateSuccessHandler { saw_cancellation: Arc::clone(&saw_cancellation) },
        TestTimer::real(),
    );

    let request = ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(3, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let record = runner.run_attempt(request);

    assert!(saw_cancellation.load(Ordering::SeqCst));
    assert_eq!(record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
    assert_eq!(record.retry_decision, Ok(RetryDecision::Retry));
    assert_eq!(record.retry_decision_input.outcome_kind, AttemptOutcomeKind::Timeout);
}

#[test]
fn d01_t_n1_no_timeout_path_has_no_watchdog_side_effects() {
    struct NoTimeoutHandler;

    impl ExecutorHandler for NoTimeoutHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            assert!(!input.cancellation_context.token().is_cancelled());
            HandlerOutput::Success { output: None, consumption: vec![] }
        }
    }

    let runner = AttemptRunner::with_timer(NoTimeoutHandler, TestTimer::real());
    let request = ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(2, None, None).expect("test constraints should be valid"),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let record = runner.run_attempt(request);

    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
    assert!(!record.timeout_enforcement.cancellation_requested);
    assert!(!record.timeout_enforcement.cancellation_observed);
    assert!(!record.timeout_enforcement.watchdog_joined);
}

#[test]
fn d01_t_n2_non_cooperative_handler_is_flagged_and_timeout_classified() {
    struct NonCooperativeHandler;

    impl ExecutorHandler for NonCooperativeHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let _input = ctx.input;
            thread::sleep(Duration::from_millis(2));
            HandlerOutput::Success { output: Some(vec![1]), consumption: vec![] }
        }
    }

    let runner = AttemptRunner::with_timer(NonCooperativeHandler, TestTimer::real());
    let request = ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(2, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let record = runner.run_attempt(request);

    assert_eq!(record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NonCooperative);
    assert!(record.timeout_enforcement.cancellation_requested);
    assert!(!record.timeout_enforcement.cancellation_observed);
    assert!(record.timeout_enforcement.watchdog_joined);
}

#[test]
fn d01_t_n2b_non_cooperative_timeout_emits_metric_exactly_once() {
    struct NonCooperativeHandler;

    impl ExecutorHandler for NonCooperativeHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let _input = ctx.input;
            thread::sleep(Duration::from_millis(2));
            HandlerOutput::Success { output: Some(vec![1, 2]), consumption: vec![] }
        }
    }

    let metrics = TimeoutCooperationMetrics::default();
    let runner = AttemptRunner::with_timer_and_metrics(
        NonCooperativeHandler,
        TestTimer::real(),
        metrics.clone(),
    );

    let record = runner.run_attempt(ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(2, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    });

    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NonCooperative);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.non_cooperative, 1);
    assert_eq!(snapshot.cooperative_threshold_breach, 0);
    assert_eq!(snapshot.cooperative, 0);
    assert_eq!(snapshot.not_applicable, 0);
}

#[test]
fn f003_t_p2_timeout_disabled_path_is_not_applicable_without_threshold_side_effects() {
    struct NoTimeoutHandler;

    impl ExecutorHandler for NoTimeoutHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            assert!(!input.cancellation_context.token().is_cancelled());
            HandlerOutput::Success { output: Some(vec![1, 2, 3]), consumption: vec![] }
        }
    }

    let metrics = TimeoutCooperationMetrics::default();
    let runner = AttemptRunner::with_timer_metrics_and_cadence_policy(
        NoTimeoutHandler,
        TestTimer::real(),
        metrics.clone(),
        TimeoutCadencePolicy::new(Duration::from_millis(1)),
    );

    let record = runner.run_attempt(ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(2, None, None).expect("test constraints should be valid"),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    });

    assert_eq!(record.response, ExecutorResponse::Success { output: Some(vec![1, 2, 3]) });
    assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
    assert_eq!(record.timeout_enforcement.cancellation_observation_latency, None);
    assert_eq!(record.timeout_enforcement.cadence_threshold, Duration::from_millis(1));
    assert!(!record.timeout_enforcement.cancellation_requested);
    assert!(!record.timeout_enforcement.cancellation_observed);

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.not_applicable, 1);
    assert_eq!(snapshot.cooperative, 0);
    assert_eq!(snapshot.cooperative_threshold_breach, 0);
    assert_eq!(snapshot.non_cooperative, 0);
}

#[test]
fn f003_t_n2_delayed_poll_handler_breaches_cadence_threshold() {
    struct DelayedPollHandler;

    impl ExecutorHandler for DelayedPollHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            thread::sleep(Duration::from_millis(10));
            let _ = input.cancellation_context.token().is_cancelled();
            HandlerOutput::RetryableFailure {
                error: "late cancellation poll".to_string(),
                consumption: vec![],
            }
        }
    }

    let runner = AttemptRunner::with_timer_metrics_and_cadence_policy(
        DelayedPollHandler,
        TestTimer::real(),
        TimeoutCooperationMetrics::default(),
        TimeoutCadencePolicy::new(Duration::from_millis(1)),
    );

    let record = runner.run_attempt(ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(2, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    });

    assert_eq!(record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
    assert_eq!(
        record.timeout_enforcement.cooperation,
        TimeoutCooperation::CooperativeThresholdBreach
    );
    assert!(record.timeout_enforcement.cancellation_requested);
    assert!(record.timeout_enforcement.cancellation_observed);
    assert_eq!(record.timeout_enforcement.cadence_threshold, Duration::from_millis(1));
    assert!(
        record
            .timeout_enforcement
            .cancellation_observation_latency
            .expect("observation latency must exist for observed cancellation")
            > Duration::from_millis(1)
    );
}

#[test]
fn f003_t_n3_threshold_breach_metric_is_emitted_exactly_once() {
    struct DelayedPollHandler;

    impl ExecutorHandler for DelayedPollHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            thread::sleep(Duration::from_millis(10));
            let _ = input.cancellation_context.token().is_cancelled();
            HandlerOutput::RetryableFailure {
                error: "late cancellation poll".to_string(),
                consumption: vec![],
            }
        }
    }

    let metrics = TimeoutCooperationMetrics::default();
    let runner = AttemptRunner::with_timer_metrics_and_cadence_policy(
        DelayedPollHandler,
        TestTimer::real(),
        metrics.clone(),
        TimeoutCadencePolicy::new(Duration::from_millis(1)),
    );

    let record = runner.run_attempt(ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(2, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    });

    assert_eq!(
        record.timeout_enforcement.cooperation,
        TimeoutCooperation::CooperativeThresholdBreach
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.cooperative_threshold_breach, 1);
    assert_eq!(snapshot.cooperative, 0);
    assert_eq!(snapshot.non_cooperative, 0);
    assert_eq!(snapshot.not_applicable, 0);
}

#[test]
fn d01_t_n3_cancellation_state_is_isolated_between_consecutive_attempts() {
    struct PerAttemptObservationHandler {
        saw_cancel_in_second: Arc<AtomicBool>,
    }

    impl ExecutorHandler for PerAttemptObservationHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            if input.metadata.attempt_number == 1 {
                thread::sleep(Duration::from_millis(2));
                return HandlerOutput::RetryableFailure {
                    error: "first attempt timeout path".to_string(),
                    consumption: vec![],
                };
            }

            if input.cancellation_context.token().is_cancelled() {
                self.saw_cancel_in_second.store(true, Ordering::SeqCst);
            }
            HandlerOutput::Success { output: Some(vec![2]), consumption: vec![] }
        }
    }

    let saw_cancel_in_second = Arc::new(AtomicBool::new(false));
    let runner = AttemptRunner::with_timer(
        PerAttemptObservationHandler { saw_cancel_in_second: Arc::clone(&saw_cancel_in_second) },
        TestTimer::real(),
    );

    let run_id = RunId::new();
    let first = ExecutorRequest {
        run_id,
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new_for_testing(3, Some(0), None),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    };
    let second = ExecutorRequest {
        run_id,
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(3, None, None).expect("test constraints should be valid"),
        attempt_number: 2,
        submission: None,
        children: None,
        cancellation_context: None,
    };

    let first_record = runner.run_attempt(first);
    let second_record = runner.run_attempt(second);

    assert_eq!(first_record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
    assert_eq!(first_record.timeout_enforcement.cooperation, TimeoutCooperation::NonCooperative);
    assert_eq!(second_record.response, ExecutorResponse::Success { output: Some(vec![2]) });
    assert_eq!(second_record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
    assert!(!saw_cancel_in_second.load(Ordering::SeqCst));
}

#[test]
fn d01_t_n4a_operation_panic_path_keeps_runner_usable_and_does_not_skip_cleanup() {
    struct PanicThenSuccessHandler {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ExecutorHandler for PanicThenSuccessHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let _input = ctx.input;
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                panic!("forced panic for cleanup-path assertion");
            }

            HandlerOutput::Success { output: Some(vec![7]), consumption: vec![] }
        }
    }

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let runner = AttemptRunner::with_timer(
        PanicThenSuccessHandler { calls: Arc::clone(&calls) },
        TestTimer::real(),
    );

    let panic_result = catch_unwind(AssertUnwindSafe(|| {
        let _ = runner.run_attempt(ExecutorRequest {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            payload: vec![],
            constraints: TaskConstraints::new(2, Some(30), None)
                .expect("test constraints should be valid"),
            attempt_number: 1,
            submission: None,
            children: None,
            cancellation_context: None,
        });
    }));
    assert!(panic_result.is_err());

    let recovery = runner.run_attempt(ExecutorRequest {
        run_id: RunId::new(),
        attempt_id: AttemptId::new(),
        payload: vec![],
        constraints: TaskConstraints::new(2, Some(30), None)
            .expect("test constraints should be valid"),
        attempt_number: 1,
        submission: None,
        children: None,
        cancellation_context: None,
    });

    assert_eq!(recovery.response, ExecutorResponse::Success { output: Some(vec![7]) });
    assert!(recovery.timeout_enforcement.watchdog_joined);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn d01_t_n4b_near_deadline_race_preserves_consistent_cleanup_and_flags() {
    struct RaceHandler {
        observed_cancel: Arc<AtomicBool>,
    }

    impl ExecutorHandler for RaceHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            for _ in 0..20_000 {
                if input.cancellation_context.token().is_cancelled() {
                    self.observed_cancel.store(true, Ordering::SeqCst);
                    break;
                }
                std::hint::spin_loop();
            }

            HandlerOutput::Success { output: Some(vec![3]), consumption: vec![] }
        }
    }

    for _ in 0..16 {
        let observed_cancel = Arc::new(AtomicBool::new(false));
        let runner = AttemptRunner::with_timer_metrics_and_cadence_policy(
            RaceHandler { observed_cancel: Arc::clone(&observed_cancel) },
            TestTimer::real(),
            TimeoutCooperationMetrics::default(),
            TimeoutCadencePolicy::new(Duration::from_millis(3)),
        );

        let record = runner.run_attempt(ExecutorRequest {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            payload: vec![],
            constraints: TaskConstraints::new_for_testing(2, Some(0), None),
            attempt_number: 1,
            submission: None,
            children: None,
            cancellation_context: None,
        });

        assert_eq!(record.response, ExecutorResponse::Timeout { timeout_secs: 0 });
        assert!(record.timeout_enforcement.watchdog_joined);
        assert_eq!(record.timeout_enforcement.cadence_threshold, Duration::from_millis(3));
        if record.timeout_enforcement.cancellation_observed {
            assert!(record.timeout_enforcement.cancellation_requested);
            let latency = record
                .timeout_enforcement
                .cancellation_observation_latency
                .expect("latency must exist when cancellation is observed");
            if latency > Duration::from_millis(3) {
                assert_eq!(
                    record.timeout_enforcement.cooperation,
                    TimeoutCooperation::CooperativeThresholdBreach
                );
            } else {
                assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::Cooperative);
            }
            assert!(observed_cancel.load(Ordering::SeqCst));
        } else {
            assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NonCooperative);
            assert_eq!(record.timeout_enforcement.cancellation_observation_latency, None);
        }
    }
}
