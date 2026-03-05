//! Attempt runner for local executor orchestration.
//!
//! The runner composes three concerns for a single attempt execution:
//! - handler invocation with explicit run/attempt identity,
//! - timeout classification and enforcement with explicit cancellation-poll cadence evidence,
//! - retry-decision input generation from the terminal attempt outcome.
//!
//! This module intentionally does not mutate run derivation/accounting state.

use std::time::{Duration, Instant};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
};

use actionqueue_core::budget::BudgetConsumption;
use actionqueue_core::ids::{AttemptId, RunId};

use crate::handler::{AttemptMetadata, ExecutorHandler, HandlerInput, HandlerOutput};
use crate::retry::{decide_retry_transition, RetryDecision, RetryDecisionError};
use crate::timeout::{TimeoutClassification, TimeoutClock, TimeoutFailure, TimeoutGuard};
use crate::types::{ExecutorRequest, ExecutorResponse};

const DEFAULT_MAX_CANCELLATION_POLL_LATENCY: Duration = Duration::from_millis(250);

/// Policy surface controlling timeout cancellation-poll cadence enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutCadencePolicy {
    max_cancellation_poll_latency: Duration,
}

impl TimeoutCadencePolicy {
    /// Creates a cadence policy with an explicit max cancellation-poll latency.
    pub fn new(max_cancellation_poll_latency: Duration) -> Self {
        Self { max_cancellation_poll_latency }
    }

    /// Returns the maximum allowed latency from cancellation request to first observed poll.
    pub fn max_cancellation_poll_latency(&self) -> Duration {
        self.max_cancellation_poll_latency
    }
}

impl Default for TimeoutCadencePolicy {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CANCELLATION_POLL_LATENCY)
    }
}

#[derive(Debug, Clone, Copy)]
struct AttemptTimeoutClock<'a, T> {
    timer: &'a T,
}

impl<'a, T> TimeoutClock for AttemptTimeoutClock<'a, T>
where
    T: AttemptTimer,
{
    type Mark = T::Mark;

    fn mark_now(&self) -> Self::Mark {
        self.timer.start()
    }

    fn elapsed_since(&self, mark: Self::Mark) -> Duration {
        self.timer.elapsed_since(mark)
    }
}

/// Classification of a terminal attempt outcome for retry-decision inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcomeKind {
    /// The attempt completed successfully.
    Success,
    /// The attempt failed and may be retried.
    RetryableFailure,
    /// The attempt failed permanently.
    TerminalFailure,
    /// The attempt exceeded timeout constraints.
    Timeout,
    /// The attempt was voluntarily suspended (budget exhaustion / preemption).
    Suspended,
}

impl AttemptOutcomeKind {
    pub fn from_response(response: &ExecutorResponse) -> Self {
        match response {
            ExecutorResponse::Success { .. } => Self::Success,
            ExecutorResponse::RetryableFailure { .. } => Self::RetryableFailure,
            ExecutorResponse::TerminalFailure { .. } => Self::TerminalFailure,
            ExecutorResponse::Timeout { .. } => Self::Timeout,
            ExecutorResponse::Suspended { .. } => Self::Suspended,
        }
    }
}

/// Retry input payload derived from one completed attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryDecisionInput {
    /// Run identifier for the attempt.
    pub run_id: RunId,
    /// Attempt identifier for the attempt.
    pub attempt_id: AttemptId,
    /// Attempt number for this execution (1-indexed).
    pub attempt_number: u32,
    /// Hard cap for attempts from task constraints snapshot.
    pub max_attempts: u32,
    /// Terminal outcome classification for retry policy evaluation.
    pub outcome_kind: AttemptOutcomeKind,
}

/// Cooperative-cancellation status for timeout enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutCooperation {
    /// Timeout cancellation did not apply (timeout disabled or no timeout classification).
    NotApplicable,
    /// Timeout occurred and the handler observed cancellation within cadence policy.
    Cooperative,
    /// Timeout occurred and cancellation was observed, but beyond cadence threshold.
    CooperativeThresholdBreach,
    /// Timeout occurred but the handler never observed cancellation.
    NonCooperative,
}

/// Snapshot of timeout-cooperation metric counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[must_use = "metrics snapshot should be inspected or reported"]
pub struct TimeoutCooperationMetricsSnapshot {
    /// Number of timeout outcomes where cancellation was observed cooperatively.
    pub cooperative: u64,
    /// Number of timeout outcomes where cancellation observation breached cadence threshold.
    pub cooperative_threshold_breach: u64,
    /// Number of timeout outcomes where cancellation was not observed.
    pub non_cooperative: u64,
    /// Number of outcomes where timeout cooperation did not apply.
    pub not_applicable: u64,
}

/// Concrete timeout-cooperation metric sink for attempt outcomes.
#[derive(Debug, Clone, Default)]
pub struct TimeoutCooperationMetrics {
    cooperative: Arc<AtomicU64>,
    cooperative_threshold_breach: Arc<AtomicU64>,
    non_cooperative: Arc<AtomicU64>,
    not_applicable: Arc<AtomicU64>,
}

impl TimeoutCooperationMetrics {
    /// Records one timeout-cooperation outcome.
    pub fn record(&self, cooperation: TimeoutCooperation) {
        match cooperation {
            TimeoutCooperation::Cooperative => {
                self.cooperative.fetch_add(1, Ordering::SeqCst);
            }
            TimeoutCooperation::CooperativeThresholdBreach => {
                self.cooperative_threshold_breach.fetch_add(1, Ordering::SeqCst);
            }
            TimeoutCooperation::NonCooperative => {
                self.non_cooperative.fetch_add(1, Ordering::SeqCst);
            }
            TimeoutCooperation::NotApplicable => {
                self.not_applicable.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Returns current metric counts.
    pub fn snapshot(&self) -> TimeoutCooperationMetricsSnapshot {
        TimeoutCooperationMetricsSnapshot {
            cooperative: self.cooperative.load(Ordering::SeqCst),
            cooperative_threshold_breach: self.cooperative_threshold_breach.load(Ordering::SeqCst),
            non_cooperative: self.non_cooperative.load(Ordering::SeqCst),
            not_applicable: self.not_applicable.load(Ordering::SeqCst),
        }
    }
}

/// Inspectable timeout-enforcement report for one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutEnforcementReport {
    /// True if the timeout watchdog requested cancellation while work was active.
    pub cancellation_requested: bool,
    /// True if a handler poll observed cancellation.
    pub cancellation_observed: bool,
    /// Latency from cancellation request to first observed cancellation poll.
    pub cancellation_observation_latency: Option<Duration>,
    /// Cadence threshold used to classify timeout cooperation.
    pub cadence_threshold: Duration,
    /// True if a watchdog worker was started and deterministically joined.
    pub watchdog_joined: bool,
    /// Cooperative-cancellation interpretation for metrics and gate assertions.
    pub cooperation: TimeoutCooperation,
}

/// Terminal record for one completed attempt execution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "attempt outcome should be inspected for state transition decisions"]
pub struct AttemptOutcomeRecord {
    /// Run identifier propagated through execution boundary.
    pub run_id: RunId,
    /// Attempt identifier propagated through execution boundary.
    pub attempt_id: AttemptId,
    /// Deterministic attempt response classification.
    pub response: ExecutorResponse,
    /// Measured attempt execution duration.
    pub elapsed: Duration,
    /// Explicit timeout classification with stable reason-code semantics.
    pub timeout_classification: TimeoutClassification,
    /// Timeout enforcement report including cadence-aware cooperation classification.
    pub timeout_enforcement: TimeoutEnforcementReport,
    /// Retry input derived from this exact attempt outcome.
    pub retry_decision_input: RetryDecisionInput,
    /// Retry transition decision derived via [`crate::retry::decide_retry_transition`].
    ///
    /// An error indicates invalid attempt-counter inputs (for example `N + 1`
    /// attempts beyond the configured hard cap).
    pub retry_decision: Result<RetryDecision, RetryDecisionError>,
    /// Resource consumption reported by the handler for this attempt.
    pub consumption: Vec<BudgetConsumption>,
}

/// Monotonic timer abstraction used for timeout enforcement.
pub trait AttemptTimer {
    /// Opaque mark captured before handler execution.
    type Mark: Copy;

    /// Captures a start mark.
    fn start(&self) -> Self::Mark;

    /// Returns elapsed duration since `mark`.
    fn elapsed_since(&self, mark: Self::Mark) -> Duration;
}

/// System monotonic timer based on [`Instant`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemAttemptTimer;

impl AttemptTimer for SystemAttemptTimer {
    type Mark = Instant;

    fn start(&self) -> Self::Mark {
        Instant::now()
    }

    fn elapsed_since(&self, mark: Self::Mark) -> Duration {
        mark.elapsed()
    }
}

/// Runs single-attempt executions by composing handler, timeout, and retry input derivation.
#[derive(Debug, Clone)]
pub struct AttemptRunner<H, T = SystemAttemptTimer> {
    handler: H,
    timer: T,
    timeout_metrics: TimeoutCooperationMetrics,
    timeout_cadence_policy: TimeoutCadencePolicy,
}

impl<H> AttemptRunner<H, SystemAttemptTimer>
where
    H: ExecutorHandler,
{
    /// Creates an attempt runner using the default system timer.
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            timer: SystemAttemptTimer,
            timeout_metrics: TimeoutCooperationMetrics::default(),
            timeout_cadence_policy: TimeoutCadencePolicy::default(),
        }
    }
}

impl<H, T> AttemptRunner<H, T>
where
    H: ExecutorHandler,
    T: AttemptTimer,
{
    /// Creates an attempt runner with an explicit timer implementation.
    pub fn with_timer(handler: H, timer: T) -> Self {
        Self {
            handler,
            timer,
            timeout_metrics: TimeoutCooperationMetrics::default(),
            timeout_cadence_policy: TimeoutCadencePolicy::default(),
        }
    }

    /// Creates an attempt runner with explicit timer and timeout metric sink.
    pub fn with_timer_and_metrics(
        handler: H,
        timer: T,
        timeout_metrics: TimeoutCooperationMetrics,
    ) -> Self {
        Self {
            handler,
            timer,
            timeout_metrics,
            timeout_cadence_policy: TimeoutCadencePolicy::default(),
        }
    }

    /// Creates an attempt runner with explicit timer, metric sink, and cadence policy.
    pub fn with_timer_metrics_and_cadence_policy(
        handler: H,
        timer: T,
        timeout_metrics: TimeoutCooperationMetrics,
        timeout_cadence_policy: TimeoutCadencePolicy,
    ) -> Self {
        Self { handler, timer, timeout_metrics, timeout_cadence_policy }
    }

    /// Returns the timeout cooperation metric sink used by this runner.
    pub fn timeout_metrics(&self) -> &TimeoutCooperationMetrics {
        &self.timeout_metrics
    }

    /// Returns the timeout cadence policy used by this runner.
    pub fn timeout_cadence_policy(&self) -> TimeoutCadencePolicy {
        self.timeout_cadence_policy
    }

    /// Executes exactly one attempt and returns one terminal attempt outcome record.
    pub fn run_attempt(&self, mut request: ExecutorRequest) -> AttemptOutcomeRecord {
        let run_id = request.run_id;
        let attempt_id = request.attempt_id;
        let attempt_number = request.attempt_number;
        let max_attempts = request.constraints.max_attempts();
        let timeout_secs = request.constraints.timeout_secs();

        let clock = AttemptTimeoutClock { timer: &self.timer };
        let payload = request.payload;
        let safety_level = request.constraints.safety_level();
        let metadata = AttemptMetadata { max_attempts, attempt_number, timeout_secs, safety_level };
        let submission = request.submission.take();
        let children = request.children.take();
        let external_ctx = request.cancellation_context.take();
        let guard = TimeoutGuard::with_clock(clock);
        let make_handler_call = |cancellation_context: &crate::handler::CancellationContext| {
            self.handler.execute(crate::handler::ExecutorContext {
                input: HandlerInput {
                    run_id,
                    attempt_id,
                    payload,
                    metadata,
                    cancellation_context: cancellation_context.clone(),
                },
                submission,
                children,
            })
        };
        let guarded = if let Some(ctx) = external_ctx {
            guard.execute_with_external_cancellation(timeout_secs, ctx, make_handler_call)
        } else {
            guard.execute_with_cancellation(timeout_secs, make_handler_call)
        };
        let crate::timeout::CancellableExecution {
            value: handler_output,
            elapsed,
            timeout: timeout_classification,
            cancel_requested,
            cancellation_observed,
            cancellation_observation_latency,
            watchdog_joined,
            watchdog_spawn_failed: _,
        } = guarded;

        let cooperation = classify_timeout_cooperation(
            &timeout_classification,
            cancellation_observed,
            cancellation_observation_latency,
            self.timeout_cadence_policy,
        );
        let timeout_enforcement = TimeoutEnforcementReport {
            cancellation_requested: cancel_requested,
            cancellation_observed,
            cancellation_observation_latency,
            cadence_threshold: self.timeout_cadence_policy.max_cancellation_poll_latency(),
            watchdog_joined,
            cooperation,
        };
        self.timeout_metrics.record(timeout_enforcement.cooperation);

        let (response, consumption) = classify_response(handler_output, &timeout_classification);
        let retry_decision_input = RetryDecisionInput {
            run_id,
            attempt_id,
            attempt_number,
            max_attempts,
            outcome_kind: AttemptOutcomeKind::from_response(&response),
        };
        let retry_decision = decide_retry_transition(&retry_decision_input);

        AttemptOutcomeRecord {
            run_id,
            attempt_id,
            response,
            elapsed,
            timeout_classification,
            timeout_enforcement,
            retry_decision_input,
            retry_decision,
            consumption,
        }
    }
}

fn classify_response(
    output: HandlerOutput,
    timeout: &TimeoutClassification,
) -> (ExecutorResponse, Vec<BudgetConsumption>) {
    let consumption = output.consumption().to_vec();

    // Timeout overrides all non-Suspended handler responses. A handler that
    // explicitly suspends takes priority — budget-based suspension is voluntary.
    if let TimeoutClassification::TimedOut(TimeoutFailure { timeout_secs, .. }) = timeout {
        if !matches!(output, HandlerOutput::Suspended { .. }) {
            return (ExecutorResponse::Timeout { timeout_secs: *timeout_secs }, consumption);
        }
    }

    let response = match output {
        HandlerOutput::Success { output, .. } => ExecutorResponse::Success { output },
        HandlerOutput::RetryableFailure { error, .. } => {
            ExecutorResponse::RetryableFailure { error }
        }
        HandlerOutput::TerminalFailure { error, .. } => ExecutorResponse::TerminalFailure { error },
        HandlerOutput::Suspended { output, .. } => ExecutorResponse::Suspended { output },
    };
    (response, consumption)
}

fn classify_timeout_cooperation(
    timeout_classification: &TimeoutClassification,
    cancellation_observed: bool,
    cancellation_observation_latency: Option<Duration>,
    timeout_cadence_policy: TimeoutCadencePolicy,
) -> TimeoutCooperation {
    if !timeout_classification.is_timed_out() {
        return TimeoutCooperation::NotApplicable;
    }

    if !cancellation_observed {
        return TimeoutCooperation::NonCooperative;
    }

    match cancellation_observation_latency {
        Some(latency) if latency > timeout_cadence_policy.max_cancellation_poll_latency() => {
            TimeoutCooperation::CooperativeThresholdBreach
        }
        Some(_) => TimeoutCooperation::Cooperative,
        None => TimeoutCooperation::NonCooperative,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use actionqueue_core::ids::{AttemptId, RunId};
    use actionqueue_core::task::constraints::TaskConstraints;

    use super::{
        AttemptOutcomeKind, AttemptRunner, AttemptTimer, TimeoutCadencePolicy, TimeoutCooperation,
        TimeoutCooperationMetrics, TimeoutCooperationMetricsSnapshot,
    };
    use crate::handler::{ExecutorContext, ExecutorHandler, HandlerInput, HandlerOutput};
    use crate::retry::RetryDecision;
    use crate::timeout::{TimeoutClassification, TimeoutFailure, TimeoutReasonCode};
    use crate::types::ExecutorRequest;

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

    struct RecordingHandler {
        output: HandlerOutput,
        input: Mutex<Option<HandlerInput>>,
    }

    impl RecordingHandler {
        fn new(output: HandlerOutput) -> Self {
            Self { output, input: Mutex::new(None) }
        }
    }

    impl ExecutorHandler for RecordingHandler {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let input = ctx.input;
            *self.input.lock().unwrap() = Some(input);
            self.output.clone()
        }
    }

    #[test]
    fn run_attempt_propagates_ids_and_maps_success() {
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();
        let handler = RecordingHandler::new(HandlerOutput::Success {
            output: Some(vec![1, 2, 3]),
            consumption: vec![],
        });
        let runner =
            AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_millis(5) });

        let request = ExecutorRequest {
            run_id,
            attempt_id,
            payload: vec![9, 8, 7],
            constraints: TaskConstraints::new(3, Some(60), None)
                .expect("test constraints must be valid"),
            attempt_number: 2,
            submission: None,
            children: None,
            cancellation_context: None,
        };

        let record = runner.run_attempt(request);

        assert_eq!(record.run_id, run_id);
        assert_eq!(record.attempt_id, attempt_id);
        assert_eq!(
            record.response,
            crate::types::ExecutorResponse::Success { output: Some(vec![1, 2, 3]) }
        );
        assert_eq!(record.retry_decision, Ok(RetryDecision::Complete));
        assert_eq!(record.retry_decision_input.outcome_kind, AttemptOutcomeKind::Success);
        assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
        assert_eq!(record.timeout_enforcement.cancellation_observation_latency, None);
        assert_eq!(
            record.timeout_enforcement.cadence_threshold,
            TimeoutCadencePolicy::default().max_cancellation_poll_latency()
        );
        assert_eq!(
            record.timeout_classification,
            TimeoutClassification::CompletedInTime {
                timeout_secs: 60,
                elapsed: Duration::from_millis(5),
                reason_code: TimeoutReasonCode::WithinLimit,
            }
        );

        let captured =
            runner.handler.input.lock().unwrap().clone().expect("handler should capture input");

        assert_eq!(captured.run_id, run_id);
        assert_eq!(captured.attempt_id, attempt_id);
        assert_eq!(captured.metadata.max_attempts, 3);
        assert_eq!(captured.metadata.attempt_number, 2);
        assert_eq!(captured.metadata.timeout_secs, Some(60));
    }

    #[test]
    fn run_attempt_marks_timeout_when_elapsed_exceeds_limit() {
        let handler = RecordingHandler::new(HandlerOutput::Success {
            output: Some(vec![42]),
            consumption: vec![],
        });
        let runner =
            AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_secs(2) });

        let request = ExecutorRequest {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            payload: vec![],
            constraints: TaskConstraints::new(2, Some(1), None)
                .expect("test constraints must be valid"),
            attempt_number: 1,
            submission: None,
            children: None,
            cancellation_context: None,
        };

        let record = runner.run_attempt(request);

        assert_eq!(record.response, crate::types::ExecutorResponse::Timeout { timeout_secs: 1 });
        assert_eq!(record.retry_decision, Ok(RetryDecision::Retry));
        assert_eq!(record.retry_decision_input.outcome_kind, AttemptOutcomeKind::Timeout);
        assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NonCooperative);
        assert_eq!(record.timeout_enforcement.cancellation_observation_latency, None);
        assert_eq!(
            record.timeout_enforcement.cadence_threshold,
            TimeoutCadencePolicy::default().max_cancellation_poll_latency()
        );
        assert_eq!(
            record.timeout_classification,
            TimeoutClassification::TimedOut(crate::timeout::TimeoutFailure {
                timeout_secs: 1,
                elapsed: Duration::from_secs(2),
                reason_code: TimeoutReasonCode::DeadlineExceeded,
            })
        );
    }

    #[test]
    fn run_attempt_preserves_retryable_failure_without_timeout() {
        let handler = RecordingHandler::new(HandlerOutput::RetryableFailure {
            error: "transient error".to_string(),
            consumption: vec![],
        });
        let runner =
            AttemptRunner::with_timer(handler, FixedTimer { elapsed: Duration::from_millis(1) });

        let request = ExecutorRequest {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            payload: vec![],
            constraints: TaskConstraints::new(5, Some(30), None)
                .expect("test constraints must be valid"),
            attempt_number: 3,
            submission: None,
            children: None,
            cancellation_context: None,
        };

        let record = runner.run_attempt(request);

        assert_eq!(
            record.response,
            crate::types::ExecutorResponse::RetryableFailure {
                error: "transient error".to_string(),
            }
        );
        assert_eq!(record.retry_decision, Ok(RetryDecision::Retry));
        assert_eq!(record.retry_decision_input.outcome_kind, AttemptOutcomeKind::RetryableFailure);
        assert_eq!(record.retry_decision_input.attempt_number, 3);
        assert_eq!(record.retry_decision_input.max_attempts, 5);
        assert_eq!(record.timeout_enforcement.cooperation, TimeoutCooperation::NotApplicable);
        assert_eq!(record.timeout_enforcement.cancellation_observation_latency, None);
    }

    #[test]
    fn run_attempt_emits_timeout_cooperation_metric_once() {
        let metrics = TimeoutCooperationMetrics::default();
        let runner = AttemptRunner::with_timer_and_metrics(
            RecordingHandler::new(HandlerOutput::Success {
                output: Some(vec![7]),
                consumption: vec![],
            }),
            FixedTimer { elapsed: Duration::from_secs(3) },
            metrics.clone(),
        );

        let request = ExecutorRequest {
            run_id: RunId::new(),
            attempt_id: AttemptId::new(),
            payload: vec![],
            constraints: TaskConstraints::new(2, Some(1), None)
                .expect("test constraints must be valid"),
            attempt_number: 1,
            submission: None,
            children: None,
            cancellation_context: None,
        };

        let _record = runner.run_attempt(request);

        assert_eq!(
            metrics.snapshot(),
            TimeoutCooperationMetricsSnapshot {
                cooperative: 0,
                cooperative_threshold_breach: 0,
                non_cooperative: 1,
                not_applicable: 0,
            }
        );
    }

    #[test]
    fn timeout_cooperation_marks_threshold_breach_when_observation_latency_exceeds_policy() {
        let timeout = TimeoutClassification::TimedOut(TimeoutFailure {
            timeout_secs: 1,
            elapsed: Duration::from_secs(2),
            reason_code: TimeoutReasonCode::DeadlineExceeded,
        });

        let cooperation = super::classify_timeout_cooperation(
            &timeout,
            true,
            Some(Duration::from_millis(15)),
            TimeoutCadencePolicy::new(Duration::from_millis(5)),
        );

        assert_eq!(cooperation, TimeoutCooperation::CooperativeThresholdBreach);
    }

    #[test]
    fn timeout_cooperation_marks_cooperative_when_observation_latency_is_within_policy() {
        let timeout = TimeoutClassification::TimedOut(TimeoutFailure {
            timeout_secs: 1,
            elapsed: Duration::from_secs(2),
            reason_code: TimeoutReasonCode::DeadlineExceeded,
        });

        let cooperation = super::classify_timeout_cooperation(
            &timeout,
            true,
            Some(Duration::from_millis(5)),
            TimeoutCadencePolicy::new(Duration::from_millis(10)),
        );

        assert_eq!(cooperation, TimeoutCooperation::Cooperative);
    }

    #[test]
    fn classify_response_suspended_takes_priority_over_timeout() {
        let output = HandlerOutput::Suspended { output: Some(vec![1, 2, 3]), consumption: vec![] };
        let timeout = TimeoutClassification::TimedOut(TimeoutFailure {
            timeout_secs: 10,
            elapsed: Duration::from_secs(15),
            reason_code: TimeoutReasonCode::DeadlineExceeded,
        });
        let (response, _) = super::classify_response(output, &timeout);
        assert!(
            matches!(response, crate::types::ExecutorResponse::Suspended { .. }),
            "Suspended should take priority over Timeout, got: {response:?}"
        );
    }
}
