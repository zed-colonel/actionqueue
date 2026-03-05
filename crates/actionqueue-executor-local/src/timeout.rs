//! Timeout classification and enforcement APIs.
//!
//! This module provides two distinct concerns for timeout handling:
//!
//! - **Classification**: Pure measurement of elapsed time and timeout status
//!   without side effects. See [`TimeoutClassifier`].
//! - **Enforcement**: Active timeout control via cancellation. See [`TimeoutGuard`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::handler::CancellationContext;

/// Default watchdog poll interval.
const DEFAULT_WATCHDOG_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[cfg(test)]
static ACTIVE_WATCHDOG_THREADS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
struct ActiveWatchdogThreadGuard;

#[cfg(test)]
impl Drop for ActiveWatchdogThreadGuard {
    fn drop(&mut self) {
        ACTIVE_WATCHDOG_THREADS.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn active_watchdog_threads() -> u64 {
    ACTIVE_WATCHDOG_THREADS.load(Ordering::SeqCst)
}

#[derive(Debug)]
struct WatchdogLifecycle {
    operation_completed: Arc<AtomicBool>,
    cancellation_requested: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    /// True if thread::spawn failed (graceful degradation — no watchdog active).
    watchdog_spawn_failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WatchdogOutcome {
    cancellation_requested: bool,
    watchdog_joined: bool,
    watchdog_spawn_failed: bool,
}

impl WatchdogLifecycle {
    fn new(
        timeout_secs: Option<u64>,
        cancellation_context: &CancellationContext,
        poll_interval: Duration,
    ) -> Self {
        let operation_completed = Arc::new(AtomicBool::new(false));
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let mut watchdog_spawn_failed = false;

        let handle = timeout_secs.and_then(|secs| {
            let completion = Arc::clone(&operation_completed);
            let requested = Arc::clone(&cancellation_requested);
            let context = cancellation_context.clone();
            let deadline = Duration::from_secs(secs);

            match thread::Builder::new().name("actionqueue-timeout-watchdog".to_string()).spawn(
                move || {
                    #[cfg(test)]
                    ACTIVE_WATCHDOG_THREADS.fetch_add(1, Ordering::SeqCst);
                    #[cfg(test)]
                    let _active_watchdog_guard = ActiveWatchdogThreadGuard;

                    let started_at = Instant::now();
                    loop {
                        if completion.load(Ordering::SeqCst) {
                            return;
                        }

                        let elapsed = started_at.elapsed();
                        if elapsed >= deadline {
                            break;
                        }

                        let remaining = deadline.saturating_sub(elapsed);
                        thread::sleep(remaining.min(poll_interval));
                    }

                    if !completion.load(Ordering::SeqCst) {
                        context.cancel();
                        requested.store(true, Ordering::SeqCst);
                    }
                },
            ) {
                Ok(handle) => Some(handle),
                Err(_) => {
                    watchdog_spawn_failed = true;
                    None
                }
            }
        });

        Self { operation_completed, cancellation_requested, handle, watchdog_spawn_failed }
    }

    fn mark_operation_completed(&self) {
        self.operation_completed.store(true, Ordering::SeqCst);
    }

    fn join_handle(&mut self) {
        if let Some(handle) = self.handle.take() {
            if let Err(panic_payload) = handle.join() {
                if std::thread::panicking() {
                    std::process::abort();
                }
                std::panic::resume_unwind(panic_payload);
            }
        }
    }

    fn finish(mut self) -> WatchdogOutcome {
        let watchdog_started = self.handle.is_some();
        let spawn_failed = self.watchdog_spawn_failed;
        self.mark_operation_completed();
        self.join_handle();

        WatchdogOutcome {
            cancellation_requested: self.cancellation_requested.load(Ordering::SeqCst),
            watchdog_joined: watchdog_started,
            watchdog_spawn_failed: spawn_failed,
        }
    }
}

impl Drop for WatchdogLifecycle {
    fn drop(&mut self) {
        self.mark_operation_completed();
        self.join_handle();
    }
}

/// Inspectable reason codes for timeout classification outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutReasonCode {
    /// The execution exceeded the configured timeout deadline.
    DeadlineExceeded,
    /// No timeout was configured for the execution.
    NoTimeoutConfigured,
    /// A timeout was configured and the execution completed within the limit.
    WithinLimit,
}

/// Explicit timeout failure data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutFailure {
    /// Configured timeout in seconds.
    pub timeout_secs: u64,
    /// Measured elapsed execution duration.
    pub elapsed: Duration,
    /// Stable reason code for timeout inspection.
    pub reason_code: TimeoutReasonCode,
}

/// Classification of attempt execution relative to timeout constraints.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum TimeoutClassification {
    /// No timeout was configured.
    NotConfigured {
        /// Measured elapsed execution duration.
        elapsed: Duration,
        /// Stable reason code for inspectability.
        reason_code: TimeoutReasonCode,
    },
    /// Timeout was configured and execution completed in time.
    CompletedInTime {
        /// Configured timeout in seconds.
        timeout_secs: u64,
        /// Measured elapsed execution duration.
        elapsed: Duration,
        /// Stable reason code for inspectability.
        reason_code: TimeoutReasonCode,
    },
    /// Timeout was configured and execution exceeded the deadline.
    TimedOut(TimeoutFailure),
}

impl TimeoutClassification {
    /// Returns true when classification indicates a timeout failure.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, TimeoutClassification::TimedOut(_))
    }
}

/// Output of a guarded execution.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "execution result should be inspected for timeout classification"]
pub struct GuardedExecution<T> {
    /// Return value produced by the wrapped operation.
    pub value: T,
    /// Measured elapsed execution duration.
    pub elapsed: Duration,
    /// Explicit timeout classification for the execution.
    pub timeout: TimeoutClassification,
}

/// Output of a guarded execution with cancellation flag.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "execution result should be inspected for timeout classification"]
pub struct CancellableExecution<T> {
    /// Return value produced by the wrapped operation.
    pub value: T,
    /// Measured elapsed execution duration.
    pub elapsed: Duration,
    /// Explicit timeout classification for the execution.
    pub timeout: TimeoutClassification,
    /// True if deadline signaling requested cancellation while operation was still running.
    pub cancel_requested: bool,
    /// True if any handler poll observed cancellation while running.
    pub cancellation_observed: bool,
    /// Latency between the first cancellation request and first observed cancellation poll.
    pub cancellation_observation_latency: Option<Duration>,
    /// True if a watchdog worker was started and deterministically joined.
    pub watchdog_joined: bool,
    /// True if the watchdog thread failed to spawn (graceful degradation).
    pub watchdog_spawn_failed: bool,
}

/// Monotonic clock abstraction used by timeout classification.
pub trait TimeoutClock {
    /// Opaque mark captured before execution begins.
    type Mark: Copy;

    /// Captures the current mark.
    fn mark_now(&self) -> Self::Mark;

    /// Returns elapsed duration since a previously captured mark.
    fn elapsed_since(&self, mark: Self::Mark) -> Duration;
}

/// System clock implementation for timeout classification using [`Instant`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTimeoutClock;

impl TimeoutClock for SystemTimeoutClock {
    type Mark = Instant;

    fn mark_now(&self) -> Self::Mark {
        Instant::now()
    }

    fn elapsed_since(&self, mark: Self::Mark) -> Duration {
        mark.elapsed()
    }
}

/// Pure timeout classifier for observability.
///
/// The [`TimeoutClassifier`] provides side-effect free timeout classification
/// based on elapsed execution time. It is used to inspect execution duration
/// relative to configured timeouts without performing any enforcement actions.
///
/// # Invariants
///
/// - Classification is purely observational - no side effects or state changes.
/// - Given `(timeout_secs, elapsed)`, classification is deterministic.
#[derive(Debug, Clone)]
pub struct TimeoutClassifier<C = SystemTimeoutClock> {
    clock: C,
}

impl TimeoutClassifier<SystemTimeoutClock> {
    /// Creates a timeout classifier using the default system clock.
    pub fn new() -> Self {
        Self { clock: SystemTimeoutClock }
    }
}

impl Default for TimeoutClassifier<SystemTimeoutClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> TimeoutClassifier<C>
where
    C: TimeoutClock,
{
    /// Creates a timeout classifier with an explicit clock implementation.
    pub fn with_clock(clock: C) -> Self {
        Self { clock }
    }

    /// Classifies elapsed execution relative to a configured timeout.
    ///
    /// This is a pure, side-effect free operation that only inspects
    /// elapsed time and returns a deterministic classification.
    pub fn classify(&self, timeout_secs: Option<u64>, elapsed: Duration) -> TimeoutClassification {
        classify_timeout(timeout_secs, elapsed)
    }

    /// Executes `operation`, captures elapsed time, and classifies timeout outcome.
    ///
    /// The timeout decision is deterministic for a given `(timeout_secs, elapsed)`.
    /// This method wraps the operation with timing but performs no enforcement.
    pub fn execute_and_classify<T, F>(
        &self,
        timeout_secs: Option<u64>,
        operation: F,
    ) -> GuardedExecution<T>
    where
        F: FnOnce() -> T,
    {
        let mark = self.clock.mark_now();
        let value = operation();
        let elapsed = self.clock.elapsed_since(mark);
        let timeout = classify_timeout(timeout_secs, elapsed);

        GuardedExecution { value, elapsed, timeout }
    }
}

/// Deterministic timeout guard wrapper with enforcement.
///
/// The [`TimeoutGuard`] extends [`TimeoutClassifier`] with active timeout enforcement
/// via cancellation signaling when a timeout occurs.
#[derive(Debug, Clone)]
pub struct TimeoutGuard<C = SystemTimeoutClock> {
    classifier: TimeoutClassifier<C>,
    poll_interval: Duration,
}

impl TimeoutGuard<SystemTimeoutClock> {
    /// Creates a timeout guard using the default system clock.
    pub fn new() -> Self {
        Self { classifier: TimeoutClassifier::new(), poll_interval: DEFAULT_WATCHDOG_POLL_INTERVAL }
    }
}

impl Default for TimeoutGuard<SystemTimeoutClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> TimeoutGuard<C>
where
    C: TimeoutClock,
{
    /// Creates a timeout guard with an explicit clock implementation.
    pub fn with_clock(clock: C) -> Self {
        Self {
            classifier: TimeoutClassifier::with_clock(clock),
            poll_interval: DEFAULT_WATCHDOG_POLL_INTERVAL,
        }
    }

    /// Sets the watchdog poll interval. Default is 1ms.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Executes `operation`, captures elapsed time, and classifies timeout outcome.
    ///
    /// The timeout decision is deterministic for a given `(timeout_secs, elapsed)`.
    /// This method is provided for backward compatibility; use [`TimeoutClassifier::execute_and_classify`]
    /// when only classification is needed without any enforcement semantics.
    pub fn execute<T, F>(&self, timeout_secs: Option<u64>, operation: F) -> GuardedExecution<T>
    where
        F: FnOnce() -> T,
    {
        self.classifier.execute_and_classify(timeout_secs, operation)
    }

    /// Executes `operation` with cancellation support, capturing elapsed time and classifying timeout.
    ///
    /// When a timeout is configured, this method starts an attempt-local watchdog worker.
    /// The watchdog sleeps until the timeout deadline and requests cancellation if the
    /// operation has not completed yet. The watchdog is deterministically joined before
    /// returning from this method.
    ///
    /// Timeout classification remains inspectable output, but enforcement does not depend
    /// on post-return classification. Enforcement (cancellation request) can happen during
    /// active handler execution.
    ///
    /// Cleanup invariant: watchdog lifecycle is panic-safe and always joined via scoped
    /// lifecycle teardown before return or unwind propagation.
    pub fn execute_with_cancellation<T, F>(
        &self,
        timeout_secs: Option<u64>,
        operation: F,
    ) -> CancellableExecution<T>
    where
        F: FnOnce(&CancellationContext) -> T,
    {
        let context = CancellationContext::new();
        self.execute_with_external_cancellation(timeout_secs, context, operation)
    }

    /// Executes `operation` with an externally-provided cancellation context.
    ///
    /// Identical to [`execute_with_cancellation`](Self::execute_with_cancellation)
    /// except the caller supplies the [`CancellationContext`]. This allows the
    /// dispatch loop to retain a clone of the context so it can signal
    /// cancellation (e.g. budget exhaustion) while the handler is running.
    pub fn execute_with_external_cancellation<T, F>(
        &self,
        timeout_secs: Option<u64>,
        cancellation_context: CancellationContext,
        operation: F,
    ) -> CancellableExecution<T>
    where
        F: FnOnce(&CancellationContext) -> T,
    {
        let mark = self.classifier.clock.mark_now();
        let watchdog =
            WatchdogLifecycle::new(timeout_secs, &cancellation_context, self.poll_interval);

        let value = operation(&cancellation_context);
        let watchdog_outcome = watchdog.finish();

        let elapsed = self.classifier.clock.elapsed_since(mark);
        let timeout = self.classifier.classify(timeout_secs, elapsed);
        let cancel_requested = watchdog_outcome.cancellation_requested;
        let cancellation_observed = cancellation_context.was_cancellation_observed();
        let cancellation_observation_latency =
            cancellation_context.cancellation_observation_latency();

        CancellableExecution {
            value,
            elapsed,
            timeout,
            cancel_requested,
            cancellation_observed,
            cancellation_observation_latency,
            watchdog_joined: watchdog_outcome.watchdog_joined,
            watchdog_spawn_failed: watchdog_outcome.watchdog_spawn_failed,
        }
    }
}

/// Deterministically classifies elapsed execution relative to configured timeout.
///
/// This is a pure, side-effect free operation suitable for standalone classification
/// without any enforcement machinery.
///
/// # Boundary behavior
///
/// When `elapsed == timeout`, the result is [`TimeoutClassification::CompletedInTime`].
/// Only `elapsed > timeout` yields [`TimeoutClassification::TimedOut`]. Note that the
/// watchdog thread fires at `elapsed >= deadline`, so cancellation may have been
/// requested even for executions classified as `CompletedInTime` (a narrow race where
/// the operation finishes in the same tick the watchdog fires). Callers should inspect
/// [`CancellableExecution::cancel_requested`] for the authoritative cancellation signal.
pub fn classify_timeout(timeout_secs: Option<u64>, elapsed: Duration) -> TimeoutClassification {
    match timeout_secs {
        None => TimeoutClassification::NotConfigured {
            elapsed,
            reason_code: TimeoutReasonCode::NoTimeoutConfigured,
        },
        Some(secs) => {
            let timeout = Duration::from_secs(secs);
            if elapsed > timeout {
                TimeoutClassification::TimedOut(TimeoutFailure {
                    timeout_secs: secs,
                    elapsed,
                    reason_code: TimeoutReasonCode::DeadlineExceeded,
                })
            } else {
                TimeoutClassification::CompletedInTime {
                    timeout_secs: secs,
                    elapsed,
                    reason_code: TimeoutReasonCode::WithinLimit,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::{
        classify_timeout, TimeoutClassification, TimeoutClassifier, TimeoutClock, TimeoutFailure,
        TimeoutGuard, TimeoutReasonCode,
    };

    #[derive(Debug, Clone, Copy)]
    struct FixedClock {
        elapsed: Duration,
    }

    impl TimeoutClock for FixedClock {
        type Mark = ();

        fn mark_now(&self) -> Self::Mark {}

        fn elapsed_since(&self, _mark: Self::Mark) -> Duration {
            self.elapsed
        }
    }

    #[test]
    fn classify_timeout_marks_not_configured_when_absent() {
        let elapsed = Duration::from_secs(3);
        let outcome = classify_timeout(None, elapsed);

        assert_eq!(
            outcome,
            TimeoutClassification::NotConfigured {
                elapsed,
                reason_code: TimeoutReasonCode::NoTimeoutConfigured,
            }
        );
        assert!(!outcome.is_timed_out());
    }

    #[test]
    fn classify_timeout_marks_in_time_when_elapsed_meets_limit() {
        let elapsed = Duration::from_secs(5);
        let outcome = classify_timeout(Some(5), elapsed);

        assert_eq!(
            outcome,
            TimeoutClassification::CompletedInTime {
                timeout_secs: 5,
                elapsed,
                reason_code: TimeoutReasonCode::WithinLimit,
            }
        );
        assert!(!outcome.is_timed_out());
    }

    #[test]
    fn classify_timeout_marks_timeout_when_elapsed_exceeds_limit() {
        let elapsed = Duration::from_secs(6);
        let outcome = classify_timeout(Some(5), elapsed);

        assert_eq!(
            outcome,
            TimeoutClassification::TimedOut(TimeoutFailure {
                timeout_secs: 5,
                elapsed,
                reason_code: TimeoutReasonCode::DeadlineExceeded,
            })
        );
        assert!(outcome.is_timed_out());
    }

    #[test]
    fn guard_wraps_operation_and_reports_timeout_data() {
        let guard = TimeoutGuard::with_clock(FixedClock { elapsed: Duration::from_secs(9) });

        let guarded = guard.execute(Some(3), || 42u32);

        assert_eq!(guarded.value, 42);
        assert_eq!(guarded.elapsed, Duration::from_secs(9));
        assert_eq!(
            guarded.timeout,
            TimeoutClassification::TimedOut(TimeoutFailure {
                timeout_secs: 3,
                elapsed: Duration::from_secs(9),
                reason_code: TimeoutReasonCode::DeadlineExceeded,
            })
        );
    }

    #[test]
    fn classifier_provides_side_effect_free_classification() {
        let classifier =
            TimeoutClassifier::with_clock(FixedClock { elapsed: Duration::from_secs(7) });

        let classification = classifier.classify(Some(5), Duration::from_secs(7));

        assert!(classification.is_timed_out());
        assert_eq!(
            classification,
            TimeoutClassification::TimedOut(TimeoutFailure {
                timeout_secs: 5,
                elapsed: Duration::from_secs(7),
                reason_code: TimeoutReasonCode::DeadlineExceeded,
            })
        );
    }

    #[test]
    fn classifier_execute_and_classify_wraps_operation() {
        let classifier =
            TimeoutClassifier::with_clock(FixedClock { elapsed: Duration::from_secs(4) });

        let result = classifier.execute_and_classify(Some(5), || 42u32);

        assert_eq!(result.value, 42);
        assert_eq!(result.elapsed, Duration::from_secs(4));
        assert_eq!(
            result.timeout,
            TimeoutClassification::CompletedInTime {
                timeout_secs: 5,
                elapsed: Duration::from_secs(4),
                reason_code: TimeoutReasonCode::WithinLimit,
            }
        );
    }

    #[test]
    fn execute_with_cancellation_requests_deadline_signal_during_execution() {
        let guard = TimeoutGuard::new();
        let saw_cancel = Arc::new(AtomicBool::new(false));
        let saw_cancel_clone = Arc::clone(&saw_cancel);

        let result = guard.execute_with_cancellation(Some(0), move |context| {
            for _ in 0..10_000 {
                if context.token().is_cancelled() {
                    saw_cancel_clone.store(true, Ordering::SeqCst);
                    break;
                }
                std::hint::spin_loop();
            }
            9u8
        });

        assert_eq!(result.value, 9);
        assert!(result.cancel_requested);
        assert!(result.cancellation_observed);
        assert!(result.cancellation_observation_latency.is_some());
        assert!(result.watchdog_joined);
        assert!(saw_cancel.load(Ordering::SeqCst));
        assert!(result.timeout.is_timed_out());
    }

    #[test]
    fn execute_with_cancellation_does_not_start_watchdog_when_timeout_is_disabled() {
        let guard = TimeoutGuard::new();
        let result = guard.execute_with_cancellation(None, |context| {
            assert!(!context.token().is_cancelled());
            11u8
        });

        assert_eq!(result.value, 11);
        assert!(!result.cancel_requested);
        assert!(!result.cancellation_observed);
        assert_eq!(result.cancellation_observation_latency, None);
        assert!(!result.watchdog_joined);
        assert!(matches!(
            result.timeout,
            TimeoutClassification::NotConfigured {
                reason_code: TimeoutReasonCode::NoTimeoutConfigured,
                ..
            }
        ));
    }

    #[test]
    fn d01_t_n4a_operation_panic_still_joins_watchdog_cleanup() {
        let baseline = super::active_watchdog_threads();
        let guard = TimeoutGuard::new();

        let panic_result = catch_unwind(AssertUnwindSafe(|| {
            let _ = guard.execute_with_cancellation(Some(30), |_context| -> u8 {
                panic!("operation panic for cleanup-path verification");
            });
        }));

        assert!(panic_result.is_err());

        for _ in 0..100 {
            if super::active_watchdog_threads() == baseline {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }

        assert_eq!(super::active_watchdog_threads(), baseline);
    }

    #[test]
    fn configurable_poll_interval_is_used() {
        let guard = TimeoutGuard::new().with_poll_interval(Duration::from_millis(10));

        let result = guard.execute_with_cancellation(Some(0), move |context| {
            // With a 10ms poll interval, the watchdog should still eventually fire
            for _ in 0..10_000 {
                if context.token().is_cancelled() {
                    return true;
                }
                std::hint::spin_loop();
            }
            false
        });

        assert!(result.cancel_requested);
        assert!(result.watchdog_joined);
        assert!(!result.watchdog_spawn_failed);
    }
}
