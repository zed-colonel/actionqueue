//! RetryWait-to-Ready promotion logic using backoff strategies.
//!
//! This module provides functionality for promoting runs from the RetryWait
//! state to the Ready state when their computed backoff delay has elapsed.

use actionqueue_core::run::run_instance::{RunInstance, RunInstanceError};
use actionqueue_core::run::state::RunState;
use actionqueue_executor_local::backoff::BackoffStrategy;

/// Result of promoting RetryWait runs to Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct RetryPromotionResult {
    /// Runs that were promoted from RetryWait to Ready.
    promoted: Vec<RunInstance>,
    /// Runs that remain in RetryWait state (backoff delay not yet elapsed).
    still_waiting: Vec<RunInstance>,
}

impl RetryPromotionResult {
    /// Returns the runs that were promoted from RetryWait to Ready.
    pub fn promoted(&self) -> &[RunInstance] {
        &self.promoted
    }

    /// Returns the runs that remain in RetryWait state.
    pub fn still_waiting(&self) -> &[RunInstance] {
        &self.still_waiting
    }
}

/// Promotes RetryWait runs to Ready based on backoff delay computation.
///
/// For each run in `retry_waiting`, the function computes the `retry_ready_at`
/// timestamp from the run's `attempt_count` and the provided backoff strategy.
/// Runs whose computed ready time is `<= current_time` are promoted.
///
/// # Arguments
///
/// * `retry_waiting` - Runs currently in RetryWait state
/// * `current_time` - The current time according to the scheduler clock
/// * `strategy` - The backoff strategy used to compute retry delays
pub fn promote_retry_wait_to_ready(
    retry_waiting: &[RunInstance],
    current_time: u64,
    strategy: &dyn BackoffStrategy,
) -> Result<RetryPromotionResult, RunInstanceError> {
    let mut promoted = Vec::new();
    let mut still_waiting = Vec::new();

    for run in retry_waiting {
        if run.state() != RunState::RetryWait {
            return Err(RunInstanceError::InvalidTransition {
                run_id: run.id(),
                from: run.state(),
                to: RunState::Ready,
            });
        }

        let attempt_count = run.attempt_count();
        let retry_wait_entered_at = run.last_state_change_at();
        let ready_at = actionqueue_executor_local::backoff::retry_ready_at(
            retry_wait_entered_at,
            attempt_count,
            strategy,
        );

        if ready_at <= current_time {
            // Zero delay or immediate — promote now
            let mut ready_run = run.clone();
            ready_run.transition_to(RunState::Ready)?;
            promoted.push(ready_run);
        } else {
            still_waiting.push(run.clone());
        }
    }

    Ok(RetryPromotionResult { promoted, still_waiting })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::run_instance::RunInstance;
    use actionqueue_core::run::state::RunState;
    use actionqueue_executor_local::backoff::{ExponentialBackoff, FixedBackoff};

    use super::*;

    fn make_retry_wait_run_at(
        task_id: TaskId,
        attempt_count: u32,
        retry_wait_entered_at: u64,
    ) -> RunInstance {
        use actionqueue_core::ids::AttemptId;

        let mut run = RunInstance::new_scheduled(task_id, 0, 0).expect("valid run");
        run.transition_to(RunState::Ready).expect("valid transition");
        run.transition_to(RunState::Leased).expect("valid transition");
        run.transition_to(RunState::Running).expect("valid transition");
        run.start_attempt(AttemptId::new()).expect("start attempt");
        run.finish_attempt(run.current_attempt_id().unwrap()).expect("finish attempt");
        run.transition_to(RunState::RetryWait).expect("valid transition");

        // Simulate additional attempts by cycling through states
        for _ in 1..attempt_count {
            run.transition_to(RunState::Ready).expect("valid transition");
            run.transition_to(RunState::Leased).expect("valid transition");
            run.transition_to(RunState::Running).expect("valid transition");
            run.start_attempt(AttemptId::new()).expect("start attempt");
            run.finish_attempt(run.current_attempt_id().unwrap()).expect("finish attempt");
            run.transition_to(RunState::RetryWait).expect("valid transition");
        }

        // Record when the run entered RetryWait (simulates reducer behavior)
        run.record_state_change_at(retry_wait_entered_at);
        run
    }

    #[test]
    fn fixed_backoff_not_promoted_before_delay_elapses() {
        let task_id = TaskId::new();
        // Run entered RetryWait at t=970, delay=30 → ready_at=1000
        let run = make_retry_wait_run_at(task_id, 1, 970);
        let strategy = FixedBackoff::new(Duration::from_secs(30));

        // At t=999, delay hasn't elapsed yet
        let result = promote_retry_wait_to_ready(std::slice::from_ref(&run), 999, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert!(result.promoted().is_empty());
        assert_eq!(result.still_waiting().len(), 1);
    }

    #[test]
    fn fixed_backoff_promoted_after_delay_elapses() {
        let task_id = TaskId::new();
        // Run entered RetryWait at t=970, delay=30 → ready_at=1000
        let run = make_retry_wait_run_at(task_id, 1, 970);
        let strategy = FixedBackoff::new(Duration::from_secs(30));

        // At t=1000, delay has elapsed (ready_at=1000 <= current_time=1000)
        let result = promote_retry_wait_to_ready(std::slice::from_ref(&run), 1000, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert_eq!(result.promoted().len(), 1);
        assert_eq!(result.promoted()[0].state(), RunState::Ready);
        assert!(result.still_waiting().is_empty());
    }

    #[test]
    fn fixed_backoff_zero_delay_promotes_immediately() {
        let task_id = TaskId::new();
        let run = make_retry_wait_run_at(task_id, 1, 1000);
        let strategy = FixedBackoff::new(Duration::from_secs(0));

        // Zero delay: ready_at = 1000 + 0 = 1000, which is <= 1000
        let result = promote_retry_wait_to_ready(&[run], 1000, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert_eq!(result.promoted().len(), 1);
        assert_eq!(result.promoted()[0].state(), RunState::Ready);
        assert!(result.still_waiting().is_empty());
    }

    #[test]
    fn exponential_backoff_produces_increasing_delays() {
        let task_id = TaskId::new();
        // Both entered RetryWait at t=990
        let run1 = make_retry_wait_run_at(task_id, 1, 990); // attempt 1
        let run2 = make_retry_wait_run_at(task_id, 2, 990); // attempt 2

        let strategy =
            ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(3600)).unwrap();

        // At t=999: neither promoted
        // attempt 1: ready_at = 990 + 10 = 1000 → not yet (1000 > 999)
        // attempt 2: ready_at = 990 + 20 = 1010 → not yet (1010 > 999)
        let result = promote_retry_wait_to_ready(&[run1.clone(), run2.clone()], 999, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert!(result.promoted().is_empty());
        assert_eq!(result.still_waiting().len(), 2);

        // At t=1000: only attempt 1 promoted (1000 <= 1000), attempt 2 still waiting
        let result = promote_retry_wait_to_ready(&[run1.clone(), run2.clone()], 1000, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert_eq!(result.promoted().len(), 1);
        assert_eq!(result.still_waiting().len(), 1);

        // At t=1010: both promoted
        let result = promote_retry_wait_to_ready(&[run1, run2], 1010, &strategy)
            .expect("promotion should succeed for valid RetryWait runs");
        assert_eq!(result.promoted().len(), 2);
        assert!(result.still_waiting().is_empty());
    }

    #[test]
    fn empty_input_returns_empty_result() {
        let strategy = FixedBackoff::new(Duration::from_secs(5));
        let result = promote_retry_wait_to_ready(&[], 1000, &strategy)
            .expect("promotion should succeed for empty input");
        assert!(result.promoted().is_empty());
        assert!(result.still_waiting().is_empty());
    }

    #[test]
    fn non_retry_wait_input_returns_error() {
        let task_id = TaskId::new();
        let run = RunInstance::new_scheduled(task_id, 0, 0).expect("valid run");
        assert_eq!(run.state(), RunState::Scheduled);

        let strategy = FixedBackoff::new(Duration::from_secs(5));
        let result = promote_retry_wait_to_ready(&[run], 1000, &strategy);

        assert!(
            matches!(
                &result,
                Err(RunInstanceError::InvalidTransition {
                    from: RunState::Scheduled,
                    to: RunState::Ready,
                    ..
                })
            ),
            "expected InvalidTransition for non-RetryWait input, got: {result:?}"
        );
    }

    #[test]
    fn extreme_attempt_count_does_not_panic() {
        let task_id = TaskId::new();
        // Build a run with a high attempt count (10 cycles) to exercise
        // exponential backoff with large attempt numbers. The backoff
        // computation must saturate instead of panicking.
        let run = make_retry_wait_run_at(task_id, 10, 0);
        assert_eq!(run.attempt_count(), 10);
        assert_eq!(run.state(), RunState::RetryWait);

        let strategy =
            ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(3600)).unwrap();

        // With current_time=u64::MAX, even a saturating backoff delay should
        // be <= u64::MAX, so the run must be promoted.
        let result = promote_retry_wait_to_ready(std::slice::from_ref(&run), u64::MAX, &strategy)
            .expect("extreme attempt count must not panic");

        assert_eq!(result.promoted().len(), 1);
        assert_eq!(result.promoted()[0].state(), RunState::Ready);
        assert!(result.still_waiting().is_empty());
    }
}
