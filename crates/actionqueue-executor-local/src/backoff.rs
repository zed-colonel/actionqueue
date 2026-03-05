//! Backoff strategies for retry delay computation.
//!
//! This module provides pluggable backoff strategies that compute the delay
//! before a retried run becomes eligible for re-promotion from `RetryWait`
//! to `Ready`.

use std::time::Duration;

/// Error returned when backoff configuration is invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackoffConfigError {
    /// Base delay exceeds maximum delay.
    BaseExceedsMax {
        /// The base delay that was too large.
        base: Duration,
        /// The maximum delay that was exceeded.
        max: Duration,
    },
}

impl std::fmt::Display for BackoffConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackoffConfigError::BaseExceedsMax { base, max } => {
                write!(f, "base delay ({base:?}) must not exceed max delay ({max:?})")
            }
        }
    }
}

impl std::error::Error for BackoffConfigError {}

/// A strategy for computing retry delay based on attempt number.
pub trait BackoffStrategy: Send + Sync {
    /// Returns the delay before the given attempt should be retried.
    ///
    /// `attempt_number` is 1-indexed: the first retry after the initial attempt
    /// is `attempt_number = 1`.
    fn delay_for_attempt(&self, attempt_number: u32) -> Duration;
}

/// Fixed-interval backoff: every retry waits the same duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedBackoff {
    /// The constant interval between retries.
    interval: Duration,
}

impl FixedBackoff {
    /// Creates a new fixed backoff strategy with the given interval.
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }

    /// Returns the constant interval between retries.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

impl BackoffStrategy for FixedBackoff {
    fn delay_for_attempt(&self, _attempt_number: u32) -> Duration {
        self.interval
    }
}

/// Exponential backoff: delay doubles with each attempt, capped at a maximum.
///
/// The delay for attempt N is `min(base * 2^(N-1), max)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
}

impl ExponentialBackoff {
    /// Creates a new exponential backoff strategy.
    ///
    /// # Errors
    /// Returns [`BackoffConfigError::BaseExceedsMax`] if `base` exceeds `max`.
    pub fn new(base: Duration, max: Duration) -> Result<Self, BackoffConfigError> {
        if base > max {
            return Err(BackoffConfigError::BaseExceedsMax { base, max });
        }
        Ok(Self { base, max })
    }

    /// Returns the base delay.
    pub fn base(&self) -> Duration {
        self.base
    }

    /// Returns the maximum delay cap.
    pub fn max(&self) -> Duration {
        self.max
    }
}

impl BackoffStrategy for ExponentialBackoff {
    fn delay_for_attempt(&self, attempt_number: u32) -> Duration {
        debug_assert!(attempt_number >= 1, "attempt_number is 1-indexed");
        let exponent = attempt_number.saturating_sub(1);
        let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let base_millis = u64::try_from(self.base.as_millis()).unwrap_or(u64::MAX);
        let delay_millis = base_millis.saturating_mul(multiplier);
        let delay = Duration::from_millis(delay_millis);
        delay.min(self.max)
    }
}

/// Computes the timestamp at which a retried run becomes ready.
///
/// `retry_wait_entered_at` is the timestamp when the run entered `RetryWait`.
/// Returns `retry_wait_entered_at + backoff_delay`, saturating at `u64::MAX`.
pub fn retry_ready_at(
    retry_wait_entered_at: u64,
    attempt_number: u32,
    strategy: &dyn BackoffStrategy,
) -> u64 {
    let delay = strategy.delay_for_attempt(attempt_number);
    let delay_secs = delay.as_secs().saturating_add(u64::from(delay.subsec_nanos() > 0));
    retry_wait_entered_at.saturating_add(delay_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_backoff_returns_constant_delay() {
        let strategy = FixedBackoff::new(Duration::from_secs(5));

        assert_eq!(strategy.delay_for_attempt(1), Duration::from_secs(5));
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_secs(5));
        assert_eq!(strategy.delay_for_attempt(100), Duration::from_secs(5));
    }

    #[test]
    fn exponential_backoff_doubles_per_attempt() {
        let strategy =
            ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(3600)).unwrap();

        assert_eq!(strategy.delay_for_attempt(1), Duration::from_secs(1)); // 1 * 2^0
        assert_eq!(strategy.delay_for_attempt(2), Duration::from_secs(2)); // 1 * 2^1
        assert_eq!(strategy.delay_for_attempt(3), Duration::from_secs(4)); // 1 * 2^2
        assert_eq!(strategy.delay_for_attempt(4), Duration::from_secs(8)); // 1 * 2^3
    }

    #[test]
    fn exponential_backoff_caps_at_max() {
        let strategy =
            ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(10)).unwrap();

        assert_eq!(strategy.delay_for_attempt(1), Duration::from_secs(1));
        assert_eq!(strategy.delay_for_attempt(4), Duration::from_secs(8));
        assert_eq!(strategy.delay_for_attempt(5), Duration::from_secs(10)); // capped
        assert_eq!(strategy.delay_for_attempt(100), Duration::from_secs(10)); // still capped
    }

    #[test]
    fn exponential_backoff_saturates_on_overflow() {
        let strategy =
            ExponentialBackoff::new(Duration::from_secs(1000), Duration::from_millis(u64::MAX))
                .unwrap();

        // Very high attempt should saturate, not panic
        let delay = strategy.delay_for_attempt(u32::MAX);
        assert!(delay.as_millis() > 0);
    }

    #[test]
    fn retry_ready_at_adds_delay_to_current_time() {
        let strategy = FixedBackoff::new(Duration::from_secs(30));

        assert_eq!(retry_ready_at(1000, 1, &strategy), 1030);
        assert_eq!(retry_ready_at(1000, 5, &strategy), 1030);
    }

    #[test]
    fn retry_ready_at_saturates_at_u64_max() {
        let strategy = FixedBackoff::new(Duration::from_secs(100));

        assert_eq!(retry_ready_at(u64::MAX - 10, 1, &strategy), u64::MAX);
    }

    #[test]
    fn retry_ready_at_rounds_up_sub_second_delay() {
        let strategy = FixedBackoff::new(Duration::from_millis(500));

        // 500ms should round up to 1 second, not truncate to 0
        assert_eq!(retry_ready_at(1000, 1, &strategy), 1001);
    }

    #[test]
    fn retry_ready_at_exact_seconds_not_rounded_up() {
        let strategy = FixedBackoff::new(Duration::from_secs(3));

        // Exact 3s should produce exactly +3, not +4
        assert_eq!(retry_ready_at(1000, 1, &strategy), 1003);
    }

    #[test]
    fn retry_ready_at_exponential_produces_increasing_delays() {
        let strategy =
            ExponentialBackoff::new(Duration::from_secs(10), Duration::from_secs(3600)).unwrap();

        let t1 = retry_ready_at(1000, 1, &strategy);
        let t2 = retry_ready_at(1000, 2, &strategy);
        let t3 = retry_ready_at(1000, 3, &strategy);

        assert_eq!(t1, 1010); // 1000 + 10
        assert_eq!(t2, 1020); // 1000 + 20
        assert_eq!(t3, 1040); // 1000 + 40
        assert!(t1 < t2);
        assert!(t2 < t3);
    }
}
