//! Repeat derivation - creates multiple runs for tasks with Repeat policy.
//!
//! The Repeat derivation ensures that each task with a Repeat run policy has
//! exactly N runs created according to the specified count and interval.
//! It tracks derived runs to prevent duplicate creation.

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::task::run_policy::RunPolicyError;

use crate::derive::{map_run_construction_error, DerivationError, DerivationSuccess};
use crate::time::clock::Clock;

/// Parameters for repeat derivation that group the repeat policy fields
/// with derivation state.
pub struct RepeatDerivationParams {
    /// Number of runs to derive for the repeat policy.
    count: u32,
    /// Interval in seconds between successive runs.
    interval_secs: u64,
    /// Number of runs already derived (avoids duplicates).
    already_derived: u32,
    /// Stable base timestamp for deterministic schedule calculation.
    schedule_origin: u64,
}

impl RepeatDerivationParams {
    /// Creates a new set of repeat derivation parameters.
    pub fn new(count: u32, interval_secs: u64, already_derived: u32, schedule_origin: u64) -> Self {
        Self { count, interval_secs, already_derived, schedule_origin }
    }

    /// Returns the number of runs to derive.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Returns the interval in seconds between successive runs.
    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    /// Returns the number of runs already derived.
    pub fn already_derived(&self) -> u32 {
        self.already_derived
    }

    /// Returns the stable base timestamp for schedule calculation.
    pub fn schedule_origin(&self) -> u64 {
        self.schedule_origin
    }
}

/// Derives runs for a Repeat policy task.
///
/// Creates runs up to the count if they don't exist, spaced by the interval.
/// Returns a DerivationSuccess with newly created runs and the total policy count.
///
/// # Errors
///
/// Returns [`DerivationError::ArithmeticOverflow`] if the schedule time calculation
/// `schedule_origin + (index * interval_secs)` would overflow `u64::MAX` for any index.
/// This ensures strict exact-N accounting - derivation either produces exactly N runs
/// or fails with a typed error.
///
/// Returns [`DerivationError::InvalidPolicy`] if `count == 0` or
/// `interval_secs == 0`, preserving defensive contract enforcement for callers
/// that bypass the primary run-policy boundary.
///
/// `schedule_origin` is the stable base timestamp for repeat scheduling. Derived
/// run times are always `schedule_origin + index * interval_secs`, independent
/// of the current clock time, ensuring deterministic re-derivation.
pub fn derive_repeat(
    clock: &impl Clock,
    task_id: TaskId,
    params: RepeatDerivationParams,
) -> Result<DerivationSuccess, DerivationError> {
    let RepeatDerivationParams { count, interval_secs, already_derived, schedule_origin } = params;

    if count == 0 {
        return Err(DerivationError::InvalidPolicy {
            source: RunPolicyError::InvalidRepeatCount { count },
        });
    }

    if interval_secs == 0 {
        return Err(DerivationError::InvalidPolicy {
            source: RunPolicyError::InvalidRepeatIntervalSecs { interval_secs },
        });
    }

    let now = clock.now();
    let mut derived = Vec::new();

    // Derive only missing runs, with deterministic schedule positions.
    // Use checked arithmetic to detect overflow.
    for i in already_derived..count {
        // Calculate: schedule_origin + (i as u64 * interval_secs)
        // Fail with typed error if overflow would occur.
        let idx: u64 = i as u64;
        let product = match idx.checked_mul(interval_secs) {
            Some(p) => p,
            None => {
                // Overflow in multiplication: cannot derive remaining runs safely.
                // Return typed error instead of partial result.
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Repeat".to_string(),
                    operation: format!("multiplication: index {idx} * interval {interval_secs}"),
                });
            }
        };

        let scheduled_at = match schedule_origin.checked_add(product) {
            Some(sa) => sa,
            None => {
                // Overflow in addition: cannot derive remaining runs safely.
                // Return typed error instead of partial result.
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Repeat".to_string(),
                    operation: format!(
                        "addition: schedule_origin {schedule_origin} + product {product}"
                    ),
                });
            }
        };

        let run = RunInstance::new_scheduled(task_id, scheduled_at, now)
            .map_err(|source| map_run_construction_error(task_id, source))?;

        derived.push(run);
    }

    Ok(DerivationSuccess::new(derived, count))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(
        count: u32,
        interval_secs: u64,
        already_derived: u32,
        schedule_origin: u64,
    ) -> RepeatDerivationParams {
        RepeatDerivationParams::new(count, interval_secs, already_derived, schedule_origin)
    }

    #[test]
    fn derive_repeat_creates_correct_count() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_repeat(&clock, task_id, params(3, 60, 0, 1000)).unwrap();

        assert_eq!(result.derived().len(), 3);
        assert_eq!(result.already_derived(), 3);

        // Check the scheduled times
        assert_eq!(result.derived()[0].scheduled_at(), 1000); // t=1000
        assert_eq!(result.derived()[1].scheduled_at(), 1060); // t=1000 + 60
        assert_eq!(result.derived()[2].scheduled_at(), 1120); // t=1000 + 120
    }

    #[test]
    fn derive_repeat_does_not_duplicate() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        // First derivation creates 5 runs
        let result1 = derive_repeat(&clock, task_id, params(5, 60, 0, 1000)).unwrap();
        assert_eq!(result1.derived().len(), 5);

        // Second derivation with already_derived=2 should create 3 more
        let result2 = derive_repeat(&clock, task_id, params(5, 60, 2, 1000)).unwrap();

        assert_eq!(result2.derived().len(), 3);
        assert_eq!(result2.already_derived(), 5);
    }

    #[test]
    fn derive_repeat_rejects_zero_count() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_repeat(&clock, task_id, params(0, 60, 0, 1000));

        assert_eq!(
            result,
            Err(DerivationError::InvalidPolicy {
                source: RunPolicyError::InvalidRepeatCount { count: 0 },
            })
        );
    }

    #[test]
    fn derive_repeat_rejects_zero_interval() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_repeat(&clock, task_id, params(3, 0, 0, 1000));

        assert_eq!(
            result,
            Err(DerivationError::InvalidPolicy {
                source: RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 },
            })
        );
    }

    #[test]
    fn derive_repeat_remains_stable_when_clock_advances() {
        let mut clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let first = derive_repeat(&clock, task_id, params(4, 60, 0, 900)).unwrap();
        assert_eq!(first.derived().len(), 4);
        assert_eq!(first.derived()[0].scheduled_at(), 900);
        assert_eq!(first.derived()[1].scheduled_at(), 960);
        assert_eq!(first.derived()[2].scheduled_at(), 1020);
        assert_eq!(first.derived()[3].scheduled_at(), 1080);

        clock.advance_by(600);

        let second = derive_repeat(&clock, task_id, params(4, 60, 2, 900)).unwrap();
        assert_eq!(second.derived().len(), 2);
        assert_eq!(second.derived()[0].scheduled_at(), 1020);
        assert_eq!(second.derived()[1].scheduled_at(), 1080);
    }

    #[test]
    fn derive_repeat_returns_typed_error_for_nil_task_id_without_partial_derivation() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");

        let result = derive_repeat(&clock, task_id, params(4, 60, 0, 1000));

        assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
    }
}
