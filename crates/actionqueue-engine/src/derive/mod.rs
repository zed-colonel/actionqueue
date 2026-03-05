//! Derivation logic for task run creation.
//!
//! This module handles deriving runs from task specifications according
//! to their run policies.

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::RunInstanceConstructionError;
use actionqueue_core::task::run_policy::{RunPolicy, RunPolicyError};

use crate::time::clock::Clock;

#[cfg(feature = "workflow")]
pub mod cron;
pub mod once;
pub mod repeat;

/// Error returned when run derivation fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DerivationError {
    /// The run policy payload is invalid for derivation.
    InvalidPolicy {
        /// The typed policy validation failure.
        source: RunPolicyError,
    },
    /// Arithmetic overflow would occur during run derivation.
    /// This indicates the derivation request cannot be fulfilled with representable values.
    ArithmeticOverflow {
        /// The run policy that caused the overflow.
        policy: String,
        /// Description of the overflowed calculation.
        operation: String,
    },
    /// Run construction failed because the task identifier is nil/invalid.
    InvalidTaskIdForRunConstruction {
        /// Rejected task identifier.
        task_id: TaskId,
    },
    /// Run construction failed for a reason other than task identifier shape.
    RunConstructionFailed {
        /// Task identifier associated with the failed construction.
        task_id: TaskId,
        /// Typed construction failure returned by core run constructors.
        source: RunInstanceConstructionError,
    },
}

impl std::fmt::Display for DerivationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DerivationError::InvalidPolicy { source } => {
                write!(f, "invalid run policy for derivation: {source}")
            }
            DerivationError::ArithmeticOverflow { policy, operation } => {
                write!(f, "arithmetic overflow in {policy} derivation: {operation}")
            }
            DerivationError::InvalidTaskIdForRunConstruction { task_id } => {
                write!(f, "invalid task identifier for run derivation: {task_id}")
            }
            DerivationError::RunConstructionFailed { task_id, source } => {
                write!(f, "run construction failed during derivation for task {task_id}: {source}")
            }
        }
    }
}

impl std::error::Error for DerivationError {}

/// Maps a typed run-construction failure into a deterministic derivation error.
pub(crate) fn map_run_construction_error(
    task_id: TaskId,
    source: RunInstanceConstructionError,
) -> DerivationError {
    match source {
        RunInstanceConstructionError::InvalidTaskId { task_id } => {
            DerivationError::InvalidTaskIdForRunConstruction { task_id }
        }
        _ => DerivationError::RunConstructionFailed { task_id, source },
    }
}

/// Result of deriving runs for a task from its run policy.
pub type DerivationResult = Result<DerivationSuccess, DerivationError>;

/// Success result from run derivation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct DerivationSuccess {
    /// Newly derived run instances.
    derived: Vec<RunInstance>,
    /// Total number of runs considered derived for the task after this call.
    already_derived: u32,
}

impl DerivationSuccess {
    /// Creates a new derivation success result.
    pub(crate) fn new(derived: Vec<RunInstance>, already_derived: u32) -> Self {
        Self { derived, already_derived }
    }

    /// Returns the newly derived run instances.
    pub fn derived(&self) -> &[RunInstance] {
        &self.derived
    }

    /// Consumes self and returns the newly derived run instances.
    pub fn into_derived(self) -> Vec<RunInstance> {
        self.derived
    }

    /// Returns the total number of runs considered derived for the task after this call.
    pub fn already_derived(&self) -> u32 {
        self.already_derived
    }
}

/// Pre-derivation validation for run policies.
///
/// This function validates that a run policy can be derived without
/// arithmetic overflow or other impossible conditions before any
/// derivation work is performed.
///
/// # Arguments
///
/// * `run_policy` - The run policy to validate
/// * `already_derived` - The number of runs already derived
/// * `schedule_origin` - The base timestamp for repeat policy scheduling
///
/// # Returns
///
/// * `Ok(())` if the policy is valid for derivation
/// * `Err(DerivationError)` if the policy cannot be derived
fn validate_derivation(
    run_policy: &RunPolicy,
    already_derived: u32,
    schedule_origin: u64,
) -> Result<(), DerivationError> {
    run_policy.validate().map_err(|source| DerivationError::InvalidPolicy { source })?;

    match run_policy {
        RunPolicy::Once => {
            // Once policy: already_derived should not exceed 1
            if already_derived > 1 {
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Once".to_string(),
                    operation: format!(
                        "invalid state: already_derived ({already_derived}) > max (1)"
                    ),
                });
            }
            Ok(())
        }
        #[cfg(feature = "workflow")]
        RunPolicy::Cron(_) => {
            // Cron expression is validated at CronPolicy::new(); no arithmetic
            // overflow is possible since occurrences are computed on-demand.
            let _ = (already_derived, schedule_origin);
            Ok(())
        }
        RunPolicy::Repeat(ref policy) => {
            let count = policy.count();
            let interval_secs = policy.interval_secs();
            // Validate count values - count should be >= already_derived
            if already_derived > count {
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Repeat".to_string(),
                    operation: format!(
                        "invalid state: already_derived ({already_derived}) > count ({count})"
                    ),
                });
            }

            // Check that count and interval don't cause overflow
            // The maximum index we'll compute is (count - 1)
            let max_index = count - 1;

            // Convert to u64 for multiplication check
            let max_index_u64: u64 = max_index as u64;
            let interval_u64: u64 = interval_secs;

            // Check multiplication: max_index * interval_secs
            if max_index_u64.checked_mul(interval_u64).is_none() {
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Repeat".to_string(),
                    operation: format!(
                        "multiplication overflow: max_index {max_index_u64} * interval \
                         {interval_u64}"
                    ),
                });
            }

            // Check that schedule_origin + (count - 1) * interval_secs doesn't overflow
            // This is the maximum scheduled_at value we'll compute
            let max_product = max_index_u64 * interval_u64;
            if schedule_origin.checked_add(max_product).is_none() {
                return Err(DerivationError::ArithmeticOverflow {
                    policy: "Repeat".to_string(),
                    operation: format!(
                        "addition overflow: schedule_origin {schedule_origin} + max_product \
                         {max_product}"
                    ),
                });
            }

            Ok(())
        }
    }
}

/// Derives runs from a task run policy.
///
/// This function is the policy-routing boundary for run derivation. Concrete
/// policy behavior is implemented in policy-specific modules.
///
/// `schedule_origin` is the stable base timestamp used by repeat policies to
/// compute deterministic scheduled times.
pub fn derive_runs(
    clock: &impl Clock,
    task_id: TaskId,
    run_policy: &RunPolicy,
    already_derived: u32,
    schedule_origin: u64,
) -> DerivationResult {
    // Validate the policy before derivation to catch impossible cases early
    validate_derivation(run_policy, already_derived, schedule_origin)?;

    match run_policy {
        RunPolicy::Once => once::derive_once(clock, task_id, already_derived),
        RunPolicy::Repeat(ref policy) => repeat::derive_repeat(
            clock,
            task_id,
            repeat::RepeatDerivationParams::new(
                policy.count(),
                policy.interval_secs(),
                already_derived,
                schedule_origin,
            ),
        ),
        #[cfg(feature = "workflow")]
        RunPolicy::Cron(ref policy) => {
            // `schedule_origin` serves as the "start looking from" timestamp for
            // cron. The initial window of CRON_WINDOW_SIZE runs is derived here.
            // Subsequent rolling-window derivation is handled by `derive_cron_runs`
            // in the dispatch loop, which bypasses this path.
            cron::derive_cron_initial(clock, task_id, policy, already_derived, schedule_origin)
        }
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::RunId;
    use actionqueue_core::task::run_policy::RepeatPolicy;

    use super::*;

    #[test]
    fn derive_runs_routes_once_policy() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_runs(&clock, task_id, &RunPolicy::Once, 0, 0)
            .expect("Once derivation must succeed");

        assert_eq!(result.derived().len(), 1);
        assert_eq!(result.already_derived(), 1);
        assert_eq!(result.derived()[0].scheduled_at(), 1000);
    }

    #[test]
    fn derive_runs_routes_repeat_policy_with_stable_origin() {
        let mut clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();
        let run_policy = RunPolicy::repeat(4, 60).expect("policy should be valid");

        let first = derive_runs(&clock, task_id, &run_policy, 0, 900)
            .expect("Repeat derivation must succeed for valid inputs");
        assert_eq!(first.derived().len(), 4);
        assert_eq!(first.derived()[0].scheduled_at(), 900);
        assert_eq!(first.derived()[1].scheduled_at(), 960);
        assert_eq!(first.derived()[2].scheduled_at(), 1020);
        assert_eq!(first.derived()[3].scheduled_at(), 1080);

        clock.advance_by(500);

        let second = derive_runs(&clock, task_id, &run_policy, 2, 900)
            .expect("Repeat derivation must succeed for valid inputs");
        assert_eq!(second.derived().len(), 2);
        assert_eq!(second.derived()[0].scheduled_at(), 1020);
        assert_eq!(second.derived()[1].scheduled_at(), 1080);
    }

    #[test]
    fn validate_derivation_once_rejects_invalid_already_derived() {
        // already_derived > 1 should be rejected
        let result = validate_derivation(&RunPolicy::Once, 2, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_derivation_repeat_rejects_already_derived_exceeds_count() {
        // already_derived > count should be rejected
        let policy = RunPolicy::repeat(3, 60).expect("policy should be valid");
        let result = validate_derivation(&policy, 5, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_derivation_repeat_rejects_multiplication_overflow() {
        // count * interval would overflow u64
        let policy = RunPolicy::repeat(u32::MAX, u64::MAX).expect("policy should be valid");
        let result = validate_derivation(&policy, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn validate_derivation_repeat_rejects_addition_overflow() {
        // schedule_origin + (count - 1) * interval_secs would overflow u64
        let large_origin = u64::MAX - 100;
        let policy = RunPolicy::repeat(100, u64::MAX / 50).expect("policy should be valid");
        let result = validate_derivation(&policy, 0, large_origin);
        assert!(result.is_err());
    }

    #[test]
    fn validate_derivation_repeat_accepts_valid_values() {
        // Valid values should pass validation
        let result = validate_derivation(
            &RunPolicy::repeat(10, 60).expect("policy should be valid"),
            0,
            1000,
        );
        assert!(result.is_ok());

        // With already_derived
        let result = validate_derivation(
            &RunPolicy::repeat(10, 60).expect("policy should be valid"),
            5,
            1000,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn validate_derivation_repeat_rejects_zero_count_as_invalid_policy() {
        let result = RepeatPolicy::new(0, 60);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatCount { count: 0 }));
    }

    #[test]
    fn validate_derivation_repeat_rejects_zero_interval_as_invalid_policy() {
        let result = RepeatPolicy::new(3, 0);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 }));
    }

    #[test]
    fn map_run_construction_error_returns_dedicated_invalid_task_id_variant() {
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");

        let mapped = map_run_construction_error(
            task_id,
            RunInstanceConstructionError::InvalidTaskId { task_id },
        );

        assert_eq!(mapped, DerivationError::InvalidTaskIdForRunConstruction { task_id });
    }

    #[test]
    fn map_run_construction_error_preserves_non_task_id_failures() {
        let task_id = "00000000-0000-0000-0000-000000000002"
            .parse::<TaskId>()
            .expect("task id literal must parse");
        let run_id = "00000000-0000-0000-0000-000000000001"
            .parse::<RunId>()
            .expect("run id literal must parse");
        let source = RunInstanceConstructionError::ReadyScheduledAtAfterCreatedAt {
            run_id,
            scheduled_at: 2_000,
            created_at: 1_000,
        };

        let mapped = map_run_construction_error(task_id, source);

        assert_eq!(mapped, DerivationError::RunConstructionFailed { task_id, source });
    }
}
