//! Once derivation - creates exactly one run for tasks with Once policy.
//!
//! The Once derivation ensures that each task with a Once run policy has
//! exactly one run created. It tracks derived runs to prevent duplicate
//! creation.

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::run_instance::RunInstance;

use crate::derive::{map_run_construction_error, DerivationError, DerivationSuccess};
use crate::time::clock::Clock;

/// Derives exactly one run for a Once policy task.
///
/// If at least one run has already been derived, no new run is created.
pub fn derive_once(
    clock: &impl Clock,
    task_id: TaskId,
    already_derived: u32,
) -> Result<DerivationSuccess, DerivationError> {
    let now = clock.now();

    // If we already have one run derived, nothing to do
    if already_derived >= 1 {
        return Ok(DerivationSuccess::new(Vec::new(), 1));
    }

    // Create one scheduled run (ready immediately for Once policy)
    let run = RunInstance::new_scheduled(
        task_id, now, // scheduled_at: ready immediately for Once
        now, // created_at
    )
    .map_err(|source| map_run_construction_error(task_id, source))?;

    Ok(DerivationSuccess::new(vec![run], 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_once_creates_one_run() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_once(&clock, task_id, 0).unwrap();

        assert_eq!(result.derived().len(), 1);
        assert_eq!(result.already_derived(), 1);
        assert_eq!(result.derived()[0].task_id(), task_id);
        assert_eq!(result.derived()[0].state(), actionqueue_core::run::state::RunState::Scheduled);
        assert_eq!(result.derived()[0].scheduled_at(), 1000);
        assert_eq!(result.derived()[0].created_at(), 1000);
    }

    #[test]
    fn derive_once_does_not_duplicate() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = TaskId::new();

        // First derivation
        let result1 = derive_once(&clock, task_id, 0).unwrap();
        assert_eq!(result1.derived().len(), 1);

        // Second derivation should not create another run
        let result2 = derive_once(&clock, task_id, 1).unwrap();

        assert_eq!(result2.derived().len(), 0);
        assert_eq!(result2.already_derived(), 1);
    }

    #[test]
    fn derive_once_returns_typed_error_for_nil_task_id_instead_of_panicking() {
        let clock = crate::time::clock::MockClock::new(1000);
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");

        let result = derive_once(&clock, task_id, 0);

        assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
    }
}
