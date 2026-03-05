//! Lease acquisition logic.
//!
//! This module implements the lease acquisition behavior:
//! - A lease may be acquired when a run has no active lease.
//! - Acquisition fails if a lease already exists for the run.
//!
//! The one-active-lease-per-run invariant is enforced at acquisition time.

use actionqueue_core::ids::RunId;

use crate::lease::model::{Lease, LeaseExpiry, LeaseOwner};

/// Result of a lease acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum AcquireResult {
    /// Lease was successfully acquired.
    Acquired(Lease),
    /// Lease acquisition failed because the run already has an active lease.
    AlreadyLeased,
    /// Lease acquisition failed because the provided lease slot belongs to a
    /// different run than requested.
    WrongRunSlot {
        /// Run id requested by this acquisition attempt.
        requested_run_id: RunId,
        /// Run id currently occupying the provided lease slot.
        existing_run_id: RunId,
    },
}

/// Attempt to acquire a lease for a run.
///
/// Returns `AcquireResult::Acquired` if no lease exists for the run.
/// Returns `AcquireResult::AlreadyLeased` if the run already has an active lease.
///
/// `current_lease` is expected to be the lease slot for `run_id`.
/// If `current_lease` is occupied by the same run, acquisition is rejected to
/// preserve one-active-lease-per-run.
/// If `current_lease` is occupied by a different run, the function returns
/// `AcquireResult::WrongRunSlot` to prevent false same-run rejection.
pub fn acquire(
    run_id: RunId,
    owner: LeaseOwner,
    expiry: LeaseExpiry,
    current_lease: Option<Lease>,
) -> AcquireResult {
    match current_lease {
        Some(existing) if existing.run_id() == run_id => AcquireResult::AlreadyLeased,
        Some(existing) => AcquireResult::WrongRunSlot {
            requested_run_id: run_id,
            existing_run_id: existing.run_id(),
        },
        None => AcquireResult::Acquired(Lease::new(run_id, owner, expiry)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_succeeds_when_no_existing_lease() {
        let run_id = RunId::new();
        let owner = LeaseOwner::new("worker-1");
        let expiry = LeaseExpiry::at(2000);

        let result = acquire(run_id, owner, expiry, None);

        match result {
            AcquireResult::Acquired(lease) => {
                assert_eq!(lease.run_id(), run_id);
                assert_eq!(lease.owner().as_str(), "worker-1");
                assert_eq!(lease.expiry().expires_at(), 2000);
            }
            AcquireResult::AlreadyLeased => panic!("Expected acquisition to succeed"),
            AcquireResult::WrongRunSlot { .. } => {
                panic!("Expected acquisition to use matching run slot")
            }
        }
    }

    #[test]
    fn acquire_fails_when_lease_already_active() {
        let run_id = RunId::new();
        let existing_lease = Lease::new(run_id, "worker-1".into(), LeaseExpiry::at(2000));

        let result =
            acquire(run_id, "worker-2".into(), LeaseExpiry::at(3000), Some(existing_lease));

        assert_eq!(result, AcquireResult::AlreadyLeased);
    }

    #[test]
    fn acquire_returns_rejection_on_conflict() {
        let run_id = RunId::new();
        let existing_lease = Lease::new(run_id, "worker-1".into(), LeaseExpiry::at(2000));

        let result =
            acquire(run_id, "worker-2".into(), LeaseExpiry::at(3000), Some(existing_lease));

        assert_eq!(result, AcquireResult::AlreadyLeased);
    }

    #[test]
    fn acquire_reports_wrong_run_slot_when_existing_lease_is_for_different_run() {
        let requested_run_id = RunId::new();
        let existing_run_id = RunId::new();
        let existing_lease = Lease::new(existing_run_id, "worker-1".into(), LeaseExpiry::at(2000));

        let result = acquire(
            requested_run_id,
            "worker-2".into(),
            LeaseExpiry::at(3000),
            Some(existing_lease),
        );

        assert_eq!(result, AcquireResult::WrongRunSlot { requested_run_id, existing_run_id });
    }
}
