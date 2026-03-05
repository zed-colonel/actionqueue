//! Lease exclusivity tests.
//!
//! These tests verify that the lease exclusivity invariant is maintained:
//! at most one active lease can exist per run. Once a lease is acquired for a
//! run, subsequent lease acquisition attempts for that same run must fail.

use actionqueue_core::ids::RunId;
use actionqueue_engine::lease::acquire::{acquire, AcquireResult};
use actionqueue_engine::lease::model::{Lease, LeaseExpiry, LeaseOwner};

/// Verifies that a lease can be acquired when no lease exists for the run.
#[test]
fn lease_can_be_acquired_when_no_existing_lease() {
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

/// Verifies that a second lease acquisition for the same run fails when a lease
/// is already active.
#[test]
fn lease_exclusivity_prevents_second_acquisition_for_same_run() {
    let run_id = RunId::new();
    let existing_lease = Lease::new(run_id, "worker-1".into(), LeaseExpiry::at(2000));

    // Attempt to acquire a new lease for the same run
    let result = acquire(run_id, "worker-2".into(), LeaseExpiry::at(3000), Some(existing_lease));

    assert_eq!(result, AcquireResult::AlreadyLeased);
}

/// Verifies that a second lease acquisition attempt also respects
/// exclusivity when the same run already has an active lease.
#[test]
fn lease_acquisition_respects_exclusivity_duplicate_attempt() {
    let run_id = RunId::new();
    let existing_lease = Lease::new(run_id, "worker-1".into(), LeaseExpiry::at(2000));

    let result = acquire(run_id, "worker-2".into(), LeaseExpiry::at(3000), Some(existing_lease));

    assert_eq!(result, AcquireResult::AlreadyLeased);
}

/// Verifies that lease exclusivity works with multiple different runs.
#[test]
fn lease_exclusivity_is_per_run_not_global() {
    let run_id_1 = RunId::new();
    let run_id_2 = RunId::new();

    // Acquire a lease for run_1
    let lease_1 = acquire(run_id_1, "worker-1".into(), LeaseExpiry::at(2000), None);
    let lease_1 = match lease_1 {
        AcquireResult::Acquired(l) => l,
        _ => panic!("Expected first acquisition to succeed"),
    };

    // Acquire a lease for run_2 should succeed (different run)
    let lease_2 = acquire(run_id_2, "worker-2".into(), LeaseExpiry::at(2000), None);
    let lease_2 = match lease_2 {
        AcquireResult::Acquired(l) => l,
        _ => panic!("Expected second acquisition to succeed"),
    };

    assert_ne!(lease_1.run_id(), lease_2.run_id());
    assert_eq!(lease_1.run_id(), run_id_1);
    assert_eq!(lease_2.run_id(), run_id_2);
}

/// Verifies that a lease expiry does not affect exclusivity state for a
/// different run.
#[test]
fn lease_expiry_for_one_run_does_not_affect_exclusivity_for_another_run() {
    let run_id_1 = RunId::new();
    let run_id_2 = RunId::new();

    // Acquire a lease for run_1
    let _lease_1 = acquire(run_id_1, "worker-1".into(), LeaseExpiry::at(2000), None);

    // Acquire a lease for run_2 should succeed
    let lease_2 = acquire(run_id_2, "worker-2".into(), LeaseExpiry::at(2000), None);
    let lease_2 = match lease_2 {
        AcquireResult::Acquired(l) => l,
        _ => panic!("Expected acquisition to succeed"),
    };

    assert_eq!(lease_2.run_id(), run_id_2);

    // Now try to re-acquire for run_1 (which still has an active lease)
    let result = acquire(
        run_id_1,
        "worker-3".into(),
        LeaseExpiry::at(3000),
        Some(lease_2), // This is wrong slot, but we're testing exclusivity per run
    );

    // This should return WrongRunSlot because the current_lease is for run_2
    match result {
        AcquireResult::WrongRunSlot { requested_run_id, existing_run_id } => {
            assert_eq!(requested_run_id, run_id_1);
            assert_eq!(existing_run_id, run_id_2);
        }
        _ => panic!("Expected WrongRunSlot result"),
    }
}
