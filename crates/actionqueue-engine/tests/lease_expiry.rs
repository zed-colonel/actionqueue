//! Lease expiry and re-eligibility tests.
//!
//! These tests verify the lease expiry evaluation and re-eligibility behavior:
//! - Lease remains active when current time is before expiry
//! - Lease expires when current time is at or after expiry
//! - Re-eligibility to Ready state is possible only when lease is expired
//!
//! The tests use both the direct evaluation functions and the lease struct
//! evaluation to ensure consistent behavior across all expiry evaluation paths.

use actionqueue_core::ids::RunId;
use actionqueue_engine::lease::expiry::{evaluate, evaluate_expires_at, ExpiryResult};
use actionqueue_engine::lease::model::{Lease, LeaseExpiry, LeaseOwner};

/// Verifies that a lease is considered active when current time is before expiry.
#[test]
fn lease_is_active_when_current_time_before_expiry() {
    let current_time = 1500;
    let expiry = LeaseExpiry::at(2000);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Active);
    assert!(!result.is_expired());
    assert!(!result.can_reenter_eligibility());
}

/// Verifies that a lease is considered expired when current time equals expiry.
#[test]
fn lease_is_expired_when_current_time_equals_expiry() {
    let current_time = 2000;
    let expiry = LeaseExpiry::at(2000);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Expired);
    assert!(result.is_expired());
    assert!(result.can_reenter_eligibility());
}

/// Verifies that a lease is considered expired when current time is after expiry.
#[test]
fn lease_is_expired_when_current_time_after_expiry() {
    let current_time = 2500;
    let expiry = LeaseExpiry::at(2000);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Expired);
    assert!(result.is_expired());
    assert!(result.can_reenter_eligibility());
}

/// Verifies Lease struct evaluation matches direct expiry evaluation.
#[test]
fn lease_struct_evaluation_matches_direct_expiry() {
    let run_id = RunId::new();
    let owner = LeaseOwner::new("worker-1");
    let expiry = LeaseExpiry::at(2000);
    let lease = Lease::new(run_id, owner, expiry);

    let current_time = 2500;
    let result = evaluate(current_time, &lease);

    assert_eq!(result, ExpiryResult::Expired);
    assert!(result.can_reenter_eligibility());
}

/// Verifies that the lease evaluation result is deterministic.
#[test]
fn lease_evaluation_is_deterministic() {
    let current_time = 2500;
    let expiry = LeaseExpiry::at(2000);

    let first_result = evaluate_expires_at(current_time, expiry);
    let second_result = evaluate_expires_at(current_time, expiry);

    assert_eq!(first_result, second_result);
}

/// Verifies that a lease with a very near expiry time (1 second away) is still active.
#[test]
fn lease_is_active_with_one_second_before_expiry() {
    let current_time = 1999;
    let expiry = LeaseExpiry::at(2000);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Active);
}

/// Verifies that a lease with a very far future expiry is still active.
#[test]
fn lease_is_active_with_far_future_expiry() {
    let current_time = 1000;
    let expiry = LeaseExpiry::at(999999);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Active);
}

/// Verifies that after a lease expires, re-eligibility to Ready state is possible.
#[test]
fn expiry_enables_reeligibility_to_ready_state() {
    let run_id = RunId::new();
    let owner = LeaseOwner::new("worker-1");
    let expiry = LeaseExpiry::at(2000);
    let lease = Lease::new(run_id, owner, expiry);

    // Before expiry
    let current_time_before = 1999;
    let result_before = evaluate(current_time_before, &lease);
    assert_eq!(result_before, ExpiryResult::Active);
    assert!(!result_before.can_reenter_eligibility());

    // At expiry - re-eligibility becomes possible
    let current_time_at_expiry = 2000;
    let result_at_expiry = evaluate(current_time_at_expiry, &lease);
    assert_eq!(result_at_expiry, ExpiryResult::Expired);
    assert!(result_at_expiry.can_reenter_eligibility());

    // After expiry - still eligible
    let current_time_after = 2500;
    let result_after = evaluate(current_time_after, &lease);
    assert_eq!(result_after, ExpiryResult::Expired);
    assert!(result_after.can_reenter_eligibility());
}

/// Verifies lease expiry behavior across different time boundaries.
#[test]
fn lease_expiry_time_boundaries() {
    let expiry = LeaseExpiry::at(2000);

    // Time 100 seconds before expiry
    let result_before = evaluate_expires_at(1900, expiry);
    assert_eq!(result_before, ExpiryResult::Active);

    // Time 1 second before expiry
    let result_1s_before = evaluate_expires_at(1999, expiry);
    assert_eq!(result_1s_before, ExpiryResult::Active);

    // Time at expiry
    let result_at = evaluate_expires_at(2000, expiry);
    assert_eq!(result_at, ExpiryResult::Expired);

    // Time 1 second after expiry
    let result_1s_after = evaluate_expires_at(2001, expiry);
    assert_eq!(result_1s_after, ExpiryResult::Expired);

    // Time 100 seconds after expiry
    let result_after = evaluate_expires_at(2100, expiry);
    assert_eq!(result_after, ExpiryResult::Expired);
}

/// Verifies that lease expiry evaluation is independent of lease owner.
#[test]
fn lease_expiry_is_independent_of_owner() {
    let run_id = RunId::new();
    let expiry = LeaseExpiry::at(2000);

    // Lease owned by worker-1
    let lease_1 = Lease::new(run_id, LeaseOwner::new("worker-1"), expiry);

    // Lease owned by worker-2
    let lease_2 = Lease::new(run_id, LeaseOwner::new("worker-2"), expiry);

    let current_time = 2500;

    let result_1 = evaluate(current_time, &lease_1);
    let result_2 = evaluate(current_time, &lease_2);

    assert_eq!(result_1, result_2);
    assert_eq!(result_1, ExpiryResult::Expired);
}

/// Verifies that lease expiry evaluation is independent of run ID.
#[test]
fn lease_expiry_is_independent_of_run_id() {
    let run_id_1 = RunId::new();
    let run_id_2 = RunId::new();
    let owner = LeaseOwner::new("worker-1");
    let expiry = LeaseExpiry::at(2000);

    let lease_1 = Lease::new(run_id_1, owner.clone(), expiry);
    let lease_2 = Lease::new(run_id_2, owner, expiry);

    let current_time = 2500;

    let result_1 = evaluate(current_time, &lease_1);
    let result_2 = evaluate(current_time, &lease_2);

    assert_eq!(result_1, result_2);
    assert_eq!(result_1, ExpiryResult::Expired);
}

/// Verifies that a newly created lease with zero expiry time is immediately expired.
#[test]
fn lease_with_zero_time_is_immediately_expired() {
    let current_time = 0;
    let expiry = LeaseExpiry::at(0);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Expired);
}

/// Verifies lease expiry with modern large timestamps.
#[test]
fn lease_expiry_with_large_timestamps() {
    let epoch_days_since_2020 = 2000; // Approx 5.5 years in days
    let current_time = epoch_days_since_2020 * 86400; // Convert to seconds
    let expiry = LeaseExpiry::at((epoch_days_since_2020 + 100) * 86400);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Active);
}

/// Verifies that expired lease with very large future time still expires correctly.
#[test]
fn lease_expires_when_large_time_reaches_expiry() {
    let epoch_days_since_2020 = 2000; // Approx 5.5 years in days
    let current_time = (epoch_days_since_2020 + 100) * 86400;
    let expiry = LeaseExpiry::at((epoch_days_since_2020 + 100) * 86400);

    let result = evaluate_expires_at(current_time, expiry);

    assert_eq!(result, ExpiryResult::Expired);
}
