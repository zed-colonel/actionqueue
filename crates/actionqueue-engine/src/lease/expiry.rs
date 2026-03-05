//! Lease expiry evaluation logic.
//!
//! This module implements lease expiry determination:
//! - Evaluates whether a lease is still active or expired.
//! - Exposes an explicit eligibility signal for re-entering the Ready state.
//!
//! For identical inputs, expiry evaluation must always return the same result.

use crate::lease::model::{Lease, LeaseExpiry};

/// Result of lease expiry evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum ExpiryResult {
    /// Lease is still active (not yet expired).
    Active,
    /// Lease has expired and the run can re-enter eligibility.
    Expired,
}

impl ExpiryResult {
    /// Returns true when the evaluated lease is expired.
    pub const fn is_expired(&self) -> bool {
        matches!(self, Self::Expired)
    }

    /// Returns true when the run may re-enter eligibility.
    ///
    /// In Phase 5 lease semantics, this is true if and only if the lease is
    /// expired.
    pub const fn can_reenter_eligibility(&self) -> bool {
        self.is_expired()
    }
}

/// Evaluates whether a lease is still active or has expired.
///
/// A lease is considered expired if its `expires_at` timestamp is less than
/// or equal to the current time.
pub fn evaluate_expires_at(current_time: u64, lease_expiry: LeaseExpiry) -> ExpiryResult {
    if current_time >= lease_expiry.expires_at() {
        ExpiryResult::Expired
    } else {
        ExpiryResult::Active
    }
}

/// Evaluates whether a lease is still active or has expired.
///
/// A lease is considered expired if its `expires_at` timestamp is less than
/// or equal to the current time.
///
/// Returns `ExpiryResult::Expired` if the lease is expired, otherwise
/// `ExpiryResult::Active`.
pub fn evaluate(current_time: u64, lease: &Lease) -> ExpiryResult {
    evaluate_expires_at(current_time, lease.expiry())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_is_active_when_current_time_is_before_expiry() {
        let current_time = 1500;
        let expiry = LeaseExpiry::at(2000);

        let result = evaluate_expires_at(current_time, expiry);

        assert_eq!(result, ExpiryResult::Active);
        assert!(!result.can_reenter_eligibility());
    }

    #[test]
    fn lease_is_expired_when_current_time_equals_expiry() {
        let current_time = 2000;
        let expiry = LeaseExpiry::at(2000);

        let result = evaluate_expires_at(current_time, expiry);

        assert_eq!(result, ExpiryResult::Expired);
        assert!(result.can_reenter_eligibility());
    }

    #[test]
    fn lease_is_expired_when_current_time_is_after_expiry() {
        let current_time = 2500;
        let expiry = LeaseExpiry::at(2000);

        let result = evaluate_expires_at(current_time, expiry);

        assert_eq!(result, ExpiryResult::Expired);
        assert!(result.can_reenter_eligibility());
    }

    #[test]
    fn evaluate_lease_struct() {
        let current_time = 2500;
        let run_id = actionqueue_core::ids::RunId::new();
        let owner = crate::lease::model::LeaseOwner::new("worker-1");
        let expiry = LeaseExpiry::at(2000);
        let lease = Lease::new(run_id, owner, expiry);

        let result = evaluate(current_time, &lease);

        assert_eq!(result, ExpiryResult::Expired);
    }

    #[test]
    fn evaluation_is_deterministic() {
        let current_time = 2500;
        let expiry = LeaseExpiry::at(2000);

        let first = evaluate_expires_at(current_time, expiry);
        let second = evaluate_expires_at(current_time, expiry);

        assert_eq!(first, second);
    }
}
