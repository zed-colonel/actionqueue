//! Lease data model for in-flight run ownership.
//!
//! This module defines typed lease primitives used to represent:
//! - Which run is leased.
//! - Which worker currently owns that lease.
//! - When the lease expires.

use actionqueue_core::ids::RunId;

pub use actionqueue_core::mutation::LeaseOwner;

/// Typed lease-expiry timestamp represented in epoch seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct LeaseExpiry {
    /// Absolute timestamp when the lease is no longer valid.
    expires_at: u64,
}

impl LeaseExpiry {
    /// Creates a typed expiry value from an absolute timestamp.
    pub const fn at(expires_at: u64) -> Self {
        Self { expires_at }
    }

    /// Returns the absolute timestamp when the lease expires.
    pub const fn expires_at(&self) -> u64 {
        self.expires_at
    }
}

/// Active lease for a specific run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Run that this lease belongs to.
    run_id: RunId,

    /// Worker currently holding the lease.
    owner: LeaseOwner,

    /// Lease-expiry representation in typed form.
    expiry: LeaseExpiry,
}

impl Lease {
    /// Creates a new lease value.
    pub fn new(run_id: RunId, owner: LeaseOwner, expiry: LeaseExpiry) -> Self {
        Self { run_id, owner, expiry }
    }

    /// Returns the run that this lease belongs to.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the worker currently holding the lease.
    pub fn owner(&self) -> &LeaseOwner {
        &self.owner
    }

    /// Returns the lease expiry.
    pub fn expiry(&self) -> LeaseExpiry {
        self.expiry
    }
}
