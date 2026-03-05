//! Lease data model for in-flight run ownership.
//!
//! This module defines typed lease primitives used to represent:
//! - Which run is leased.
//! - Which worker currently owns that lease.
//! - When the lease expires.

use actionqueue_core::ids::RunId;

/// Typed identifier for the worker/executor that owns a lease.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct LeaseOwner(String);

impl LeaseOwner {
    /// Creates a lease owner from a worker identity string.
    ///
    /// In debug builds, panics if the value is empty.
    pub fn new(owner: impl Into<String>) -> Self {
        let value = owner.into();
        assert!(!value.is_empty(), "LeaseOwner must not be empty");
        Self(value)
    }

    /// Returns the worker identity as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LeaseOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for LeaseOwner {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for LeaseOwner {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

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
