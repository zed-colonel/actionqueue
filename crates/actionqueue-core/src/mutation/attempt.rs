//! Attempt disposition proposals. The provisional fence is finalized in AQ-03/AQ-08.
use crate::disposition::AttemptDisposition;
use crate::ids::{AttemptId, RunId};
use crate::run::RunState;
/// Typed identifier for the worker/executor that owns a lease.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaseOwner(String);

impl LeaseOwner {
    /// Creates a lease owner from a worker identity string.
    ///
    /// Panics if the value is empty.
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

/// Pure LeaseFence proposal; no mutation authority implementation yet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaseFence {
    owner: LeaseOwner,
    granted_at_sequence: u64,
}
impl LeaseFence {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(owner: LeaseOwner, granted_at_sequence: u64) -> Self {
        Self { owner, granted_at_sequence }
    }
    /// Returns owner.
    pub fn owner(&self) -> &LeaseOwner {
        &self.owner
    }
    /// Returns granted at sequence.
    pub fn granted_at_sequence(&self) -> u64 {
        self.granted_at_sequence
    }
}
/// Pure AttemptCommitExpectation proposal; no mutation authority implementation yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptCommitExpectation {
    expected_sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    expected_state: RunState,
    expected_lease: LeaseFence,
}
impl AttemptCommitExpectation {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(
        expected_sequence: u64,
        run_id: RunId,
        attempt_id: AttemptId,
        expected_state: RunState,
        expected_lease: LeaseFence,
    ) -> Self {
        Self { expected_sequence, run_id, attempt_id, expected_state, expected_lease }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns run id.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns attempt id.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
    /// Returns expected state.
    pub fn expected_state(&self) -> RunState {
        self.expected_state
    }
    /// Returns expected lease.
    pub fn expected_lease(&self) -> &LeaseFence {
        &self.expected_lease
    }
}
/// Pure AttemptDispositionCommitCommand proposal; no mutation authority implementation yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptDispositionCommitCommand {
    expected_sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    expected_state: RunState,
    expected_lease: LeaseFence,
    disposition: AttemptDisposition,
    timestamp: u64,
}
impl AttemptDispositionCommitCommand {
    /// Constructs a proposal against explicit state and lease expectations.
    pub fn new(
        expected: AttemptCommitExpectation,
        disposition: AttemptDisposition,
        timestamp: u64,
    ) -> Self {
        Self {
            expected_sequence: expected.expected_sequence,
            run_id: expected.run_id,
            attempt_id: expected.attempt_id,
            expected_state: expected.expected_state,
            expected_lease: expected.expected_lease,
            disposition,
            timestamp,
        }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns run id.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns attempt id.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
    /// Returns expected state.
    pub fn expected_state(&self) -> RunState {
        self.expected_state
    }
    /// Returns expected lease.
    pub fn expected_lease(&self) -> &LeaseFence {
        &self.expected_lease
    }
    /// Returns disposition.
    pub fn disposition(&self) -> &AttemptDisposition {
        &self.disposition
    }
    /// Returns timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
