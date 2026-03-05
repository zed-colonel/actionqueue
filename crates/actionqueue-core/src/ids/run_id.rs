//! Stable run identity and idempotency key for external side effects.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// Unique identifier for a run instance.
///
/// `RunId` derives `Ord`/`PartialOrd` because the engine's selection algorithm
/// uses RunId ordering as a deterministic tie-breaker when priority and creation
/// time are equal. `TaskId` and `AttemptId` do not need ordering semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RunId(Uuid);

impl RunId {
    /// Creates a new random RunId.
    pub fn new() -> Self {
        RunId(Uuid::new_v4())
    }

    /// Creates a RunId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        RunId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for RunId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for RunId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(RunId)
    }
}

impl Display for RunId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// The `Ord` and `PartialOrd` traits are derived automatically from the `Ord` derive on the struct,
// which uses the underlying `Uuid`'s byte-level ordering for deterministic comparison.
