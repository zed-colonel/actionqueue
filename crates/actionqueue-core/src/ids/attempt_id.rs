//! Per-attempt identifier for execution lineage tracking.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for an attempt within a run.
///
/// Each run may have multiple attempts (for retries). The AttemptId
/// uniquely identifies each attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AttemptId(Uuid);

impl AttemptId {
    /// Creates a new random AttemptId.
    pub fn new() -> Self {
        AttemptId(Uuid::new_v4())
    }

    /// Creates an AttemptId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        AttemptId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for AttemptId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for AttemptId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(AttemptId)
    }
}

impl Display for AttemptId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
