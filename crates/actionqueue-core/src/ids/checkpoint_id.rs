//! Unique identifier for a durable checkpoint.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CheckpointId(Uuid);

impl CheckpointId {
    /// Creates a new random CheckpointId.
    pub fn new() -> Self {
        CheckpointId(Uuid::new_v4())
    }

    /// Creates a CheckpointId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        CheckpointId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Returns whether this identifier is the nil UUID.
    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl Default for CheckpointId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for CheckpointId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(CheckpointId)
    }
}

impl Display for CheckpointId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
