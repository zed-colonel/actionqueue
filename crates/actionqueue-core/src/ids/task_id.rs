//! Unique identifier for a durable task specification.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for a task definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskId(Uuid);

impl TaskId {
    /// Creates a new random TaskId.
    pub fn new() -> Self {
        TaskId(Uuid::new_v4())
    }

    /// Creates a TaskId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        TaskId(uuid)
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

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(TaskId)
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
