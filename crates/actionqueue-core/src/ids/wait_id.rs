//! Unique identifier for a durable continuation specification.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for a continuation definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WaitId(Uuid);

impl WaitId {
    /// Creates a new random WaitId.
    pub fn new() -> Self {
        WaitId(Uuid::new_v4())
    }

    /// Creates a WaitId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        WaitId(uuid)
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

impl Default for WaitId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for WaitId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(WaitId)
    }
}

impl Display for WaitId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
