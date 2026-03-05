//! Unique identifier for a remote actor registered with the hub.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for a remote actor registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorId(Uuid);

impl ActorId {
    /// Creates a new random ActorId.
    pub fn new() -> Self {
        ActorId(Uuid::new_v4())
    }

    /// Creates an ActorId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        ActorId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for ActorId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for ActorId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(ActorId)
    }
}

impl Display for ActorId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
