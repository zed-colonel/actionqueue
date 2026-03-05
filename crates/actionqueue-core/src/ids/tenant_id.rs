//! Unique identifier for an organizational tenant.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for an organizational tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TenantId(Uuid);

impl TenantId {
    /// Creates a new random TenantId.
    pub fn new() -> Self {
        TenantId(Uuid::new_v4())
    }

    /// Creates a TenantId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        TenantId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for TenantId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for TenantId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(TenantId)
    }
}

impl Display for TenantId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
