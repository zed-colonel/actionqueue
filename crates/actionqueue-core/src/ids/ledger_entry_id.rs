//! Unique identifier for an append-only ledger entry.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

/// A unique identifier for a ledger entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LedgerEntryId(Uuid);

impl LedgerEntryId {
    /// Creates a new random LedgerEntryId.
    pub fn new() -> Self {
        LedgerEntryId(Uuid::new_v4())
    }

    /// Creates a LedgerEntryId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        LedgerEntryId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for LedgerEntryId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for LedgerEntryId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(LedgerEntryId)
    }
}

impl Display for LedgerEntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
