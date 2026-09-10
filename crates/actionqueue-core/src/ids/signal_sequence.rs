//! Store-minted signal order, independent of timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct SignalSequence(u64);
impl SignalSequence {
    /// Constructs a sequence assigned by the store.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    /// Returns the store order.
    pub const fn get(self) -> u64 {
        self.0
    }
}
impl std::fmt::Display for SignalSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::str::FromStr for SignalSequence {
    type Err = std::num::ParseIntError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}
