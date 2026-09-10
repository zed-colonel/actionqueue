//! Executor traits select eligible workers only. They are distinct from queue RBAC
//! `platform::Capability` and from downstream resource capabilities. A routing
//! trait grants no queue permission or downstream resource authority.
use crate::bounded::BoundedValueError;
pub use crate::bounded::BoundedValueError as ExecutorTraitError;
crate::bounded::bounded_text!(/// A bounded executor routing label.
    ExecutorTrait, crate::limits::MAX_EXECUTOR_TRAIT_BYTES, 2);

/// A non-empty, sorted and deduplicated set of executor routing labels.
/// Transparent serialization preserves the legacy vector's postcard layout.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ExecutorTraits(Vec<ExecutorTrait>);
impl ExecutorTraits {
    /// Validates labels and the supplied count, then canonicalizes the set.
    pub fn new(values: Vec<String>) -> Result<Self, ExecutorTraitError> {
        if values.is_empty() {
            return Err(BoundedValueError::Empty);
        }
        if values.len() > crate::limits::MAX_EXECUTOR_TRAITS {
            return Err(BoundedValueError::TooLarge);
        }
        let mut values =
            values.into_iter().map(ExecutorTrait::new).collect::<Result<Vec<_>, _>>()?;
        values.sort();
        values.dedup();
        Ok(Self(values))
    }
    /// Returns the canonical labels.
    pub fn as_slice(&self) -> &[ExecutorTrait] {
        &self.0
    }
    /// True when all required labels are present.
    pub fn satisfies(&self, required: &ExecutorTraits) -> bool {
        required.0.iter().all(|value| self.0.binary_search(value).is_ok())
    }
}
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for ExecutorTraits {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(<Vec<String> as serde::Deserialize>::deserialize(d)?)
            .map_err(serde::de::Error::custom)
    }
}
