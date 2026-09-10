//! Executor traits select eligible workers only. They are distinct from queue RBAC
//! `platform::Capability` and from downstream resource capabilities. A routing
//! trait grants no queue permission or downstream resource authority.
use crate::bounded::BoundedValueError;
use crate::limits::{MAX_EXECUTOR_TRAITS, MAX_EXECUTOR_TRAIT_BYTES};

/// Distinguishes invalid input collections from invalid individual routing labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorTraitError {
    /// The supplied collection is empty or exceeds the entry ceiling before deduplication.
    Collection {
        /// Number of supplied entries, including duplicates.
        count: usize,
    },
    /// An entry violates the label's byte-length or character constraints.
    Label {
        /// Zero-based index in the supplied collection, before sorting.
        index: usize,
        /// Underlying label validation failure.
        source: BoundedValueError,
    },
}

impl std::fmt::Display for ExecutorTraitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collection { count: 0 } => {
                f.write_str("executor trait collection must not be empty")
            }
            Self::Collection { count } => write!(
                f,
                "executor trait collection must contain 1..={MAX_EXECUTOR_TRAITS} entries before deduplication (received {count})"
            ),
            Self::Label { index, source } => write!(
                f,
                "executor trait label at index {index}: {source} (maximum {MAX_EXECUTOR_TRAIT_BYTES} bytes; no whitespace or control characters)"
            ),
        }
    }
}

impl std::error::Error for ExecutorTraitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Collection { .. } => None,
            Self::Label { source, .. } => Some(source),
        }
    }
}

crate::bounded::bounded_text!(/// A bounded executor routing label.
    ExecutorTrait, crate::limits::MAX_EXECUTOR_TRAIT_BYTES, crate::bounded::TextGrammar::RoutingLabel);

/// A non-empty, sorted and deduplicated set of executor routing labels.
/// Transparent serialization preserves the legacy vector's postcard layout, but
/// decoding enforces the new label grammar and ceilings. Legacy values outside
/// that domain are rejected; this is not a backward-compatibility guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct ExecutorTraits(Vec<ExecutorTrait>);
impl ExecutorTraits {
    /// Bounds the supplied count (including duplicates), validates labels, then
    /// canonicalizes the set. Deduplication does not bypass the input ceiling.
    pub fn new(values: Vec<String>) -> Result<Self, ExecutorTraitError> {
        if values.is_empty() || values.len() > MAX_EXECUTOR_TRAITS {
            return Err(ExecutorTraitError::Collection { count: values.len() });
        }
        let mut values = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                ExecutorTrait::new(value)
                    .map_err(|source| ExecutorTraitError::Label { index, source })
            })
            .collect::<Result<Vec<_>, _>>()?;
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
