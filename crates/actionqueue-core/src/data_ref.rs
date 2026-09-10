//! Immutable data references. Core does not fetch external data. AQ-07 must
//! verify inline hash-versus-bytes consistency at its commit boundary.
use crate::bounded::{BoundedCode, BoundedValueError, ContentHash, ContentType, OpaqueRef};
/// Inline or externally owned opaque data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DataRef {
    /// Bounded inline bytes.
    Inline(InlineData),
    /// External immutable locator.
    External(ExternalDataRef),
}
/// Size-checked inline bytes with declared content hash.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "InlineWire"))]
pub struct InlineData {
    content_type: Option<ContentType>,
    bytes: Vec<u8>,
    hash: ContentHash,
}
impl InlineData {
    /// Checks inline size. Hash verification is an AQ-07 commit obligation.
    pub fn new(
        content_type: Option<ContentType>,
        bytes: Vec<u8>,
        hash: ContentHash,
    ) -> Result<Self, BoundedValueError> {
        if bytes.len() > crate::limits::MAX_INLINE_DATA_BYTES {
            return Err(BoundedValueError::TooLarge);
        }
        Ok(Self { content_type, bytes, hash })
    }
    /// Content type, if known.
    pub fn content_type(&self) -> Option<&ContentType> {
        self.content_type.as_ref()
    }
    /// Opaque data bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    /// Declared content hash.
    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct InlineWire {
    content_type: Option<ContentType>,
    bytes: Vec<u8>,
    hash: ContentHash,
}
#[cfg(feature = "serde")]
impl TryFrom<InlineWire> for InlineData {
    type Error = BoundedValueError;
    fn try_from(w: InlineWire) -> Result<Self, Self::Error> {
        Self::new(w.content_type, w.bytes, w.hash)
    }
}
/// External data is never fetched or interpreted by ActionQueue.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExternalDataRef {
    /// Opaque resolver scheme.
    pub scheme: BoundedCode,
    /// Redacted external locator.
    pub locator: OpaqueRef,
    /// Immutable content hash.
    pub hash: ContentHash,
    /// Expected size, if known.
    pub size_bytes: Option<u64>,
    /// Content type, if known.
    pub content_type: Option<ContentType>,
}
