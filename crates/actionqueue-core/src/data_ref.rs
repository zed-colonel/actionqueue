//! Immutable data references and pure integrity checks; external data is never fetched.
use crate::bounded::{BoundedValueError, ContentHash, ContentType, DataScheme, OpaqueRef};
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
#[derive(Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "InlineWire"))]
pub struct InlineData {
    content_type: Option<ContentType>,
    bytes: Vec<u8>,
    hash: ContentHash,
}
impl InlineData {
    /// Checks inline size. Committers must also call `DataRef::validate`.
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
    pub scheme: DataScheme,
    /// Redacted external locator.
    pub locator: OpaqueRef,
    /// Immutable content hash.
    pub hash: ContentHash,
    /// Expected size, if known.
    pub size_bytes: Option<u64>,
    /// Content type, if known.
    pub content_type: Option<ContentType>,
}

/// Integrity failures contain no payload or locator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataValidationError {
    /// Inline hard ceiling exceeded.
    TooLarge,
    /// Declared SHA-256 does not match supplied bytes.
    HashMismatch,
    /// Declared external size does not match supplied bytes.
    SizeMismatch,
}
impl std::fmt::Display for DataValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data integrity: {self:?}")
    }
}
impl std::error::Error for DataValidationError {}
impl DataRef {
    /// Constructs bounded inline data with a SHA-256 digest of the exact bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, BoundedValueError> {
        use sha2::Digest;
        let hash = ContentHash::new(
            crate::bounded::HashAlgorithm::Sha256,
            sha2::Sha256::digest(&bytes).to_vec(),
        )?;
        Ok(Self::Inline(InlineData::new(None, bytes, hash)?))
    }

    /// Validates inline integrity. External structure is checked by its bounded types.
    pub fn validate(&self) -> Result<(), DataValidationError> {
        match self {
            Self::Inline(v) => {
                if v.bytes.len() > crate::limits::MAX_INLINE_DATA_BYTES {
                    return Err(DataValidationError::TooLarge);
                }
                self.verify_bytes(&v.bytes)
            }
            Self::External(_) => Ok(()),
        }
    }
    /// Verifies caller-resolved bytes without fetching or interpreting them.
    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<(), DataValidationError> {
        use sha2::{Digest, Sha256};
        let hash = match self {
            Self::Inline(v) => &v.hash,
            Self::External(v) => {
                if v.size_bytes.is_some_and(|n| n != bytes.len() as u64) {
                    return Err(DataValidationError::SizeMismatch);
                }
                &v.hash
            }
        };
        if Sha256::digest(bytes).as_slice() != hash.bytes() {
            return Err(DataValidationError::HashMismatch);
        }
        Ok(())
    }
}
impl std::fmt::Debug for InlineData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineData")
            .field("length", &self.bytes.len())
            .field("hash", &self.hash)
            .field("content_type", &self.content_type)
            .finish()
    }
}
