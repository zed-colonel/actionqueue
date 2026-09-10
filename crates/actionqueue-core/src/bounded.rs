//! Bounded opaque values. Core never resolves references or computes hashes.

/// Structural value validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedValueError {
    /// A required value was empty.
    Empty,
    /// A byte or collection ceiling was exceeded.
    TooLarge,
    /// A character is outside the permitted grammar.
    InvalidCharacter,
    /// Digest length does not match its algorithm.
    InvalidHashLength,
}
impl std::fmt::Display for BoundedValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("bounded value must not be empty"),
            _ => write!(f, "invalid bounded value: {self:?}"),
        }
    }
}
impl std::error::Error for BoundedValueError {}

pub(crate) fn validate_text(
    value: &str,
    limit: usize,
    grammar: u8,
) -> Result<(), BoundedValueError> {
    if value.is_empty() && grammar != 3 {
        return Err(BoundedValueError::Empty);
    }
    if value.len() > limit {
        return Err(BoundedValueError::TooLarge);
    }
    let valid = match grammar {
        0 => !value.chars().any(char::is_control),
        1 => value.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || (i > 0 && b"._-".contains(&b))
        }),
        2 => !value.chars().any(|c| c.is_whitespace() || c.is_control()),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(BoundedValueError::InvalidCharacter)
    }
}

macro_rules! bounded_text {
    ($(#[$doc:meta])* $name:ident, $limit:expr, $grammar:expr) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize))]
        #[cfg_attr(feature = "serde", serde(transparent))]
        pub struct $name(String);
        impl $name {
            /// Constructs a value after byte-length and grammar validation.
            pub fn new(value: impl Into<String>) -> Result<Self, crate::bounded::BoundedValueError> {
                let value = value.into();
                crate::bounded::validate_text(&value, $limit, $grammar)?;
                Ok(Self(value))
            }
            /// Returns the exact underlying text.
            pub fn as_str(&self) -> &str { &self.0 }
        }
        impl std::str::FromStr for $name {
            type Err = crate::bounded::BoundedValueError;
            fn from_str(value: &str) -> Result<Self, Self::Err> { Self::new(value) }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
        }
        #[cfg(feature = "serde")]
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                Self::new(<String as serde::Deserialize>::deserialize(d)?).map_err(serde::de::Error::custom)
            }
        }
    };
}
pub(crate) use bounded_text;

bounded_text!(/// A lowercase machine-readable error or scheme code.
    BoundedCode, crate::limits::MAX_CODE_BYTES, 1);
bounded_text!(/// A bounded human-readable error message (may be empty).
    BoundedMessage, crate::limits::MAX_ERROR_MESSAGE_BYTES, 3);
bounded_text!(/// Opaque content type without whitespace or controls.
    ContentType, crate::limits::MAX_CONTENT_TYPE_BYTES, 2);

/// Bounded, equality-only opaque attribution. Never dereferenced by core.
///
/// An authorization_ref is not an authorization decision.
/// A principal_ref is not proof of identity.
/// A purpose_ref is not validation of purpose.
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct OpaqueRef(String);
impl OpaqueRef {
    /// Validates a non-empty UTF-8 reference without control characters.
    pub fn new(value: impl Into<String>) -> Result<Self, BoundedValueError> {
        let value = value.into();
        validate_text(&value, crate::limits::MAX_OPAQUE_REF_BYTES, 0)?;
        Ok(Self(value))
    }
    /// Explicitly exposes the reference for persistence or exact comparison.
    pub fn expose(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for OpaqueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "opaque-ref[{} bytes]", self.0.len())
    }
}
impl std::fmt::Debug for OpaqueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::str::FromStr for OpaqueRef {
    type Err = BoundedValueError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}
#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for OpaqueRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(<String as serde::Deserialize>::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

/// A bounded error carried by an attempt disposition.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BoundedError {
    /// Machine-readable classification.
    pub code: BoundedCode,
    /// Human-readable detail.
    pub message: BoundedMessage,
}

/// Supported digest algorithms. Core validates structure, not content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HashAlgorithm {
    /// SHA-256 has a 32-byte digest.
    Sha256,
}

/// Immutable digest with an algorithm-checked byte length.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "HashWire"))]
pub struct ContentHash {
    algorithm: HashAlgorithm,
    bytes: Vec<u8>,
}
impl ContentHash {
    /// Checks the digest length; does not compute a hash.
    pub fn new(algorithm: HashAlgorithm, bytes: Vec<u8>) -> Result<Self, BoundedValueError> {
        let length = match algorithm {
            HashAlgorithm::Sha256 => 32,
        };
        if bytes.len() != length {
            return Err(BoundedValueError::InvalidHashLength);
        }
        Ok(Self { algorithm, bytes })
    }
    /// Digest algorithm.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.algorithm
    }
    /// Raw digest bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
impl std::fmt::Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.algorithm {
            HashAlgorithm::Sha256 => f.write_str("sha-256:")?,
        }
        for byte in &self.bytes {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct HashWire {
    algorithm: HashAlgorithm,
    bytes: Vec<u8>,
}
#[cfg(feature = "serde")]
impl TryFrom<HashWire> for ContentHash {
    type Error = BoundedValueError;
    fn try_from(w: HashWire) -> Result<Self, Self::Error> {
        Self::new(w.algorithm, w.bytes)
    }
}
