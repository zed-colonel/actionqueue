use std::fmt::{Display, Formatter};

/// A human-readable identifier for a department or actor group.
///
/// Department identifiers use string labels (e.g. `"engineering"`, `"ops"`)
/// rather than UUIDs, so that task routing rules are human-readable in
/// configuration and WAL events.
///
/// # Invariants
///
/// - The identifier string must be non-empty.
/// - The identifier string must not exceed 128 characters.
/// - Validated at construction; never stores an invalid value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DepartmentId(String);

/// Error returned when a `DepartmentId` cannot be constructed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepartmentIdError {
    /// The identifier string was empty.
    Empty,
    /// The identifier string exceeded the maximum allowed length.
    TooLong {
        /// The length of the rejected string.
        length: usize,
    },
}

impl std::fmt::Display for DepartmentIdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DepartmentIdError::Empty => write!(f, "department identifier must be non-empty"),
            DepartmentIdError::TooLong { length } => {
                write!(f, "department identifier length {length} exceeds maximum 128 characters")
            }
        }
    }
}

impl std::error::Error for DepartmentIdError {}

impl DepartmentId {
    /// Maximum allowed length for a department identifier.
    pub const MAX_LEN: usize = 128;

    /// Creates a new `DepartmentId` from a string value.
    ///
    /// # Errors
    ///
    /// Returns [`DepartmentIdError::Empty`] if `value` is empty.
    /// Returns [`DepartmentIdError::TooLong`] if `value` exceeds 128 characters.
    pub fn new(value: impl Into<String>) -> Result<Self, DepartmentIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(DepartmentIdError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(DepartmentIdError::TooLong { length: value.len() });
        }
        Ok(DepartmentId(value))
    }

    /// Returns the department identifier string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for DepartmentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for DepartmentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DepartmentId::new(s).map_err(serde::de::Error::custom)
    }
}
