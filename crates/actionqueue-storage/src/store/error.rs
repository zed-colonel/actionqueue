//! Fail-closed store opening diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    MissingTargetManifest,
    UnsupportedStoreFormat {
        component: String,
        supported: u32,
        found: u32,
    },
    InvalidManifest(String),
    UnsupportedFeatures(Vec<String>),
    StoreInUse,
    InvalidStore(String),
    Io(String),
    /// A checkpoint covering a later WAL sequence was installed after this candidate
    /// was captured. The installed checkpoint is kept; durable progress is unaffected.
    StaleCheckpoint {
        sequence: u64,
        installed: u64,
    },
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTargetManifest => {
                write!(f, "MissingTargetManifest: nonempty store has no AQ-CONT-1 manifest")
            }
            Self::UnsupportedStoreFormat { component, supported, found } => {
                write!(
                    f,
                    "UnsupportedStoreFormat: {component}: supported {supported}, found {found}"
                )?;
                if component == "snapshot_schema" && *supported == 10 && *found == 9 {
                    write!(f, "; run actionqueue store upgrade --data-dir SOURCE --output DEST")?;
                } else if component == "snapshot_schema" && *supported == 9 && *found == 10 {
                    write!(f, "; source is already schema 10; no upgrade is needed")?;
                }
                Ok(())
            }
            Self::InvalidManifest(s) => write!(f, "InvalidManifest: {s}"),
            Self::UnsupportedFeatures(s) => write!(f, "UnsupportedFeatures: {}", s.join(", ")),
            Self::StoreInUse => write!(f, "StoreInUse: store is locked"),
            Self::InvalidStore(s) => write!(f, "InvalidStore: {s}"),
            Self::Io(s) => write!(f, "store I/O: {s}"),
            Self::StaleCheckpoint { sequence, installed } => write!(
                f,
                "StaleCheckpoint: checkpoint candidate at sequence {sequence} is older than \
                 installed checkpoint {installed}"
            ),
        }
    }
}
impl std::error::Error for StoreError {}
impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}
