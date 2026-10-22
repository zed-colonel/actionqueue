//! Target-only snapshot loading. Compatibility and semantic failures never fall back.
use std::{fs::File, io::Read};

use super::{envelope, mapping::SnapshotMappingError, model::Snapshot};
pub trait SnapshotLoader {
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotLoaderError {
    IoError(String),
    DecodeError(String),
    MappingError(SnapshotMappingError),
    IncompatibleVersion { component: &'static str, expected: u32, found: u32 },
    CrcMismatch { expected: u32, actual: u32 },
    NotFound,
    PhysicalDamage,
}
impl SnapshotLoaderError {
    pub fn physical_damage(&self) -> bool {
        matches!(self, Self::PhysicalDamage | Self::CrcMismatch { .. })
    }
}
impl std::fmt::Display for SnapshotLoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "snapshot I/O error: {e}"),
            Self::DecodeError(e) => write!(f, "snapshot decode error: {e}"),
            Self::MappingError(e) => write!(f, "snapshot mapping error: {e}"),
            Self::IncompatibleVersion { component, expected, found } => {
                write!(f, "{component}: supported {expected}, found {found}")
            }
            Self::CrcMismatch { expected, actual } => write!(
                f,
                "snapshot checksum mismatch: expected {expected:#010x}, found {actual:#010x}"
            ),
            Self::NotFound => write!(f, "snapshot not found"),
            Self::PhysicalDamage => {
                write!(f, "snapshot is physically damaged: truncated or length mismatch")
            }
        }
    }
}
impl std::error::Error for SnapshotLoaderError {}
pub struct SnapshotFsLoader {
    path: std::path::PathBuf,
    version: u32,
    identity: Option<uuid::Uuid>,
    session: Option<crate::store::StoreSession>,
}
impl SnapshotFsLoader {
    /// Reads a legacy snapshot-schema-9 file (frame version 1) at `path`, without
    /// store binding. Every 0.3.0 store snapshot uses frame version 2, which this
    /// loader rejects as `IncompatibleVersion`; open those with [`Self::for_session`].
    pub fn new(path: std::path::PathBuf) -> Self {
        Self::with_version(path, 1)
    }
    /// Reads an unbound snapshot at `path` that must use frame `version`: 1 for
    /// snapshot schema 9 or 2 for schema 10. This is a frame version, not a schema
    /// number.
    pub fn with_version(path: std::path::PathBuf, version: u32) -> Self {
        Self { path, version, identity: None, session: None }
    }
    /// Reads the session's current schema-10 checkpoint (frame version 2) and checks
    /// it against the session's store identity and manifest.
    pub fn for_session(session: &crate::store::StoreSession) -> Self {
        Self {
            path: session.snapshot_path(),
            version: 2,
            identity: Some(session.manifest().store_id),
            session: Some(session.clone()),
        }
    }
}
impl SnapshotLoader for SnapshotFsLoader {
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError> {
        let Some((version, payload)) = read_frame(&self.path, self.version)? else {
            return Ok(None);
        };
        let snapshot = if version == 1 {
            envelope::decode(&payload, self.identity)?
        } else {
            let c = envelope::decode_checkpoint(&payload, self.session.as_ref())?;
            if self.identity.is_some_and(|id| c.binding.store != *id.as_bytes()) {
                return Err(SnapshotLoaderError::DecodeError("store identity mismatch".into()));
            }
            super::mapping::validate_snapshot(&c.snapshot)
                .map_err(SnapshotLoaderError::MappingError)?;
            c.snapshot
        };
        Ok(Some(snapshot))
    }
}
pub(crate) fn load_checkpoint(
    session: &crate::store::StoreSession,
) -> Result<Option<super::wire_v10::Checkpoint>, SnapshotLoaderError> {
    let started = std::time::Instant::now();
    let Some((_, payload)) = read_frame(&session.snapshot_path(), 2)? else {
        return Ok(None);
    };
    crate::recovery::work::snapshot_phase(|p| p.io_seconds += started.elapsed().as_secs_f64());
    envelope::decode_checkpoint(&payload, Some(session)).map(Some)
}
pub(crate) fn read_frame(
    path: &std::path::Path,
    expected_version: u32,
) -> Result<Option<(u32, Vec<u8>)>, SnapshotLoaderError> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if !m.is_file() => {
            return Err(SnapshotLoaderError::IoError(
                "snapshot must be a bounded regular file".into(),
            ))
        }
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SnapshotLoaderError::IoError(e.to_string())),
    }
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SnapshotLoaderError::IoError(e.to_string())),
    };
    let io = |e: std::io::Error| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            SnapshotLoaderError::PhysicalDamage
        } else {
            SnapshotLoaderError::IoError(e.to_string())
        }
    };
    let mut magic = [0; 8];
    for i in 0..8 {
        file.read_exact(&mut magic[i..i + 1]).map_err(io)?;
        if magic[i] != envelope::MAGIC[i] {
            return Err(SnapshotLoaderError::DecodeError("unsupported snapshot magic".into()));
        }
    }
    let mut header = [0; 12];
    file.read_exact(&mut header[..4]).map_err(io)?;
    let version = u32::from_le_bytes(header[..4].try_into().unwrap());
    if version != expected_version {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "snapshot_frame",
            expected: expected_version,
            found: version,
        });
    }
    file.read_exact(&mut header[4..]).map_err(io)?;
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    // The length itself is outside the CRC. A mismatch is physical damage,
    // including a corrupted high byte declaring a payload above the size limit.
    if file.metadata().map_err(io)?.len() != len as u64 + 20 {
        return Err(SnapshotLoaderError::PhysicalDamage);
    }
    if len > envelope::MAX_SNAPSHOT_BYTES
        || (version == 2 && len + 20 > envelope::MAX_SNAPSHOT_BYTES)
    {
        return Err(SnapshotLoaderError::DecodeError("snapshot size bound exceeded".into()));
    }
    let mut payload = vec![0; len];
    file.read_exact(&mut payload).map_err(io)?;
    let expected = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let actual = crc32fast::hash(&payload);
    if expected != actual {
        return Err(SnapshotLoaderError::CrcMismatch { expected, actual });
    }
    Ok(Some((version, payload)))
}
