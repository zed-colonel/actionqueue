//! Target-only snapshot loading. Compatibility and semantic failures never fall back.
use super::{envelope, mapping::SnapshotMappingError, model::Snapshot};
use std::{fs::File, io::Read};
pub trait SnapshotLoader {
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError>;
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotLoaderError {
    IoError(String),
    DecodeError(String),
    MappingError(SnapshotMappingError),
    IncompatibleVersion { expected: u32, found: u32 },
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
        write!(f, "snapshot load: {self:?}")
    }
}
impl std::error::Error for SnapshotLoaderError {}
pub struct SnapshotFsLoader {
    path: std::path::PathBuf,
    version: u32,
    identity: Option<uuid::Uuid>,
}
impl SnapshotFsLoader {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self { path, version: 1, identity: None }
    }
    pub fn with_version(path: std::path::PathBuf, version: u32) -> Self {
        Self { path, version, identity: None }
    }
    pub fn for_session(session: &crate::store::StoreSession) -> Self {
        Self {
            path: session.snapshot_path(),
            version: 1,
            identity: Some(session.manifest().store_id),
        }
    }
}
impl SnapshotLoader for SnapshotFsLoader {
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError> {
        let mut file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SnapshotLoaderError::IoError(e.to_string())),
        };
        let mut magic = [0; 8];
        let mut n = 0;
        while n < 8 {
            let count = file
                .read(&mut magic[n..])
                .map_err(|e| SnapshotLoaderError::IoError(e.to_string()))?;
            if count == 0 {
                if magic[..n] != envelope::MAGIC[..n] {
                    return Err(SnapshotLoaderError::DecodeError(
                        "unsupported snapshot magic".into(),
                    ));
                }
                return Err(SnapshotLoaderError::PhysicalDamage);
            }
            n += count;
        }
        if &magic != envelope::MAGIC {
            return Err(SnapshotLoaderError::DecodeError("unsupported snapshot magic".into()));
        }
        let mut header = [0u8; 12];
        file.read_exact(&mut header).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                SnapshotLoaderError::PhysicalDamage
            } else {
                SnapshotLoaderError::IoError(e.to_string())
            }
        })?;
        let version = u32::from_le_bytes(header[..4].try_into().unwrap());
        if version != 1 || version != self.version {
            return Err(SnapshotLoaderError::IncompatibleVersion { expected: 1, found: version });
        }
        let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        if len > envelope::MAX_SNAPSHOT_BYTES {
            return Err(SnapshotLoaderError::DecodeError("snapshot size bound exceeded".into()));
        }
        let mut payload = vec![0; len];
        file.read_exact(&mut payload).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                SnapshotLoaderError::PhysicalDamage
            } else {
                SnapshotLoaderError::IoError(e.to_string())
            }
        })?;
        let mut trailing = [0];
        if file.read(&mut trailing).map_err(|e| SnapshotLoaderError::IoError(e.to_string()))? != 0 {
            return Err(SnapshotLoaderError::PhysicalDamage);
        }
        let expected = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let actual = crc32fast::hash(&payload);
        if expected != actual {
            return Err(SnapshotLoaderError::CrcMismatch { expected, actual });
        }
        envelope::decode(&payload, self.identity).map(Some)
    }
}
