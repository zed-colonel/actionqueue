//! Snapshot loader interface and file system implementation.
//!
//! This module provides snapshot loading using a file system backend with
//! versioned decoding for compatibility validation.
//!
//! The loader reads snapshots that were written using the same framing format
//! as the WAL: version (4 bytes) + length (4 bytes) + CRC-32 (4 bytes) + JSON payload.

use std::fs::File;
use std::io::Read;

use crate::snapshot::mapping::{validate_snapshot, SnapshotMappingError};
use crate::snapshot::model::Snapshot;

/// A snapshot loader that reads state from storage.
pub trait SnapshotLoader {
    /// Load a snapshot from storage.
    ///
    /// Returns `Ok(Some(Snapshot))` if a valid snapshot was loaded,
    /// `Ok(None)` if no snapshot exists at the configured path,
    /// or `Err` if an error occurred during loading.
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError>;
}

/// Errors that can occur during snapshot loading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotLoaderError {
    /// I/O error during load.
    IoError(String),
    /// The snapshot could not be decoded.
    DecodeError(String),
    /// The decoded snapshot violated mapping/parity invariants.
    MappingError(SnapshotMappingError),
    /// The snapshot version is incompatible.
    IncompatibleVersion {
        /// Snapshot version expected by the loader configuration.
        expected: u32,
        /// Snapshot version found in the persisted snapshot frame.
        found: u32,
    },
    /// The snapshot CRC-32 checksum does not match the payload.
    CrcMismatch {
        /// CRC-32 checksum stored in the snapshot frame.
        expected: u32,
        /// CRC-32 checksum computed from the payload bytes.
        actual: u32,
    },
    /// The snapshot file does not exist.
    NotFound,
}

impl std::fmt::Display for SnapshotLoaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotLoaderError::IoError(e) => write!(f, "I/O error: {e}"),
            SnapshotLoaderError::DecodeError(e) => write!(f, "Decode error: {e}"),
            SnapshotLoaderError::MappingError(e) => write!(f, "Mapping error: {e}"),
            SnapshotLoaderError::IncompatibleVersion { expected, found } => {
                write!(f, "Incompatible snapshot version: expected {expected}, found {found}")
            }
            SnapshotLoaderError::CrcMismatch { expected, actual } => {
                write!(
                    f,
                    "Snapshot CRC-32 mismatch: expected {expected:#010x}, actual {actual:#010x}"
                )
            }
            SnapshotLoaderError::NotFound => write!(f, "Snapshot file not found"),
        }
    }
}

impl std::error::Error for SnapshotLoaderError {}

impl std::convert::From<std::io::Error> for SnapshotLoaderError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => SnapshotLoaderError::NotFound,
            _ => SnapshotLoaderError::IoError(err.to_string()),
        }
    }
}

/// A file system backed snapshot loader.
///
/// The loader opens a snapshot file and decodes it using the framed binary
/// format written by [`SnapshotFsWriter`](super::writer::SnapshotFsWriter):
/// - 4 bytes version (currently 4)
/// - 4 bytes length of payload
/// - 4 bytes CRC-32 of payload
/// - JSON-serialized Snapshot
///
/// Version checking is performed to ensure compatibility between the writer
/// and loader. If the snapshot version does not match the expected version,
/// `SnapshotLoaderError::IncompatibleVersion` is returned.
///
/// If the snapshot file does not exist, `load()` will return `Ok(None)`
/// instead of panicking.
pub struct SnapshotFsLoader {
    path: std::path::PathBuf,
    version: u32,
}

impl SnapshotFsLoader {
    /// Creates a new snapshot loader from the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path to the snapshot file
    pub fn new(path: std::path::PathBuf) -> Self {
        SnapshotFsLoader { path, version: 4 }
    }

    /// Creates a new snapshot loader from the given path with a custom version.
    ///
    /// This is useful for testing or when the loader needs to support
    /// multiple snapshot versions.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path to the snapshot file
    /// * `version` - The expected snapshot version
    pub fn with_version(path: std::path::PathBuf, version: u32) -> Self {
        SnapshotFsLoader { path, version }
    }

    /// Opens the snapshot file for reading.
    /// Returns Ok(None) if file doesn't exist, Ok(Some(file)) otherwise.
    fn open_file(&self) -> Result<Option<File>, SnapshotLoaderError> {
        match File::open(&self.path) {
            Ok(file) => Ok(Some(file)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SnapshotLoaderError::IoError(e.to_string())),
        }
    }

    /// Reads exactly `buf.len()` bytes, distinguishing clean EOF from partial reads.
    ///
    /// Returns `Ok(true)` if all bytes were read, `Ok(false)` if zero bytes
    /// were available (clean EOF), or an error for partial reads (truncation)
    /// and I/O failures.
    fn read_exact_or_eof(
        file: &mut File,
        buf: &mut [u8],
        frame_name: &str,
    ) -> Result<bool, SnapshotLoaderError> {
        let mut total_read = 0;
        while total_read < buf.len() {
            match file.read(&mut buf[total_read..]) {
                Ok(0) => {
                    return if total_read == 0 {
                        Ok(false)
                    } else {
                        Err(SnapshotLoaderError::DecodeError(format!(
                            "truncated snapshot {frame_name}: read {total_read} of {} bytes",
                            buf.len()
                        )))
                    };
                }
                Ok(n) => total_read += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(SnapshotLoaderError::IoError(e.to_string())),
            }
        }
        Ok(true)
    }

    /// Reads the version frame from the file.
    ///
    /// Returns `Ok(Some(version))` if read successfully, `Ok(None)` on clean
    /// EOF (empty file), or an error on truncation/I/O failure.
    fn read_version(file: &mut File) -> Result<Option<u32>, SnapshotLoaderError> {
        let mut buf = [0u8; 4];
        if Self::read_exact_or_eof(file, &mut buf, "version frame")? {
            Ok(Some(u32::from_le_bytes(buf)))
        } else {
            Ok(None)
        }
    }

    /// Reads the length frame from the file.
    ///
    /// EOF at this point is a truncation error (version was already read).
    fn read_length(file: &mut File) -> Result<usize, SnapshotLoaderError> {
        let mut buf = [0u8; 4];
        if Self::read_exact_or_eof(file, &mut buf, "length frame")? {
            Ok(u32::from_le_bytes(buf) as usize)
        } else {
            Err(SnapshotLoaderError::DecodeError(
                "truncated snapshot: EOF after version frame, expected length frame".to_string(),
            ))
        }
    }

    /// Reads the CRC-32 frame from the file.
    ///
    /// EOF at this point is a truncation error (version and length were already read).
    fn read_crc(file: &mut File) -> Result<u32, SnapshotLoaderError> {
        let mut buf = [0u8; 4];
        if Self::read_exact_or_eof(file, &mut buf, "CRC-32 frame")? {
            Ok(u32::from_le_bytes(buf))
        } else {
            Err(SnapshotLoaderError::DecodeError(
                "truncated snapshot: EOF after length frame, expected CRC-32 frame".to_string(),
            ))
        }
    }

    /// Reads the payload from the file.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to read from
    /// * `length` - The expected length of the payload
    ///
    /// # Returns
    ///
    /// Returns the payload bytes, or an error if the payload cannot be read.
    fn read_payload(file: &mut File, length: usize) -> Result<Vec<u8>, SnapshotLoaderError> {
        // Guard against unreasonable allocation from corrupted length field
        const MAX_REASONABLE_PAYLOAD: usize = 256 * 1024 * 1024; // 256 MiB
        if length > MAX_REASONABLE_PAYLOAD {
            return Err(SnapshotLoaderError::DecodeError(format!(
                "snapshot payload length {length} exceeds maximum {MAX_REASONABLE_PAYLOAD}"
            )));
        }
        let mut payload = vec![0u8; length];
        file.read_exact(&mut payload).map_err(|e| SnapshotLoaderError::IoError(e.to_string()))?;
        Ok(payload)
    }

    /// Decodes a snapshot from the payload bytes.
    ///
    /// # Arguments
    ///
    /// * `payload` - The JSON-encoded snapshot payload
    ///
    /// # Returns
    ///
    /// Returns the decoded snapshot, or an error if decoding fails.
    fn decode_snapshot(payload: &[u8]) -> Result<Snapshot, SnapshotLoaderError> {
        serde_json::from_slice(payload).map_err(|e| {
            SnapshotLoaderError::DecodeError(format!("Failed to decode snapshot: {e}"))
        })
    }
}

impl SnapshotLoader for SnapshotFsLoader {
    /// Load a snapshot from storage.
    ///
    /// The loader reads the version frame, validates it against the expected
    /// version, then reads the length and payload frames. The payload is
    /// decoded from JSON into a `Snapshot` struct.
    ///
    /// If the snapshot file does not exist, returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns `SnapshotLoaderError::IoError` if an I/O error occurs.
    /// Returns `SnapshotLoaderError::IncompatibleVersion` if the snapshot
    /// version does not match the expected version.
    /// Returns `SnapshotLoaderError::DecodeError` if the snapshot cannot
    /// be decoded.
    fn load(&mut self) -> Result<Option<Snapshot>, SnapshotLoaderError> {
        let mut file = match self.open_file()? {
            Some(f) => f,
            None => return Ok(None),
        };

        // Read version frame (clean EOF = empty file = no snapshot)
        let version = match Self::read_version(&mut file)? {
            Some(v) => v,
            None => return Ok(None),
        };

        // Validate version compatibility
        if version != self.version {
            tracing::warn!(
                expected = self.version,
                found = version,
                "snapshot version incompatible"
            );
            return Err(SnapshotLoaderError::IncompatibleVersion {
                expected: self.version,
                found: version,
            });
        }

        // Read length frame
        let length = Self::read_length(&mut file)?;

        // Read CRC-32 frame
        let expected_crc = Self::read_crc(&mut file)?;

        // Read payload
        let payload = Self::read_payload(&mut file, length)?;

        // Validate CRC-32
        let actual_crc = crc32fast::hash(&payload);
        if expected_crc != actual_crc {
            tracing::warn!(
                expected = format_args!("{expected_crc:#010x}"),
                actual = format_args!("{actual_crc:#010x}"),
                "snapshot CRC-32 mismatch"
            );
            return Err(SnapshotLoaderError::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // Decode snapshot
        let snapshot = match Self::decode_snapshot(&payload) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot decode failed");
                return Err(e);
            }
        };
        if let Err(e) = validate_snapshot(&snapshot) {
            tracing::warn!(error = %e, "snapshot validation failed");
            return Err(SnapshotLoaderError::MappingError(e));
        }

        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;

    use super::*;
    use crate::snapshot::mapping::{SnapshotMappingError, SNAPSHOT_SCHEMA_VERSION};
    use crate::snapshot::model::{SnapshotEngineControl, SnapshotMetadata};
    use crate::snapshot::writer::{SnapshotFsWriter, SnapshotWriter};

    // Atomic counter to ensure unique test file paths
    static TEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn temp_snapshot_path() -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path = dir.join(format!("actionqueue_snapshot_loader_test_{count}.tmp"));
        // Clean up if exists from previous test runs
        let _ = fs::remove_file(&path);
        path
    }

    fn open_snapshot_writer(path: std::path::PathBuf) -> SnapshotFsWriter {
        SnapshotFsWriter::new(path).expect("Failed to open snapshot writer for loader test")
    }

    fn create_test_snapshot() -> Snapshot {
        Snapshot {
            version: 4,
            timestamp: 1234567890,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 0,
                task_count: 0,
                run_count: 0,
            },
            tasks: Vec::new(),
            runs: Vec::new(),
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        }
    }

    #[test]
    fn test_new_loader_on_nonexistent_file() {
        let path = temp_snapshot_path();
        // Ensure file doesn't exist
        let _ = fs::remove_file(&path);

        // Creating a loader should not panic for missing file
        let mut loader = SnapshotFsLoader::new(path.clone());

        // load() should return Ok(None) for missing file
        let result = loader.load();
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_load_missing_file_via_with_version() {
        let path = temp_snapshot_path();
        // Ensure file doesn't exist
        let _ = fs::remove_file(&path);

        // Creating a loader with custom version should not panic for missing file
        let mut loader = SnapshotFsLoader::with_version(path.clone(), 3);

        // load() should return Ok(None) for missing file
        let result = loader.load();
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn test_load_returns_snapshot() {
        let path = temp_snapshot_path();

        // Write a snapshot first
        let snapshot = create_test_snapshot();
        let mut writer = open_snapshot_writer(path.clone());
        writer.write(&snapshot).expect("Write should succeed");
        writer.close().expect("Close should succeed");

        // Load the snapshot
        let mut loader = SnapshotFsLoader::new(path.clone());
        let loaded = loader.load().expect("Load should succeed");

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().version, 4);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_load_incompatible_version() {
        let path = temp_snapshot_path();

        // Create a snapshot file with an incompatible version
        {
            let mut file = File::create(&path).expect("Failed to create test file");
            // Write version 1 (incompatible with expected version 4)
            file.write_all(&1u32.to_le_bytes()).unwrap();
            // Write payload length (for empty JSON object)
            let payload = b"{}";
            file.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            // Write CRC-32
            file.write_all(&crc32fast::hash(payload).to_le_bytes()).unwrap();
            file.write_all(payload).unwrap();
            file.flush().unwrap();
        }

        // Create loader with expected version 4
        let mut loader = SnapshotFsLoader::new(path.clone());
        let result = loader.load();

        assert!(matches!(result, Err(SnapshotLoaderError::IncompatibleVersion { .. })));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_load_with_custom_version() {
        let path = temp_snapshot_path();

        // Write a snapshot with version 4
        {
            let mut writer = open_snapshot_writer(path.clone());
            let mut snapshot = create_test_snapshot();
            snapshot.version = 4;
            writer.write(&snapshot).expect("Write should succeed");
            writer.close().expect("Close should succeed");
        }

        // Load with custom version 4
        let mut loader = SnapshotFsLoader::with_version(path.clone(), 4);
        let loaded = loader.load().expect("Load should succeed");

        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().version, 4);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_load_invalid_json() {
        let path = temp_snapshot_path();

        // Create a file with valid framing but invalid JSON
        {
            let mut file = File::create(&path).expect("Failed to create test file");
            // Write version 4
            file.write_all(&4u32.to_le_bytes()).unwrap();
            // Write payload length for invalid JSON
            let payload = b"{invalid json}";
            file.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            // Write CRC-32
            file.write_all(&crc32fast::hash(payload).to_le_bytes()).unwrap();
            file.write_all(payload).unwrap();
            file.flush().unwrap();
        }

        let mut loader = SnapshotFsLoader::new(path.clone());
        let result = loader.load();

        assert!(matches!(result, Err(SnapshotLoaderError::DecodeError(_))));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_load_rejects_snapshot_mapping_violation() {
        let path = temp_snapshot_path();

        // Build a structurally valid snapshot JSON with inconsistent metadata.
        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234567890,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 0,
                task_count: 1,
                run_count: 0,
            },
            tasks: Vec::new(),
            runs: Vec::new(),
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };
        let payload = serde_json::to_vec(&snapshot).expect("snapshot should serialize");

        {
            let mut file = File::create(&path).expect("Failed to create test file");
            file.write_all(&4u32.to_le_bytes()).expect("version frame write should succeed");
            file.write_all(&(payload.len() as u32).to_le_bytes())
                .expect("length frame write should succeed");
            let crc = crc32fast::hash(&payload);
            file.write_all(&crc.to_le_bytes()).expect("crc frame write should succeed");
            file.write_all(&payload).expect("payload frame write should succeed");
            file.flush().expect("flush should succeed");
        }

        let mut loader = SnapshotFsLoader::new(path.clone());
        let result = loader.load();

        assert!(matches!(
            result,
            Err(SnapshotLoaderError::MappingError(SnapshotMappingError::TaskCountMismatch {
                declared: 1,
                actual: 0
            }))
        ));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_error_display() {
        assert_eq!(SnapshotLoaderError::NotFound.to_string(), "Snapshot file not found");
        assert_eq!(
            SnapshotLoaderError::IoError("test error".to_string()).to_string(),
            "I/O error: test error"
        );
        assert_eq!(
            SnapshotLoaderError::DecodeError("test error".to_string()).to_string(),
            "Decode error: test error"
        );
        assert_eq!(
            SnapshotLoaderError::MappingError(SnapshotMappingError::TaskCountMismatch {
                declared: 1,
                actual: 0
            })
            .to_string(),
            "Mapping error: snapshot task_count mismatch: declared 1, actual 0"
        );
        assert_eq!(
            SnapshotLoaderError::IncompatibleVersion { expected: 4, found: 1 }.to_string(),
            "Incompatible snapshot version: expected 4, found 1"
        );
        assert_eq!(
            SnapshotLoaderError::CrcMismatch { expected: 0xDEADBEEF, actual: 0x12345678 }
                .to_string(),
            "Snapshot CRC-32 mismatch: expected 0xdeadbeef, actual 0x12345678"
        );
    }

    #[test]
    fn test_load_rejects_old_version_3_format() {
        let path = temp_snapshot_path();

        // Write a file using the old version 3 format (no CRC-32 field)
        {
            let mut file = File::create(&path).expect("Failed to create test file");
            // Write version 3 (old format, incompatible with expected version 4)
            file.write_all(&3u32.to_le_bytes()).unwrap();
            let payload = b"{}";
            file.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            file.write_all(payload).unwrap();
            file.flush().unwrap();
        }

        let mut loader = SnapshotFsLoader::new(path.clone());
        let result = loader.load();

        assert!(matches!(
            result,
            Err(SnapshotLoaderError::IncompatibleVersion { expected: 4, found: 3 })
        ));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_load_detects_corrupted_payload() {
        let path = temp_snapshot_path();

        // Write a valid snapshot using the writer
        let snapshot = create_test_snapshot();
        let mut writer = open_snapshot_writer(path.clone());
        writer.write(&snapshot).expect("Write should succeed");
        writer.close().expect("Close should succeed");

        // Corrupt one byte in the payload (past the 12-byte header: version + length + crc)
        {
            let mut bytes = fs::read(&path).expect("snapshot file should be readable");
            assert!(bytes.len() > 12, "snapshot file should have header + payload");
            // Flip a bit in the first payload byte
            bytes[12] ^= 0xFF;
            fs::write(&path, &bytes).expect("corrupted snapshot should be writable");
        }

        let mut loader = SnapshotFsLoader::new(path.clone());
        let result = loader.load();

        assert!(
            matches!(result, Err(SnapshotLoaderError::CrcMismatch { .. })),
            "corrupted payload should produce CrcMismatch, got: {result:?}"
        );

        let _ = fs::remove_file(path);
    }
}
