//! Snapshot writer interface and file system implementation.
//!
//! This module provides snapshot persistence using a file system backend with
//! versioned encoding for compatibility validation.

use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::path::PathBuf;

use crate::snapshot::mapping::{validate_snapshot, SnapshotMappingError};
use crate::snapshot::model::Snapshot;

/// Errors that can occur when creating a [`SnapshotFsWriter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotFsWriterInitError {
    /// I/O error when opening the snapshot file.
    IoError(String),
}

impl std::fmt::Display for SnapshotFsWriterInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotFsWriterInitError::IoError(e) => {
                write!(f, "I/O error when opening snapshot file: {e}")
            }
        }
    }
}

impl std::error::Error for SnapshotFsWriterInitError {}

impl std::convert::From<std::io::Error> for SnapshotFsWriterInitError {
    fn from(err: std::io::Error) -> Self {
        SnapshotFsWriterInitError::IoError(err.to_string())
    }
}

/// A snapshot writer that persists state to storage.
pub trait SnapshotWriter {
    /// Write a snapshot to storage.
    fn write(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotWriterError>;

    /// Flush pending writes to durable storage.
    fn flush(&mut self) -> Result<(), SnapshotWriterError>;

    /// Close the writer, releasing any resources.
    fn close(self) -> Result<(), SnapshotWriterError>;
}

/// Errors that can occur during snapshot writing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotWriterError {
    /// I/O error during write.
    IoError(String),
    /// The snapshot could not be encoded.
    EncodeError(String),
    /// The snapshot violated mapping/parity invariants before encode.
    MappingError(SnapshotMappingError),
    /// The writer was closed.
    Closed,
}

impl std::fmt::Display for SnapshotWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(e) => write!(f, "I/O error: {e}"),
            Self::EncodeError(e) => write!(f, "Encode error: {e}"),
            Self::MappingError(e) => write!(f, "Mapping error: {e}"),
            Self::Closed => write!(f, "snapshot writer is closed"),
        }
    }
}

impl std::error::Error for SnapshotWriterError {}

/// A file system backed snapshot writer using atomic write-to-temp-then-rename.
///
/// The writer opens a temporary sibling file (target path + `.tmp` suffix) and
/// writes encoded snapshot data to it using the same framing format as the WAL
/// (version + length + crc32 + payload). On [`close()`](SnapshotWriter::close), the
/// temp file is flushed, renamed atomically over the target path, and the
/// parent directory is fsynced. If the writer is dropped without calling
/// `close()`, the temp file is removed and the original snapshot (if any)
/// remains intact.
pub struct SnapshotFsWriter {
    file: File,
    target_path: PathBuf,
    temp_path: PathBuf,
    is_closed: bool,
}

impl SnapshotFsWriter {
    /// Creates a new snapshot writer at the given path.
    ///
    /// Instead of opening the target path directly, a temporary sibling file
    /// (target path with `.tmp` suffix appended) is created and written to. The
    /// target file is only replaced atomically when [`close()`](SnapshotWriter::close)
    /// is called, ensuring crash safety.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path where the snapshot file should ultimately reside
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotFsWriterInitError::IoError`] if the temporary file cannot
    /// be created or opened for writing.
    pub fn new(path: PathBuf) -> Result<Self, SnapshotFsWriterInitError> {
        // Build temp path by appending ".tmp" suffix (not replacing extension),
        // so "snapshot.bin" becomes "snapshot.bin.tmp", avoiding collisions when
        // the target path already uses a ".tmp" extension.
        let file_name = path.file_name().ok_or_else(|| {
            SnapshotFsWriterInitError::IoError(format!(
                "snapshot path has no filename component: {}",
                path.display()
            ))
        })?;
        let mut temp_name = file_name.to_os_string();
        temp_name.push(".tmp");
        let temp_path = path.with_file_name(temp_name);
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(&temp_path)?;

        Ok(SnapshotFsWriter { file, target_path: path, temp_path, is_closed: false })
    }

    /// Seeks to the beginning of the file for writing.
    fn seek_to_beginning(&mut self) -> Result<(), SnapshotWriterError> {
        self.file
            .seek(std::io::SeekFrom::Start(0))
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;
        Ok(())
    }
}

impl SnapshotWriter for SnapshotFsWriter {
    /// Write a snapshot to storage.
    ///
    /// The snapshot is encoded using the same framing format as the WAL:
    /// - 4 bytes version (currently 4)
    /// - 4 bytes length of payload
    /// - 4 bytes CRC-32 of payload
    /// - JSON-serialized Snapshot
    ///
    /// # Arguments
    ///
    /// * `snapshot` - The snapshot to write
    ///
    /// # Errors
    ///
    /// Returns `SnapshotWriterError::Closed` if the writer has been closed.
    /// Returns `SnapshotWriterError::IoError` if the write operation fails.
    /// Returns `SnapshotWriterError::EncodeError` if serialization fails.
    fn write(&mut self, snapshot: &Snapshot) -> Result<(), SnapshotWriterError> {
        if self.is_closed {
            return Err(SnapshotWriterError::Closed);
        }

        validate_snapshot(snapshot).map_err(SnapshotWriterError::MappingError)?;

        // Seek to beginning to overwrite
        self.seek_to_beginning()?;

        // Serialize the snapshot to JSON for deterministic representation
        let payload = serde_json::to_vec(snapshot)
            .map_err(|e| SnapshotWriterError::EncodeError(e.to_string()))?;

        // Version frame (4 bytes, little-endian)
        let version = snapshot.version;
        self.file
            .write_all(&version.to_le_bytes())
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        // Length frame (4 bytes, little-endian)
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            SnapshotWriterError::EncodeError(format!(
                "snapshot payload too large: {} bytes exceeds u32::MAX",
                payload.len()
            ))
        })?;
        self.file
            .write_all(&payload_len.to_le_bytes())
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        // CRC-32 frame (4 bytes, little-endian)
        let crc = crc32fast::hash(&payload);
        self.file
            .write_all(&crc.to_le_bytes())
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        // Write payload
        self.file.write_all(&payload).map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Flushes pending writes to durable storage.
    ///
    /// This ensures that all buffered data is written to disk before
    /// returning. The flush operation is synchronous and will block
    /// until the data is persisted.
    ///
    /// # Errors
    ///
    /// Returns `SnapshotWriterError::Closed` if the writer has been closed.
    /// Returns `SnapshotWriterError::IoError` if the flush operation fails.
    fn flush(&mut self) -> Result<(), SnapshotWriterError> {
        if self.is_closed {
            return Err(SnapshotWriterError::Closed);
        }

        self.file.sync_all().map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Closes the writer, atomically replacing the target snapshot file.
    ///
    /// This performs:
    /// 1. `sync_all()` on the temp file to ensure data is durable
    /// 2. `rename(temp, target)` for atomic replacement
    /// 3. `fsync` on the parent directory to make the rename durable
    ///
    /// Once closed, the writer cannot be used for further operations.
    /// If `close()` is not called (e.g. due to a crash or error), the
    /// temp file is cleaned up on drop and the original snapshot is preserved.
    ///
    /// # Errors
    ///
    /// Returns `SnapshotWriterError::IoError` if the flush, rename, or
    /// directory sync operation fails.
    fn close(mut self) -> Result<(), SnapshotWriterError> {
        // Flush all data to disk before renaming
        self.flush()?;

        // Atomic rename of temp file over target
        std::fs::rename(&self.temp_path, &self.target_path)
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;

        // Fsync parent directory to make the rename durable
        if let Some(parent) = self.target_path.parent() {
            let dir = File::open(parent).map_err(|e| {
                SnapshotWriterError::IoError(format!(
                    "failed to open snapshot parent directory for fsync: {e}"
                ))
            })?;
            dir.sync_all().map_err(|e| {
                SnapshotWriterError::IoError(format!(
                    "failed to fsync snapshot parent directory: {e}"
                ))
            })?;
        }

        // Mark as closed so Drop does not attempt temp cleanup
        self.is_closed = true;

        tracing::info!("snapshot written and persisted");

        Ok(())
    }
}

impl Drop for SnapshotFsWriter {
    fn drop(&mut self) {
        // If the writer was not explicitly closed, remove the temp file so
        // that the original snapshot (if any) remains intact.
        if !self.is_closed {
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::snapshot::mapping::{SnapshotMappingError, SNAPSHOT_SCHEMA_VERSION};
    use crate::snapshot::model::SnapshotMetadata;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_snapshot_path() -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = dir.join(format!(
            "actionqueue_snapshot_writer_test_{}_{}.snap",
            std::process::id(),
            count
        ));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(temp_sibling(&path));
        path
    }

    /// Returns the temp sibling path that SnapshotFsWriter would use internally
    /// (target filename + ".tmp" suffix).
    fn temp_sibling(path: &std::path::Path) -> std::path::PathBuf {
        let mut name =
            path.file_name().expect("test path must have a filename component").to_os_string();
        name.push(".tmp");
        path.with_file_name(name)
    }

    fn create_test_snapshot(payload: &[u8]) -> Snapshot {
        let task_spec = actionqueue_core::task::task_spec::TaskSpec::new(
            actionqueue_core::ids::TaskId::new(),
            actionqueue_core::task::task_spec::TaskPayload::with_content_type(
                payload.to_vec(),
                "application/octet-stream",
            ),
            actionqueue_core::task::run_policy::RunPolicy::Once,
            actionqueue_core::task::constraints::TaskConstraints::default(),
            actionqueue_core::task::metadata::TaskMetadata::default(),
        )
        .expect("test task spec should be valid");
        Snapshot {
            version: 4,
            timestamp: 1234567890,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 42,
                task_count: 1,
                run_count: 0,
            },
            tasks: vec![test_snapshot_task(task_spec)],
            runs: Vec::new(),
            engine: crate::snapshot::model::SnapshotEngineControl::default(),
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

    fn test_snapshot_task(
        task_spec: actionqueue_core::task::task_spec::TaskSpec,
    ) -> crate::snapshot::model::SnapshotTask {
        crate::snapshot::model::SnapshotTask {
            task_spec,
            created_at: 0,
            updated_at: None,
            canceled_at: None,
        }
    }

    #[test]
    fn test_new_creates_temp_file() {
        let path = temp_snapshot_path();
        let temp_path = temp_sibling(&path);
        let writer =
            SnapshotFsWriter::new(path.clone()).expect("snapshot writer creation should succeed");
        assert!(temp_path.exists(), "temp file should exist after new()");
        assert!(!path.exists(), "target file should not exist after new()");
        drop(writer);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&temp_path);
    }

    #[test]
    fn test_write_persists_snapshot_payload() {
        let path = temp_snapshot_path();
        let mut writer =
            SnapshotFsWriter::new(path.clone()).expect("snapshot writer creation should succeed");
        let snapshot = create_test_snapshot(&[1, 2, 3]);

        writer.write(&snapshot).expect("snapshot write should succeed");
        writer.flush().expect("snapshot flush should succeed");
        writer.close().expect("snapshot close should succeed");

        let bytes = fs::read(&path).expect("snapshot file should be readable");
        assert!(bytes.len() > 12);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reopen_truncates_existing_snapshot_file() {
        let path = temp_snapshot_path();

        {
            let mut writer = SnapshotFsWriter::new(path.clone())
                .expect("first snapshot writer creation should succeed");
            let large_snapshot = create_test_snapshot(&[9; 128]);
            writer.write(&large_snapshot).expect("first write should succeed");
            writer.close().expect("first close should succeed");
        }
        let len_before = fs::metadata(&path).expect("metadata should be readable").len();

        {
            let mut writer = SnapshotFsWriter::new(path.clone())
                .expect("second snapshot writer creation should succeed");
            let small_snapshot = create_test_snapshot(&[1]);
            writer.write(&small_snapshot).expect("second write should succeed");
            writer.close().expect("second close should succeed");
        }
        let len_after = fs::metadata(&path).expect("metadata should be readable").len();

        assert!(len_after < len_before);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_new_returns_error_when_parent_directory_is_missing() {
        let parent = std::env::temp_dir().join(format!(
            "actionqueue_snapshot_writer_missing_parent_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&parent);
        let path = parent.join("snapshot.bin");

        let result = SnapshotFsWriter::new(path);
        assert!(matches!(result, Err(SnapshotFsWriterInitError::IoError(_))));
    }

    #[test]
    fn test_write_rejects_mapping_violation() {
        let path = temp_snapshot_path();
        let temp_path = temp_sibling(&path);
        let mut writer =
            SnapshotFsWriter::new(path.clone()).expect("snapshot writer creation should succeed");

        let mut snapshot = create_test_snapshot(&[1, 2, 3]);
        snapshot.metadata.task_count = 99;

        let result = writer.write(&snapshot);
        assert!(matches!(
            result,
            Err(SnapshotWriterError::MappingError(SnapshotMappingError::TaskCountMismatch {
                declared: 99,
                actual: 1
            }))
        ));

        drop(writer);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&temp_path);
    }

    #[test]
    fn test_target_file_absent_until_close() {
        let path = temp_snapshot_path();
        let mut writer =
            SnapshotFsWriter::new(path.clone()).expect("snapshot writer creation should succeed");
        let snapshot = create_test_snapshot(&[1, 2, 3]);

        writer.write(&snapshot).expect("snapshot write should succeed");
        writer.flush().expect("snapshot flush should succeed");

        // Target file must NOT exist yet — only the temp file should be present
        assert!(!path.exists(), "target file should not exist before close()");
        assert!(temp_sibling(&path).exists(), "temp file should exist before close()");

        writer.close().expect("snapshot close should succeed");

        // Now the target file should exist and temp should be gone
        assert!(path.exists(), "target file should exist after close()");
        assert!(!temp_sibling(&path).exists(), "temp file should not exist after close()");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_drop_without_close_preserves_original() {
        let path = temp_snapshot_path();

        // Write an initial snapshot and close it properly
        {
            let mut writer = SnapshotFsWriter::new(path.clone())
                .expect("first snapshot writer creation should succeed");
            let snapshot = create_test_snapshot(&[10, 20, 30]);
            writer.write(&snapshot).expect("first write should succeed");
            writer.close().expect("first close should succeed");
        }

        let original_bytes = fs::read(&path).expect("original snapshot should be readable");
        assert!(!original_bytes.is_empty(), "original snapshot should not be empty");

        // Open a new writer (writes to temp), write data, then drop WITHOUT close
        {
            let mut writer = SnapshotFsWriter::new(path.clone())
                .expect("second snapshot writer creation should succeed");
            let different_snapshot = create_test_snapshot(&[99; 64]);
            writer.write(&different_snapshot).expect("second write should succeed");
            // Intentionally drop without calling close()
        }

        // Original snapshot must be intact
        let preserved_bytes =
            fs::read(&path).expect("snapshot should still be readable after aborted write");
        assert_eq!(
            original_bytes, preserved_bytes,
            "original snapshot content should be preserved when writer is dropped without close()"
        );

        // Temp file should have been cleaned up by Drop
        assert!(!temp_sibling(&path).exists(), "temp file should be cleaned up on drop");

        let _ = fs::remove_file(path);
    }
}
