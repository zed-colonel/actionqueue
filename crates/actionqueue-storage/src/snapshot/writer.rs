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
    ready_to_publish: bool,
    session: Option<crate::store::StoreSession>,
}

impl SnapshotFsWriter {
    pub fn new(session: &crate::store::StoreSession) -> Result<Self, SnapshotFsWriterInitError> {
        session.require_write().map_err(|e| SnapshotFsWriterInitError::IoError(e.to_string()))?;
        let mut writer = Self::at_path(session.snapshot_path())?;
        writer.session = Some(session.clone());
        Ok(writer)
    }
    #[cfg(any(test, feature = "testing"))]
    pub fn new_raw_for_test(path: PathBuf) -> Result<Self, SnapshotFsWriterInitError> {
        Self::at_path(path)
    }

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
    fn at_path(path: PathBuf) -> Result<Self, SnapshotFsWriterInitError> {
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
        temp_name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
        let temp_path = path.with_file_name(temp_name);
        let file = OpenOptions::new().create_new(true).write(true).open(&temp_path)?;

        Ok(SnapshotFsWriter {
            file,
            target_path: path,
            temp_path,
            is_closed: false,
            ready_to_publish: false,
            session: None,
        })
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

        self.ready_to_publish = false;
        validate_snapshot(snapshot).map_err(SnapshotWriterError::MappingError)?;
        if let Some(session) = &self.session {
            crate::recovery::bootstrap::validate_snapshot_for_session(session, snapshot)
                .map_err(|e| SnapshotWriterError::EncodeError(e.to_string()))?;
        }

        // Seek to beginning to overwrite
        self.seek_to_beginning()?;

        let bytes = super::envelope::encode(
            snapshot,
            self.session.as_ref().map(|s| s.manifest().store_id).unwrap_or_default(),
        )
        .map_err(SnapshotWriterError::EncodeError)?;
        self.file.set_len(0).map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;
        self.file.write_all(&bytes).map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;
        self.ready_to_publish = true;

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
        if !self.ready_to_publish {
            return Err(SnapshotWriterError::EncodeError(
                "no complete validated snapshot to publish".into(),
            ));
        }
        // Snapshot publication must never outrun durability of its covered WAL prefix.
        if let Some(session) = &self.session {
            File::open(session.wal_path())
                .and_then(|file| file.sync_all())
                .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;
        }
        // Flush all data to disk before renaming
        self.flush()?;

        crate::store::fault::checkpoint("snapshot_before_rename")
            .map_err(|e| SnapshotWriterError::IoError(e.to_string()))?;
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
