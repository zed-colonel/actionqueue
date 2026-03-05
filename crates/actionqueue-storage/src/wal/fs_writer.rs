//! WAL file system writer.
//!
//! This module provides a file system backed WAL writer that appends events
//! to a file using the encoded binary format from the codec module.
//!
//! The writer maintains an open file handle and appends encoded events
//! sequentially. Flush operations ensure data is persisted to disk.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::wal::codec::encode;
use crate::wal::event::WalEvent;
use crate::wal::repair::RepairPolicy;
use crate::wal::tail_validation::WalCorruption;
use crate::wal::writer::{WalWriter, WalWriterError};

/// Internal result type for streaming WAL record reads during bootstrap.
enum StreamingReadResult {
    /// A complete, valid record was read with the given sequence number.
    Record { sequence: u64 },
    /// Clean end of file (no more data).
    Eof,
    /// Corruption detected at the given offset.
    Corruption(WalCorruption),
    /// An I/O error occurred.
    IoError(String),
}

/// Errors that can occur when creating a [`WalFsWriter`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFsWriterInitError {
    /// I/O error when opening or bootstrapping the WAL file.
    IoError(String),
    /// Strict corruption detected while validating existing WAL bytes.
    Corruption(WalCorruption),
}

impl std::fmt::Display for WalFsWriterInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalFsWriterInitError::IoError(e) => {
                write!(f, "I/O error when opening or bootstrapping WAL file: {e}")
            }
            WalFsWriterInitError::Corruption(details) => {
                write!(f, "strict WAL tail validation failed during bootstrap: {details}")
            }
        }
    }
}

impl std::error::Error for WalFsWriterInitError {}

impl std::convert::From<std::io::Error> for WalFsWriterInitError {
    fn from(err: std::io::Error) -> Self {
        WalFsWriterInitError::IoError(err.to_string())
    }
}

impl std::convert::From<WalCorruption> for WalFsWriterInitError {
    fn from(err: WalCorruption) -> Self {
        WalFsWriterInitError::Corruption(err)
    }
}

/// A file system backed WAL writer.
///
/// The writer opens a file in read-write mode and manually seeks to end
/// before each write. On write failure, it truncates back to the pre-write
/// position to ensure atomicity. If truncation itself fails, the writer
/// becomes permanently poisoned and rejects all subsequent appends.
pub struct WalFsWriter {
    file: File,
    current_sequence: u64,
    is_closed: bool,
    poisoned: bool,
}

impl WalFsWriter {
    /// Creates a new WAL writer at the given path.
    ///
    /// If the file does not exist, it will be created. If it exists, events
    /// will be appended to the end of the file.
    ///
    /// During initialization, the writer strictly validates all existing framed
    /// records from the start of the WAL. Any trailing corruption (partial
    /// header, partial payload, unsupported version, or decode failure)
    /// hard-fails bootstrap with typed corruption diagnostics.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path where the WAL file should be located
    ///
    /// # Errors
    ///
    /// Returns [`WalFsWriterInitError::IoError`] if the WAL file cannot be
    /// opened or read during bootstrap.
    ///
    /// Returns [`WalFsWriterInitError::Corruption`] if strict WAL tail
    /// validation detects corruption in existing bytes.
    pub fn new(path: std::path::PathBuf) -> Result<Self, WalFsWriterInitError> {
        Self::new_with_repair(path, RepairPolicy::Strict)
    }

    /// Creates a new WAL writer with the specified repair policy.
    ///
    /// Under [`RepairPolicy::Strict`], behaves identically to [`new`](Self::new).
    ///
    /// Under [`RepairPolicy::TruncatePartial`], if only trailing bytes are
    /// corrupt (incomplete record at the end), they are truncated and the
    /// writer opens normally from the last valid record. Mid-stream
    /// corruption (corrupt record followed by valid records) still hard-fails.
    pub fn new_with_repair(
        path: std::path::PathBuf,
        policy: RepairPolicy,
    ) -> Result<Self, WalFsWriterInitError> {
        match policy {
            RepairPolicy::Strict => {
                let mut file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                let current_sequence = Self::load_current_sequence_strict(&file)?;
                file.seek(SeekFrom::End(0))?;
                Ok(WalFsWriter { file, current_sequence, is_closed: false, poisoned: false })
            }
            RepairPolicy::TruncatePartial => {
                let file = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&path)?;
                let (current_sequence, needs_truncation) =
                    Self::load_current_sequence_lenient(&file)?;

                if let Some(valid_end_offset) = needs_truncation {
                    drop(file);
                    crate::wal::repair::truncate_to_last_valid(&path, valid_end_offset)
                        .map_err(|e| WalFsWriterInitError::IoError(e.to_string()))?;
                    let mut file = OpenOptions::new()
                        .create(true)
                        .truncate(false)
                        .read(true)
                        .write(true)
                        .open(&path)?;
                    file.seek(SeekFrom::End(0))?;
                    Ok(WalFsWriter { file, current_sequence, is_closed: false, poisoned: false })
                } else {
                    let mut file = file;
                    file.seek(SeekFrom::End(0))?;
                    Ok(WalFsWriter { file, current_sequence, is_closed: false, poisoned: false })
                }
            }
        }
    }

    /// Loads the current sequence number from the file using streaming
    /// record-by-record validation. Any corruption returns a typed bootstrap error.
    fn load_current_sequence_strict(file: &File) -> Result<u64, WalFsWriterInitError> {
        let mut file_ref = file.try_clone()?;
        file_ref.seek(SeekFrom::Start(0))?;

        let mut last_sequence = 0u64;
        loop {
            match Self::read_next_record_sequence(&mut file_ref) {
                StreamingReadResult::Record { sequence } => last_sequence = sequence,
                StreamingReadResult::Eof => return Ok(last_sequence),
                StreamingReadResult::Corruption(corruption) => {
                    return Err(WalFsWriterInitError::Corruption(corruption));
                }
                StreamingReadResult::IoError(e) => {
                    return Err(WalFsWriterInitError::IoError(e));
                }
            }
        }
    }

    /// Loads sequence via lenient streaming validation, returning the valid end
    /// offset if truncation is needed.
    fn load_current_sequence_lenient(
        file: &File,
    ) -> Result<(u64, Option<u64>), WalFsWriterInitError> {
        let mut file_ref = file.try_clone()?;
        file_ref.seek(SeekFrom::Start(0))?;

        let mut last_sequence = 0u64;
        loop {
            let record_start = file_ref
                .stream_position()
                .map_err(|e| WalFsWriterInitError::IoError(e.to_string()))?;
            match Self::read_next_record_sequence(&mut file_ref) {
                StreamingReadResult::Record { sequence } => {
                    last_sequence = sequence;
                }
                StreamingReadResult::Eof => return Ok((last_sequence, None)),
                StreamingReadResult::Corruption(_) => {
                    // Lenient: trailing corruption is truncatable
                    return Ok((last_sequence, Some(record_start)));
                }
                StreamingReadResult::IoError(e) => {
                    return Err(WalFsWriterInitError::IoError(e));
                }
            }
        }
    }

    /// Reads a single WAL record header+payload, returning the sequence number.
    /// Uses two-phase read: header first, then payload.
    fn read_next_record_sequence(file: &mut File) -> StreamingReadResult {
        use crate::wal::codec::{HEADER_LEN, VERSION};
        use crate::wal::tail_validation::{WalCorruption, WalCorruptionReasonCode};

        let record_start = match file.stream_position() {
            Ok(pos) => pos,
            Err(e) => return StreamingReadResult::IoError(e.to_string()),
        };

        // Phase 1: Read 12-byte header
        let mut header = [0u8; HEADER_LEN];
        let mut total = 0;
        while total < HEADER_LEN {
            match file.read(&mut header[total..]) {
                Ok(0) => {
                    return if total == 0 {
                        StreamingReadResult::Eof
                    } else {
                        StreamingReadResult::Corruption(WalCorruption {
                            offset: record_start,
                            reason: WalCorruptionReasonCode::IncompleteHeader,
                        })
                    };
                }
                Ok(n) => total += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return StreamingReadResult::IoError(e.to_string()),
            }
        }

        let version = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap());

        if version != VERSION {
            return StreamingReadResult::Corruption(WalCorruption {
                offset: record_start,
                reason: WalCorruptionReasonCode::UnsupportedVersion,
            });
        }

        let payload_len_usize = payload_len as usize;

        // Guard against unreasonable allocation from corrupted length field
        const MAX_REASONABLE_PAYLOAD: usize = 256 * 1024 * 1024; // 256 MiB
        if payload_len_usize > MAX_REASONABLE_PAYLOAD {
            return StreamingReadResult::Corruption(WalCorruption {
                offset: record_start,
                reason: WalCorruptionReasonCode::DecodeFailure,
            });
        }

        // Phase 2: Read payload bytes
        let mut payload = vec![0u8; payload_len_usize];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return StreamingReadResult::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::IncompletePayload,
                });
            }
            Err(e) => return StreamingReadResult::IoError(e.to_string()),
        }

        // Assemble full record and decode to extract sequence
        let mut record = Vec::with_capacity(HEADER_LEN + payload_len_usize);
        record.extend_from_slice(&header);
        record.extend_from_slice(&payload);

        match crate::wal::codec::decode(&record) {
            Ok(event) => StreamingReadResult::Record { sequence: event.sequence() },
            Err(crate::wal::codec::DecodeError::CrcMismatch { .. }) => {
                StreamingReadResult::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::CrcMismatch,
                })
            }
            Err(_) => StreamingReadResult::Corruption(WalCorruption {
                offset: record_start,
                reason: WalCorruptionReasonCode::DecodeFailure,
            }),
        }
    }

    /// Returns the highest sequence number observed in the WAL (0 for empty WAL).
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }
}

impl WalWriter for WalFsWriter {
    /// Append an event to the WAL.
    ///
    /// The event is encoded using the codec's `encode` function and
    /// appended to the underlying file. The current sequence tracker
    /// is updated to the event's sequence number if it's higher.
    ///
    /// # Arguments
    ///
    /// * `event` - The WAL event to append
    ///
    /// # Errors
    ///
    /// Returns `WalWriterError::Closed` if the writer has been closed.
    /// Returns `WalWriterError::IoError` if the write operation fails.
    fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError> {
        if self.is_closed {
            return Err(WalWriterError::Closed);
        }
        if self.poisoned {
            return Err(WalWriterError::Poisoned);
        }

        // Enforce strict sequence monotonicity
        if event.sequence() <= self.current_sequence {
            return Err(WalWriterError::SequenceViolation {
                expected: self.current_sequence.saturating_add(1),
                provided: event.sequence(),
            });
        }

        // Encode the event to bytes
        let bytes = encode(event).map_err(|e| WalWriterError::EncodeError(e.to_string()))?;

        // Record pre-write position for truncation on failure
        let pre_write_pos =
            self.file.stream_position().map_err(|e| WalWriterError::IoError(e.to_string()))?;

        // Write the encoded bytes to the file
        if let Err(write_err) = self.file.write_all(&bytes) {
            // Attempt to truncate back to pre-write position
            let truncate_result = self.file.set_len(pre_write_pos);
            let seek_result = if truncate_result.is_ok() {
                self.file.seek(SeekFrom::End(0))
            } else {
                Err(std::io::Error::other("skipped after truncate failure"))
            };

            if truncate_result.is_err() || seek_result.is_err() {
                tracing::error!(
                    write_error = %write_err,
                    truncate_ok = truncate_result.is_ok(),
                    seek_ok = seek_result.is_ok(),
                    pre_write_pos,
                    "WAL write failed and recovery truncation also failed; \
                     writer is permanently poisoned"
                );
                self.poisoned = true;
                return Err(WalWriterError::Poisoned);
            }
            return Err(WalWriterError::IoError(write_err.to_string()));
        }

        // Update current sequence
        self.current_sequence = event.sequence();

        tracing::debug!(sequence = event.sequence(), "WAL event appended");

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
    /// Returns `WalWriterError::Closed` if the writer has been closed.
    /// Returns `WalWriterError::IoError` if the flush operation fails.
    fn flush(&mut self) -> Result<(), WalWriterError> {
        if self.is_closed {
            return Err(WalWriterError::Closed);
        }

        // Sync the file data to disk
        self.file.sync_all().map_err(|e| WalWriterError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Closes the writer, releasing the file handle.
    ///
    /// A flush is performed before closing to ensure all data is
    /// persisted. Once closed, the writer cannot be used for further
    /// operations.
    ///
    /// # Errors
    ///
    /// Returns `WalWriterError::IoError` if the flush or close operation fails.
    fn close(self) -> Result<(), WalWriterError> {
        let mut this = self;

        // Flush before closing
        this.flush()?;

        // Mark as closed so Drop does not redundantly call sync_all
        this.is_closed = true;

        Ok(())
    }
}

impl Drop for WalFsWriter {
    fn drop(&mut self) {
        // If the writer was not explicitly closed, try to flush on drop
        // This is a best-effort approach since we can't return errors from drop
        if !self.is_closed {
            let _ = self.file.sync_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::wal::codec;
    use crate::wal::tail_validation::WalCorruptionReasonCode;

    // Atomic counter to ensure unique test file paths across parallel tests.
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_wal_path() -> PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        // Unique per invocation to avoid cross-test collisions under parallel execution.
        let path =
            dir.join(format!("actionqueue_wal_writer_test_{}_{}.tmp", std::process::id(), count));
        // Clean up if exists from previous test runs
        let _ = fs::remove_file(&path);
        path
    }

    fn create_test_task_spec(payload: Vec<u8>) -> actionqueue_core::task::task_spec::TaskSpec {
        actionqueue_core::task::task_spec::TaskSpec::new(
            actionqueue_core::ids::TaskId::new(),
            actionqueue_core::task::task_spec::TaskPayload::with_content_type(
                payload,
                "application/octet-stream",
            ),
            actionqueue_core::task::run_policy::RunPolicy::Once,
            actionqueue_core::task::constraints::TaskConstraints::default(),
            actionqueue_core::task::metadata::TaskMetadata::default(),
        )
        .expect("test task spec should be valid")
    }

    #[test]
    fn test_creates_new_file() {
        let path = temp_wal_path();
        let writer =
            WalFsWriter::new(path.clone()).expect("writer creation should succeed for new file");

        assert!(path.exists());
        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_append_writes_encoded_event() {
        let path = temp_wal_path();
        let mut writer =
            WalFsWriter::new(path.clone()).expect("writer creation should succeed for append test");

        let event = WalEvent::new(
            1,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );

        writer.append(&event).expect("Append should succeed");

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_flush_succeeds() {
        let path = temp_wal_path();
        let mut writer =
            WalFsWriter::new(path.clone()).expect("writer creation should succeed for flush test");

        let event = WalEvent::new(
            1,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );

        writer.append(&event).expect("Append should succeed");
        writer.flush().expect("Flush should succeed");

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_close_succeeds() {
        let path = temp_wal_path();
        let writer =
            WalFsWriter::new(path.clone()).expect("writer creation should succeed for close test");
        writer.close().expect("Close should succeed");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_bootstrap_empty_wal() {
        let path = temp_wal_path();
        let mut writer = WalFsWriter::new(path.clone())
            .expect("writer creation should succeed for empty bootstrap test");

        // For an empty file, current_sequence should be 0
        // The writer's current_sequence field is not public, so we test via
        // the behavior of appending events starting from sequence 0

        // Add an event and verify the sequence continues correctly
        let event = WalEvent::new(
            5,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );

        writer.append(&event).expect("Append should succeed");
        drop(writer);

        // Reopen and verify sequence tracking works
        let mut writer2 = WalFsWriter::new(path.clone())
            .expect("writer reopening should succeed for empty bootstrap test");
        let event2 = WalEvent::new(
            10,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![4, 5, 6]),
                timestamp: 0,
            },
        );
        writer2.append(&event2).expect("Append should succeed");
        drop(writer2);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_bootstrap_with_existing_events() {
        let path = temp_wal_path();

        // First, create events and write them using a temporary writer
        {
            let mut writer = WalFsWriter::new(path.clone())
                .expect("initial writer creation should succeed for bootstrap test");
            let event1 = WalEvent::new(
                3,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            let event2 = WalEvent::new(
                7,
                crate::wal::event::WalEventType::RunStateChanged {
                    run_id: actionqueue_core::ids::RunId::new(),
                    previous_state: actionqueue_core::run::state::RunState::Scheduled,
                    new_state: actionqueue_core::run::state::RunState::Running,
                    timestamp: 1000,
                },
            );
            let event3 = WalEvent::new(
                12,
                crate::wal::event::WalEventType::AttemptStarted {
                    run_id: actionqueue_core::ids::RunId::new(),
                    attempt_id: actionqueue_core::ids::AttemptId::new(),
                    timestamp: 2000,
                },
            );
            writer.append(&event1).expect("Append should succeed");
            writer.append(&event2).expect("Append should succeed");
            writer.append(&event3).expect("Append should succeed");
            writer.flush().expect("Flush should succeed");
        }

        // Reopen the file and verify that the sequence is properly bootstrapped
        // The writer should now have current_sequence = 12
        let mut writer = WalFsWriter::new(path.clone())
            .expect("writer reopening should succeed for bootstrap test");

        // Add a new event and verify it continues from the bootstrap sequence
        let event = WalEvent::new(
            15, // Higher than the last sequence
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![7, 8, 9]),
                timestamp: 0,
            },
        );
        writer.append(&event).expect("Append should succeed");

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_bootstrap_fails_with_partial_record() {
        let path = temp_wal_path();

        // Write some complete events first
        {
            let mut writer = WalFsWriter::new(path.clone())
                .expect("initial writer creation should succeed for partial bootstrap test");
            let event1 = WalEvent::new(
                5,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            let event2 = WalEvent::new(
                10,
                crate::wal::event::WalEventType::RunStateChanged {
                    run_id: actionqueue_core::ids::RunId::new(),
                    previous_state: actionqueue_core::run::state::RunState::Scheduled,
                    new_state: actionqueue_core::run::state::RunState::Running,
                    timestamp: 1000,
                },
            );
            writer.append(&event1).expect("Append should succeed");
            writer.append(&event2).expect("Append should succeed");
            writer.flush().expect("Flush should succeed");
        }

        let expected_offset = {
            let first = codec::encode(&WalEvent::new(
                5,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            ))
            .expect("encode should succeed")
            .len() as u64;

            let second = codec::encode(&WalEvent::new(
                10,
                crate::wal::event::WalEventType::RunStateChanged {
                    run_id: actionqueue_core::ids::RunId::new(),
                    previous_state: actionqueue_core::run::state::RunState::Scheduled,
                    new_state: actionqueue_core::run::state::RunState::Running,
                    timestamp: 1000,
                },
            ))
            .expect("encode should succeed")
            .len() as u64;

            first + second
        };

        // Append a trailing partial payload record.
        {
            use std::fs::OpenOptions;
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .expect("Failed to open file");

            file.write_all(&codec::VERSION.to_le_bytes()).expect("Failed to write version");
            file.write_all(&50u32.to_le_bytes()).expect("Failed to write length");
            file.write_all(&0u32.to_le_bytes()).expect("Failed to write CRC");
            file.write_all(&[0u8; 20]).expect("Failed to write partial payload");
            file.sync_all().expect("Failed to flush");
        }

        let result = WalFsWriter::new(path.clone());
        assert!(matches!(
            result,
            Err(WalFsWriterInitError::Corruption(WalCorruption {
                offset,
                reason: WalCorruptionReasonCode::IncompletePayload
            })) if offset == expected_offset
        ));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_new_returns_error_when_parent_directory_is_missing() {
        let parent = std::env::temp_dir().join(format!(
            "actionqueue_wal_writer_missing_parent_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&parent);
        let path = parent.join("wal.log");

        let result = WalFsWriter::new(path);
        assert!(matches!(result, Err(WalFsWriterInitError::IoError(_))));
    }

    #[test]
    fn test_rejects_duplicate_sequence() {
        let path = temp_wal_path();
        let mut writer = WalFsWriter::new(path.clone())
            .expect("writer creation should succeed for duplicate test");

        // First event should succeed
        let event1 = WalEvent::new(
            1,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );
        writer.append(&event1).expect("First append should succeed");

        // Duplicate sequence should be rejected
        let event2 = WalEvent::new(
            1, // Same as event1
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![4, 5, 6]),
                timestamp: 0,
            },
        );
        let result = writer.append(&event2);
        assert!(matches!(
            result,
            Err(WalWriterError::SequenceViolation { expected: 2, provided: 1 })
        ));

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_rejects_non_increasing_sequence() {
        let path = temp_wal_path();
        let mut writer = WalFsWriter::new(path.clone())
            .expect("writer creation should succeed for non-increasing test");

        // First event should succeed
        let event1 = WalEvent::new(
            5,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );
        writer.append(&event1).expect("First append should succeed");

        // Lower sequence should be rejected
        let event2 = WalEvent::new(
            3, // Lower than event1
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![4, 5, 6]),
                timestamp: 0,
            },
        );
        let result = writer.append(&event2);
        assert!(matches!(
            result,
            Err(WalWriterError::SequenceViolation { expected: 6, provided: 3 })
        ));

        // Same sequence should also be rejected
        let event3 = WalEvent::new(
            5, // Same as event1
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![7, 8, 9]),
                timestamp: 0,
            },
        );
        let result = writer.append(&event3);
        assert!(matches!(
            result,
            Err(WalWriterError::SequenceViolation { expected: 6, provided: 5 })
        ));

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_sequence_rejection_preserves_file() {
        let path = temp_wal_path();
        let mut writer = WalFsWriter::new(path.clone())
            .expect("writer creation should succeed for preservation test");

        // Valid event should be written
        let event1 = WalEvent::new(
            1,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![1, 2, 3]),
                timestamp: 0,
            },
        );
        writer.append(&event1).expect("First append should succeed");
        writer.flush().expect("Flush should succeed");

        // Invalid event should not corrupt file
        let event2 = WalEvent::new(
            1, // Duplicate
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![4, 5, 6]),
                timestamp: 0,
            },
        );
        let result = writer.append(&event2);
        assert!(matches!(result, Err(WalWriterError::SequenceViolation { .. })));

        // Writer should still be usable with correct sequence
        let event3 = WalEvent::new(
            2,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![7, 8, 9]),
                timestamp: 0,
            },
        );
        writer.append(&event3).expect("Append after rejection should succeed");

        drop(writer);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_sequence_rejection_across_writer_restart() {
        let path = temp_wal_path();

        // First writer session - write some events
        {
            let mut writer = WalFsWriter::new(path.clone())
                .expect("writer creation should succeed for restart test");
            let event1 = WalEvent::new(
                1,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            writer.append(&event1).expect("First append should succeed");
            let event2 = WalEvent::new(
                2,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![4, 5, 6]),
                    timestamp: 0,
                },
            );
            writer.append(&event2).expect("Second append should succeed");
        }

        // Second writer session (bootstrap from file) - should reject old sequences
        {
            let mut writer = WalFsWriter::new(path.clone())
                .expect("writer reopening should succeed for restart test");

            // Sequence 1 should be rejected (already in file)
            let event1 = WalEvent::new(
                1,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![7, 8, 9]),
                    timestamp: 0,
                },
            );
            let result = writer.append(&event1);
            assert!(matches!(
                result,
                Err(WalWriterError::SequenceViolation { expected: 3, provided: 1 })
            ));

            // Sequence 2 should also be rejected
            let event2 = WalEvent::new(
                2,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![10, 11, 12]),
                    timestamp: 0,
                },
            );
            let result = writer.append(&event2);
            assert!(matches!(
                result,
                Err(WalWriterError::SequenceViolation { expected: 3, provided: 2 })
            ));

            // Sequence 3 should succeed (continues from last sequence in file)
            let event3 = WalEvent::new(
                3,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![13, 14, 15]),
                    timestamp: 0,
                },
            );
            writer.append(&event3).expect("Append after bootstrap should succeed");
        }

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_strict_policy_fails_on_partial_record() {
        let path = temp_wal_path();

        // Write a valid event then a partial record
        {
            let mut writer = WalFsWriter::new(path.clone()).expect("initial writer should succeed");
            let event = WalEvent::new(
                1,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            writer.append(&event).expect("append should succeed");
            writer.flush().expect("flush should succeed");
        }

        // Append partial record (version + length + CRC + partial payload)
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&codec::VERSION.to_le_bytes()).unwrap();
            file.write_all(&50u32.to_le_bytes()).unwrap();
            file.write_all(&0u32.to_le_bytes()).unwrap(); // CRC (dummy)
            file.write_all(&[0u8; 10]).unwrap(); // only 10 of 50 bytes
            file.sync_all().unwrap();
        }

        let result = WalFsWriter::new_with_repair(path.clone(), RepairPolicy::Strict);
        assert!(matches!(result, Err(WalFsWriterInitError::Corruption(_))));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_truncate_partial_repairs_and_continues() {
        let path = temp_wal_path();

        // Write 2 valid events
        {
            let mut writer = WalFsWriter::new(path.clone()).expect("initial writer should succeed");
            let event1 = WalEvent::new(
                1,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            let event2 = WalEvent::new(
                2,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![4, 5, 6]),
                    timestamp: 0,
                },
            );
            writer.append(&event1).expect("append 1 should succeed");
            writer.append(&event2).expect("append 2 should succeed");
            writer.flush().expect("flush should succeed");
        }

        // Append partial 3rd record (version + length + CRC + partial payload)
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&codec::VERSION.to_le_bytes()).unwrap();
            file.write_all(&100u32.to_le_bytes()).unwrap();
            file.write_all(&0u32.to_le_bytes()).unwrap(); // CRC (dummy)
            file.write_all(&[0xAB; 20]).unwrap(); // partial payload
            file.sync_all().unwrap();
        }

        // TruncatePartial should succeed
        let mut writer = WalFsWriter::new_with_repair(path.clone(), RepairPolicy::TruncatePartial)
            .expect("repair should succeed");

        // Should be able to append event 3 after repair
        let event3 = WalEvent::new(
            3,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: create_test_task_spec(vec![7, 8, 9]),
                timestamp: 0,
            },
        );
        writer.append(&event3).expect("append after repair should succeed");
        writer.flush().expect("flush should succeed");

        drop(writer);

        // Verify the file is valid by opening with strict mode
        let writer2 =
            WalFsWriter::new(path.clone()).expect("strict open after repair should succeed");
        assert_eq!(writer2.current_sequence(), 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_truncate_partial_no_corruption_matches_strict() {
        let path = temp_wal_path();

        // Write clean events
        {
            let mut writer = WalFsWriter::new(path.clone()).expect("initial writer should succeed");
            let event = WalEvent::new(
                1,
                crate::wal::event::WalEventType::TaskCreated {
                    task_spec: create_test_task_spec(vec![1, 2, 3]),
                    timestamp: 0,
                },
            );
            writer.append(&event).expect("append should succeed");
            writer.flush().expect("flush should succeed");
        }

        // TruncatePartial on clean file should behave like Strict
        let writer = WalFsWriter::new_with_repair(path.clone(), RepairPolicy::TruncatePartial)
            .expect("repair on clean file should succeed");
        assert_eq!(writer.current_sequence(), 1);

        let _ = fs::remove_file(path);
    }
}
