//! WAL file system reader.
//!
//! This module provides a file system backed WAL reader that sequentially reads
//! events from a file using the encoded binary format from the codec module.
//!
//! The reader enforces strict framed-record integrity with one shared tail
//! validator. Any corruption (including partial header, partial payload,
//! unsupported version, or decode-invalid record) is surfaced as typed
//! [`WalReaderError::Corruption`] details with record-start offsets.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use crate::wal::codec::{decode, HEADER_LEN, VERSION};
use crate::wal::event::WalEvent;
use crate::wal::reader::{WalReader, WalReaderError};
use crate::wal::tail_validation::{WalCorruption, WalCorruptionReasonCode};

/// Errors that can occur when creating a [`WalFsReader`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFsReaderError {
    /// I/O error when opening the file.
    IoError(String),
}

impl std::fmt::Display for WalFsReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalFsReaderError::IoError(e) => write!(f, "I/O error when opening WAL file: {e}"),
        }
    }
}

impl std::error::Error for WalFsReaderError {}

impl std::convert::From<std::io::Error> for WalFsReaderError {
    fn from(err: std::io::Error) -> Self {
        WalFsReaderError::IoError(err.to_string())
    }
}

/// Result of a read-exact-or-eof operation.
enum ReadExactResult {
    /// All requested bytes were read successfully.
    Ok,
    /// Zero bytes available — clean end of file.
    Eof,
    /// Some bytes were read but not enough — partial/corrupt record.
    Partial,
    /// An I/O error other than EOF occurred.
    IoError(String),
}

/// Reads exactly `buf.len()` bytes, distinguishing clean EOF from partial reads.
///
/// Unlike `read_exact` (which returns `UnexpectedEof` for both 0-bytes-available
/// and partial reads), this function returns distinct results for each case.
fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> ReadExactResult {
    let mut total_read = 0;
    while total_read < buf.len() {
        match reader.read(&mut buf[total_read..]) {
            std::result::Result::Ok(0) => {
                return if total_read == 0 {
                    ReadExactResult::Eof
                } else {
                    ReadExactResult::Partial
                };
            }
            std::result::Result::Ok(n) => total_read += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return ReadExactResult::IoError(e.to_string()),
        }
    }
    ReadExactResult::Ok
}

/// A file system backed WAL reader.
///
/// The reader opens a file and sequentially reads encoded WAL events.
/// Each event is expected to follow the framed binary format from the codec:
/// - 4 bytes version (currently 2)
/// - 4 bytes length of payload
/// - 4 bytes CRC-32 checksum
/// - payload bytes
///
/// If strict framed validation finds corruption, the reader returns
/// [`WalReaderError::Corruption`] with typed offset+reason diagnostics.
pub struct WalFsReader {
    file: File,
    current_sequence: u64,
    is_end: bool,
    pending_event: Option<WalEvent>,
}

impl WalFsReader {
    /// Creates a new WAL reader from the given path.
    ///
    /// Opens the file in read-only mode and positions the reader at the
    /// beginning of the file. The reader will start reading from sequence
    /// 0 (or the first event in the file).
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path to the WAL file
    ///
    /// # Errors
    ///
    /// Returns `WalFsReaderError::IoError` if the file cannot be opened.
    pub fn new(path: std::path::PathBuf) -> Result<Self, WalFsReaderError> {
        let file = File::open(&path)?;

        // Determine if file is empty by checking file size
        let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
        let is_end = file_len == 0;

        Ok(WalFsReader { file, current_sequence: 0, is_end, pending_event: None })
    }

    /// Returns the current sequence number being read.
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }

    /// Resets the end-of-file flag, allowing the reader to attempt reading
    /// new events that may have been appended after the previous EOF.
    pub fn reset_eof(&mut self) {
        self.is_end = false;
    }

    /// Strictly validates the full WAL from file start using streaming record-by-record
    /// reads, then restores the prior cursor. Does not buffer the entire file in memory.
    fn validate_full_file_strict(&mut self) -> Result<(), WalReaderError> {
        let original_pos =
            self.file.stream_position().map_err(|e| WalReaderError::IoError(e.to_string()))?;
        let original_is_end = self.is_end;

        self.file.seek(SeekFrom::Start(0)).map_err(|e| WalReaderError::IoError(e.to_string()))?;
        self.is_end = false;

        let result = loop {
            match self.read_next_event_bytes() {
                Ok(Some(_)) => continue,
                Ok(None) => break Ok(()),
                Err(e @ WalReaderError::Corruption(_)) => break Err(e),
                Err(e) => break Err(e),
            }
        };

        // Restore original position regardless of validation outcome
        self.file
            .seek(SeekFrom::Start(original_pos))
            .map_err(|e| WalReaderError::IoError(e.to_string()))?;
        self.is_end = original_is_end;

        if let Err(e) = &result {
            if matches!(e, WalReaderError::Corruption(_)) {
                self.is_end = true;
            }
        }
        result
    }

    /// Attempts to read the next event using streaming two-phase read.
    ///
    /// Phase 1: Read exactly `HEADER_LEN` (12) bytes for version + length + CRC.
    /// Phase 2: Read exactly `payload_len` bytes for the payload.
    ///
    /// This avoids buffering the entire WAL tail on every read.
    ///
    /// Returns `Ok(Some((event, bytes_read)))` if a complete event was read,
    /// `Ok(None)` if end of file was reached cleanly,
    /// or `Err(WalReaderError::Corruption)` if strict corruption is detected.
    fn read_next_event_bytes(&mut self) -> Result<Option<(WalEvent, usize)>, WalReaderError> {
        let record_start =
            self.file.stream_position().map_err(|e| WalReaderError::IoError(e.to_string()))?;

        // Phase 1: Read the 12-byte header (version + length + CRC-32)
        let mut header_buf = [0u8; HEADER_LEN];
        let header_result = read_exact_or_eof(&mut self.file, &mut header_buf);
        match header_result {
            ReadExactResult::Ok => {}
            ReadExactResult::Eof => {
                self.is_end = true;
                return Ok(None);
            }
            ReadExactResult::Partial => {
                self.is_end = true;
                return Err(WalReaderError::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::IncompleteHeader,
                }));
            }
            ReadExactResult::IoError(e) => return Err(WalReaderError::IoError(e)),
        }

        // Parse header fields
        let version = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let payload_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap()) as usize;

        // Guard against unreasonable allocation from corrupted length field
        const MAX_REASONABLE_PAYLOAD: usize = 256 * 1024 * 1024; // 256 MiB
        if payload_len > MAX_REASONABLE_PAYLOAD {
            self.is_end = true;
            return Err(WalReaderError::Corruption(WalCorruption {
                offset: record_start,
                reason: WalCorruptionReasonCode::DecodeFailure,
            }));
        }

        // Validate version before reading payload
        if version != VERSION {
            self.is_end = true;
            return Err(WalReaderError::Corruption(WalCorruption {
                offset: record_start,
                reason: WalCorruptionReasonCode::UnsupportedVersion,
            }));
        }

        // Phase 2: Read exactly payload_len bytes
        let mut payload_buf = vec![0u8; payload_len];
        match self.file.read_exact(&mut payload_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.is_end = true;
                return Err(WalReaderError::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::IncompletePayload,
                }));
            }
            Err(e) => return Err(WalReaderError::IoError(e.to_string())),
        }

        // Assemble full record and decode
        let total_len = HEADER_LEN + payload_len;
        let mut record = Vec::with_capacity(total_len);
        record.extend_from_slice(&header_buf);
        record.extend_from_slice(&payload_buf);

        match decode(&record) {
            Ok(event) => Ok(Some((event, total_len))),
            Err(crate::wal::codec::DecodeError::CrcMismatch { .. }) => {
                self.is_end = true;
                Err(WalReaderError::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::CrcMismatch,
                }))
            }
            Err(_) => {
                self.is_end = true;
                Err(WalReaderError::Corruption(WalCorruption {
                    offset: record_start,
                    reason: WalCorruptionReasonCode::DecodeFailure,
                }))
            }
        }
    }
}

impl WalReader for WalFsReader {
    /// Read the next event from the WAL.
    ///
    /// Attempts to read the next complete event from the file. If strict tail
    /// validation detects corruption at or after the current cursor, returns
    /// `WalReaderError::Corruption`.
    ///
    /// If the end of the file is reached with no corruption (all events
    /// read), returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns `WalReaderError::Corruption` if strict corruption is encountered.
    ///
    /// Returns `WalReaderError::IoError` if an I/O error occurs.
    fn read_next(&mut self) -> Result<Option<WalEvent>, WalReaderError> {
        // Check if we have a pending event from seek_to_sequence
        if let Some(pending) = self.pending_event.take() {
            self.current_sequence = pending.sequence();
            return Ok(Some(pending));
        }

        if self.is_end {
            return Ok(None);
        }

        match self.read_next_event_bytes() {
            Ok(Some((event, _bytes_read))) => {
                self.current_sequence = event.sequence();
                Ok(Some(event))
            }
            Ok(None) => {
                self.is_end = true;
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Seek to a specific sequence number.
    ///
    /// This implementation performs a linear scan from the beginning of the
    /// file to find the event with the specified sequence number. This is
    /// O(n) but guarantees correctness.
    ///
    /// If the sequence number is not found in the file, returns
    /// `WalReaderError::EndOfWal`.
    ///
    /// # Errors
    ///
    /// Returns `WalReaderError::Corruption` if any corruption is detected
    /// while scanning. Per strict policy this fails even when target sequence
    /// appears before corruption.
    fn seek_to_sequence(&mut self, sequence: u64) -> Result<(), WalReaderError> {
        // Strict seek policy: fail on any corruption in the WAL, even when the
        // target sequence appears before the corrupt tail.
        self.validate_full_file_strict()?;

        // Reset to beginning
        self.file.seek(SeekFrom::Start(0)).map_err(|e| WalReaderError::IoError(e.to_string()))?;

        self.is_end = false;

        // Scan for the target sequence
        while !self.is_end {
            match self.read_next_event_bytes() {
                Ok(Some((event, _))) => {
                    if event.sequence() == sequence {
                        // Found the target! Store it as pending so it's returned on next read
                        self.pending_event = Some(event);
                        self.current_sequence = sequence;
                        return Ok(());
                    }
                    self.current_sequence = event.sequence();
                }
                Ok(None) => {
                    // End of file without finding the sequence
                    self.is_end = true;
                    return Err(WalReaderError::EndOfWal);
                }
                Err(e) => return Err(e),
            }
        }

        Err(WalReaderError::EndOfWal)
    }

    /// Returns true if the reader is at the end of the WAL.
    ///
    /// Returns true if all complete events have been read or if corruption was
    /// encountered.
    fn is_end(&self) -> bool {
        self.is_end
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    // Atomic counter to ensure unique test file paths
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_wal_path() -> PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = dir.join(format!("actionqueue_wal_reader_test_{count}.tmp"));
        // Clean up if exists from previous test runs
        let _ = fs::remove_file(&path);
        path
    }

    #[test]
    fn test_new_returns_error_when_file_is_missing() {
        let path = std::env::temp_dir().join(format!(
            "actionqueue_wal_reader_missing_file_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_file(&path);

        let result = WalFsReader::new(path);
        assert!(matches!(result, Err(WalFsReaderError::IoError(_))));
    }

    fn create_test_event(seq: u64) -> WalEvent {
        WalEvent::new(
            seq,
            crate::wal::event::WalEventType::TaskCreated {
                task_spec: actionqueue_core::task::task_spec::TaskSpec::new(
                    actionqueue_core::ids::TaskId::new(),
                    actionqueue_core::task::task_spec::TaskPayload::with_content_type(
                        vec![1, 2, 3],
                        "application/octet-stream",
                    ),
                    actionqueue_core::task::run_policy::RunPolicy::Once,
                    actionqueue_core::task::constraints::TaskConstraints::default(),
                    actionqueue_core::task::metadata::TaskMetadata::default(),
                )
                .expect("test task spec should be valid"),
                timestamp: 0,
            },
        )
    }

    #[test]
    fn test_new_reader_on_empty_file() {
        let path = temp_wal_path();
        // Create empty file
        fs::write(&path, []).unwrap();

        let mut reader = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_new_reader_on_empty_file");

        // Should return None immediately on empty file
        let result = reader.read_next();
        assert!(matches!(result, Ok(None)));
        assert!(reader.is_end());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_read_next_returns_events_in_order() {
        let path = temp_wal_path();

        // Write some events first
        let mut writer = File::create(&path).unwrap();
        let event1 = create_test_event(1);
        let event2 = create_test_event(2);
        let event3 = create_test_event(3);

        writer
            .write_all(&crate::wal::codec::encode(&event1).expect("encode should succeed"))
            .unwrap();
        writer
            .write_all(&crate::wal::codec::encode(&event2).expect("encode should succeed"))
            .unwrap();
        writer
            .write_all(&crate::wal::codec::encode(&event3).expect("encode should succeed"))
            .unwrap();
        writer.flush().unwrap();

        let mut reader = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_read_next_returns_events_in_order");

        // Read all events
        let e1 = reader.read_next().expect("First read should succeed");
        assert!(e1.is_some());
        assert_eq!(e1.as_ref().unwrap().sequence(), 1);

        let e2 = reader.read_next().expect("Second read should succeed");
        assert!(e2.is_some());
        assert_eq!(e2.as_ref().unwrap().sequence(), 2);

        let e3 = reader.read_next().expect("Third read should succeed");
        assert!(e3.is_some());
        assert_eq!(e3.as_ref().unwrap().sequence(), 3);

        // Should return None when at end
        let e4 = reader.read_next().expect("Fourth read should return None");
        assert!(e4.is_none());
        assert!(reader.is_end());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_partial_record_detected_at_end() {
        let path = temp_wal_path();

        // Create a file with a complete event followed by a partial record
        let event1 = create_test_event(1);
        let event1_bytes = crate::wal::codec::encode(&event1).expect("encode should succeed");

        // Write complete event
        let mut writer = File::create(&path).unwrap();
        writer.write_all(&event1_bytes).unwrap();

        // Write partial record: version (4B) + length (4B) + CRC (4B) + partial payload (2B)
        // This should trigger a typed strict corruption error.
        writer.write_all(&crate::wal::codec::VERSION.to_le_bytes()).unwrap();
        writer.write_all(&4u32.to_le_bytes()).unwrap(); // length = 4
        writer.write_all(&0u32.to_le_bytes()).unwrap(); // CRC (dummy)
        writer.write_all(&[1u8, 2u8]).unwrap(); // only 2 bytes of 4 expected
        writer.flush().unwrap();

        let mut reader = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_partial_record_detected_at_end");

        // First event should read successfully
        let e1 = reader.read_next().expect("First read should succeed");
        assert!(e1.is_some());
        assert_eq!(e1.as_ref().unwrap().sequence(), 1);

        // Second read should return strict corruption details
        let e2 = reader.read_next();
        assert!(matches!(e2, Err(WalReaderError::Corruption(_))));
        assert!(reader.is_end());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_seek_to_sequence() {
        let path = temp_wal_path();

        // Write some events
        let mut writer = File::create(&path).unwrap();
        let event1 = create_test_event(1);
        let event2 = create_test_event(2);
        let event3 = create_test_event(3);

        writer
            .write_all(&crate::wal::codec::encode(&event1).expect("encode should succeed"))
            .unwrap();
        writer
            .write_all(&crate::wal::codec::encode(&event2).expect("encode should succeed"))
            .unwrap();
        writer
            .write_all(&crate::wal::codec::encode(&event3).expect("encode should succeed"))
            .unwrap();
        writer.flush().unwrap();

        let mut reader = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_seek_to_sequence");

        // Seek to sequence 2
        reader.seek_to_sequence(2).expect("Seek should succeed");

        // Next read should start from sequence 2
        let next = reader.read_next().expect("Read after seek should succeed");
        assert!(next.is_some());
        assert_eq!(next.as_ref().unwrap().sequence(), 2);

        // Seek to non-existent sequence should return EndOfWal
        let mut reader2 = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_seek_to_sequence (second reader)");
        let result = reader2.seek_to_sequence(999);
        assert!(matches!(result, Err(WalReaderError::EndOfWal)));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_current_sequence() {
        let path = temp_wal_path();

        // Write some events
        let mut writer = File::create(&path).unwrap();
        let event1 = create_test_event(42);
        let event2 = create_test_event(43);

        writer
            .write_all(&crate::wal::codec::encode(&event1).expect("encode should succeed"))
            .unwrap();
        writer
            .write_all(&crate::wal::codec::encode(&event2).expect("encode should succeed"))
            .unwrap();
        writer.flush().unwrap();

        let mut reader = WalFsReader::new(path.clone())
            .expect("Failed to open WAL file for test_current_sequence");

        // After reading first event
        reader.read_next().expect("First read should succeed");
        assert_eq!(reader.current_sequence(), 42);

        // After reading second event
        reader.read_next().expect("Second read should succeed");
        assert_eq!(reader.current_sequence(), 43);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_reset_eof_allows_reading_appended_events() {
        let path = temp_wal_path();

        // Write 2 events
        {
            let mut writer = File::create(&path).unwrap();
            let event1 = create_test_event(1);
            let event2 = create_test_event(2);
            writer
                .write_all(&crate::wal::codec::encode(&event1).expect("encode should succeed"))
                .unwrap();
            writer
                .write_all(&crate::wal::codec::encode(&event2).expect("encode should succeed"))
                .unwrap();
            writer.flush().unwrap();
        }

        let mut reader =
            WalFsReader::new(path.clone()).expect("Failed to open WAL file for test_reset_eof");

        // Read to EOF
        assert!(reader.read_next().expect("read 1").is_some());
        assert!(reader.read_next().expect("read 2").is_some());
        assert!(reader.read_next().expect("read eof").is_none());
        assert!(reader.is_end());

        // Append a 3rd event externally
        {
            let mut writer = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            let event3 = create_test_event(3);
            writer
                .write_all(&crate::wal::codec::encode(&event3).expect("encode should succeed"))
                .unwrap();
            writer.flush().unwrap();
        }

        // Without reset, read_next returns None
        assert!(reader.read_next().expect("still eof").is_none());

        // After reset_eof, can read new event
        reader.reset_eof();
        let event = reader.read_next().expect("read after reset").expect("should have event 3");
        assert_eq!(event.sequence(), 3);

        let _ = fs::remove_file(path);
    }
}
