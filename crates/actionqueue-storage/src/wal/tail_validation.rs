//! Shared strict WAL framed-record tail validation.
//!
//! This module defines the single source of truth for classifying trailing WAL
//! corruption across writer bootstrap, reader iteration, and replay paths.

use crate::wal::codec::{decode, DecodeError, HEADER_LEN, VERSION};
use crate::wal::event::WalEvent;

const WAL_HEADER_LEN: usize = HEADER_LEN;

/// Stable, machine-readable corruption reason codes for WAL tail validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCorruptionReasonCode {
    /// The WAL ended before a complete record header could be read.
    IncompleteHeader,
    /// The WAL ended before all bytes declared by the length frame were present.
    IncompletePayload,
    /// The framed record declares an unsupported WAL version.
    UnsupportedVersion,
    /// A complete framed record failed to decode as a valid [`WalEvent`].
    DecodeFailure,
    /// The CRC-32 checksum in the header does not match the payload.
    CrcMismatch,
}

impl std::fmt::Display for WalCorruptionReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalCorruptionReasonCode::IncompleteHeader => write!(f, "incomplete_header"),
            WalCorruptionReasonCode::IncompletePayload => write!(f, "incomplete_payload"),
            WalCorruptionReasonCode::UnsupportedVersion => write!(f, "unsupported_version"),
            WalCorruptionReasonCode::DecodeFailure => write!(f, "decode_failure"),
            WalCorruptionReasonCode::CrcMismatch => write!(f, "crc_mismatch"),
        }
    }
}

/// Typed WAL corruption details used by all restart paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCorruption {
    /// Byte offset of the failing record start boundary.
    pub offset: u64,
    /// Stable machine-readable reason code.
    pub reason: WalCorruptionReasonCode,
}

impl WalCorruption {
    /// Creates typed corruption details at a record-start boundary.
    pub fn new(offset: usize, reason: WalCorruptionReasonCode) -> Self {
        Self { offset: offset as u64, reason }
    }
}

impl std::fmt::Display for WalCorruption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL corruption at offset {} ({})", self.offset, self.reason)
    }
}

impl std::error::Error for WalCorruption {}

/// Summary emitted when WAL validation succeeds with no corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct WalValidationSummary {
    /// Highest observed sequence in the WAL (or 0 for empty WAL).
    pub last_valid_sequence: u64,
    /// End offset after the last complete decoded record.
    pub end_offset: u64,
}

/// A decoded framed WAL record returned by the shared parser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedWalRecord {
    /// Decoded WAL event.
    pub event: WalEvent,
    /// Record byte length including framing header.
    pub record_len: usize,
}

/// Parses one framed record at `offset`.
///
/// Returns:
/// - `Ok(None)` when `offset` is exactly at EOF,
/// - `Ok(Some(record))` for a complete decoded record,
/// - `Err(WalCorruption)` for strict corruption classification at record boundary.
pub fn parse_record_at(
    buffer: &[u8],
    offset: usize,
) -> Result<Option<ParsedWalRecord>, WalCorruption> {
    if offset >= buffer.len() {
        return Ok(None);
    }

    let remaining = buffer.len() - offset;
    if remaining < WAL_HEADER_LEN {
        return Err(WalCorruption::new(offset, WalCorruptionReasonCode::IncompleteHeader));
    }

    let version = u32::from_le_bytes(
        buffer[offset..offset + 4]
            .try_into()
            .map_err(|_| WalCorruption::new(offset, WalCorruptionReasonCode::IncompleteHeader))?,
    );
    let payload_len = u32::from_le_bytes(
        buffer[offset + 4..offset + 8]
            .try_into()
            .map_err(|_| WalCorruption::new(offset, WalCorruptionReasonCode::IncompleteHeader))?,
    ) as usize;

    let total_len = WAL_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| WalCorruption::new(offset, WalCorruptionReasonCode::DecodeFailure))?;

    let record_end = offset
        .checked_add(total_len)
        .ok_or_else(|| WalCorruption::new(offset, WalCorruptionReasonCode::DecodeFailure))?;
    if record_end > buffer.len() {
        return Err(WalCorruption::new(offset, WalCorruptionReasonCode::IncompletePayload));
    }

    if version != VERSION {
        return Err(WalCorruption::new(offset, WalCorruptionReasonCode::UnsupportedVersion));
    }

    let event =
        decode(&buffer[offset..offset + total_len]).map_err(|decode_error| match decode_error {
            DecodeError::UnsupportedVersion(_) => {
                WalCorruption::new(offset, WalCorruptionReasonCode::UnsupportedVersion)
            }
            DecodeError::CrcMismatch { .. } => {
                WalCorruption::new(offset, WalCorruptionReasonCode::CrcMismatch)
            }
            DecodeError::InvalidLength(_) | DecodeError::Decode(_) => {
                WalCorruption::new(offset, WalCorruptionReasonCode::DecodeFailure)
            }
        })?;

    Ok(Some(ParsedWalRecord { event, record_len: total_len }))
}

/// Strictly validates an entire WAL byte stream.
///
/// Validation succeeds only when all framed records are complete and decodable.
/// Any trailing corruption fails with typed record-boundary diagnostics.
pub fn validate_tail_strict(buffer: &[u8]) -> Result<WalValidationSummary, WalCorruption> {
    let mut offset = 0usize;
    let mut last_valid_sequence = 0u64;

    while let Some(record) = parse_record_at(buffer, offset)? {
        if record.event.sequence() > last_valid_sequence {
            last_valid_sequence = record.event.sequence();
        }
        offset += record.record_len;
    }

    Ok(WalValidationSummary { last_valid_sequence, end_offset: offset as u64 })
}

/// Result of lenient WAL tail validation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct LenientValidationResult {
    /// Highest observed sequence in the last valid record (0 for empty WAL).
    pub last_valid_sequence: u64,
    /// Byte offset after the last complete, valid record.
    pub last_valid_offset: u64,
    /// If trailing corruption was found, details about it.
    pub trailing_corruption: Option<WalCorruption>,
}

/// Leniently validates a WAL byte stream, reporting trailing corruption
/// without failing.
///
/// Reads all complete, decodable records and reports the offset of the last
/// valid record boundary. If trailing bytes cannot be parsed as a valid record,
/// they are reported as `trailing_corruption` but the function still succeeds.
///
/// Mid-stream corruption (corruption followed by valid records) is NOT handled
/// by this function — that would indicate a more serious integrity issue.
pub fn validate_tail_lenient(buffer: &[u8]) -> LenientValidationResult {
    let mut offset = 0usize;
    let mut last_valid_sequence = 0u64;

    loop {
        match parse_record_at(buffer, offset) {
            Ok(None) => {
                // Clean EOF
                break;
            }
            Ok(Some(record)) => {
                if record.event.sequence() > last_valid_sequence {
                    last_valid_sequence = record.event.sequence();
                }
                offset += record.record_len;
            }
            Err(corruption) => {
                // Trailing corruption found
                return LenientValidationResult {
                    last_valid_sequence,
                    last_valid_offset: offset as u64,
                    trailing_corruption: Some(corruption),
                };
            }
        }
    }

    LenientValidationResult {
        last_valid_sequence,
        last_valid_offset: offset as u64,
        trailing_corruption: None,
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

    use super::*;
    use crate::wal::codec;
    use crate::wal::event::WalEventType;

    fn event(seq: u64) -> WalEvent {
        WalEvent::new(
            seq,
            WalEventType::TaskCreated {
                task_spec: TaskSpec::new(
                    TaskId::new(),
                    TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .expect("valid test task"),
                timestamp: 0,
            },
        )
    }

    #[test]
    fn validate_tail_strict_accepts_clean_wal() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&codec::encode(&event(5)).expect("encode should succeed"));
        bytes.extend_from_slice(&codec::encode(&event(9)).expect("encode should succeed"));

        let summary = validate_tail_strict(&bytes).expect("clean WAL should validate");
        assert_eq!(summary.last_valid_sequence, 9);
        assert_eq!(summary.end_offset, bytes.len() as u64);
    }

    #[test]
    fn parse_record_at_reports_incomplete_header() {
        let bytes = vec![1u8, 2u8, 3u8];
        let error = parse_record_at(&bytes, 0).expect_err("partial header must fail");
        assert_eq!(error.offset, 0);
        assert_eq!(error.reason, WalCorruptionReasonCode::IncompleteHeader);
    }

    #[test]
    fn parse_record_at_reports_incomplete_payload() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&codec::VERSION.to_le_bytes());
        bytes.extend_from_slice(&10u32.to_le_bytes()); // length = 10
        bytes.extend_from_slice(&0u32.to_le_bytes()); // CRC (dummy)
                                                      // Only 2 bytes of payload instead of 10
        bytes.extend_from_slice(&[0u8; 2]);

        let error = parse_record_at(&bytes, 0).expect_err("partial payload must fail");
        assert_eq!(error.offset, 0);
        assert_eq!(error.reason, WalCorruptionReasonCode::IncompletePayload);
    }

    #[test]
    fn parse_record_at_reports_unsupported_version() {
        let mut bytes = codec::encode(&event(1)).expect("encode should succeed");
        bytes[0..4].copy_from_slice(&999u32.to_le_bytes());

        let error = parse_record_at(&bytes, 0).expect_err("unsupported version must fail");
        assert_eq!(error.offset, 0);
        assert_eq!(error.reason, WalCorruptionReasonCode::UnsupportedVersion);
    }

    #[test]
    fn parse_record_at_reports_crc_mismatch() {
        let payload = b"nope";
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&codec::VERSION.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // wrong CRC
        bytes.extend_from_slice(payload);

        let error = parse_record_at(&bytes, 0).expect_err("CRC mismatch must fail");
        assert_eq!(error.offset, 0);
        assert_eq!(error.reason, WalCorruptionReasonCode::CrcMismatch);
    }

    #[test]
    fn parse_record_at_reports_decode_failure() {
        let payload = b"not valid postcard data";
        let crc = crc32fast::hash(payload);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&codec::VERSION.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&crc.to_le_bytes()); // valid CRC
        bytes.extend_from_slice(payload);

        let error = parse_record_at(&bytes, 0).expect_err("invalid payload must fail");
        assert_eq!(error.offset, 0);
        assert_eq!(error.reason, WalCorruptionReasonCode::DecodeFailure);
    }
}
