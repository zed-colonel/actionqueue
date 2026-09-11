//! One parser and corruption classification for readers, recovery, and repair.
use std::io::{Read, Seek};

use super::{
    codec::{self, DecodeError, HEADER_LEN},
    event::WalEvent,
    reader::WalReaderError,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalCorruptionReasonCode {
    IncompleteHeader,
    IncompletePayload,
    UnsupportedVersion { supported: u32, found: u32 },
    DecodeFailure,
    CrcMismatch,
    InvalidMagic,
    HeaderIntegrity,
    UnsupportedRecordKind,
    UnsupportedRecordSchema { kind: u16, supported: u16, found: u16 },
    StoreIdentityMismatch,
    SequenceViolation,
    OversizedPayload,
}
impl std::fmt::Display for WalCorruptionReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion { supported, found } => {
                write!(f, "wal_format: supported {supported}, found {found}")
            }
            Self::UnsupportedRecordSchema { kind, supported, found } => {
                write!(f, "wal_record_schema (kind {kind}): supported {supported}, found {found}")
            }
            _ => write!(f, "{self:?}"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCorruption {
    pub offset: u64,
    pub reason: WalCorruptionReasonCode,
}
impl WalCorruption {
    pub fn new(offset: usize, reason: WalCorruptionReasonCode) -> Self {
        Self { offset: offset as u64, reason }
    }
    pub fn repairable(&self) -> bool {
        matches!(
            self.reason,
            WalCorruptionReasonCode::IncompleteHeader | WalCorruptionReasonCode::IncompletePayload
        )
    }
}
impl std::fmt::Display for WalCorruption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL corruption at offset {} ({})", self.offset, self.reason)
    }
}
impl std::error::Error for WalCorruption {}
fn reason(e: DecodeError) -> WalCorruptionReasonCode {
    use WalCorruptionReasonCode as R;
    match e {
        DecodeError::UnsupportedVersion { supported, found } => {
            R::UnsupportedVersion { supported, found }
        }
        DecodeError::CrcMismatch { .. } => R::CrcMismatch,
        DecodeError::InvalidMagic => R::InvalidMagic,
        DecodeError::HeaderIntegrity => R::HeaderIntegrity,
        DecodeError::UnsupportedRecordKind(_) => R::UnsupportedRecordKind,
        DecodeError::UnsupportedRecordSchema { kind, found } => {
            R::UnsupportedRecordSchema { kind, supported: 1, found }
        }
        DecodeError::StoreIdentityMismatch => R::StoreIdentityMismatch,
        DecodeError::SequenceViolation => R::SequenceViolation,
        DecodeError::InvalidLength(_) => R::OversizedPayload,
        DecodeError::Decode(_) => R::DecodeFailure,
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedWalRecord {
    pub event: WalEvent,
    pub record_len: usize,
}
/// Reads at most one bounded frame. Identity and sequence are checked before allocation.
pub(crate) fn read_record<R: Read + Seek>(
    reader: &mut R,
    store_id: Option<uuid::Uuid>,
    expected: Option<u64>,
) -> Result<Option<ParsedWalRecord>, WalReaderError> {
    let offset = reader.stream_position().map_err(|e| WalReaderError::IoError(e.to_string()))?;
    let fail = |r| WalReaderError::Corruption(WalCorruption { offset, reason: r });
    let mut bytes = [0u8; HEADER_LEN];
    let mut count = 0;
    while count < HEADER_LEN {
        match reader.read(&mut bytes[count..]) {
            Ok(0) if count == 0 => return Ok(None),
            Ok(0) => {
                let n = count.min(8);
                if bytes[..n] != codec::MAGIC[..n] {
                    return Err(fail(WalCorruptionReasonCode::InvalidMagic));
                }
                // Reject every incompatibility identifiable even in a partial header.
                if count >= 12 && bytes[8..12] != codec::VERSION.to_le_bytes() {
                    return Err(fail(WalCorruptionReasonCode::UnsupportedVersion {
                        supported: codec::VERSION,
                        found: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                    }));
                }
                if count >= 14 {
                    super::wire_v1::check_kind(u16::from_le_bytes(
                        bytes[12..14].try_into().unwrap(),
                    ))
                    .map_err(|e| fail(reason(e)))?;
                }
                if count >= 16 && bytes[14..16] != 1u16.to_le_bytes() {
                    return Err(fail(WalCorruptionReasonCode::UnsupportedRecordSchema {
                        kind: u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
                        supported: 1,
                        found: u16::from_le_bytes(bytes[14..16].try_into().unwrap()),
                    }));
                }
                if count >= 32 && store_id.is_some_and(|id| bytes[16..32] != *id.as_bytes()) {
                    return Err(fail(WalCorruptionReasonCode::StoreIdentityMismatch));
                }
                if count >= 40 && expected.is_some_and(|seq| bytes[32..40] != seq.to_le_bytes()) {
                    return Err(fail(WalCorruptionReasonCode::SequenceViolation));
                }
                return Err(fail(WalCorruptionReasonCode::IncompleteHeader));
            }
            Ok(n) => count += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(WalReaderError::IoError(e.to_string())),
        }
    }
    let h = codec::header(&bytes).map_err(|e| fail(reason(e)))?;
    if store_id.is_some_and(|id| id != h.store_id) {
        return Err(fail(WalCorruptionReasonCode::StoreIdentityMismatch));
    }
    if expected.is_some_and(|seq| seq != h.sequence) {
        return Err(fail(WalCorruptionReasonCode::SequenceViolation));
    }
    let mut frame = Vec::from(bytes);
    frame.resize(HEADER_LEN + h.length, 0);
    reader.read_exact(&mut frame[HEADER_LEN..]).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            fail(WalCorruptionReasonCode::IncompletePayload)
        } else {
            WalReaderError::IoError(e.to_string())
        }
    })?;
    let event = codec::decode(&frame).map_err(|e| fail(reason(e)))?;
    Ok(Some(ParsedWalRecord { event, record_len: frame.len() }))
}
pub fn parse_record_at(
    buffer: &[u8],
    offset: usize,
) -> Result<Option<ParsedWalRecord>, WalCorruption> {
    let mut cursor = std::io::Cursor::new(buffer);
    cursor.set_position(offset as u64);
    read_record(&mut cursor, None, None).map_err(|e| match e {
        WalReaderError::Corruption(c) => c,
        _ => WalCorruption::new(offset, WalCorruptionReasonCode::DecodeFailure),
    })
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalValidationSummary {
    pub last_valid_sequence: u64,
    pub end_offset: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LenientValidationResult {
    pub last_valid_sequence: u64,
    pub last_valid_offset: u64,
    pub trailing_corruption: Option<WalCorruption>,
}
pub fn validate_tail_lenient(buffer: &[u8]) -> LenientValidationResult {
    let mut offset = 0;
    let mut seq = 0;
    loop {
        match parse_record_at(buffer, offset) {
            Ok(Some(r)) => {
                seq = r.event.sequence();
                offset += r.record_len;
            }
            Ok(None) => {
                return LenientValidationResult {
                    last_valid_sequence: seq,
                    last_valid_offset: offset as u64,
                    trailing_corruption: None,
                }
            }
            Err(c) => {
                return LenientValidationResult {
                    last_valid_sequence: seq,
                    last_valid_offset: offset as u64,
                    trailing_corruption: Some(c),
                }
            }
        }
    }
}
pub fn validate_tail_strict(buffer: &[u8]) -> Result<WalValidationSummary, WalCorruption> {
    let r = validate_tail_lenient(buffer);
    if let Some(c) = r.trailing_corruption {
        return Err(c);
    }
    Ok(WalValidationSummary {
        last_valid_sequence: r.last_valid_sequence,
        end_offset: r.last_valid_offset,
    })
}
