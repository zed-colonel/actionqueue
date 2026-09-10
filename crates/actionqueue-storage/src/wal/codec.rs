//! AQ-CONT-1 WAL v1: 52-byte header followed by a bounded postcard wire payload.
//! Header: magic[8], format u32, kind u16, schema u16, store UUID[16],
//! sequence u64, length u32, payload CRC32 u32, header CRC32 u32. Integers are LE.
use super::{event::WalEvent, wire_v1};
pub const MAGIC: &[u8; 8] = b"AQCONT1W";
pub const VERSION: u32 = 1;
pub const HEADER_LEN: usize = 52;
pub const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    Serialization(String),
    PayloadTooLarge(usize),
}
impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL encode: {self:?}")
    }
}
impl std::error::Error for EncodeError {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    UnsupportedVersion { supported: u32, found: u32 },
    InvalidLength(String),
    CrcMismatch { expected: u32, actual: u32 },
    Decode(String),
    InvalidMagic,
    HeaderIntegrity,
    UnsupportedRecordKind(u16),
    UnsupportedRecordSchema { kind: u16, found: u16 },
    StoreIdentityMismatch,
    SequenceViolation,
}
impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion { supported, found } => {
                write!(f, "wal_format: supported {supported}, found {found}")
            }
            Self::UnsupportedRecordSchema { kind, found } => {
                write!(f, "wal_record_schema (kind {kind}): supported 1, found {found}")
            }
            _ => write!(f, "WAL decode: {self:?}"),
        }
    }
}
impl std::error::Error for DecodeError {}
#[derive(Debug, Clone, Copy)]
pub(crate) struct Header {
    pub store_id: uuid::Uuid,
    pub sequence: u64,
    pub kind: u16,
    pub length: usize,
    pub crc: u32,
}
pub(crate) fn header(bytes: &[u8]) -> Result<Header, DecodeError> {
    if bytes.len() < HEADER_LEN {
        return Err(DecodeError::InvalidLength("short header".into()));
    }
    if &bytes[..8] != MAGIC {
        return Err(DecodeError::InvalidMagic);
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(DecodeError::UnsupportedVersion { supported: VERSION, found: version });
    }
    if crc32fast::hash(&bytes[..48]) != u32::from_le_bytes(bytes[48..52].try_into().unwrap()) {
        return Err(DecodeError::HeaderIntegrity);
    }
    let kind = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    wire_v1::check_kind(kind)?;
    let schema = u16::from_le_bytes(bytes[14..16].try_into().unwrap());
    if schema != 1 {
        return Err(DecodeError::UnsupportedRecordSchema { kind, found: schema });
    }
    let length = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    if length > MAX_PAYLOAD_SIZE {
        return Err(DecodeError::InvalidLength(format!(
            "payload {length} exceeds {MAX_PAYLOAD_SIZE}"
        )));
    }
    Ok(Header {
        store_id: uuid::Uuid::from_bytes(bytes[16..32].try_into().unwrap()),
        sequence: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        kind,
        length,
        crc: u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
    })
}
/// Encodes a standalone frame. Store writers always supply their manifest identity.
pub fn encode(event: &WalEvent) -> Result<Vec<u8>, EncodeError> {
    encode_for_store(event, uuid::Uuid::nil())
}
pub fn encode_for_store(event: &WalEvent, store_id: uuid::Uuid) -> Result<Vec<u8>, EncodeError> {
    let payload = wire_v1::encode_payload(event.event())?;
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(EncodeError::PayloadTooLarge(payload.len()));
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&wire_v1::kind(event.event()).to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(store_id.as_bytes());
    bytes.extend_from_slice(&event.sequence().to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}
pub fn decode(bytes: &[u8]) -> Result<WalEvent, DecodeError> {
    let h = header(bytes)?;
    if bytes.len() != HEADER_LEN + h.length {
        return Err(DecodeError::InvalidLength("frame length mismatch".into()));
    }
    let payload = &bytes[HEADER_LEN..];
    let actual = crc32fast::hash(payload);
    if h.crc != actual {
        return Err(DecodeError::CrcMismatch { expected: h.crc, actual });
    }
    Ok(WalEvent::new(h.sequence, wire_v1::decode_payload(h.kind, payload)?))
}
