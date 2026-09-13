//! AQ-CONT-1 WAL v1: 52-byte header followed by a bounded postcard wire payload.
//! Header: `magic[8]`, format u32, kind u16, schema u16, store `UUID[16]`,
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
                write!(f, "wal_record_schema (kind {kind}): unsupported schema {found}")
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
    pub schema: u16,
    pub length: usize,
    pub crc: u32,
}
pub(crate) fn check_record_schema(kind: u16, schema: u16) -> Result<(), DecodeError> {
    if kind != 352 {
        wire_v1::check_kind(kind)?;
    }
    if schema != 1
        && schema != wire_v1::schema(kind)
        && !(schema == 2 && matches!(kind, 16 | 256 | 352))
    {
        return Err(DecodeError::UnsupportedRecordSchema { kind, found: schema });
    }
    Ok(())
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
    let schema = u16::from_le_bytes(bytes[14..16].try_into().unwrap());
    check_record_schema(kind, schema)?;
    let length = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    if matches!(kind, 288..=291)
        && length + HEADER_LEN > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES
    {
        return Err(DecodeError::InvalidLength("signal frame exceeds hard ceiling".into()));
    }
    if matches!(kind,304..=307 | 320..=321)
        && length + HEADER_LEN > actionqueue_core::limits::MAX_WAIT_RECORD_BYTES
    {
        return Err(DecodeError::InvalidLength("continuation frame exceeds hard ceiling".into()));
    }
    if length > MAX_PAYLOAD_SIZE {
        return Err(DecodeError::InvalidLength(format!(
            "payload {length} exceeds {MAX_PAYLOAD_SIZE}"
        )));
    }
    Ok(Header {
        store_id: uuid::Uuid::from_bytes(bytes[16..32].try_into().unwrap()),
        sequence: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        kind,
        schema,
        length,
        crc: u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
    })
}
/// Encodes a standalone frame. Store writers always supply their manifest identity.
pub fn encode(event: &WalEvent) -> Result<Vec<u8>, EncodeError> {
    encode_for_store(event, uuid::Uuid::nil())
}
pub fn encode_for_store(event: &WalEvent, store_id: uuid::Uuid) -> Result<Vec<u8>, EncodeError> {
    let payload = if let Some(control) = event.control() {
        let attribution =
            serde_json::to_vec(control).map_err(|e| EncodeError::Serialization(e.to_string()))?;
        let inner =
            encode_for_store(&WalEvent::new(event.sequence(), event.event().clone()), store_id)?;
        let mut payload = (attribution.len() as u32).to_le_bytes().to_vec();
        payload.extend(attribution);
        payload.extend(inner);
        payload
    } else {
        wire_v1::encode_payload(event.event())?
    };

    if matches!(wire_v1::kind(event.event()), 288..=291)
        && payload.len() + HEADER_LEN > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES
    {
        return Err(EncodeError::PayloadTooLarge(payload.len()));
    }
    if matches!(wire_v1::kind(event.event()),304..=307 | 320..=321)
        && payload.len() + HEADER_LEN > actionqueue_core::limits::MAX_WAIT_RECORD_BYTES
    {
        return Err(EncodeError::PayloadTooLarge(payload.len()));
    }
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(EncodeError::PayloadTooLarge(payload.len()));
    }
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    let kind = if event.control().is_some() { 352 } else { wire_v1::kind(event.event()) };
    bytes.extend_from_slice(&kind.to_le_bytes());
    let schema = if kind == 352 {
        2
    } else if matches!(
        event.event(),
        super::event::WalEventType::AttemptStarted { .. }
            | super::event::WalEventType::AttemptFinished { .. }
    ) {
        1
    } else {
        wire_v1::schema(wire_v1::kind(event.event()))
    };
    bytes.extend_from_slice(&schema.to_le_bytes());
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
    if h.kind == 352 {
        let event: WalEvent = if h.schema == 1 {
            serde_json::from_slice(payload).map_err(|e| DecodeError::Decode(e.to_string()))?
        } else {
            let prefix = payload
                .get(..4)
                .ok_or_else(|| DecodeError::Decode("truncated control envelope".into()))?;
            let length = u32::from_le_bytes(prefix.try_into().unwrap()) as usize;
            let split = 4usize
                .checked_add(length)
                .filter(|n| *n <= payload.len())
                .ok_or_else(|| DecodeError::Decode("invalid control length".into()))?;
            let control = serde_json::from_slice(&payload[4..split])
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            let inner_header = header(&payload[split..])?;
            // Reject recursive envelopes before decoding; depth is always one.
            if inner_header.kind == 352
                || inner_header.store_id != h.store_id
                || inner_header.sequence != h.sequence
            {
                return Err(DecodeError::Decode("invalid inner control identity".into()));
            }
            decode(&payload[split..])?.with_control(control)
        };
        let kind = wire_v1::kind(event.event());
        if (matches!(kind, 288..=291)
            && bytes.len() > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES)
            || (matches!(kind,304..=307 | 320..=321)
                && bytes.len() > actionqueue_core::limits::MAX_WAIT_RECORD_BYTES)
        {
            return Err(DecodeError::InvalidLength("attributed frame exceeds hard ceiling".into()));
        }
        if event.sequence() != h.sequence || event.control().is_none() {
            return Err(DecodeError::Decode("invalid attributed frame".into()));
        }
        return Ok(event);
    }
    Ok(WalEvent::new(h.sequence, wire_v1::decode_schema(h.kind, h.schema, payload)?))
}
