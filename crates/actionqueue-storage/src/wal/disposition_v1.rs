//! Version-one disposition payload. A bounded JSON typed record is independent of
//! enum discriminants in postcard and retains the domain's validated decoding.
use super::codec::{DecodeError, EncodeError};
use crate::mutation::disposition::DispositionRecord;
pub fn encode(record: &DispositionRecord) -> Result<Vec<u8>, EncodeError> {
    let bytes =
        serde_json::to_vec(record).map_err(|e| EncodeError::Serialization(e.to_string()))?;
    if bytes.len() + super::codec::HEADER_LEN > actionqueue_core::limits::MAX_ADMISSION_RECORD_BYTES
    {
        return Err(EncodeError::PayloadTooLarge(bytes.len()));
    }
    Ok(bytes)
}
pub fn decode(bytes: &[u8]) -> Result<DispositionRecord, DecodeError> {
    if bytes.len() + super::codec::HEADER_LEN > actionqueue_core::limits::MAX_ADMISSION_RECORD_BYTES
    {
        return Err(DecodeError::InvalidLength("disposition exceeds format ceiling".into()));
    }
    serde_json::from_slice(bytes).map_err(|e| DecodeError::Decode(e.to_string()))
}
