//! WAL encoding and decoding.
//!
//! This module defines the encode/decode contract for WAL events with deterministic ordering
//! and version framing policy.
//!
//! # Format
//!
//! The WAL encoding uses a framed binary format with CRC-32 integrity checks:
//!
//! ```text
//! +-------------+-------------+------------+------------------------+
//! | Version (4B)| Length (4B) | CRC-32 (4B)| Serialized Event (N B) |
//! +-------------+-------------+------------+------------------------+
//! ```
//!
//! - **Version**: 4-byte LE unsigned integer indicating the format version (currently 5)
//! - **Length**: 4-byte LE unsigned integer indicating the length of the serialized event
//! - **CRC-32**: 4-byte LE CRC-32 checksum of the serialized event payload
//! - **Serialized Event**: The postcard-encoded WalEvent structure
//!
//! This format ensures:
//! - **Deterministic ordering**: The same event always produces the same bytes
//! - **Version framing**: future format changes can be handled by checking the version
//! - **Length prefixing**: allows efficient reading without backtracking
//! - **Integrity**: CRC-32 detects corruption in the payload

use crate::wal::event::WalEvent;

/// The current WAL format version.
pub const VERSION: u32 = 5;

/// Size of the WAL record header in bytes (version + length + crc32).
pub const HEADER_LEN: usize = 12;

/// Maximum payload size that can be encoded in a WAL record.
pub const MAX_PAYLOAD_SIZE: usize = u32::MAX as usize;

/// Errors that can occur during WAL encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// The event could not be serialized.
    Serialization(String),
    /// The serialized payload exceeds the maximum encodable size (u32::MAX).
    PayloadTooLarge(usize),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodeError::Serialization(msg) => write!(f, "WAL encode serialization error: {msg}"),
            EncodeError::PayloadTooLarge(size) => {
                write!(f, "WAL encode payload too large: {size} bytes (max {MAX_PAYLOAD_SIZE})")
            }
        }
    }
}

impl std::error::Error for EncodeError {}

/// Encodes a WAL event into bytes.
///
/// The encoding includes:
/// - A 4-byte version header
/// - A 4-byte length frame
/// - A 4-byte CRC-32 checksum of the payload
/// - The postcard-serialized event payload
///
/// # Determinism
///
/// This function produces deterministic output - the same event will always
/// produce the same bytes. This is critical for replay and recovery.
///
/// # Errors
///
/// Returns [`EncodeError::Serialization`] if the event cannot be serialized.
pub fn encode(event: &WalEvent) -> Result<Vec<u8>, EncodeError> {
    // Serialize the event using postcard for compact binary representation
    let payload =
        postcard::to_allocvec(event).map_err(|e| EncodeError::Serialization(e.to_string()))?;

    // Compute CRC-32 of the payload
    let crc = crc32fast::hash(&payload);

    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());

    // Write version frame (4 bytes, little-endian)
    bytes.extend_from_slice(&VERSION.to_le_bytes());

    // Write length frame (4 bytes, little-endian)
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| EncodeError::PayloadTooLarge(payload.len()))?;
    bytes.extend_from_slice(&payload_len.to_le_bytes());

    // Write CRC-32 (4 bytes, little-endian)
    bytes.extend_from_slice(&crc.to_le_bytes());

    // Write payload
    bytes.extend_from_slice(&payload);

    Ok(bytes)
}

/// Decodes a WAL event from bytes.
///
/// # Format
///
/// See the module documentation for the expected byte format.
///
/// # Errors
///
/// Returns an error if:
/// - The version frame doesn't match the expected version
/// - The length frame exceeds reasonable bounds
/// - The CRC-32 checksum doesn't match
/// - The payload cannot be deserialized to WalEvent
pub fn decode(bytes: &[u8]) -> Result<WalEvent, DecodeError> {
    // Minimum bytes needed: 4 (version) + 4 (length) + 4 (crc) + 1 (minimum payload)
    if bytes.len() < HEADER_LEN + 1 {
        return Err(DecodeError::InvalidLength(format!(
            "Buffer too short: {} bytes (minimum {} required)",
            bytes.len(),
            HEADER_LEN + 1
        )));
    }

    // Read version frame
    let version =
        u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| {
            DecodeError::InvalidLength("version frame must be 4 bytes".to_string())
        })?);

    if version != VERSION {
        return Err(DecodeError::UnsupportedVersion(format!(
            "Unsupported WAL version: {version}. Current version: {VERSION}"
        )));
    }

    // Read length frame
    let length = u32::from_le_bytes(
        bytes[4..8]
            .try_into()
            .map_err(|_| DecodeError::InvalidLength("length frame must be 4 bytes".to_string()))?,
    ) as usize;

    // Validate length against available bytes
    if length > bytes.len() - HEADER_LEN {
        return Err(DecodeError::InvalidLength(format!(
            "Length frame indicates {} bytes but only {} available",
            length,
            bytes.len() - HEADER_LEN
        )));
    }

    // Read expected CRC-32
    let expected_crc = u32::from_le_bytes(
        bytes[8..12]
            .try_into()
            .map_err(|_| DecodeError::InvalidLength("CRC frame must be 4 bytes".to_string()))?,
    );

    // Extract payload
    let payload = &bytes[HEADER_LEN..HEADER_LEN + length];

    // Validate CRC-32
    let actual_crc = crc32fast::hash(payload);
    if actual_crc != expected_crc {
        return Err(DecodeError::CrcMismatch { expected: expected_crc, actual: actual_crc });
    }

    // Deserialize the event
    postcard::from_bytes(payload)
        .map_err(|e| DecodeError::Decode(format!("Failed to deserialize WalEvent: {e}")))
}

/// Errors that can occur during WAL decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The version frame doesn't match the expected version
    UnsupportedVersion(String),
    /// The buffer length is invalid
    InvalidLength(String),
    /// The CRC-32 checksum does not match
    CrcMismatch {
        /// Expected CRC-32 value from header.
        expected: u32,
        /// Actual CRC-32 computed from payload.
        actual: u32,
    },
    /// The payload could not be decoded
    Decode(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::UnsupportedVersion(msg) => write!(f, "unsupported version: {msg}"),
            DecodeError::InvalidLength(msg) => write!(f, "invalid length: {msg}"),
            DecodeError::CrcMismatch { expected, actual } => {
                write!(f, "CRC-32 mismatch: expected {expected:#010x}, actual {actual:#010x}")
            }
            DecodeError::Decode(msg) => write!(f, "decode error: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[cfg(test)]
mod tests {
    use actionqueue_core::budget::BudgetDimension;
    use actionqueue_core::ids::{AttemptId, RunId, TaskId};
    use actionqueue_core::mutation::AttemptResultKind;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::subscription::{EventFilter, SubscriptionId};
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

    use super::*;
    use crate::wal::event::WalEventType;

    #[test]
    fn encode_produces_versioned_output() {
        let event = WalEvent::new(42, create_test_event());

        let encoded = encode(&event).expect("encode should succeed");

        // First 4 bytes should be the current VERSION (little-endian)
        assert_eq!(&encoded[0..4], &VERSION.to_le_bytes());
    }

    #[test]
    fn encode_length_frame_matches_payload() {
        let event = WalEvent::new(100, create_test_event());

        let encoded = encode(&event).expect("encode should succeed");

        // Bytes 4-8 should be the length of the payload
        let length = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
        let expected_length = encoded.len() - HEADER_LEN;

        assert_eq!(length, expected_length);
    }

    #[test]
    fn encode_includes_crc32() {
        let event = WalEvent::new(42, create_test_event());

        let encoded = encode(&event).expect("encode should succeed");

        // Extract CRC from header
        let header_crc = u32::from_le_bytes(encoded[8..12].try_into().unwrap());

        // Compute CRC from payload
        let payload = &encoded[HEADER_LEN..];
        let computed_crc = crc32fast::hash(payload);

        assert_eq!(header_crc, computed_crc);
    }

    #[test]
    fn decode_produces_equivalent_event() {
        let original = WalEvent::new(123, create_test_event());

        let encoded = encode(&original).expect("encode should succeed");
        let decoded = decode(&encoded).expect("Failed to decode");

        assert_eq!(original, decoded);
    }

    #[test]
    fn encode_is_deterministic() {
        let event = WalEvent::new(999, create_test_event());

        let encoded1 = encode(&event).expect("encode should succeed");
        let encoded2 = encode(&event).expect("encode should succeed");

        assert_eq!(encoded1, encoded2);
    }

    #[test]
    fn decode_rejects_invalid_version() {
        let mut bytes = Vec::new();

        // Write invalid version
        bytes.extend_from_slice(&99u32.to_le_bytes());

        // Write length
        let payload = b"test";
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        // Write CRC
        bytes.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());

        // Write payload
        bytes.extend_from_slice(payload);

        let result = decode(&bytes);

        assert!(matches!(result, Err(DecodeError::UnsupportedVersion(_))));
    }

    #[test]
    fn decode_rejects_buffer_too_short() {
        let bytes = vec![0; 5]; // Less than minimum 13 bytes

        let result = decode(&bytes);

        assert!(matches!(result, Err(DecodeError::InvalidLength(_))));
    }

    #[test]
    fn decode_rejects_length_exceeds_buffer() {
        let mut bytes = Vec::new();

        // Write version
        bytes.extend_from_slice(&VERSION.to_le_bytes());

        // Write length that exceeds available data
        bytes.extend_from_slice(&1000u32.to_le_bytes());

        // Write CRC (dummy)
        bytes.extend_from_slice(&0u32.to_le_bytes());

        // Write only 1 byte of payload
        bytes.extend_from_slice(b"x");

        let result = decode(&bytes);

        assert!(matches!(result, Err(DecodeError::InvalidLength(_))));
    }

    #[test]
    fn decode_rejects_crc_mismatch() {
        let event = WalEvent::new(1, create_test_event());
        let mut encoded = encode(&event).expect("encode should succeed");

        // Corrupt a payload byte
        if encoded.len() > HEADER_LEN {
            encoded[HEADER_LEN] ^= 0xFF;
        }

        let result = decode(&encoded);
        assert!(matches!(result, Err(DecodeError::CrcMismatch { .. })));
    }

    #[test]
    fn decode_rejects_invalid_payload() {
        let mut bytes = Vec::new();

        let payload = b"NOT VALID POSTCARD";

        // Write version
        bytes.extend_from_slice(&VERSION.to_le_bytes());

        // Write length
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());

        // Write valid CRC for the payload
        bytes.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());

        // Write invalid payload
        bytes.extend_from_slice(payload);

        let result = decode(&bytes);

        assert!(matches!(result, Err(DecodeError::Decode(_))));
    }

    #[test]
    fn crc32_detects_single_bit_flip() {
        let event = WalEvent::new(42, create_test_event());
        let encoded = encode(&event).expect("encode should succeed");

        // Flip a single bit in each payload byte
        for i in HEADER_LEN..encoded.len() {
            let mut corrupted = encoded.clone();
            corrupted[i] ^= 0x01;
            assert!(decode(&corrupted).is_err(), "single bit flip at byte {i} should be detected");
        }
    }

    #[test]
    fn roundtrip_all_event_types() {
        let task_id = TaskId::new();
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();
        let task_spec = TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid test task");

        let run_instance = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
            run_id, task_id, 100, 100,
        )
        .expect("valid scheduled run");

        let events: Vec<WalEventType> = vec![
            WalEventType::TaskCreated { task_spec, timestamp: 1000 },
            WalEventType::RunCreated { run_instance },
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2000,
            },
            WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 3000 },
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Success,
                error: None,
                output: None,
                timestamp: 4000,
            },
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Failure,
                error: Some("test error".to_string()),
                output: None,
                timestamp: 4001,
            },
            WalEventType::TaskCanceled { task_id, timestamp: 5000 },
            WalEventType::RunCanceled { run_id, timestamp: 5001 },
            WalEventType::LeaseAcquired {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 9000,
                timestamp: 6000,
            },
            WalEventType::LeaseHeartbeat {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 10000,
                timestamp: 7000,
            },
            WalEventType::LeaseExpired {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 10000,
                timestamp: 11000,
            },
            WalEventType::LeaseReleased {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 10000,
                timestamp: 8000,
            },
            WalEventType::EnginePaused { timestamp: 12000 },
            WalEventType::EngineResumed { timestamp: 13000 },
            WalEventType::DependencyDeclared {
                task_id,
                depends_on: vec![TaskId::new(), TaskId::new()],
                timestamp: 14000,
            },
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result: AttemptResultKind::Success,
                error: None,
                output: Some(b"test-output".to_vec()),
                timestamp: 15000,
            },
            WalEventType::RunSuspended {
                run_id,
                reason: Some("budget exhausted".to_string()),
                timestamp: 16000,
            },
            WalEventType::RunResumed { run_id, timestamp: 17000 },
            WalEventType::BudgetAllocated {
                task_id,
                dimension: BudgetDimension::Token,
                limit: 1000,
                timestamp: 18000,
            },
            WalEventType::BudgetConsumed {
                task_id,
                dimension: BudgetDimension::CostCents,
                amount: 250,
                timestamp: 19000,
            },
            WalEventType::BudgetExhausted {
                task_id,
                dimension: BudgetDimension::TimeSecs,
                timestamp: 20000,
            },
            WalEventType::BudgetReplenished {
                task_id,
                dimension: BudgetDimension::Token,
                new_limit: 2000,
                timestamp: 21000,
            },
            WalEventType::SubscriptionCreated {
                subscription_id: SubscriptionId::new(),
                task_id,
                filter: EventFilter::TaskCompleted { task_id },
                timestamp: 22000,
            },
            WalEventType::SubscriptionTriggered {
                subscription_id: SubscriptionId::new(),
                timestamp: 23000,
            },
            WalEventType::SubscriptionCanceled {
                subscription_id: SubscriptionId::new(),
                timestamp: 24000,
            },
        ];

        for (seq, event_type) in events.into_iter().enumerate() {
            let original = WalEvent::new((seq + 1) as u64, event_type);
            let encoded = encode(&original).expect("encode should succeed");
            let decoded = decode(&encoded).expect("decode should succeed");
            assert_eq!(original, decoded, "roundtrip failed for event at sequence {}", seq + 1);
        }
    }

    /// Payload size validation is a compile-time guard: postcard cannot serialize
    /// a payload exceeding u32::MAX bytes in practice, but the checked conversion
    /// prevents a silent truncation cast if it ever could.
    #[test]
    fn payload_too_large_variant_exists_and_displays() {
        let err = EncodeError::PayloadTooLarge(usize::MAX);
        let msg = err.to_string();
        assert!(msg.contains("too large"), "Display should mention 'too large': {msg}");
        assert!(msg.contains(&usize::MAX.to_string()), "Display should include the size: {msg}");
    }

    #[test]
    fn max_payload_size_equals_u32_max() {
        assert_eq!(MAX_PAYLOAD_SIZE, u32::MAX as usize);
    }

    fn create_test_event() -> crate::wal::event::WalEventType {
        crate::wal::event::WalEventType::RunStateChanged {
            run_id: RunId::new(),
            previous_state: RunState::Scheduled,
            new_state: RunState::Running,
            timestamp: 1_000_000,
        }
    }
}
