//! Target snapshot framing, identity binding and reserved continuation sections.
use serde::{Deserialize, Serialize};

use super::{loader::SnapshotLoaderError, model::Snapshot};
use crate::recovery::projection::{snapshot_digest, ProjectionDigest};
pub const MAGIC: &[u8; 8] = b"AQCONT1S";
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
#[cfg(test)]
thread_local! {
    /// Unit tests lower the encode-side frame bound to build oversized images cheaply.
    pub(crate) static TEST_ENCODE_LIMIT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(MAX_SNAPSHOT_BYTES) };
}
fn encode_limit() -> usize {
    #[cfg(test)]
    return TEST_ENCODE_LIMIT.with(std::cell::Cell::get);
    #[cfg(not(test))]
    MAX_SNAPSHOT_BYTES
}
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReservedSections {
    waits: Vec<Vec<u8>>,
    checkpoints: Vec<Vec<u8>>,
    resume_assignments: Vec<Vec<u8>>,
    causal_control: Vec<Vec<u8>>,
}
impl ReservedSections {
    fn is_empty(&self) -> bool {
        self.waits.is_empty()
            && self.checkpoints.is_empty()
            && self.resume_assignments.is_empty()
            && self.causal_control.is_empty()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Envelope {
    store_id: uuid::Uuid,
    snapshot_schema: u32,
    projection_version: u32,
    wal_sequence: u64,
    digest: ProjectionDigest,
    reserved: ReservedSections,
    projection: serde_json::Value,
}
pub(crate) fn encode(snapshot: &Snapshot, store_id: uuid::Uuid) -> Result<Vec<u8>, String> {
    let envelope = Envelope {
        store_id,
        snapshot_schema: 9,
        projection_version: 9,
        wal_sequence: snapshot.metadata.wal_sequence,
        digest: snapshot_digest(snapshot).map_err(|e| e.to_string())?,
        reserved: ReservedSections::default(),
        projection: serde_json::to_value(snapshot).map_err(|e| e.to_string())?,
    };
    let payload = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
    if payload.len() > MAX_SNAPSHOT_BYTES {
        return Err("snapshot exceeds maximum size".into());
    }
    // Publication must pass the same decoding and domain validation as loading.
    // In-memory mapping alone cannot detect stricter Deserialize implementations.
    decode(&payload, Some(store_id)).map_err(|e| e.to_string())?;
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}
pub(crate) fn decode(
    payload: &[u8],
    identity: Option<uuid::Uuid>,
) -> Result<Snapshot, SnapshotLoaderError> {
    let invalid = |s: String| SnapshotLoaderError::DecodeError(s);
    let envelope: Envelope = serde_json::from_slice(payload).map_err(|e| invalid(e.to_string()))?;
    if identity.is_some_and(|id| id != envelope.store_id) {
        return Err(invalid("store identity mismatch".into()));
    }
    if envelope.snapshot_schema != 9 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "snapshot_schema",
            expected: 9,
            found: envelope.snapshot_schema,
        });
    }
    if envelope.projection_version != 9 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "projection_version",
            expected: 9,
            found: envelope.projection_version,
        });
    }
    if !envelope.reserved.is_empty() {
        return Err(invalid("unsupported projection section".into()));
    }
    let snapshot: Snapshot =
        serde_json::from_value(envelope.projection).map_err(|e| invalid(e.to_string()))?;
    if snapshot.version != 9 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "projection_image",
            expected: 9,
            found: snapshot.version,
        });
    }
    if snapshot.metadata.wal_sequence != envelope.wal_sequence {
        return Err(invalid("snapshot envelope mismatch".into()));
    }
    super::mapping::validate_snapshot(&snapshot).map_err(SnapshotLoaderError::MappingError)?;
    if snapshot_digest(&snapshot).map_err(|e| invalid(e.to_string()))? != envelope.digest {
        return Err(invalid("projection digest mismatch".into()));
    }
    Ok(snapshot)
}

/// Frame 2 contains a compact schema-10 checkpoint and an independent SHA-256.
pub(crate) fn encode_checkpoint(c: &super::wire_v10::Checkpoint) -> Result<Vec<u8>, String> {
    encode_checkpoint_measured(c).map(|(b, _)| b)
}
pub(crate) fn encode_checkpoint_measured(
    c: &super::wire_v10::Checkpoint,
) -> Result<super::wire_v10::EncodedCheckpoint, String> {
    use sha2::{Digest, Sha256};
    let (mut payload, mut sections) = super::wire_v10::encode_measured(c)?;
    if payload.len().checked_add(32).is_none_or(|n| n > encode_limit()) {
        return Err("snapshot exceeds maximum size".into());
    }
    let mut hash = Sha256::new();
    hash.update(b"AQ-CONT-1\0checkpoint\0v10\0");
    hash.update(&payload);
    payload.extend_from_slice(&hash.finalize());
    let mut bytes = MAGIC.to_vec();
    bytes.extend(2u32.to_le_bytes());
    bytes.extend((payload.len() as u32).to_le_bytes());
    bytes.extend(crc32fast::hash(&payload).to_le_bytes());
    bytes.extend(payload);
    if bytes.len() > encode_limit() {
        return Err("complete checkpoint exceeds maximum size".into());
    }
    sections.push(("framing_integrity".into(), 52));
    Ok((bytes, sections))
}
/// Publication must pass the same decoding and domain validation as loading. Decodes
/// a complete frame from [`encode_checkpoint`] as startup reads it, then hydrates its
/// image and restores its inspection index.
pub(crate) fn verify_encoded_checkpoint(
    bytes: &[u8],
    session: &crate::store::StoreSession,
) -> Result<(), String> {
    let c = decode_checkpoint(&bytes[20..], Some(session)).map_err(|e| e.to_string())?;
    let mut hydrated = crate::recovery::reducer::ReplayReducer::from_projection_image(
        crate::recovery::projection::ProjectionImageV8(c.snapshot),
    )
    .map_err(|e| e.to_string())?;
    hydrated.restore_inspection(c.inspection).map_err(|e| e.to_string())
}
pub(crate) fn decode_checkpoint(
    payload: &[u8],
    session: Option<&crate::store::StoreSession>,
) -> Result<super::wire_v10::Checkpoint, SnapshotLoaderError> {
    use sha2::{Digest, Sha256};
    let started = std::time::Instant::now();
    let n = payload.len().checked_sub(32).ok_or(SnapshotLoaderError::PhysicalDamage)?;
    let mut hash = Sha256::new();
    hash.update(b"AQ-CONT-1\0checkpoint\0v10\0");
    hash.update(&payload[..n]);
    if hash.finalize().as_slice() != &payload[n..] {
        return Err(SnapshotLoaderError::PhysicalDamage);
    }
    crate::recovery::work::snapshot_phase(|p| {
        p.integrity_seconds += started.elapsed().as_secs_f64()
    });
    let started = std::time::Instant::now();
    let checkpoint =
        super::wire_v10::decode(&payload[..n]).map_err(SnapshotLoaderError::DecodeError)?;
    crate::recovery::work::snapshot_phase(|p| p.decode_seconds += started.elapsed().as_secs_f64());
    let started = std::time::Instant::now();
    if checkpoint.binding.cursor.sequence == 0
        || checkpoint.binding.cursor.last_start >= checkpoint.binding.cursor.end
    {
        return Err(SnapshotLoaderError::DecodeError("invalid checkpoint cursor".into()));
    }
    if let Some(s) = session {
        if checkpoint.binding.store != *s.manifest().store_id.as_bytes()
            || checkpoint.binding.manifest != s.manifest().digest()
        {
            return Err(SnapshotLoaderError::DecodeError(
                "checkpoint store/manifest identity mismatch".into(),
            ));
        }
        super::mapping::validate_profile(&checkpoint.snapshot, &s.manifest().features)
            .map_err(|e| SnapshotLoaderError::DecodeError(e.to_string()))?;
    }
    crate::recovery::work::snapshot_phase(|p| {
        p.profile_validation_seconds += started.elapsed().as_secs_f64()
    });
    Ok(checkpoint)
}
