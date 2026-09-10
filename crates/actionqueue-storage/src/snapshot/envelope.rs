//! Target snapshot framing, identity binding and reserved continuation sections.
use super::{loader::SnapshotLoaderError, model::Snapshot};
use crate::recovery::projection::{snapshot_digest, ProjectionDigest};
use serde::{Deserialize, Serialize};
pub const MAGIC: &[u8; 8] = b"AQCONT1S";
pub const MAX_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReservedSections {
    admissions: Vec<Vec<u8>>,
    signals: Vec<Vec<u8>>,
    waits: Vec<Vec<u8>>,
    checkpoints: Vec<Vec<u8>>,
    resume_assignments: Vec<Vec<u8>>,
    causal_control: Vec<Vec<u8>>,
}
impl ReservedSections {
    fn is_empty(&self) -> bool {
        self.admissions.is_empty()
            && self.signals.is_empty()
            && self.waits.is_empty()
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
        snapshot_schema: 1,
        projection_version: 1,
        wal_sequence: snapshot.metadata.wal_sequence,
        digest: snapshot_digest(snapshot).map_err(|e| e.to_string())?,
        reserved: ReservedSections::default(),
        projection: serde_json::to_value(snapshot).map_err(|e| e.to_string())?,
    };
    let payload = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
    if payload.len() > MAX_SNAPSHOT_BYTES {
        return Err("snapshot exceeds maximum size".into());
    }
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
    if envelope.snapshot_schema != 1 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "snapshot_schema",
            expected: 1,
            found: envelope.snapshot_schema,
        });
    }
    if envelope.projection_version != 1 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "projection_version",
            expected: 1,
            found: envelope.projection_version,
        });
    }
    if !envelope.reserved.is_empty() {
        return Err(invalid("unsupported projection section".into()));
    }
    let snapshot: Snapshot =
        serde_json::from_value(envelope.projection).map_err(|e| invalid(e.to_string()))?;
    if snapshot.version != 1 {
        return Err(SnapshotLoaderError::IncompatibleVersion {
            component: "projection_image",
            expected: 1,
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
