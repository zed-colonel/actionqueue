//! Proven checkpoint capture is cheap; encoding and atomic publication can run off-lane.
use super::writer::{SnapshotFsWriter, SnapshotWriter};
use crate::{
    recovery::reducer::ReplayReducer,
    store::{StoreError, StoreSession},
    wal::checkpoint::WalCursor,
};
/// Private roots, cursor, and session provenance cannot be supplied independently.
pub struct CheckpointCandidate {
    session: StoreSession,
    projection: ReplayReducer,
    cursor: WalCursor,
    timestamp: u64,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct PublishedCheckpoint {
    pub sequence: u64,
    pub bytes: u64,
    pub preparation_seconds: f64,
    pub encoding_publication_seconds: f64,
    pub sections: Vec<(String, u64)>,
}
impl CheckpointCandidate {
    pub(crate) fn new(
        session: StoreSession,
        projection: ReplayReducer,
        cursor: WalCursor,
        timestamp: u64,
    ) -> Self {
        Self { session, projection, cursor, timestamp }
    }
    pub fn sequence(&self) -> u64 {
        self.cursor.sequence
    }
}
pub fn capture_checkpoint<W: crate::wal::writer::WalWriter>(
    authority: &mut crate::mutation::StorageMutationAuthority<W, ReplayReducer>,
    timestamp: u64,
) -> Result<CheckpointCandidate, StoreError> {
    authority.capture_checkpoint(timestamp)
}
pub fn publish_checkpoint(
    candidate: CheckpointCandidate,
) -> Result<PublishedCheckpoint, StoreError> {
    let invalid = |e: &dyn std::fmt::Display| StoreError::InvalidStore(e.to_string());
    let started = std::time::Instant::now();
    let snapshot =
        super::build::build_snapshot_from_projection(&candidate.projection, candidate.timestamp)
            .map_err(|e| invalid(&e))?;
    let inspection = candidate.projection.inspection_index.export();
    let preparation_seconds = started.elapsed().as_secs_f64();
    let c = super::wire_v10::Checkpoint {
        binding: super::wire_v10::Binding {
            store: *candidate.session.manifest().store_id.as_bytes(),
            manifest: candidate.session.manifest().digest(),
            commitment_version: 1,
            cursor: candidate.cursor,
        },
        snapshot,
        inspection,
    };
    let started = std::time::Instant::now();
    let mut writer = SnapshotFsWriter::new(&candidate.session).map_err(|e| invalid(&e))?;
    let sections = writer.write_checkpoint(&c).map_err(|e| invalid(&e))?;
    let bytes = sections.iter().map(|(_, n)| n).sum();
    writer.close().map_err(|e| invalid(&e))?;
    Ok(PublishedCheckpoint {
        sequence: candidate.cursor.sequence,
        sections,
        bytes,
        preparation_seconds,
        encoding_publication_seconds: started.elapsed().as_secs_f64(),
    })
}
