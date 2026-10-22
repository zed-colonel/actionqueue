//! Proven checkpoint capture is cheap; encoding and atomic publication can run off-lane.
use super::writer::{
    SnapshotFsWriter, SnapshotFsWriterInitError, SnapshotWriter, SnapshotWriterError,
};
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
/// Encodes, proves and atomically installs a captured checkpoint.
///
/// The encoded bytes must decode, hydrate and restore their inspection index exactly
/// as startup will before they replace the installed checkpoint, so a checkpoint that
/// startup would reject fails its publication instead. Every error leaves the
/// installed checkpoint and the WAL unchanged, apart from the WAL sync before rename.
pub fn publish_checkpoint(
    candidate: CheckpointCandidate,
) -> Result<PublishedCheckpoint, StoreError> {
    let encoded = encode_candidate(&candidate).map_err(StoreError::InvalidStore)?;
    install_checkpoint(candidate, encoded)
}
/// A candidate's complete checkpoint frame, already proven loadable.
pub(crate) struct EncodedCandidate {
    bytes: Vec<u8>,
    sections: Vec<(String, u64)>,
    preparation_seconds: f64,
    started: std::time::Instant,
}
/// Builds, encodes and proves a candidate's checkpoint without touching the store.
/// An error means this state has no publishable checkpoint, such as one over the
/// size or collection bounds; the store remains complete without one.
pub(crate) fn encode_candidate(
    candidate: &CheckpointCandidate,
) -> Result<EncodedCandidate, String> {
    let started = std::time::Instant::now();
    let snapshot =
        super::build::build_snapshot_from_projection(&candidate.projection, candidate.timestamp)
            .map_err(|e| e.to_string())?;
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
    let (bytes, sections) = super::envelope::encode_checkpoint_measured(&c)?;
    drop(c);
    super::envelope::verify_encoded_checkpoint(&bytes, &candidate.session)
        .map_err(|e| format!("checkpoint failed its pre-publication load: {e}"))?;
    Ok(EncodedCandidate { bytes, sections, preparation_seconds, started })
}
pub(crate) fn install_checkpoint(
    candidate: CheckpointCandidate,
    encoded: EncodedCandidate,
) -> Result<PublishedCheckpoint, StoreError> {
    let invalid = |e: &dyn std::fmt::Display| StoreError::InvalidStore(e.to_string());
    // Disk failures and a lost publication race are not store corruption.
    let writer_error = |e: SnapshotWriterError| match e {
        SnapshotWriterError::IoError(e) => StoreError::Io(format!("checkpoint publication: {e}")),
        SnapshotWriterError::Stale { sequence, installed } => {
            StoreError::StaleCheckpoint { sequence, installed }
        }
        e => invalid(&e),
    };
    let mut writer = SnapshotFsWriter::new(&candidate.session).map_err(
        |SnapshotFsWriterInitError::IoError(e)| {
            StoreError::Io(format!("opening snapshot file: {e}"))
        },
    )?;
    writer.write_encoded(&encoded.bytes, candidate.cursor.sequence).map_err(writer_error)?;
    let bytes = encoded.sections.iter().map(|(_, n)| n).sum();
    writer.close().map_err(writer_error)?;
    Ok(PublishedCheckpoint {
        sequence: candidate.cursor.sequence,
        sections: encoded.sections,
        bytes,
        preparation_seconds: encoded.preparation_seconds,
        encoding_publication_seconds: encoded.started.elapsed().as_secs_f64(),
    })
}
