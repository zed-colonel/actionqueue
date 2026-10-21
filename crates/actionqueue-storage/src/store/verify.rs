//! Explicit, non-mutating full-history verification. Damaged included snapshots fail.
use super::{open_store, OpenOptions, StoreError, StoreSession};
use crate::{
    recovery::{
        bootstrap::{recover, validate_replay_event},
        projection::{snapshot_digest, ProjectionDigest, ProjectionImageV8},
        reducer::ReplayReducer,
    },
    snapshot::loader::load_checkpoint,
    wal::{fs_reader::WalFsReader, reader::WalReader, repair::RepairPolicy},
};
use std::path::Path;
#[derive(Debug, serde::Serialize)]
pub struct StoreVerification {
    pub sequence: u64,
    pub snapshot_sequence: u64,
    pub projection_digest: ProjectionDigest,
    pub store_id: uuid::Uuid,
}
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
/// Never initializes, repairs, or reconciles execution.
pub fn verify_store(root: &Path) -> Result<StoreVerification, StoreError> {
    let s = open_store(root, OpenOptions::ReadOnly)?;
    verify_session(&s, false).map(|(_, v)| v)
}
pub(crate) fn verify_session(
    session: &StoreSession,
    omit_damaged: bool,
) -> Result<(crate::recovery::bootstrap::RecoveredProjection, StoreVerification), StoreError> {
    let c = match load_checkpoint(session) {
        Ok(c) => c,
        Err(e) if omit_damaged && e.physical_damage() => None,
        Err(e) => return Err(invalid(e)),
    };
    let full = recover(session, RepairPolicy::Strict, false)?;
    let digest = full.projection.projection_digest()?;
    let snapshot_sequence = c.as_ref().map_or(0, |c| c.binding.cursor.sequence);
    if let Some(c) = c {
        let expected_digest = snapshot_digest(&c.snapshot)?;
        let mut hydrated = ReplayReducer::from_projection_image(ProjectionImageV8(c.snapshot))?;
        hydrated.restore_inspection(c.inspection.clone())?;
        let mut reader = WalFsReader::for_session(session).map_err(invalid)?;
        let mut prefix = ReplayReducer::new();
        while prefix.latest_sequence() < snapshot_sequence {
            let e = reader
                .read_next()
                .map_err(invalid)?
                .ok_or_else(|| invalid("checkpoint exceeds WAL"))?;
            validate_replay_event(session, &mut prefix, &e)?;
        }
        if reader.cursor() != c.binding.cursor
            || prefix.projection_digest()? != expected_digest
            || prefix.inspection_index.export() != c.inspection
            || !prefix.same_operational_order(&hydrated)
        {
            return Err(invalid("checkpoint differs from complete WAL prefix"));
        }
        while let Some(e) = reader.read_next().map_err(invalid)? {
            validate_replay_event(session, &mut hydrated, &e)?;
        }
        if hydrated.projection_digest()? != digest
            || hydrated.inspection_index.export() != full.projection.inspection_index.export()
            || !hydrated.same_operational_order(&full.projection)
        {
            return Err(invalid("checkpoint plus tail differs from complete WAL replay"));
        }
    }
    let result = StoreVerification {
        sequence: full.projection.latest_sequence(),
        snapshot_sequence,
        projection_digest: digest,
        store_id: session.manifest().store_id,
    };
    Ok((full, result))
}

pub(crate) fn verify_checkpoint_prefix(session: &StoreSession) -> Result<(), StoreError> {
    let Some(c) = load_checkpoint(session).map_err(invalid)? else {
        return Ok(());
    };
    let restored = ReplayReducer::from_projection_image(ProjectionImageV8(c.snapshot.clone()))?;
    let mut reader = WalFsReader::for_session(session).map_err(invalid)?;
    let mut prefix = ReplayReducer::new();
    while prefix.latest_sequence() < c.binding.cursor.sequence {
        let e = reader
            .read_next()
            .map_err(invalid)?
            .ok_or_else(|| invalid("checkpoint exceeds WAL"))?;
        validate_replay_event(session, &mut prefix, &e)?;
    }
    if reader.cursor() != c.binding.cursor
        || prefix.projection_digest()? != snapshot_digest(&c.snapshot)?
        || prefix.inspection_index.export() != c.inspection
        || !prefix.same_operational_order(&restored)
    {
        return Err(invalid("checkpoint differs from complete WAL prefix"));
    }
    Ok(())
}
