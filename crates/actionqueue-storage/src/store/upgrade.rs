//! Explicit, source-preserving schema-9 -> schema-10 upgrade. No downgrade.
use super::{
    backup::{check_destination, make_dirs, sync_store},
    open_store,
    session::*,
    verify::{verify_store, StoreVerification},
    OpenOptions, StoreError,
};
use crate::{
    recovery::bootstrap::{recover, validate_snapshot_for_session},
    snapshot::{
        checkpoint::{encode_candidate, install_checkpoint},
        envelope,
        loader::read_frame,
    },
    wal::{
        event::{WalEvent, WalEventType},
        fs_reader::WalFsReader,
        reader::WalReader,
        repair::RepairPolicy,
    },
};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
/// The verified destination of [`upgrade_store`] and what it left out of the source.
#[derive(Debug, serde::Serialize)]
pub struct StoreUpgrade {
    #[serde(flatten)]
    pub verification: StoreVerification,
    /// Length of an incomplete final frame left out of the destination WAL, or 0.
    /// A crash during an append leaves such a frame; it was never acknowledged, and
    /// repair truncates it the same way. The source keeps it.
    pub dropped_tail_bytes: u64,
    /// Why the destination has no checkpoint, when its state cannot be published as
    /// one (for example over the checkpoint size or collection bounds). The
    /// destination is complete and verified; its startup replays the whole WAL.
    pub checkpoint_omitted: Option<String>,
}
pub fn upgrade_store(source: &Path, output: &Path) -> Result<StoreUpgrade, StoreError> {
    check_destination(source, output)?;
    let source_session = open_legacy(source)?;
    // The source is never modified, so an incomplete final frame is left out of the
    // destination rather than repaired in place.
    let recovered = recover(&source_session, RepairPolicy::TruncatePartial, false)?;
    // Every present source snapshot must pass full prefix equivalence. Operation
    // associations missing from schema 9 are reconstructed from the complete WAL.
    let snapshot_path = source_session.snapshot_path();
    let frame = read_frame(&snapshot_path, 1).map_err(|e| {
        if e.physical_damage() {
            invalid(format!(
                "damaged schema-9 snapshot at {}: {e}; preserve a copy of the source store, \
                 remove snapshots/snapshot.bin from the offline upgrade source, and retry \
                 actionqueue store upgrade to reconstruct from the complete WAL",
                snapshot_path.display()
            ))
        } else {
            invalid(e)
        }
    })?;
    if let Some((_, payload)) = frame {
        let snapshot = envelope::decode(&payload, Some(source_session.manifest().store_id))
            .map_err(invalid)?;
        validate_snapshot_for_session(&source_session, &snapshot)?;
    }
    let digest = recovered.projection.projection_digest()?;
    let inspection = recovered.projection.inspection_index.export();
    let staging = Staging::new(output)?;
    make_dirs(&staging.0)?;
    let mut manifest = source_session.manifest().clone();
    manifest.snapshot_schema = 10;
    manifest.validate()?;
    write_synced(
        &staging.0.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest).map_err(invalid)?,
    )?;
    write_synced(&staging.0.join("store.lock"), &[])?;
    let mut reader = WalFsReader::for_session(&source_session).map_err(invalid)?;
    reader.read_next().map_err(invalid)?.ok_or_else(|| invalid("missing source initialization"))?;
    let first_end = reader.cursor().end;
    let first =
        WalEvent::new(1, WalEventType::StoreInitialized { manifest_digest: manifest.digest() });
    let bytes = crate::wal::codec::encode_for_store(&first, manifest.store_id).map_err(invalid)?;
    let complete_end = recovered.cursor.end;
    let mut from = File::open(source_session.wal_path())?;
    let source_len = from.metadata()?.len();
    if source_len != recovered.observed_wal_len
        || recovered.incomplete_tail.as_ref().map_or(complete_end, |t| t.offset) != complete_end
    {
        return Err(invalid("source WAL changed during upgrade"));
    }
    from.seek(SeekFrom::Start(first_end))?;
    let mut to = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(staging.0.join("wal/actionqueue.wal"))?;
    to.write_all(&bytes)?;
    std::io::copy(&mut (&mut from).take(complete_end - first_end), &mut to)?;
    to.sync_all()?;
    let dropped_tail_bytes = source_len - complete_end;
    if dropped_tail_bytes > 0 {
        tracing::warn!(
            offset = complete_end,
            bytes = dropped_tail_bytes,
            "store upgrade left the source WAL's incomplete final frame out of the destination"
        );
    }
    sync_store(&staging.0)?;
    let mut authority = open_store(&staging.0, OpenOptions::ReadWrite)?.into_authority()?;
    if authority.projection().projection_digest()? != digest
        || authority.projection().inspection_index.export() != inspection
        || !authority.projection().same_operational_order(&recovered.projection)
    {
        return Err(invalid("upgrade projection or inspection parity failed"));
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(invalid)?
        .as_secs();
    let candidate = authority.capture_checkpoint(timestamp)?;
    // A schema-10 store without a checkpoint is complete, so a state with no
    // publishable checkpoint still upgrades. Disk failures still fail the upgrade.
    let checkpoint_omitted = match encode_candidate(&candidate) {
        Ok(encoded) => {
            install_checkpoint(candidate, encoded)?;
            None
        }
        Err(reason) => {
            // The candidate shares the destination session; release it before verifying.
            drop(candidate);
            tracing::warn!(%reason, "store upgrade published no checkpoint; startup replays the complete WAL");
            Some(reason)
        }
    };
    drop(authority);
    let verification = verify_store(&staging.0)?;
    if verification.projection_digest != digest
        || verification.sequence != recovered.projection.latest_sequence()
        || (verification.snapshot_sequence == 0) != checkpoint_omitted.is_some()
    {
        return Err(invalid("upgraded destination verification failed"));
    }
    sync_store(&staging.0)?;
    staging.publish(output)?;
    Ok(StoreUpgrade { verification, dropped_tail_bytes, checkpoint_omitted })
}
