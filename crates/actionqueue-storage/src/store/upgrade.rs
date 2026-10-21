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
    snapshot::{checkpoint::publish_checkpoint, envelope, loader::read_frame},
    wal::{
        event::{WalEvent, WalEventType},
        fs_reader::WalFsReader,
        reader::WalReader,
        repair::RepairPolicy,
    },
};
use std::{
    fs::{self, File},
    io::{Seek, SeekFrom, Write},
    path::Path,
};
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
pub fn upgrade_store(source: &Path, output: &Path) -> Result<StoreVerification, StoreError> {
    check_destination(source, output)?;
    let source_session = open_legacy(source)?;
    let recovered = recover(&source_session, RepairPolicy::Strict, false)?;
    // Every present source snapshot must pass full prefix equivalence. Operation
    // associations missing from schema 9 are reconstructed from the complete WAL.
    if let Some((_, payload)) = read_frame(&source_session.snapshot_path(), 1).map_err(invalid)? {
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
    let mut from = File::open(source_session.wal_path())?;
    from.seek(SeekFrom::Start(first_end))?;
    let mut to = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(staging.0.join("wal/actionqueue.wal"))?;
    to.write_all(&bytes)?;
    std::io::copy(&mut from, &mut to)?;
    to.sync_all()?;
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
    publish_checkpoint(candidate)?;
    drop(authority);
    let result = verify_store(&staging.0)?;
    if result.projection_digest != digest
        || result.sequence != recovered.projection.latest_sequence()
    {
        return Err(invalid("upgraded destination verification failed"));
    }
    sync_store(&staging.0)?;
    staging.publish(output)?;
    Ok(result)
}
