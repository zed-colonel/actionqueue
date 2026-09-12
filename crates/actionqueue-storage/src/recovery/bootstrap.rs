//! Target-only recovery. Validate complete history and snapshot equivalence before writing.
use std::{path::PathBuf, time::Instant};

use super::{projection::ProjectionImageV8, reducer::ReplayReducer};
use crate::{
    snapshot::{
        loader::{SnapshotFsLoader, SnapshotLoader},
        model::Snapshot,
    },
    store::{open_store, OpenOptions, StoreError, StoreSession},
    wal::{
        event::WalEventType,
        fs_reader::WalFsReader,
        fs_writer::WalFsWriter,
        reader::{WalReader, WalReaderError},
        repair::RepairPolicy,
        tail_validation::WalCorruption,
        InstrumentedWalWriter, WalAppendTelemetry,
    },
};
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecoveryObservations {
    pub recovery_duration_seconds: f64,
    pub events_applied_total: u64,
    pub snapshot_events_applied: u64,
    pub wal_replay_events_applied: u64,
}
impl RecoveryObservations {
    pub const fn zero() -> Self {
        Self {
            recovery_duration_seconds: 0.0,
            events_applied_total: 0,
            snapshot_events_applied: 0,
            wal_replay_events_applied: 0,
        }
    }
}
pub struct RecoveryBootstrap {
    pub projection: ReplayReducer,
    pub wal_writer: InstrumentedWalWriter<WalFsWriter>,
    pub wal_append_telemetry: WalAppendTelemetry,
    pub wal_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub snapshot_loaded: bool,
    pub snapshot_sequence: u64,
    pub recovery_observations: RecoveryObservations,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryBootstrapError {
    WalInit(String),
    WalRead(String),
    SnapshotLoad(String),
    WalReplay(String),
    SnapshotBootstrap(String),
}
impl std::fmt::Display for RecoveryBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "recovery: {self:?}")
    }
}
impl std::error::Error for RecoveryBootstrapError {}
#[derive(Debug)]
pub struct RecoveredProjection {
    pub projection: ReplayReducer,
    pub snapshot_loaded: bool,
    pub snapshot_sequence: u64,
    pub incomplete_tail: Option<WalCorruption>,
}
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
/// Offline recovery never repairs, initializes, executes, or reconciles runtime state.
pub fn recover_read_only(
    session: &StoreSession,
    policy: RepairPolicy,
) -> Result<RecoveredProjection, StoreError> {
    recover(session, policy, true)
}
pub(crate) fn recover(
    session: &StoreSession,
    policy: RepairPolicy,
    use_snapshot: bool,
) -> Result<RecoveredProjection, StoreError> {
    let snapshot = if use_snapshot {
        match SnapshotFsLoader::for_session(session).load() {
            Ok(snapshot) => snapshot,
            Err(e) if e.physical_damage() => None,
            Err(e) => return Err(invalid(e)),
        }
    } else {
        None
    };
    let mut hydrated = snapshot
        .as_ref()
        .map(|s| bootstrap_reducer_from_snapshot(s).map_err(invalid))
        .transpose()?
        .map(|(r, _)| r);
    let snapshot_sequence = snapshot.as_ref().map_or(0, |s| s.metadata.wal_sequence);
    let mut reader = WalFsReader::for_session(session).map_err(invalid)?;
    let mut full = ReplayReducer::new();
    let mut tail = None;
    let mut matched_snapshot = snapshot.is_none();
    loop {
        let event = match reader.read_next() {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(WalReaderError::Corruption(c))
                if policy == RepairPolicy::TruncatePartial
                    && c.repairable()
                    && full.latest_sequence() >= 1 =>
            {
                tail = Some(c);
                break;
            }
            Err(e) => return Err(invalid(e)),
        };
        if full.latest_sequence() == 0 {
            if !matches!(event.event(), WalEventType::StoreInitialized { manifest_digest } if *manifest_digest == session.manifest().digest())
            {
                return Err(invalid("first record must bind StoreInitialized to manifest"));
            }
        } else if matches!(event.event(), WalEventType::StoreInitialized { .. }) {
            return Err(invalid("repeated StoreInitialized"));
        }
        crate::store::check_event_profile(event.event(), &session.manifest().features)?;
        full.validate_target_event(event.event()).map_err(invalid)?;
        full.apply(&event).map_err(invalid)?;
        if let Some(h) = &mut hydrated {
            if event.sequence() == snapshot_sequence {
                if full.projection_digest()? != h.projection_digest()? {
                    return Err(invalid("snapshot projection differs from WAL prefix"));
                }
                matched_snapshot = true;
            } else if event.sequence() > snapshot_sequence {
                h.apply(&event).map_err(invalid)?;
            }
        }
    }
    if full.latest_sequence() == 0 {
        return Err(invalid(
            "complete WAL with StoreInitialized required; snapshot-only stores are unsupported",
        ));
    }
    if !matched_snapshot {
        return Err(invalid("snapshot sequence is outside complete WAL history"));
    }
    // Enforce the same projection invariants as append, inspection and snapshot
    // publication, including WAL-only recovery. Repair may only use this result
    // after the complete candidate prefix has passed validation.
    full.projection_image()?;
    if let Some(h) = &hydrated {
        if h.projection_digest()? != full.projection_digest()? {
            return Err(invalid("snapshot plus tail differs from WAL replay"));
        }
    }
    Ok(RecoveredProjection {
        projection: hydrated.unwrap_or(full),
        snapshot_loaded: snapshot.is_some(),
        snapshot_sequence,
        incomplete_tail: tail,
    })
}
pub fn load_projection_from_storage(
    data_root: &std::path::Path,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    load_projection_with_features(data_root, crate::store::capabilities())
}
pub fn load_projection_with_features(
    data_root: &std::path::Path,
    features: Vec<String>,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    let started = Instant::now();
    let session = open_store(data_root, OpenOptions::Initialize { features })
        .map_err(|e| RecoveryBootstrapError::WalInit(e.to_string()))?;
    let recovered = recover_read_only(&session, RepairPolicy::Strict)
        .map_err(|e| RecoveryBootstrapError::WalReplay(e.to_string()))?;
    let wal_path = session.wal_path();
    let snapshot_path = session.snapshot_path();
    let writer =
        WalFsWriter::new(session).map_err(|e| RecoveryBootstrapError::WalInit(e.to_string()))?;
    let wal_append_telemetry = WalAppendTelemetry::new();
    let latest = recovered.projection.latest_sequence();
    Ok(RecoveryBootstrap {
        projection: recovered.projection,
        wal_writer: InstrumentedWalWriter::new(writer, wal_append_telemetry.clone()),
        wal_append_telemetry,
        wal_path,
        snapshot_path,
        snapshot_loaded: recovered.snapshot_loaded,
        snapshot_sequence: recovered.snapshot_sequence,
        recovery_observations: RecoveryObservations {
            recovery_duration_seconds: started.elapsed().as_secs_f64(),
            events_applied_total: latest,
            snapshot_events_applied: 0,
            wal_replay_events_applied: latest,
        },
    })
}
fn bootstrap_reducer_from_snapshot(
    snapshot: &Snapshot,
) -> Result<(ReplayReducer, u64), StoreError> {
    ReplayReducer::from_projection_image(ProjectionImageV8(snapshot.clone())).map(|r| (r, 0))
}

/// A snapshot can publish only an exact image of an existing WAL prefix.
pub(crate) fn validate_snapshot_for_session(
    session: &StoreSession,
    snapshot: &Snapshot,
) -> Result<(), StoreError> {
    let restored = ReplayReducer::from_projection_image(ProjectionImageV8(snapshot.clone()))?;
    let mut reader = WalFsReader::for_session(session).map_err(invalid)?;
    let mut prefix = ReplayReducer::new();
    while prefix.latest_sequence() < snapshot.metadata.wal_sequence {
        let event =
            reader.read_next().map_err(invalid)?.ok_or_else(|| invalid("snapshot exceeds WAL"))?;
        crate::store::check_event_profile(event.event(), &session.manifest().features)?;
        prefix.validate_target_event(event.event()).map_err(invalid)?;
        prefix.apply(&event).map_err(invalid)?;
    }
    if prefix.latest_sequence() == 0
        || prefix.projection_digest()? != restored.projection_digest()?
    {
        return Err(invalid("snapshot differs from WAL prefix"));
    }
    Ok(())
}
