//! Bounded checkpoint recovery. Explicit verification validates complete retained history.
use std::{path::PathBuf, time::Instant};

use super::{projection::ProjectionImageV8, reducer::ReplayReducer};
use crate::{
    snapshot::model::Snapshot,
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
    pub phases: RecoveryPhases,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryBootstrapError {
    WalInit(String),
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
    pub(crate) cursor: crate::wal::checkpoint::WalCursor,
    pub(crate) session_id: uuid::Uuid,
    pub(crate) origin: uuid::Uuid,
    pub(crate) observed_wal_len: u64,
    pub phases: RecoveryPhases,
}
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct RecoveryPhases {
    pub snapshot_load_seconds: f64,
    pub hydration_seconds: f64,
    pub tail_replay_seconds: f64,
    pub snapshot: super::work::SnapshotWork,
}
impl RecoveredProjection {
    pub(crate) fn matches_session(&self, session: &StoreSession) -> bool {
        self.session_id == session.id()
    }
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
    super::work::reset_snapshot_phases();
    let mut phases = RecoveryPhases::default();
    let start = Instant::now();
    let checkpoint = if use_snapshot {
        match crate::snapshot::loader::load_checkpoint(session) {
            Ok(c) => c,
            Err(e) if e.physical_damage() => None,
            Err(e) => return Err(invalid(e)),
        }
    } else {
        None
    };
    phases.snapshot_load_seconds = start.elapsed().as_secs_f64();
    let snapshot_sequence = checkpoint.as_ref().map_or(0, |c| c.binding.cursor.sequence);
    let snapshot_loaded = checkpoint.is_some();
    let start = Instant::now();
    let (mut projection, mut reader) = if let Some(c) = checkpoint {
        let reader = WalFsReader::from_checkpoint(session, c.binding.cursor).map_err(invalid)?;
        let mut p = ReplayReducer::from_projection_image(ProjectionImageV8(c.snapshot))?;
        let index_started = Instant::now();
        p.restore_inspection(c.inspection)?;
        super::work::snapshot_phase(|w| {
            w.inspection_index_seconds += index_started.elapsed().as_secs_f64()
        });
        (p, reader)
    } else {
        (ReplayReducer::new(), WalFsReader::for_session(session).map_err(invalid)?)
    };
    phases.hydration_seconds = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let mut tail = None;
    loop {
        let event = match reader.read_next() {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(WalReaderError::Corruption(c))
                if policy == RepairPolicy::TruncatePartial
                    && c.repairable()
                    && projection.latest_sequence() >= 1 =>
            {
                // Destructive repair must validate the entire candidate prefix first.
                if snapshot_loaded {
                    crate::store::verify::verify_checkpoint_prefix(session)?;
                    return recover(session, policy, false);
                }
                tail = Some(c);
                break;
            }
            Err(e) => return Err(invalid(e)),
        };
        validate_replay_event(session, &mut projection, &event)?;
    }
    if projection.latest_sequence() == 0 {
        return Err(invalid(
            "complete WAL with StoreInitialized required; snapshot-only stores are unsupported",
        ));
    }
    if !snapshot_loaded {
        projection.projection_image()?;
    }
    phases.tail_replay_seconds = start.elapsed().as_secs_f64();
    phases.snapshot = super::work::snapshot_phases();
    let cursor = reader.cursor();
    let origin = uuid::Uuid::new_v4();
    projection.checkpoint_origin = Some(origin);
    session.observe_snapshot(snapshot_sequence);
    Ok(RecoveredProjection {
        projection,
        snapshot_loaded,
        snapshot_sequence,
        incomplete_tail: tail,
        cursor,
        session_id: session.id(),
        origin,
        observed_wal_len: std::fs::metadata(session.wal_path())?.len(),
        phases,
    })
}
pub(crate) fn validate_replay_event(
    session: &StoreSession,
    p: &mut ReplayReducer,
    event: &crate::wal::event::WalEvent,
) -> Result<(), StoreError> {
    if p.latest_sequence() == 0 {
        if !matches!(event.event(),WalEventType::StoreInitialized { manifest_digest } if *manifest_digest==session.manifest().digest())
        {
            return Err(invalid("first record must bind StoreInitialized to manifest"));
        }
    } else if matches!(event.event(), WalEventType::StoreInitialized { .. }) {
        return Err(invalid("repeated StoreInitialized"));
    }
    crate::store::check_event_profile(event.event(), &session.manifest().features)?;
    p.validate_target_event(event.event()).map_err(invalid)?;
    p.apply(event).map_err(invalid)
}
pub fn load_projection_from_storage(
    data_root: &std::path::Path,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    // A platform-capable binary does not silently enable tenancy on a new
    // local store. Existing manifests remain authoritative; hosts opt in via
    // load_projection_with_features when provisioning a platform store.
    load_projection_with_features(
        data_root,
        crate::store::capabilities().into_iter().filter(|f| f != "platform").collect(),
    )
}
pub fn load_projection_with_features(
    data_root: &std::path::Path,
    features: Vec<String>,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    load_projection_with_options(data_root, OpenOptions::Initialize { features })
}
/// Opens and recovers an existing store without provisioning a missing directory.
pub fn load_existing_projection(
    data_root: &std::path::Path,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    load_projection_with_options(data_root, OpenOptions::ReadWrite)
}
fn load_projection_with_options(
    data_root: &std::path::Path,
    options: OpenOptions,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    let started = Instant::now();
    let session = open_store(data_root, options)
        .map_err(|e| RecoveryBootstrapError::WalInit(e.to_string()))?;
    let recovered = recover_read_only(&session, RepairPolicy::Strict)
        .map_err(|e| RecoveryBootstrapError::WalReplay(e.to_string()))?;
    let wal_path = session.wal_path();
    let snapshot_path = session.snapshot_path();
    let writer = WalFsWriter::from_recovered(session, &recovered)
        .map_err(|e| RecoveryBootstrapError::WalInit(e.to_string()))?;
    let wal_append_telemetry = WalAppendTelemetry::new();
    let latest = recovered.projection.latest_sequence();
    let replayed = latest - recovered.snapshot_sequence;
    Ok(RecoveryBootstrap {
        projection: recovered.projection,
        wal_writer: InstrumentedWalWriter::new(writer, wal_append_telemetry.clone()),
        wal_append_telemetry,
        wal_path,
        snapshot_path,
        snapshot_loaded: recovered.snapshot_loaded,
        snapshot_sequence: recovered.snapshot_sequence,
        phases: recovered.phases,
        recovery_observations: RecoveryObservations {
            recovery_duration_seconds: started.elapsed().as_secs_f64(),
            events_applied_total: replayed,
            snapshot_events_applied: 0,
            wal_replay_events_applied: replayed,
        },
    })
}
/// Arbitrary images take the full-validation path. Fast capture requires authority provenance.
pub(crate) fn validate_snapshot_for_session(
    session: &StoreSession,
    snapshot: &Snapshot,
) -> Result<crate::snapshot::wire_v10::Checkpoint, StoreError> {
    let restored = ReplayReducer::from_projection_image(ProjectionImageV8(snapshot.clone()))?;
    let mut reader = WalFsReader::for_session(session).map_err(invalid)?;
    let mut prefix = ReplayReducer::new();
    while prefix.latest_sequence() < snapshot.metadata.wal_sequence {
        let event =
            reader.read_next().map_err(invalid)?.ok_or_else(|| invalid("snapshot exceeds WAL"))?;
        validate_replay_event(session, &mut prefix, &event)?;
    }
    if prefix.latest_sequence() == 0
        || prefix.projection_digest()? != restored.projection_digest()?
    {
        return Err(invalid("snapshot differs from WAL prefix"));
    }
    crate::snapshot::mapping::validate_profile(snapshot, &session.manifest().features)?;
    Ok(crate::snapshot::wire_v10::Checkpoint {
        binding: crate::snapshot::wire_v10::Binding {
            store: *session.manifest().store_id.as_bytes(),
            manifest: session.manifest().digest(),
            commitment_version: 1,
            cursor: reader.cursor(),
        },
        snapshot: snapshot.clone(),
        inspection: prefix.inspection_index.export(),
    })
}
