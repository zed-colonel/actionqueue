//! Recovery bootstrap utilities for loading projections from storage.
//!
//! This module provides a deterministic, replay-only bootstrap path that
//! reconstructs authoritative projection state from snapshot + WAL tail.
//! It enforces strict invariant boundaries by avoiding direct mutation
//! of projection internals and by mapping all snapshot contents through
//! WAL-equivalent events.

use std::path::PathBuf;
use std::time::Instant;

use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_core::run::RunInstance;

use crate::recovery::reducer::ReplayReducer;
use crate::recovery::replay::ReplayDriver;
use crate::snapshot::loader::{SnapshotFsLoader, SnapshotLoader};
use crate::snapshot::mapping::{
    map_snapshot_attempt_history, map_snapshot_lease_metadata, map_snapshot_run_history,
};
use crate::snapshot::model::Snapshot;
use crate::wal::event::{WalEvent, WalEventType};
use crate::wal::fs_reader::WalFsReader;
use crate::wal::fs_writer::WalFsWriter;
use crate::wal::reader::{WalReader, WalReaderError};
use crate::wal::{InstrumentedWalWriter, WalAppendTelemetry};

/// Authoritative recovery observations emitted by storage bootstrap execution.
#[derive(Debug, Clone, Copy, PartialEq)]
#[must_use]
pub struct RecoveryObservations {
    /// Measured bootstrap recovery wall duration in seconds.
    pub recovery_duration_seconds: f64,
    /// Total applied recovery events (`snapshot + WAL replay`).
    pub events_applied_total: u64,
    /// Applied event count contributed by snapshot hydration.
    pub snapshot_events_applied: u64,
    /// Applied event count contributed by WAL tail replay.
    pub wal_replay_events_applied: u64,
}

impl RecoveryObservations {
    /// Returns a zeroed observation payload for deterministic test setup.
    pub const fn zero() -> Self {
        Self {
            recovery_duration_seconds: 0.0,
            events_applied_total: 0,
            snapshot_events_applied: 0,
            wal_replay_events_applied: 0,
        }
    }
}

/// Recovery bootstrap output containing projection state and storage handles.
#[must_use]
pub struct RecoveryBootstrap {
    /// Replayed projection state derived from snapshot + WAL.
    pub projection: ReplayReducer,
    /// WAL writer handle for future mutation/control lanes.
    pub wal_writer: InstrumentedWalWriter<WalFsWriter>,
    /// Authoritative WAL append telemetry for daemon metrics surfaces.
    pub wal_append_telemetry: WalAppendTelemetry,
    /// Full WAL file path.
    pub wal_path: PathBuf,
    /// Full snapshot file path.
    pub snapshot_path: PathBuf,
    /// Whether a snapshot was loaded.
    pub snapshot_loaded: bool,
    /// The WAL sequence number encoded in the loaded snapshot (0 if none).
    pub snapshot_sequence: u64,
    /// Authoritative recovery observations from this bootstrap execution.
    pub recovery_observations: RecoveryObservations,
}

/// Typed errors for recovery bootstrap failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryBootstrapError {
    /// WAL initialization failed.
    WalInit(String),
    /// WAL read failed.
    WalRead(String),
    /// Snapshot load failed.
    SnapshotLoad(String),
    /// WAL replay failed.
    WalReplay(String),
    /// Snapshot bootstrap failed.
    SnapshotBootstrap(String),
}

impl std::fmt::Display for RecoveryBootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryBootstrapError::WalInit(msg) => write!(f, "WAL init error: {msg}"),
            RecoveryBootstrapError::WalRead(msg) => write!(f, "WAL read error: {msg}"),
            RecoveryBootstrapError::SnapshotLoad(msg) => write!(f, "snapshot load error: {msg}"),
            RecoveryBootstrapError::WalReplay(msg) => write!(f, "WAL replay error: {msg}"),
            RecoveryBootstrapError::SnapshotBootstrap(msg) => {
                write!(f, "snapshot bootstrap error: {msg}")
            }
        }
    }
}

impl std::error::Error for RecoveryBootstrapError {}

/// Loads an authoritative projection from storage using snapshot + WAL tail.
///
/// This function creates required storage directories, initializes WAL handles,
/// attempts snapshot load, then replays the WAL tail from the snapshot sequence.
///
/// # Errors
///
/// Returns [`RecoveryBootstrapError`] variants with the underlying error string
/// for deterministic diagnostics.
pub fn load_projection_from_storage(
    data_root: &std::path::Path,
) -> Result<RecoveryBootstrap, RecoveryBootstrapError> {
    let recovery_started_at = Instant::now();
    tracing::info!(data_dir = %data_root.display(), "storage bootstrap started");

    let wal_dir = data_root.join("wal");
    let snapshot_dir = data_root.join("snapshots");
    let wal_path = wal_dir.join("actionqueue.wal");
    let snapshot_path = snapshot_dir.join("snapshot.bin");

    std::fs::create_dir_all(&wal_dir)
        .and_then(|_| std::fs::create_dir_all(&snapshot_dir))
        .map_err(|err| RecoveryBootstrapError::WalInit(err.to_string()))?;

    let wal_writer = WalFsWriter::new(wal_path.clone())
        .map_err(|err| RecoveryBootstrapError::WalInit(err.to_string()))?;
    let wal_append_telemetry = WalAppendTelemetry::new();
    let wal_writer = InstrumentedWalWriter::new(wal_writer, wal_append_telemetry.clone());

    let wal_reader = WalFsReader::new(wal_path.clone())
        .map_err(|err| RecoveryBootstrapError::WalRead(err.to_string()))?;

    let (snapshot_loaded, snapshot_sequence, reducer, snapshot_events_applied) =
        match SnapshotFsLoader::new(snapshot_path.clone()).load() {
            Ok(None) => (false, 0, ReplayReducer::new(), 0),
            Ok(Some(snapshot)) => {
                let sequence = snapshot.metadata.wal_sequence;
                let task_count = snapshot.metadata.task_count;
                tracing::info!(wal_sequence = sequence, task_count, "snapshot loaded successfully");
                match bootstrap_reducer_from_snapshot(&snapshot) {
                    Ok((reducer, snapshot_events_applied)) => {
                        (true, sequence, reducer, snapshot_events_applied)
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "snapshot bootstrap failed, falling back to WAL-only replay"
                        );
                        (false, 0, ReplayReducer::new(), 0)
                    }
                }
            }
            Err(err) => {
                // Snapshots are derived acceleration artifacts — a corrupt snapshot
                // must not prevent recovery. Fall back to full WAL replay.
                tracing::warn!(
                    error = %err,
                    "snapshot load failed, falling back to WAL-only replay"
                );
                (false, 0, ReplayReducer::new(), 0)
            }
        };

    // Validate snapshot sequence against WAL max sequence.
    let wal_max_sequence = wal_writer.inner().current_sequence();
    if snapshot_loaded && snapshot_sequence > wal_max_sequence {
        if wal_max_sequence == 0 {
            tracing::info!(
                snapshot_sequence,
                "bootstrapping from snapshot with empty WAL — WAL events prior to snapshot are \
                 not recoverable"
            );
        } else {
            return Err(RecoveryBootstrapError::SnapshotBootstrap(format!(
                "snapshot sequence {snapshot_sequence} exceeds WAL max sequence {wal_max_sequence}"
            )));
        }
    }

    let mut wal_reader = wal_reader;

    if snapshot_sequence > 0 {
        match wal_reader.seek_to_sequence(snapshot_sequence + 1) {
            Ok(()) => {}
            Err(WalReaderError::EndOfWal) => {
                let recovery_duration_seconds = recovery_started_at.elapsed().as_secs_f64();
                let wal_replay_events_applied = 0;
                let events_applied_total =
                    snapshot_events_applied.saturating_add(wal_replay_events_applied);
                return Ok(RecoveryBootstrap {
                    projection: reducer,
                    wal_writer,
                    wal_append_telemetry,
                    wal_path,
                    snapshot_path,
                    snapshot_loaded,
                    snapshot_sequence,
                    recovery_observations: RecoveryObservations {
                        recovery_duration_seconds,
                        events_applied_total,
                        snapshot_events_applied,
                        wal_replay_events_applied,
                    },
                });
            }
            Err(err) => return Err(RecoveryBootstrapError::WalRead(err.to_string())),
        }
    }

    let mut driver = ReplayDriver::new(wal_reader, reducer);
    let wal_replay_events_applied = driver
        .run_with_applied_count()
        .map_err(|err| RecoveryBootstrapError::WalReplay(err.to_string()))?;
    tracing::info!(event_count = wal_replay_events_applied, "WAL replay complete");
    let reducer = driver.into_reducer();
    let recovery_duration_seconds = recovery_started_at.elapsed().as_secs_f64();
    let events_applied_total = snapshot_events_applied.saturating_add(wal_replay_events_applied);

    tracing::info!(
        events_applied_total,
        snapshot_loaded,
        snapshot_events_applied,
        wal_replay_events_applied,
        recovery_duration_seconds,
        "recovery bootstrap complete"
    );

    Ok(RecoveryBootstrap {
        projection: reducer,
        wal_writer,
        wal_append_telemetry,
        wal_path,
        snapshot_path,
        snapshot_loaded,
        snapshot_sequence,
        recovery_observations: RecoveryObservations {
            recovery_duration_seconds,
            events_applied_total,
            snapshot_events_applied,
            wal_replay_events_applied,
        },
    })
}

fn bootstrap_reducer_from_snapshot(
    snapshot: &Snapshot,
) -> Result<(ReplayReducer, u64), RecoveryBootstrapError> {
    let mut reducer = ReplayReducer::new();
    let mut sequence = 1u64;
    let mut snapshot_events_applied = 0u64;

    for task in &snapshot.tasks {
        let event = WalEvent::new(
            sequence,
            WalEventType::TaskCreated {
                task_spec: task.task_spec.clone(),
                timestamp: task.created_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        if let Some(canceled_at) = task.canceled_at {
            let event = WalEvent::new(
                sequence,
                WalEventType::TaskCanceled { task_id: task.task_spec.id(), timestamp: canceled_at },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }
    }

    // Synthesize dependency declarations from snapshot (S-1 fix).
    for decl in &snapshot.dependency_declarations {
        let event = WalEvent::new(
            sequence,
            WalEventType::DependencyDeclared {
                task_id: decl.task_id,
                depends_on: decl.depends_on.clone(),
                timestamp: decl.declared_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    if let Some(paused_at) = snapshot.engine.paused_at {
        let event = WalEvent::new(sequence, WalEventType::EnginePaused { timestamp: paused_at });
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    if let Some(resumed_at) = snapshot.engine.resumed_at {
        let event = WalEvent::new(sequence, WalEventType::EngineResumed { timestamp: resumed_at });
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    for run in &snapshot.runs {
        let run_instance = run.run_instance.clone();
        let scheduled_run = RunInstance::new_scheduled_with_id(
            run_instance.id(),
            run_instance.task_id(),
            run_instance.scheduled_at(),
            run_instance.created_at(),
        )
        .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;

        let event =
            WalEvent::new(sequence, WalEventType::RunCreated { run_instance: scheduled_run });
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        let snapshot_state = run_instance.state();
        let mut previous_state = RunState::Scheduled;

        for entry in run.state_history.iter().skip(1) {
            if entry.from != Some(previous_state) {
                return Err(RecoveryBootstrapError::SnapshotBootstrap(
                    "invalid snapshot state history for bootstrap".to_string(),
                ));
            }
            if !is_valid_transition(previous_state, entry.to) {
                return Err(RecoveryBootstrapError::SnapshotBootstrap(
                    "invalid snapshot state transition for bootstrap".to_string(),
                ));
            }

            let event = WalEvent::new(
                sequence,
                WalEventType::RunStateChanged {
                    run_id: run_instance.id(),
                    previous_state,
                    new_state: entry.to,
                    timestamp: entry.timestamp,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
            previous_state = entry.to;
        }

        if previous_state != snapshot_state {
            return Err(RecoveryBootstrapError::SnapshotBootstrap(
                "snapshot state history does not match run state".to_string(),
            ));
        }

        // Restore attempt state from snapshot attempt history.
        // Bootstrap replay walks state transitions but never calls
        // start_attempt()/finish_attempt(), so we must seed these directly.
        let attempt_count = run.attempts.len() as u32;
        let active_attempt = if snapshot_state == RunState::Running {
            // If the run is Running, the last attempt may still be active
            // (no finished_at timestamp means the attempt is in-flight).
            run.attempts.last().and_then(|a| {
                if a.finished_at.is_none() {
                    Some(a.attempt_id)
                } else {
                    None
                }
            })
        } else {
            None
        };
        if let Some(run_inst) = reducer.get_run_instance_mut(run_instance.id()) {
            run_inst.restore_attempt_state_for_bootstrap(attempt_count, active_attempt);
        }

        reducer.set_run_history(
            run_instance.id(),
            map_snapshot_run_history(run.state_history.clone()),
        );
        reducer.set_attempt_history(
            run_instance.id(),
            map_snapshot_attempt_history(run.attempts.clone()),
        );
        let lease_metadata = map_snapshot_lease_metadata(run.lease.clone());
        if let Some(metadata) = lease_metadata {
            reducer.set_lease_for_bootstrap(run_instance.id(), metadata);
        }
    }

    // Synthesize budget allocation events from snapshot.
    for budget in &snapshot.budgets {
        let event = WalEvent::new(
            sequence,
            WalEventType::BudgetAllocated {
                task_id: budget.task_id,
                dimension: budget.dimension,
                limit: budget.limit,
                timestamp: budget.allocated_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        if budget.consumed > 0 {
            let event = WalEvent::new(
                sequence,
                WalEventType::BudgetConsumed {
                    task_id: budget.task_id,
                    dimension: budget.dimension,
                    amount: budget.consumed,
                    timestamp: budget.allocated_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }

        if budget.exhausted && budget.consumed < budget.limit {
            let event = WalEvent::new(
                sequence,
                WalEventType::BudgetExhausted {
                    task_id: budget.task_id,
                    dimension: budget.dimension,
                    timestamp: budget.allocated_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }
    }

    // Synthesize subscription events from snapshot.
    for sub in &snapshot.subscriptions {
        let event = WalEvent::new(
            sequence,
            WalEventType::SubscriptionCreated {
                subscription_id: sub.subscription_id,
                task_id: sub.task_id,
                filter: sub.filter.clone(),
                timestamp: sub.created_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        if let Some(triggered_at) = sub.triggered_at {
            let event = WalEvent::new(
                sequence,
                WalEventType::SubscriptionTriggered {
                    subscription_id: sub.subscription_id,
                    timestamp: triggered_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }

        if let Some(canceled_at) = sub.canceled_at {
            let event = WalEvent::new(
                sequence,
                WalEventType::SubscriptionCanceled {
                    subscription_id: sub.subscription_id,
                    timestamp: canceled_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }
    }

    // Bootstrap actor, tenant, role, capability, and ledger state from snapshot.
    for actor in &snapshot.actors {
        let event = WalEvent::new(
            sequence,
            WalEventType::ActorRegistered {
                actor_id: actor.actor_id,
                identity: actor.identity.clone(),
                capabilities: actor.capabilities.clone(),
                department: actor.department.clone(),
                heartbeat_interval_secs: actor.heartbeat_interval_secs,
                tenant_id: actor.tenant_id,
                timestamp: actor.registered_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        if let Some(deregistered_at) = actor.deregistered_at {
            let event = WalEvent::new(
                sequence,
                WalEventType::ActorDeregistered {
                    actor_id: actor.actor_id,
                    timestamp: deregistered_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }
    }

    for tenant in &snapshot.tenants {
        let event = WalEvent::new(
            sequence,
            WalEventType::TenantCreated {
                tenant_id: tenant.tenant_id,
                name: tenant.name.clone(),
                timestamp: tenant.created_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    for ra in &snapshot.role_assignments {
        let event = WalEvent::new(
            sequence,
            WalEventType::RoleAssigned {
                actor_id: ra.actor_id,
                role: ra.role.clone(),
                tenant_id: ra.tenant_id,
                timestamp: ra.assigned_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    for cg in &snapshot.capability_grants {
        let event = WalEvent::new(
            sequence,
            WalEventType::CapabilityGranted {
                actor_id: cg.actor_id,
                capability: cg.capability.clone(),
                tenant_id: cg.tenant_id,
                timestamp: cg.granted_at,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);

        if let Some(revoked_at) = cg.revoked_at {
            let event = WalEvent::new(
                sequence,
                WalEventType::CapabilityRevoked {
                    actor_id: cg.actor_id,
                    capability: cg.capability.clone(),
                    tenant_id: cg.tenant_id,
                    timestamp: revoked_at,
                },
            );
            reducer
                .apply(&event)
                .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
            snapshot_events_applied = snapshot_events_applied.saturating_add(1);
            sequence = sequence.saturating_add(1);
        }
    }

    for le in &snapshot.ledger_entries {
        let event = WalEvent::new(
            sequence,
            WalEventType::LedgerEntryAppended {
                entry_id: le.entry_id,
                tenant_id: le.tenant_id,
                ledger_key: le.ledger_key.clone(),
                actor_id: le.actor_id,
                payload: le.payload.clone(),
                timestamp: le.timestamp,
            },
        );
        reducer
            .apply(&event)
            .map_err(|err| RecoveryBootstrapError::SnapshotBootstrap(err.to_string()))?;
        snapshot_events_applied = snapshot_events_applied.saturating_add(1);
        sequence = sequence.saturating_add(1);
    }

    // Note: `sequence` counts synthesized events (tasks, deps, engine, runs, state
    // transitions), which is intentionally less than `snapshot.metadata.wal_sequence`
    // because attempt events (AttemptStart, AttemptFinished) and lease events
    // (LeaseAcquire, LeaseRelease, LeaseExpire, LeaseHeartbeat) are seeded directly
    // via dedicated bootstrap methods rather than synthesized as WAL events.
    // The snapshot's structural integrity is validated by `validate_snapshot()` before
    // we reach this point.
    let _ = sequence; // consumed by synthesis loop

    reducer.set_latest_sequence_for_bootstrap(snapshot.metadata.wal_sequence);

    Ok((reducer, snapshot_events_applied))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use actionqueue_core::ids::{AttemptId, RunId, TaskId};
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

    use super::*;
    use crate::snapshot::mapping::SNAPSHOT_SCHEMA_VERSION;
    use crate::snapshot::model::{
        SnapshotAttemptHistoryEntry, SnapshotEngineControl, SnapshotLeaseMetadata,
        SnapshotMetadata, SnapshotRun, SnapshotRunStateHistoryEntry, SnapshotTask,
    };
    use crate::snapshot::writer::{SnapshotFsWriter, SnapshotWriter};
    use crate::wal::codec;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_data_root() -> PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = dir.join(format!(
            "actionqueue_recovery_bootstrap_test_{}_{}",
            std::process::id(),
            count
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    /// Test helper: builds a SnapshotTask with standard test defaults.
    /// When new fields are added to SnapshotTask, only this function needs updating.
    fn test_snapshot_task(task_spec: TaskSpec) -> SnapshotTask {
        SnapshotTask { task_spec, created_at: 0, updated_at: None, canceled_at: None }
    }

    /// Test helper: builds a SnapshotAttemptHistoryEntry with standard defaults.
    fn test_attempt_entry(
        attempt_id: AttemptId,
        started_at: u64,
        finished_at: Option<u64>,
    ) -> SnapshotAttemptHistoryEntry {
        SnapshotAttemptHistoryEntry {
            attempt_id,
            started_at,
            finished_at,
            result: None,
            error: None,
            output: None,
        }
    }

    fn create_task_spec(payload: &[u8]) -> TaskSpec {
        TaskSpec::new(
            TaskId::new(),
            TaskPayload::with_content_type(payload.to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("task spec should be valid")
    }

    fn create_snapshot(task: &TaskSpec, run_state: RunState, wal_sequence: u64) -> Snapshot {
        let run_instance = RunInstance::new_scheduled_with_id(RunId::new(), task.id(), 10, 10)
            .expect("scheduled run should be valid");

        let run_instance = if run_state == RunState::Scheduled {
            run_instance
        } else {
            let mut run_instance = run_instance;
            run_instance.transition_to(run_state).expect("transition should be valid for test");
            run_instance
        };

        let state_history = if run_state == RunState::Scheduled {
            vec![SnapshotRunStateHistoryEntry {
                from: None,
                to: RunState::Scheduled,
                timestamp: 10,
            }]
        } else {
            vec![
                SnapshotRunStateHistoryEntry { from: None, to: RunState::Scheduled, timestamp: 10 },
                SnapshotRunStateHistoryEntry {
                    from: Some(RunState::Scheduled),
                    to: run_state,
                    timestamp: 11,
                },
            ]
        };

        Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence,
                task_count: 1,
                run_count: 1,
            },
            tasks: vec![test_snapshot_task(task.clone())],
            runs: vec![SnapshotRun {
                run_instance,
                state_history,
                attempts: Vec::new(),
                lease: None,
            }],
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        }
    }

    fn write_snapshot(path: &Path, snapshot: &Snapshot) {
        let mut writer = SnapshotFsWriter::new(path.to_path_buf())
            .expect("snapshot writer should open for bootstrap test");
        writer.write(snapshot).expect("snapshot write should succeed");
        writer.flush().expect("snapshot flush should succeed");
        writer.close().expect("snapshot close should succeed");
    }

    fn write_wal_events(path: &Path, events: &[WalEvent]) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("wal file open should succeed");
        for event in events {
            let bytes = codec::encode(event).expect("encode should succeed");
            file.write_all(&bytes).expect("wal write should succeed");
        }
        file.sync_all().expect("wal sync should succeed");
    }

    #[test]
    fn test_bootstrap_empty_storage() {
        let data_root = temp_data_root();
        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for empty storage");

        assert!(!result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 0);
        assert_eq!(result.projection.task_count(), 0);
        assert_eq!(result.projection.run_count(), 0);
        assert_eq!(result.projection.latest_sequence(), 0);
        assert_eq!(result.wal_append_telemetry.snapshot().append_success_total, 0);
        assert_eq!(result.wal_append_telemetry.snapshot().append_failure_total, 0);
        assert_eq!(result.recovery_observations.events_applied_total, 0);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 0);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 0);
        assert!(result.recovery_observations.recovery_duration_seconds >= 0.0);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_only_with_exact_sequence() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[1, 2, 3]);
        let snapshot = create_snapshot(&task, RunState::Scheduled, 3);

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for snapshot-only storage");

        assert!(result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 3);
        assert_eq!(result.projection.task_count(), 1);
        assert_eq!(result.projection.run_count(), 1);
        assert_eq!(result.projection.latest_sequence(), 3);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 2);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 0);
        assert_eq!(result.recovery_observations.events_applied_total, 2);
        assert!(result.recovery_observations.recovery_duration_seconds >= 0.0);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_with_wal_tail() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[4, 5, 6]);
        let snapshot = create_snapshot(&task, RunState::Scheduled, 3);

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let wal_path = data_root.join("wal").join("actionqueue.wal");
        std::fs::create_dir_all(wal_path.parent().expect("wal parent"))
            .expect("wal dir should create");

        let extra_task = create_task_spec(&[7, 8, 9]);
        let wal_events = vec![WalEvent::new(
            4,
            WalEventType::TaskCreated { task_spec: extra_task.clone(), timestamp: 0 },
        )];
        write_wal_events(&wal_path, &wal_events);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for snapshot + WAL tail");

        assert!(result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 3);
        assert_eq!(result.projection.task_count(), 2);
        assert_eq!(result.projection.latest_sequence(), 4);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 2);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 1);
        assert_eq!(result.recovery_observations.events_applied_total, 3);
        assert!(result.recovery_observations.recovery_duration_seconds >= 0.0);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_with_high_wal_sequence_succeeds() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[10, 11, 12]);
        // wal_sequence=10 is higher than synthesized event count (2 events for
        // 1 task + 1 run), reflecting real snapshots where attempt/lease events
        // in the WAL are seeded directly rather than synthesized.
        let snapshot = create_snapshot(&task, RunState::Scheduled, 10);

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root);
        assert!(result.is_ok(), "snapshot with high wal_sequence should bootstrap successfully");
        let recovery = result.expect("recovery should succeed");
        assert!(recovery.snapshot_loaded, "snapshot should be loaded despite high wal_sequence");

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn p6_011_t_p3_bootstrap_hydrates_task_canceled_projection_from_snapshot() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[1, 9, 9]);
        let canceled_at = 77;
        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 2,
                task_count: 1,
                run_count: 0,
            },
            tasks: vec![SnapshotTask {
                canceled_at: Some(canceled_at),
                ..test_snapshot_task(task.clone())
            }],
            runs: Vec::new(),
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for canceled task");

        assert!(result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 2);
        assert!(result.projection.is_task_canceled(task.id()));
        assert_eq!(result.projection.task_canceled_at(task.id()), Some(canceled_at));
        assert_eq!(result.projection.latest_sequence(), 2);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 2);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 0);
        assert_eq!(result.recovery_observations.events_applied_total, 2);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn p6_013_t_p3_bootstrap_hydrates_engine_paused_projection_from_snapshot() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[2, 2, 2]);
        let paused_at = 120;
        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 2,
                task_count: 1,
                run_count: 0,
            },
            tasks: vec![test_snapshot_task(task)],
            runs: Vec::new(),
            engine: SnapshotEngineControl {
                paused: true,
                paused_at: Some(paused_at),
                resumed_at: None,
            },
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for engine paused snapshot");

        assert!(result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 2);
        assert!(result.projection.is_engine_paused());
        assert_eq!(result.projection.engine_paused_at(), Some(paused_at));
        assert_eq!(result.projection.engine_resumed_at(), None);
        assert_eq!(result.projection.latest_sequence(), 2);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 2);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 0);
        assert_eq!(result.recovery_observations.events_applied_total, 2);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn p6_013_t_p4_bootstrap_hydrates_engine_resumed_projection_from_snapshot() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[2, 2, 3]);
        let paused_at = 120;
        let resumed_at = 180;
        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 3,
                task_count: 1,
                run_count: 0,
            },
            tasks: vec![test_snapshot_task(task)],
            runs: Vec::new(),
            engine: SnapshotEngineControl {
                paused: false,
                paused_at: Some(paused_at),
                resumed_at: Some(resumed_at),
            },
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for engine resumed snapshot");

        assert!(result.snapshot_loaded);
        assert_eq!(result.snapshot_sequence, 3);
        assert!(!result.projection.is_engine_paused());
        assert_eq!(result.projection.engine_paused_at(), Some(paused_at));
        assert_eq!(result.projection.engine_resumed_at(), Some(resumed_at));
        assert_eq!(result.projection.latest_sequence(), 3);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 3);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 0);
        assert_eq!(result.recovery_observations.events_applied_total, 3);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_wal_only_reports_wal_replay_events() {
        let data_root = temp_data_root();
        let wal_path = data_root.join("wal").join("actionqueue.wal");
        std::fs::create_dir_all(wal_path.parent().expect("wal parent"))
            .expect("wal dir should create");

        let task = create_task_spec(&[7, 7, 7]);
        let wal_events =
            vec![WalEvent::new(1, WalEventType::TaskCreated { task_spec: task, timestamp: 0 })];
        write_wal_events(&wal_path, &wal_events);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for WAL-only storage");

        assert!(!result.snapshot_loaded);
        assert_eq!(result.recovery_observations.snapshot_events_applied, 0);
        assert_eq!(result.recovery_observations.wal_replay_events_applied, 1);
        assert_eq!(result.recovery_observations.events_applied_total, 1);
        assert!(result.recovery_observations.recovery_duration_seconds >= 0.0);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_snapshot_sequence_exceeds_wal_max_sequence() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[1, 2, 3]);
        // wal_sequence=3 is valid for a snapshot with 1 task + 1 run (2 bootstrap events)
        // but WAL max will be only 1
        let snapshot = create_snapshot(&task, RunState::Scheduled, 3);

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        // Create WAL with max sequence = 1 (less than snapshot's 3)
        let wal_path = data_root.join("wal").join("actionqueue.wal");
        std::fs::create_dir_all(wal_path.parent().expect("wal parent"))
            .expect("wal dir should create");
        let wal_task = create_task_spec(&[4, 5, 6]);
        let wal_events =
            vec![WalEvent::new(1, WalEventType::TaskCreated { task_spec: wal_task, timestamp: 0 })];
        write_wal_events(&wal_path, &wal_events);

        let result = load_projection_from_storage(&data_root);
        assert!(matches!(
            result,
            Err(RecoveryBootstrapError::SnapshotBootstrap(msg))
                if msg.contains("snapshot sequence 3 exceeds WAL max sequence 1")
        ));

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_snapshot_sequence_within_wal_max_succeeds() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[1, 2, 3]);
        let snapshot = create_snapshot(&task, RunState::Scheduled, 3);

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        // Create WAL with max sequence = 4 (greater than snapshot's 3)
        let wal_path = data_root.join("wal").join("actionqueue.wal");
        std::fs::create_dir_all(wal_path.parent().expect("wal parent"))
            .expect("wal dir should create");
        let extra_task = create_task_spec(&[7, 8, 9]);
        let wal_events = vec![WalEvent::new(
            4,
            WalEventType::TaskCreated { task_spec: extra_task, timestamp: 0 },
        )];
        write_wal_events(&wal_path, &wal_events);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed when snapshot sequence <= WAL max");

        assert!(result.snapshot_loaded);
        assert_eq!(result.projection.task_count(), 2);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_with_active_lease() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[20, 21, 22]);
        let run_id = RunId::new();

        // Build a Leased run with active lease data in the snapshot
        let mut run_instance = RunInstance::new_scheduled_with_id(run_id, task.id(), 10, 10)
            .expect("scheduled run should be valid");
        run_instance.transition_to(RunState::Ready).expect("transition to Ready");
        run_instance.transition_to(RunState::Leased).expect("transition to Leased");

        let state_history = vec![
            SnapshotRunStateHistoryEntry { from: None, to: RunState::Scheduled, timestamp: 10 },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Scheduled),
                to: RunState::Ready,
                timestamp: 11,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Ready),
                to: RunState::Leased,
                timestamp: 12,
            },
        ];

        let lease = Some(SnapshotLeaseMetadata {
            owner: "worker-1".to_string(),
            expiry: 1000,
            acquired_at: 12,
            updated_at: 12,
        });

        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 5,
                task_count: 1,
                run_count: 1,
            },
            tasks: vec![test_snapshot_task(task.clone())],
            runs: vec![SnapshotRun { run_instance, state_history, attempts: Vec::new(), lease }],
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for snapshot with active lease");

        assert!(result.snapshot_loaded);
        let lease = result.projection.get_lease(&run_id);
        assert!(lease.is_some(), "get_lease() should return active lease after bootstrap");
        let (owner, expiry) = lease.unwrap();
        assert_eq!(owner, "worker-1");
        assert_eq!(*expiry, 1000);

        let metadata = result.projection.get_lease_metadata(&run_id);
        assert!(metadata.is_some(), "lease metadata should be present");
        assert_eq!(metadata.unwrap().owner(), "worker-1");
        assert_eq!(metadata.unwrap().acquired_at(), 12);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_with_running_attempt() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[30, 31, 32]);
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();

        // Build a Running run with attempt history.
        // We must call start_attempt() so the RunInstance's attempt_count
        // matches the snapshot attempt history (validation checks parity).
        let mut run_instance = RunInstance::new_scheduled_with_id(run_id, task.id(), 10, 10)
            .expect("scheduled run should be valid");
        run_instance.transition_to(RunState::Ready).expect("transition to Ready");
        run_instance.transition_to(RunState::Leased).expect("transition to Leased");
        run_instance.transition_to(RunState::Running).expect("transition to Running");
        run_instance.start_attempt(attempt_id).expect("start attempt");

        let state_history = vec![
            SnapshotRunStateHistoryEntry { from: None, to: RunState::Scheduled, timestamp: 10 },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Scheduled),
                to: RunState::Ready,
                timestamp: 11,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Ready),
                to: RunState::Leased,
                timestamp: 12,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Leased),
                to: RunState::Running,
                timestamp: 13,
            },
        ];

        let attempts = vec![test_attempt_entry(attempt_id, 13, None)];

        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 6,
                task_count: 1,
                run_count: 1,
            },
            tasks: vec![test_snapshot_task(task.clone())],
            runs: vec![SnapshotRun { run_instance, state_history, attempts, lease: None }],
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap should succeed for snapshot with running attempt");

        assert!(result.snapshot_loaded);
        let run = result.projection.get_run_instance(&run_id);
        assert!(run.is_some(), "run instance should exist after bootstrap");
        let run = run.unwrap();
        assert_eq!(run.attempt_count(), 1);
        assert_eq!(run.current_attempt_id(), Some(attempt_id));
        assert_eq!(run.state(), RunState::Running);

        let _ = fs::remove_dir_all(data_root);
    }

    #[test]
    fn test_bootstrap_snapshot_wal_tail_lease_heartbeat() {
        let data_root = temp_data_root();
        let task = create_task_spec(&[40, 41, 42]);
        let run_id = RunId::new();

        // Build a Leased run with active lease data
        let mut run_instance = RunInstance::new_scheduled_with_id(run_id, task.id(), 10, 10)
            .expect("scheduled run should be valid");
        run_instance.transition_to(RunState::Ready).expect("transition to Ready");
        run_instance.transition_to(RunState::Leased).expect("transition to Leased");

        let state_history = vec![
            SnapshotRunStateHistoryEntry { from: None, to: RunState::Scheduled, timestamp: 10 },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Scheduled),
                to: RunState::Ready,
                timestamp: 11,
            },
            SnapshotRunStateHistoryEntry {
                from: Some(RunState::Ready),
                to: RunState::Leased,
                timestamp: 12,
            },
        ];

        let lease = Some(SnapshotLeaseMetadata {
            owner: "worker-1".to_string(),
            expiry: 1000,
            acquired_at: 12,
            updated_at: 12,
        });

        let snapshot = Snapshot {
            version: 4,
            timestamp: 1234,
            metadata: SnapshotMetadata {
                schema_version: SNAPSHOT_SCHEMA_VERSION,
                wal_sequence: 5,
                task_count: 1,
                run_count: 1,
            },
            tasks: vec![test_snapshot_task(task.clone())],
            runs: vec![SnapshotRun { run_instance, state_history, attempts: Vec::new(), lease }],
            engine: SnapshotEngineControl::default(),
            dependency_declarations: Vec::new(),
            budgets: Vec::new(),
            subscriptions: Vec::new(),
            actors: Vec::new(),
            tenants: Vec::new(),
            role_assignments: Vec::new(),
            capability_grants: Vec::new(),
            ledger_entries: Vec::new(),
        };

        let snapshot_path = data_root.join("snapshots").join("snapshot.bin");
        std::fs::create_dir_all(snapshot_path.parent().expect("snapshot parent"))
            .expect("snapshot dir should create");
        write_snapshot(&snapshot_path, &snapshot);

        // Write a WAL tail with a lease heartbeat event (sequence 6)
        let wal_path = data_root.join("wal").join("actionqueue.wal");
        std::fs::create_dir_all(wal_path.parent().expect("wal parent"))
            .expect("wal dir should create");
        let wal_events = vec![WalEvent::new(
            6,
            WalEventType::LeaseHeartbeat {
                run_id,
                owner: "worker-1".to_string(),
                expiry: 2000,
                timestamp: 50,
            },
        )];
        write_wal_events(&wal_path, &wal_events);

        let result = load_projection_from_storage(&data_root)
            .expect("bootstrap with snapshot lease + WAL heartbeat should succeed");

        assert!(result.snapshot_loaded);
        let lease = result.projection.get_lease(&run_id);
        assert!(lease.is_some(), "lease should be present after heartbeat replay");
        let (owner, expiry) = lease.unwrap();
        assert_eq!(owner, "worker-1");
        assert_eq!(*expiry, 2000, "expiry should be updated by heartbeat");

        let _ = fs::remove_dir_all(data_root);
    }
}
