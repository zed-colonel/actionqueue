//! Builds a Snapshot from the current projection state.

use crate::recovery::reducer::ReplayReducer;
use crate::snapshot::mapping::{validate_snapshot, SnapshotMappingError, SNAPSHOT_SCHEMA_VERSION};
use crate::snapshot::model::{
    Snapshot, SnapshotActor, SnapshotAttemptHistoryEntry, SnapshotBudget, SnapshotCapabilityGrant,
    SnapshotDependencyDeclaration, SnapshotEngineControl, SnapshotLeaseMetadata,
    SnapshotLedgerEntry, SnapshotMetadata, SnapshotRoleAssignment, SnapshotRun,
    SnapshotRunStateHistoryEntry, SnapshotSubscription, SnapshotTask, SnapshotTenant,
};

/// The snapshot format version written by this implementation.
const SNAPSHOT_FORMAT_VERSION: u32 = 9;

/// Builds a validated [`Snapshot`] from the current state of a [`ReplayReducer`].
///
/// This function extracts all tasks, runs (with history, attempts, and lease
/// metadata), and engine control state from the reducer and assembles them
/// into a snapshot. The snapshot is validated before being returned.
pub fn build_snapshot_from_projection(
    reducer: &ReplayReducer,
    timestamp: u64,
) -> Result<Snapshot, SnapshotMappingError> {
    let task_count = reducer.task_records().count();
    let run_count = reducer.run_instances().count();
    tracing::debug!(task_count, run_count, "building snapshot from projection");

    let tasks: Vec<SnapshotTask> = reducer
        .task_records()
        .map(|tr| SnapshotTask {
            task_spec: tr.task_spec().clone(),
            created_at: tr.created_at(),
            updated_at: tr.updated_at(),
            canceled_at: tr.canceled_at(),
        })
        .collect();

    // Emit runs in per-task index order so hydration rebuilds `runs_by_task` exactly
    // as WAL replay did; cascades then emit records in the same order on every replica.
    let runs: Vec<SnapshotRun> = reducer
        .runs_in_index_order()
        .map(|ri| {
            let run_id = ri.id();
            let state_history = reducer
                .get_run_history(&run_id)
                .map(|h| {
                    h.iter()
                        .map(|entry| SnapshotRunStateHistoryEntry {
                            from: entry.from(),
                            to: entry.to(),
                            timestamp: entry.timestamp(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let attempts = reducer
                .get_attempt_history(&run_id)
                .map(|a| {
                    a.iter()
                        .map(|entry| SnapshotAttemptHistoryEntry {
                            disposition: entry.disposition.clone(),
                            accepted_start: entry.accepted_start.clone(),
                            finish_origin: entry.finish_origin,
                            attempt_id: entry.attempt_id(),
                            started_at: entry.started_at(),
                            finished_at: entry.finished_at(),
                            result: entry.result(),
                            error: entry.error().map(|s| s.to_string()),
                            output: entry.output.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            let lease = reducer.get_lease_metadata(&run_id).map(|lm| SnapshotLeaseMetadata {
                granted_at_sequence: lm.granted_at_sequence(),
                owner: lm.owner().to_string(),
                expiry: lm.expiry(),
                acquired_at: lm.acquired_at(),
                updated_at: lm.updated_at(),
            });

            SnapshotRun { run_instance: ri.clone(), state_history, attempts, lease }
        })
        .collect();

    let dependency_declarations: Vec<SnapshotDependencyDeclaration> = reducer
        .dependency_declarations()
        .map(|(task_id, prereqs)| SnapshotDependencyDeclaration {
            task_id,
            depends_on: {
                let mut deps: Vec<actionqueue_core::ids::TaskId> =
                    prereqs.iter().copied().collect();
                deps.sort_by_key(|id| *id.as_uuid());
                deps
            },
            declared_at: reducer.dependency_declared_at.get(&task_id).copied().unwrap_or(0),
        })
        .collect();

    let budgets: Vec<SnapshotBudget> = reducer
        .budgets()
        .map(|((task_id, _dimension), record)| SnapshotBudget {
            task_id: *task_id,
            dimension: record.dimension,
            limit: record.limit,
            consumed: record.consumed,
            allocated_at: record.allocated_at,
            exhausted: record.exhausted,
        })
        .collect();

    let subscriptions: Vec<SnapshotSubscription> = reducer
        .subscriptions()
        .map(|(_id, record)| SnapshotSubscription {
            matched_sequence: record.matched_sequence,
            subscription_id: record.subscription_id,
            task_id: record.task_id,
            filter: record.filter.clone(),
            created_at: record.created_at,
            triggered_at: record.triggered_at,
            canceled_at: record.canceled_at,
        })
        .collect();

    let actors: Vec<SnapshotActor> = reducer
        .actors()
        .map(|(_, record)| SnapshotActor {
            actor_id: record.actor_id,
            identity: record.identity.clone(),
            executor_traits: record.executor_traits.clone(),
            department: record.department.clone(),
            heartbeat_interval_secs: record.heartbeat_interval_secs,
            tenant_id: record.tenant_id,
            registered_at: record.registered_at,
            last_heartbeat_at: record.last_heartbeat_at,
            deregistered_at: record.deregistered_at,
        })
        .collect();

    let tenants: Vec<SnapshotTenant> = reducer
        .tenants()
        .map(|(_, record)| SnapshotTenant {
            tenant_id: record.tenant_id,
            name: record.name.clone(),
            created_at: record.created_at,
        })
        .collect();

    let role_assignments: Vec<SnapshotRoleAssignment> = reducer
        .role_assignments()
        .map(|r| SnapshotRoleAssignment {
            actor_id: r.actor_id,
            role: r.role.clone(),
            tenant_id: r.tenant_id,
            assigned_at: r.assigned_at,
        })
        .collect();

    let capability_grants: Vec<SnapshotCapabilityGrant> = reducer
        .capability_grants()
        .map(|r| SnapshotCapabilityGrant {
            actor_id: r.actor_id,
            capability: r.capability.clone(),
            tenant_id: r.tenant_id,
            granted_at: r.granted_at,
            revoked_at: r.revoked_at,
        })
        .collect();

    let ledger_entries: Vec<SnapshotLedgerEntry> = reducer
        .ledger_entries()
        .map(|r| SnapshotLedgerEntry {
            entry_id: r.entry_id,
            tenant_id: r.tenant_id,
            ledger_key: r.ledger_key.clone(),
            actor_id: r.actor_id,
            payload: r.payload.clone(),
            timestamp: r.timestamp,
        })
        .collect();

    let snapshot = Snapshot {
        control_history: reducer.control_history.iter().map(|(s, c)| (*s, c.clone())).collect(),
        waits: reducer.waits.records().cloned().collect(),
        cancellations: reducer.cancellations.clone(),
        dispatch_sequences: reducer.dispatch_sequences.iter().map(|(r, s)| (*r, *s)).collect(),
        administrative_wakes: reducer.administrative_wakes.values().cloned().collect(),
        administrative_pending: reducer
            .administrative_pending
            .iter()
            .map(|(r, c)| (*r, *c))
            .collect(),
        pending_resumes: reducer.waits.pending.iter().map(|(r, w)| (*r, *w)).collect(),
        key_reservations: reducer.key_reservations.iter().map(|(r, k)| (*r, k.clone())).collect(),
        signals: reducer.signals().records().cloned().collect(),
        last_signal_sequence: reducer.signals().last_sequence().get(),
        admissions: reducer.admissions().cloned().collect(),
        version: SNAPSHOT_FORMAT_VERSION,
        timestamp,
        metadata: SnapshotMetadata {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            wal_sequence: reducer.latest_sequence(),
            task_count: tasks.len() as u64,
            run_count: runs.len() as u64,
        },
        tasks,
        runs,
        engine: SnapshotEngineControl {
            paused: reducer.is_engine_paused(),
            paused_at: reducer.engine_paused_at(),
            resumed_at: reducer.engine_resumed_at(),
        },
        dependency_declarations,
        budgets,
        subscriptions,
        actors,
        tenants,
        role_assignments,
        capability_grants,
        ledger_entries,
    };

    validate_snapshot(&snapshot)?;
    tracing::debug!(
        wal_sequence = snapshot.metadata.wal_sequence,
        task_count = snapshot.metadata.task_count,
        run_count = snapshot.metadata.run_count,
        "snapshot build complete"
    );
    Ok(snapshot)
}
