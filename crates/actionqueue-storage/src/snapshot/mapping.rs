//! Explicit mapping boundary between snapshot wrappers and canonical core models.
//!
//! This module centralizes snapshot validation and run payload mapping so that
//! snapshot persistence remains a strict projection of core semantic truth.

use std::collections::HashSet;

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::run_instance::RunInstance as CoreRunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_core::subscription::SubscriptionId;

use crate::snapshot::model::{
    Snapshot, SnapshotAttemptHistoryEntry, SnapshotEngineControl, SnapshotLeaseMetadata,
    SnapshotRun, SnapshotRunStateHistoryEntry,
};

/// Current snapshot schema version accepted by the explicit mapping boundary.
///
/// Version history:
/// - v4: Sprint 1 release (WAL v2, JSON snapshots)
/// - v5: Sprint 2 additions — parent_task_id on TaskSpec, output on AttemptOutcome,
///   required_capabilities on TaskConstraints
/// - v6: Sprint 2 review — dependency declarations persisted in snapshots
/// - v7: Sprint 3 — budgets, subscriptions, Suspended run state
pub const SNAPSHOT_SCHEMA_VERSION: u32 = 8;

/// Typed mapping and validation errors for snapshot/core parity enforcement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotMappingError {
    /// Snapshot metadata schema version is unknown to this mapping boundary.
    UnsupportedSchemaVersion {
        /// Schema version expected by the current implementation.
        expected: u32,
        /// Schema version found in the snapshot payload.
        found: u32,
    },
    /// Snapshot metadata task count does not match payload contents.
    TaskCountMismatch {
        /// Declared task count in snapshot metadata.
        declared: u64,
        /// Actual number of task entries in snapshot payload.
        actual: u64,
    },
    /// Snapshot metadata run count does not match payload contents.
    RunCountMismatch {
        /// Declared run count in snapshot metadata.
        declared: u64,
        /// Actual number of run entries in snapshot payload.
        actual: u64,
    },
    /// Duplicate task identifier in snapshot payload.
    DuplicateTaskId {
        /// Duplicate task identifier.
        task_id: TaskId,
    },
    /// Snapshot task has cancellation timestamp that violates causal ordering.
    InvalidTaskCanceledAtCausality {
        /// Task identifier associated with the invalid payload.
        task_id: TaskId,
        /// Task creation timestamp encoded in snapshot.
        created_at: u64,
        /// Task cancellation timestamp encoded in snapshot.
        canceled_at: u64,
    },
    /// Duplicate run identifier in snapshot payload.
    DuplicateRunId {
        /// Duplicate run identifier.
        run_id: RunId,
    },
    /// A run references a task that does not exist in the snapshot task set.
    RunReferencesUnknownTask {
        /// Run identifier with the invalid reference.
        run_id: RunId,
        /// Missing task identifier.
        task_id: TaskId,
    },
    /// A run has a nil/invalid run identifier.
    InvalidRunId {
        /// Invalid run identifier value.
        run_id: RunId,
    },
    /// A run has a nil/invalid task identifier.
    InvalidTaskId {
        /// Run identifier associated with the invalid task identifier.
        run_id: RunId,
        /// Invalid task identifier value.
        task_id: TaskId,
    },
    /// A `Ready` run has impossible schedule causality (`scheduled_at > created_at`).
    InvalidReadyScheduleCausality {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Scheduled timestamp encoded in snapshot.
        scheduled_at: u64,
        /// Created timestamp encoded in snapshot.
        created_at: u64,
    },
    /// Attempt lineage state is semantically inconsistent with run state.
    InvalidAttemptLineageState {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Run state that cannot carry an active attempt identifier.
        state: RunState,
    },
    /// Attempt lineage count is inconsistent with active attempt payload.
    InvalidAttemptLineageCount {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Attempt count encoded in snapshot.
        attempt_count: u32,
    },
    /// Snapshot run is missing the initial Scheduled history entry.
    MissingInitialRunStateHistory {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
    },
    /// Snapshot run state history entry is invalid.
    InvalidRunStateHistoryTransition {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Transition source state.
        from: RunState,
        /// Transition target state.
        to: RunState,
    },
    /// Snapshot run has invalid attempt history count.
    InvalidAttemptHistoryCount {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Attempt count encoded in snapshot.
        attempt_count: u32,
        /// Attempt history entries provided in snapshot.
        history_len: usize,
    },
    /// Snapshot run has invalid active attempt history alignment.
    InvalidActiveAttemptHistory {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
    },
    /// Snapshot run has lease metadata when not allowed by state.
    InvalidLeasePresence {
        /// Run identifier associated with the invalid payload.
        run_id: RunId,
        /// Run state that rejected lease presence.
        state: RunState,
    },
    /// A dependency declaration references a task not present in the snapshot.
    DependencyReferencesUnknownTask {
        /// The task with the dependency declaration.
        task_id: TaskId,
        /// The prerequisite task not found in the snapshot.
        prereq_id: TaskId,
    },
    /// A dependency declaration's declaring task is not present in the snapshot.
    DependencyDeclarationUnknownTask {
        /// The declaring task not found in the snapshot.
        task_id: TaskId,
    },
    /// Duplicate dependency declaration for the same task_id.
    DuplicateDependencyDeclaration {
        /// The task with duplicated declarations.
        task_id: TaskId,
    },
    /// Duplicate budget allocation for the same (task_id, dimension) pair.
    DuplicateBudgetAllocation {
        /// The task with duplicated allocations.
        task_id: TaskId,
        /// The duplicated dimension.
        dimension: BudgetDimension,
    },
    /// Budget allocation references a task not present in the snapshot.
    BudgetReferencesUnknownTask {
        /// The task referenced by the budget.
        task_id: TaskId,
    },
    /// Duplicate subscription identifier in snapshot.
    DuplicateSubscriptionId {
        /// The duplicated subscription identifier.
        subscription_id: SubscriptionId,
    },
    /// Subscription references a task not present in the snapshot.
    SubscriptionReferencesUnknownTask {
        /// The subscription identifier.
        subscription_id: SubscriptionId,
        /// The missing task identifier.
        task_id: TaskId,
    },
    /// Snapshot engine control has invalid paused causality.
    InvalidEnginePausedCausality,
    /// Snapshot engine control has invalid resumed causality.
    InvalidEngineResumedCausality,
    /// Snapshot engine control has invalid pause/resume ordering.
    InvalidEnginePauseResumeOrdering {
        /// Snapshot pause timestamp.
        paused_at: u64,
        /// Snapshot resume timestamp.
        resumed_at: u64,
    },
}

impl std::fmt::Display for SnapshotMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchemaVersion { expected, found } => {
                write!(f, "unsupported snapshot schema version: expected {expected}, found {found}")
            }
            Self::TaskCountMismatch { declared, actual } => {
                write!(f, "snapshot task_count mismatch: declared {declared}, actual {actual}")
            }
            Self::RunCountMismatch { declared, actual } => {
                write!(f, "snapshot run_count mismatch: declared {declared}, actual {actual}")
            }
            Self::DuplicateTaskId { task_id } => {
                write!(f, "duplicate snapshot task id: {task_id}")
            }
            Self::InvalidTaskCanceledAtCausality { task_id, created_at, canceled_at } => write!(
                f,
                "snapshot task {task_id} has invalid canceled_at causality: canceled_at \
                 ({canceled_at}) < created_at ({created_at})"
            ),
            Self::DuplicateRunId { run_id } => {
                write!(f, "duplicate snapshot run id: {run_id}")
            }
            Self::RunReferencesUnknownTask { run_id, task_id } => {
                write!(f, "snapshot run {run_id} references unknown task {task_id}")
            }
            Self::InvalidRunId { run_id } => {
                write!(f, "snapshot run has invalid nil run id: {run_id}")
            }
            Self::InvalidTaskId { run_id, task_id } => {
                write!(f, "snapshot run {run_id} has invalid nil task id: {task_id}")
            }
            Self::InvalidReadyScheduleCausality { run_id, scheduled_at, created_at } => write!(
                f,
                "snapshot run {run_id} has invalid Ready schedule causality: scheduled_at \
                 ({scheduled_at}) > created_at ({created_at})"
            ),
            Self::InvalidAttemptLineageState { run_id, state } => write!(
                f,
                "snapshot run {run_id} has active attempt lineage in invalid state {state:?}"
            ),
            Self::InvalidAttemptLineageCount { run_id, attempt_count } => write!(
                f,
                "snapshot run {run_id} has active attempt with invalid attempt_count \
                 ({attempt_count})"
            ),
            Self::MissingInitialRunStateHistory { run_id } => {
                write!(f, "snapshot run {run_id} missing initial Scheduled state history entry")
            }
            Self::InvalidRunStateHistoryTransition { run_id, from, to } => write!(
                f,
                "snapshot run {run_id} has invalid state history transition {from:?} -> {to:?}"
            ),
            Self::InvalidAttemptHistoryCount { run_id, attempt_count, history_len } => write!(
                f,
                "snapshot run {run_id} has attempt history length {history_len} inconsistent with \
                 attempt_count ({attempt_count})"
            ),
            Self::InvalidActiveAttemptHistory { run_id } => {
                write!(f, "snapshot run {run_id} has invalid active attempt history alignment")
            }
            Self::InvalidLeasePresence { run_id, state } => {
                write!(f, "snapshot run {run_id} has lease metadata in invalid state {state:?}")
            }
            Self::DependencyReferencesUnknownTask { task_id, prereq_id } => write!(
                f,
                "snapshot dependency declaration for task {task_id} references unknown \
                 prerequisite task {prereq_id}"
            ),
            Self::DependencyDeclarationUnknownTask { task_id } => {
                write!(f, "snapshot dependency declaration for unknown task {task_id}")
            }
            Self::DuplicateDependencyDeclaration { task_id } => {
                write!(f, "snapshot contains duplicate dependency declaration for task {task_id}")
            }
            Self::DuplicateBudgetAllocation { task_id, dimension } => {
                write!(f, "duplicate budget allocation for task {task_id} dimension {dimension}")
            }
            Self::BudgetReferencesUnknownTask { task_id } => {
                write!(f, "budget allocation references unknown task {task_id}")
            }
            Self::DuplicateSubscriptionId { subscription_id } => {
                write!(f, "duplicate subscription id {subscription_id}")
            }
            Self::SubscriptionReferencesUnknownTask { subscription_id, task_id } => {
                write!(f, "subscription {subscription_id} references unknown task {task_id}")
            }
            Self::InvalidEnginePausedCausality => {
                write!(f, "snapshot engine state invalid: paused=true requires paused_at")
            }
            Self::InvalidEngineResumedCausality => {
                write!(
                    f,
                    "snapshot engine state invalid: paused=false with paused_at requires \
                     resumed_at"
                )
            }
            Self::InvalidEnginePauseResumeOrdering { paused_at, resumed_at } => write!(
                f,
                "snapshot engine state invalid: resumed_at ({resumed_at}) < paused_at \
                 ({paused_at})"
            ),
        }
    }
}

impl std::error::Error for SnapshotMappingError {}

/// Maps a canonical core run payload into the snapshot wrapper.
pub fn map_core_run_to_snapshot(run_instance: CoreRunInstance) -> SnapshotRun {
    SnapshotRun { run_instance, state_history: Vec::new(), attempts: Vec::new(), lease: None }
}

/// Maps a snapshot run wrapper into a canonical core run payload.
pub fn map_snapshot_run_to_core(
    snapshot_run: &SnapshotRun,
) -> Result<CoreRunInstance, SnapshotMappingError> {
    validate_core_run_payload(&snapshot_run.run_instance)?;
    validate_snapshot_run_details(snapshot_run)?;
    Ok(snapshot_run.run_instance.clone())
}

/// Validates full snapshot parity and mapping invariants before bootstrap.
pub fn validate_snapshot(snapshot: &Snapshot) -> Result<(), SnapshotMappingError> {
    if snapshot.metadata.schema_version != SNAPSHOT_SCHEMA_VERSION {
        return Err(SnapshotMappingError::UnsupportedSchemaVersion {
            expected: SNAPSHOT_SCHEMA_VERSION,
            found: snapshot.metadata.schema_version,
        });
    }

    let task_count = snapshot.tasks.len() as u64;
    if snapshot.metadata.task_count != task_count {
        return Err(SnapshotMappingError::TaskCountMismatch {
            declared: snapshot.metadata.task_count,
            actual: task_count,
        });
    }

    let run_count = snapshot.runs.len() as u64;
    if snapshot.metadata.run_count != run_count {
        return Err(SnapshotMappingError::RunCountMismatch {
            declared: snapshot.metadata.run_count,
            actual: run_count,
        });
    }

    let mut task_ids = HashSet::new();
    for task in &snapshot.tasks {
        let task_id = task.task_spec.id();
        if !task_ids.insert(task_id) {
            return Err(SnapshotMappingError::DuplicateTaskId { task_id });
        }

        if let Some(canceled_at) = task.canceled_at {
            if canceled_at < task.created_at {
                return Err(SnapshotMappingError::InvalidTaskCanceledAtCausality {
                    task_id,
                    created_at: task.created_at,
                    canceled_at,
                });
            }
        }
    }

    let mut run_ids = HashSet::new();
    for run in &snapshot.runs {
        let core_run = map_snapshot_run_to_core(run)?;
        let run_id = core_run.id();
        let task_id = core_run.task_id();

        if !run_ids.insert(run_id) {
            return Err(SnapshotMappingError::DuplicateRunId { run_id });
        }

        if !task_ids.contains(&task_id) {
            return Err(SnapshotMappingError::RunReferencesUnknownTask { run_id, task_id });
        }
    }

    validate_engine_control(&snapshot.engine)?;

    // Validate dependency declarations.
    let mut dep_task_ids = HashSet::new();
    for decl in &snapshot.dependency_declarations {
        if !dep_task_ids.insert(decl.task_id) {
            return Err(SnapshotMappingError::DuplicateDependencyDeclaration {
                task_id: decl.task_id,
            });
        }
        if !task_ids.contains(&decl.task_id) {
            return Err(SnapshotMappingError::DependencyDeclarationUnknownTask {
                task_id: decl.task_id,
            });
        }
        for &prereq_id in &decl.depends_on {
            if !task_ids.contains(&prereq_id) {
                return Err(SnapshotMappingError::DependencyReferencesUnknownTask {
                    task_id: decl.task_id,
                    prereq_id,
                });
            }
        }
    }

    // Validate budget records.
    let mut budget_keys = HashSet::new();
    for budget in &snapshot.budgets {
        if !budget_keys.insert((budget.task_id, budget.dimension)) {
            return Err(SnapshotMappingError::DuplicateBudgetAllocation {
                task_id: budget.task_id,
                dimension: budget.dimension,
            });
        }
        if !task_ids.contains(&budget.task_id) {
            return Err(SnapshotMappingError::BudgetReferencesUnknownTask {
                task_id: budget.task_id,
            });
        }
    }

    // Validate subscription records.
    let mut sub_ids = HashSet::new();
    for sub in &snapshot.subscriptions {
        if !sub_ids.insert(sub.subscription_id) {
            return Err(SnapshotMappingError::DuplicateSubscriptionId {
                subscription_id: sub.subscription_id,
            });
        }
        if !task_ids.contains(&sub.task_id) {
            return Err(SnapshotMappingError::SubscriptionReferencesUnknownTask {
                subscription_id: sub.subscription_id,
                task_id: sub.task_id,
            });
        }
    }

    // Actor, tenant, role, capability, and ledger records are validated at the
    // platform layer. Basic structural integrity is ensured by WAL ordering;
    // no cross-referencing validation is required here.

    Ok(())
}

fn validate_engine_control(engine: &SnapshotEngineControl) -> Result<(), SnapshotMappingError> {
    if engine.paused && engine.paused_at.is_none() {
        return Err(SnapshotMappingError::InvalidEnginePausedCausality);
    }

    if engine.paused && engine.resumed_at.is_some() {
        return Err(SnapshotMappingError::InvalidEngineResumedCausality);
    }

    if engine.paused_at.is_none() && engine.resumed_at.is_some() {
        return Err(SnapshotMappingError::InvalidEngineResumedCausality);
    }

    if !engine.paused && engine.paused_at.is_some() && engine.resumed_at.is_none() {
        return Err(SnapshotMappingError::InvalidEngineResumedCausality);
    }

    if !engine.paused {
        if let (Some(paused_at), Some(resumed_at)) = (engine.paused_at, engine.resumed_at) {
            if resumed_at < paused_at {
                return Err(SnapshotMappingError::InvalidEnginePauseResumeOrdering {
                    paused_at,
                    resumed_at,
                });
            }
        }
    }

    Ok(())
}

fn validate_core_run_payload(run_instance: &CoreRunInstance) -> Result<(), SnapshotMappingError> {
    let run_id = run_instance.id();
    if run_id.as_uuid().is_nil() {
        return Err(SnapshotMappingError::InvalidRunId { run_id });
    }

    let task_id = run_instance.task_id();
    if task_id.as_uuid().is_nil() {
        return Err(SnapshotMappingError::InvalidTaskId { run_id, task_id });
    }

    if run_instance.state() == RunState::Ready
        && run_instance.scheduled_at() > run_instance.created_at()
    {
        return Err(SnapshotMappingError::InvalidReadyScheduleCausality {
            run_id,
            scheduled_at: run_instance.scheduled_at(),
            created_at: run_instance.created_at(),
        });
    }

    if run_instance.current_attempt_id().is_some()
        && !matches!(run_instance.state(), RunState::Running | RunState::Canceled)
    {
        return Err(SnapshotMappingError::InvalidAttemptLineageState {
            run_id,
            state: run_instance.state(),
        });
    }

    if run_instance.current_attempt_id().is_some() && run_instance.attempt_count() == 0 {
        return Err(SnapshotMappingError::InvalidAttemptLineageCount {
            run_id,
            attempt_count: run_instance.attempt_count(),
        });
    }

    Ok(())
}

fn validate_snapshot_run_details(snapshot_run: &SnapshotRun) -> Result<(), SnapshotMappingError> {
    let run_id = snapshot_run.run_instance.id();
    let created_at = snapshot_run.run_instance.created_at();

    if snapshot_run.state_history.is_empty() {
        return Err(SnapshotMappingError::MissingInitialRunStateHistory { run_id });
    }

    let first = &snapshot_run.state_history[0];
    if first.from.is_some() || first.to != RunState::Scheduled || first.timestamp != created_at {
        return Err(SnapshotMappingError::MissingInitialRunStateHistory { run_id });
    }

    let mut previous_state = RunState::Scheduled;
    for entry in snapshot_run.state_history.iter().skip(1) {
        let from = entry.from.ok_or(SnapshotMappingError::InvalidRunStateHistoryTransition {
            run_id,
            from: previous_state,
            to: entry.to,
        })?;

        if from != previous_state || !is_valid_transition(from, entry.to) {
            return Err(SnapshotMappingError::InvalidRunStateHistoryTransition {
                run_id,
                from,
                to: entry.to,
            });
        }

        previous_state = entry.to;
    }

    if previous_state != snapshot_run.run_instance.state() {
        return Err(SnapshotMappingError::InvalidRunStateHistoryTransition {
            run_id,
            from: previous_state,
            to: snapshot_run.run_instance.state(),
        });
    }

    let attempt_count = snapshot_run.run_instance.attempt_count();
    if snapshot_run.attempts.len() != attempt_count as usize {
        return Err(SnapshotMappingError::InvalidAttemptHistoryCount {
            run_id,
            attempt_count,
            history_len: snapshot_run.attempts.len(),
        });
    }

    if let Some(current_attempt_id) = snapshot_run.run_instance.current_attempt_id() {
        let unfinished: Vec<&SnapshotAttemptHistoryEntry> = snapshot_run
            .attempts
            .iter()
            .filter(|entry| entry.attempt_id == current_attempt_id)
            .collect();
        if unfinished.len() != 1 || unfinished[0].finished_at.is_some() {
            return Err(SnapshotMappingError::InvalidActiveAttemptHistory { run_id });
        }
    }

    if snapshot_run.lease.is_some()
        && !matches!(
            snapshot_run.run_instance.state(),
            RunState::Ready | RunState::Leased | RunState::Running
        )
    {
        return Err(SnapshotMappingError::InvalidLeasePresence {
            run_id,
            state: snapshot_run.run_instance.state(),
        });
    }

    Ok(())
}

/// Maps snapshot run history entries into reducer history entries.
pub fn map_snapshot_run_history(
    entries: Vec<SnapshotRunStateHistoryEntry>,
) -> Vec<crate::recovery::reducer::RunStateHistoryEntry> {
    entries
        .into_iter()
        .map(|entry| crate::recovery::reducer::RunStateHistoryEntry {
            from: entry.from,
            to: entry.to,
            timestamp: entry.timestamp,
        })
        .collect::<Vec<crate::recovery::reducer::RunStateHistoryEntry>>()
}

/// Maps snapshot attempt history entries into reducer attempt history entries.
pub fn map_snapshot_attempt_history(
    entries: Vec<SnapshotAttemptHistoryEntry>,
) -> Vec<crate::recovery::reducer::AttemptHistoryEntry> {
    entries
        .into_iter()
        .map(|entry| crate::recovery::reducer::AttemptHistoryEntry {
            attempt_id: entry.attempt_id,
            started_at: entry.started_at,
            finished_at: entry.finished_at,
            result: entry.result,
            error: entry.error,
            output: entry.output,
        })
        .collect()
}

/// Maps snapshot lease metadata into reducer lease metadata.
pub fn map_snapshot_lease_metadata(
    lease: Option<SnapshotLeaseMetadata>,
) -> Option<crate::recovery::reducer::LeaseMetadata> {
    lease.map(|metadata| crate::recovery::reducer::LeaseMetadata {
        owner: metadata.owner,
        expiry: metadata.expiry,
        acquired_at: metadata.acquired_at,
        updated_at: metadata.updated_at,
    })
}
