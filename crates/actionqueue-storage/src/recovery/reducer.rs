//! Recovery reducer - applies events to reconstruct state.
//!
//! A replay reducer applies WAL events to reconstruct the current state
//! of the ActionQueue system. This is used during recovery to rebuild state
//! from the WAL.
//!
//! Lease replay semantics are strict by design:
//! - lease events must reference a known run,
//! - lease events must satisfy explicit run-state preconditions,
//! - lease mutations must satisfy owner/expiry causality checks,
//! - terminal runs cannot retain active lease projection.

use std::collections::{HashMap, HashSet};

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{ActorId, LedgerEntryId, RunId, TaskId, TenantId};
use actionqueue_core::mutation::{AttemptOutcome, AttemptResultKind};
use actionqueue_core::platform::{Capability, Role};
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_core::run::RunInstanceError;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};

use crate::wal::event::{WalEvent, WalEventType};

/// Projection record for a task with timestamp metadata.
#[derive(Debug, Clone)]
pub struct TaskRecord {
    /// Canonical task specification.
    task_spec: actionqueue_core::task::task_spec::TaskSpec,
    /// Task creation timestamp (Unix epoch seconds).
    created_at: u64,
    /// Task update timestamp (Unix epoch seconds), if any.
    updated_at: Option<u64>,
    /// Task cancellation timestamp (Unix epoch seconds), if canceled.
    canceled_at: Option<u64>,
}

/// A deterministic state transition record for a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStateHistoryEntry {
    /// The previous state, or None for the initial Scheduled entry.
    pub(crate) from: Option<RunState>,
    /// The new state recorded by the WAL event.
    pub(crate) to: RunState,
    /// The timestamp associated with the transition.
    pub(crate) timestamp: u64,
}

impl RunStateHistoryEntry {
    /// Returns the previous run state, if any.
    pub fn from(&self) -> Option<RunState> {
        self.from
    }

    /// Returns the new run state.
    pub fn to(&self) -> RunState {
        self.to
    }

    /// Returns the timestamp of the transition.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// A deterministic attempt lineage record for a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptHistoryEntry {
    /// The attempt identifier.
    pub(crate) attempt_id: actionqueue_core::ids::AttemptId,
    /// The timestamp when the attempt started.
    pub(crate) started_at: u64,
    /// The timestamp when the attempt finished, if finished.
    pub(crate) finished_at: Option<u64>,
    /// Canonical attempt result taxonomy, if finished.
    pub(crate) result: Option<AttemptResultKind>,
    /// Optional error message when the attempt failed.
    pub(crate) error: Option<String>,
    /// Optional opaque output bytes produced by the handler on success.
    pub(crate) output: Option<Vec<u8>>,
}

impl AttemptHistoryEntry {
    /// Returns the attempt identifier.
    pub fn attempt_id(&self) -> actionqueue_core::ids::AttemptId {
        self.attempt_id
    }

    /// Returns the timestamp when the attempt started.
    pub fn started_at(&self) -> u64 {
        self.started_at
    }

    /// Returns the timestamp when the attempt finished, if any.
    pub fn finished_at(&self) -> Option<u64> {
        self.finished_at
    }

    /// Returns canonical attempt result taxonomy, if finished.
    pub fn result(&self) -> Option<AttemptResultKind> {
        self.result
    }

    /// Returns whether the attempt succeeded, if finished.
    ///
    /// This is a compatibility view over [`Self::result`].
    pub fn success(&self) -> Option<bool> {
        self.result.map(|result| matches!(result, AttemptResultKind::Success))
    }

    /// Returns the error message, if any.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Returns the optional opaque output bytes produced by the handler.
    pub fn output(&self) -> Option<&[u8]> {
        self.output.as_deref()
    }
}

/// A deterministic lease metadata record for a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseMetadata {
    /// Lease owner string.
    pub(crate) owner: String,
    /// Lease expiry timestamp.
    pub(crate) expiry: u64,
    /// Timestamp when the lease was acquired.
    pub(crate) acquired_at: u64,
    /// Timestamp when the lease was last updated.
    pub(crate) updated_at: u64,
}

impl LeaseMetadata {
    /// Returns the lease owner.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the lease expiry timestamp.
    pub fn expiry(&self) -> u64 {
        self.expiry
    }

    /// Returns the lease acquisition timestamp.
    pub fn acquired_at(&self) -> u64 {
        self.acquired_at
    }

    /// Returns the lease last-updated timestamp.
    pub fn updated_at(&self) -> u64 {
        self.updated_at
    }
}

/// Actor projection record.
#[derive(Debug, Clone)]
pub struct ActorRecord {
    pub actor_id: ActorId,
    pub identity: String,
    pub capabilities: Vec<String>,
    pub department: Option<String>,
    pub heartbeat_interval_secs: u64,
    pub tenant_id: Option<TenantId>,
    pub registered_at: u64,
    pub last_heartbeat_at: Option<u64>,
    pub deregistered_at: Option<u64>,
}

/// Tenant projection record.
#[derive(Debug, Clone)]
pub struct TenantRecord {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: u64,
}

/// Role assignment record.
#[derive(Debug, Clone)]
pub struct RoleAssignmentRecord {
    pub actor_id: ActorId,
    pub role: Role,
    pub tenant_id: TenantId,
    pub assigned_at: u64,
}

/// Capability grant record.
#[derive(Debug, Clone)]
pub struct CapabilityGrantRecord {
    pub actor_id: ActorId,
    pub capability: Capability,
    pub tenant_id: TenantId,
    pub granted_at: u64,
    pub revoked_at: Option<u64>,
}

/// Ledger entry record.
#[derive(Debug, Clone)]
pub struct LedgerEntryRecord {
    pub entry_id: LedgerEntryId,
    pub tenant_id: TenantId,
    pub ledger_key: String,
    pub actor_id: Option<ActorId>,
    pub payload: Vec<u8>,
    pub timestamp: u64,
}

/// Budget allocation and consumption record.
#[derive(Debug, Clone)]
pub struct BudgetRecord {
    /// The budget dimension this record covers.
    pub dimension: BudgetDimension,
    /// The maximum amount allowed before dispatch is blocked.
    pub limit: u64,
    /// The total amount consumed so far.
    pub consumed: u64,
    /// The timestamp when the budget was allocated.
    pub allocated_at: u64,
    /// Whether the budget has been exhausted (consumed >= limit).
    pub exhausted: bool,
}

/// Subscription state record.
#[derive(Debug, Clone)]
pub struct SubscriptionRecord {
    /// The subscription identifier.
    pub subscription_id: SubscriptionId,
    /// The subscribing task identifier.
    pub task_id: actionqueue_core::ids::TaskId,
    /// The event filter for this subscription.
    pub filter: EventFilter,
    /// The timestamp when the subscription was created.
    pub created_at: u64,
    /// The timestamp when the subscription was last triggered, if any.
    pub triggered_at: Option<u64>,
    /// The timestamp when the subscription was canceled, if any.
    pub canceled_at: Option<u64>,
}

impl TaskRecord {
    /// Returns the canonical task specification.
    pub fn task_spec(&self) -> &actionqueue_core::task::task_spec::TaskSpec {
        &self.task_spec
    }

    /// Returns the task creation timestamp.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Returns the task update timestamp, if any.
    pub fn updated_at(&self) -> Option<u64> {
        self.updated_at
    }

    /// Returns the task cancellation timestamp, if canceled.
    pub fn canceled_at(&self) -> Option<u64> {
        self.canceled_at
    }
}

/// A reducer that applies WAL events to reconstruct state.
#[derive(Debug, Clone)]
pub struct ReplayReducer {
    /// The current state of all runs being replayed.
    runs: HashMap<actionqueue_core::ids::RunId, RunState>,
    /// The current state of all tasks being replayed.
    tasks: HashMap<actionqueue_core::ids::TaskId, TaskRecord>,
    /// The current state of all run instances being replayed.
    run_instances:
        HashMap<actionqueue_core::ids::RunId, actionqueue_core::run::run_instance::RunInstance>,
    /// Secondary index: task_id → run_ids for O(R_task) lookups.
    runs_by_task: HashMap<TaskId, Vec<RunId>>,
    /// Run lifecycle history derived from WAL transitions.
    run_history: HashMap<actionqueue_core::ids::RunId, Vec<RunStateHistoryEntry>>,
    /// Attempt lineage derived from WAL attempt events.
    attempt_history: HashMap<actionqueue_core::ids::RunId, Vec<AttemptHistoryEntry>>,
    /// The active lease projection for runs (`owner`, `expiry`).
    leases: HashMap<actionqueue_core::ids::RunId, (String, u64)>,
    /// Lease metadata derived from WAL lease events.
    lease_metadata: HashMap<actionqueue_core::ids::RunId, LeaseMetadata>,
    /// The latest sequence number processed.
    latest_sequence: u64,
    /// Task canceled projection keyed by task identifier.
    task_canceled_at: HashMap<TaskId, u64>,
    /// Whether engine scheduling/dispatch is currently paused.
    engine_paused: bool,
    /// Timestamp of most recent engine pause, if any.
    engine_paused_at: Option<u64>,
    /// Timestamp of most recent engine resume, if any.
    engine_resumed_at: Option<u64>,
    /// Declared task dependencies: task_id → prerequisite task_ids.
    /// Rebuilt from `DependencyDeclared` WAL events. Used by the dispatch loop
    /// to reconstruct the `DependencyGate` after recovery.
    dependency_declarations: HashMap<TaskId, HashSet<TaskId>>,
    /// Budget allocation and consumption records keyed by (task_id, dimension).
    budgets: HashMap<(actionqueue_core::ids::TaskId, BudgetDimension), BudgetRecord>,
    /// Subscription state records keyed by subscription identifier.
    subscriptions: HashMap<SubscriptionId, SubscriptionRecord>,
    /// Actor projection records keyed by actor identifier.
    actors: HashMap<ActorId, ActorRecord>,
    /// Tenant projection records keyed by tenant identifier.
    tenants: HashMap<TenantId, TenantRecord>,
    /// Role assignment records keyed by (actor_id, tenant_id).
    role_assignments: HashMap<(ActorId, TenantId), RoleAssignmentRecord>,
    /// Capability grant records keyed by (actor_id, capability_key, tenant_id).
    /// Using String as capability key to avoid Hash bound on Capability.
    capability_grants: HashMap<(ActorId, String, TenantId), CapabilityGrantRecord>,
    /// Ledger entries (append-only, ordered by insertion).
    ledger_entries: Vec<LedgerEntryRecord>,
}

impl ReplayReducer {
    /// Creates a new replay reducer.
    pub fn new() -> Self {
        ReplayReducer {
            runs: HashMap::new(),
            tasks: HashMap::new(),
            run_instances: HashMap::new(),
            runs_by_task: HashMap::new(),
            run_history: HashMap::new(),
            attempt_history: HashMap::new(),
            leases: HashMap::new(),
            lease_metadata: HashMap::new(),
            latest_sequence: 0,
            task_canceled_at: HashMap::new(),
            engine_paused: false,
            engine_paused_at: None,
            engine_resumed_at: None,
            dependency_declarations: HashMap::new(),
            budgets: HashMap::new(),
            subscriptions: HashMap::new(),
            actors: HashMap::new(),
            tenants: HashMap::new(),
            role_assignments: HashMap::new(),
            capability_grants: HashMap::new(),
            ledger_entries: Vec::new(),
        }
    }

    /// Returns the current lease state (owner, expiry) for a run by ID.
    /// Returns None if no lease is active for the run.
    pub fn get_lease(&self, run_id: &actionqueue_core::ids::RunId) -> Option<&(String, u64)> {
        self.leases.get(run_id)
    }

    /// Returns the current lease metadata for a run by ID.
    /// Returns None if no lease metadata is active for the run.
    pub fn get_lease_metadata(
        &self,
        run_id: &actionqueue_core::ids::RunId,
    ) -> Option<&LeaseMetadata> {
        self.lease_metadata.get(run_id)
    }

    /// Returns the current state of a run by ID.
    pub fn get_run_state(&self, run_id: &actionqueue_core::ids::RunId) -> Option<&RunState> {
        self.runs.get(run_id)
    }

    /// Returns the current state of a task by ID.
    pub fn get_task(
        &self,
        task_id: &actionqueue_core::ids::TaskId,
    ) -> Option<&actionqueue_core::task::task_spec::TaskSpec> {
        self.tasks.get(task_id).map(TaskRecord::task_spec)
    }

    /// Returns the full task record by ID.
    pub fn get_task_record(&self, task_id: &actionqueue_core::ids::TaskId) -> Option<&TaskRecord> {
        self.tasks.get(task_id)
    }

    /// Returns the current state of a run instance by ID.
    pub fn get_run_instance(
        &self,
        run_id: &actionqueue_core::ids::RunId,
    ) -> Option<&actionqueue_core::run::run_instance::RunInstance> {
        self.run_instances.get(run_id)
    }

    /// Returns a mutable reference to a run instance by ID.
    ///
    /// This is used during snapshot bootstrap to restore attempt state that
    /// cannot be replayed through normal WAL event application.
    pub(crate) fn get_run_instance_mut(
        &mut self,
        run_id: actionqueue_core::ids::RunId,
    ) -> Option<&mut actionqueue_core::run::run_instance::RunInstance> {
        self.run_instances.get_mut(&run_id)
    }

    /// Returns the run state history for a run by ID.
    pub fn get_run_history(
        &self,
        run_id: &actionqueue_core::ids::RunId,
    ) -> Option<&[RunStateHistoryEntry]> {
        self.run_history.get(run_id).map(Vec::as_slice)
    }

    /// Returns the attempt history for a run by ID.
    pub fn get_attempt_history(
        &self,
        run_id: &actionqueue_core::ids::RunId,
    ) -> Option<&[AttemptHistoryEntry]> {
        self.attempt_history.get(run_id).map(Vec::as_slice)
    }

    /// Returns the latest sequence number that has been applied.
    pub fn latest_sequence(&self) -> u64 {
        self.latest_sequence
    }

    /// Returns the number of runs being tracked.
    pub fn run_count(&self) -> usize {
        self.run_instances.len()
    }

    /// Returns the number of tasks being tracked.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Returns an iterator over run instances and their states.
    ///
    /// This method provides access to the run instances projection for
    /// read-only operations like stats collection.
    pub fn run_instances(
        &self,
    ) -> impl Iterator<Item = &actionqueue_core::run::run_instance::RunInstance> {
        self.run_instances.values()
    }

    /// Returns run identifiers owned by the provided task in deterministic order.
    pub fn run_ids_for_task(&self, task_id: TaskId) -> Vec<RunId> {
        let mut run_ids = self.runs_by_task.get(&task_id).cloned().unwrap_or_default();
        run_ids.sort();
        run_ids
    }

    /// Returns run instances for the provided task via the O(R_task) secondary index.
    pub fn runs_for_task(
        &self,
        task_id: TaskId,
    ) -> impl Iterator<Item = &actionqueue_core::run::run_instance::RunInstance> {
        self.runs_by_task
            .get(&task_id)
            .into_iter()
            .flat_map(|ids| ids.iter().filter_map(|id| self.run_instances.get(id)))
    }

    /// Returns true if the task is marked canceled in projection state.
    pub fn is_task_canceled(&self, task_id: TaskId) -> bool {
        self.task_canceled_at.contains_key(&task_id)
    }

    /// Returns the task canceled timestamp, if present.
    pub fn task_canceled_at(&self, task_id: TaskId) -> Option<u64> {
        self.task_canceled_at.get(&task_id).copied()
    }

    /// Returns true when engine scheduling/dispatch is paused in projection state.
    pub fn is_engine_paused(&self) -> bool {
        self.engine_paused
    }

    /// Returns the timestamp of the most recent engine pause event, if any.
    pub fn engine_paused_at(&self) -> Option<u64> {
        self.engine_paused_at
    }

    /// Returns the timestamp of the most recent engine resume event, if any.
    pub fn engine_resumed_at(&self) -> Option<u64> {
        self.engine_resumed_at
    }

    /// Returns the dependency declarations for gate reconstruction at bootstrap.
    ///
    /// Provides raw prerequisite data so the dispatch loop can rebuild the
    /// `DependencyGate` without a circular storage → workflow dependency.
    pub fn dependency_declarations(&self) -> impl Iterator<Item = (TaskId, &HashSet<TaskId>)> + '_ {
        self.dependency_declarations.iter().map(|(task_id, prereqs)| (*task_id, prereqs))
    }

    /// Returns (child_task_id, parent_task_id) pairs for hierarchy reconstruction at bootstrap.
    ///
    /// Derived from the `parent_task_id` field on each `TaskSpec` stored in the projection.
    /// The dispatch loop uses these to rebuild the `HierarchyTracker` without a circular
    /// storage → workflow dependency.
    pub fn parent_child_mappings(&self) -> impl Iterator<Item = (TaskId, TaskId)> + '_ {
        self.tasks.values().filter_map(|tr| {
            tr.task_spec().parent_task_id().map(|parent| (tr.task_spec().id(), parent))
        })
    }

    /// Returns an iterator over task IDs and specs.
    ///
    /// This method provides access to the tasks projection for
    /// read-only operations like stats collection.
    pub fn tasks(&self) -> impl Iterator<Item = &actionqueue_core::task::task_spec::TaskSpec> {
        self.tasks.values().map(TaskRecord::task_spec)
    }

    /// Returns an iterator over task records with timestamp metadata.
    pub fn task_records(&self) -> impl Iterator<Item = &TaskRecord> {
        self.tasks.values()
    }

    /// Applies an event to the current state.
    pub fn apply(&mut self, event: &WalEvent) -> Result<(), ReplayReducerError> {
        // Validate sequence order (must be monotonically increasing).
        // This check also prevents duplicate events since a repeated sequence
        // would not be strictly greater than latest_sequence.
        if event.sequence() <= self.latest_sequence {
            return Err(ReplayReducerError::InvalidTransition);
        }

        match event.event() {
            WalEventType::TaskCreated { task_spec, timestamp } => {
                self.apply_task_created(task_spec, *timestamp)?;
            }
            WalEventType::RunCreated { run_instance } => {
                self.apply_run_created(run_instance)?;
            }
            WalEventType::RunStateChanged { run_id, previous_state, new_state, timestamp } => {
                self.apply_run_state_changed(run_id, previous_state, new_state, *timestamp)?;
            }
            WalEventType::AttemptStarted { run_id, attempt_id, timestamp } => {
                self.apply_attempt_started(run_id, attempt_id, *timestamp)?;
            }
            WalEventType::AttemptFinished {
                run_id,
                attempt_id,
                result,
                error,
                output,
                timestamp,
            } => {
                let outcome =
                    AttemptOutcome::from_raw_parts(*result, error.clone(), output.clone())
                        .unwrap_or_else(|e| {
                            tracing::warn!(
                                "WAL replay: invalid attempt outcome shape: {e}; falling back to \
                                 safe reconstruction"
                            );
                            match result {
                                actionqueue_core::mutation::AttemptResultKind::Success => {
                                    AttemptOutcome::success()
                                }
                                actionqueue_core::mutation::AttemptResultKind::Failure => {
                                    AttemptOutcome::failure(
                                        error
                                            .clone()
                                            .unwrap_or_else(|| "unknown failure".to_string()),
                                    )
                                }
                                actionqueue_core::mutation::AttemptResultKind::Timeout => {
                                    AttemptOutcome::timeout(
                                        error
                                            .clone()
                                            .unwrap_or_else(|| "unknown timeout".to_string()),
                                    )
                                }
                                actionqueue_core::mutation::AttemptResultKind::Suspended => {
                                    AttemptOutcome::suspended()
                                }
                            }
                        });
                self.apply_attempt_finished(run_id, attempt_id, outcome, *timestamp)?;
            }
            WalEventType::TaskCanceled { task_id, timestamp } => {
                self.apply_task_canceled(task_id, *timestamp)?;
            }
            WalEventType::RunCanceled { run_id, timestamp } => {
                self.apply_run_canceled(run_id, *timestamp)?;
            }
            WalEventType::LeaseAcquired { run_id, owner, expiry, timestamp } => {
                self.apply_lease_acquired(run_id, owner.clone(), *expiry, *timestamp)?;
            }
            WalEventType::LeaseHeartbeat { run_id, owner, expiry, timestamp } => {
                self.apply_lease_heartbeat(run_id, owner.clone(), *expiry, *timestamp)?;
            }
            WalEventType::LeaseExpired { run_id, owner, expiry, timestamp } => {
                self.apply_lease_expired(run_id, owner.clone(), *expiry, *timestamp)?;
            }
            WalEventType::LeaseReleased { run_id, owner, expiry, timestamp } => {
                self.apply_lease_released(run_id, owner.clone(), *expiry, *timestamp)?;
            }
            WalEventType::EnginePaused { timestamp } => {
                self.apply_engine_paused(*timestamp)?;
            }
            WalEventType::EngineResumed { timestamp } => {
                self.apply_engine_resumed(*timestamp)?;
            }
            WalEventType::DependencyDeclared { task_id, depends_on, .. } => {
                self.apply_dependency_declared(*task_id, depends_on);
            }
            WalEventType::RunSuspended { run_id, reason: _, timestamp } => {
                self.apply_run_state_changed(
                    run_id,
                    &RunState::Running,
                    &RunState::Suspended,
                    *timestamp,
                )?;
            }
            WalEventType::RunResumed { run_id, timestamp } => {
                self.apply_run_state_changed(
                    run_id,
                    &RunState::Suspended,
                    &RunState::Ready,
                    *timestamp,
                )?;
            }
            WalEventType::BudgetAllocated { task_id, dimension, limit, timestamp } => {
                self.apply_budget_allocated(*task_id, *dimension, *limit, *timestamp);
            }
            WalEventType::BudgetConsumed { task_id, dimension, amount, timestamp } => {
                self.apply_budget_consumed(*task_id, *dimension, *amount, *timestamp);
            }
            WalEventType::BudgetExhausted { task_id, dimension, .. } => {
                // Exhaustion is normally derived from consumption (consumed >= limit).
                // This handler exists for snapshot bootstrap: if a snapshot records
                // exhausted=true with consumed<limit, the bootstrap synthesizes this
                // event to restore the flag.
                if let Some(record) = self.budgets.get_mut(&(*task_id, *dimension)) {
                    record.exhausted = true;
                }
            }
            WalEventType::BudgetReplenished { task_id, dimension, new_limit, timestamp } => {
                self.apply_budget_replenished(*task_id, *dimension, *new_limit, *timestamp);
            }
            WalEventType::SubscriptionCreated { subscription_id, task_id, filter, timestamp } => {
                self.apply_subscription_created(
                    *subscription_id,
                    *task_id,
                    filter.clone(),
                    *timestamp,
                );
            }
            WalEventType::SubscriptionTriggered { subscription_id, timestamp } => {
                self.apply_subscription_triggered(*subscription_id, *timestamp);
            }
            WalEventType::SubscriptionCanceled { subscription_id, timestamp } => {
                self.apply_subscription_canceled(*subscription_id, *timestamp);
            }

            // ── WAL v5: Actor events ──────────────────────────────────────
            WalEventType::ActorRegistered {
                actor_id,
                identity,
                capabilities,
                department,
                heartbeat_interval_secs,
                tenant_id,
                timestamp,
            } => {
                self.actors.insert(
                    *actor_id,
                    ActorRecord {
                        actor_id: *actor_id,
                        identity: identity.clone(),
                        capabilities: capabilities.clone(),
                        department: department.clone(),
                        heartbeat_interval_secs: *heartbeat_interval_secs,
                        tenant_id: *tenant_id,
                        registered_at: *timestamp,
                        last_heartbeat_at: None,
                        deregistered_at: None,
                    },
                );
            }
            WalEventType::ActorDeregistered { actor_id, timestamp } => {
                if let Some(record) = self.actors.get_mut(actor_id) {
                    record.deregistered_at = Some(*timestamp);
                }
            }
            WalEventType::ActorHeartbeat { actor_id, timestamp } => {
                if let Some(record) = self.actors.get_mut(actor_id) {
                    record.last_heartbeat_at = Some(*timestamp);
                }
            }

            // ── WAL v5: Platform events ───────────────────────────────────
            WalEventType::TenantCreated { tenant_id, name, timestamp } => {
                self.tenants.insert(
                    *tenant_id,
                    TenantRecord {
                        tenant_id: *tenant_id,
                        name: name.clone(),
                        created_at: *timestamp,
                    },
                );
            }
            WalEventType::RoleAssigned { actor_id, role, tenant_id, timestamp } => {
                self.role_assignments.insert(
                    (*actor_id, *tenant_id),
                    RoleAssignmentRecord {
                        actor_id: *actor_id,
                        role: role.clone(),
                        tenant_id: *tenant_id,
                        assigned_at: *timestamp,
                    },
                );
            }
            WalEventType::CapabilityGranted { actor_id, capability, tenant_id, timestamp } => {
                let key = capability_key(capability);
                self.capability_grants.insert(
                    (*actor_id, key.clone(), *tenant_id),
                    CapabilityGrantRecord {
                        actor_id: *actor_id,
                        capability: capability.clone(),
                        tenant_id: *tenant_id,
                        granted_at: *timestamp,
                        revoked_at: None,
                    },
                );
            }
            WalEventType::CapabilityRevoked { actor_id, capability, tenant_id, timestamp } => {
                let key = capability_key(capability);
                if let Some(record) = self.capability_grants.get_mut(&(*actor_id, key, *tenant_id))
                {
                    record.revoked_at = Some(*timestamp);
                }
            }
            WalEventType::LedgerEntryAppended {
                entry_id,
                tenant_id,
                ledger_key,
                actor_id,
                payload,
                timestamp,
            } => {
                self.ledger_entries.push(LedgerEntryRecord {
                    entry_id: *entry_id,
                    tenant_id: *tenant_id,
                    ledger_key: ledger_key.clone(),
                    actor_id: *actor_id,
                    payload: payload.clone(),
                    timestamp: *timestamp,
                });
            }
        }

        // Commit bookkeeping only after semantic application succeeds.
        self.latest_sequence = event.sequence();

        Ok(())
    }

    fn apply_task_created(
        &mut self,
        task_spec: &actionqueue_core::task::task_spec::TaskSpec,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        let task_id = task_spec.id();
        if self.tasks.contains_key(&task_id) {
            return Err(ReplayReducerError::DuplicateEvent);
        }
        self.tasks.insert(
            task_id,
            TaskRecord {
                task_spec: task_spec.clone(),
                created_at: timestamp,
                updated_at: None,
                canceled_at: None,
            },
        );
        Ok(())
    }

    fn apply_task_canceled(
        &mut self,
        task_id: &TaskId,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        let task_record = self.tasks.get_mut(task_id).ok_or(ReplayReducerError::TaskCausality(
            TaskCausalityError::UnknownTask { task_id: *task_id },
        ))?;

        if let Some(existing_timestamp) = task_record.canceled_at {
            return Err(ReplayReducerError::TaskCausality(TaskCausalityError::AlreadyCanceled {
                task_id: *task_id,
                first_canceled_at: existing_timestamp,
                duplicate_canceled_at: timestamp,
            }));
        }

        task_record.canceled_at = Some(timestamp);
        self.task_canceled_at.insert(*task_id, timestamp);
        Ok(())
    }

    fn apply_run_created(
        &mut self,
        run_instance: &actionqueue_core::run::run_instance::RunInstance,
    ) -> Result<(), ReplayReducerError> {
        let run_id = run_instance.id();
        if self.runs.contains_key(&run_id) {
            return Err(ReplayReducerError::DuplicateEvent);
        }
        // Verify run state is valid (must start in Scheduled state)
        if run_instance.state() != RunState::Scheduled {
            return Err(ReplayReducerError::InvalidTransition);
        }
        let task_id = run_instance.task_id();
        self.runs.insert(run_id, run_instance.state());
        self.run_instances.insert(run_id, run_instance.clone());
        self.runs_by_task.entry(task_id).or_default().push(run_id);
        self.run_history.insert(
            run_id,
            vec![RunStateHistoryEntry {
                from: None,
                to: RunState::Scheduled,
                timestamp: run_instance.created_at(),
            }],
        );
        self.attempt_history.insert(run_id, Vec::new());
        Ok(())
    }

    fn apply_run_state_changed(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        previous_state: &RunState,
        new_state: &RunState,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        // Check that the run exists
        let current_state = self.runs.get(run_id).ok_or(ReplayReducerError::InvalidTransition)?;

        // Verify the transition matches what was recorded
        if *current_state != *previous_state {
            return Err(ReplayReducerError::InvalidTransition);
        }

        // Verify the transition is valid according to run transitions
        if !is_valid_transition(*previous_state, *new_state) {
            return Err(ReplayReducerError::InvalidTransition);
        }

        // Attempt lifecycle integrity:
        // - entering Running requires no active attempt yet
        // - leaving Running requires the active attempt to be closed first
        if let Some(run_instance) = self.run_instances.get(run_id) {
            if run_instance.state() != *current_state {
                return Err(ReplayReducerError::CorruptedData);
            }

            if *new_state == RunState::Running && run_instance.current_attempt_id().is_some() {
                return Err(ReplayReducerError::InvalidTransition);
            }

            if *current_state == RunState::Running
                && *new_state != RunState::Running
                && *new_state != RunState::Canceled
                && run_instance.current_attempt_id().is_some()
            {
                return Err(ReplayReducerError::InvalidTransition);
            }
        }

        // Update the run state
        self.runs.insert(*run_id, *new_state);

        // Update the run instance state if present
        if let Some(run_instance) = self.run_instances.get_mut(run_id) {
            run_instance.transition_to(*new_state).map_err(Self::map_run_instance_error)?;
            run_instance.record_state_change_at(timestamp);
        }

        let history = self.run_history.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;
        history.push(RunStateHistoryEntry {
            from: Some(*previous_state),
            to: *new_state,
            timestamp,
        });

        self.clear_lease_projection_if_terminal(run_id, *new_state)?;

        Ok(())
    }

    fn apply_attempt_started(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        attempt_id: &actionqueue_core::ids::AttemptId,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        // Verify the run exists and is actively running.
        let current_state = self.runs.get(run_id).ok_or(ReplayReducerError::InvalidTransition)?;
        if *current_state != RunState::Running {
            return Err(ReplayReducerError::InvalidTransition);
        }

        let run_instance =
            self.run_instances.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;

        if run_instance.state() != RunState::Running {
            return Err(ReplayReducerError::CorruptedData);
        }

        if run_instance.current_attempt_id().is_some() {
            return Err(ReplayReducerError::InvalidTransition);
        }

        run_instance.start_attempt(*attempt_id).map_err(Self::map_run_instance_error)?;

        let attempts = self.attempt_history.entry(*run_id).or_default();
        attempts.push(AttemptHistoryEntry {
            attempt_id: *attempt_id,
            started_at: timestamp,
            finished_at: None,
            result: None,
            error: None,
            output: None,
        });

        Ok(())
    }

    fn apply_attempt_finished(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        attempt_id: &actionqueue_core::ids::AttemptId,
        outcome: AttemptOutcome,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        // Verify the run exists and is actively running.
        let current_state = self.runs.get(run_id).ok_or(ReplayReducerError::InvalidTransition)?;
        if *current_state != RunState::Running {
            return Err(ReplayReducerError::InvalidTransition);
        }

        let run_instance =
            self.run_instances.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;

        if run_instance.state() != RunState::Running {
            return Err(ReplayReducerError::CorruptedData);
        }

        run_instance.finish_attempt(*attempt_id).map_err(Self::map_run_instance_error)?;

        let attempts =
            self.attempt_history.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;
        let entry = attempts
            .iter_mut()
            .find(|entry| entry.attempt_id == *attempt_id)
            .ok_or(ReplayReducerError::CorruptedData)?;

        if entry.finished_at.is_some() {
            return Err(ReplayReducerError::CorruptedData);
        }

        entry.finished_at = Some(timestamp);
        let (result_kind, error_detail, output) = outcome.into_parts();
        entry.result = Some(result_kind);
        entry.error = error_detail;
        entry.output = output;

        Ok(())
    }

    fn apply_run_canceled(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        let current_state = self.runs.get(run_id).ok_or(ReplayReducerError::InvalidTransition)?;
        let previous_state = *current_state;

        // Cancellation must be a valid transition from the current state.
        if !is_valid_transition(previous_state, RunState::Canceled) {
            return Err(ReplayReducerError::InvalidTransition);
        }

        self.runs.insert(*run_id, RunState::Canceled);

        let run_instance =
            self.run_instances.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;
        run_instance.transition_to(RunState::Canceled).map_err(Self::map_run_instance_error)?;
        run_instance.record_state_change_at(timestamp);

        let history = self.run_history.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;
        history.push(RunStateHistoryEntry {
            from: Some(previous_state),
            to: RunState::Canceled,
            timestamp,
        });

        self.clear_lease_projection_if_terminal(run_id, RunState::Canceled)?;

        Ok(())
    }

    fn apply_lease_acquired(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        owner: String,
        expiry: u64,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        self.validate_lease_run_precondition(run_id, LeaseEventKind::Acquire)?;

        if self.leases.contains_key(run_id) {
            return Err(ReplayReducerError::LeaseCausality(
                LeaseCausalityError::LeaseAlreadyActive { run_id: *run_id },
            ));
        }

        let metadata_owner = owner.clone();
        self.leases.insert(*run_id, (owner, expiry));
        self.lease_metadata.insert(
            *run_id,
            LeaseMetadata {
                owner: metadata_owner,
                expiry,
                acquired_at: timestamp,
                updated_at: timestamp,
            },
        );
        Ok(())
    }

    fn apply_lease_heartbeat(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        owner: String,
        expiry: u64,
        timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        self.validate_lease_run_precondition(run_id, LeaseEventKind::Heartbeat)?;

        let (current_owner, current_expiry) =
            self.active_lease_snapshot(run_id, LeaseEventKind::Heartbeat)?;

        if owner != current_owner {
            return Err(ReplayReducerError::LeaseCausality(LeaseCausalityError::OwnerMismatch {
                run_id: *run_id,
                event: LeaseEventKind::Heartbeat,
                expected_owner: current_owner,
                actual_owner: owner,
            }));
        }

        if expiry < current_expiry {
            return Err(ReplayReducerError::LeaseCausality(
                LeaseCausalityError::NonMonotonicHeartbeatExpiry {
                    run_id: *run_id,
                    previous_expiry: current_expiry,
                    proposed_expiry: expiry,
                },
            ));
        }

        self.leases.insert(*run_id, (current_owner, expiry));
        let metadata =
            self.lease_metadata.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;
        metadata.expiry = expiry;
        metadata.updated_at = timestamp;
        Ok(())
    }

    fn apply_lease_expired(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        owner: String,
        expiry: u64,
        _timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        let current_state = self.validate_lease_run_precondition(run_id, LeaseEventKind::Expire)?;

        self.validate_exact_lease_metadata_match(run_id, LeaseEventKind::Expire, &owner, expiry)?;

        self.leases.remove(run_id);
        self.lease_metadata.remove(run_id);

        if current_state == RunState::Leased {
            self.transition_run_to_ready_after_lease_close(run_id)?;
        }

        Ok(())
    }

    fn apply_lease_released(
        &mut self,
        run_id: &actionqueue_core::ids::RunId,
        owner: String,
        expiry: u64,
        _timestamp: u64,
    ) -> Result<(), ReplayReducerError> {
        let current_state =
            self.validate_lease_run_precondition(run_id, LeaseEventKind::Release)?;

        self.validate_exact_lease_metadata_match(run_id, LeaseEventKind::Release, &owner, expiry)?;

        self.leases.remove(run_id);
        self.lease_metadata.remove(run_id);

        if current_state == RunState::Leased {
            self.transition_run_to_ready_after_lease_close(run_id)?;
        }

        Ok(())
    }

    /// Validates that a lease event references a known run and that the run is in a state
    /// allowed for the lease event kind.
    fn validate_lease_run_precondition(
        &self,
        run_id: &RunId,
        event: LeaseEventKind,
    ) -> Result<RunState, ReplayReducerError> {
        let run_state =
            self.runs.get(run_id).copied().ok_or(ReplayReducerError::LeaseCausality(
                LeaseCausalityError::UnknownRun { run_id: *run_id, event },
            ))?;

        if !Self::is_allowed_lease_event_state(event, run_state) {
            return Err(ReplayReducerError::LeaseCausality(LeaseCausalityError::InvalidRunState {
                run_id: *run_id,
                event,
                state: run_state,
            }));
        }

        Ok(run_state)
    }

    /// Returns true when `run_state` is an allowed precondition for the lease `event`.
    fn is_allowed_lease_event_state(event: LeaseEventKind, run_state: RunState) -> bool {
        match event {
            LeaseEventKind::Acquire => matches!(run_state, RunState::Ready | RunState::Leased),
            LeaseEventKind::Heartbeat => matches!(run_state, RunState::Leased | RunState::Running),
            LeaseEventKind::Expire | LeaseEventKind::Release => {
                matches!(run_state, RunState::Ready | RunState::Leased | RunState::Running)
            }
        }
    }

    /// Returns a cloned snapshot of the current active lease metadata for a run.
    fn active_lease_snapshot(
        &self,
        run_id: &RunId,
        event: LeaseEventKind,
    ) -> Result<(String, u64), ReplayReducerError> {
        self.leases.get(run_id).cloned().ok_or(ReplayReducerError::LeaseCausality(
            LeaseCausalityError::MissingActiveLease { run_id: *run_id, event },
        ))
    }

    /// Requires exact owner and expiry equality between lease event metadata and projection.
    fn validate_exact_lease_metadata_match(
        &self,
        run_id: &RunId,
        event: LeaseEventKind,
        owner: &str,
        expiry: u64,
    ) -> Result<(), ReplayReducerError> {
        let (expected_owner, expected_expiry) = self.active_lease_snapshot(run_id, event)?;

        if owner != expected_owner {
            return Err(ReplayReducerError::LeaseCausality(LeaseCausalityError::OwnerMismatch {
                run_id: *run_id,
                event,
                expected_owner,
                actual_owner: owner.to_string(),
            }));
        }

        if expiry != expected_expiry {
            return Err(ReplayReducerError::LeaseCausality(LeaseCausalityError::ExpiryMismatch {
                run_id: *run_id,
                event,
                expected_expiry,
                actual_expiry: expiry,
            }));
        }

        Ok(())
    }

    /// Transitions the run projection from `Leased` to `Ready` after a lease close event.
    fn transition_run_to_ready_after_lease_close(
        &mut self,
        run_id: &RunId,
    ) -> Result<(), ReplayReducerError> {
        let run_instance =
            self.run_instances.get_mut(run_id).ok_or(ReplayReducerError::CorruptedData)?;

        if run_instance.state() != RunState::Leased {
            return Err(ReplayReducerError::CorruptedData);
        }

        run_instance.transition_to(RunState::Ready).map_err(Self::map_run_instance_error)?;
        self.runs.insert(*run_id, RunState::Ready);
        Ok(())
    }

    fn apply_engine_paused(&mut self, timestamp: u64) -> Result<(), ReplayReducerError> {
        if self.engine_paused {
            return Err(ReplayReducerError::EngineControlCausality(
                EngineControlCausalityError::AlreadyPaused {
                    first_paused_at: self.engine_paused_at,
                    duplicate_paused_at: timestamp,
                },
            ));
        }

        self.engine_paused = true;
        self.engine_paused_at = Some(timestamp);
        self.engine_resumed_at = None;
        Ok(())
    }

    fn apply_engine_resumed(&mut self, timestamp: u64) -> Result<(), ReplayReducerError> {
        if !self.engine_paused {
            return Err(ReplayReducerError::EngineControlCausality(
                EngineControlCausalityError::NotPaused { attempted_resumed_at: timestamp },
            ));
        }

        if let Some(paused_at) = self.engine_paused_at {
            if timestamp < paused_at {
                return Err(ReplayReducerError::EngineControlCausality(
                    EngineControlCausalityError::ResumeBeforePause {
                        paused_at,
                        resumed_at: timestamp,
                    },
                ));
            }
        }

        self.engine_paused = false;
        self.engine_resumed_at = Some(timestamp);
        Ok(())
    }

    fn apply_dependency_declared(&mut self, task_id: TaskId, depends_on: &[TaskId]) {
        if !self.tasks.contains_key(&task_id) {
            tracing::warn!(
                %task_id,
                "dependency declaration for unknown task during WAL replay"
            );
        }
        for prereq in depends_on {
            if !self.tasks.contains_key(prereq) {
                tracing::warn!(
                    %task_id,
                    prereq_id = %prereq,
                    "dependency prerequisite references unknown task during WAL replay"
                );
            }
        }
        // NOTE: infallible by design — dependency accumulation cannot fail
        let entry = self.dependency_declarations.entry(task_id).or_default();
        for &prereq in depends_on {
            entry.insert(prereq);
        }
    }

    /// Clears active lease projection whenever a run enters terminal state and asserts that
    /// terminal/lease coexistence is impossible post-apply.
    fn clear_lease_projection_if_terminal(
        &mut self,
        run_id: &RunId,
        new_state: RunState,
    ) -> Result<(), ReplayReducerError> {
        if !new_state.is_terminal() {
            return Ok(());
        }

        self.leases.remove(run_id);
        self.lease_metadata.remove(run_id);

        Ok(())
    }

    /// Removes history entries for runs that are in terminal state (Completed, Failed, Canceled).
    ///
    /// This should be called after a snapshot write successfully captures the current state,
    /// since the snapshot will contain the full history for these terminal runs.
    /// Active (non-terminal) run histories are preserved intact.
    pub fn trim_terminal_history(&mut self) {
        let terminal_run_ids: Vec<RunId> =
            self.runs.iter().filter(|(_, state)| state.is_terminal()).map(|(id, _)| *id).collect();

        for run_id in &terminal_run_ids {
            self.run_history.remove(run_id);
            self.attempt_history.remove(run_id);
            self.lease_metadata.remove(run_id);
        }
    }

    /// Seeds the run history for a run from validated snapshot data.
    pub(crate) fn set_run_history(&mut self, run_id: RunId, history: Vec<RunStateHistoryEntry>) {
        self.run_history.insert(run_id, history);
    }

    /// Seeds the attempt history for a run from validated snapshot data.
    pub(crate) fn set_attempt_history(
        &mut self,
        run_id: RunId,
        attempts: Vec<AttemptHistoryEntry>,
    ) {
        self.attempt_history.insert(run_id, attempts);
    }

    /// Seeds both the active lease projection and lease metadata from snapshot data.
    ///
    /// During snapshot bootstrap, `set_lease_metadata` alone is insufficient because
    /// `get_lease()` queries the `leases` map (not `lease_metadata`). This method
    /// populates both maps so that post-bootstrap lease queries (e.g. heartbeat
    /// causality checks) find the correct owner/expiry state.
    pub(crate) fn set_lease_for_bootstrap(&mut self, run_id: RunId, metadata: LeaseMetadata) {
        self.leases.insert(run_id, (metadata.owner.clone(), metadata.expiry));
        self.lease_metadata.insert(run_id, metadata);
    }

    /// Sets reducer sequence during trusted bootstrap hydration.
    pub(crate) fn set_latest_sequence_for_bootstrap(&mut self, sequence: u64) {
        self.latest_sequence = sequence;
    }

    fn map_run_instance_error(error: RunInstanceError) -> ReplayReducerError {
        match error {
            RunInstanceError::AttemptCountOverflow { .. } => ReplayReducerError::CorruptedData,
            _ => ReplayReducerError::InvalidTransition,
        }
    }

    /// Returns an iterator over all budget records.
    pub fn budgets(
        &self,
    ) -> impl Iterator<Item = (&(actionqueue_core::ids::TaskId, BudgetDimension), &BudgetRecord)>
    {
        self.budgets.iter()
    }

    /// Returns the budget record for a specific (task, dimension) pair, if any.
    pub fn get_budget(
        &self,
        task_id: &actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
    ) -> Option<&BudgetRecord> {
        self.budgets.get(&(*task_id, dimension))
    }

    /// Returns true when the specified budget has been exhausted.
    pub fn is_budget_exhausted(
        &self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
    ) -> bool {
        self.budgets.get(&(task_id, dimension)).is_some_and(|r| r.exhausted)
    }

    /// Returns true when a budget allocation exists for the specified (task, dimension) pair.
    pub fn budget_allocation_exists(
        &self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
    ) -> bool {
        self.budgets.contains_key(&(task_id, dimension))
    }

    /// Returns an iterator over all subscription records.
    pub fn subscriptions(&self) -> impl Iterator<Item = (&SubscriptionId, &SubscriptionRecord)> {
        self.subscriptions.iter()
    }

    /// Returns the subscription record for the given identifier, if any.
    pub fn get_subscription(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<&SubscriptionRecord> {
        self.subscriptions.get(subscription_id)
    }

    /// Returns true when the specified subscription exists in the projection.
    pub fn subscription_exists(&self, subscription_id: SubscriptionId) -> bool {
        self.subscriptions.contains_key(&subscription_id)
    }

    /// Returns true when the specified subscription has been canceled.
    pub fn is_subscription_canceled(&self, subscription_id: SubscriptionId) -> bool {
        self.subscriptions.get(&subscription_id).is_some_and(|r| r.canceled_at.is_some())
    }

    /// Returns an iterator over all actor records.
    pub fn actors(&self) -> impl Iterator<Item = (&ActorId, &ActorRecord)> {
        self.actors.iter()
    }

    /// Returns the actor record for the given identifier, if any.
    pub fn get_actor(&self, actor_id: &ActorId) -> Option<&ActorRecord> {
        self.actors.get(actor_id)
    }

    /// Returns true when the actor is registered and active.
    pub fn is_actor_active(&self, actor_id: ActorId) -> bool {
        self.actors.get(&actor_id).is_some_and(|r| r.deregistered_at.is_none())
    }

    /// Returns an iterator over all tenant records.
    pub fn tenants(&self) -> impl Iterator<Item = (&TenantId, &TenantRecord)> {
        self.tenants.iter()
    }

    /// Returns the tenant record for the given identifier, if any.
    pub fn get_tenant(&self, tenant_id: &TenantId) -> Option<&TenantRecord> {
        self.tenants.get(tenant_id)
    }

    /// Returns true when the tenant exists in the projection.
    pub fn tenant_exists(&self, tenant_id: TenantId) -> bool {
        self.tenants.contains_key(&tenant_id)
    }

    /// Returns the role assigned to an actor in a tenant, if any.
    pub fn get_role_assignment(
        &self,
        actor_id: ActorId,
        tenant_id: TenantId,
    ) -> Option<&RoleAssignmentRecord> {
        self.role_assignments.get(&(actor_id, tenant_id))
    }

    /// Returns true when an actor has a given capability in a tenant.
    pub fn actor_has_capability(
        &self,
        actor_id: ActorId,
        capability_key: &str,
        tenant_id: TenantId,
    ) -> bool {
        let key = (actor_id, capability_key.to_string(), tenant_id);
        self.capability_grants.get(&key).is_some_and(|r| r.revoked_at.is_none())
    }

    /// Returns an iterator over all ledger entry records.
    /// Returns an iterator over all role assignment records.
    pub fn role_assignments(&self) -> impl Iterator<Item = &RoleAssignmentRecord> {
        self.role_assignments.values()
    }

    /// Returns an iterator over all capability grant records.
    pub fn capability_grants(&self) -> impl Iterator<Item = &CapabilityGrantRecord> {
        self.capability_grants.values()
    }

    pub fn ledger_entries(&self) -> impl Iterator<Item = &LedgerEntryRecord> {
        self.ledger_entries.iter()
    }

    fn apply_budget_allocated(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
        limit: u64,
        timestamp: u64,
    ) {
        self.budgets.insert(
            (task_id, dimension),
            BudgetRecord {
                dimension,
                limit,
                consumed: 0,
                allocated_at: timestamp,
                exhausted: false,
            },
        );
    }

    fn apply_budget_consumed(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
        amount: u64,
        _timestamp: u64,
    ) {
        if let Some(record) = self.budgets.get_mut(&(task_id, dimension)) {
            record.consumed = record.consumed.saturating_add(amount);
            if record.consumed >= record.limit {
                record.exhausted = true;
            }
        }
    }

    // NOTE: `BudgetExhausted` WAL events are never appended to the WAL on disk.
    // Exhaustion is normally a derived projection: `apply_budget_consumed` sets
    // `exhausted = true` when `consumed >= limit`. The `BudgetExhausted` event
    // may be synthesized during snapshot bootstrap for reducer replay (see
    // `bootstrap.rs`) to restore the flag when consumed < limit.

    fn apply_budget_replenished(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: BudgetDimension,
        new_limit: u64,
        _timestamp: u64,
    ) {
        if let Some(record) = self.budgets.get_mut(&(task_id, dimension)) {
            record.limit = new_limit;
            record.consumed = 0;
            record.exhausted = false;
        }
    }

    fn apply_subscription_created(
        &mut self,
        subscription_id: SubscriptionId,
        task_id: actionqueue_core::ids::TaskId,
        filter: EventFilter,
        timestamp: u64,
    ) {
        self.subscriptions.insert(
            subscription_id,
            SubscriptionRecord {
                subscription_id,
                task_id,
                filter,
                created_at: timestamp,
                triggered_at: None,
                canceled_at: None,
            },
        );
    }

    fn apply_subscription_triggered(&mut self, subscription_id: SubscriptionId, timestamp: u64) {
        if let Some(record) = self.subscriptions.get_mut(&subscription_id) {
            record.triggered_at = Some(timestamp);
        }
    }

    fn apply_subscription_canceled(&mut self, subscription_id: SubscriptionId, timestamp: u64) {
        if let Some(record) = self.subscriptions.get_mut(&subscription_id) {
            record.canceled_at = Some(timestamp);
        }
    }
}

impl Default for ReplayReducer {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during replay reduction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayReducerError {
    /// Invalid state transition during replay.
    InvalidTransition,
    /// Duplicate event detected.
    DuplicateEvent,
    /// Corrupted event data.
    CorruptedData,
    /// Lease replay causality invariant violation.
    LeaseCausality(LeaseCausalityError),
    /// Task replay causality invariant violation.
    TaskCausality(TaskCausalityError),
    /// Engine control replay causality invariant violation.
    EngineControlCausality(EngineControlCausalityError),
}

/// Typed engine control replay causality failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineControlCausalityError {
    /// Engine pause event was applied while already paused.
    AlreadyPaused {
        /// Timestamp of previously applied pause event, if known.
        first_paused_at: Option<u64>,
        /// Timestamp carried by duplicate pause event.
        duplicate_paused_at: u64,
    },
    /// Engine resume event was applied while engine is not paused.
    NotPaused {
        /// Timestamp carried by invalid resume event.
        attempted_resumed_at: u64,
    },
    /// Engine resume timestamp violated pause->resume ordering.
    ResumeBeforePause {
        /// Timestamp of currently active pause event.
        paused_at: u64,
        /// Timestamp carried by resume event.
        resumed_at: u64,
    },
}

impl std::fmt::Display for EngineControlCausalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineControlCausalityError::AlreadyPaused { first_paused_at, duplicate_paused_at } => {
                if let Some(first_paused_at) = first_paused_at {
                    write!(
                        f,
                        "engine pause rejected: already paused at {first_paused_at}, duplicate \
                         timestamp {duplicate_paused_at}"
                    )
                } else {
                    write!(
                        f,
                        "engine pause rejected: already paused, duplicate timestamp \
                         {duplicate_paused_at}"
                    )
                }
            }
            EngineControlCausalityError::NotPaused { attempted_resumed_at } => {
                write!(
                    f,
                    "engine resume rejected: engine not paused at timestamp {attempted_resumed_at}"
                )
            }
            EngineControlCausalityError::ResumeBeforePause { paused_at, resumed_at } => {
                write!(
                    f,
                    "engine resume rejected: resumed_at {resumed_at} precedes paused_at \
                     {paused_at}"
                )
            }
        }
    }
}

/// Typed task replay causality failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskCausalityError {
    /// Task cancellation references a task ID that has not been created.
    UnknownTask {
        /// Task ID referenced by the event.
        task_id: TaskId,
    },
    /// Task cancellation for a task that is already canceled.
    AlreadyCanceled {
        /// Canceled task identifier.
        task_id: TaskId,
        /// First applied cancellation timestamp.
        first_canceled_at: u64,
        /// Duplicate cancellation timestamp from the current event.
        duplicate_canceled_at: u64,
    },
}

impl std::fmt::Display for TaskCausalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskCausalityError::UnknownTask { task_id } => {
                write!(f, "task canceled rejected: unknown task {task_id}")
            }
            TaskCausalityError::AlreadyCanceled {
                task_id,
                first_canceled_at,
                duplicate_canceled_at,
            } => {
                write!(
                    f,
                    "task canceled rejected for task {task_id}: already canceled at \
                     {first_canceled_at}, duplicate timestamp {duplicate_canceled_at}"
                )
            }
        }
    }
}

/// Lease event kinds used by typed causality errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEventKind {
    /// Lease acquired event.
    Acquire,
    /// Lease heartbeat event.
    Heartbeat,
    /// Lease expired event.
    Expire,
    /// Lease released event.
    Release,
}

impl std::fmt::Display for LeaseEventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseEventKind::Acquire => write!(f, "lease acquire"),
            LeaseEventKind::Heartbeat => write!(f, "lease heartbeat"),
            LeaseEventKind::Expire => write!(f, "lease expire"),
            LeaseEventKind::Release => write!(f, "lease release"),
        }
    }
}

/// Typed lease replay causality failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseCausalityError {
    /// Lease event references a run ID that has not been created.
    UnknownRun {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Lease event kind being validated.
        event: LeaseEventKind,
    },
    /// Lease event is not allowed from the run's current state.
    InvalidRunState {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Lease event kind being validated.
        event: LeaseEventKind,
        /// Current run state at validation time.
        state: RunState,
    },
    /// Lease mutation event requires an active lease projection but none exists.
    MissingActiveLease {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Lease event kind being validated.
        event: LeaseEventKind,
    },
    /// Lease acquire encountered an already-active lease for the run.
    LeaseAlreadyActive {
        /// Run ID with an existing active lease.
        run_id: RunId,
    },
    /// Lease event owner did not match active lease owner.
    OwnerMismatch {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Lease event kind being validated.
        event: LeaseEventKind,
        /// Owner currently projected as active.
        expected_owner: String,
        /// Owner carried by the event payload.
        actual_owner: String,
    },
    /// Lease event expiry did not match active lease expiry.
    ExpiryMismatch {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Lease event kind being validated.
        event: LeaseEventKind,
        /// Expiry currently projected as active.
        expected_expiry: u64,
        /// Expiry carried by the event payload.
        actual_expiry: u64,
    },
    /// Heartbeat attempted to regress lease expiry.
    NonMonotonicHeartbeatExpiry {
        /// Run ID referenced by the event.
        run_id: RunId,
        /// Current projected expiry.
        previous_expiry: u64,
        /// Heartbeat proposed expiry.
        proposed_expiry: u64,
    },
}

impl std::fmt::Display for LeaseCausalityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseCausalityError::UnknownRun { run_id, event } => {
                write!(f, "{event} rejected: unknown run {run_id}")
            }
            LeaseCausalityError::InvalidRunState { run_id, event, state } => {
                write!(f, "{event} rejected for run {run_id}: invalid state {state:?}")
            }
            LeaseCausalityError::MissingActiveLease { run_id, event } => {
                write!(f, "{event} rejected for run {run_id}: missing active lease")
            }
            LeaseCausalityError::LeaseAlreadyActive { run_id } => {
                write!(f, "lease acquire rejected for run {run_id}: lease already active")
            }
            LeaseCausalityError::OwnerMismatch { run_id, event, expected_owner, actual_owner } => {
                write!(
                    f,
                    "{event} rejected for run {run_id}: owner mismatch expected={expected_owner} \
                     actual={actual_owner}"
                )
            }
            LeaseCausalityError::ExpiryMismatch {
                run_id,
                event,
                expected_expiry,
                actual_expiry,
            } => {
                write!(
                    f,
                    "{event} rejected for run {run_id}: expiry mismatch \
                     expected={expected_expiry} actual={actual_expiry}"
                )
            }
            LeaseCausalityError::NonMonotonicHeartbeatExpiry {
                run_id,
                previous_expiry,
                proposed_expiry,
            } => {
                write!(
                    f,
                    "lease heartbeat rejected for run {run_id}: expiry regression \
                     previous={previous_expiry} proposed={proposed_expiry}"
                )
            }
        }
    }
}

impl std::fmt::Display for ReplayReducerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayReducerError::InvalidTransition => {
                write!(f, "Invalid state transition during replay")
            }
            ReplayReducerError::DuplicateEvent => write!(f, "Duplicate event detected"),
            ReplayReducerError::CorruptedData => write!(f, "Corrupted event data"),
            ReplayReducerError::LeaseCausality(details) => {
                write!(f, "Lease causality violation during replay: {details}")
            }
            ReplayReducerError::TaskCausality(details) => {
                write!(f, "Task causality violation during replay: {details}")
            }
            ReplayReducerError::EngineControlCausality(details) => {
                write!(f, "Engine control causality violation during replay: {details}")
            }
        }
    }
}

impl std::error::Error for ReplayReducerError {}

/// Returns a stable string key for a `Capability`, used as HashMap key.
fn capability_key(cap: &Capability) -> String {
    match cap {
        Capability::CanSubmit => "CanSubmit".to_string(),
        Capability::CanExecute => "CanExecute".to_string(),
        Capability::CanReview => "CanReview".to_string(),
        Capability::CanApprove => "CanApprove".to_string(),
        Capability::CanCancel => "CanCancel".to_string(),
        Capability::Custom(s) => format!("Custom:{s}"),
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{AttemptId, RunId, TaskId};
    use actionqueue_core::mutation::AttemptResultKind;
    use actionqueue_core::run::run_instance::RunInstance;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

    use super::*;

    fn test_task_spec(id: u128) -> TaskSpec {
        TaskSpec::new(
            TaskId::from_uuid(uuid::Uuid::from_u128(id)),
            TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
            actionqueue_core::task::run_policy::RunPolicy::Once,
            actionqueue_core::task::constraints::TaskConstraints::default(),
            actionqueue_core::task::metadata::TaskMetadata::default(),
        )
        .expect("test task spec should be valid")
    }

    fn event(seq: u64, event: WalEventType) -> WalEvent {
        WalEvent::new(seq, event)
    }

    /// Drives a run from Scheduled through to Completed via the standard lifecycle:
    /// Scheduled -> Ready -> Leased -> Running -> (attempt start/finish) -> Completed.
    fn drive_run_to_completed(reducer: &mut ReplayReducer, run_id: RunId, seq_start: u64) -> u64 {
        let attempt_id =
            AttemptId::from_uuid(uuid::Uuid::from_u128(run_id.as_uuid().as_u128() + 1));
        let mut seq = seq_start;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                    timestamp: 100,
                },
            ))
            .expect("scheduled->ready");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Ready,
                    new_state: RunState::Leased,
                    timestamp: 200,
                },
            ))
            .expect("ready->leased");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Leased,
                    new_state: RunState::Running,
                    timestamp: 300,
                },
            ))
            .expect("leased->running");
        seq += 1;

        reducer
            .apply(&event(seq, WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 400 }))
            .expect("attempt started");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::AttemptFinished {
                    run_id,
                    attempt_id,
                    result: AttemptResultKind::Success,
                    error: None,
                    output: None,
                    timestamp: 500,
                },
            ))
            .expect("attempt finished");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Running,
                    new_state: RunState::Completed,
                    timestamp: 600,
                },
            ))
            .expect("running->completed");
        seq += 1;

        seq
    }

    /// Drives a run from Scheduled through to Failed via the standard lifecycle:
    /// Scheduled -> Ready -> Leased -> Running -> (attempt start/finish) -> Failed.
    fn drive_run_to_failed(reducer: &mut ReplayReducer, run_id: RunId, seq_start: u64) -> u64 {
        let attempt_id =
            AttemptId::from_uuid(uuid::Uuid::from_u128(run_id.as_uuid().as_u128() + 1));
        let mut seq = seq_start;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                    timestamp: 100,
                },
            ))
            .expect("scheduled->ready");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Ready,
                    new_state: RunState::Leased,
                    timestamp: 200,
                },
            ))
            .expect("ready->leased");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Leased,
                    new_state: RunState::Running,
                    timestamp: 300,
                },
            ))
            .expect("leased->running");
        seq += 1;

        reducer
            .apply(&event(seq, WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 400 }))
            .expect("attempt started");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::AttemptFinished {
                    run_id,
                    attempt_id,
                    result: AttemptResultKind::Failure,
                    error: Some("test failure".to_string()),
                    output: None,
                    timestamp: 500,
                },
            ))
            .expect("attempt finished");
        seq += 1;

        reducer
            .apply(&event(
                seq,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Running,
                    new_state: RunState::Failed,
                    timestamp: 600,
                },
            ))
            .expect("running->failed");
        seq += 1;

        seq
    }

    #[test]
    fn trim_terminal_history_removes_completed_run_history() {
        let mut reducer = ReplayReducer::new();
        let task = test_task_spec(0xA001);
        let task_id = task.id();
        let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB001));

        // Create task and run.
        reducer
            .apply(&event(1, WalEventType::TaskCreated { task_spec: task, timestamp: 10 }))
            .unwrap();
        let run = RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(2, WalEventType::RunCreated { run_instance: run })).unwrap();

        // Drive to Completed.
        drive_run_to_completed(&mut reducer, run_id, 3);

        // Precondition: history exists.
        assert!(reducer.get_run_history(&run_id).is_some());
        assert!(reducer.get_attempt_history(&run_id).is_some());

        // Trim.
        reducer.trim_terminal_history();

        // History should be removed for the terminal run.
        assert!(reducer.get_run_history(&run_id).is_none());
        assert!(reducer.get_attempt_history(&run_id).is_none());
        assert!(reducer.get_lease_metadata(&run_id).is_none());

        // The run itself (state and instance) should still exist.
        assert_eq!(reducer.get_run_state(&run_id), Some(&RunState::Completed));
        assert!(reducer.get_run_instance(&run_id).is_some());
    }

    #[test]
    fn trim_terminal_history_removes_failed_and_canceled_run_history() {
        let mut reducer = ReplayReducer::new();

        // Failed run.
        let task_f = test_task_spec(0xA010);
        let task_id_f = task_f.id();
        let run_id_f = RunId::from_uuid(uuid::Uuid::from_u128(0xB010));

        reducer
            .apply(&event(1, WalEventType::TaskCreated { task_spec: task_f, timestamp: 10 }))
            .unwrap();
        let run_f = RunInstance::new_scheduled_with_id(run_id_f, task_id_f, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(2, WalEventType::RunCreated { run_instance: run_f })).unwrap();
        let seq = drive_run_to_failed(&mut reducer, run_id_f, 3);

        // Canceled run.
        let task_c = test_task_spec(0xA020);
        let task_id_c = task_c.id();
        let run_id_c = RunId::from_uuid(uuid::Uuid::from_u128(0xB020));

        reducer
            .apply(&event(seq, WalEventType::TaskCreated { task_spec: task_c, timestamp: 20 }))
            .unwrap();
        let run_c = RunInstance::new_scheduled_with_id(run_id_c, task_id_c, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(seq + 1, WalEventType::RunCreated { run_instance: run_c })).unwrap();
        reducer
            .apply(&event(
                seq + 2,
                WalEventType::RunStateChanged {
                    run_id: run_id_c,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                    timestamp: 100,
                },
            ))
            .unwrap();
        reducer
            .apply(&event(seq + 3, WalEventType::RunCanceled { run_id: run_id_c, timestamp: 200 }))
            .unwrap();

        // Precondition: both have history.
        assert!(reducer.get_run_history(&run_id_f).is_some());
        assert!(reducer.get_run_history(&run_id_c).is_some());

        reducer.trim_terminal_history();

        // Both should be trimmed.
        assert!(reducer.get_run_history(&run_id_f).is_none());
        assert!(reducer.get_attempt_history(&run_id_f).is_none());
        assert!(reducer.get_run_history(&run_id_c).is_none());
        assert!(reducer.get_attempt_history(&run_id_c).is_none());

        // State projections remain.
        assert_eq!(reducer.get_run_state(&run_id_f), Some(&RunState::Failed));
        assert_eq!(reducer.get_run_state(&run_id_c), Some(&RunState::Canceled));
    }

    #[test]
    fn trim_terminal_history_preserves_active_run_history() {
        let mut reducer = ReplayReducer::new();
        let task = test_task_spec(0xA002);
        let task_id = task.id();
        let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB002));

        // Create task and run (stays in Scheduled — a non-terminal state).
        reducer
            .apply(&event(1, WalEventType::TaskCreated { task_spec: task, timestamp: 10 }))
            .unwrap();
        let run = RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(2, WalEventType::RunCreated { run_instance: run })).unwrap();

        // Advance to Ready (still non-terminal).
        reducer
            .apply(&event(
                3,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                    timestamp: 100,
                },
            ))
            .unwrap();

        // Precondition: history exists with 2 entries (Scheduled, Ready).
        let history = reducer.get_run_history(&run_id).expect("history should exist");
        assert_eq!(history.len(), 2);

        // Trim should be a no-op for this active run.
        reducer.trim_terminal_history();

        let history = reducer.get_run_history(&run_id).expect("history should still exist");
        assert_eq!(history.len(), 2);
        assert!(reducer.get_attempt_history(&run_id).is_some());
    }

    #[test]
    fn trim_terminal_history_no_op_when_no_terminal_runs() {
        let mut reducer = ReplayReducer::new();
        let task = test_task_spec(0xA003);
        let task_id = task.id();
        let run_id = RunId::from_uuid(uuid::Uuid::from_u128(0xB003));

        reducer
            .apply(&event(1, WalEventType::TaskCreated { task_spec: task, timestamp: 10 }))
            .unwrap();
        let run = RunInstance::new_scheduled_with_id(run_id, task_id, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(2, WalEventType::RunCreated { run_instance: run })).unwrap();

        // Pre-trim state.
        assert_eq!(reducer.run_count(), 1);
        let run_history_len =
            reducer.get_run_history(&run_id).expect("run history should exist").len();
        let attempt_history_len =
            reducer.get_attempt_history(&run_id).expect("attempt history should exist").len();

        // Trim.
        reducer.trim_terminal_history();

        // Post-trim state should be identical.
        assert_eq!(reducer.run_count(), 1);
        assert_eq!(
            reducer.get_run_history(&run_id).expect("run history should still exist").len(),
            run_history_len
        );
        assert_eq!(
            reducer.get_attempt_history(&run_id).expect("attempt history should still exist").len(),
            attempt_history_len
        );
    }

    #[test]
    fn trim_terminal_history_mixed_terminal_and_active() {
        let mut reducer = ReplayReducer::new();

        // Terminal run (completed).
        let task_t = test_task_spec(0xA004);
        let task_id_t = task_t.id();
        let run_id_terminal = RunId::from_uuid(uuid::Uuid::from_u128(0xB004));

        reducer
            .apply(&event(1, WalEventType::TaskCreated { task_spec: task_t, timestamp: 10 }))
            .unwrap();
        let run_t = RunInstance::new_scheduled_with_id(run_id_terminal, task_id_t, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(2, WalEventType::RunCreated { run_instance: run_t })).unwrap();
        let seq = drive_run_to_completed(&mut reducer, run_id_terminal, 3);

        // Active run (stays in Ready).
        let task_a = test_task_spec(0xA005);
        let task_id_a = task_a.id();
        let run_id_active = RunId::from_uuid(uuid::Uuid::from_u128(0xB005));

        reducer
            .apply(&event(seq, WalEventType::TaskCreated { task_spec: task_a, timestamp: 20 }))
            .unwrap();
        let run_a = RunInstance::new_scheduled_with_id(run_id_active, task_id_a, 1000, 1000)
            .expect("run should build");
        reducer.apply(&event(seq + 1, WalEventType::RunCreated { run_instance: run_a })).unwrap();
        reducer
            .apply(&event(
                seq + 2,
                WalEventType::RunStateChanged {
                    run_id: run_id_active,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                    timestamp: 100,
                },
            ))
            .unwrap();

        // Trim.
        reducer.trim_terminal_history();

        // Terminal run history removed.
        assert!(reducer.get_run_history(&run_id_terminal).is_none());
        assert!(reducer.get_attempt_history(&run_id_terminal).is_none());

        // Active run history preserved.
        assert!(reducer.get_run_history(&run_id_active).is_some());
        assert!(reducer.get_attempt_history(&run_id_active).is_some());
        let active_history =
            reducer.get_run_history(&run_id_active).expect("active history should exist");
        assert_eq!(active_history.len(), 2); // Scheduled + Ready
    }
}
