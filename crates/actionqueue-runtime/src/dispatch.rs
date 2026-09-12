//! Core dispatch loop that composes all engine primitives.
//!
//! The dispatch loop orchestrates the full run lifecycle:
//! promote → select → gate → lease → execute → finish → release.
//!
//! Workers execute via `tokio::task::spawn_blocking` and communicate results
//! back through an unbounded MPSC channel. The dispatch loop owns all WAL
//! mutation authority exclusively — workers never write to the WAL.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use actionqueue_core::admission::{EnsureTaskOutcome, EnsureTaskRequest};
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
#[cfg(feature = "workflow")]
use actionqueue_core::mutation::RunCreateCommand;
use actionqueue_core::mutation::{
    AttemptStartCommand, DependencyDeclareCommand, DurabilityPolicy, LeaseAcquireCommand,
    LeaseHeartbeatCommand, MutationAuthority, MutationCommand, RunStateTransitionCommand,
    TaskCancelCommand,
};
use actionqueue_core::run::run_instance::{RunInstance, RunInstanceError};
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy;
use actionqueue_core::task::safety::SafetyLevel;
use actionqueue_core::task::task_spec::TaskSpec;
use actionqueue_engine::concurrency::key_gate::{ConcurrencyKey, KeyGate, ReleaseResult};
use actionqueue_engine::derive::DerivationError;
use actionqueue_engine::index::ready::ReadyIndex;
use actionqueue_engine::index::scheduled::ScheduledIndex;
use actionqueue_engine::scheduler::promotion::{
    promote_scheduled_to_ready_via_authority, AuthorityPromotionError, PromotionParams,
};
use actionqueue_engine::scheduler::retry_promotion::promote_retry_wait_to_ready;
use actionqueue_engine::selection::default_selector::{ready_inputs_from_index, select_ready_runs};
use actionqueue_engine::time::clock::Clock;
use actionqueue_executor_local::backoff::BackoffStrategy;
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_executor_local::identity::{ExecutorIdentity, LocalExecutorIdentity};
use actionqueue_executor_local::types::ExecutorRequest;
use actionqueue_executor_local::{AttemptRunner, SystemAttemptTimer};
use actionqueue_storage::mutation::authority::{MutationAuthorityError, StorageMutationAuthority};
use actionqueue_storage::recovery::reducer::{ReplayReducer, ReplayReducerError};
use actionqueue_storage::snapshot::build::build_snapshot_from_projection;
use actionqueue_storage::snapshot::mapping::SnapshotMappingError;
use actionqueue_storage::snapshot::writer::{
    SnapshotFsWriter, SnapshotWriter, SnapshotWriterError,
};
use actionqueue_storage::wal::writer::WalWriter;
use actionqueue_workflow::children::build_children_snapshot;
use actionqueue_workflow::dag::DependencyGate;
use actionqueue_workflow::hierarchy::HierarchyTracker;

use tokio::sync::mpsc;

use crate::admission::AdmissionError;
use crate::config::BackoffStrategyConfig;
use crate::worker::{InFlightRun, WorkerResult};

/// Builds a [`DependencyGate`] from the recovery reducer's dependency state.
///
/// Called at bootstrap and after each created admission to reconstruct the gate
/// from the committed projection. Satisfaction is derived from the
/// projection's run states (which tasks have at least one Completed run with all
/// runs terminal). Failure is derived similarly (all runs terminal, none Completed).
fn build_dependency_gate(projection: &ReplayReducer) -> DependencyGate {
    use actionqueue_core::run::state::RunState;

    let mut gate = DependencyGate::new();

    // Collect dependency map once for O(1) lookups.
    let dep_map: std::collections::HashMap<_, _> = projection.dependency_declarations().collect();

    // Populate declarations from WAL events.
    for (&task_id, prereqs) in &dep_map {
        // Declarations were already cycle-checked at submission time.
        if let Err(err) = gate.declare(task_id, prereqs.iter().copied().collect()) {
            tracing::warn!(%task_id, error = %err, "dependency gate declaration failed during rebuild");
        }
    }

    // Derive satisfaction state from current run states using the O(R_task) index.
    // Collect all unique prerequisite task_ids across all declarations.
    let all_prereqs: std::collections::HashSet<_> =
        dep_map.values().flat_map(|prereqs| prereqs.iter().copied()).collect();

    for task_id in all_prereqs {
        let runs: Vec<_> = projection.runs_for_task(task_id).collect();
        if runs.is_empty() {
            continue;
        }
        let all_terminal = runs.iter().all(|r| r.state().is_terminal());
        if !all_terminal {
            continue;
        }
        let has_completed = runs.iter().any(|r| r.state() == RunState::Completed);
        if has_completed {
            gate.force_satisfy(task_id);
        } else {
            gate.force_fail(task_id);
        }
    }

    // Declarations were installed before prerequisite completion was restored.
    // Recompute every dependent now, using completed tasks (not other tasks'
    // eligibility), so reconstruction is independent of HashMap iteration order.
    for &task_id in dep_map.keys() {
        gate.recompute_satisfaction_pub(task_id);
    }

    // Cascade failure from directly-failed prerequisites to all transitive dependents.
    // Without this, a crash between a prerequisite failing and the cascade being
    // committed would leave dependent tasks permanently stranded after recovery.
    let _ = gate.propagate_failures();

    gate
}

/// Builds a [`HierarchyTracker`] from the recovery reducer's task hierarchy.
///
/// Called once at `DispatchLoop::new()` to reconstruct the tracker from WAL events
/// that were already replayed during bootstrap.
///
/// Two-pass approach:
/// 1. Register all parent-child relationships (terminal_tasks is empty → no orphan errors).
/// 2. Mark tasks terminal whose runs are all in terminal state or that are canceled with no runs.
fn build_hierarchy_tracker(projection: &ReplayReducer) -> HierarchyTracker {
    let mut tracker = HierarchyTracker::new();

    // Pass 1: register all parent-child pairs.
    // terminal_tasks is empty here, so orphan prevention never fires.
    for (child_id, parent_id) in projection.parent_child_mappings() {
        // Depth limit should not be exceeded for valid WAL data;
        // ignore errors (they indicate a WAL invariant violation, not a tracker bug).
        let _ = tracker.register_child(parent_id, child_id);
    }

    // Pass 2: mark tasks terminal based on projection run states using the O(R_task) index.
    for task_record in projection.task_records() {
        let task_id = task_record.task_spec().id();
        let runs: Vec<_> = projection.runs_for_task(task_id).collect();
        let is_terminal = if runs.is_empty() {
            projection.is_task_canceled(task_id)
        } else {
            runs.iter().all(|r| r.state().is_terminal())
        };
        if is_terminal {
            tracker.mark_terminal(task_id);
        }
    }

    tracker
}

/// Result of a single dispatch tick.
#[derive(Debug, Clone, Default)]
#[must_use = "tick result should be inspected for dispatch activity"]
pub struct TickResult {
    /// Number of runs promoted from Scheduled to Ready.
    pub promoted_scheduled: usize,
    /// Number of runs promoted from RetryWait to Ready.
    pub promoted_retry_wait: usize,
    /// Number of runs selected and dispatched for execution.
    pub dispatched: usize,
    /// Number of runs that completed (terminal state).
    pub completed: usize,
    /// Whether the engine is currently paused.
    pub engine_paused: bool,
}

/// Summary of a `run_until_idle` session.
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct RunSummary {
    /// Total ticks executed.
    pub ticks: usize,
    /// Total runs dispatched across all ticks.
    pub total_dispatched: usize,
    /// Total runs completed across all ticks.
    pub total_completed: usize,
}

/// Concrete authority error type used by the dispatch loop.
pub type AuthorityError = MutationAuthorityError<ReplayReducerError>;

/// Errors that can occur during dispatch.
#[derive(Debug)]
pub enum DispatchError {
    /// Compound admission failed.
    Admission(AdmissionError),
    /// Persisted state cannot be reconciled without changing its meaning.
    RecoveryInvariant(String),
    /// WAL sequence counter overflow.
    SequenceOverflow,
    /// A mutation command submitted to the storage authority failed.
    Authority(AuthorityError),
    /// Scheduled-to-ready promotion via authority failed.
    ScheduledPromotion(Box<AuthorityPromotionError<AuthorityError>>),
    /// RetryWait-to-ready promotion failed due to an invalid state transition.
    RetryPromotion(RunInstanceError),
    /// Run derivation from a task's run policy failed.
    Derivation(DerivationError),
    /// Snapshot build from projection failed.
    SnapshotBuild(SnapshotMappingError),
    /// Snapshot I/O failed during write or close.
    SnapshotWrite(SnapshotWriterError),
    /// Snapshot writer initialization failed.
    SnapshotInit(String),
    /// Internal state inconsistency (e.g., task not found after run transition).
    StateInconsistency {
        /// Run that triggered the inconsistency.
        run_id: RunId,
        /// Human-readable context for diagnostics.
        context: String,
    },
    /// Continuation limits cannot guarantee durable attempt closure.
    InvalidContinuationLimits(crate::config::ConfigError),
    /// Backoff strategy configuration is invalid (e.g., base exceeds max).
    InvalidBackoffConfig,
    /// Dependency declaration would introduce a cycle in the task DAG.
    DependencyCycle(actionqueue_workflow::dag::CycleError),
    /// Retry decision from attempt outcome violated retry invariants.
    RetryDecision(actionqueue_executor_local::RetryDecisionError),
    /// A dynamically submitted task was rejected (parent not found or terminal).
    SubmissionRejected {
        /// Task that was rejected.
        task_id: TaskId,
        /// Human-readable reason for rejection.
        context: String,
    },
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidContinuationLimits(e) => write!(f, "{e}"),
            Self::Admission(e) => write!(f, "{e}"),
            DispatchError::RecoveryInvariant(message) => write!(f, "recovery invariant: {message}"),
            DispatchError::SequenceOverflow => write!(f, "WAL sequence counter overflow"),
            DispatchError::Authority(e) => write!(f, "authority error: {e}"),
            DispatchError::ScheduledPromotion(e) => {
                write!(f, "scheduled promotion error: {e}")
            }
            DispatchError::RetryPromotion(e) => write!(f, "retry promotion error: {e}"),
            DispatchError::Derivation(e) => write!(f, "run derivation error: {e}"),
            DispatchError::SnapshotBuild(e) => write!(f, "snapshot build error: {e}"),
            DispatchError::SnapshotWrite(e) => write!(f, "snapshot write error: {e}"),
            DispatchError::SnapshotInit(e) => write!(f, "snapshot init error: {e}"),
            DispatchError::StateInconsistency { run_id, context } => {
                write!(f, "state inconsistency for run {run_id}: {context}")
            }
            DispatchError::InvalidBackoffConfig => {
                write!(f, "invalid backoff configuration")
            }
            DispatchError::RetryDecision(e) => write!(f, "retry decision error: {e}"),
            DispatchError::DependencyCycle(e) => write!(f, "dependency cycle: {e}"),
            DispatchError::SubmissionRejected { task_id, context } => {
                write!(f, "submission rejected for task {task_id}: {context}")
            }
        }
    }
}

impl std::error::Error for DispatchError {}

/// The core dispatch loop that composes all engine primitives.
///
/// Workers execute handler logic via `tokio::task::spawn_blocking` and send
/// results back through an unbounded MPSC channel. All WAL mutation authority
/// remains exclusively owned by the dispatch loop — workers never touch the WAL.
///
/// The `I: ExecutorIdentity` generic defaults to `LocalExecutorIdentity` so
/// existing construction sites don't need changes for v0.x. Sprint 4 remote
/// actors will supply their own identity via this parameter.
pub struct DispatchLoop<
    W: WalWriter,
    H: ExecutorHandler,
    C: Clock = actionqueue_engine::time::clock::SystemClock,
    I: ExecutorIdentity = LocalExecutorIdentity,
> {
    authority: StorageMutationAuthority<W, ReplayReducer>,
    runner: Arc<AttemptRunner<H, SystemAttemptTimer>>,
    clock: C,
    /// Identity of the executor acquiring leases.
    identity: I,
    key_gate: KeyGate,
    backoff: Box<dyn BackoffStrategy + Send + Sync>,
    max_concurrent: usize,
    lease_timeout_secs: u64,
    result_tx: mpsc::UnboundedSender<WorkerResult>,
    result_rx: mpsc::UnboundedReceiver<WorkerResult>,
    in_flight: HashMap<actionqueue_core::ids::RunId, InFlightRun>,
    pending_result: Option<WorkerResult>,
    draining: bool,
    snapshot_path: Option<PathBuf>,
    snapshot_event_threshold: Option<u64>,
    events_since_last_snapshot: u64,

    /// DAG dependency gate — gates Scheduled → Ready promotion.
    /// Built from the projection at bootstrap and kept in sync as runs complete.
    dependency_gate: DependencyGate,
    /// Hierarchy tracker — parent-child task tree for cascade cancellation.
    /// Built from the projection at bootstrap and kept in sync as tasks are
    /// created (via handler submissions) and reach terminal state.
    hierarchy_tracker: HierarchyTracker,
    /// TaskIds with pending cascade cancellations. Seeded at bootstrap with
    /// tasks that have `canceled_at` and at least one non-terminal descendant.
    /// Entries are removed once the cascade completes (self-quenching).
    pending_hierarchy_cascade: std::collections::HashSet<TaskId>,
    /// In-memory budget state reconstructed from WAL events at bootstrap.
    /// Updated each tick as worker results arrive with consumption records.
    #[cfg(feature = "budget")]
    budget_tracker: actionqueue_budget::BudgetTracker,
    /// In-memory subscription state reconstructed from WAL events at bootstrap.
    /// Consulted for subscription-triggered promotion eligibility and event matching.
    #[cfg(feature = "budget")]
    subscription_registry: actionqueue_budget::SubscriptionRegistry,
    /// Per-task cache of parsed cron::Schedule objects (workflow feature only).
    /// Avoids re-parsing cron expressions on every tick for cron-policy tasks.
    #[cfg(feature = "workflow")]
    cron_schedule_cache: actionqueue_engine::derive::cron::CronScheduleCache,
    /// In-memory actor registry (actor feature only).
    #[cfg(feature = "actor")]
    actor_registry: actionqueue_actor::ActorRegistry,
    /// Heartbeat monitor for actor timeout detection (actor feature only).
    #[cfg(feature = "actor")]
    heartbeat_monitor: actionqueue_actor::HeartbeatMonitor,
    /// Department grouping registry (actor feature only).
    #[cfg(feature = "actor")]
    department_registry: actionqueue_actor::DepartmentRegistry,
    /// Tenant registry (platform feature only).
    #[cfg(feature = "platform")]
    tenant_registry: actionqueue_platform::TenantRegistry,
    /// RBAC enforcer (platform feature only).
    #[cfg(feature = "platform")]
    rbac_enforcer: actionqueue_platform::RbacEnforcer,
    /// Append ledger (platform feature only).
    #[cfg(feature = "platform")]
    ledger: actionqueue_platform::AppendLedger,
    /// TaskIds that have fully reached terminal state and are pending GC from
    /// in-memory data structures (DependencyGate, HierarchyTracker, BudgetTracker,
    /// SubscriptionRegistry, CronScheduleCache). Populated when all runs for a
    /// task are terminal; drained each tick after cascades are complete.
    pending_gc_tasks: std::collections::HashSet<TaskId>,
}

/// Configuration parameters for the dispatch loop that group backoff,
/// concurrency, lease, and snapshot settings.
pub struct DispatchConfig {
    /// Backoff strategy configuration for retry delay computation.
    pub(crate) backoff_config: BackoffStrategyConfig,
    /// Maximum number of concurrently executing runs.
    pub(crate) max_concurrent: usize,
    /// Lease timeout in seconds for dispatched runs.
    pub(crate) lease_timeout_secs: u64,
    /// Filesystem path for snapshot persistence, if enabled.
    pub(crate) snapshot_path: Option<PathBuf>,
    /// Number of WAL events between automatic snapshot writes, if enabled.
    pub(crate) snapshot_event_threshold: Option<u64>,
}

impl DispatchConfig {
    /// Creates a new dispatch configuration.
    pub fn new(
        backoff_config: BackoffStrategyConfig,
        max_concurrent: usize,
        lease_timeout_secs: u64,
        snapshot_path: Option<PathBuf>,
        snapshot_event_threshold: Option<u64>,
    ) -> Self {
        Self {
            backoff_config,
            max_concurrent,
            lease_timeout_secs,
            snapshot_path,
            snapshot_event_threshold,
        }
    }
}

impl<W: WalWriter, H: ExecutorHandler + 'static, C: Clock> DispatchLoop<W, H, C> {
    /// Creates a new dispatch loop.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError::InvalidBackoffConfig`] if the backoff strategy
    /// configuration is invalid (e.g., exponential base exceeds max), or
    /// [`DispatchError::InvalidContinuationLimits`] before recovery if the
    /// disposition quota cannot accommodate durable failure and recovery.
    pub fn new(
        mut authority: StorageMutationAuthority<W, ReplayReducer>,
        handler: H,
        clock: C,
        config: DispatchConfig,
    ) -> Result<Self, DispatchError> {
        crate::config::RuntimeConfig::validate_continuation_limits(authority.continuation_limits())
            .map_err(DispatchError::InvalidContinuationLimits)?;
        crate::waits::recover_execution(&mut authority, clock.now())
            .map_err(DispatchError::Authority)?;
        // Settle controls, retained matches, overdue deadlines and their cascades
        // before constructing any in-memory coordination from the projection.
        crate::waits::reconcile(&mut authority, clock.now()).map_err(DispatchError::Authority)?;
        let backoff: Box<dyn BackoffStrategy + Send + Sync> = match &config.backoff_config {
            BackoffStrategyConfig::Fixed { interval } => {
                Box::new(actionqueue_executor_local::FixedBackoff::new(*interval))
            }
            BackoffStrategyConfig::Exponential { base, max } => Box::new(
                actionqueue_executor_local::ExponentialBackoff::new(*base, *max)
                    .map_err(|_| DispatchError::InvalidBackoffConfig)?,
            ),
        };

        let (result_tx, result_rx) = mpsc::unbounded_channel();

        // Rebuild the dependency gate from projection state (WAL events already applied).
        let dependency_gate = build_dependency_gate(authority.projection());
        // Rebuild the hierarchy tracker from task parent_task_id fields.
        let hierarchy_tracker = build_hierarchy_tracker(authority.projection());

        // Rebuild budget tracker from WAL-sourced projection records.
        #[cfg(feature = "budget")]
        let budget_tracker = {
            let mut tracker = actionqueue_budget::BudgetTracker::new();
            for ((task_id, dimension), record) in authority.projection().budgets() {
                tracker.allocate(*task_id, *dimension, record.limit);
                if record.consumed > 0 {
                    tracker.consume(*task_id, *dimension, record.consumed);
                }
            }
            tracker
        };

        // Rebuild subscription registry from WAL-sourced projection records.
        #[cfg(feature = "budget")]
        let subscription_registry = {
            let mut registry = actionqueue_budget::SubscriptionRegistry::new();
            for (sub_id, record) in authority.projection().subscriptions() {
                if record.canceled_at.is_none() {
                    registry.register(*sub_id, record.task_id, record.filter.clone());
                    if record.triggered_at.is_some() {
                        registry.trigger(*sub_id);
                    }
                }
            }
            registry
        };

        // Seed pending cascade with canceled tasks that may have non-terminal descendants.
        let pending_hierarchy_cascade: std::collections::HashSet<TaskId> = authority
            .projection()
            .task_records()
            .filter(|tr| tr.canceled_at().is_some())
            .map(|tr| tr.task_spec().id())
            .collect();

        // Rebuild actor registry from WAL-sourced projection records.
        #[cfg(feature = "actor")]
        let actor_registry = {
            let mut registry = actionqueue_actor::ActorRegistry::new();
            for (actor_id, record) in authority.projection().actors() {
                if record.deregistered_at.is_none() {
                    let caps = actionqueue_core::actor::ExecutorTraits::new(
                        record.executor_traits.clone(),
                    )
                    .map_err(|e| DispatchError::RecoveryInvariant(e.to_string()))?;
                    let mut reg = actionqueue_core::actor::ActorRegistration::new(
                        *actor_id,
                        record.identity.clone(),
                        caps,
                        record.heartbeat_interval_secs,
                    );
                    if let Some(tid) = record.tenant_id {
                        reg = reg.with_tenant(tid);
                    }
                    if let Some(dept_str) = &record.department {
                        let dept = actionqueue_core::ids::DepartmentId::new(dept_str.clone())
                            .map_err(|e| DispatchError::RecoveryInvariant(e.to_string()))?;
                        reg = reg.with_department(dept);
                    }
                    registry.register(reg);
                }
            }
            registry
        };

        // Rebuild heartbeat monitor from actor records.
        #[cfg(feature = "actor")]
        let heartbeat_monitor = {
            let mut monitor = actionqueue_actor::HeartbeatMonitor::new();
            for (actor_id, record) in authority.projection().actors() {
                if record.deregistered_at.is_none() {
                    let policy = actionqueue_core::actor::HeartbeatPolicy::with_default_multiplier(
                        record.heartbeat_interval_secs,
                    );
                    let last_beat = record.last_heartbeat_at.unwrap_or(record.registered_at);
                    monitor.record_registration(*actor_id, policy, last_beat);
                }
            }
            monitor
        };

        // Rebuild department registry from actor records.
        #[cfg(feature = "actor")]
        let department_registry = {
            let mut registry = actionqueue_actor::DepartmentRegistry::new();
            for (actor_id, record) in authority.projection().actors() {
                if record.deregistered_at.is_none() {
                    if let Some(dept_str) = &record.department {
                        if let Ok(dept) = actionqueue_core::ids::DepartmentId::new(dept_str.clone())
                        {
                            registry.assign(*actor_id, dept);
                        }
                    }
                }
            }
            registry
        };

        // Rebuild tenant registry from WAL-sourced projection records.
        #[cfg(feature = "platform")]
        let tenant_registry = {
            let mut registry = actionqueue_platform::TenantRegistry::new();
            for (_, record) in authority.projection().tenants() {
                registry.register(actionqueue_core::platform::TenantRegistration::new(
                    record.tenant_id,
                    record.name.clone(),
                ));
            }
            registry
        };

        // Rebuild RBAC enforcer from WAL-sourced projection records.
        #[cfg(feature = "platform")]
        let rbac_enforcer = {
            let mut enforcer = actionqueue_platform::RbacEnforcer::new();
            for record in authority.projection().role_assignments() {
                enforcer.assign_role(record.actor_id, record.role.clone(), record.tenant_id);
            }
            for record in authority.projection().capability_grants() {
                if record.revoked_at.is_none() {
                    enforcer.grant_capability(
                        record.actor_id,
                        record.capability.clone(),
                        record.tenant_id,
                    );
                }
            }
            enforcer
        };

        // Rebuild ledger from WAL-sourced projection records.
        #[cfg(feature = "platform")]
        let ledger = {
            let mut ledger = actionqueue_platform::AppendLedger::new();
            for record in authority.projection().ledger_entries() {
                let entry = actionqueue_core::platform::LedgerEntry::new(
                    record.entry_id,
                    record.tenant_id,
                    record.ledger_key.clone(),
                    record.payload.clone(),
                    record.timestamp,
                );
                let entry =
                    if let Some(aid) = record.actor_id { entry.with_actor(aid) } else { entry };
                ledger.append(entry);
            }
            ledger
        };

        let mut dispatch = Self {
            authority,
            runner: Arc::new(AttemptRunner::new(handler)),
            clock,
            identity: LocalExecutorIdentity,
            key_gate: KeyGate::new(),
            backoff,
            max_concurrent: config.max_concurrent,
            lease_timeout_secs: config.lease_timeout_secs,
            result_tx,
            result_rx,
            in_flight: HashMap::new(),
            pending_result: None,
            draining: false,
            snapshot_path: config.snapshot_path,
            snapshot_event_threshold: config.snapshot_event_threshold,
            events_since_last_snapshot: 0,

            dependency_gate,
            hierarchy_tracker,
            pending_hierarchy_cascade,
            #[cfg(feature = "budget")]
            budget_tracker,
            #[cfg(feature = "budget")]
            subscription_registry,
            #[cfg(feature = "workflow")]
            cron_schedule_cache: actionqueue_engine::derive::cron::CronScheduleCache::new(),
            #[cfg(feature = "actor")]
            actor_registry,
            #[cfg(feature = "actor")]
            heartbeat_monitor,
            #[cfg(feature = "actor")]
            department_registry,
            #[cfg(feature = "platform")]
            tenant_registry,
            #[cfg(feature = "platform")]
            rbac_enforcer,
            #[cfg(feature = "platform")]
            ledger,
            pending_gc_tasks: std::collections::HashSet::new(),
        };
        dispatch.rebuild_key_gate()?;
        Ok(dispatch)
    }

    fn rebuild_key_gate(&mut self) -> Result<(), DispatchError> {
        let mut gate = KeyGate::new();
        // Cancellation removes the durable reservation before the worker stops.
        // Keep its exclusion until a result proves execution has returned.
        let executing_keys = self.in_flight.values().filter_map(|inf| {
            self.projection()
                .get_task(&inf.task_id)
                .and_then(|task| task.constraints().concurrency_key())
                .map(|key| (inf.run_id, key))
        });
        for (run, key) in self.authority.projection().key_reservations().chain(executing_keys) {
            if matches!(
                actionqueue_engine::concurrency::lifecycle::acquire_key(
                    Some(key.into()),
                    run,
                    &mut gate
                ),
                actionqueue_engine::concurrency::lifecycle::LifecycleResult::KeyOccupied { .. }
            ) {
                return Err(DispatchError::Authority(MutationAuthorityError::Wait(
                    actionqueue_core::mutation::WaitRejection::ConflictingKeyOwnership,
                )));
            }
        }
        self.key_gate = gate;
        Ok(())
    }
    /// Next durable timer, even when paused or draining.
    pub fn next_wait_deadline(&self) -> Option<u64> {
        self.projection().waits().next_deadline()
    }
    /// Compound host-attested run/task cancellation.
    pub fn cancel(
        &mut self,
        command: actionqueue_core::mutation::CancelCommand,
    ) -> Result<(), DispatchError> {
        let _ = self
            .authority
            .submit_command(MutationCommand::Cancel(command), DurabilityPolicy::Immediate)
            .map_err(DispatchError::Authority)?;
        // Complete descendant/dependency work synchronously, before a following
        // signal admission or tick can resolve an affected wait.
        crate::waits::recover_cancellations(&mut self.authority, self.clock.now())
            .map_err(DispatchError::Authority)?;
        self.refresh_coordination();
        self.rebuild_key_gate()
    }
    /// Handler-independent establishment; returned outcome remains committed on scan failure.
    pub fn establish_wait(
        &mut self,
        command: actionqueue_core::mutation::WaitEstablishCommand,
    ) -> Result<actionqueue_core::mutation::WaitOutcome, crate::waits::WaitError> {
        crate::waits::establish(&mut self.authority, command)
    }
    pub fn reconcile_waits(&mut self) -> Result<usize, crate::waits::WaitError> {
        let before = self.projection().latest_sequence();
        let result = crate::waits::reconcile(&mut self.authority, self.clock.now());
        if self.projection().latest_sequence() != before {
            self.refresh_coordination();
        }
        result
    }

    /// Continuations and controls can terminate runs without a worker result.
    /// Reconstruct coordination from the settled durable state, just as on
    /// bootstrap, including cascades committed by the handler-independent service.
    fn refresh_coordination(&mut self) {
        self.dependency_gate = build_dependency_gate(self.projection());
        self.hierarchy_tracker = build_hierarchy_tracker(self.projection());
        for task in self.authority.projection().task_records() {
            let id = task.task_spec().id();
            if task.canceled_at().is_some() {
                self.pending_hierarchy_cascade.insert(id);
            }
            if self.hierarchy_tracker.is_terminal(id) {
                self.pending_gc_tasks.insert(id);
            }
        }
    }
    /// Returns a reference to the projection (current state view).
    pub fn projection(&self) -> &ReplayReducer {
        self.authority.projection()
    }

    /// Computes the next WAL sequence number with overflow protection.
    fn next_sequence(&self) -> Result<u64, DispatchError> {
        self.authority
            .projection()
            .latest_sequence()
            .checked_add(1)
            .ok_or(DispatchError::SequenceOverflow)
    }

    /// Drains completed worker results from the channel and applies
    /// state transitions via the WAL mutation authority.
    fn drain_completed_results(
        &mut self,
        result: &mut TickResult,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        // Process any result stashed from the async recv in run_until_idle.
        if let Some(stashed) = self.pending_result.take() {
            self.process_worker_result(stashed, result, current_time)?;
        }
        while let Ok(worker_result) = self.result_rx.try_recv() {
            self.process_worker_result(worker_result, result, current_time)?;
        }

        Ok(())
    }

    fn process_worker_result(
        &mut self,
        worker_result: WorkerResult,
        result: &mut TickResult,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        let run_id = worker_result.run_id;
        let attempt_id = worker_result.attempt_id;

        tracing::debug!(%run_id, %attempt_id, "worker result received");

        if self.authority.recovery_required() {
            return Err(DispatchError::Authority(MutationAuthorityError::RecoveryRequired));
        }

        // The cancellation already closed this attempt durably. Observing its
        // result only releases process-local ownership; it cannot rewrite history.
        if self.projection().get_run_state(&run_id) == Some(&RunState::Canceled)
            && self.in_flight.get(&run_id).is_some_and(|inf| inf.attempt_id == attempt_id)
        {
            self.in_flight.remove(&run_id);
            self.rebuild_key_gate()?;
            return Ok(());
        }

        // A result belongs to the exact accepted execution, not merely to a run.
        // Heartbeats preserve the grant sequence; unrelated commits do not revoke it.
        let eligible = self.in_flight.get(&run_id).is_some_and(|inf| inf.attempt_id == attempt_id)
            && self.projection().get_run_instance(&run_id).is_some_and(|run| {
                run.state() == RunState::Running
                    && run.current_attempt_id() == Some(attempt_id)
                    && !self.projection().is_task_canceled(run.task_id())
            })
            && self.projection().get_lease_metadata(&run_id).is_some_and(|lease| {
                lease.owner() == worker_result.lease_fence.owner().as_str()
                    && lease.granted_at_sequence()
                        == worker_result.lease_fence.granted_at_sequence()
                    && current_time < lease.expiry()
            })
            && self
                .projection()
                .get_attempt_history(&run_id)
                .and_then(|history| history.last())
                .and_then(|attempt| attempt.accepted_start())
                .is_some_and(|start| start.fence == worker_result.lease_fence);
        if !eligible {
            tracing::debug!(%run_id, %attempt_id, "discarding stale worker result");
            return Ok(());
        }

        let expected = actionqueue_core::mutation::AttemptCommitExpectation::new(
            self.next_sequence()?,
            run_id,
            attempt_id,
            RunState::Running,
            worker_result.lease_fence.clone(),
        );
        crate::disposition::commit(
            &mut self.authority,
            expected,
            worker_result.disposition,
            current_time,
        )
        .map_err(DispatchError::Authority)?;
        let target_state = *self.projection().get_run_state(&run_id).expect("committed run");
        self.dependency_gate = build_dependency_gate(self.authority.projection());
        self.hierarchy_tracker = build_hierarchy_tracker(self.authority.projection());
        {
            // Capture task_id before the in_flight borrow for use in the gate notification.
            let task_id = self.in_flight.get(&run_id).map(|inf| inf.task_id);

            // Release concurrency key for terminal runs, on RetryWait if the hold
            // policy is ReleaseOnRetry, or on Suspended (same semantics as RetryWait).
            if let Some(inf) = self.in_flight.get(&run_id) {
                tracing::debug!(
                    %run_id,
                    attempt_id = %inf.attempt_id,
                    attempt_number = inf.attempt_number,
                    max_attempts = inf.max_attempts,
                    ?target_state,
                    "processing run completion"
                );
                Self::try_release_concurrency_key(
                    &self.authority,
                    &mut self.key_gate,
                    run_id,
                    inf.task_id,
                    target_state,
                );
            }

            // Increment completed counter only for terminal state transitions.
            // Suspended is not terminal — the run may be resumed later.
            if target_state.is_terminal() {
                result.completed += 1;
                if let Some(tid) = task_id {
                    // Notify the dependency gate so dependents can be unblocked.
                    self.notify_dependency_gate_terminal(tid, current_time)?;
                }
            }

            // Fire events for subscription matching.
            #[cfg(feature = "budget")]
            if let Some(tid) = task_id {
                self.fire_events_for_transition(tid, target_state)?;
            }
        }

        #[cfg(feature = "budget")]
        if let Some(inf) = self.in_flight.get(&run_id) {
            let task_id = inf.task_id;
            let consumption = self
                .projection()
                .get_attempt_history(&run_id)
                .and_then(|h| h.last())
                .and_then(|a| a.disposition.as_ref())
                .map(|r| r.disposition.consumption().to_vec())
                .unwrap_or_default();
            for c in &consumption {
                self.budget_tracker.consume(task_id, c.dimension, c.amount);
            }
            if !consumption.is_empty() {
                self.fire_budget_threshold_events(task_id)?;
            }
        }
        crate::waits::reconcile(&mut self.authority, current_time)
            .map_err(DispatchError::Authority)?;
        self.rebuild_key_gate()?;
        // Remove from in-flight tracking
        self.in_flight.remove(&run_id);
        Ok(())
    }

    /// Notifies the dependency gate when a task's run reaches a terminal state.
    ///
    /// When a task has completed (all runs terminal + at least one Completed),
    /// marks dependent tasks as eligible. When a task has permanently failed
    /// (all runs terminal, none Completed), marks dependent tasks as failed
    /// and cancels their non-terminal runs.
    fn notify_dependency_gate_terminal(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        // Check if all runs for this task are now terminal (O(R_task) via index).
        let all_runs_terminal =
            self.authority.projection().runs_for_task(task_id).all(|r| r.state().is_terminal());

        if !all_runs_terminal {
            return Ok(()); // Task still has in-flight or scheduled runs.
        }

        // All runs are terminal — mark in the hierarchy tracker for orphan prevention.
        self.hierarchy_tracker.mark_terminal(task_id);

        // Enqueue for GC: clean up in-memory structures once hierarchy cascade is done.
        self.pending_gc_tasks.insert(task_id);

        // Check if the task has at least one Completed run (O(R_task) via index).
        let has_completed = self
            .authority
            .projection()
            .runs_for_task(task_id)
            .any(|r| r.state() == RunState::Completed);

        if has_completed {
            // Task succeeded — notify gate so dependents become eligible.
            // The gate update is in-memory; satisfaction is reconstructed at
            // recovery from the projection (which tasks have Completed runs).
            let newly_eligible = self.dependency_gate.notify_completed(task_id);
            if !newly_eligible.is_empty() {
                tracing::debug!(
                    task_id = %task_id,
                    newly_eligible = newly_eligible.len(),
                    "dependency gate: prerequisite satisfied, dependents now eligible"
                );
            }
        } else {
            // Task permanently failed — cascade failure and cancel blocked runs.
            let newly_blocked = self.dependency_gate.notify_failed(task_id);
            for blocked_id in newly_blocked {
                tracing::debug!(
                    failed_prerequisite = %task_id,
                    blocked_task = %blocked_id,
                    "dependency gate: cascading failure to dependent task"
                );
                // Cancel all non-terminal runs of the permanently blocked task (O(R_task)).
                let runs_to_cancel: Vec<_> = self
                    .authority
                    .projection()
                    .runs_for_task(blocked_id)
                    .filter(|r| !r.state().is_terminal())
                    .map(|r| (r.id(), r.state()))
                    .collect();
                for (run_id, prev_state) in runs_to_cancel {
                    self.cancel_run_and_release_key(run_id, blocked_id, prev_state, current_time)?;
                }
            }
        }

        Ok(())
    }

    /// Cascades cancellation from canceled tasks to their non-terminal descendants.
    ///
    /// Called each tick (step 0c). For each task marked as canceled in the projection
    /// that has non-terminal descendants in the hierarchy tracker, this method:
    /// 1. Marks the canceled ancestor as terminal in the tracker (its runs were
    ///    already canceled by the control API or dependency cascade).
    /// 2. Collects all non-terminal descendants via `collect_cancellation_cascade`.
    /// 3. WAL-appends `TaskCancel` + `RunStateTransition → Canceled` for each
    ///    descendant that is not yet canceled.
    /// 4. Marks each newly-canceled descendant as terminal in the tracker.
    ///
    /// The self-quenching property: once all descendants are terminal, repeated
    /// calls return immediately with no WAL writes.
    fn cascade_hierarchy_cancellations(&mut self, current_time: u64) -> Result<(), DispatchError> {
        let canceled_task_ids: Vec<TaskId> =
            self.pending_hierarchy_cascade.iter().copied().collect();

        let mut completed_cascades: Vec<TaskId> = Vec::new();

        for canceled_id in canceled_task_ids {
            // Mark the canceled ancestor as terminal if all its runs are terminal.
            let all_runs_terminal = self
                .authority
                .projection()
                .runs_for_task(canceled_id)
                .all(|r| r.state().is_terminal());
            if all_runs_terminal {
                self.hierarchy_tracker.mark_terminal(canceled_id);
            }

            let cascade = self.hierarchy_tracker.collect_cancellation_cascade(canceled_id);
            if cascade.is_empty() {
                completed_cascades.push(canceled_id);
                continue;
            }

            for descendant_id in cascade {
                tracing::debug!(
                    canceled_ancestor = %canceled_id,
                    descendant = %descendant_id,
                    "hierarchy: cascading cancellation to descendant"
                );

                // Cancel the descendant task if not yet canceled (idempotent guard).
                if !self.authority.projection().is_task_canceled(descendant_id) {
                    let seq = self.next_sequence()?;
                    let _ = self
                        .authority
                        .submit_command(
                            MutationCommand::TaskCancel(TaskCancelCommand::new(
                                seq,
                                descendant_id,
                                current_time,
                            )),
                            DurabilityPolicy::Immediate,
                        )
                        .map_err(DispatchError::Authority)?;
                    // Descendant may itself have children — enqueue for cascade.
                    self.pending_hierarchy_cascade.insert(descendant_id);
                }

                // Cancel all non-terminal runs of the descendant (O(R_task) via index).
                let runs_to_cancel: Vec<_> = self
                    .authority
                    .projection()
                    .runs_for_task(descendant_id)
                    .filter(|r| !r.state().is_terminal())
                    .map(|r| (r.id(), r.state()))
                    .collect();

                for (run_id, prev_state) in runs_to_cancel {
                    self.cancel_run_and_release_key(
                        run_id,
                        descendant_id,
                        prev_state,
                        current_time,
                    )?;
                }

                // All descendant runs are now canceled — mark terminal in tracker.
                self.hierarchy_tracker.mark_terminal(descendant_id);
            }
        }

        // Remove completed cascades (tasks whose cascade returned empty).
        for task_id in completed_cascades {
            self.pending_hierarchy_cascade.remove(&task_id);
        }

        Ok(())
    }

    /// Garbage-collects fully-terminal tasks from in-memory data structures.
    ///
    /// Called each tick (step 0c-gc), after hierarchy cascades complete. For each
    /// task in `pending_gc_tasks` whose cascade has quenched (no non-terminal
    /// descendants), removes it from the DependencyGate, HierarchyTracker,
    /// BudgetTracker, SubscriptionRegistry, and CronScheduleCache.
    fn gc_terminal_tasks(&mut self) {
        let candidates: Vec<TaskId> = self.pending_gc_tasks.iter().copied().collect();

        for task_id in candidates {
            // Only GC once the hierarchy cascade has fully quenched for this task.
            // If collect_cancellation_cascade returns non-empty, some descendants
            // are still non-terminal; skip for now.
            if !self.hierarchy_tracker.collect_cancellation_cascade(task_id).is_empty() {
                continue;
            }

            self.pending_gc_tasks.remove(&task_id);

            // GC the task from all in-memory structures.
            self.dependency_gate.gc_task(task_id);
            self.hierarchy_tracker.gc_subtree(task_id);

            #[cfg(feature = "budget")]
            self.budget_tracker.gc_task(task_id);

            #[cfg(feature = "budget")]
            self.subscription_registry.gc_task(task_id);

            #[cfg(feature = "workflow")]
            self.cron_schedule_cache.remove(task_id);
        }
    }

    /// Derives new cron runs to maintain the rolling window.
    ///
    /// Called each tick (step 0d). For each task with [`RunPolicy::Cron`], checks
    /// how many non-terminal runs exist. If below [`actionqueue_engine::derive::cron::CRON_WINDOW_SIZE`],
    /// derives additional runs via [`actionqueue_engine::derive::cron::derive_cron`] and WAL-appends them.
    ///
    /// Respects `CronPolicy::max_occurrences`: stops deriving once the total
    /// run count for the task reaches the configured maximum.
    ///
    /// Self-quenching: tasks with no derivable occurrences remaining are skipped.
    #[cfg(feature = "workflow")]
    fn derive_cron_runs(&mut self, current_time: u64) -> Result<(), DispatchError> {
        use actionqueue_core::task::run_policy::RunPolicy;
        use actionqueue_engine::derive::cron::{derive_cron_cached, CRON_WINDOW_SIZE};

        // Collect cron task IDs and their policies without holding the projection borrow.
        let cron_tasks: Vec<(TaskId, actionqueue_core::task::run_policy::CronPolicy)> = self
            .authority
            .projection()
            .task_records()
            .filter_map(|tr| {
                if let RunPolicy::Cron(ref policy) = *tr.task_spec().run_policy() {
                    Some((tr.task_spec().id(), policy.clone()))
                } else {
                    None
                }
            })
            .collect();

        for (task_id, policy) in cron_tasks {
            // Skip canceled tasks.
            if self.authority.projection().is_task_canceled(task_id) {
                continue;
            }

            // Clone run instances so we don't hold a borrow from self.authority
            // while calling self.cron_schedule_cache (different field, but method
            // calls take &self and Rust can't always split-borrow through them).
            let all_runs: Vec<RunInstance> =
                self.authority.projection().runs_for_task(task_id).cloned().collect();

            let total_derived = u32::try_from(all_runs.len()).unwrap_or(u32::MAX);
            let non_terminal_count =
                u32::try_from(all_runs.iter().filter(|r| !r.state().is_terminal()).count())
                    .unwrap_or(u32::MAX);

            // Check max_occurrences cap.
            if let Some(max) = policy.max_occurrences() {
                if total_derived >= max {
                    continue; // All allowed occurrences already derived.
                }
            }

            let to_derive = CRON_WINDOW_SIZE.saturating_sub(non_terminal_count);
            if to_derive == 0 {
                continue;
            }

            // Cap to_derive by remaining max_occurrences budget.
            let to_derive = if let Some(max) = policy.max_occurrences() {
                to_derive.min(max.saturating_sub(total_derived))
            } else {
                to_derive
            };
            if to_derive == 0 {
                continue;
            }

            // Find the latest scheduled_at among all existing runs for this task.
            // New occurrences are derived strictly after this timestamp, preventing
            // duplicate runs for already-scheduled time slots.
            let last_scheduled_at = all_runs
                .iter()
                .map(|r| r.scheduled_at())
                .max()
                .unwrap_or_else(|| current_time.saturating_sub(1));

            // Two-phase cache access to avoid borrow-checker conflicts:
            // Phase 1: ensure schedule is cached (mutable borrow ends after this call).
            self.cron_schedule_cache.ensure(task_id, &policy);
            // Phase 2: immutable borrow of cache during derive only.
            let schedule =
                self.cron_schedule_cache.get(task_id).expect("schedule was just ensured");
            let new_runs =
                derive_cron_cached(task_id, schedule, last_scheduled_at, current_time, to_derive)
                    .map_err(DispatchError::Derivation)?;

            if new_runs.is_empty() {
                continue; // No upcoming occurrences (finite schedule exhausted).
            }

            tracing::debug!(
                %task_id,
                count = new_runs.len(),
                "cron: deriving rolling window runs"
            );

            for run in new_runs {
                let seq = self.next_sequence()?;
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::RunCreate(RunCreateCommand::new(seq, run)),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
            }
        }

        Ok(())
    }

    /// Recover elapsed ownership before accepting results or renewing leases. A late
    /// result can never select the execution to close or contribute proposed effects.
    fn recover_expired_executions(
        &mut self,
        result: &mut TickResult,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        let expired = crate::waits::recover_expired_execution(&mut self.authority, current_time)
            .map_err(DispatchError::Authority)?;
        if expired.is_empty() {
            return Ok(());
        }
        self.dependency_gate = build_dependency_gate(self.projection());
        self.hierarchy_tracker = build_hierarchy_tracker(self.projection());
        for (run_id, attempt_id) in expired {
            // Only retire the slot associated with the recovered attempt. A newer
            // local execution, if present, keeps its ownership and reservation.
            if self.in_flight.get(&run_id).is_some_and(|inf| Some(inf.attempt_id) == attempt_id) {
                self.in_flight.remove(&run_id);
            }
            let run = self.projection().get_run_instance(&run_id).expect("recovered run");
            let task_id = run.task_id();
            let target = run.state();
            if target.is_terminal() {
                result.completed += 1;
                self.notify_dependency_gate_terminal(task_id, current_time)?;
            }
            #[cfg(feature = "budget")]
            self.fire_events_for_transition(task_id, target)?;
        }
        self.rebuild_key_gate()?;
        Ok(())
    }

    /// Heartbeats in-flight leases approaching expiry.
    fn heartbeat_in_flight_leases(&mut self, current_time: u64) -> Result<(), DispatchError> {
        // Heartbeat when 1/3 of lease time remains. Integer division truncates;
        // the minimum threshold of 1 second prevents a zero-second threshold for
        // very short leases (lease_timeout_secs validated >= 3 in RuntimeConfig).
        let heartbeat_threshold = (self.lease_timeout_secs / 3).max(1);
        let run_ids_needing_heartbeat: Vec<actionqueue_core::ids::RunId> = self
            .in_flight
            .values()
            .filter(|inf| {
                self.projection().get_run_instance(&inf.run_id).is_some_and(|run| {
                    run.state() == RunState::Running
                        && run.current_attempt_id() == Some(inf.attempt_id)
                }) && self.projection().get_lease_metadata(&inf.run_id).is_some_and(|lease| {
                    lease.owner() == self.identity.identity()
                        && current_time < lease.expiry()
                        && current_time.saturating_add(heartbeat_threshold) >= lease.expiry()
                        && self
                            .projection()
                            .get_attempt_history(&inf.run_id)
                            .and_then(|history| history.last())
                            .and_then(|attempt| attempt.accepted_start())
                            .is_some_and(|start| {
                                start.fence.owner().as_str() == lease.owner()
                                    && start.fence.granted_at_sequence()
                                        == lease.granted_at_sequence()
                            })
                })
            })
            .map(|inf| inf.run_id)
            .collect();

        for run_id in run_ids_needing_heartbeat {
            let new_expiry = current_time.saturating_add(self.lease_timeout_secs);
            let (attempt_id, attempt_number, max_attempts) = self
                .in_flight
                .get(&run_id)
                .map(|inf| (inf.attempt_id, inf.attempt_number, inf.max_attempts))
                .unwrap_or_default();
            tracing::debug!(
                %run_id, %attempt_id, attempt_number, max_attempts,
                new_expiry, "lease heartbeat extended"
            );
            let seq = self.next_sequence()?;
            let _ = self
                .authority
                .submit_command(
                    MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(
                        seq,
                        run_id,
                        self.identity.identity(),
                        new_expiry,
                        current_time,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;

            if let Some(inf) = self.in_flight.get_mut(&run_id) {
                inf.lease_expiry = new_expiry;
            }
        }

        Ok(())
    }

    /// Durably cancels a non-terminal run, then releases its concurrency key.
    ///
    /// Every cancellation cascade goes through this method so that release is
    /// inseparable from cancellation for runs without an executing worker. The
    /// key is released only when this run actually holds it: Scheduled and
    /// Ready runs never acquired one, so they release nothing and emit no
    /// warning.
    ///
    /// A run whose worker is still executing (present in `in_flight`) keeps
    /// the key. Releasing it here would let a competitor start under the same
    /// key while the worker runs, violating the mutual exclusion the key
    /// exists for. The canceled worker receives no further lease heartbeats;
    /// observing its result releases the slot and key without changing the
    /// cancellation history. Other stale worker dispositions remain AQ-08.
    fn cancel_run_and_release_key(
        &mut self,
        run_id: RunId,
        task_id: TaskId,
        prev_state: RunState,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    prev_state,
                    RunState::Canceled,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        if self.in_flight.contains_key(&run_id) {
            tracing::debug!(
                %run_id, %task_id, ?prev_state,
                "run canceled while in flight; concurrency key held until the worker is reconciled"
            );
            return Ok(());
        }
        let holds_key = self
            .authority
            .projection()
            .get_task(&task_id)
            .and_then(|task| task.constraints().concurrency_key().map(ConcurrencyKey::new))
            .is_some_and(|key| self.key_gate.key_holder(&key) == Some(run_id));
        if holds_key {
            Self::try_release_concurrency_key(
                &self.authority,
                &mut self.key_gate,
                run_id,
                task_id,
                RunState::Canceled,
            );
        }
        Ok(())
    }

    /// Attempts to release the concurrency key for a run entering a terminal or
    /// RetryWait, Suspended, or Awaiting state, depending on the task's policies.
    fn try_release_concurrency_key(
        authority: &StorageMutationAuthority<W, ReplayReducer>,
        key_gate: &mut KeyGate,
        run_id: RunId,
        task_id: TaskId,
        target_state: RunState,
    ) {
        let should_release = if target_state.is_terminal() {
            true
        } else if target_state == RunState::Awaiting {
            // AQ-03 adds the persisted per-task wait policy (AQ-ADR-009). Until
            // then the accessor always returns the default, ReleaseWhileAwaiting,
            // so HoldWhileAwaiting is not selectable here yet.
            authority.projection().get_task(&task_id).is_some_and(|task| {
                task.constraints().concurrency_key_wait_policy().releases_while_awaiting()
            })
        } else if target_state == RunState::RetryWait || target_state == RunState::Suspended {
            // Suspended follows the same hold policy as RetryWait: the run is paused
            // and may resume, so whether the key is held depends on the task's policy.
            authority
                .projection()
                .get_task(&task_id)
                .map(|task| {
                    task.constraints().concurrency_key_hold_policy()
                        == ConcurrencyKeyHoldPolicy::ReleaseOnRetry
                })
                .unwrap_or(false)
        } else {
            return;
        };

        if !should_release {
            return;
        }

        let Some(task) = authority.projection().get_task(&task_id) else {
            tracing::warn!(%run_id, %task_id, "skipping key release: task not found");
            return;
        };

        let Some(key_str) = task.constraints().concurrency_key() else {
            return; // No concurrency key — nothing to release
        };

        let key = ConcurrencyKey::new(key_str);
        match key_gate.release(key, run_id) {
            ReleaseResult::Released { .. } => {}
            ReleaseResult::NotHeld { key: k, attempting_run_id } => {
                tracing::warn!(
                    %attempting_run_id, key = %k,
                    "concurrency key release failed — key not held by this run"
                );
            }
        }
    }

    /// Advances the state machine one step.
    ///
    /// A tick performs:
    /// 0a. Drain workflow submissions from handlers (non-blocking)
    /// 0b. Drain completed worker results (non-blocking)
    /// 0c. Cascade hierarchy cancellations to descendants of canceled tasks
    /// 0c-gc. GC terminal tasks from in-memory data structures
    /// 0d. Derive new cron runs to maintain the rolling window (workflow feature)
    /// 0f. Check actor heartbeat timeouts (actor feature)
    /// 1. Heartbeat in-flight leases approaching expiry
    /// 2. Check engine paused state — skip if paused
    /// 3. Promote Scheduled → Ready (time-based)
    /// 4. Promote RetryWait → Ready (backoff-based)
    /// 5. Select ready runs (priority-FIFO-RunId)
    /// 6. For each selected: check concurrency key gate → lease → dispatch (spawn worker)
    pub async fn tick(&mut self) -> Result<TickResult, DispatchError> {
        tracing::trace!("dispatch tick starting");
        let mut result = TickResult::default();
        let current_time = self.clock.now();
        let seq_before_tick = self.authority.projection().latest_sequence();

        self.recover_expired_executions(&mut result, current_time)?;

        // Step 0b: Drain completed worker results
        self.drain_completed_results(&mut result, current_time)?;

        // Step 0e: Signal cancellation to in-flight runs whose budget is exhausted.
        #[cfg(feature = "budget")]
        self.signal_budget_exhaustion_cancellations();

        // Step 0c: Cascade hierarchy cancellations from canceled tasks to descendants
        self.cascade_hierarchy_cancellations(current_time)?;

        // Step 0c-gc: GC terminal tasks from in-memory data structures.
        // Runs after cascade so that descendants' GC is not triggered prematurely.
        self.gc_terminal_tasks();

        // Step 0d: Derive new cron runs to maintain the rolling window

        // Step 0f: Check actor heartbeat timeouts (actor feature only).
        #[cfg(feature = "actor")]
        self.check_actor_heartbeat_timeouts()?;
        #[cfg(feature = "workflow")]
        self.derive_cron_runs(current_time)?;

        // Step 1: Heartbeat in-flight leases approaching expiry
        self.heartbeat_in_flight_leases(current_time)?;

        self.reconcile_waits().map_err(DispatchError::Authority)?;
        self.rebuild_key_gate()?;
        // Step 2: Check engine paused state or draining mode — skip promotion and dispatch.
        if self.authority.projection().is_engine_paused() {
            result.engine_paused = true;
            let events_this_tick =
                self.authority.projection().latest_sequence().saturating_sub(seq_before_tick);
            self.events_since_last_snapshot += events_this_tick;
            self.maybe_write_snapshot(current_time)?;
            return Ok(result);
        }

        if self.draining {
            let events_this_tick =
                self.authority.projection().latest_sequence().saturating_sub(seq_before_tick);
            self.events_since_last_snapshot += events_this_tick;
            self.maybe_write_snapshot(current_time)?;
            return Ok(result);
        }

        // Step 3: Promote Scheduled → Ready (time-based + dependency gate)
        // Runs whose task has unsatisfied dependencies stay in Scheduled
        // until the dependency gate marks them eligible.
        let scheduled_runs: Vec<RunInstance> = self
            .authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == RunState::Scheduled)
            .filter(|r| self.dependency_gate.is_eligible(r.task_id()))
            .cloned()
            .collect();

        if !scheduled_runs.is_empty() {
            let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);
            let promo_result = promote_scheduled_to_ready_via_authority(
                &scheduled_index,
                PromotionParams::new(
                    current_time,
                    self.next_sequence()?,
                    current_time,
                    DurabilityPolicy::Immediate,
                ),
                &mut self.authority,
            )
            .map_err(|e| DispatchError::ScheduledPromotion(Box::new(e)))?;
            result.promoted_scheduled = promo_result.outcomes().len();
        }

        // Step 3b: Promote Scheduled → Ready for subscription-triggered tasks
        // (bypasses the scheduled_at time check).
        #[cfg(feature = "budget")]
        {
            let promoted = self.promote_subscription_triggered_scheduled(current_time)?;
            result.promoted_scheduled += promoted;
        }

        // Step 4: Promote RetryWait → Ready (backoff-based)
        let retry_waiting: Vec<RunInstance> = self
            .authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == RunState::RetryWait)
            .cloned()
            .collect();

        if !retry_waiting.is_empty() {
            let promo = promote_retry_wait_to_ready(&retry_waiting, current_time, &*self.backoff)
                .map_err(DispatchError::RetryPromotion)?;
            for run in promo.promoted() {
                let seq = self.next_sequence()?;
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            seq,
                            run.id(),
                            RunState::RetryWait,
                            RunState::Ready,
                            current_time,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
            }
            result.promoted_retry_wait = promo.promoted().len();
        }

        // Step 5: Select ready runs
        let ready_runs: Vec<RunInstance> = self
            .authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == RunState::Ready)
            .cloned()
            .collect();

        if ready_runs.is_empty() {
            let events_this_tick =
                self.authority.projection().latest_sequence().saturating_sub(seq_before_tick);
            self.events_since_last_snapshot += events_this_tick;
            self.maybe_write_snapshot(current_time)?;
            return Ok(result);
        }

        let ready_index = ReadyIndex::from_runs(ready_runs);
        let inputs = ready_inputs_from_index(&ready_index);
        let selection = select_ready_runs(&inputs);

        // Step 6: For each selected, check concurrency gate → lease → dispatch
        let available_slots = self.max_concurrent.saturating_sub(self.in_flight.len());
        let mut dispatched = 0usize;
        for run in selection.into_selected() {
            if dispatched >= available_slots {
                break;
            }

            // Look up task ONCE, before any state transitions
            let task = match self.authority.projection().get_task(&run.task_id()) {
                Some(t) => t,
                None => {
                    tracing::warn!(
                        run_id = %run.id(),
                        task_id = %run.task_id(),
                        "skipping run: parent task not found in projection"
                    );
                    continue;
                }
            };

            // Cache payload and constraints before state transitions
            let payload = task.payload().to_vec();
            let constraints = task.constraints().clone();

            // Step 6a: Check budget gate — skip if any dimension is exhausted.
            #[cfg(feature = "budget")]
            {
                let gate = actionqueue_budget::BudgetGate::new(&self.budget_tracker);
                if !gate.can_dispatch(run.task_id()) {
                    tracing::debug!(
                        run_id = %run.id(),
                        task_id = %run.task_id(),
                        "skipping run: budget exhausted"
                    );
                    continue;
                }
            }

            // Check concurrency key gate (using cached task)
            let acquired_key = if let Some(key_str) = constraints.concurrency_key() {
                let key = actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key_str);
                match self.key_gate.acquire(key.clone(), run.id()) {
                    actionqueue_engine::concurrency::key_gate::AcquireResult::Acquired {
                        ..
                    } => Some(key),
                    actionqueue_engine::concurrency::key_gate::AcquireResult::Occupied {
                        ..
                    } => continue,
                }
            } else {
                None
            };

            // Dispatch the run through WAL commands. If any step fails after
            // key acquisition, release the key before propagating the error
            // to avoid permanently blocking the concurrency key.
            match self.dispatch_single_run(&run, &constraints, current_time) {
                Ok((attempt_id, lease_expiry, attempt_number, max_attempts)) => {
                    // Advisory warning: Transactional tasks with retries may cause
                    // duplicate side effects.
                    if constraints.safety_level() == SafetyLevel::Transactional && max_attempts > 1
                    {
                        tracing::warn!(
                            run_id = %run.id(),
                            max_attempts,
                            "task has Transactional safety level with retries — retries may \
                             cause duplicate side effects"
                        );
                    }

                    // Track in-flight
                    let run_id = run.id();
                    let task_id = run.task_id();

                    // Create cancellation context BEFORE spawn_blocking so
                    // the dispatch loop retains a clone for in-flight
                    // suspension signaling (e.g. budget exhaustion).
                    #[cfg(feature = "budget")]
                    let (cancellation_ctx, cancel_ctx_clone) = {
                        let ctx = actionqueue_executor_local::handler::CancellationContext::new();
                        let clone = Some(ctx.clone());
                        (Some(ctx), clone)
                    };
                    #[cfg(not(feature = "budget"))]
                    let cancellation_ctx: Option<
                        actionqueue_executor_local::handler::CancellationContext,
                    > = None;

                    self.in_flight.insert(
                        run_id,
                        InFlightRun {
                            run_id,
                            attempt_id,
                            task_id,
                            lease_expiry,
                            attempt_number,
                            max_attempts,
                            #[cfg(feature = "budget")]
                            cancellation_context: cancel_ctx_clone,
                        },
                    );

                    let children = build_children_snapshot(self.authority.projection(), task_id);

                    // Spawn worker via spawn_blocking with timeout enforcement.
                    tracing::info!(
                        %run_id, %attempt_id, attempt_number,
                        "spawning handler for attempt"
                    );
                    let runner = Arc::clone(&self.runner);
                    let result_tx = self.result_tx.clone();

                    let resume_context =
                        self.authority.projection().attempt_resume(run_id, attempt_id);
                    let causal_context = self
                        .authority
                        .projection()
                        .task_admission(task_id)
                        .map(|a| a.request().causal_context().clone());
                    let lease_fence = self
                        .authority
                        .projection()
                        .get_attempt_history(&run_id)
                        .and_then(|history| history.last())
                        .and_then(|attempt| attempt.accepted_start())
                        .expect("dispatch accepted this attempt start")
                        .fence
                        .clone();
                    let failure_attempt_count = self
                        .projection()
                        .get_run_instance(&run_id)
                        .unwrap()
                        .failure_attempt_count();
                    tokio::task::spawn_blocking(move || {
                        let request = ExecutorRequest {
                            failure_attempt_count,
                            lease_fence,
                            resume_context,
                            causal_context,
                            run_id,
                            attempt_id,
                            payload,
                            constraints,
                            attempt_number,

                            children,
                            cancellation_context: cancellation_ctx,
                        };
                        let outcome = runner.run_attempt(request);

                        let worker_result = WorkerResult {
                            lease_fence: outcome.lease_fence,
                            run_id,
                            attempt_id,
                            disposition: outcome.disposition,
                        };

                        if result_tx.send(worker_result).is_err() {
                            tracing::error!(
                                %run_id,
                                "worker result channel closed — dispatch loop may have crashed"
                            );
                        }
                    });

                    dispatched += 1;
                }
                Err(e) => {
                    // Release concurrency key on dispatch failure to avoid
                    // permanently blocking future runs with the same key.
                    if let Some(key) = acquired_key {
                        let _ = self.key_gate.release(key, run.id());
                    }
                    tracing::error!(
                        run_id = %run.id(),
                        error = %e,
                        "dispatch failed for run, skipping"
                    );
                    continue;
                }
            }
        }

        result.dispatched = dispatched;

        // Snapshot check: count events appended during this tick and write
        // a snapshot when the cumulative count exceeds the threshold.
        let events_this_tick =
            self.authority.projection().latest_sequence().saturating_sub(seq_before_tick);
        self.events_since_last_snapshot += events_this_tick;
        self.maybe_write_snapshot(current_time)?;

        Ok(result)
    }

    /// Dispatches a single run through the WAL command sequence:
    /// Ready→Leased transition, lease acquire, Leased→Running transition, attempt start.
    ///
    /// Returns (attempt_id, lease_expiry, attempt_number, max_attempts) on success.
    fn dispatch_single_run(
        &mut self,
        run: &RunInstance,
        constraints: &actionqueue_core::task::constraints::TaskConstraints,
        current_time: u64,
    ) -> Result<(AttemptId, u64, u32, u32), DispatchError> {
        // Transition Ready → Leased
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run.id(),
                    RunState::Ready,
                    RunState::Leased,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Acquire lease
        let lease_expiry = current_time.saturating_add(self.lease_timeout_secs);
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
                    seq,
                    run.id(),
                    self.identity.identity(),
                    lease_expiry,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Transition Leased → Running
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run.id(),
                    RunState::Leased,
                    RunState::Running,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Record attempt start
        let attempt_id = AttemptId::new();
        let seq = self.next_sequence()?;
        let started = self
            .authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run.id(),
                    attempt_id,
                    current_time,
                    {
                        let l = self
                            .authority
                            .projection()
                            .get_lease_metadata(&run.id())
                            .expect("acquired lease");
                        actionqueue_core::mutation::LeaseFence::new(
                            l.owner().into(),
                            l.granted_at_sequence(),
                        )
                    },
                    self.authority.projection().pending_resume(run.id()).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        if !matches!(
            started.applied(),
            actionqueue_core::mutation::AppliedMutation::AttemptStart { .. }
        ) {
            return Err(DispatchError::StateInconsistency {
                run_id: run.id(),
                context: "attempt start was already accepted; worker was not spawned".into(),
            });
        }
        let max_attempts = constraints.max_attempts();
        let attempt_number =
            run.attempt_count().checked_add(1).ok_or(DispatchError::SequenceOverflow)?;

        Ok((attempt_id, lease_expiry, attempt_number, max_attempts))
    }

    /// Writes a snapshot if the cumulative event count exceeds the configured threshold.
    fn maybe_write_snapshot(&mut self, current_time: u64) -> Result<(), DispatchError> {
        let threshold = match self.snapshot_event_threshold {
            Some(t) => t,
            None => return Ok(()),
        };
        let _path = match &self.snapshot_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        if self.events_since_last_snapshot < threshold {
            return Ok(());
        }

        let snapshot = build_snapshot_from_projection(self.authority.projection(), current_time)
            .map_err(DispatchError::SnapshotBuild)?;

        let mut writer =
            SnapshotFsWriter::new(self.authority.store_session().ok_or_else(|| {
                DispatchError::SnapshotInit("snapshot requires a store session".into())
            })?)
            .map_err(|e| DispatchError::SnapshotInit(format!("{e}")))?;
        writer.write(&snapshot).map_err(DispatchError::SnapshotWrite)?;
        writer.close().map_err(DispatchError::SnapshotWrite)?;

        self.events_since_last_snapshot = 0;
        tracing::info!(
            wal_sequence = snapshot.metadata.wal_sequence,
            task_count = snapshot.metadata.task_count,
            run_count = snapshot.metadata.run_count,
            "automatic snapshot written"
        );

        Ok(())
    }

    /// Cancels all non-terminal runs of tasks whose dependencies have permanently
    /// failed. Called once after bootstrap to close the recovery gap where a crash
    /// occurred between a prerequisite failing and the cascade cancellation being
    /// committed.
    fn cancel_dependency_failed_runs(&mut self, current_time: u64) -> Result<(), DispatchError> {
        let failed_tasks: Vec<TaskId> = self
            .authority
            .projection()
            .task_records()
            .map(|tr| tr.task_spec().id())
            .filter(|&tid| self.dependency_gate.is_dependency_failed(tid))
            .collect();
        for task_id in failed_tasks {
            let runs_to_cancel: Vec<_> = self
                .authority
                .projection()
                .runs_for_task(task_id)
                .filter(|r| !r.state().is_terminal())
                .map(|r| (r.id(), r.state()))
                .collect();
            for (run_id, prev_state) in runs_to_cancel {
                self.cancel_run_and_release_key(run_id, task_id, prev_state, current_time)?;
            }
        }
        Ok(())
    }

    /// Loops `tick()` until no work remains (no in-flight, no promotions, no dispatches).
    pub async fn run_until_idle(&mut self) -> Result<RunSummary, DispatchError> {
        let mut summary = RunSummary::default();

        let current_time = self.clock.now();
        self.cancel_dependency_failed_runs(current_time)?;

        loop {
            let tick = self.tick().await?;
            summary.ticks += 1;
            summary.total_dispatched += tick.dispatched;
            summary.total_completed += tick.completed;

            // Idle when no promotions happened, nothing was dispatched,
            // and no in-flight work remains
            if tick.promoted_scheduled == 0
                && tick.promoted_retry_wait == 0
                && tick.dispatched == 0
                && self.in_flight.is_empty()
            {
                break;
            }

            // If there are in-flight tasks but nothing else to do, wait for
            // a worker result instead of spinning. The result is stashed in
            // pending_result so drain_completed_results() processes it on
            // the next tick iteration.
            if tick.promoted_scheduled == 0
                && tick.promoted_retry_wait == 0
                && tick.dispatched == 0
                && !self.in_flight.is_empty()
            {
                let received = tokio::select! {
                    result=self.result_rx.recv()=>result,
                    _=tokio::time::sleep(std::time::Duration::from_millis(100))=>{ continue; }
                };
                if let Some(worker_result) = received {
                    self.pending_result = Some(worker_result);
                } else {
                    // All worker senders dropped — no more results possible.
                    // Clear in_flight to prevent infinite loop.
                    tracing::warn!(
                        orphaned_runs = self.in_flight.len(),
                        "worker result channel closed with in-flight runs"
                    );
                    self.in_flight.clear();
                    break;
                }
            } else if tick.dispatched > 0 {
                // Yield to the runtime when work was dispatched, preventing
                // CPU spinning when the dispatch loop is actively processing.
                tokio::task::yield_now().await;
            }
        }

        Ok(summary)
    }

    /// Begins graceful drain: stops promoting and dispatching new work,
    /// but continues processing in-flight results and heartbeating leases.
    pub fn start_drain(&mut self) {
        self.draining = true;
    }

    /// Drains in-flight work until idle or the timeout expires.
    pub async fn drain_until_idle(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<RunSummary, DispatchError> {
        self.start_drain();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut summary = RunSummary::default();

        loop {
            if self.in_flight.is_empty() {
                break;
            }

            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    remaining_in_flight = self.in_flight.len(),
                    "drain timeout expired with in-flight runs"
                );
                break;
            }

            let tick = self.tick().await?;
            summary.ticks += 1;
            summary.total_completed += tick.completed;

            if tick.completed == 0 && !self.in_flight.is_empty() {
                // Wait for a result with the remaining timeout.
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(
                    remaining.min(std::time::Duration::from_millis(100)),
                    self.result_rx.recv(),
                )
                .await
                {
                    Ok(Some(result)) => {
                        self.pending_result = Some(result);
                    }
                    Ok(None) => {
                        self.in_flight.clear();
                        break;
                    }
                    Err(_) if tokio::time::Instant::now() < deadline => continue,
                    Err(_) => {
                        tracing::warn!(
                            remaining_in_flight = self.in_flight.len(),
                            "drain timeout expired waiting for worker results"
                        );
                        break;
                    }
                }
            }
        }

        Ok(summary)
    }

    /// Idempotent convenience admission using stable task/<uuid> key, trace, and correlation.
    /// Retain the task UUID on retry. Outbox callers should supply explicit ensure_task requests.
    pub fn submit_task(&mut self, spec: TaskSpec) -> Result<EnsureTaskOutcome, AdmissionError> {
        self.ensure_task(
            EnsureTaskRequest::for_task(spec, vec![]).map_err(AdmissionError::Rejected)?,
        )
    }
    /// Inspect a signal in its tenant namespace, including retired records.
    pub fn get_signal(
        &self,
        tenant: Option<actionqueue_core::ids::TenantId>,
        id: &actionqueue_core::ids::SignalId,
    ) -> Option<&actionqueue_storage::mutation::signal::SignalRecord> {
        self.projection().signals().get_signal(tenant, id)
    }
    /// Bounded sequence-paginated inspection with an exclusive cursor.
    pub fn list_signals(
        &self,
        tenant: Option<actionqueue_core::ids::TenantId>,
        after: actionqueue_core::ids::SignalSequence,
        limit: usize,
    ) -> Vec<&actionqueue_storage::mutation::signal::SignalRecord> {
        self.projection().signals().list_signals(tenant, after, limit)
    }
    /// Resident signal and live capacity-rejection counters.
    pub fn signal_statistics(&self) -> actionqueue_storage::recovery::signals::SignalStatistics {
        self.authority.signal_statistics()
    }
    /// Admits a durable signal with host-attested scope and attribution.
    pub fn admit_signal(
        &mut self,
        request: actionqueue_core::continuation::AdmitSignalRequest,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<
        actionqueue_core::continuation::AdmitSignalOutcome,
        crate::signals::SignalAdmissionError,
    > {
        let before = self.projection().latest_sequence();
        let result =
            crate::signals::admit_signal(&mut self.authority, request, ingress, &self.clock);
        if self.projection().latest_sequence() != before {
            self.refresh_coordination();
        }
        result
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn pin_signal(
        &mut self,
        signal_id: actionqueue_core::ids::SignalId,
        pin_id: actionqueue_core::continuation::SignalPinId,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        crate::signals::pin_signal(&mut self.authority, signal_id, pin_id, ingress, &self.clock)
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn unpin_signal(
        &mut self,
        signal_id: actionqueue_core::ids::SignalId,
        pin_id: actionqueue_core::continuation::SignalPinId,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        crate::signals::unpin_signal(&mut self.authority, signal_id, pin_id, ingress, &self.clock)
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn retire_signals(
        &mut self,
        sequences: Vec<actionqueue_core::ids::SignalSequence>,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        crate::signals::retire_signals(&mut self.authority, sequences, ingress, &self.clock)
    }
    /// Commits one complete admission and publishes scheduling caches before another tick.
    pub fn ensure_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> Result<EnsureTaskOutcome, AdmissionError> {
        let outcome = crate::admission::ensure_task(&mut self.authority, request, &self.clock)?;
        if outcome.is_created() {
            self.dependency_gate = build_dependency_gate(self.authority.projection());
            self.hierarchy_tracker = build_hierarchy_tracker(self.authority.projection());
        }
        Ok(outcome)
    }

    /// Declares a DAG dependency: `task_id` may not promote until all `prereqs` complete.
    ///
    /// Validates cycle-freedom and prerequisite existence before WAL-appending.
    pub fn declare_dependency(
        &mut self,
        task_id: TaskId,
        prereqs: Vec<TaskId>,
    ) -> Result<(), DispatchError> {
        // Cycle check.
        self.dependency_gate
            .check_cycle(task_id, &prereqs)
            .map_err(DispatchError::DependencyCycle)?;

        // All prerequisites must be known.
        for prereq_id in &prereqs {
            if self.authority.projection().get_task(prereq_id).is_none() {
                return Err(DispatchError::SubmissionRejected {
                    task_id,
                    context: format!("prerequisite {prereq_id} not in projection"),
                });
            }
        }

        let seq = self.next_sequence()?;
        let current_time = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(
                    seq,
                    task_id,
                    prereqs.clone(),
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Update in-memory gate (safe since cycle was already checked).
        let _ = self.dependency_gate.declare(task_id, prereqs.clone());

        // Catch up prerequisites that already completed (e.g., when declaring
        // a dependency AFTER the prerequisite ran). GC may have removed the
        // completed task from `satisfied`; re-check the projection to restore it.
        for prereq_id in &prereqs {
            let runs: Vec<_> = self.authority.projection().runs_for_task(*prereq_id).collect();
            if !runs.is_empty() {
                let all_terminal = runs.iter().all(|r| r.state().is_terminal());
                let has_completed = runs.iter().any(|r| r.state() == RunState::Completed);
                if all_terminal && has_completed {
                    self.dependency_gate.force_satisfy(*prereq_id);
                } else if all_terminal && !has_completed {
                    self.dependency_gate.force_fail(*prereq_id);
                }
            }
        }
        // Re-evaluate satisfaction for task_id after prereq state is restored.
        self.dependency_gate.recompute_satisfaction_pub(task_id);

        Ok(())
    }

    /// Allocates a budget for a task/dimension pair and WAL-appends the event.
    ///
    /// Called by external callers (e.g. acceptance tests or the Caelum runtime)
    /// to establish the consumption ceiling before the task begins executing.
    #[cfg(feature = "budget")]
    pub fn allocate_budget(
        &mut self,
        task_id: TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
        limit: u64,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{BudgetAllocateCommand, MutationCommand as MC};
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MC::BudgetAllocate(BudgetAllocateCommand::new(
                    seq,
                    task_id,
                    dimension,
                    limit,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;
        self.budget_tracker.allocate(task_id, dimension, limit);
        Ok(())
    }

    /// Replenishes an exhausted budget and WAL-appends the event.
    ///
    /// After replenishment the budget gate will allow the blocked task to be
    /// dispatched again on the next tick.
    #[cfg(feature = "budget")]
    pub fn replenish_budget(
        &mut self,
        task_id: TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
        new_limit: u64,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{BudgetReplenishCommand, MutationCommand as MC};
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MC::BudgetReplenish(BudgetReplenishCommand::new(
                    seq,
                    task_id,
                    dimension,
                    new_limit,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;
        self.budget_tracker.replenish(task_id, dimension, new_limit);
        Ok(())
    }

    /// Query remaining budget for a task on a specific dimension.
    ///
    /// Returns `None` if no budget is allocated for this task+dimension.
    #[cfg(feature = "budget")]
    pub fn budget_remaining(
        &self,
        task_id: TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
    ) -> Option<u64> {
        self.budget_tracker.remaining(task_id, dimension)
    }

    /// Check if a budget dimension is exhausted for a task.
    ///
    /// Returns `false` if no budget is allocated (no budget = no limit).
    #[cfg(feature = "budget")]
    pub fn is_budget_exhausted(
        &self,
        task_id: TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
    ) -> bool {
        self.budget_tracker.is_exhausted(task_id, dimension)
    }

    /// Resumes a suspended run by WAL-appending a `RunResume` command.
    ///
    /// Transitions the run from `Suspended → Ready` so it will be dispatched
    /// on the next tick. Returns an error if the run is not currently suspended.
    #[cfg(feature = "budget")]
    pub fn resume_run(&mut self, run_id: RunId) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{MutationCommand as MC, RunResumeCommand};
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MC::RunResume(RunResumeCommand::new(seq, run_id, current_time)),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;
        Ok(())
    }

    /// Signals cancellation to in-flight runs whose budget is now exhausted.
    ///
    /// Called each tick after draining worker results and recording consumption.
    /// The handler observes the cancellation via its `CancellationContext` and
    /// cooperatively returns `Suspended`.
    #[cfg(feature = "budget")]
    fn signal_budget_exhaustion_cancellations(&self) {
        for inf in self.in_flight.values() {
            if self.budget_tracker.is_any_exhausted(inf.task_id) {
                if let Some(ref ctx) = inf.cancellation_context {
                    ctx.cancel();
                    tracing::debug!(
                        run_id = %inf.run_id,
                        task_id = %inf.task_id,
                        "budget exhausted: signaling cancellation to handler"
                    );
                }
            }
        }
    }

    /// Promotes Scheduled runs whose task has a triggered subscription.
    ///
    /// Bypasses the `scheduled_at` time check. Returns the number of runs promoted.
    /// After promotion the one-shot trigger is cleared.
    #[cfg(feature = "budget")]
    fn promote_subscription_triggered_scheduled(
        &mut self,
        current_time: u64,
    ) -> Result<usize, DispatchError> {
        let triggered: Vec<RunInstance> = self
            .authority
            .projection()
            .run_instances()
            .filter(|r| r.state() == RunState::Scheduled)
            .filter(|r| self.subscription_registry.is_triggered(r.task_id()))
            .filter(|r| self.dependency_gate.is_eligible(r.task_id()))
            .cloned()
            .collect();

        let count = triggered.len();
        for run in &triggered {
            let seq = self.next_sequence()?;
            let _ = self
                .authority
                .submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        seq,
                        run.id(),
                        RunState::Scheduled,
                        RunState::Ready,
                        current_time,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;
            self.subscription_registry.clear_triggered(run.task_id());
        }
        Ok(count)
    }

    /// Creates a new event subscription and WAL-appends it.
    ///
    /// The subscription is registered in the in-memory registry and will be
    /// matched against events fired after each tick's worker result processing.
    #[cfg(feature = "budget")]
    pub fn create_subscription(
        &mut self,
        task_id: TaskId,
        filter: actionqueue_core::subscription::EventFilter,
    ) -> Result<actionqueue_core::subscription::SubscriptionId, DispatchError> {
        use actionqueue_core::mutation::SubscriptionCreateCommand;
        let sub_id = actionqueue_core::subscription::SubscriptionId::new();
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
                    seq,
                    sub_id,
                    task_id,
                    filter.clone(),
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;
        self.subscription_registry.register(sub_id, task_id, filter);
        Ok(sub_id)
    }

    /// Fires a custom event and matches it against active subscriptions.
    ///
    /// Any subscriptions with a `Custom { key }` filter matching the event
    /// key are triggered. Triggered subscriptions cause their associated
    /// task's Scheduled runs to be promoted on the next tick.
    #[cfg(feature = "budget")]
    pub fn fire_custom_event(&mut self, key: String) -> Result<(), DispatchError> {
        let event = actionqueue_budget::ActionQueueEvent::CustomEvent { key };
        let matched = actionqueue_budget::check_event(&event, &self.subscription_registry);
        for sub_id in matched {
            self.trigger_subscription_durable(sub_id)?;
        }
        Ok(())
    }

    /// Fires events for state transitions and matches against subscriptions.
    ///
    /// Called from `process_worker_result` after a terminal state transition.
    /// Fires `RunChangedState` for every transition, and `TaskReachedTerminalSuccess`
    /// when all runs of a task are terminal with at least one Completed.
    #[cfg(feature = "budget")]
    fn fire_events_for_transition(
        &mut self,
        task_id: TaskId,
        new_state: RunState,
    ) -> Result<(), DispatchError> {
        use actionqueue_budget::{check_event, ActionQueueEvent};

        // Fire RunChangedState event.
        let event = ActionQueueEvent::RunChangedState { task_id, new_state };
        let matched = check_event(&event, &self.subscription_registry);
        for sub_id in matched {
            self.trigger_subscription_durable(sub_id)?;
        }

        // If terminal: check if task reached terminal success.
        if new_state.is_terminal() {
            self.fire_task_terminal_success_event(task_id)?;
        }
        Ok(())
    }

    /// Fires `TaskReachedTerminalSuccess` if all runs of the task are terminal
    /// and at least one is Completed.
    #[cfg(feature = "budget")]
    fn fire_task_terminal_success_event(&mut self, task_id: TaskId) -> Result<(), DispatchError> {
        use actionqueue_budget::{check_event, ActionQueueEvent};

        let all_terminal =
            self.authority.projection().runs_for_task(task_id).all(|r| r.state().is_terminal());
        if !all_terminal {
            return Ok(());
        }
        let any_completed = self
            .authority
            .projection()
            .runs_for_task(task_id)
            .any(|r| r.state() == RunState::Completed);
        if !any_completed {
            return Ok(());
        }
        let event = ActionQueueEvent::TaskReachedTerminalSuccess { task_id };
        let matched = check_event(&event, &self.subscription_registry);
        for sub_id in matched {
            self.trigger_subscription_durable(sub_id)?;
        }
        Ok(())
    }

    /// Fires budget threshold events after consumption is recorded.
    ///
    /// For each (task, dimension) that has a budget allocated, checks if
    /// the consumption percentage crossed any subscribed threshold.
    #[cfg(feature = "budget")]
    fn fire_budget_threshold_events(&mut self, task_id: TaskId) -> Result<(), DispatchError> {
        use actionqueue_budget::{check_event, ActionQueueEvent};
        use actionqueue_core::budget::BudgetDimension;

        for &dim in &[BudgetDimension::Token, BudgetDimension::CostCents, BudgetDimension::TimeSecs]
        {
            if let Some(pct) = self.budget_tracker.threshold_pct(task_id, dim) {
                let event =
                    ActionQueueEvent::BudgetThresholdCrossed { task_id, dimension: dim, pct };
                let matched = check_event(&event, &self.subscription_registry);
                for sub_id in matched {
                    self.trigger_subscription_durable(sub_id)?;
                }
            }
        }
        Ok(())
    }

    /// WAL-appends a `SubscriptionTriggered` event and triggers the
    /// subscription in-memory. This ensures subscription triggers survive
    /// crash recovery.
    #[cfg(feature = "budget")]
    fn trigger_subscription_durable(
        &mut self,
        subscription_id: actionqueue_core::subscription::SubscriptionId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::SubscriptionTriggerCommand;
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                MutationCommand::SubscriptionTrigger(SubscriptionTriggerCommand::new(
                    seq,
                    subscription_id,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;
        self.subscription_registry.trigger(subscription_id);
        Ok(())
    }

    // ── Actor feature methods ──────────────────────────────────────────────

    /// Registers a remote actor with the hub.
    #[cfg(feature = "actor")]
    pub fn register_actor(
        &mut self,
        registration: actionqueue_core::actor::ActorRegistration,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{ActorRegisterCommand, MutationCommand};

        let actor_id = registration.actor_id();
        let policy = actionqueue_core::actor::HeartbeatPolicy::with_default_multiplier(
            registration.heartbeat_interval_secs(),
        );
        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::ActorRegister(ActorRegisterCommand::new(
                    seq,
                    registration.clone(),
                    ts,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.actor_registry.register(registration);
        self.heartbeat_monitor.record_registration(actor_id, policy, ts);
        Ok(())
    }

    /// Deregisters a remote actor from the hub.
    #[cfg(feature = "actor")]
    pub fn deregister_actor(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{ActorDeregisterCommand, MutationCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::ActorDeregister(ActorDeregisterCommand::new(seq, actor_id, ts)),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.actor_registry.deregister(actor_id);
        self.heartbeat_monitor.remove(actor_id);
        self.department_registry.remove(actor_id);
        Ok(())
    }

    /// Records an actor heartbeat.
    #[cfg(feature = "actor")]
    pub fn actor_heartbeat(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{ActorHeartbeatCommand, MutationCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::ActorHeartbeat(ActorHeartbeatCommand::new(seq, actor_id, ts)),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.heartbeat_monitor.record_heartbeat(actor_id, ts);
        Ok(())
    }

    /// Detects and handles actors whose heartbeat has timed out.
    ///
    /// Called each tick (step 0f). For each timed-out actor, deregisters
    /// them from all registries and releases their leases.
    #[cfg(feature = "actor")]
    fn check_actor_heartbeat_timeouts(&mut self) -> Result<(), DispatchError> {
        let now = self.clock.now();
        let timed_out = self.heartbeat_monitor.check_timeouts(now);
        for actor_id in timed_out {
            tracing::warn!(%actor_id, "actor heartbeat timeout — deregistering");
            self.deregister_actor(actor_id)?;
        }
        Ok(())
    }

    // ── Platform feature methods ───────────────────────────────────────────

    /// Creates a new organizational tenant.
    #[cfg(feature = "platform")]
    pub fn create_tenant(
        &mut self,
        registration: actionqueue_core::platform::TenantRegistration,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{MutationCommand, TenantCreateCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::TenantCreate(TenantCreateCommand::new(
                    seq,
                    registration.clone(),
                    ts,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.tenant_registry.register(registration);
        Ok(())
    }

    /// Assigns a role to an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn assign_role(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        role: actionqueue_core::platform::Role,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{MutationCommand, RoleAssignCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::RoleAssign(RoleAssignCommand::new(
                    seq,
                    actor_id,
                    role.clone(),
                    tenant_id,
                    ts,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.rbac_enforcer.assign_role(actor_id, role, tenant_id);
        Ok(())
    }

    /// Grants a capability to an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn grant_capability(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        capability: actionqueue_core::platform::Capability,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{CapabilityGrantCommand, MutationCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
                    seq,
                    actor_id,
                    capability.clone(),
                    tenant_id,
                    ts,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.rbac_enforcer.grant_capability(actor_id, capability, tenant_id);
        Ok(())
    }

    /// Revokes a capability from an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn revoke_capability(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        capability: actionqueue_core::platform::Capability,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{CapabilityRevokeCommand, MutationCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
                    seq,
                    actor_id,
                    capability.clone(),
                    tenant_id,
                    ts,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.rbac_enforcer.revoke_capability(actor_id, &capability, tenant_id);
        Ok(())
    }

    /// Appends an entry to the organizational ledger.
    #[cfg(feature = "platform")]
    pub fn append_ledger_entry(
        &mut self,
        entry: actionqueue_core::platform::LedgerEntry,
    ) -> Result<(), DispatchError> {
        use actionqueue_core::mutation::{LedgerAppendCommand, MutationCommand};

        let seq = self.next_sequence()?;
        let ts = self.clock.now();
        let _ = self
            .authority
            .submit_command(
                MutationCommand::LedgerAppend(LedgerAppendCommand::new(seq, entry.clone(), ts)),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        self.ledger.append(entry);
        Ok(())
    }

    /// Returns the append ledger.
    #[cfg(feature = "platform")]
    pub fn ledger(&self) -> &actionqueue_platform::AppendLedger {
        &self.ledger
    }

    /// Returns the RBAC enforcer.
    #[cfg(feature = "platform")]
    pub fn rbac(&self) -> &actionqueue_platform::RbacEnforcer {
        &self.rbac_enforcer
    }

    /// Returns the actor registry.
    #[cfg(feature = "actor")]
    pub fn actor_registry(&self) -> &actionqueue_actor::ActorRegistry {
        &self.actor_registry
    }

    /// Returns the tenant registry.
    #[cfg(feature = "platform")]
    pub fn tenant_registry(&self) -> &actionqueue_platform::TenantRegistry {
        &self.tenant_registry
    }

    /// Consumes the dispatch loop and returns the mutation authority.
    pub fn into_authority(self) -> StorageMutationAuthority<W, ReplayReducer> {
        self.authority
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::TaskPayload;
    use actionqueue_engine::time::clock::MockClock;
    use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext};
    use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;

    use super::*;

    struct DependencyHandler;

    impl ExecutorHandler for DependencyHandler {
        fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
            match ctx.input.payload.as_slice() {
                b"suspend" => {
                    actionqueue_core::disposition::AttemptDisposition::suspended(None, None)
                }
                b"fail" => actionqueue_core::disposition::AttemptDisposition::terminal_failure(
                    actionqueue_core::bounded::BoundedError::new("prerequisite failed").unwrap(),
                ),
                _ => actionqueue_core::disposition::AttemptDisposition::complete(None),
            }
        }
    }

    fn task(payload: &[u8], key: Option<&str>) -> TaskSpec {
        let mut constraints = TaskConstraints::new(3, None, key.map(str::to_owned)).unwrap();
        constraints.set_concurrency_key_hold_policy(ConcurrencyKeyHoldPolicy::HoldDuringRetry);
        TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(payload.to_vec()),
            RunPolicy::Once,
            constraints,
            TaskMetadata::default(),
        )
        .unwrap()
    }

    async fn dependency_cancellation_releases_held_key(catch_up: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);

        let suspended = task(b"suspend", Some("shared"));
        let suspended_id = suspended.id();
        dispatch.submit_task(suspended).unwrap();
        let _ = dispatch.run_until_idle().await.unwrap();
        let suspended_run = dispatch.projection().run_ids_for_task(suspended_id)[0];
        assert_eq!(dispatch.projection().get_run_state(&suspended_run), Some(&RunState::Suspended));

        let competitor = task(b"success", Some("shared"));
        let competitor_id = competitor.id();
        dispatch.submit_task(competitor).unwrap();
        let _ = dispatch.run_until_idle().await.unwrap();
        let competitor_run = dispatch.projection().run_ids_for_task(competitor_id)[0];
        assert_eq!(dispatch.projection().get_run_state(&competitor_run), Some(&RunState::Ready));

        if catch_up {
            // Model the gap between a gate learning of dependency failure and
            // committing cancellation, keeping the existing key owner in memory.
            dispatch.dependency_gate.force_fail(suspended_id);
        } else {
            let prerequisite = task(b"fail", None);
            let prerequisite_id = prerequisite.id();
            dispatch.submit_task(prerequisite).unwrap();
            dispatch.declare_dependency(suspended_id, vec![prerequisite_id]).unwrap();
        }
        let _ = dispatch.run_until_idle().await.unwrap();
        assert_eq!(dispatch.projection().get_run_state(&suspended_run), Some(&RunState::Canceled));
        assert_eq!(
            dispatch.projection().get_run_state(&competitor_run),
            Some(&RunState::Completed),
            "dependency cancellation must free the held key without a restart"
        );
    }

    #[tokio::test]
    async fn dependency_failure_cascade_releases_suspended_key() {
        dependency_cancellation_releases_held_key(false).await;
    }

    #[tokio::test]
    async fn dependency_failure_catch_up_releases_suspended_key() {
        dependency_cancellation_releases_held_key(true).await;
    }

    /// Collects log lines written by a thread-local tracing subscriber.
    #[derive(Clone, Default)]
    struct LogBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl LogBuffer {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    fn capture_warnings() -> (LogBuffer, tracing::subscriber::DefaultGuard) {
        let buffer = LogBuffer::default();
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (buffer, guard)
    }

    #[test]
    fn unsafe_disposition_limit_does_not_create_a_store() {
        use crate::{
            config::{ConfigError, RuntimeConfig},
            engine::{ActionQueueEngine, BootstrapError},
        };
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("unopened");
        let minimum = RuntimeConfig::minimum_disposition_bytes();
        for limit in [0, 91, 99, minimum - 1, minimum, minimum + 1, usize::MAX] {
            let config = RuntimeConfig {
                data_dir: data_dir.clone(),
                continuation_limits: actionqueue_core::limits::ContinuationLimits {
                    output_bytes: 0,
                    checkpoint_bytes: 0,
                    disposition_bytes: limit,
                },
                ..Default::default()
            };
            if limit >= minimum {
                assert!(config.validate().is_ok());
                continue;
            }
            let error = ActionQueueEngine::new(config, DependencyHandler)
                .bootstrap()
                .err()
                .expect("unsafe quota must reject");
            assert!(
                matches!(error, BootstrapError::Config(ConfigError::DispositionLimitTooLow { minimum: m }) if m == minimum)
            );
            assert!(!data_dir.exists());
        }
    }

    #[test]
    fn direct_dispatch_rejects_unsafe_continuation_limits() {
        let dir = tempfile::tempdir().unwrap();
        let recovery = load_projection_from_storage(dir.path()).unwrap();
        let mut authority = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);
        let digest = authority.projection().projection_digest().unwrap();
        authority.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
            disposition_bytes: 99,
            ..Default::default()
        });
        let error = DispatchLoop::new(
            authority,
            DependencyHandler,
            MockClock::new(1000),
            DispatchConfig::new(
                BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
                1,
                30,
                None,
                None,
            ),
        )
        .err()
        .expect("unsafe quota must reject");
        assert!(matches!(
            error,
            DispatchError::InvalidContinuationLimits(
                crate::config::ConfigError::DispositionLimitTooLow { .. }
            )
        ));
        let recovery = load_projection_from_storage(dir.path()).unwrap();
        assert_eq!(recovery.projection.projection_digest().unwrap(), digest);
    }

    fn new_dispatch<H: ExecutorHandler + 'static>(
        dir: &std::path::Path,
        handler: H,
    ) -> DispatchLoop<
        actionqueue_storage::wal::writer::InstrumentedWalWriter<
            actionqueue_storage::wal::fs_writer::WalFsWriter,
        >,
        H,
        MockClock,
    > {
        let recovery = load_projection_from_storage(dir).unwrap();
        let authority = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);
        DispatchLoop::new(
            authority,
            handler,
            MockClock::new(1000),
            DispatchConfig::new(
                BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
                1,
                30,
                None,
                None,
            ),
        )
        .unwrap()
    }

    type TestDispatch = DispatchLoop<
        actionqueue_storage::wal::writer::InstrumentedWalWriter<
            actionqueue_storage::wal::fs_writer::WalFsWriter,
        >,
        DependencyHandler,
        MockClock,
    >;

    fn accepted_worker(dispatch: &mut TestDispatch) -> WorkerResult {
        let spec = task(b"success", None);
        let task_id = spec.id();
        let constraints = spec.constraints().clone();
        dispatch.submit_task(spec).unwrap();
        let run_id = dispatch.projection().run_ids_for_task(task_id)[0];
        let _ = dispatch
            .authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    dispatch.next_sequence().unwrap(),
                    run_id,
                    RunState::Scheduled,
                    RunState::Ready,
                    1000,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
        let run = dispatch.projection().get_run_instance(&run_id).unwrap().clone();
        let (attempt_id, lease_expiry, attempt_number, max_attempts) =
            dispatch.dispatch_single_run(&run, &constraints, 1000).unwrap();
        dispatch.in_flight.insert(
            run_id,
            InFlightRun {
                run_id,
                attempt_id,
                task_id,
                lease_expiry,
                max_attempts,
                attempt_number,
                #[cfg(feature = "budget")]
                cancellation_context: None,
            },
        );
        let lease = dispatch.projection().get_lease_metadata(&run_id).unwrap();
        WorkerResult {
            lease_fence: actionqueue_core::mutation::LeaseFence::new(
                lease.owner().into(),
                lease.granted_at_sequence(),
            ),
            run_id,
            attempt_id,
            disposition: actionqueue_core::disposition::AttemptDisposition::complete(
                (Some(vec![42]))
                    .map(|v| actionqueue_core::data_ref::DataRef::from_bytes(v).unwrap()),
            ),
        }
    }

    #[test]
    fn stale_worker_results_preserve_projection_and_in_flight_owner() {
        use actionqueue_core::mutation::{LeaseFence, LeaseOwner};
        for case in ["wrong-owner", "old-grant", "expired", "old-attempt", "newer-owner"] {
            let dir = tempfile::tempdir().unwrap();
            let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
            let mut worker = accepted_worker(&mut dispatch);
            let run_id = worker.run_id;
            let mut now = 1001;
            match case {
                "wrong-owner" => {
                    worker.lease_fence = LeaseFence::new(
                        LeaseOwner::new("other-worker"),
                        worker.lease_fence.granted_at_sequence(),
                    )
                }
                "old-grant" => {
                    worker.lease_fence = LeaseFence::new(
                        worker.lease_fence.owner().clone(),
                        worker.lease_fence.granted_at_sequence() - 1,
                    )
                }
                "expired" => now = dispatch.in_flight[&run_id].lease_expiry,
                "old-attempt" => worker.attempt_id = AttemptId::new(),
                "newer-owner" => {
                    dispatch.in_flight.get_mut(&run_id).unwrap().attempt_id = AttemptId::new()
                }
                _ => unreachable!(),
            }
            let owner = dispatch.in_flight[&run_id].attempt_id;
            let before = dispatch.projection().projection_digest().unwrap();
            let sequence = dispatch.projection().latest_sequence();
            let mut tick = TickResult::default();
            dispatch.process_worker_result(worker, &mut tick, now).unwrap();
            assert_eq!(dispatch.projection().projection_digest().unwrap(), before, "{case}");
            assert_eq!(dispatch.projection().latest_sequence(), sequence, "{case}");
            assert_eq!(dispatch.in_flight[&run_id].attempt_id, owner, "{case}");
            assert_eq!(tick.completed, 0, "{case}");
        }
    }

    #[tokio::test]
    async fn completed_result_at_expiry_releases_capacity_before_heartbeat() {
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
        dispatch.lease_timeout_secs = 3;
        let mut first = task(b"success", Some("expiry-key"));
        first
            .set_constraints(TaskConstraints::new(1, None, Some("expiry-key".into())).unwrap())
            .unwrap();
        let first_id = first.id();
        dispatch.submit_task(first).unwrap();
        assert_eq!(dispatch.tick().await.unwrap().dispatched, 1);
        // Receive the actual worker envelope, proving execution and result delivery
        // finished before the delayed tick. Tick drains this pending envelope.
        dispatch.pending_result = Some(
            tokio::time::timeout(Duration::from_secs(10), dispatch.result_rx.recv())
                .await
                .unwrap()
                .unwrap(),
        );
        let run_id = dispatch.pending_result.as_ref().unwrap().run_id;
        let second = task(b"success", Some("expiry-key"));
        let second_id = second.id();
        dispatch.submit_task(second).unwrap();
        dispatch.clock.set(1003);
        assert_eq!(dispatch.tick().await.unwrap().dispatched, 1);
        assert!(!dispatch.in_flight.contains_key(&run_id));
        assert!(dispatch.projection().get_lease(&run_id).is_none());
        let _ = tokio::time::timeout(Duration::from_secs(10), dispatch.run_until_idle())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            dispatch.projection().runs_for_task(first_id).next().unwrap().state(),
            RunState::Failed
        );
        assert_eq!(
            dispatch.projection().runs_for_task(second_id).next().unwrap().state(),
            RunState::Completed
        );
    }

    #[test]
    fn expiry_recovery_and_late_result_preserve_newer_execution() {
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
        let worker = accepted_worker(&mut dispatch);
        let run_id = worker.run_id;
        let mut tick = TickResult::default();
        let before = dispatch.projection().projection_digest().unwrap();
        dispatch.heartbeat_in_flight_leases(1030).unwrap();
        assert_eq!(dispatch.projection().projection_digest().unwrap(), before);
        dispatch.recover_expired_executions(&mut tick, 1030).unwrap();
        dispatch.recover_expired_executions(&mut tick, 1030).unwrap();
        assert_eq!(dispatch.projection().get_run_state(&run_id), Some(&RunState::RetryWait));
        assert_eq!(
            dispatch.projection().get_run_instance(&run_id).unwrap().failure_attempt_count(),
            1
        );
        assert!(!dispatch.in_flight.contains_key(&run_id));
        let _ = dispatch
            .authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    dispatch.next_sequence().unwrap(),
                    run_id,
                    RunState::RetryWait,
                    RunState::Ready,
                    1030,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
        let run = dispatch.projection().get_run_instance(&run_id).unwrap().clone();
        let constraints =
            dispatch.projection().get_task(&run.task_id()).unwrap().constraints().clone();
        let (attempt_id, lease_expiry, attempt_number, max_attempts) =
            dispatch.dispatch_single_run(&run, &constraints, 1030).unwrap();
        dispatch.in_flight.insert(
            run_id,
            InFlightRun {
                run_id,
                attempt_id,
                task_id: run.task_id(),
                lease_expiry,
                attempt_number,
                max_attempts,
                #[cfg(feature = "budget")]
                cancellation_context: None,
            },
        );
        let before = dispatch.projection().projection_digest().unwrap();
        dispatch.recover_expired_executions(&mut tick, 1031).unwrap();
        dispatch.process_worker_result(worker, &mut tick, 1031).unwrap();
        assert_eq!(dispatch.projection().projection_digest().unwrap(), before);
        assert_eq!(dispatch.in_flight[&run_id].attempt_id, attempt_id);
        assert_eq!(
            dispatch.projection().get_lease_metadata(&run_id).unwrap().expiry(),
            lease_expiry
        );
    }

    #[test]
    fn heartbeats_and_unrelated_commits_preserve_worker_eligibility() {
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
        let worker = accepted_worker(&mut dispatch);
        let run_id = worker.run_id;
        let _ = dispatch
            .authority
            .submit_command(
                MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(
                    dispatch.next_sequence().unwrap(),
                    run_id,
                    worker.lease_fence.owner().as_str(),
                    1100,
                    1001,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();
        dispatch.in_flight.get_mut(&run_id).unwrap().lease_expiry = 1100;
        dispatch.submit_task(task(b"unrelated", None)).unwrap();
        let mut tick = TickResult::default();
        dispatch.process_worker_result(worker.clone(), &mut tick, 1040).unwrap();
        assert_eq!(dispatch.projection().get_run_state(&run_id), Some(&RunState::Completed));
        assert_eq!(tick.completed, 1);
        assert!(!dispatch.in_flight.contains_key(&run_id));
        let before = dispatch.projection().projection_digest().unwrap();
        dispatch.process_worker_result(worker, &mut tick, 1041).unwrap();
        assert_eq!(dispatch.projection().projection_digest().unwrap(), before);
        assert_eq!(tick.completed, 1, "duplicate result cannot complete twice");
    }

    #[test]
    fn canceled_worker_result_cannot_change_durable_state() {
        use actionqueue_core::mutation::{CancelCommand, CancelTarget};
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
        let worker = accepted_worker(&mut dispatch);
        let run_id = worker.run_id;
        dispatch
            .cancel(CancelCommand {
                expected_sequence: dispatch.next_sequence().unwrap(),
                target: CancelTarget::Run(run_id),
                tenant_id: None,
                control_context: None,
                timestamp: 1000,
            })
            .unwrap();
        let before = dispatch.projection().projection_digest().unwrap();
        let mut tick = TickResult::default();
        dispatch.process_worker_result(worker, &mut tick, 1001).unwrap();
        assert_eq!(dispatch.projection().projection_digest().unwrap(), before);
        assert!(!dispatch.in_flight.contains_key(&run_id));
        assert_eq!(tick.completed, 0);
    }

    #[cfg(feature = "actor")]
    #[test]
    fn recovery_rejects_invalid_persisted_actor_traits_without_modifying_store() {
        use actionqueue_core::ids::ActorId;
        use actionqueue_storage::wal::{
            codec,
            event::{WalEvent, WalEventType},
        };
        let dir = tempfile::tempdir().unwrap();
        let recovery = load_projection_from_storage(dir.path()).unwrap();
        let manifest = recovery.wal_writer.inner().session().unwrap().manifest().clone();
        let path = recovery.wal_path.clone();
        drop(recovery);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(
            &codec::encode_for_store(
                &WalEvent::new(
                    2,
                    WalEventType::ActorRegistered {
                        actor_id: ActorId::new(),
                        identity: "persisted-actor".into(),
                        executor_traits: vec!["bad trait".into()],
                        department: None,
                        heartbeat_interval_secs: 30,
                        tenant_id: None,
                        timestamp: 1000,
                    },
                ),
                manifest.store_id,
            )
            .unwrap(),
        );
        std::fs::write(&path, &bytes).unwrap();
        assert!(load_projection_from_storage(dir.path()).is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }

    #[tokio::test]
    async fn dependency_cascade_skips_release_for_runs_that_never_held_the_key() {
        let (log, _guard) = capture_warnings();
        tracing::warn!("capture-probe");
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);

        let prerequisite = task(b"fail", None);
        let prerequisite_id = prerequisite.id();
        let dependent = task(b"success", Some("shared"));
        let dependent_id = dependent.id();
        dispatch.submit_task(prerequisite).unwrap();
        dispatch.submit_task(dependent).unwrap();
        dispatch.declare_dependency(dependent_id, vec![prerequisite_id]).unwrap();
        let _ = dispatch.run_until_idle().await.unwrap();

        let dependent_run = dispatch.projection().run_ids_for_task(dependent_id)[0];
        assert_eq!(dispatch.projection().get_run_state(&dependent_run), Some(&RunState::Canceled));
        assert_eq!(dispatch.key_gate.key_holder(&ConcurrencyKey::new("shared")), None);
        let contents = log.contents();
        assert!(contents.contains("capture-probe"), "log capture is not wired: {contents}");
        assert!(
            !contents.contains("key not held by this run"),
            "cancelling a run that never acquired the key must not warn: {contents}"
        );
    }

    /// Blocks every execution until the test releases it.
    struct BlockingHandler {
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl ExecutorHandler for BlockingHandler {
        fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
            let _ = self.release.lock().unwrap().recv();
            actionqueue_core::disposition::AttemptDisposition::complete(None)
        }
    }

    #[tokio::test]
    async fn dependency_cascade_keeps_key_while_worker_is_still_executing() {
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(
            dir.path(),
            BlockingHandler { release: std::sync::Mutex::new(release_rx) },
        );

        let blocked = task(b"block", Some("shared"));
        let blocked_id = blocked.id();
        dispatch.submit_task(blocked).unwrap();
        for _ in 0..3 {
            if !dispatch.in_flight.is_empty() {
                break;
            }
            let _ = dispatch.tick().await.unwrap();
        }
        let blocked_run = dispatch.projection().run_ids_for_task(blocked_id)[0];
        assert!(dispatch.in_flight.contains_key(&blocked_run), "worker must be in flight");
        assert_eq!(dispatch.projection().get_run_state(&blocked_run), Some(&RunState::Running));
        let key = ConcurrencyKey::new("shared");
        assert_eq!(dispatch.key_gate.key_holder(&key), Some(blocked_run));

        dispatch.dependency_gate.force_fail(blocked_id);
        dispatch.cancel_dependency_failed_runs(1000).unwrap();

        assert_eq!(dispatch.projection().get_run_state(&blocked_run), Some(&RunState::Canceled));
        assert_eq!(
            dispatch.key_gate.key_holder(&key),
            Some(blocked_run),
            "an in-flight run keeps its key so no competitor starts under it"
        );
        release_tx.send(()).unwrap();
    }

    #[test]
    fn coordination_refresh_preserves_uncanceled_tasks_without_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut dispatch = new_dispatch(dir.path(), DependencyHandler);
        let parent = task(b"parent", None);
        let parent_id = parent.id();
        dispatch.submit_task(parent).unwrap();
        let child = task(b"child", None).with_parent(parent_id);
        let child_id = child.id();
        let _ = dispatch
            .authority
            .submit_command(
                MutationCommand::TaskCreate(actionqueue_core::mutation::TaskCreateCommand::new(
                    dispatch.next_sequence().unwrap(),
                    child,
                    1000,
                )),
                DurabilityPolicy::Immediate,
            )
            .unwrap();

        dispatch.refresh_coordination();
        dispatch.gc_terminal_tasks();
        assert_eq!(dispatch.hierarchy_tracker.depth(child_id), 1);
        assert!(!dispatch.hierarchy_tracker.is_terminal(child_id));

        dispatch
            .cancel(actionqueue_core::mutation::CancelCommand {
                expected_sequence: dispatch.next_sequence().unwrap(),
                target: actionqueue_core::mutation::CancelTarget::Task(parent_id),
                tenant_id: None,
                control_context: None,
                timestamp: 1000,
            })
            .unwrap();
        assert!(dispatch.projection().is_task_canceled(child_id));
        dispatch.gc_terminal_tasks();
        assert_eq!(dispatch.hierarchy_tracker.depth(child_id), 0);
    }

    #[tokio::test]
    async fn live_cancellation_holds_key_until_worker_returns() {
        use actionqueue_core::mutation::{CancelCommand, CancelTarget};

        for cancel_task in [false, true] {
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let dir = tempfile::tempdir().unwrap();
            let mut dispatch = new_dispatch(
                dir.path(),
                BlockingHandler { release: std::sync::Mutex::new(release_rx) },
            );
            dispatch.max_concurrent = 2;
            let blocked = task(b"block", Some("shared"));
            let blocked_id = blocked.id();
            dispatch.submit_task(blocked).unwrap();
            assert_eq!(dispatch.tick().await.unwrap().dispatched, 1);
            let blocked_run = dispatch.projection().run_ids_for_task(blocked_id)[0];
            let competitor = task(b"compete", Some("shared"));
            let competitor_id = competitor.id();
            dispatch.submit_task(competitor).unwrap();
            let competitor_run = dispatch.projection().run_ids_for_task(competitor_id)[0];
            dispatch
                .cancel(CancelCommand {
                    expected_sequence: dispatch.next_sequence().unwrap(),
                    target: if cancel_task {
                        CancelTarget::Task(blocked_id)
                    } else {
                        CancelTarget::Run(blocked_run)
                    },
                    tenant_id: None,
                    control_context: None,
                    timestamp: 1000,
                })
                .unwrap();
            let canceled_history =
                dispatch.projection().get_attempt_history(&blocked_run).map(<[_]>::to_vec);
            assert!(!dispatch.projection().key_reservations().any(|(id, _)| id == blocked_run));
            for _ in 0..3 {
                assert_eq!(dispatch.tick().await.unwrap().dispatched, 0);
                assert!(dispatch.in_flight.contains_key(&blocked_run));
                assert_eq!(
                    dispatch.projection().get_run_state(&competitor_run),
                    Some(&RunState::Ready)
                );
                assert_eq!(
                    dispatch.key_gate.key_holder(&ConcurrencyKey::new("shared")),
                    Some(blocked_run)
                );
            }
            // A canceled worker must not block progress when its former lease expires.
            dispatch.heartbeat_in_flight_leases(2000).unwrap();
            release_tx.send(()).unwrap();
            release_tx.send(()).unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(5), dispatch.run_until_idle())
                .await
                .unwrap()
                .unwrap();
            assert!(dispatch.in_flight.is_empty());
            assert_eq!(
                dispatch.projection().get_run_state(&competitor_run),
                Some(&RunState::Completed)
            );
            assert_eq!(
                dispatch.projection().get_attempt_history(&blocked_run),
                canceled_history.as_deref()
            );
        }
    }
}
