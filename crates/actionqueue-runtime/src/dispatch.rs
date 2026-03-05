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

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AttemptStartCommand, DependencyDeclareCommand, DurabilityPolicy, LeaseAcquireCommand,
    LeaseHeartbeatCommand, LeaseReleaseCommand, MutationAuthority, MutationCommand,
    RunCreateCommand, RunStateTransitionCommand, TaskCancelCommand, TaskCreateCommand,
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
use actionqueue_workflow::submission::{submission_channel, SubmissionChannel, SubmissionReceiver};
use tokio::sync::mpsc;

use crate::config::BackoffStrategyConfig;
use crate::worker::{InFlightRun, WorkerResult};

/// Builds a [`DependencyGate`] from the recovery reducer's dependency state.
///
/// Called once at `DispatchLoop::new()` to reconstruct the gate from WAL events
/// that were already replayed during bootstrap. Satisfaction is derived from the
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
            tracing::warn!(%task_id, error = %err, "dependency gate declare failed at bootstrap");
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
    /// WAL sequence counter overflow.
    SequenceOverflow,
    /// A mutation command submitted to the storage authority failed.
    Authority(AuthorityError),
    /// Scheduled-to-ready promotion via authority failed.
    ScheduledPromotion(AuthorityPromotionError<AuthorityError>),
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
    /// Sender side of the workflow submission channel (Arc'd, given to handlers).
    submission_tx: std::sync::Arc<SubmissionChannel>,
    /// Receiver side of the workflow submission channel (drained each tick).
    submission_rx: SubmissionReceiver,
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
    /// configuration is invalid (e.g., exponential base exceeds max).
    pub fn new(
        authority: StorageMutationAuthority<W, ReplayReducer>,
        handler: H,
        clock: C,
        config: DispatchConfig,
    ) -> Result<Self, DispatchError> {
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
        let (submission_tx, submission_rx) = submission_channel();

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
                    let caps = actionqueue_core::actor::ActorCapabilities::new(
                        record.capabilities.clone(),
                    )
                    .unwrap_or_else(|_| {
                        actionqueue_core::actor::ActorCapabilities::new(vec!["_".to_string()])
                            .expect("fallback capability")
                    });
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
                        if let Ok(dept) = actionqueue_core::ids::DepartmentId::new(dept_str.clone())
                        {
                            reg = reg.with_department(dept);
                        }
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

        Ok(Self {
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
            submission_tx,
            submission_rx,
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
        })
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

    /// Drains all pending task submissions proposed by handlers via
    /// `ExecutorContext.submission` and processes them through the mutation authority.
    ///
    /// Each submission is validated and WAL-appended as a standard TaskCreate +
    /// RunCreate sequence. Invalid submissions (e.g., nil task ID, terminal parent)
    /// are logged and dropped — handlers have no error path for submission failures.
    fn drain_submissions(&mut self, current_time: u64) -> Result<(), DispatchError> {
        while let Some(submission) = self.submission_rx.try_recv() {
            match self.process_submission(submission, current_time) {
                Ok(()) => {}
                Err(DispatchError::SubmissionRejected { ref task_id, ref context }) => {
                    tracing::error!(
                        %task_id,
                        %context,
                        "workflow submission rejected; submission dropped"
                    );
                }
                Err(DispatchError::DependencyCycle(ref err)) => {
                    tracing::error!(
                        error = %err,
                        "workflow submission rejected (dependency cycle); submission dropped"
                    );
                }
                Err(fatal) => return Err(fatal),
            }
        }
        Ok(())
    }

    /// Validates and commits a single handler-proposed task submission.
    fn process_submission(
        &mut self,
        submission: actionqueue_workflow::submission::TaskSubmission,
        current_time: u64,
    ) -> Result<(), DispatchError> {
        let (task_spec, mut dependencies) = submission.into_parts();
        let task_id = task_spec.id();
        let parent_task_id = task_spec.parent_task_id();

        // Deduplicate dependencies to avoid redundant WAL entries and graph edges.
        {
            let mut seen = std::collections::HashSet::new();
            dependencies.retain(|id| seen.insert(*id));
        }

        // Validate parent: must exist and not be terminal (orphan prevention).
        if let Some(parent_id) = parent_task_id {
            if self.authority.projection().get_task(&parent_id).is_none() {
                return Err(DispatchError::SubmissionRejected {
                    task_id: task_spec.id(),
                    context: format!("parent {parent_id} not found"),
                });
            }
            if self.hierarchy_tracker.is_terminal(parent_id) {
                return Err(DispatchError::SubmissionRejected {
                    task_id: task_spec.id(),
                    context: format!("parent {parent_id} is terminal (orphan prevention)"),
                });
            }
        }

        // WAL-append the task.
        let task_seq = self.next_sequence()?;
        let _ = self
            .authority
            .submit_command(
                actionqueue_core::mutation::MutationCommand::TaskCreate(
                    actionqueue_core::mutation::TaskCreateCommand::new(
                        task_seq,
                        task_spec.clone(),
                        current_time,
                    ),
                ),
                actionqueue_core::mutation::DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Derive and WAL-append runs.
        let already_derived = self.authority.projection().run_ids_for_task(task_id).len() as u32;
        let derivation = actionqueue_engine::derive::derive_runs(
            &self.clock,
            task_id,
            task_spec.run_policy(),
            already_derived,
            current_time,
        )
        .map_err(DispatchError::Derivation)?;

        for run in derivation.into_derived() {
            let run_seq = self.next_sequence()?;
            let _ = self
                .authority
                .submit_command(
                    actionqueue_core::mutation::MutationCommand::RunCreate(
                        actionqueue_core::mutation::RunCreateCommand::new(run_seq, run),
                    ),
                    actionqueue_core::mutation::DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;
        }

        // If the submission includes dependency declarations, WAL-append and register.
        if !dependencies.is_empty() {
            // Check for cycles BEFORE WAL append so invalid declarations are
            // never persisted. The read-only check_cycle leaves the gate
            // unmodified; declare() below is guaranteed to succeed after this.
            self.dependency_gate
                .check_cycle(task_id, &dependencies)
                .map_err(DispatchError::DependencyCycle)?;

            // Reject if any prerequisite is not yet in the projection.
            // This prevents the mutation authority from fatally rejecting
            // the DependencyDeclareCommand (UnknownTask), which would crash
            // the dispatch loop instead of gracefully dropping the submission.
            for prereq_id in &dependencies {
                if self.authority.projection().get_task(prereq_id).is_none() {
                    return Err(DispatchError::SubmissionRejected {
                        task_id,
                        context: format!("prerequisite {prereq_id} not yet in projection"),
                    });
                }
            }
            let dep_seq = self.next_sequence()?;
            let _ = self
                .authority
                .submit_command(
                    MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(
                        dep_seq,
                        task_id,
                        dependencies.clone(),
                        current_time,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;
            // Gate declare is guaranteed to succeed after check_cycle above.
            let _ = self.dependency_gate.declare(task_id, dependencies);
        }

        // Register parent-child in the hierarchy tracker (after WAL commit).
        if let Some(parent_id) = parent_task_id {
            // Validation already passed above; ignore errors here (should not occur).
            let _ = self.hierarchy_tracker.register_child(parent_id, task_id);
        }

        tracing::debug!(task_id = %task_id, "workflow submission committed");
        Ok(())
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

        // Record attempt finish via authority
        let seq = self.next_sequence()?;
        let finish_cmd =
            actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                seq,
                run_id,
                attempt_id,
                &worker_result.response,
                current_time,
            );
        let _ = actionqueue_engine::scheduler::attempt_finish::submit_attempt_finish_via_authority(
            finish_cmd,
            DurabilityPolicy::Immediate,
            &mut self.authority,
        )
        .map_err(|e| DispatchError::Authority(e.into_source()))?;

        // Compute the effective attempt number for retry cap purposes.
        // Suspended attempts do not count against max_attempts: they are
        // budget-driven pauses, not failure-driven retries.
        let suspended_count = self
            .authority
            .projection()
            .get_attempt_history(&run_id)
            .map(|history| {
                history
                    .iter()
                    .filter(|a| {
                        a.result() == Some(actionqueue_core::mutation::AttemptResultKind::Suspended)
                    })
                    .count() as u32
            })
            .unwrap_or(0);
        let effective_attempt = worker_result.attempt_number.saturating_sub(suspended_count);

        // Determine target state by delegating to the canonical retry decision
        // function. This gets us defensive validation (rejects N+1 paths, validates
        // max_attempts >= 1, validates attempt_number >= 1) for free.
        let outcome_kind =
            actionqueue_executor_local::AttemptOutcomeKind::from_response(&worker_result.response);
        let retry_input = actionqueue_executor_local::RetryDecisionInput {
            run_id,
            attempt_id,
            attempt_number: effective_attempt,
            max_attempts: worker_result.max_attempts,
            outcome_kind,
        };
        let decision = actionqueue_executor_local::retry::decide_retry_transition(&retry_input)
            .map_err(DispatchError::RetryDecision)?;
        let new_state = Some(decision.target_state());

        if let Some(target_state) = new_state {
            tracing::info!(%run_id, ?target_state, "run state transition applied");

            // Release the lease while the run is still in Running state.
            if let Some(inf) = self.in_flight.get(&run_id) {
                let seq = self.next_sequence()?;
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
                            seq,
                            run_id,
                            self.identity.identity(),
                            inf.lease_expiry,
                            current_time,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
            }

            // Apply the state transition. Suspended uses the dedicated RunSuspend
            // command (which emits a RunSuspended WAL event); all other transitions
            // use the generic RunStateTransition command.
            let seq = self.next_sequence()?;
            if target_state == RunState::Suspended {
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::RunSuspend(
                            actionqueue_core::mutation::RunSuspendCommand::new(
                                seq,
                                run_id,
                                None,
                                current_time,
                            ),
                        ),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
            } else {
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            seq,
                            run_id,
                            RunState::Running,
                            target_state,
                            current_time,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
            }

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

        // Record budget consumption reported by the handler, if any.
        // Budget consumption uses Deferred durability for performance.
        // If a crash occurs after the run's state transition (Immediate)
        // but before this write flushes, at most one extra dispatch beyond
        // the budget cap may occur on recovery. Accepted trade-off.
        #[cfg(feature = "budget")]
        {
            use actionqueue_core::mutation::{BudgetConsumeCommand, MutationCommand as MC};
            if let Some(inf) = self.in_flight.get(&run_id) {
                let task_id = inf.task_id;
                for c in &worker_result.consumption {
                    let seq = self.next_sequence()?;
                    let _ = self
                        .authority
                        .submit_command(
                            MC::BudgetConsume(BudgetConsumeCommand::new(
                                seq,
                                task_id,
                                c.dimension,
                                c.amount,
                                current_time,
                            )),
                            DurabilityPolicy::Deferred,
                        )
                        .map_err(DispatchError::Authority)?;
                    self.budget_tracker.consume(task_id, c.dimension, c.amount);
                }
                // Fire budget threshold events after consumption.
                if !worker_result.consumption.is_empty() {
                    self.fire_budget_threshold_events(task_id)?;
                }
            }
        }

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
                for (run_id, current_state) in runs_to_cancel {
                    let seq = self.next_sequence()?;
                    let _ = self
                        .authority
                        .submit_command(
                            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                                seq,
                                run_id,
                                current_state,
                                RunState::Canceled,
                                current_time,
                            )),
                            DurabilityPolicy::Immediate,
                        )
                        .map_err(DispatchError::Authority)?;
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

                    // Release concurrency key for cascade-canceled runs.
                    // Without this, a Suspended run with HoldDuringRetry policy
                    // would permanently leak its concurrency key slot.
                    Self::try_release_concurrency_key(
                        &self.authority,
                        &mut self.key_gate,
                        run_id,
                        descendant_id,
                        RunState::Canceled,
                    );
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

    /// Heartbeats in-flight leases approaching expiry.
    fn heartbeat_in_flight_leases(&mut self, current_time: u64) -> Result<(), DispatchError> {
        // Heartbeat when 1/3 of lease time remains. Integer division truncates;
        // the minimum threshold of 1 second prevents a zero-second threshold for
        // very short leases (lease_timeout_secs validated >= 3 in RuntimeConfig).
        let heartbeat_threshold = (self.lease_timeout_secs / 3).max(1);
        let run_ids_needing_heartbeat: Vec<actionqueue_core::ids::RunId> = self
            .in_flight
            .values()
            .filter(|inf| current_time.saturating_add(heartbeat_threshold) >= inf.lease_expiry)
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

    /// Attempts to release the concurrency key for a run entering a terminal or
    /// RetryWait state, depending on the task's hold policy.
    fn try_release_concurrency_key(
        authority: &StorageMutationAuthority<W, ReplayReducer>,
        key_gate: &mut KeyGate,
        run_id: RunId,
        task_id: TaskId,
        target_state: RunState,
    ) {
        let should_release = if target_state.is_terminal() {
            true
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

        // Step 0a: Drain workflow submissions proposed by handlers
        self.drain_submissions(current_time)?;

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
            .map_err(DispatchError::ScheduledPromotion)?;
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

                    // Build workflow context: submission channel and children snapshot.
                    // The submission channel allows handlers to propose child tasks;
                    // the children snapshot gives Coordinator handlers a point-in-time
                    // view of their children's states (both are None for non-workflow tasks).
                    let submission = Some(std::sync::Arc::clone(&self.submission_tx)
                        as std::sync::Arc<dyn actionqueue_executor_local::TaskSubmissionPort>);
                    let children = build_children_snapshot(self.authority.projection(), task_id);

                    // Spawn worker via spawn_blocking with timeout enforcement.
                    tracing::info!(
                        %run_id, %attempt_id, attempt_number,
                        "spawning handler for attempt"
                    );
                    let runner = Arc::clone(&self.runner);
                    let result_tx = self.result_tx.clone();

                    tokio::task::spawn_blocking(move || {
                        let request = ExecutorRequest {
                            run_id,
                            attempt_id,
                            payload,
                            constraints,
                            attempt_number,
                            submission,
                            children,
                            cancellation_context: cancellation_ctx,
                        };
                        let outcome = runner.run_attempt(request);

                        let worker_result = WorkerResult {
                            run_id,
                            attempt_id,
                            response: outcome.response,
                            max_attempts,
                            attempt_number,
                            consumption: outcome.consumption,
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
        let _ = self
            .authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run.id(),
                    attempt_id,
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

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
        let path = match &self.snapshot_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        if self.events_since_last_snapshot < threshold {
            return Ok(());
        }

        let snapshot = build_snapshot_from_projection(self.authority.projection(), current_time)
            .map_err(DispatchError::SnapshotBuild)?;

        let mut writer =
            SnapshotFsWriter::new(path).map_err(|e| DispatchError::SnapshotInit(format!("{e}")))?;
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
            for (run_id, current_state) in runs_to_cancel {
                let seq = self.next_sequence()?;
                let _ = self
                    .authority
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            seq,
                            run_id,
                            current_state,
                            RunState::Canceled,
                            current_time,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .map_err(DispatchError::Authority)?;
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
                if let Some(worker_result) = self.result_rx.recv().await {
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
                match tokio::time::timeout(remaining, self.result_rx.recv()).await {
                    Ok(Some(result)) => {
                        self.pending_result = Some(result);
                    }
                    Ok(None) => {
                        self.in_flight.clear();
                        break;
                    }
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

    /// Submits a new task and derives initial runs.
    pub fn submit_task(&mut self, spec: TaskSpec) -> Result<(), DispatchError> {
        let current_time = self.clock.now();
        let seq = self.next_sequence()?;

        // Create task
        let _ = self
            .authority
            .submit_command(
                MutationCommand::TaskCreate(TaskCreateCommand::new(
                    seq,
                    spec.clone(),
                    current_time,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(DispatchError::Authority)?;

        // Derive runs
        let already_derived = self.authority.projection().run_ids_for_task(spec.id()).len() as u32;
        let derivation = actionqueue_engine::derive::derive_runs(
            &self.clock,
            spec.id(),
            spec.run_policy(),
            already_derived,
            current_time,
        )
        .map_err(DispatchError::Derivation)?;

        for run in derivation.into_derived() {
            let seq = self.next_sequence()?;
            let _ = self
                .authority
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, run)),
                    DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;
        }

        Ok(())
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
