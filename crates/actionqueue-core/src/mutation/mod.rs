//! Engine-facing mutation boundary contracts.
//!
//! This module defines the command-semantic boundary between engine intent and
//! storage durability authority.
//!
//! Policy: callers propose typed mutation commands through
//! [`MutationAuthority::submit_command`], while storage-owned implementations
//! own WAL mapping, append discipline, durability sync, and projection apply.

use crate::actor::ActorRegistration;
use crate::budget::BudgetDimension;
use crate::ids::{ActorId, AttemptId, RunId, TaskId, TenantId};
use crate::platform::{Capability, LedgerEntry, Role, TenantRegistration};
use crate::run::state::RunState;
use crate::run::RunInstance;
use crate::subscription::{EventFilter, SubscriptionId};
use crate::task::task_spec::TaskSpec;

/// Durability behavior requested for a submitted mutation command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// Require an immediate durability sync after WAL append.
    Immediate,
    /// Allow deferred durability sync according to implementation policy.
    Deferred,
}

/// Semantic mutation command proposed by an engine-facing caller.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "mutation commands should be submitted to a MutationAuthority"]
pub enum MutationCommand {
    /// Request durable creation of a task specification.
    TaskCreate(TaskCreateCommand),
    /// Request durable creation of a run instance.
    RunCreate(RunCreateCommand),
    /// Request a validated run state transition.
    RunStateTransition(RunStateTransitionCommand),
    /// Request durable record of a run-attempt start.
    AttemptStart(AttemptStartCommand),
    /// Request durable record of a run-attempt finish.
    AttemptFinish(AttemptFinishCommand),
    /// Request durable record of lease acquisition for a run.
    LeaseAcquire(LeaseAcquireCommand),
    /// Request durable record of a lease heartbeat update.
    LeaseHeartbeat(LeaseHeartbeatCommand),
    /// Request durable record of a lease-expired event.
    LeaseExpire(LeaseExpireCommand),
    /// Request durable record of a lease-release event.
    LeaseRelease(LeaseReleaseCommand),
    /// Request durable record of engine pause intent.
    EnginePause(EnginePauseCommand),
    /// Request durable record of engine resume intent.
    EngineResume(EngineResumeCommand),
    /// Request durable record of task cancellation intent.
    TaskCancel(TaskCancelCommand),
    /// Request durable record of task dependency declarations.
    DependencyDeclare(DependencyDeclareCommand),
    /// Request durable record of a run suspension.
    RunSuspend(RunSuspendCommand),
    /// Request durable record of a run resumption.
    RunResume(RunResumeCommand),
    /// Request durable allocation of a budget for a task dimension.
    BudgetAllocate(BudgetAllocateCommand),
    /// Request durable record of resource consumption by a task.
    BudgetConsume(BudgetConsumeCommand),
    /// Request durable replenishment of an exhausted budget.
    BudgetReplenish(BudgetReplenishCommand),
    /// Request durable creation of an event subscription.
    SubscriptionCreate(SubscriptionCreateCommand),
    /// Request durable cancellation of an event subscription.
    SubscriptionCancel(SubscriptionCancelCommand),
    /// Request durable record of a subscription trigger.
    SubscriptionTrigger(SubscriptionTriggerCommand),
    /// Request durable registration of a remote actor.
    ActorRegister(ActorRegisterCommand),
    /// Request durable deregistration of a remote actor.
    ActorDeregister(ActorDeregisterCommand),
    /// Request durable record of an actor heartbeat.
    ActorHeartbeat(ActorHeartbeatCommand),
    /// Request durable creation of an organizational tenant.
    TenantCreate(TenantCreateCommand),
    /// Request durable role assignment for an actor within a tenant.
    RoleAssign(RoleAssignCommand),
    /// Request durable capability grant for an actor within a tenant.
    CapabilityGrant(CapabilityGrantCommand),
    /// Request durable revocation of a capability grant.
    CapabilityRevoke(CapabilityRevokeCommand),
    /// Request durable append of a ledger entry.
    LedgerAppend(LedgerAppendCommand),
}

/// Semantic command for engine-pause control mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnginePauseCommand {
    sequence: u64,
    timestamp: u64,
}

impl EnginePauseCommand {
    /// Creates a new engine-pause command.
    pub fn new(sequence: u64, timestamp: u64) -> Self {
        Self { sequence, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for engine-resume control mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineResumeCommand {
    sequence: u64,
    timestamp: u64,
}

impl EngineResumeCommand {
    /// Creates a new engine-resume command.
    pub fn new(sequence: u64, timestamp: u64) -> Self {
        Self { sequence, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for task creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskCreateCommand {
    sequence: u64,
    task_spec: TaskSpec,
    timestamp: u64,
}

impl TaskCreateCommand {
    /// Creates a new task-creation command.
    pub fn new(sequence: u64, task_spec: TaskSpec, timestamp: u64) -> Self {
        Self { sequence, task_spec, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the task specification.
    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for run creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCreateCommand {
    sequence: u64,
    run_instance: RunInstance,
}

impl RunCreateCommand {
    /// Creates a new run-creation command.
    pub fn new(sequence: u64, run_instance: RunInstance) -> Self {
        Self { sequence, run_instance }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the run instance.
    pub fn run_instance(&self) -> &RunInstance {
        &self.run_instance
    }
}

/// Semantic command for run lifecycle state transition mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStateTransitionCommand {
    sequence: u64,
    run_id: RunId,
    previous_state: RunState,
    new_state: RunState,
    timestamp: u64,
}

impl RunStateTransitionCommand {
    /// Creates a new run-state transition command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        previous_state: RunState,
        new_state: RunState,
        timestamp: u64,
    ) -> Self {
        Self { sequence, run_id, previous_state, new_state, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the expected current run state.
    pub fn previous_state(&self) -> RunState {
        self.previous_state
    }

    /// Returns the requested target run state.
    pub fn new_state(&self) -> RunState {
        self.new_state
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for attempt-start lifecycle mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttemptStartCommand {
    sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    timestamp: u64,
}

impl AttemptStartCommand {
    /// Creates a new attempt-start command.
    pub fn new(sequence: u64, run_id: RunId, attempt_id: AttemptId, timestamp: u64) -> Self {
        Self { sequence, run_id, attempt_id, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the attempt identifier.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Canonical durable attempt outcome taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AttemptResultKind {
    /// Attempt completed successfully.
    Success,
    /// Attempt completed with a non-timeout failure.
    Failure,
    /// Attempt completed due to timeout classification.
    Timeout,
    /// Attempt was preempted (e.g. budget exhaustion) and the run is now Suspended.
    /// Does not count toward the max_attempts retry cap.
    Suspended,
}

/// Semantic grouping of attempt result kind, optional error detail, and optional
/// opaque handler output.
///
/// The `result` and `error` fields are semantically coupled: the error message
/// is only meaningful in the context of its result kind. The `output` field is
/// an opaque byte payload that successful handlers can use to return structured
/// data to the dispatch loop / projection (e.g., ContextStore references).
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "attempt outcome should be inspected for state transition decisions"]
pub struct AttemptOutcome {
    result: AttemptResultKind,
    error: Option<String>,
    /// Optional opaque output bytes produced by the handler.
    ///
    /// For successful attempts, the handler may return structured data (e.g.,
    /// ContextStore keys, diagnostic payloads, external resource identifiers).
    /// This output is threaded through to the WAL and projection so callers can
    /// query it from the run's attempt history.
    output: Option<Vec<u8>>,
}

/// Typed validation error for [`AttemptOutcome`] reconstruction from raw parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcomeError {
    /// A `Success` or `Suspended` outcome was provided with a non-None error field.
    SuccessWithError {
        /// The unexpected error detail.
        error: String,
    },
    /// A `Failure` or `Timeout` outcome was provided without an error field.
    NonSuccessWithoutError {
        /// The result kind that requires an error detail.
        result: AttemptResultKind,
    },
    /// A `Failure` or `Timeout` outcome was provided with output bytes.
    NonSuccessWithOutput {
        /// The result kind that cannot carry output.
        result: AttemptResultKind,
    },
}

impl std::fmt::Display for AttemptOutcomeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptOutcomeError::SuccessWithError { error } => {
                write!(f, "Success/Suspended outcome must not have an error detail, got: {error}")
            }
            AttemptOutcomeError::NonSuccessWithoutError { result } => {
                write!(f, "{result:?} outcome must have an error detail")
            }
            AttemptOutcomeError::NonSuccessWithOutput { result } => {
                write!(f, "non-success outcome {result:?} cannot carry output bytes")
            }
        }
    }
}

impl std::error::Error for AttemptOutcomeError {}

impl AttemptOutcome {
    /// Creates a successful outcome with no error detail and no output.
    pub fn success() -> Self {
        Self { result: AttemptResultKind::Success, error: None, output: None }
    }

    /// Creates a successful outcome carrying opaque output bytes.
    ///
    /// An empty vec is normalized to `None` (no output).
    pub fn success_with_output(output: Vec<u8>) -> Self {
        let output = if output.is_empty() { None } else { Some(output) };
        Self { result: AttemptResultKind::Success, error: None, output }
    }

    /// Creates a failure outcome with an error message.
    pub fn failure(error: impl Into<String>) -> Self {
        Self { result: AttemptResultKind::Failure, error: Some(error.into()), output: None }
    }

    /// Creates a timeout outcome with an error message.
    pub fn timeout(error: impl Into<String>) -> Self {
        Self { result: AttemptResultKind::Timeout, error: Some(error.into()), output: None }
    }

    /// Creates a suspended outcome with no output.
    ///
    /// Suspended attempts do not count toward the max_attempts retry cap.
    pub fn suspended() -> Self {
        Self { result: AttemptResultKind::Suspended, error: None, output: None }
    }

    /// Creates a suspended outcome carrying opaque partial-state bytes.
    ///
    /// An empty vec is normalized to `None` (no output). The handler may use
    /// this to persist partial execution state for use when the run resumes.
    pub fn suspended_with_output(output: Vec<u8>) -> Self {
        let output = if output.is_empty() { None } else { Some(output) };
        Self { result: AttemptResultKind::Suspended, error: None, output }
    }

    /// Reconstructs an outcome from raw parts with semantic validation.
    ///
    /// This is intended for WAL replay / deserialization paths where the result
    /// kind, error, and output are stored separately. Validates that:
    /// - `Success` / `Suspended` have `error == None` (output is optional)
    /// - `Failure` / `Timeout` have `error == Some(_)` and `output == None`
    ///
    /// # Errors
    ///
    /// Returns [`AttemptOutcomeError`] if the result kind and error do not
    /// satisfy the semantic coupling invariant.
    pub fn from_raw_parts(
        result: AttemptResultKind,
        error: Option<String>,
        output: Option<Vec<u8>>,
    ) -> Result<Self, AttemptOutcomeError> {
        match result {
            AttemptResultKind::Success | AttemptResultKind::Suspended => {
                if let Some(err) = error {
                    return Err(AttemptOutcomeError::SuccessWithError { error: err });
                }
            }
            AttemptResultKind::Failure | AttemptResultKind::Timeout => {
                if error.is_none() {
                    return Err(AttemptOutcomeError::NonSuccessWithoutError { result });
                }
                if output.is_some() {
                    return Err(AttemptOutcomeError::NonSuccessWithOutput { result });
                }
            }
        }
        Ok(Self { result, error, output })
    }

    /// Returns the canonical attempt result kind.
    pub fn result(&self) -> AttemptResultKind {
        self.result
    }

    /// Returns the optional error detail.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// Returns the optional opaque output bytes produced by the handler.
    pub fn output(&self) -> Option<&[u8]> {
        self.output.as_deref()
    }

    /// Consumes the outcome, returning its parts for ownership transfer.
    pub fn into_parts(self) -> (AttemptResultKind, Option<String>, Option<Vec<u8>>) {
        (self.result, self.error, self.output)
    }
}

/// Semantic command for attempt-finish lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptFinishCommand {
    sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    outcome: AttemptOutcome,
    timestamp: u64,
}

impl AttemptFinishCommand {
    /// Creates a new attempt-finish command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        attempt_id: AttemptId,
        outcome: AttemptOutcome,
        timestamp: u64,
    ) -> Self {
        Self { sequence, run_id, attempt_id, outcome, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the attempt identifier.
    pub fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the attempt outcome.
    pub fn outcome(&self) -> &AttemptOutcome {
        &self.outcome
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns the canonical attempt result kind.
    pub fn result(&self) -> AttemptResultKind {
        self.outcome.result
    }

    /// Returns the optional error detail.
    pub fn error(&self) -> Option<&str> {
        self.outcome.error.as_deref()
    }

    /// Returns the optional opaque output bytes produced by the handler.
    pub fn output(&self) -> Option<&[u8]> {
        self.outcome.output.as_deref()
    }
}

/// Shared accessor methods for lease command types.
macro_rules! lease_command_accessors {
    () => {
        /// Returns the expected WAL sequence.
        pub fn sequence(&self) -> u64 {
            self.sequence
        }

        /// Returns the targeted run identifier.
        pub fn run_id(&self) -> RunId {
            self.run_id
        }

        /// Returns the lease owner identity.
        pub fn owner(&self) -> &str {
            &self.owner
        }

        /// Returns the lease expiry timestamp.
        pub fn expiry(&self) -> u64 {
            self.expiry
        }

        /// Returns the command timestamp.
        pub fn timestamp(&self) -> u64 {
            self.timestamp
        }
    };
}

/// Semantic command for lease-acquire lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseAcquireCommand {
    sequence: u64,
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}

impl LeaseAcquireCommand {
    /// Creates a new lease-acquire command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        owner: impl Into<String>,
        expiry: u64,
        timestamp: u64,
    ) -> Self {
        let owner = owner.into();
        debug_assert!(!owner.is_empty(), "lease owner must not be empty");
        Self { sequence, run_id, owner, expiry, timestamp }
    }

    lease_command_accessors!();
}

/// Semantic command for lease-heartbeat lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHeartbeatCommand {
    sequence: u64,
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}

impl LeaseHeartbeatCommand {
    /// Creates a new lease-heartbeat command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        owner: impl Into<String>,
        expiry: u64,
        timestamp: u64,
    ) -> Self {
        let owner = owner.into();
        debug_assert!(!owner.is_empty(), "lease owner must not be empty");
        Self { sequence, run_id, owner, expiry, timestamp }
    }

    lease_command_accessors!();
}

/// Semantic command for lease-expire lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseExpireCommand {
    sequence: u64,
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}

impl LeaseExpireCommand {
    /// Creates a new lease-expire command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        owner: impl Into<String>,
        expiry: u64,
        timestamp: u64,
    ) -> Self {
        let owner = owner.into();
        debug_assert!(!owner.is_empty(), "lease owner must not be empty");
        Self { sequence, run_id, owner, expiry, timestamp }
    }

    lease_command_accessors!();
}

/// Semantic command for lease-release lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseReleaseCommand {
    sequence: u64,
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}

impl LeaseReleaseCommand {
    /// Creates a new lease-release command.
    pub fn new(
        sequence: u64,
        run_id: RunId,
        owner: impl Into<String>,
        expiry: u64,
        timestamp: u64,
    ) -> Self {
        let owner = owner.into();
        debug_assert!(!owner.is_empty(), "lease owner must not be empty");
        Self { sequence, run_id, owner, expiry, timestamp }
    }

    lease_command_accessors!();
}

/// Semantic command for task cancellation mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCancelCommand {
    sequence: u64,
    task_id: TaskId,
    timestamp: u64,
}

impl TaskCancelCommand {
    /// Creates a new task-cancel command.
    pub fn new(sequence: u64, task_id: TaskId, timestamp: u64) -> Self {
        Self { sequence, task_id, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted task identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for declaring task dependency relationships.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyDeclareCommand {
    sequence: u64,
    task_id: TaskId,
    depends_on: Vec<TaskId>,
    timestamp: u64,
}

impl DependencyDeclareCommand {
    /// Creates a new dependency declaration command.
    ///
    /// # Panics (debug builds only)
    ///
    /// Panics if `depends_on` is empty. Empty declarations are rejected by the
    /// mutation authority in all builds; the debug_assert guards against logic
    /// errors in internal callers.
    pub fn new(sequence: u64, task_id: TaskId, depends_on: Vec<TaskId>, timestamp: u64) -> Self {
        debug_assert!(!depends_on.is_empty());
        Self { sequence, task_id, depends_on, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the task whose promotion will be gated.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the prerequisite task identifiers.
    pub fn depends_on(&self) -> &[TaskId] {
        &self.depends_on
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Successful mutation command outcome metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct MutationOutcome {
    sequence: u64,
    applied: AppliedMutation,
}

impl MutationOutcome {
    /// Creates a new mutation outcome.
    pub fn new(sequence: u64, applied: AppliedMutation) -> Self {
        Self { sequence, applied }
    }

    /// Returns the WAL sequence assigned to the durable event.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the semantic effect applied by the mutation.
    pub fn applied(&self) -> &AppliedMutation {
        &self.applied
    }
}

/// Applied semantic mutation metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppliedMutation {
    /// Task specification was durably created.
    TaskCreate {
        /// Created task identifier.
        task_id: TaskId,
    },
    /// Run instance was durably created.
    RunCreate {
        /// Created run identifier.
        run_id: RunId,
        /// Owning task identifier.
        task_id: TaskId,
    },
    /// Run lifecycle transition was durably applied.
    RunStateTransition {
        /// Run receiving the transition.
        run_id: RunId,
        /// Transition source state.
        previous_state: RunState,
        /// Transition target state.
        new_state: RunState,
    },
    /// Attempt start was durably applied.
    AttemptStart {
        /// Run that owns the attempt.
        run_id: RunId,
        /// Attempt that started.
        attempt_id: AttemptId,
    },
    /// Attempt finish was durably applied.
    AttemptFinish {
        /// Run that owns the attempt.
        run_id: RunId,
        /// Attempt that finished.
        attempt_id: AttemptId,
        /// Attempt outcome (result kind + optional error detail).
        outcome: AttemptOutcome,
    },
    /// Lease acquire was durably applied.
    LeaseAcquire {
        /// Run targeted by lease acquisition.
        run_id: RunId,
        /// Lease owner identity.
        owner: String,
        /// Lease expiry timestamp.
        expiry: u64,
    },
    /// Lease heartbeat was durably applied.
    LeaseHeartbeat {
        /// Run targeted by lease heartbeat.
        run_id: RunId,
        /// Lease owner identity.
        owner: String,
        /// Lease expiry timestamp after heartbeat.
        expiry: u64,
    },
    /// Lease expiry was durably applied.
    LeaseExpire {
        /// Run targeted by lease expiry.
        run_id: RunId,
        /// Lease owner identity.
        owner: String,
        /// Lease expiry timestamp being expired.
        expiry: u64,
    },
    /// Lease release was durably applied.
    LeaseRelease {
        /// Run targeted by lease release.
        run_id: RunId,
        /// Lease owner identity.
        owner: String,
        /// Lease expiry timestamp at release time.
        expiry: u64,
    },
    /// Engine pause intent was durably applied.
    EnginePause,
    /// Engine resume intent was durably applied.
    EngineResume,
    /// Task cancellation intent was durably applied.
    TaskCancel {
        /// Task targeted by cancellation intent.
        task_id: TaskId,
    },
    /// Task dependency declarations were durably applied.
    DependencyDeclare {
        /// Task whose promotion is gated.
        task_id: TaskId,
        /// The prerequisite task identifiers.
        depends_on: Vec<TaskId>,
    },
    /// Run suspension was durably applied.
    RunSuspend {
        /// The suspended run.
        run_id: RunId,
    },
    /// Run resumption was durably applied.
    RunResume {
        /// The resumed run.
        run_id: RunId,
    },
    /// Budget allocation was durably applied.
    BudgetAllocate {
        /// Task whose budget was allocated.
        task_id: TaskId,
        /// Dimension of the allocated budget.
        dimension: BudgetDimension,
        /// Allocated limit.
        limit: u64,
    },
    /// Budget consumption was durably applied.
    BudgetConsume {
        /// Task whose budget was consumed.
        task_id: TaskId,
        /// Dimension consumed.
        dimension: BudgetDimension,
        /// Amount consumed.
        amount: u64,
    },
    /// Budget replenishment was durably applied.
    BudgetReplenish {
        /// Task whose budget was replenished.
        task_id: TaskId,
        /// Dimension replenished.
        dimension: BudgetDimension,
        /// New limit after replenishment.
        new_limit: u64,
    },
    /// Subscription creation was durably applied.
    SubscriptionCreate {
        /// The created subscription.
        subscription_id: SubscriptionId,
        /// The subscribing task.
        task_id: TaskId,
    },
    /// Subscription cancellation was durably applied.
    SubscriptionCancel {
        /// The canceled subscription.
        subscription_id: SubscriptionId,
    },
    /// Subscription trigger was durably applied.
    SubscriptionTrigger {
        /// The triggered subscription.
        subscription_id: SubscriptionId,
    },
    /// No specific mutation outcome needed (actor/platform events).
    NoOp,
}

/// Engine-facing command submission contract.
///
/// Implementors own mutation ordering and persistence semantics.
pub trait MutationAuthority {
    /// Typed implementation error.
    type Error;

    /// Submit a semantic mutation command through the authority lane.
    fn submit_command(
        &mut self,
        command: MutationCommand,
        durability: DurabilityPolicy,
    ) -> Result<MutationOutcome, Self::Error>;
}

/// Semantic command for run suspension mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSuspendCommand {
    sequence: u64,
    run_id: RunId,
    reason: Option<String>,
    timestamp: u64,
}

impl RunSuspendCommand {
    /// Creates a new run-suspend command.
    pub fn new(sequence: u64, run_id: RunId, reason: Option<String>, timestamp: u64) -> Self {
        Self { sequence, run_id, reason, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the optional suspension reason.
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for run resumption mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunResumeCommand {
    sequence: u64,
    run_id: RunId,
    timestamp: u64,
}

impl RunResumeCommand {
    /// Creates a new run-resume command.
    pub fn new(sequence: u64, run_id: RunId, timestamp: u64) -> Self {
        Self { sequence, run_id, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for budget allocation mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetAllocateCommand {
    sequence: u64,
    task_id: TaskId,
    dimension: BudgetDimension,
    limit: u64,
    timestamp: u64,
}

impl BudgetAllocateCommand {
    /// Creates a new budget-allocate command.
    pub fn new(
        sequence: u64,
        task_id: TaskId,
        dimension: BudgetDimension,
        limit: u64,
        timestamp: u64,
    ) -> Self {
        debug_assert!(limit > 0, "budget limit must be greater than zero");
        Self { sequence, task_id, dimension, limit, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted task identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the budget dimension.
    pub fn dimension(&self) -> BudgetDimension {
        self.dimension
    }

    /// Returns the budget limit.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for budget consumption mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetConsumeCommand {
    sequence: u64,
    task_id: TaskId,
    dimension: BudgetDimension,
    amount: u64,
    timestamp: u64,
}

impl BudgetConsumeCommand {
    /// Creates a new budget-consume command.
    pub fn new(
        sequence: u64,
        task_id: TaskId,
        dimension: BudgetDimension,
        amount: u64,
        timestamp: u64,
    ) -> Self {
        debug_assert!(amount > 0, "budget consume amount must be greater than zero");
        Self { sequence, task_id, dimension, amount, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted task identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the budget dimension.
    pub fn dimension(&self) -> BudgetDimension {
        self.dimension
    }

    /// Returns the amount consumed.
    pub fn amount(&self) -> u64 {
        self.amount
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for budget replenishment mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetReplenishCommand {
    sequence: u64,
    task_id: TaskId,
    dimension: BudgetDimension,
    new_limit: u64,
    timestamp: u64,
}

impl BudgetReplenishCommand {
    /// Creates a new budget-replenish command.
    pub fn new(
        sequence: u64,
        task_id: TaskId,
        dimension: BudgetDimension,
        new_limit: u64,
        timestamp: u64,
    ) -> Self {
        debug_assert!(new_limit > 0, "budget replenish limit must be greater than zero");
        Self { sequence, task_id, dimension, new_limit, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the targeted task identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the budget dimension.
    pub fn dimension(&self) -> BudgetDimension {
        self.dimension
    }

    /// Returns the new budget limit.
    pub fn new_limit(&self) -> u64 {
        self.new_limit
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for subscription creation mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionCreateCommand {
    sequence: u64,
    subscription_id: SubscriptionId,
    task_id: TaskId,
    filter: EventFilter,
    timestamp: u64,
}

impl SubscriptionCreateCommand {
    /// Creates a new subscription-create command.
    pub fn new(
        sequence: u64,
        subscription_id: SubscriptionId,
        task_id: TaskId,
        filter: EventFilter,
        timestamp: u64,
    ) -> Self {
        if let EventFilter::BudgetThreshold { threshold_pct, .. } = &filter {
            debug_assert!(
                *threshold_pct <= 100,
                "budget threshold percentage must be 0-100, got {threshold_pct}"
            );
        }
        if let EventFilter::Custom { key } = &filter {
            debug_assert!(!key.is_empty(), "custom event key must not be empty");
        }
        Self { sequence, subscription_id, task_id, filter, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the subscribing task identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the event filter.
    pub fn filter(&self) -> &EventFilter {
        &self.filter
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for subscription cancellation mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionCancelCommand {
    sequence: u64,
    subscription_id: SubscriptionId,
    timestamp: u64,
}

impl SubscriptionCancelCommand {
    /// Creates a new subscription-cancel command.
    pub fn new(sequence: u64, subscription_id: SubscriptionId, timestamp: u64) -> Self {
        Self { sequence, subscription_id, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for subscription trigger mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionTriggerCommand {
    sequence: u64,
    subscription_id: SubscriptionId,
    timestamp: u64,
}

impl SubscriptionTriggerCommand {
    /// Creates a new subscription-trigger command.
    pub fn new(sequence: u64, subscription_id: SubscriptionId, timestamp: u64) -> Self {
        Self { sequence, subscription_id, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the subscription identifier.
    pub fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

// ── Sprint 4: Actor / Platform mutation commands ─────────────────────────────

/// Semantic command for remote actor registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorRegisterCommand {
    sequence: u64,
    registration: ActorRegistration,
    timestamp: u64,
}

impl ActorRegisterCommand {
    /// Creates a new actor-register command.
    pub fn new(sequence: u64, registration: ActorRegistration, timestamp: u64) -> Self {
        Self { sequence, registration, timestamp }
    }

    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor registration.
    pub fn registration(&self) -> &ActorRegistration {
        &self.registration
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for remote actor deregistration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorDeregisterCommand {
    sequence: u64,
    actor_id: ActorId,
    timestamp: u64,
}

impl ActorDeregisterCommand {
    /// Creates a new actor-deregister command.
    pub fn new(sequence: u64, actor_id: ActorId, timestamp: u64) -> Self {
        Self { sequence, actor_id, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for an actor heartbeat update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorHeartbeatCommand {
    sequence: u64,
    actor_id: ActorId,
    timestamp: u64,
}

impl ActorHeartbeatCommand {
    /// Creates a new actor-heartbeat command.
    pub fn new(sequence: u64, actor_id: ActorId, timestamp: u64) -> Self {
        Self { sequence, actor_id, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for tenant creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantCreateCommand {
    sequence: u64,
    registration: TenantRegistration,
    timestamp: u64,
}

impl TenantCreateCommand {
    /// Creates a new tenant-create command.
    pub fn new(sequence: u64, registration: TenantRegistration, timestamp: u64) -> Self {
        Self { sequence, registration, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the tenant registration.
    pub fn registration(&self) -> &TenantRegistration {
        &self.registration
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for role assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAssignCommand {
    sequence: u64,
    actor_id: ActorId,
    role: Role,
    tenant_id: TenantId,
    timestamp: u64,
}

impl RoleAssignCommand {
    /// Creates a new role-assign command.
    pub fn new(
        sequence: u64,
        actor_id: ActorId,
        role: Role,
        tenant_id: TenantId,
        timestamp: u64,
    ) -> Self {
        Self { sequence, actor_id, role, tenant_id, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    /// Returns the role to assign.
    pub fn role(&self) -> &Role {
        &self.role
    }
    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for capability grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityGrantCommand {
    sequence: u64,
    actor_id: ActorId,
    capability: Capability,
    tenant_id: TenantId,
    timestamp: u64,
}

impl CapabilityGrantCommand {
    /// Creates a new capability-grant command.
    pub fn new(
        sequence: u64,
        actor_id: ActorId,
        capability: Capability,
        tenant_id: TenantId,
        timestamp: u64,
    ) -> Self {
        Self { sequence, actor_id, capability, tenant_id, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    /// Returns the capability to grant.
    pub fn capability(&self) -> &Capability {
        &self.capability
    }
    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for capability revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityRevokeCommand {
    sequence: u64,
    actor_id: ActorId,
    capability: Capability,
    tenant_id: TenantId,
    timestamp: u64,
}

impl CapabilityRevokeCommand {
    /// Creates a new capability-revoke command.
    pub fn new(
        sequence: u64,
        actor_id: ActorId,
        capability: Capability,
        tenant_id: TenantId,
        timestamp: u64,
    ) -> Self {
        Self { sequence, actor_id, capability, tenant_id, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }
    /// Returns the capability to revoke.
    pub fn capability(&self) -> &Capability {
        &self.capability
    }
    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Semantic command for ledger entry append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerAppendCommand {
    sequence: u64,
    entry: LedgerEntry,
    timestamp: u64,
}

impl LedgerAppendCommand {
    /// Creates a new ledger-append command.
    pub fn new(sequence: u64, entry: LedgerEntry, timestamp: u64) -> Self {
        Self { sequence, entry, timestamp }
    }
    /// Returns the expected WAL sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Returns the ledger entry.
    pub fn entry(&self) -> &LedgerEntry {
        &self.entry
    }
    /// Returns the command timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
