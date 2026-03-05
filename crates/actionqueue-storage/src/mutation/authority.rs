//! Storage-owned mutation authority implementation.
//!
//! This module implements the D-04 WAL-first authority lane:
//! validate command -> map event -> append -> durability sync policy -> apply projection.

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    ActorDeregisterCommand, ActorHeartbeatCommand, ActorRegisterCommand, AppliedMutation,
    AttemptFinishCommand, AttemptStartCommand, BudgetAllocateCommand, BudgetConsumeCommand,
    BudgetReplenishCommand, CapabilityGrantCommand, CapabilityRevokeCommand,
    DependencyDeclareCommand, DurabilityPolicy, EnginePauseCommand, EngineResumeCommand,
    LeaseAcquireCommand, LeaseExpireCommand, LeaseHeartbeatCommand, LeaseReleaseCommand,
    LedgerAppendCommand, MutationAuthority, MutationCommand, MutationOutcome, RoleAssignCommand,
    RunCreateCommand, RunResumeCommand, RunStateTransitionCommand, RunSuspendCommand,
    SubscriptionCancelCommand, SubscriptionCreateCommand, SubscriptionTriggerCommand,
    TaskCancelCommand, TaskCreateCommand, TenantCreateCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use actionqueue_core::subscription::SubscriptionId;

use crate::recovery::reducer::ReplayReducer;
use crate::recovery::reducer::ReplayReducerError;
use crate::wal::event::{WalEvent, WalEventType};
use crate::wal::writer::{WalWriter, WalWriterError};

/// Projection behavior required by the mutation authority.
pub trait MutationProjection {
    /// Typed projection apply error.
    type Error;

    /// Returns the latest durable sequence represented by this projection.
    fn latest_sequence(&self) -> u64;

    /// Returns the current run state for validation, if known.
    fn run_state(&self, run_id: &RunId) -> Option<RunState>;

    /// Returns true when the task exists in current projection state.
    fn task_exists(&self, task_id: TaskId) -> bool;

    /// Returns true when the task is already marked canceled in projection state.
    fn is_task_canceled(&self, task_id: TaskId) -> bool;

    /// Returns true when engine control projection is currently paused.
    fn is_engine_paused(&self) -> bool;

    /// Returns the active attempt identifier for the run, if one exists.
    fn active_attempt_id(&self, run_id: &RunId) -> Option<AttemptId>;

    /// Returns the active lease metadata `(owner, expiry)` for the run, if one exists.
    fn active_lease(&self, run_id: &RunId) -> Option<(String, u64)>;

    /// Returns true when a budget allocation exists for the specified (task, dimension) pair.
    fn budget_allocation_exists(&self, _task_id: TaskId, _dimension: BudgetDimension) -> bool {
        false
    }

    /// Returns true when the specified subscription exists in the projection.
    fn subscription_exists(&self, _subscription_id: SubscriptionId) -> bool {
        false
    }

    /// Returns true when the specified subscription has been canceled.
    fn is_subscription_canceled(&self, _subscription_id: SubscriptionId) -> bool {
        false
    }

    /// Applies a durable event to the in-memory projection.
    fn apply_event(&mut self, event: &WalEvent) -> Result<(), Self::Error>;
}

impl MutationProjection for ReplayReducer {
    type Error = ReplayReducerError;

    fn latest_sequence(&self) -> u64 {
        ReplayReducer::latest_sequence(self)
    }

    fn run_state(&self, run_id: &RunId) -> Option<RunState> {
        self.get_run_state(run_id).copied()
    }

    fn task_exists(&self, task_id: TaskId) -> bool {
        self.get_task(&task_id).is_some()
    }

    fn is_task_canceled(&self, task_id: TaskId) -> bool {
        ReplayReducer::is_task_canceled(self, task_id)
    }

    fn is_engine_paused(&self) -> bool {
        ReplayReducer::is_engine_paused(self)
    }

    fn active_attempt_id(&self, run_id: &RunId) -> Option<AttemptId> {
        self.get_run_instance(run_id).and_then(|run| run.current_attempt_id())
    }

    fn active_lease(&self, run_id: &RunId) -> Option<(String, u64)> {
        self.get_lease(run_id).cloned()
    }

    fn budget_allocation_exists(&self, task_id: TaskId, dimension: BudgetDimension) -> bool {
        ReplayReducer::budget_allocation_exists(self, task_id, dimension)
    }

    fn subscription_exists(&self, subscription_id: SubscriptionId) -> bool {
        ReplayReducer::subscription_exists(self, subscription_id)
    }

    fn is_subscription_canceled(&self, subscription_id: SubscriptionId) -> bool {
        ReplayReducer::is_subscription_canceled(self, subscription_id)
    }

    fn apply_event(&mut self, event: &WalEvent) -> Result<(), Self::Error> {
        self.apply(event)
    }
}

/// Storage-owned implementation of the mutation authority lane.
#[derive(Debug)]
pub struct StorageMutationAuthority<W: WalWriter, P: MutationProjection> {
    wal_writer: W,
    projection: P,
}

impl<W: WalWriter, P: MutationProjection> StorageMutationAuthority<W, P> {
    /// Creates a new storage authority with an owned WAL writer and projection.
    pub fn new(wal_writer: W, projection: P) -> Self {
        Self { wal_writer, projection }
    }

    /// Returns the projection used by this authority.
    pub fn projection(&self) -> &P {
        &self.projection
    }

    /// Returns the mutable projection used by this authority.
    pub fn projection_mut(&mut self) -> &mut P {
        &mut self.projection
    }

    /// Decomposes the authority into its owned parts.
    pub fn into_parts(self) -> (W, P) {
        (self.wal_writer, self.projection)
    }

    fn validate_command(
        &self,
        command: &MutationCommand,
    ) -> Result<ValidatedCommand, MutationValidationError> {
        match command {
            MutationCommand::TaskCreate(details) => {
                self.validate_task_create(details)?;
                Ok(ValidatedCommand::TaskCreate(details.clone()))
            }
            MutationCommand::RunCreate(details) => {
                self.validate_run_create(details)?;
                Ok(ValidatedCommand::RunCreate(details.clone()))
            }
            MutationCommand::RunStateTransition(details) => {
                self.validate_run_state_transition(*details)?;
                Ok(ValidatedCommand::RunStateTransition(*details))
            }
            MutationCommand::AttemptStart(details) => {
                self.validate_attempt_start(*details)?;
                Ok(ValidatedCommand::AttemptStart(*details))
            }
            MutationCommand::AttemptFinish(details) => {
                self.validate_attempt_finish(details)?;
                Ok(ValidatedCommand::AttemptFinish(details.clone()))
            }
            MutationCommand::LeaseAcquire(details) => {
                self.validate_lease_acquire(details)?;
                Ok(ValidatedCommand::LeaseAcquire(details.clone()))
            }
            MutationCommand::LeaseHeartbeat(details) => {
                self.validate_lease_heartbeat(details)?;
                Ok(ValidatedCommand::LeaseHeartbeat(details.clone()))
            }
            MutationCommand::LeaseExpire(details) => {
                self.validate_lease_expire(details)?;
                Ok(ValidatedCommand::LeaseExpire(details.clone()))
            }
            MutationCommand::LeaseRelease(details) => {
                self.validate_lease_release(details)?;
                Ok(ValidatedCommand::LeaseRelease(details.clone()))
            }
            MutationCommand::EnginePause(details) => {
                self.validate_engine_pause(*details)?;
                Ok(ValidatedCommand::EnginePause(*details))
            }
            MutationCommand::EngineResume(details) => {
                self.validate_engine_resume(*details)?;
                Ok(ValidatedCommand::EngineResume(*details))
            }
            MutationCommand::TaskCancel(details) => {
                self.validate_task_cancel(*details)?;
                Ok(ValidatedCommand::TaskCancel(*details))
            }
            MutationCommand::DependencyDeclare(details) => {
                self.validate_dependency_declare(details)?;
                Ok(ValidatedCommand::DependencyDeclare(details.clone()))
            }
            MutationCommand::RunSuspend(details) => {
                self.validate_run_suspend(details)?;
                Ok(ValidatedCommand::RunSuspend(details.clone()))
            }
            MutationCommand::RunResume(details) => {
                self.validate_run_resume(details)?;
                Ok(ValidatedCommand::RunResume(*details))
            }
            MutationCommand::BudgetAllocate(details) => {
                self.validate_budget_allocate(details)?;
                Ok(ValidatedCommand::BudgetAllocate(*details))
            }
            MutationCommand::BudgetConsume(details) => {
                self.validate_budget_consume(details)?;
                Ok(ValidatedCommand::BudgetConsume(*details))
            }
            MutationCommand::BudgetReplenish(details) => {
                self.validate_budget_replenish(details)?;
                Ok(ValidatedCommand::BudgetReplenish(*details))
            }
            MutationCommand::SubscriptionCreate(details) => {
                self.validate_subscription_create(details)?;
                Ok(ValidatedCommand::SubscriptionCreate(details.clone()))
            }
            MutationCommand::SubscriptionCancel(details) => {
                self.validate_subscription_cancel(details)?;
                Ok(ValidatedCommand::SubscriptionCancel(*details))
            }
            MutationCommand::SubscriptionTrigger(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::SubscriptionTrigger(*details))
            }
            MutationCommand::ActorRegister(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::ActorRegister(details.clone()))
            }
            MutationCommand::ActorDeregister(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::ActorDeregister(*details))
            }
            MutationCommand::ActorHeartbeat(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::ActorHeartbeat(*details))
            }
            MutationCommand::TenantCreate(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::TenantCreate(details.clone()))
            }
            MutationCommand::RoleAssign(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::RoleAssign(details.clone()))
            }
            MutationCommand::CapabilityGrant(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::CapabilityGrant(details.clone()))
            }
            MutationCommand::CapabilityRevoke(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::CapabilityRevoke(details.clone()))
            }
            MutationCommand::LedgerAppend(details) => {
                self.validate_sequence(details.sequence())?;
                Ok(ValidatedCommand::LedgerAppend(details.clone()))
            }
        }
    }

    fn validate_sequence(&self, provided: u64) -> Result<(), MutationValidationError> {
        let expected_sequence = self
            .projection
            .latest_sequence()
            .checked_add(1)
            .ok_or(MutationValidationError::SequenceOverflow)?;

        if provided != expected_sequence {
            tracing::warn!(
                expected = expected_sequence,
                provided,
                "sequence monotonicity violation"
            );
            return Err(MutationValidationError::NonMonotonicSequence {
                expected: expected_sequence,
                provided,
            });
        }

        Ok(())
    }

    fn validate_task_create(
        &self,
        command: &TaskCreateCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        if self.projection.task_exists(command.task_spec().id()) {
            return Err(MutationValidationError::TaskAlreadyExists {
                task_id: command.task_spec().id(),
            });
        }

        Ok(())
    }

    fn validate_run_create(
        &self,
        command: &RunCreateCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        if !self.projection.task_exists(command.run_instance().task_id()) {
            return Err(MutationValidationError::UnknownTask {
                task_id: command.run_instance().task_id(),
            });
        }

        if self.projection.run_state(&command.run_instance().id()).is_some() {
            return Err(MutationValidationError::RunAlreadyExists {
                run_id: command.run_instance().id(),
            });
        }

        if command.run_instance().state() != RunState::Scheduled {
            return Err(MutationValidationError::RunCreateRequiresScheduled {
                run_id: command.run_instance().id(),
                state: command.run_instance().state(),
            });
        }

        Ok(())
    }

    fn validate_run_state_transition(
        &self,
        command: RunStateTransitionCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        let observed_state = self
            .projection
            .run_state(&command.run_id())
            .ok_or(MutationValidationError::UnknownRun { run_id: command.run_id() })?;

        if observed_state != command.previous_state() {
            return Err(MutationValidationError::PreviousStateMismatch {
                run_id: command.run_id(),
                expected: command.previous_state(),
                actual: observed_state,
            });
        }

        if !is_valid_transition(command.previous_state(), command.new_state()) {
            return Err(MutationValidationError::InvalidTransition {
                run_id: command.run_id(),
                from: command.previous_state(),
                to: command.new_state(),
            });
        }

        Ok(())
    }

    fn validate_attempt_start(
        &self,
        command: AttemptStartCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        let observed_state = self
            .projection
            .run_state(&command.run_id())
            .ok_or(MutationValidationError::AttemptStartUnknownRun { run_id: command.run_id() })?;

        if observed_state != RunState::Running {
            return Err(MutationValidationError::AttemptStartRequiresRunning {
                run_id: command.run_id(),
                state: observed_state,
            });
        }

        if let Some(active_attempt_id) = self.projection.active_attempt_id(&command.run_id()) {
            return Err(MutationValidationError::AttemptStartAlreadyActive {
                run_id: command.run_id(),
                active_attempt_id,
            });
        }

        Ok(())
    }

    fn validate_attempt_finish(
        &self,
        command: &AttemptFinishCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        let observed_state = self
            .projection
            .run_state(&command.run_id())
            .ok_or(MutationValidationError::AttemptFinishUnknownRun { run_id: command.run_id() })?;

        if observed_state != RunState::Running {
            return Err(MutationValidationError::AttemptFinishRequiresRunning {
                run_id: command.run_id(),
                state: observed_state,
            });
        }

        let active_attempt_id = self.projection.active_attempt_id(&command.run_id()).ok_or(
            MutationValidationError::AttemptFinishMissingActive { run_id: command.run_id() },
        )?;

        if active_attempt_id != command.attempt_id() {
            return Err(MutationValidationError::AttemptFinishAttemptMismatch {
                run_id: command.run_id(),
                expected_attempt_id: active_attempt_id,
                provided_attempt_id: command.attempt_id(),
            });
        }

        Ok(())
    }

    fn validate_lease_acquire(
        &self,
        command: &LeaseAcquireCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        let observed_state = self.projection.run_state(&command.run_id()).ok_or(
            MutationValidationError::LeaseUnknownRun {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Acquire,
            },
        )?;

        if !matches!(observed_state, RunState::Ready | RunState::Leased) {
            return Err(MutationValidationError::LeaseInvalidRunState {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Acquire,
                state: observed_state,
            });
        }

        if self.projection.active_lease(&command.run_id()).is_some() {
            return Err(MutationValidationError::LeaseAlreadyActive { run_id: command.run_id() });
        }

        Ok(())
    }

    fn validate_lease_heartbeat(
        &self,
        command: &LeaseHeartbeatCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        let observed_state = self.projection.run_state(&command.run_id()).ok_or(
            MutationValidationError::LeaseUnknownRun {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Heartbeat,
            },
        )?;

        if !matches!(observed_state, RunState::Leased | RunState::Running) {
            return Err(MutationValidationError::LeaseInvalidRunState {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Heartbeat,
                state: observed_state,
            });
        }

        let (active_owner, active_expiry) = self.projection.active_lease(&command.run_id()).ok_or(
            MutationValidationError::LeaseMissingActive {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Heartbeat,
            },
        )?;

        if active_owner != command.owner() {
            return Err(MutationValidationError::LeaseOwnerMismatch {
                run_id: command.run_id(),
                event: LeaseValidationEvent::Heartbeat,
            });
        }

        if command.expiry() < active_expiry {
            return Err(MutationValidationError::LeaseHeartbeatExpiryRegression {
                run_id: command.run_id(),
                previous_expiry: active_expiry,
                proposed_expiry: command.expiry(),
            });
        }

        Ok(())
    }

    fn validate_lease_expire(
        &self,
        command: &LeaseExpireCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_lease_close(LeaseCloseParams {
            sequence: command.sequence(),
            run_id: command.run_id(),
            owner: command.owner(),
            expiry: command.expiry(),
            event: LeaseValidationEvent::Expire,
        })
    }

    fn validate_lease_release(
        &self,
        command: &LeaseReleaseCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_lease_close(LeaseCloseParams {
            sequence: command.sequence(),
            run_id: command.run_id(),
            owner: command.owner(),
            expiry: command.expiry(),
            event: LeaseValidationEvent::Release,
        })
    }

    fn validate_lease_close(
        &self,
        params: LeaseCloseParams<'_>,
    ) -> Result<(), MutationValidationError> {
        let LeaseCloseParams { sequence, run_id, owner, expiry, event } = params;
        self.validate_sequence(sequence)?;

        let observed_state = self
            .projection
            .run_state(&run_id)
            .ok_or(MutationValidationError::LeaseUnknownRun { run_id, event })?;

        if !matches!(observed_state, RunState::Ready | RunState::Leased | RunState::Running) {
            return Err(MutationValidationError::LeaseInvalidRunState {
                run_id,
                event,
                state: observed_state,
            });
        }

        let (active_owner, active_expiry) = self
            .projection
            .active_lease(&run_id)
            .ok_or(MutationValidationError::LeaseMissingActive { run_id, event })?;

        if active_owner != owner {
            return Err(MutationValidationError::LeaseOwnerMismatch { run_id, event });
        }

        if active_expiry != expiry {
            return Err(MutationValidationError::LeaseExpiryMismatch {
                run_id,
                event,
                expected_expiry: active_expiry,
                provided_expiry: expiry,
            });
        }

        Ok(())
    }

    fn validate_task_cancel(
        &self,
        command: TaskCancelCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }

        if self.projection.is_task_canceled(command.task_id()) {
            return Err(MutationValidationError::TaskAlreadyCanceled {
                task_id: command.task_id(),
            });
        }

        Ok(())
    }

    fn validate_dependency_declare(
        &self,
        command: &DependencyDeclareCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        // Reject empty dependency lists — they are semantically meaningless.
        if command.depends_on().is_empty() {
            return Err(MutationValidationError::EmptyDependencyList {
                task_id: command.task_id(),
            });
        }

        // The task that will be gated must exist in the projection.
        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }

        // All prerequisite tasks must also exist.
        for &prereq in command.depends_on() {
            if !self.projection.task_exists(prereq) {
                return Err(MutationValidationError::UnknownTask { task_id: prereq });
            }
        }

        Ok(())
    }

    fn validate_engine_pause(
        &self,
        command: EnginePauseCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        if self.projection.is_engine_paused() {
            return Err(MutationValidationError::EngineAlreadyPaused);
        }

        Ok(())
    }

    fn validate_engine_resume(
        &self,
        command: EngineResumeCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;

        if !self.projection.is_engine_paused() {
            return Err(MutationValidationError::EngineNotPaused);
        }

        Ok(())
    }

    fn validate_run_suspend(
        &self,
        command: &RunSuspendCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        let state = self
            .projection
            .run_state(&command.run_id())
            .ok_or(MutationValidationError::UnknownRun { run_id: command.run_id() })?;
        if state != RunState::Running {
            return Err(MutationValidationError::RunSuspendRequiresRunning {
                run_id: command.run_id(),
                state,
            });
        }
        Ok(())
    }

    fn validate_run_resume(
        &self,
        command: &RunResumeCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        let state = self
            .projection
            .run_state(&command.run_id())
            .ok_or(MutationValidationError::UnknownRun { run_id: command.run_id() })?;
        if state != RunState::Suspended {
            return Err(MutationValidationError::RunResumeRequiresSuspended {
                run_id: command.run_id(),
                state,
            });
        }
        Ok(())
    }

    fn validate_budget_allocate(
        &self,
        command: &BudgetAllocateCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }
        Ok(())
    }

    fn validate_budget_consume(
        &self,
        command: &BudgetConsumeCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }
        Ok(())
    }

    fn validate_budget_replenish(
        &self,
        command: &BudgetReplenishCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }
        if !self.projection.budget_allocation_exists(command.task_id(), command.dimension()) {
            return Err(MutationValidationError::BudgetNotAllocated {
                task_id: command.task_id(),
                dimension: command.dimension(),
            });
        }
        Ok(())
    }

    fn validate_subscription_create(
        &self,
        command: &SubscriptionCreateCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        if !self.projection.task_exists(command.task_id()) {
            return Err(MutationValidationError::UnknownTask { task_id: command.task_id() });
        }
        if self.projection.subscription_exists(command.subscription_id()) {
            return Err(MutationValidationError::SubscriptionAlreadyExists {
                subscription_id: command.subscription_id(),
            });
        }
        Ok(())
    }

    fn validate_subscription_cancel(
        &self,
        command: &SubscriptionCancelCommand,
    ) -> Result<(), MutationValidationError> {
        self.validate_sequence(command.sequence())?;
        if !self.projection.subscription_exists(command.subscription_id()) {
            return Err(MutationValidationError::UnknownSubscription {
                subscription_id: command.subscription_id(),
            });
        }
        if self.projection.is_subscription_canceled(command.subscription_id()) {
            return Err(MutationValidationError::SubscriptionAlreadyCanceled {
                subscription_id: command.subscription_id(),
            });
        }
        Ok(())
    }

    fn build_event_and_applied(validated: ValidatedCommand) -> (WalEvent, AppliedMutation) {
        match validated {
            ValidatedCommand::TaskCreate(command) => {
                let task_id = command.task_spec().id();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::TaskCreated {
                        task_spec: command.task_spec().clone(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::TaskCreate { task_id };
                (event, applied)
            }
            ValidatedCommand::RunCreate(command) => {
                let run_id = command.run_instance().id();
                let task_id = command.run_instance().task_id();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::RunCreated { run_instance: command.run_instance().clone() },
                );
                let applied = AppliedMutation::RunCreate { run_id, task_id };
                (event, applied)
            }
            ValidatedCommand::RunStateTransition(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::RunStateChanged {
                        run_id: command.run_id(),
                        previous_state: command.previous_state(),
                        new_state: command.new_state(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::RunStateTransition {
                    run_id: command.run_id(),
                    previous_state: command.previous_state(),
                    new_state: command.new_state(),
                };
                (event, applied)
            }
            ValidatedCommand::AttemptStart(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::AttemptStarted {
                        run_id: command.run_id(),
                        attempt_id: command.attempt_id(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::AttemptStart {
                    run_id: command.run_id(),
                    attempt_id: command.attempt_id(),
                };
                (event, applied)
            }
            ValidatedCommand::AttemptFinish(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::AttemptFinished {
                        run_id: command.run_id(),
                        attempt_id: command.attempt_id(),
                        result: command.result(),
                        error: command.error().map(|s| s.to_string()),
                        output: command.output().map(|b| b.to_vec()),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::AttemptFinish {
                    run_id: command.run_id(),
                    attempt_id: command.attempt_id(),
                    outcome: command.outcome().clone(),
                };
                (event, applied)
            }
            ValidatedCommand::LeaseAcquire(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::LeaseAcquired {
                        run_id: command.run_id(),
                        owner: command.owner().to_string(),
                        expiry: command.expiry(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::LeaseAcquire {
                    run_id: command.run_id(),
                    owner: command.owner().to_string(),
                    expiry: command.expiry(),
                };
                (event, applied)
            }
            ValidatedCommand::LeaseHeartbeat(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::LeaseHeartbeat {
                        run_id: command.run_id(),
                        owner: command.owner().to_string(),
                        expiry: command.expiry(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::LeaseHeartbeat {
                    run_id: command.run_id(),
                    owner: command.owner().to_string(),
                    expiry: command.expiry(),
                };
                (event, applied)
            }
            ValidatedCommand::LeaseExpire(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::LeaseExpired {
                        run_id: command.run_id(),
                        owner: command.owner().to_string(),
                        expiry: command.expiry(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::LeaseExpire {
                    run_id: command.run_id(),
                    owner: command.owner().to_string(),
                    expiry: command.expiry(),
                };
                (event, applied)
            }
            ValidatedCommand::LeaseRelease(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::LeaseReleased {
                        run_id: command.run_id(),
                        owner: command.owner().to_string(),
                        expiry: command.expiry(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::LeaseRelease {
                    run_id: command.run_id(),
                    owner: command.owner().to_string(),
                    expiry: command.expiry(),
                };
                (event, applied)
            }
            ValidatedCommand::EnginePause(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::EnginePaused { timestamp: command.timestamp() },
                );
                (event, AppliedMutation::EnginePause)
            }
            ValidatedCommand::EngineResume(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::EngineResumed { timestamp: command.timestamp() },
                );
                (event, AppliedMutation::EngineResume)
            }
            ValidatedCommand::TaskCancel(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::TaskCanceled {
                        task_id: command.task_id(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::TaskCancel { task_id: command.task_id() };
                (event, applied)
            }
            ValidatedCommand::DependencyDeclare(command) => {
                let depends_on = command.depends_on().to_vec();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::DependencyDeclared {
                        task_id: command.task_id(),
                        depends_on: depends_on.clone(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied =
                    AppliedMutation::DependencyDeclare { task_id: command.task_id(), depends_on };
                (event, applied)
            }
            ValidatedCommand::RunSuspend(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::RunSuspended {
                        run_id: command.run_id(),
                        reason: command.reason().map(|s| s.to_string()),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::RunSuspend { run_id: command.run_id() })
            }
            ValidatedCommand::RunResume(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::RunResumed {
                        run_id: command.run_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::RunResume { run_id: command.run_id() })
            }
            ValidatedCommand::BudgetAllocate(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::BudgetAllocated {
                        task_id: command.task_id(),
                        dimension: command.dimension(),
                        limit: command.limit(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::BudgetAllocate {
                    task_id: command.task_id(),
                    dimension: command.dimension(),
                    limit: command.limit(),
                };
                (event, applied)
            }
            ValidatedCommand::BudgetConsume(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::BudgetConsumed {
                        task_id: command.task_id(),
                        dimension: command.dimension(),
                        amount: command.amount(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::BudgetConsume {
                    task_id: command.task_id(),
                    dimension: command.dimension(),
                    amount: command.amount(),
                };
                (event, applied)
            }
            ValidatedCommand::BudgetReplenish(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::BudgetReplenished {
                        task_id: command.task_id(),
                        dimension: command.dimension(),
                        new_limit: command.new_limit(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::BudgetReplenish {
                    task_id: command.task_id(),
                    dimension: command.dimension(),
                    new_limit: command.new_limit(),
                };
                (event, applied)
            }
            ValidatedCommand::SubscriptionCreate(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::SubscriptionCreated {
                        subscription_id: command.subscription_id(),
                        task_id: command.task_id(),
                        filter: command.filter().clone(),
                        timestamp: command.timestamp(),
                    },
                );
                let applied = AppliedMutation::SubscriptionCreate {
                    subscription_id: command.subscription_id(),
                    task_id: command.task_id(),
                };
                (event, applied)
            }
            ValidatedCommand::SubscriptionCancel(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::SubscriptionCanceled {
                        subscription_id: command.subscription_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (
                    event,
                    AppliedMutation::SubscriptionCancel {
                        subscription_id: command.subscription_id(),
                    },
                )
            }
            ValidatedCommand::SubscriptionTrigger(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::SubscriptionTriggered {
                        subscription_id: command.subscription_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (
                    event,
                    AppliedMutation::SubscriptionTrigger {
                        subscription_id: command.subscription_id(),
                    },
                )
            }
            ValidatedCommand::ActorRegister(command) => {
                let reg = command.registration();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::ActorRegistered {
                        actor_id: reg.actor_id(),
                        identity: reg.identity().to_string(),
                        capabilities: reg.capabilities().as_slice().to_vec(),
                        department: reg.department().map(|d| d.as_str().to_string()),
                        heartbeat_interval_secs: reg.heartbeat_interval_secs(),
                        tenant_id: reg.tenant_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::ActorDeregister(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::ActorDeregistered {
                        actor_id: command.actor_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::ActorHeartbeat(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::ActorHeartbeat {
                        actor_id: command.actor_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::TenantCreate(command) => {
                let reg = command.registration();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::TenantCreated {
                        tenant_id: reg.tenant_id(),
                        name: reg.name().to_string(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::RoleAssign(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::RoleAssigned {
                        actor_id: command.actor_id(),
                        role: command.role().clone(),
                        tenant_id: command.tenant_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::CapabilityGrant(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::CapabilityGranted {
                        actor_id: command.actor_id(),
                        capability: command.capability().clone(),
                        tenant_id: command.tenant_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::CapabilityRevoke(command) => {
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::CapabilityRevoked {
                        actor_id: command.actor_id(),
                        capability: command.capability().clone(),
                        tenant_id: command.tenant_id(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
            ValidatedCommand::LedgerAppend(command) => {
                let entry = command.entry();
                let event = WalEvent::new(
                    command.sequence(),
                    WalEventType::LedgerEntryAppended {
                        entry_id: entry.entry_id(),
                        tenant_id: entry.tenant_id(),
                        ledger_key: entry.ledger_key().to_string(),
                        actor_id: entry.actor_id(),
                        payload: entry.payload().to_vec(),
                        timestamp: command.timestamp(),
                    },
                );
                (event, AppliedMutation::NoOp)
            }
        }
    }
}

impl<W: WalWriter, P: MutationProjection> MutationAuthority for StorageMutationAuthority<W, P> {
    type Error = MutationAuthorityError<P::Error>;

    fn submit_command(
        &mut self,
        command: MutationCommand,
        durability: DurabilityPolicy,
    ) -> Result<MutationOutcome, Self::Error> {
        // Stage 1: validate command.
        let validated =
            self.validate_command(&command).map_err(MutationAuthorityError::Validation)?;

        // Stage 2: map validated command to canonical WAL event.
        let (event, applied) = Self::build_event_and_applied(validated);

        // Stage 3: append WAL event.
        self.wal_writer.append(&event).map_err(MutationAuthorityError::Append)?;

        // Stage 4: durability sync by policy.
        if durability == DurabilityPolicy::Immediate {
            if let Err(flush_error) = self.wal_writer.flush() {
                return Err(MutationAuthorityError::PartialDurability {
                    sequence: event.sequence(),
                    flush_error,
                });
            }
        }

        // Stage 5: apply in-memory projection.
        self.projection.apply_event(&event).map_err(|source| MutationAuthorityError::Apply {
            sequence: event.sequence(),
            source,
        })?;

        tracing::debug!(sequence = event.sequence(), "command submitted");
        Ok(MutationOutcome::new(event.sequence(), applied))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ValidatedCommand {
    TaskCreate(TaskCreateCommand),
    RunCreate(RunCreateCommand),
    RunStateTransition(RunStateTransitionCommand),
    AttemptStart(AttemptStartCommand),
    AttemptFinish(AttemptFinishCommand),
    LeaseAcquire(LeaseAcquireCommand),
    LeaseHeartbeat(LeaseHeartbeatCommand),
    LeaseExpire(LeaseExpireCommand),
    LeaseRelease(LeaseReleaseCommand),
    EnginePause(EnginePauseCommand),
    EngineResume(EngineResumeCommand),
    TaskCancel(TaskCancelCommand),
    DependencyDeclare(DependencyDeclareCommand),
    RunSuspend(RunSuspendCommand),
    RunResume(RunResumeCommand),
    BudgetAllocate(BudgetAllocateCommand),
    BudgetConsume(BudgetConsumeCommand),
    BudgetReplenish(BudgetReplenishCommand),
    SubscriptionCreate(SubscriptionCreateCommand),
    SubscriptionCancel(SubscriptionCancelCommand),
    SubscriptionTrigger(SubscriptionTriggerCommand),
    ActorRegister(ActorRegisterCommand),
    ActorDeregister(ActorDeregisterCommand),
    ActorHeartbeat(ActorHeartbeatCommand),
    TenantCreate(TenantCreateCommand),
    RoleAssign(RoleAssignCommand),
    CapabilityGrant(CapabilityGrantCommand),
    CapabilityRevoke(CapabilityRevokeCommand),
    LedgerAppend(LedgerAppendCommand),
}

/// Lease lifecycle event kind used by typed validation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseValidationEvent {
    /// Lease acquire command validation.
    Acquire,
    /// Lease heartbeat command validation.
    Heartbeat,
    /// Lease expire command validation.
    Expire,
    /// Lease release command validation.
    Release,
}

/// Parameters for lease-close validation that group the common fields
/// shared by expire and release validation paths.
struct LeaseCloseParams<'a> {
    sequence: u64,
    run_id: RunId,
    owner: &'a str,
    expiry: u64,
    event: LeaseValidationEvent,
}

/// Typed validation failures from the authority validation stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationValidationError {
    /// Projection sequence could not be advanced because it overflowed `u64`.
    SequenceOverflow,
    /// Command sequence is stale or otherwise non-monotonic.
    NonMonotonicSequence {
        /// The expected next sequence.
        expected: u64,
        /// The command-provided sequence.
        provided: u64,
    },
    /// Run targeted by the command does not exist in projection state.
    UnknownRun {
        /// Missing run identifier.
        run_id: RunId,
    },
    /// Task targeted by the command does not exist in projection state.
    UnknownTask {
        /// Missing task identifier.
        task_id: TaskId,
    },
    /// Task creation rejected because task already exists.
    TaskAlreadyExists {
        /// Existing task identifier.
        task_id: TaskId,
    },
    /// Task cancellation rejected because task is already canceled.
    TaskAlreadyCanceled {
        /// Already canceled task identifier.
        task_id: TaskId,
    },
    /// Engine pause command rejected because engine is already paused.
    EngineAlreadyPaused,
    /// Engine resume command rejected because engine is not paused.
    EngineNotPaused,
    /// Run creation rejected because run already exists.
    RunAlreadyExists {
        /// Existing run identifier.
        run_id: RunId,
    },
    /// Run creation requires Scheduled initial state.
    RunCreateRequiresScheduled {
        /// Target run identifier.
        run_id: RunId,
        /// Proposed initial state.
        state: RunState,
    },
    /// Command expected previous state does not match observed projection state.
    PreviousStateMismatch {
        /// Target run identifier.
        run_id: RunId,
        /// Expected command previous state.
        expected: RunState,
        /// Observed projection state.
        actual: RunState,
    },
    /// Command requested an invalid lifecycle transition.
    InvalidTransition {
        /// Target run identifier.
        run_id: RunId,
        /// Requested source state.
        from: RunState,
        /// Requested target state.
        to: RunState,
    },
    /// Attempt-start command references a run that is unknown in projection state.
    AttemptStartUnknownRun {
        /// Missing run identifier.
        run_id: RunId,
    },
    /// Attempt-start command requires the run to be in Running.
    AttemptStartRequiresRunning {
        /// Target run identifier.
        run_id: RunId,
        /// Observed projection state.
        state: RunState,
    },
    /// Attempt-start command rejected because the run already has an active attempt.
    AttemptStartAlreadyActive {
        /// Target run identifier.
        run_id: RunId,
        /// Already active attempt identifier.
        active_attempt_id: AttemptId,
    },
    /// Attempt-finish command references a run that is unknown in projection state.
    AttemptFinishUnknownRun {
        /// Missing run identifier.
        run_id: RunId,
    },
    /// Attempt-finish command requires the run to be in Running.
    AttemptFinishRequiresRunning {
        /// Target run identifier.
        run_id: RunId,
        /// Observed projection state.
        state: RunState,
    },
    /// Attempt-finish command requires an active attempt to exist.
    AttemptFinishMissingActive {
        /// Target run identifier.
        run_id: RunId,
    },
    /// Attempt-finish command attempt ID does not match active projection attempt ID.
    AttemptFinishAttemptMismatch {
        /// Target run identifier.
        run_id: RunId,
        /// Currently active attempt identifier.
        expected_attempt_id: AttemptId,
        /// Attempt identifier provided by command.
        provided_attempt_id: AttemptId,
    },
    /// Lease lifecycle command references a run that is unknown in projection state.
    LeaseUnknownRun {
        /// Missing run identifier.
        run_id: RunId,
        /// Lease lifecycle command kind.
        event: LeaseValidationEvent,
    },
    /// Lease lifecycle command is invalid for the run's observed lifecycle state.
    LeaseInvalidRunState {
        /// Target run identifier.
        run_id: RunId,
        /// Lease lifecycle command kind.
        event: LeaseValidationEvent,
        /// Observed projection run state.
        state: RunState,
    },
    /// Lease-acquire command rejected because a lease is already active.
    LeaseAlreadyActive {
        /// Target run identifier.
        run_id: RunId,
    },
    /// Lease lifecycle command requires active lease metadata but none exists.
    LeaseMissingActive {
        /// Target run identifier.
        run_id: RunId,
        /// Lease lifecycle command kind.
        event: LeaseValidationEvent,
    },
    /// Lease lifecycle command owner does not match active lease owner.
    LeaseOwnerMismatch {
        /// Target run identifier.
        run_id: RunId,
        /// Lease lifecycle command kind.
        event: LeaseValidationEvent,
    },
    /// Lease close command expiry does not match active lease expiry.
    LeaseExpiryMismatch {
        /// Target run identifier.
        run_id: RunId,
        /// Lease lifecycle command kind.
        event: LeaseValidationEvent,
        /// Expected active lease expiry.
        expected_expiry: u64,
        /// Command-provided expiry.
        provided_expiry: u64,
    },
    /// Lease heartbeat command attempted to regress lease expiry.
    LeaseHeartbeatExpiryRegression {
        /// Target run identifier.
        run_id: RunId,
        /// Current active lease expiry.
        previous_expiry: u64,
        /// Heartbeat-proposed expiry.
        proposed_expiry: u64,
    },
    /// Dependency declaration command contains an empty depends_on list.
    EmptyDependencyList {
        /// The task whose dependency list was empty.
        task_id: TaskId,
    },
    /// Run suspend command rejected because the run is not in Running state.
    RunSuspendRequiresRunning {
        /// Target run identifier.
        run_id: RunId,
        /// Observed projection state.
        state: RunState,
    },
    /// Run resume command rejected because the run is not in Suspended state.
    RunResumeRequiresSuspended {
        /// Target run identifier.
        run_id: RunId,
        /// Observed projection state.
        state: RunState,
    },
    /// Budget replenish command rejected because no allocation exists.
    BudgetNotAllocated {
        /// Task targeted by the command.
        task_id: TaskId,
        /// Budget dimension that has no allocation.
        dimension: BudgetDimension,
    },
    /// Subscription create command rejected because the subscription already exists.
    SubscriptionAlreadyExists {
        /// Existing subscription identifier.
        subscription_id: SubscriptionId,
    },
    /// Subscription lifecycle command references a subscription that is unknown.
    UnknownSubscription {
        /// Missing subscription identifier.
        subscription_id: SubscriptionId,
    },
    /// Subscription cancel command rejected because the subscription is already canceled.
    SubscriptionAlreadyCanceled {
        /// Already canceled subscription identifier.
        subscription_id: SubscriptionId,
    },
}

impl std::fmt::Display for MutationValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MutationValidationError::SequenceOverflow => {
                write!(f, "mutation sequence overflow while computing next expected sequence")
            }
            MutationValidationError::NonMonotonicSequence { expected, provided } => {
                write!(
                    f,
                    "mutation sequence rejected: expected next sequence {expected}, received \
                     {provided}"
                )
            }
            MutationValidationError::UnknownRun { run_id } => {
                write!(f, "mutation rejected: unknown run {run_id}")
            }
            MutationValidationError::UnknownTask { task_id } => {
                write!(f, "mutation rejected: unknown task {task_id}")
            }
            MutationValidationError::TaskAlreadyExists { task_id } => {
                write!(f, "mutation rejected: task {task_id} already exists")
            }
            MutationValidationError::TaskAlreadyCanceled { task_id } => {
                write!(f, "mutation rejected: task {task_id} already canceled")
            }
            MutationValidationError::EngineAlreadyPaused => {
                write!(f, "mutation rejected: engine already paused")
            }
            MutationValidationError::EngineNotPaused => {
                write!(f, "mutation rejected: engine not paused")
            }
            MutationValidationError::RunAlreadyExists { run_id } => {
                write!(f, "mutation rejected: run {run_id} already exists")
            }
            MutationValidationError::RunCreateRequiresScheduled { run_id, state } => {
                write!(
                    f,
                    "mutation rejected for run {run_id}: run creation requires Scheduled state, \
                     got {state:?}"
                )
            }
            MutationValidationError::PreviousStateMismatch { run_id, expected, actual } => {
                write!(
                    f,
                    "mutation rejected for run {run_id}: previous_state mismatch \
                     expected={expected:?} actual={actual:?}"
                )
            }
            MutationValidationError::InvalidTransition { run_id, from, to } => {
                write!(
                    f,
                    "mutation rejected for run {run_id}: invalid transition {from:?} -> {to:?}"
                )
            }
            MutationValidationError::AttemptStartUnknownRun { run_id } => {
                write!(f, "attempt-start rejected: unknown run {run_id}")
            }
            MutationValidationError::AttemptStartRequiresRunning { run_id, state } => {
                write!(
                    f,
                    "attempt-start rejected for run {run_id}: run must be Running, observed \
                     {state:?}"
                )
            }
            MutationValidationError::AttemptStartAlreadyActive { run_id, active_attempt_id } => {
                write!(
                    f,
                    "attempt-start rejected for run {run_id}: active attempt {active_attempt_id} \
                     already exists"
                )
            }
            MutationValidationError::AttemptFinishUnknownRun { run_id } => {
                write!(f, "attempt-finish rejected: unknown run {run_id}")
            }
            MutationValidationError::AttemptFinishRequiresRunning { run_id, state } => {
                write!(
                    f,
                    "attempt-finish rejected for run {run_id}: run must be Running, observed \
                     {state:?}"
                )
            }
            MutationValidationError::AttemptFinishMissingActive { run_id } => {
                write!(f, "attempt-finish rejected for run {run_id}: no active attempt present")
            }
            MutationValidationError::AttemptFinishAttemptMismatch {
                run_id,
                expected_attempt_id,
                provided_attempt_id,
            } => {
                write!(
                    f,
                    "attempt-finish rejected for run {run_id}: attempt mismatch \
                     expected={expected_attempt_id} provided={provided_attempt_id}"
                )
            }
            MutationValidationError::LeaseUnknownRun { run_id, event } => {
                write!(f, "{event} rejected: unknown run {run_id}")
            }
            MutationValidationError::LeaseInvalidRunState { run_id, event, state } => {
                write!(f, "{event} rejected for run {run_id}: invalid state {state:?}")
            }
            MutationValidationError::LeaseAlreadyActive { run_id } => {
                write!(f, "lease acquire rejected for run {run_id}: lease already active")
            }
            MutationValidationError::LeaseMissingActive { run_id, event } => {
                write!(f, "{event} rejected for run {run_id}: missing active lease")
            }
            MutationValidationError::LeaseOwnerMismatch { run_id, event } => {
                write!(f, "{event} rejected for run {run_id}: owner mismatch")
            }
            MutationValidationError::LeaseExpiryMismatch {
                run_id,
                event,
                expected_expiry,
                provided_expiry,
            } => {
                write!(
                    f,
                    "{event} rejected for run {run_id}: expiry mismatch \
                     expected={expected_expiry} provided={provided_expiry}"
                )
            }
            MutationValidationError::LeaseHeartbeatExpiryRegression {
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
            MutationValidationError::EmptyDependencyList { task_id } => {
                write!(
                    f,
                    "dependency declaration rejected for task {task_id}: depends_on list is empty"
                )
            }
            MutationValidationError::RunSuspendRequiresRunning { run_id, state } => {
                write!(
                    f,
                    "run-suspend rejected for run {run_id}: run must be Running, observed \
                     {state:?}"
                )
            }
            MutationValidationError::RunResumeRequiresSuspended { run_id, state } => {
                write!(
                    f,
                    "run-resume rejected for run {run_id}: run must be Suspended, observed \
                     {state:?}"
                )
            }
            MutationValidationError::BudgetNotAllocated { task_id, dimension } => {
                write!(
                    f,
                    "budget-replenish rejected for task {task_id}: no allocation exists for \
                     dimension {dimension}"
                )
            }
            MutationValidationError::SubscriptionAlreadyExists { subscription_id } => {
                write!(
                    f,
                    "subscription-create rejected: subscription {subscription_id} already exists"
                )
            }
            MutationValidationError::UnknownSubscription { subscription_id } => {
                write!(f, "mutation rejected: unknown subscription {subscription_id}")
            }
            MutationValidationError::SubscriptionAlreadyCanceled { subscription_id } => {
                write!(
                    f,
                    "subscription-cancel rejected: subscription {subscription_id} already canceled"
                )
            }
        }
    }
}

impl std::fmt::Display for LeaseValidationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseValidationEvent::Acquire => write!(f, "lease acquire"),
            LeaseValidationEvent::Heartbeat => write!(f, "lease heartbeat"),
            LeaseValidationEvent::Expire => write!(f, "lease expire"),
            LeaseValidationEvent::Release => write!(f, "lease release"),
        }
    }
}

impl std::error::Error for MutationValidationError {}

/// Typed stage-aware authority failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationAuthorityError<ProjectionError> {
    /// Validation stage failure.
    Validation(MutationValidationError),
    /// WAL append stage failure.
    Append(WalWriterError),
    /// Append succeeded but flush failed. The event MAY be durable on restart
    /// (OS page cache) but fsync was not confirmed.
    PartialDurability {
        /// Sequence that was appended but not confirmed durable.
        sequence: u64,
        /// Underlying flush error.
        flush_error: WalWriterError,
    },
    /// Projection apply stage failure after append.
    Apply {
        /// Sequence that was already appended durably.
        sequence: u64,
        /// Underlying projection apply error.
        source: ProjectionError,
    },
}

impl<ProjectionError: std::fmt::Display> std::fmt::Display
    for MutationAuthorityError<ProjectionError>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MutationAuthorityError::Validation(error) => {
                write!(f, "mutation validation failed: {error}")
            }
            MutationAuthorityError::Append(error) => {
                write!(f, "mutation append stage failed: {error}")
            }
            MutationAuthorityError::PartialDurability { sequence, flush_error } => {
                write!(
                    f,
                    "mutation partial durability: append succeeded at sequence {sequence} but \
                     flush failed: {flush_error}"
                )
            }
            MutationAuthorityError::Apply { sequence, source } => {
                write!(
                    f,
                    "mutation apply stage failed after durable append sequence {sequence}: \
                     {source}"
                )
            }
        }
    }
}

impl<ProjectionError: std::error::Error + 'static> std::error::Error
    for MutationAuthorityError<ProjectionError>
{
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{RunId, TaskId};
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

    use super::*;

    #[derive(Debug, Default)]
    struct ProjectionStub {
        latest_sequence: u64,
        tasks: std::collections::HashSet<TaskId>,
        canceled_tasks: std::collections::HashSet<TaskId>,
        engine_paused: bool,
        runs: std::collections::HashMap<RunId, RunState>,
        active_attempts: std::collections::HashMap<RunId, AttemptId>,
        active_leases: std::collections::HashMap<RunId, (String, u64)>,
    }

    impl MutationProjection for ProjectionStub {
        type Error = &'static str;

        fn latest_sequence(&self) -> u64 {
            self.latest_sequence
        }

        fn run_state(&self, run_id: &RunId) -> Option<RunState> {
            self.runs.get(run_id).copied()
        }

        fn task_exists(&self, task_id: TaskId) -> bool {
            self.tasks.contains(&task_id)
        }

        fn is_task_canceled(&self, task_id: TaskId) -> bool {
            self.canceled_tasks.contains(&task_id)
        }

        fn is_engine_paused(&self) -> bool {
            self.engine_paused
        }

        fn active_attempt_id(&self, run_id: &RunId) -> Option<AttemptId> {
            self.active_attempts.get(run_id).copied()
        }

        fn active_lease(&self, run_id: &RunId) -> Option<(String, u64)> {
            self.active_leases.get(run_id).cloned()
        }

        fn apply_event(&mut self, event: &WalEvent) -> Result<(), Self::Error> {
            self.latest_sequence = event.sequence();
            match event.event() {
                WalEventType::TaskCreated { task_spec, .. } => {
                    self.tasks.insert(task_spec.id());
                }
                WalEventType::RunCreated { run_instance } => {
                    self.runs.insert(run_instance.id(), run_instance.state());
                }
                WalEventType::RunStateChanged { run_id, new_state, .. } => {
                    self.runs.insert(*run_id, *new_state);
                }
                WalEventType::AttemptStarted { run_id, attempt_id, .. } => {
                    self.active_attempts.insert(*run_id, *attempt_id);
                }
                WalEventType::AttemptFinished { run_id, .. } => {
                    self.active_attempts.remove(run_id);
                }
                WalEventType::LeaseAcquired { run_id, owner, expiry, .. } => {
                    self.active_leases.insert(*run_id, (owner.clone(), *expiry));
                }
                WalEventType::LeaseHeartbeat { run_id, owner, expiry, .. } => {
                    self.active_leases.insert(*run_id, (owner.clone(), *expiry));
                }
                WalEventType::LeaseExpired { run_id, .. }
                | WalEventType::LeaseReleased { run_id, .. } => {
                    self.active_leases.remove(run_id);
                }
                WalEventType::TaskCanceled { task_id, .. } => {
                    self.canceled_tasks.insert(*task_id);
                }
                WalEventType::EnginePaused { .. } => {
                    self.engine_paused = true;
                }
                WalEventType::EngineResumed { .. } => {
                    self.engine_paused = false;
                }
                WalEventType::DependencyDeclared { .. } => {
                    // Dependency declarations are tracked in the full ReplayReducer,
                    // not in the lightweight test projection stub.
                }
                _ => {
                    // Sprint 3+ event types are not tracked in the lightweight test
                    // projection stub; they are handled by the full ReplayReducer.
                }
            }
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct WriterStub {
        events: Vec<WalEvent>,
        fail_append: bool,
        fail_flush: bool,
    }

    impl WalWriter for WriterStub {
        fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError> {
            if self.fail_append {
                return Err(WalWriterError::IoError("append failed".to_string()));
            }
            self.events.push(event.clone());
            Ok(())
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            if self.fail_flush {
                return Err(WalWriterError::IoError("flush failed".to_string()));
            }
            Ok(())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    fn test_task_spec(task_id: TaskId) -> TaskSpec {
        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("test task spec should be valid")
    }

    #[test]
    fn submits_task_create_then_run_create_then_transition() {
        let task_id = TaskId::new();
        let run =
            actionqueue_core::run::run_instance::RunInstance::new_scheduled(task_id, 100, 100)
                .expect("test run should be valid");

        let writer = WriterStub::default();
        let projection = ProjectionStub::default();
        let mut authority = StorageMutationAuthority::new(writer, projection);

        let task_outcome = authority
            .submit_command(
                MutationCommand::TaskCreate(TaskCreateCommand::new(1, test_task_spec(task_id), 10)),
                DurabilityPolicy::Immediate,
            )
            .expect("task create should succeed");
        assert_eq!(task_outcome.sequence(), 1);

        let run_outcome = authority
            .submit_command(
                MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
                DurabilityPolicy::Immediate,
            )
            .expect("run create should succeed");
        assert_eq!(run_outcome.sequence(), 2);

        let transition_outcome = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    3,
                    run.id(),
                    RunState::Scheduled,
                    RunState::Ready,
                    100,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("transition should succeed");
        assert_eq!(transition_outcome.sequence(), 3);

        let (_writer, projection) = authority.into_parts();
        assert!(projection.task_exists(task_id));
        assert_eq!(projection.run_state(&run.id()), Some(RunState::Ready));
    }

    #[test]
    fn partial_durability_when_append_ok_flush_err() {
        let task_id = TaskId::new();
        let writer = WriterStub { fail_flush: true, ..Default::default() };
        let projection = ProjectionStub::default();
        let mut authority = StorageMutationAuthority::new(writer, projection);

        let result = authority.submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, test_task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        );

        match result {
            Err(MutationAuthorityError::PartialDurability { sequence, flush_error }) => {
                assert_eq!(sequence, 1);
                assert_eq!(flush_error, WalWriterError::IoError("flush failed".to_string()));
            }
            other => panic!("expected PartialDurability, got {other:?}"),
        }
    }
}
