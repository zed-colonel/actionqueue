//! Embedded API surface for the ActionQueue runtime.
//!
//! Provides [`ActionQueueEngine`] as the primary entry point for embedding
//! ActionQueue as a library in Rust applications.

use actionqueue_core::task::task_spec::TaskSpec;
use actionqueue_engine::time::clock::{Clock, SystemClock};
use actionqueue_executor_local::handler::ExecutorHandler;
use actionqueue_storage::recovery::bootstrap::RecoveryBootstrapError;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::writer::InstrumentedWalWriter;
use tracing;

use crate::config::RuntimeConfig;
use crate::dispatch::{DispatchError, DispatchLoop, RunSummary, TickResult};

/// Errors that can occur during engine bootstrap.
#[derive(Debug)]
pub enum BootstrapError {
    /// Configuration is invalid.
    Config(crate::config::ConfigError),
    /// Storage recovery failed.
    Recovery(RecoveryBootstrapError),
    /// Directory creation failed.
    Io(String),
    /// Dispatch loop initialization failed.
    Dispatch(DispatchError),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Config(e) => write!(f, "config error: {e}"),
            BootstrapError::Recovery(e) => write!(f, "recovery error: {e}"),
            BootstrapError::Io(e) => write!(f, "I/O error: {e}"),
            BootstrapError::Dispatch(e) => write!(f, "dispatch init error: {e}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

/// Errors that can occur during engine operations.
#[derive(Debug)]
pub enum EngineError {
    /// Dispatch loop error.
    Dispatch(DispatchError),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Dispatch(e) => write!(f, "dispatch error: {e}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<DispatchError> for EngineError {
    fn from(e: DispatchError) -> Self {
        EngineError::Dispatch(e)
    }
}

/// Pre-bootstrap engine configuration holder.
pub struct ActionQueueEngine<H: ExecutorHandler> {
    config: RuntimeConfig,
    handler: H,
}

impl<H: ExecutorHandler + 'static> ActionQueueEngine<H> {
    /// Creates a new engine with the given configuration and handler.
    pub fn new(config: RuntimeConfig, handler: H) -> Self {
        Self { config, handler }
    }

    /// Bootstraps the engine by recovering state from storage and
    /// constructing the dispatch loop with the system clock.
    pub fn bootstrap(self) -> Result<BootstrappedEngine<H, SystemClock>, BootstrapError> {
        self.bootstrap_with_clock(SystemClock)
    }

    /// Bootstraps with an explicit clock (for testing).
    pub fn bootstrap_with_clock<C: Clock>(
        self,
        clock: C,
    ) -> Result<BootstrappedEngine<H, C>, BootstrapError> {
        self.config.validate().map_err(BootstrapError::Config)?;

        let data_dir = self.config.data_dir.display().to_string();
        tracing::info!(data_dir, "bootstrapping engine");

        // Recover from storage
        let recovery = actionqueue_storage::recovery::bootstrap::load_projection_with_features(
            &self.config.data_dir,
            self.config.store_features.clone(),
        )
        .map_err(BootstrapError::Recovery)?;

        // Build mutation authority
        let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
            recovery.wal_writer,
            recovery.projection,
        );

        authority.set_admission_limits(self.config.admission_limits);
        authority.set_continuation_limits(self.config.continuation_limits);
        authority.set_signal_limits(self.config.signal_limits);
        authority.set_signal_retention_policy(self.config.signal_retention);

        // Compute snapshot path — must match bootstrap.rs snapshot_dir / "snapshot.bin"
        let snapshot_path = self
            .config
            .snapshot_event_threshold
            .map(|_| self.config.data_dir.join("snapshots").join("snapshot.bin"));

        // Build dispatch loop
        let dispatch = DispatchLoop::new(
            authority,
            self.handler,
            clock,
            crate::dispatch::DispatchConfig::new(
                self.config.backoff_strategy.clone(),
                self.config.dispatch_concurrency.get(),
                self.config.lease_timeout_secs,
                snapshot_path,
                self.config.snapshot_event_threshold,
            )
            .with_local_executor_traits(self.config.local_executor_traits.clone()),
        )
        .map_err(BootstrapError::Dispatch)?;

        tracing::info!(data_dir, "engine bootstrap complete");
        Ok(BootstrappedEngine { dispatch })
    }
}

/// A bootstrapped engine ready for task submission and execution.
pub struct BootstrappedEngine<H: ExecutorHandler + 'static, C: Clock = SystemClock> {
    dispatch: DispatchLoop<InstrumentedWalWriter<WalFsWriter>, H, C>,
}

impl<H: ExecutorHandler + 'static, C: Clock> BootstrappedEngine<H, C> {
    /// Idempotent convenience admission with `task/<uuid>` key, trace, and correlation.
    /// Constructs an explicitly host-bound embedded control surface.
    pub fn with_host(mut self, host: actionqueue_core::control::HostControlContext) -> Self {
        self.set_control_context(Some(host));
        self
    }
    /// Binds a trusted host identity/scope for embedded control convenience methods.
    /// Execution and recovery do not inherit control permissions from this binding.
    pub fn set_control_context(
        &mut self,
        host: Option<actionqueue_core::control::HostControlContext>,
    ) {
        self.dispatch.set_control_context(host);
    }
    /// Inspects eligible remote work under current grants.
    #[cfg(feature = "actor")]
    pub fn claimable_remote(
        &self,
        host: &actionqueue_core::control::HostControlContext,
    ) -> Result<Vec<actionqueue_core::ids::RunId>, actionqueue_core::control::ControlError> {
        self.dispatch.claimable_remote(host)
    }
    /// Renews the named remote attempt and fence.
    #[cfg(feature = "actor")]
    // Preserve the explicit dependencies of this existing boundary API.
    #[allow(clippy::too_many_arguments)]
    pub fn renew_remote(
        &mut self,
        host: &actionqueue_core::control::HostControlContext,
        run_id: actionqueue_core::ids::RunId,
        attempt_id: actionqueue_core::ids::AttemptId,
        fence: actionqueue_core::mutation::LeaseFence,
        expiry: u64,
    ) -> Result<(), actionqueue_core::control::ControlError> {
        self.dispatch.renew_remote(host, run_id, attempt_id, fence, expiry)
    }
    /// Claims remote execution under the embedded loop's coordination gates.
    #[cfg(feature = "actor")]
    pub fn claim_remote(
        &mut self,
        host: &actionqueue_core::control::HostControlContext,
        request: actionqueue_actor::protocol::RemoteClaim,
    ) -> Result<crate::remote::RemoteWork, actionqueue_core::control::ControlError> {
        self.dispatch.claim_remote(host, request)
    }
    /// Commits an authenticated remote result and refreshes runtime coordination.
    #[cfg(feature = "actor")]
    pub fn submit_remote_result(
        &mut self,
        host: &actionqueue_core::control::HostControlContext,
        result: actionqueue_actor::protocol::RemoteAttemptResult,
    ) -> Result<(), actionqueue_core::control::ControlError> {
        self.dispatch.submit_remote_result(host, result)
    }
    /// Retain the preallocated task UUID on retry.
    pub fn control(
        &mut self,
        operation: crate::control::ControlOperation,
    ) -> Result<crate::control::ControlOutcome, crate::control::ServiceError> {
        self.dispatch.control(operation)
    }
    /// Redacted host-bound structural inspection.
    pub fn inspector(
        &self,
    ) -> Result<crate::inspection::Inspector<'_>, crate::inspection::InspectionError> {
        self.dispatch.inspector(Default::default(), false)
    }
    /// Disclosure additionally requires an explicit trusted host policy.
    pub fn inspector_with_disclosure(
        &self,
        policy: crate::inspection::DisclosurePolicy,
    ) -> Result<crate::inspection::Inspector<'_>, crate::inspection::InspectionError> {
        self.dispatch.inspector(policy, true)
    }
    pub fn submit_task(
        &mut self,
        spec: TaskSpec,
    ) -> Result<actionqueue_core::admission::EnsureTaskOutcome, crate::admission::AdmissionError>
    {
        let task_id = spec.id();
        tracing::debug!(%task_id, "submit_task");
        self.dispatch.submit_task(spec)
    }

    pub fn get_admission(
        &self,
        key: &actionqueue_core::ids::AdmissionKey,
    ) -> Result<crate::views::AdmissionView, crate::inspection::InspectionError> {
        self.inspector()?.get_admission(key)
    }
    pub fn get_task(
        &self,
        id: actionqueue_core::ids::TaskId,
    ) -> Result<crate::views::TaskView, crate::inspection::InspectionError> {
        self.inspector()?.get_task(id)
    }
    pub fn get_run(
        &self,
        id: actionqueue_core::ids::RunId,
    ) -> Result<crate::views::RunView, crate::inspection::InspectionError> {
        self.inspector()?.get_run(id)
    }
    pub fn get_attempt(
        &self,
        run: actionqueue_core::ids::RunId,
        attempt: actionqueue_core::ids::AttemptId,
    ) -> Result<crate::views::AttemptView, crate::inspection::InspectionError> {
        self.inspector()?.get_attempt(run, attempt)
    }
    pub fn get_wait(
        &self,
        id: actionqueue_core::ids::WaitId,
    ) -> Result<crate::views::WaitView, crate::inspection::InspectionError> {
        self.inspector()?.get_wait(id)
    }
    pub fn get_signal(
        &self,
        id: &actionqueue_core::ids::SignalId,
    ) -> Result<crate::views::SignalView, crate::inspection::InspectionError> {
        self.inspector()?.get_signal(id)
    }
    pub fn get_checkpoint(
        &self,
        id: actionqueue_core::ids::CheckpointId,
    ) -> Result<crate::views::CheckpointView, crate::inspection::InspectionError> {
        self.inspector()?.get_checkpoint(id)
    }
    #[cfg(feature = "serde")]
    pub fn list_signals(
        &self,
        q: &crate::inspection::Query,
    ) -> Result<crate::views::Page<crate::views::SignalView>, crate::inspection::InspectionError>
    {
        self.inspector()?.list_signals(q)
    }
    #[cfg(feature = "serde")]
    pub fn list_waits(
        &self,
        q: &crate::inspection::Query,
    ) -> Result<crate::views::Page<crate::views::WaitView>, crate::inspection::InspectionError>
    {
        self.inspector()?.list_waits(q)
    }
    #[cfg(feature = "serde")]
    pub fn trace(
        &self,
        q: &crate::inspection::Query,
    ) -> Result<crate::views::TraceView, crate::inspection::InspectionError> {
        self.inspector()?.trace(q)
    }
    pub fn cancel_task(
        &mut self,
        id: actionqueue_core::ids::TaskId,
    ) -> Result<crate::control::ControlOutcome, crate::control::ServiceError> {
        self.control(crate::control::ControlOperation::Cancel(
            actionqueue_core::mutation::CancelTarget::Task(id),
        ))
    }
    pub fn cancel_run(
        &mut self,
        id: actionqueue_core::ids::RunId,
    ) -> Result<crate::control::ControlOutcome, crate::control::ServiceError> {
        self.control(crate::control::ControlOperation::Cancel(
            actionqueue_core::mutation::CancelTarget::Run(id),
        ))
    }
    pub fn cancel_wait(
        &mut self,
        run_id: actionqueue_core::ids::RunId,
        wait_id: actionqueue_core::ids::WaitId,
    ) -> Result<crate::control::ControlOutcome, crate::control::ServiceError> {
        self.control(crate::control::ControlOperation::CancelWait { run_id, wait_id })
    }
    pub fn resolve_wait(
        &mut self,
        run_id: actionqueue_core::ids::RunId,
        wait_id: actionqueue_core::ids::WaitId,
    ) -> Result<crate::control::ControlOutcome, crate::control::ServiceError> {
        self.control(crate::control::ControlOperation::ResolveWait { run_id, wait_id })
    }
    /// Resident signal and live capacity-rejection counters.
    pub fn signal_statistics(&self) -> actionqueue_storage::recovery::signals::SignalStatistics {
        self.dispatch.signal_statistics()
    }
    /// Admits a signal with the configured trusted host context.
    pub fn admit_signal(
        &mut self,
        request: actionqueue_core::continuation::AdmitSignalRequest,
    ) -> Result<actionqueue_core::continuation::AdmitSignalOutcome, crate::control::ServiceError>
    {
        match self.control(crate::control::ControlOperation::AdmitSignal(request))? {
            crate::control::ControlOutcome::Signal(outcome) => Ok(outcome),
            _ => unreachable!("signal operation"),
        }
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn pin_signal(
        &mut self,
        signal_id: actionqueue_core::ids::SignalId,
        pin_id: actionqueue_core::continuation::SignalPinId,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        self.dispatch.pin_signal(signal_id, pin_id, ingress)
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn unpin_signal(
        &mut self,
        signal_id: actionqueue_core::ids::SignalId,
        pin_id: actionqueue_core::continuation::SignalPinId,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        self.dispatch.unpin_signal(signal_id, pin_id, ingress)
    }
    /// Explicit durable retention control through the mutation authority.
    pub fn retire_signals(
        &mut self,
        sequences: Vec<actionqueue_core::ids::SignalSequence>,
        ingress: actionqueue_core::continuation::SignalIngressContext,
    ) -> Result<usize, crate::signals::SignalAdmissionError> {
        self.dispatch.retire_signals(sequences, ingress)
    }
    /// Ensures a durable tenant-scoped admission; changed meaning returns a typed conflict.
    pub fn ensure_task(
        &mut self,
        request: actionqueue_core::admission::EnsureTaskRequest,
    ) -> Result<actionqueue_core::admission::EnsureTaskOutcome, crate::control::ServiceError> {
        match self.control(crate::control::ControlOperation::AdmitTask(request))? {
            crate::control::ControlOutcome::Task(outcome) => Ok(outcome),
            _ => unreachable!("task operation"),
        }
    }
    /// Advances the dispatch loop by one tick.
    pub async fn tick(&mut self) -> Result<TickResult, EngineError> {
        self.dispatch.tick().await.map_err(EngineError::Dispatch)
    }

    /// Runs the dispatch loop until no work remains.
    pub async fn run_until_idle(&mut self) -> Result<RunSummary, EngineError> {
        self.dispatch.run_until_idle().await.map_err(EngineError::Dispatch)
    }

    /// Returns a reference to the current projection state.
    pub fn projection(&self) -> &ReplayReducer {
        self.dispatch.projection()
    }

    /// Declares a DAG dependency.
    pub fn declare_dependency(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        prereqs: Vec<actionqueue_core::ids::TaskId>,
    ) -> Result<(), EngineError> {
        tracing::debug!(%task_id, prereq_count = prereqs.len(), "declare_dependency");
        self.dispatch.declare_dependency(task_id, prereqs).map_err(EngineError::Dispatch)
    }

    /// Allocates a token budget for a task/dimension pair.
    #[cfg(feature = "budget")]
    pub fn allocate_budget(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
        limit: u64,
    ) -> Result<(), EngineError> {
        tracing::debug!(%task_id, ?dimension, limit, "allocate_budget");
        self.dispatch.allocate_budget(task_id, dimension, limit).map_err(EngineError::Dispatch)
    }

    /// Replenishes an exhausted budget dimension for a task.
    #[cfg(feature = "budget")]
    pub fn replenish_budget(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
        new_limit: u64,
    ) -> Result<(), EngineError> {
        tracing::debug!(%task_id, ?dimension, new_limit, "replenish_budget");
        self.dispatch.replenish_budget(task_id, dimension, new_limit).map_err(EngineError::Dispatch)
    }

    /// Query remaining budget for a task on a specific dimension.
    ///
    /// Returns `None` if no budget is allocated for this task+dimension.
    #[cfg(feature = "budget")]
    pub fn budget_remaining(
        &self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
    ) -> Option<u64> {
        self.dispatch.budget_remaining(task_id, dimension)
    }

    /// Check if a budget dimension is exhausted for a task.
    ///
    /// Returns `false` if no budget is allocated (no budget = no limit).
    #[cfg(feature = "budget")]
    pub fn is_budget_exhausted(
        &self,
        task_id: actionqueue_core::ids::TaskId,
        dimension: actionqueue_core::budget::BudgetDimension,
    ) -> bool {
        self.dispatch.is_budget_exhausted(task_id, dimension)
    }

    /// Resumes a suspended run (transitions Suspended → Ready).
    pub fn resume_run(&mut self, run_id: actionqueue_core::ids::RunId) -> Result<(), EngineError> {
        tracing::debug!(%run_id, "resume_run");
        self.dispatch.resume_run(run_id).map_err(EngineError::Dispatch)
    }

    /// Creates a new event subscription.
    ///
    /// Returns the generated `SubscriptionId` which the caller may use to
    /// inspect the subscription state later.
    #[cfg(feature = "budget")]
    pub fn create_subscription(
        &mut self,
        task_id: actionqueue_core::ids::TaskId,
        filter: actionqueue_core::subscription::EventFilter,
    ) -> Result<actionqueue_core::subscription::SubscriptionId, EngineError> {
        tracing::debug!(%task_id, "create_subscription");
        self.dispatch.create_subscription(task_id, filter).map_err(EngineError::Dispatch)
    }

    // ── Actor feature ──────────────────────────────────────────────────────

    /// Registers a remote actor with the hub.
    #[cfg(feature = "actor")]
    pub fn register_actor(
        &mut self,
        registration: actionqueue_core::actor::ActorRegistration,
    ) -> Result<(), EngineError> {
        let actor_id = registration.actor_id();
        tracing::debug!(%actor_id, "register_actor");
        self.dispatch.register_actor(registration).map_err(EngineError::Dispatch)
    }

    /// Deregisters a remote actor from the hub.
    #[cfg(feature = "actor")]
    pub fn deregister_actor(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
    ) -> Result<(), EngineError> {
        tracing::debug!(%actor_id, "deregister_actor");
        self.dispatch.deregister_actor(actor_id).map_err(EngineError::Dispatch)
    }

    /// Records an actor heartbeat.
    #[cfg(feature = "actor")]
    pub fn actor_heartbeat(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
    ) -> Result<(), EngineError> {
        tracing::trace!(%actor_id, "actor_heartbeat");
        self.dispatch.actor_heartbeat(actor_id).map_err(EngineError::Dispatch)
    }

    /// Returns a reference to the actor registry.
    #[cfg(feature = "actor")]
    pub fn actor_registry(&self) -> &actionqueue_actor::ActorRegistry {
        self.dispatch.actor_registry()
    }

    // ── Platform feature ───────────────────────────────────────────────────

    /// Creates a new organizational tenant.
    #[cfg(feature = "platform")]
    pub fn create_tenant(
        &mut self,
        registration: actionqueue_core::platform::TenantRegistration,
    ) -> Result<(), EngineError> {
        let tenant_id = registration.tenant_id();
        tracing::debug!(%tenant_id, "create_tenant");
        self.dispatch.create_tenant(registration).map_err(EngineError::Dispatch)
    }

    /// Assigns a role to an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn assign_role(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        role: actionqueue_core::platform::Role,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), EngineError> {
        tracing::debug!(%actor_id, ?role, %tenant_id, "assign_role");
        self.dispatch.assign_role(actor_id, role, tenant_id).map_err(EngineError::Dispatch)
    }

    /// Grants a capability to an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn grant_capability(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        capability: actionqueue_core::platform::Capability,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), EngineError> {
        tracing::debug!(%actor_id, ?capability, %tenant_id, "grant_capability");
        self.dispatch
            .grant_capability(actor_id, capability, tenant_id)
            .map_err(EngineError::Dispatch)
    }

    /// Revokes a capability from an actor within a tenant.
    #[cfg(feature = "platform")]
    pub fn revoke_capability(
        &mut self,
        actor_id: actionqueue_core::ids::ActorId,
        capability: actionqueue_core::platform::Capability,
        tenant_id: actionqueue_core::ids::TenantId,
    ) -> Result<(), EngineError> {
        tracing::debug!(%actor_id, ?capability, %tenant_id, "revoke_capability");
        self.dispatch
            .revoke_capability(actor_id, capability, tenant_id)
            .map_err(EngineError::Dispatch)
    }

    /// Appends an entry to the organizational ledger.
    #[cfg(feature = "platform")]
    pub fn append_ledger_entry(
        &mut self,
        entry: actionqueue_core::platform::LedgerEntry,
    ) -> Result<(), EngineError> {
        tracing::debug!(
            entry_id = %entry.entry_id(),
            tenant_id = %entry.tenant_id(),
            ledger_key = entry.ledger_key(),
            "append_ledger_entry"
        );
        self.dispatch.append_ledger_entry(entry).map_err(EngineError::Dispatch)
    }

    /// Returns a reference to the append ledger.
    #[cfg(feature = "platform")]
    pub fn ledger(&self) -> &actionqueue_platform::AppendLedger {
        self.dispatch.ledger()
    }

    /// Returns a reference to the RBAC enforcer.
    #[cfg(feature = "platform")]
    pub fn rbac(&self) -> &actionqueue_platform::RbacEnforcer {
        self.dispatch.rbac()
    }

    /// Returns a reference to the tenant registry.
    #[cfg(feature = "platform")]
    pub fn tenant_registry(&self) -> &actionqueue_platform::TenantRegistry {
        self.dispatch.tenant_registry()
    }

    /// Gracefully drains in-flight work and then shuts down.
    ///
    /// Stops promoting and dispatching new work, but continues processing
    /// in-flight results and heartbeating leases until all in-flight work
    /// completes or the timeout expires. The WAL writer is flushed and
    /// closed on drop of the underlying authority.
    pub async fn drain_and_shutdown(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<(), EngineError> {
        tracing::info!(timeout_secs = timeout.as_secs(), "drain_and_shutdown starting");
        let _ = self.dispatch.drain_until_idle(timeout).await?;
        tracing::info!("drain_and_shutdown complete");
        Ok(())
    }

    /// Shuts down the engine immediately, consuming it.
    ///
    /// In-flight workers will continue to completion in their tokio blocking tasks.
    /// Worker results for in-flight runs are lost; those runs will recover via
    /// lease expiry on the next bootstrap. The WAL writer is flushed and closed
    /// on drop of the underlying authority.
    ///
    /// For orderly drain of in-flight work, use `drain_and_shutdown()` instead.
    pub fn shutdown(self) -> Result<(), EngineError> {
        tracing::info!("engine shutdown");
        // Authority (and its WAL writer) are dropped here, triggering flush.
        Ok(())
    }
}
