//! Host-authenticated control boundary. Check current durable grants before any
//! target lookup or duplicate acknowledgement; caller/causal references are opaque.
pub use actionqueue_core::control::{ControlScope, HostControlContext, QueueAction};
use actionqueue_core::{mutation::*, time::clock::Clock};
use actionqueue_storage::{
    mutation::authority::*, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};

pub use actionqueue_core::control::ControlError;
pub use actionqueue_storage::mutation::control::{authorize, check_scope};
/// Host operations supported by the handler-independent control service.
pub enum ControlOperation {
    /// Durable task admission.
    AdmitTask(actionqueue_core::admission::EnsureTaskRequest),
    /// Durable signal admission.
    AdmitSignal(actionqueue_core::continuation::AdmitSignalRequest),
    /// Cancel a task or run.
    Cancel(CancelTarget),
    /// Resolve an identified wait.
    ResolveWait { run_id: actionqueue_core::ids::RunId, wait_id: actionqueue_core::ids::WaitId },
}
/// Successful control response.
#[derive(Debug)]
pub enum ControlOutcome {
    /// Admission identity, including exact retries.
    Task(actionqueue_core::admission::EnsureTaskOutcome),
    /// Signal identity, including exact retries.
    Signal(actionqueue_core::continuation::AdmitSignalOutcome),
    /// Applied mutation.
    Mutation(MutationOutcome),
}
/// Authenticates scope and authorizes before idempotency/target inspection.
pub fn execute_control<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    operation: ControlOperation,
    clock: &impl Clock,
) -> Result<ControlOutcome, ControlError> {
    let err = |e: String| ControlError::Mutation(e);
    match operation {
        ControlOperation::AdmitTask(request) => {
            let tenant = authorize(a, host, QueueAction::AdmitTask)?;
            check_scope(tenant, request.task_spec().tenant_id())?;
            let request = actionqueue_core::admission::EnsureTaskRequest::new(
                request.admission_key().clone(),
                request.task_spec().clone(),
                request.dependencies().to_vec(),
                request.causal_context().clone(),
                Some(host.attribution.clone()),
            )
            .map_err(|e| err(e.to_string()))?;
            crate::admission::ensure_task(a, request, clock)
                .map(ControlOutcome::Task)
                .map_err(|e| err(e.to_string()))
        }
        ControlOperation::AdmitSignal(request) => {
            let tenant = authorize(a, host, QueueAction::AdmitSignal)?;
            crate::signals::admit_signal(
                a,
                request,
                actionqueue_core::continuation::SignalIngressContext {
                    tenant_id: tenant,
                    control_context: Some(host.attribution.clone()),
                },
                clock,
            )
            .map(ControlOutcome::Signal)
            .map_err(|e| err(e.to_string()))
        }
        ControlOperation::Cancel(target) => {
            let action = match target {
                CancelTarget::Task(_) => QueueAction::CancelTask,
                CancelTarget::Run(_) => QueueAction::CancelRun,
            };
            let tenant = authorize(a, host, action)?;
            let task_id = match target {
                CancelTarget::Task(id) => id,
                CancelTarget::Run(id) => {
                    a.projection().get_run_instance(&id).ok_or(ControlError::NotFound)?.task_id()
                }
            };
            check_scope(
                tenant,
                a.projection().get_task(&task_id).ok_or(ControlError::NotFound)?.tenant_id(),
            )?;
            a.submit_command(
                MutationCommand::Cancel(CancelCommand {
                    expected_sequence: a.projection().latest_sequence().saturating_add(1),
                    target,
                    tenant_id: tenant,
                    control_context: Some(host.attribution.clone()),
                    timestamp: clock.now(),
                }),
                DurabilityPolicy::Immediate,
            )
            .map(ControlOutcome::Mutation)
            .map_err(|e| err(e.to_string()))
        }
        ControlOperation::ResolveWait { run_id, wait_id } => {
            let tenant = authorize(a, host, QueueAction::ResolveWait)?;
            let task =
                a.projection().get_run_instance(&run_id).ok_or(ControlError::NotFound)?.task_id();
            check_scope(
                tenant,
                a.projection().get_task(&task).ok_or(ControlError::NotFound)?.tenant_id(),
            )?;
            a.submit_command(
                MutationCommand::WaitResolve(WaitResolveCommand {
                    expected_sequence: a.projection().latest_sequence().saturating_add(1),
                    run_id,
                    wait_id,
                    tenant_id: tenant,
                    control_context: host.attribution.clone(),
                    timestamp: clock.now(),
                }),
                DurabilityPolicy::Immediate,
            )
            .map(ControlOutcome::Mutation)
            .map_err(|e| err(e.to_string()))
        }
    }
}

/// Executes administrative mutation commands with fresh authorization and
/// attribution in the same durable frame. Runtime/execution commands are not
/// accepted through this host-control service.
pub fn execute_mutation<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    command: MutationCommand,
) -> Result<MutationOutcome, ControlError> {
    use MutationCommand as M;
    use QueueAction as Q;
    let action = match &command {
        M::EnginePause(_) => Q::PauseEngine,
        M::EngineResume(_) => Q::ResumeEngine,
        M::RunSuspend(_) => Q::SuspendRun,
        M::RunResume(_) => Q::ResumeRun,
        M::ActorRegister(_) => Q::RegisterActor,
        M::ActorDeregister(_) => Q::DeregisterActor,
        M::ActorHeartbeat(_) => Q::HeartbeatActor,
        M::TenantCreate(_) => Q::ManageTenant,
        M::RoleAssign(_) | M::CapabilityGrant(_) | M::CapabilityRevoke(_) => Q::ManagePermission,
        M::BudgetAllocate(_) | M::BudgetReplenish(_) | M::BudgetConsume(_) => Q::ManageBudget,
        M::SubscriptionCreate(_) | M::SubscriptionCancel(_) => Q::ManageSubscription,
        M::LedgerAppend(_) => Q::AppendLedger,
        _ => return Err(ControlError::Unauthorized),
    };
    let tenant = authorize(a, host, action)?;
    let task_scope =
        |id| a.projection().get_task(&id).map(|t| t.tenant_id()).ok_or(ControlError::NotFound);
    let run_scope = |id| {
        a.projection()
            .get_run_instance(&id)
            .ok_or(ControlError::NotFound)
            .and_then(|r| task_scope(r.task_id()))
    };
    let actor_scope =
        |id| a.projection().get_actor(&id).map(|r| r.tenant_id).ok_or(ControlError::NotFound);
    let target = match &command {
        M::RunSuspend(c) => run_scope(c.run_id())?,
        M::RunResume(c) => run_scope(c.run_id())?,
        M::ActorRegister(c) => c.registration().tenant_id(),
        M::ActorHeartbeat(c) => {
            if host.actor_id != Some(c.actor_id()) {
                return Err(ControlError::Unauthorized);
            }
            actor_scope(c.actor_id())?
        }
        M::ActorDeregister(c) => actor_scope(c.actor_id())?,
        M::BudgetAllocate(c) => task_scope(c.task_id())?,
        M::BudgetReplenish(c) => task_scope(c.task_id())?,
        M::BudgetConsume(c) => task_scope(c.task_id())?,
        M::SubscriptionCreate(c) => task_scope(c.task_id())?,
        M::SubscriptionCancel(c) => task_scope(
            a.projection()
                .subscriptions()
                .find(|(id, _)| **id == c.subscription_id())
                .ok_or(ControlError::NotFound)?
                .1
                .task_id,
        )?,
        M::LedgerAppend(c) => Some(c.entry().tenant_id()),
        _ => tenant,
    };
    if !action.requires_store() {
        check_scope(tenant, target)?;
    }
    let command = if let M::RunSuspend(c) = command {
        let run = a.projection().get_run_instance(&c.run_id()).ok_or(ControlError::NotFound)?;
        let lease = a.projection().get_lease_metadata(&c.run_id()).ok_or(ControlError::NotFound)?;
        let attempt = run.current_attempt_id().ok_or(ControlError::NotFound)?;
        let reason = c
            .reason()
            .map(actionqueue_core::bounded::BoundedCode::new)
            .transpose()
            .map_err(|e| ControlError::Mutation(e.to_string()))?;
        M::AttemptDispositionCommit(AttemptDispositionCommitCommand::new(
            AttemptCommitExpectation::new(
                c.sequence(),
                c.run_id(),
                attempt,
                actionqueue_core::run::RunState::Running,
                LeaseFence::new(lease.owner().into(), lease.granted_at_sequence()),
            ),
            actionqueue_core::disposition::AttemptDisposition::suspended(None, reason),
            c.timestamp(),
        ))
    } else {
        command
    };
    a.submit_command(command.with_control(host), DurabilityPolicy::Immediate)
        .map_err(|e| ControlError::Mutation(e.to_string()))
}
