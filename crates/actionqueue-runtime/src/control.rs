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
    /// Cancel an identified wait and its owning run.
    CancelWait { run_id: actionqueue_core::ids::RunId, wait_id: actionqueue_core::ids::WaitId },
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
    clock: &(impl Clock + ?Sized),
) -> Result<ControlOutcome, ServiceError> {
    a.with_control_context(host, |a| execute_bound_control(a, host, operation, clock))
}
fn execute_bound_control<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    operation: ControlOperation,
    clock: &(impl Clock + ?Sized),
) -> Result<ControlOutcome, ServiceError> {
    let err = |e| ServiceError::Storage(e);
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
            .map_err(|e| ServiceError::Admission(crate::admission::AdmissionError::Rejected(e)))?;
            crate::admission::ensure_task(a, request, clock)
                .map(ControlOutcome::Task)
                .map_err(ServiceError::Admission)
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
            .map_err(ServiceError::Signal)
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
            .map_err(err)
        }
        ControlOperation::CancelWait { run_id, wait_id } => {
            let tenant = authorize(a, host, QueueAction::CancelWait)?;
            let c = MutationCommand::WaitCancel(
                WaitCancelCommand::new(
                    a.projection().latest_sequence().saturating_add(1),
                    run_id,
                    wait_id,
                    host.attribution.clone(),
                    clock.now(),
                )
                .with_tenant(tenant),
            );
            execute_mutation(a, host, c)
                .map(ControlOutcome::Mutation)
                .map_err(ServiceError::Authorization)
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
            .map_err(err)
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
    a.submit_command(command.with_control(host), DurabilityPolicy::Immediate)
        .map_err(|e| ControlError::Mutation(e.to_string()))
}

/// Inspect one wait within the authenticated namespace, using current grants.
pub fn inspect_wait<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    id: actionqueue_core::ids::WaitId,
) -> Result<actionqueue_storage::mutation::wait::WaitRecord, ControlError> {
    let tenant = authorize(a, host, QueueAction::InspectWait)?;
    let wait = a.projection().waits().get(id).ok_or(ControlError::NotFound)?;
    let run = a.projection().get_run_instance(&wait.run_id).ok_or(ControlError::NotFound)?;
    check_scope(
        tenant,
        a.projection().get_task(&run.task_id()).ok_or(ControlError::NotFound)?.tenant_id(),
    )?;
    Ok(wait.clone())
}
/// Inspect one signal; an absent tenant is never a wildcard.
pub fn inspect_signal<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    id: &actionqueue_core::ids::SignalId,
) -> Result<actionqueue_storage::mutation::signal::SignalRecord, ControlError> {
    let tenant = authorize(a, host, QueueAction::InspectSignal)?;
    a.projection().signals().get_signal(tenant, id).cloned().ok_or(ControlError::NotFound)
}

/// Typed public service failures. Display is deliberately independent of request data.
#[derive(Debug)]
pub enum ServiceError {
    /// Host authentication, grant or namespace failure.
    Authorization(ControlError),
    /// Admission rejection or storage failure.
    Admission(crate::admission::AdmissionError),
    /// Signal rejection, storage failure or committed identity requiring recovery.
    Signal(crate::signals::SignalAdmissionError),
    /// Uncertain storage mutation.
    Storage(MutationAuthorityError<actionqueue_storage::recovery::reducer::ReplayReducerError>),
}
impl From<ControlError> for ServiceError {
    fn from(error: ControlError) -> Self {
        Self::Authorization(error)
    }
}
impl ServiceError {
    /// Stable redacted classification, shared by transports.
    pub fn code(&self) -> &'static str {
        use crate::{admission::AdmissionError as A, signals::SignalAdmissionError as S};
        match self {
            Self::Authorization(ControlError::Unauthorized) => "forbidden",
            Self::Admission(A::Storage(MutationAuthorityError::Control(
                ControlError::Unauthorized,
            )))
            | Self::Signal(S::Storage(MutationAuthorityError::Control(
                ControlError::Unauthorized,
            )))
            | Self::Storage(MutationAuthorityError::Control(ControlError::Unauthorized)) => {
                "forbidden"
            }
            Self::Storage(MutationAuthorityError::Wait(
                actionqueue_core::mutation::WaitRejection::WaitAlreadyResolved,
            )) => "conflict",
            Self::Storage(
                MutationAuthorityError::Wait(_) | MutationAuthorityError::Validation(_),
            ) => "invalid_request",
            Self::Authorization(ControlError::Scope | ControlError::NotFound) => "not_found",
            Self::Admission(A::Rejected(
                actionqueue_core::admission::AdmissionRejection::Conflict { .. },
            ))
            | Self::Signal(S::Rejected(
                actionqueue_core::continuation::SignalRejection::Conflict,
            )) => "conflict",
            Self::Signal(S::Rejected(
                actionqueue_core::continuation::SignalRejection::Capacity,
            )) => "backpressure",
            Self::Admission(A::Rejected(_) | A::Derivation(_)) | Self::Signal(S::Rejected(_)) => {
                "invalid_request"
            }
            Self::Signal(S::Matching { .. }) => "signal_committed_recovery_required",
            _ => "storage_unavailable",
        }
    }
}
impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for ServiceError {}
