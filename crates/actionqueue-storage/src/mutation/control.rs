//! Shared authorization against the authoritative projection.
use super::authority::StorageMutationAuthority;
use crate::{recovery::reducer::ReplayReducer, wal::writer::WalWriter};
use actionqueue_core::{control::*, ids::TenantId};
/// Enforces current permissions without inspecting the operation's target.
pub fn authorize<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    action: QueueAction,
) -> Result<Option<TenantId>, ControlError> {
    let platform = a
        .store_session()
        .map(|s| s.manifest().features.iter().any(|f| f == "platform"))
        .unwrap_or(false);
    authorize_projection(a.projection(), platform, host, action)
}
/// Authorizes inspection against the same projection used to construct a response.
pub fn authorize_projection(
    p: &ReplayReducer,
    platform: bool,
    host: &HostControlContext,
    action: QueueAction,
) -> Result<Option<TenantId>, ControlError> {
    if action.requires_store() {
        return if host.scope == ControlScope::Store {
            Ok(None)
        } else {
            Err(ControlError::Unauthorized)
        };
    }
    if let ControlScope::ProvisionTenant(tenant) = host.scope {
        return if platform
            && action == QueueAction::RegisterActor
            && p.get_tenant(&tenant).is_some()
        {
            Ok(Some(tenant))
        } else {
            Err(ControlError::Unauthorized)
        };
    }
    match (platform, host.scope) {
        (false, ControlScope::SingleTenant) => Ok(None),
        (true, ControlScope::Tenant(tenant)) => {
            let actor = host.actor_id.ok_or(ControlError::Unauthorized)?;
            if !p.is_actor_active(actor)
                || p.get_actor(&actor).is_none_or(|r| r.tenant_id != Some(tenant))
                || p.get_role_assignment(actor, tenant).is_none()
                || !p.capability_grants().any(|g| {
                    g.actor_id == actor
                        && g.tenant_id == tenant
                        && g.revoked_at.is_none()
                        && g.capability == action.permission()
                })
            {
                return Err(ControlError::Unauthorized);
            }
            Ok(Some(tenant))
        }
        _ => Err(ControlError::Scope),
    }
}
/// Validates exact namespace equality. Absence is never a wildcard.
pub fn check_scope(
    expected: Option<TenantId>,
    actual: Option<TenantId>,
) -> Result<(), ControlError> {
    if expected == actual {
        Ok(())
    } else {
        Err(ControlError::Scope)
    }
}

/// Rechecks a remote proposal at the append boundary. An accepted disposition
/// already contains its complete immutable result identity, so exact retries
/// need no separate audit or deduplication record.
pub fn validate_remote<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    c: &actionqueue_core::mutation::AttemptDispositionCommitCommand,
) -> Result<Option<u64>, ControlError> {
    let platform = a
        .store_session()
        .map(|s| s.manifest().features.iter().any(|f| f == "platform"))
        .unwrap_or(false);
    validate_remote_projection(a.projection(), platform, c)
}
pub(crate) fn validate_remote_projection(
    p: &ReplayReducer,
    platform: bool,
    c: &actionqueue_core::mutation::AttemptDispositionCommitCommand,
) -> Result<Option<u64>, ControlError> {
    use actionqueue_core::{control::QueueAction as Q, disposition_digest::disposition_digest};
    let remote = c.remote().ok_or(ControlError::Unauthorized)?;
    let tenant = authorize_projection(p, platform, &remote.host, Q::SubmitResult)?;
    let actor = remote.host.actor_id.ok_or(ControlError::Unauthorized)?;
    let registered = p
        .get_actor(&actor)
        .filter(|a| a.deregistered_at.is_none())
        .ok_or(ControlError::Unauthorized)?;
    check_scope(tenant, registered.tenant_id)?;
    if remote.protocol_version != 1 || remote.contract_revision != "AQ-CONT-1-r2" {
        return Err(ControlError::Mutation("unsupported remote revision".into()));
    }
    if c.expected_lease().owner().as_str() != format!("actor:{actor}")
        || remote.digest != disposition_digest(c.disposition())
    {
        return Err(ControlError::Mutation("remote identity or digest mismatch".into()));
    }
    let run = p.get_run_instance(&c.run_id()).ok_or(ControlError::NotFound)?;
    check_scope(tenant, p.get_task(&run.task_id()).ok_or(ControlError::NotFound)?.tenant_id())?;
    if !c.disposition().child_admissions().is_empty() {
        authorize_projection(p, platform, &remote.host, Q::AdmitTask)?;
    }
    if !c.disposition().emitted_signals().is_empty() {
        authorize_projection(p, platform, &remote.host, Q::AdmitSignal)?;
    }
    if let Some(record) = p
        .get_attempt_history(&c.run_id())
        .into_iter()
        .flatten()
        .find(|r| r.attempt_id() == c.attempt_id())
        .and_then(|r| r.disposition.as_ref())
    {
        if record.fence == *c.expected_lease()
            && disposition_digest(&record.disposition) == remote.digest
        {
            return Ok(Some(record.sequence));
        }
        return Err(ControlError::Mutation("conflicting result retry".into()));
    }
    Ok(None)
}

/// Complete control classification; execution commands have their own structural
/// attempt, lease, scheduler or recovery preconditions.
pub fn action(command: &actionqueue_core::mutation::MutationCommand) -> Option<QueueAction> {
    use actionqueue_core::mutation::{CancelTarget, MutationCommand as M};
    use QueueAction as Q;
    Some(match command {
        M::AdmissionCommit(_) | M::TaskCreate(_) | M::DependencyDeclare(_) => Q::AdmitTask,
        M::SignalAdmit(_) => Q::AdmitSignal,
        M::Cancel(c) => match c.target {
            CancelTarget::Task(_) => Q::CancelTask,
            CancelTarget::Run(_) => Q::CancelRun,
        },
        M::TaskCancel(_) => Q::CancelTask,
        M::WaitResolve(_) => Q::ResolveWait,
        M::WaitCancel(_) => Q::CancelWait,
        M::SignalPin(_) | M::SignalUnpin(_) | M::RetireSignals(_) => Q::RetainSignal,
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
        _ => return None,
    })
}
/// Rechecks authorization, target namespace and embedded attribution before any
/// duplicate response or append. Administrative suspension becomes a fenced
/// disposition here, so direct callers cannot leave a live accepted attempt.
pub(crate) fn prepare_control(
    p: &ReplayReducer,
    platform: bool,
    host: &HostControlContext,
    mut command: actionqueue_core::mutation::MutationCommand,
) -> Result<actionqueue_core::mutation::MutationCommand, ControlError> {
    use actionqueue_core::mutation::*;
    use MutationCommand as M;
    let action = action(&command).ok_or(ControlError::Unauthorized)?;
    let tenant = authorize_projection(p, platform, host, action)?;
    let task_scope = |id| p.get_task(&id).map(|t| t.tenant_id()).ok_or(ControlError::NotFound);
    let run_scope = |id| {
        p.get_run_instance(&id).ok_or(ControlError::NotFound).and_then(|r| task_scope(r.task_id()))
    };
    let actor_scope = |id| p.get_actor(&id).map(|r| r.tenant_id).ok_or(ControlError::NotFound);
    let context = |actual: Option<&actionqueue_core::causal::ControlMutationContext>| {
        if actual.is_some_and(|a| a != &host.attribution) {
            Err(ControlError::Unauthorized)
        } else {
            Ok(())
        }
    };
    let target = match &mut command {
        M::AdmissionCommit(c) => {
            context(c.control_context())?;
            let tenant = c.plan().task_spec().tenant_id();
            *c = AdmissionCommitCommand::new(
                c.expected_sequence(),
                c.plan().clone(),
                Some(host.attribution.clone()),
                c.timestamp(),
            );
            tenant
        }
        M::SignalAdmit(c) => {
            context(c.envelope().control_context.as_ref())?;
            let mut envelope = c.envelope().clone();
            envelope.control_context = Some(host.attribution.clone());
            let tenant = envelope.tenant_id;
            *c = SignalAdmitCommand::new(c.expected_sequence(), envelope);
            tenant
        }
        M::Cancel(c) => {
            context(c.control_context.as_ref())?;
            check_scope(tenant, c.tenant_id)?;
            c.control_context = Some(host.attribution.clone());
            match c.target {
                CancelTarget::Task(id) => task_scope(id)?,
                CancelTarget::Run(id) => run_scope(id)?,
            }
        }
        M::WaitResolve(c) => {
            context(Some(&c.control_context))?;
            check_scope(tenant, c.tenant_id)?;
            run_scope(c.run_id)?
        }
        M::WaitCancel(c) => {
            context(Some(c.control_context()))?;
            check_scope(tenant, c.tenant_id())?;
            run_scope(c.run_id())?
        }
        M::SignalPin(c) | M::SignalUnpin(c) => {
            context(c.control_context.as_ref())?;
            c.control_context = Some(host.attribution.clone());
            c.tenant_id
        }
        M::RetireSignals(c) => {
            context(c.control_context.as_ref())?;
            c.control_context = Some(host.attribution.clone());
            c.tenant_id
        }
        M::TaskCreate(c) => c.task_spec().tenant_id(),
        M::TaskCancel(c) => task_scope(c.task_id())?,
        M::DependencyDeclare(c) => {
            for id in c.depends_on() {
                check_scope(tenant, task_scope(*id)?)?;
            }
            task_scope(c.task_id())?
        }
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
        M::SubscriptionCreate(c) => {
            use actionqueue_core::subscription::EventFilter;
            let (EventFilter::TaskCompleted { task_id }
            | EventFilter::RunStateChanged { task_id, .. }
            | EventFilter::BudgetThreshold { task_id, .. }) = c.filter();
            check_scope(tenant, task_scope(*task_id)?)?;
            task_scope(c.task_id())?
        }
        M::SubscriptionCancel(c) => task_scope(
            p.subscriptions()
                .find(|(id, _)| **id == c.subscription_id())
                .ok_or(ControlError::NotFound)?
                .1
                .task_id,
        )?,
        M::LedgerAppend(c) => {
            if c.entry().actor_id().is_some() && c.entry().actor_id() != host.actor_id {
                return Err(ControlError::Unauthorized);
            }
            Some(c.entry().tenant_id())
        }
        _ => tenant,
    };
    if !action.requires_store() {
        check_scope(tenant, target)?;
    }
    if let M::RunSuspend(c) = command {
        let run = p.get_run_instance(&c.run_id()).ok_or(ControlError::NotFound)?;
        let lease = p.get_lease_metadata(&c.run_id()).ok_or(ControlError::NotFound)?;
        let reason = c
            .reason()
            .map(actionqueue_core::bounded::BoundedCode::new)
            .transpose()
            .map_err(|e| ControlError::Mutation(e.to_string()))?;
        return Ok(M::AttemptDispositionCommit(AttemptDispositionCommitCommand::new(
            AttemptCommitExpectation::new(
                c.sequence(),
                c.run_id(),
                run.current_attempt_id().ok_or(ControlError::NotFound)?,
                actionqueue_core::run::RunState::Running,
                LeaseFence::new(lease.owner().into(), lease.granted_at_sequence()),
            ),
            actionqueue_core::disposition::AttemptDisposition::suspended(None, reason),
            c.timestamp(),
        )));
    }
    Ok(command)
}
/// Only recovery-derived controls may omit host attribution. Their durable
/// antecedents are checked here, independently of the caller's claimed origin.
pub(crate) fn validate_recovery(
    p: &ReplayReducer,
    c: &actionqueue_core::mutation::MutationCommand,
) -> Result<(), ControlError> {
    use actionqueue_core::mutation::{CancelTarget, MutationCommand as M};
    let valid = match c {
        M::ActorDeregister(c) => p.get_actor(&c.actor_id()).is_some_and(|a| a.deregistered_at.is_none()
            && c.timestamp() >= a.last_heartbeat_at.unwrap_or(a.registered_at).saturating_add(a.heartbeat_interval_secs.saturating_mul(3))),
        M::Cancel(c) if c.control_context.is_none() => match c.target {
            CancelTarget::Task(id) => p.get_task(&id).is_some_and(|t| t.tenant_id() == c.tenant_id && (
                p.is_task_canceled(id) || (t.child_lifecycle_policy() == actionqueue_core::task::task_spec::ChildLifecyclePolicy::Required && t.parent_task_id().is_some_and(|parent| p.is_task_canceled(parent)))
                || p.dependency_declarations().any(|(task,deps)| task == id && deps.iter().any(|d| p.task_terminal_status(*d).is_some_and(|s| s != actionqueue_core::continuation::TaskTerminalStatus::Succeeded)))
            )),
            _ => false,
        },
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ControlError::Unauthorized)
    }
}
