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
fn authorize_projection(
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
