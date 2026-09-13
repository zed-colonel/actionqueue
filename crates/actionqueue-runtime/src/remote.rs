//! Authenticated remote execution over the same durable accepted-start and
//! disposition paths used by local workers. No remote lease enters local in-flight state.
use actionqueue_actor::protocol::*;
use actionqueue_core::{control::*, ids::*, mutation::*, run::RunState};
use actionqueue_storage::{
    mutation::authority::*, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};

use crate::control::{authorize, check_scope};

/// Accepted work and immutable execution input. Resume context comes from the
/// accepted start's durable assignment, including on response retransmission.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoteWork {
    /// Child state at response construction, sorted by task/run identity.
    pub children: Vec<RemoteChild>,
    pub protocol_version: u32,
    pub contract_revision: String,
    pub task_id: TaskId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub lease_fence: LeaseFence,
    pub lease_expiry: u64,
    pub payload: Vec<u8>,
    pub constraints: actionqueue_core::task::constraints::TaskConstraints,
    pub attempt_number: u32,
    pub failure_attempt_count: u32,
    pub tenant_id: Option<TenantId>,
    pub causal_context: actionqueue_core::causal::CausalContext,
    pub resume_context: Option<actionqueue_core::continuation::ResumeContext>,
}
/// Structural child context supplied to remote executors.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoteChild {
    pub task_id: TaskId,
    pub runs: Vec<(RunId, RunState)>,
    pub terminal_status: Option<actionqueue_core::continuation::TaskTerminalStatus>,
}
fn err(e: impl std::fmt::Display) -> ControlError {
    ControlError::Mutation(e.to_string())
}
fn actor<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    h: &HostControlContext,
    action: QueueAction,
) -> Result<(ActorId, Option<TenantId>, actionqueue_core::executor::ExecutorTraits), ControlError> {
    let tenant = authorize(a, h, action)?;
    let actor = h.actor_id.ok_or(ControlError::Unauthorized)?;
    let reg = a
        .projection()
        .get_actor(&actor)
        .filter(|r| r.deregistered_at.is_none())
        .ok_or(ControlError::Unauthorized)?;
    check_scope(tenant, reg.tenant_id)?;
    let traits = actionqueue_core::executor::ExecutorTraits::new(reg.executor_traits.clone())
        .map_err(err)?;
    Ok((actor, tenant, traits))
}
/// Deterministically ordered eligible work in exactly the authenticated namespace.
pub fn claimable<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    h: &HostControlContext,
    now: u64,
) -> Result<Vec<RunId>, ControlError> {
    let (_, tenant, traits) = actor(a, h, QueueAction::InspectClaimable)?;
    eligible_runs(a, tenant, &traits, now)
}
fn eligible_runs<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    tenant: Option<TenantId>,
    traits: &actionqueue_core::executor::ExecutorTraits,
    now: u64,
) -> Result<Vec<RunId>, ControlError> {
    use actionqueue_engine::selection::default_selector::{
        select_ready_runs, ReadyRunSelectionInput,
    };
    let inputs: Vec<_> = a
        .projection()
        .run_instances()
        .filter(|r| {
            a.projection().get_task(&r.task_id()).is_some_and(|t| t.tenant_id() == tenant)
                && crate::claim::eligible(a.projection(), r.id(), Some(traits), now)
        })
        .cloned()
        .map(ReadyRunSelectionInput::from_ready_run)
        .collect();
    Ok(select_ready_runs(&inputs).into_selected().into_iter().map(|r| r.id()).collect())
}
fn work<W: WalWriter>(
    a: &StorageMutationAuthority<W, ReplayReducer>,
    run_id: RunId,
    attempt_id: AttemptId,
) -> Result<RemoteWork, ControlError> {
    let run = a.projection().get_run_instance(&run_id).ok_or(ControlError::NotFound)?;
    let task = a.projection().get_task(&run.task_id()).ok_or(ControlError::NotFound)?;
    let lease = a.projection().get_lease_metadata(&run_id).ok_or(ControlError::NotFound)?;
    let start = a
        .projection()
        .get_attempt_history(&run_id)
        .into_iter()
        .flatten()
        .find(|r| r.attempt_id() == attempt_id)
        .and_then(|r| r.accepted_start())
        .ok_or(ControlError::NotFound)?;
    let mut children: Vec<_> = a
        .projection()
        .task_records()
        .filter(|r| r.task_spec().parent_task_id() == Some(task.id()))
        .map(|r| {
            let id = r.task_spec().id();
            let mut runs: Vec<_> =
                a.projection().runs_for_task(id).map(|r| (r.id(), r.state())).collect();
            runs.sort_by_key(|(id, _)| *id);
            RemoteChild {
                task_id: id,
                runs,
                terminal_status: a.projection().task_terminal_status(id),
            }
        })
        .collect();
    children.sort_by_key(|c| c.task_id);
    Ok(RemoteWork {
        children,
        protocol_version: PROTOCOL_VERSION,
        contract_revision: CONTRACT_REVISION.into(),
        task_id: task.id(),
        run_id,
        attempt_id,
        lease_fence: start.fence.clone(),
        lease_expiry: lease.expiry(),
        payload: task.payload().to_vec(),
        constraints: task.constraints().clone(),
        attempt_number: run.attempt_count(),
        failure_attempt_count: run.failure_attempt_count(),
        tenant_id: task.tenant_id(),
        causal_context: a
            .projection()
            .task_admission(task.id())
            .ok_or(ControlError::NotFound)?
            .request()
            .causal_context()
            .clone(),
        resume_context: a.projection().attempt_resume(run_id, attempt_id),
    })
}
/// Claim the first eligible run. Accepted identity retries append nothing. Fresh
/// attempt IDs cannot reuse any prior attempt anywhere in the store.
pub fn claim<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    h: &HostControlContext,
    request: RemoteClaim,
    now: u64,
    lease_timeout_secs: u64,
) -> Result<RemoteWork, ControlError> {
    let (id, tenant, traits) = actor(a, h, QueueAction::ClaimRun)?;
    if !supported(request.protocol_version, &request.contract_revision)
        || request.attempt_id.as_uuid().is_nil()
        || !(3..=86400).contains(&lease_timeout_secs)
    {
        return Err(err("invalid claim envelope"));
    }
    let run = a.projection().get_run_instance(&request.run_id).ok_or(ControlError::NotFound)?;
    check_scope(
        tenant,
        a.projection().get_task(&run.task_id()).ok_or(ControlError::NotFound)?.tenant_id(),
    )?;
    if run.current_attempt_id() == Some(request.attempt_id) {
        let work = work(a, request.run_id, request.attempt_id)?;
        if run.state() == RunState::Running
            && work.lease_fence.owner() == &lease_owner(id)
            && now < work.lease_expiry
        {
            return Ok(work);
        }
        return Err(err("stale claim"));
    }
    if a.projection().run_instances().any(|r| {
        a.projection()
            .get_attempt_history(&r.id())
            .into_iter()
            .flatten()
            .any(|a| a.attempt_id() == request.attempt_id)
    }) {
        return Err(err("attempt identity already used"));
    }
    if eligible_runs(a, tenant, &traits, now)?.first() != Some(&request.run_id) {
        return Err(err("run is not next eligible work"));
    }
    if run.state() == RunState::Scheduled {
        let _ = a
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    a.projection().latest_sequence().saturating_add(1),
                    request.run_id,
                    RunState::Scheduled,
                    RunState::Ready,
                    now,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(err)?;
    }
    crate::claim::accept(
        a,
        request.run_id,
        request.attempt_id,
        lease_owner(id).as_str(),
        now,
        now.saturating_add(lease_timeout_secs),
    )
    .map_err(err)?;
    work(a, request.run_id, request.attempt_id)
}
/// Renewal names the active attempt and accepted fence. Actor liveness heartbeats
/// never renew execution, and local worker heartbeats never renew these leases.
// Preserve the explicit dependencies of this existing boundary API.
#[allow(clippy::too_many_arguments)]
pub fn renew<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    h: &HostControlContext,
    run: RunId,
    attempt: AttemptId,
    fence: LeaseFence,
    now: u64,
    expiry: u64,
) -> Result<(), ControlError> {
    let (id, tenant, _) = actor(a, h, QueueAction::RenewLease)?;
    if fence.owner() != &lease_owner(id) || expiry <= now || expiry > now.saturating_add(86400) {
        return Err(err("invalid renewal"));
    }
    let r = a.projection().get_run_instance(&run).ok_or(ControlError::NotFound)?;
    check_scope(
        tenant,
        a.projection().get_task(&r.task_id()).ok_or(ControlError::NotFound)?.tenant_id(),
    )?;
    let expected = AttemptCommitExpectation::new(
        a.projection().latest_sequence().saturating_add(1),
        run,
        attempt,
        RunState::Running,
        fence.clone(),
    );
    a.projection()
        .validate_disposition_fence(&AttemptDispositionCommitCommand::new(
            expected,
            actionqueue_core::disposition::AttemptDisposition::complete(None),
            now,
        ))
        .map_err(err)?;
    let _ = a
        .submit_command(
            MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(
                a.projection().latest_sequence().saturating_add(1),
                run,
                fence.owner().as_str(),
                expiry,
                now,
            )),
            DurabilityPolicy::Immediate,
        )
        .map_err(err)?;
    Ok(())
}
/// Authorize and validate before effects or exact-retry acknowledgement. Storage
/// independently rechecks actor, scope, digest, permissions, and accepted fence.
pub fn submit_result<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    h: &HostControlContext,
    result: RemoteAttemptResult,
    now: u64,
) -> Result<(), ControlError> {
    actor(a, h, QueueAction::SubmitResult)?;
    let expected = AttemptCommitExpectation::new(
        a.projection().latest_sequence().saturating_add(1),
        result.run_id,
        result.attempt_id,
        RunState::Running,
        result.lease_fence,
    );
    crate::disposition::commit_remote(
        a,
        expected,
        result.disposition,
        now,
        RemoteResultExpectation {
            host: h.clone(),
            protocol_version: result.protocol_version,
            contract_revision: result.contract_revision,
            digest: result.disposition_digest,
        },
    )
    .map_err(err)
}

/// Configured limits for a daemon that serves remote workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RemotePolicy {
    pub max_concurrent: usize,
    pub lease_timeout_secs: u64,
    pub retry_delay_secs: u64,
}
impl Default for RemotePolicy {
    fn default() -> Self {
        Self { max_concurrent: 4, lease_timeout_secs: 300, retry_delay_secs: 5 }
    }
}
impl RemotePolicy {
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.max_concurrent == 0 || !(3..=86400).contains(&self.lease_timeout_secs) {
            return Err(err("invalid remote dispatch policy"));
        }
        Ok(())
    }
}
/// One serialized scheduler maintenance pass. The host calls this on its timer
/// and before remote ingress. All state comes from the durable projection.
pub fn maintain<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
    policy: RemotePolicy,
) -> Result<(), ControlError> {
    policy.validate()?;
    let mut expired: Vec<_> = a
        .projection()
        .actors()
        .map(|(_, r)| r)
        .filter(|r| {
            r.deregistered_at.is_none()
                && now
                    >= r.last_heartbeat_at
                        .unwrap_or(r.registered_at)
                        .saturating_add(r.heartbeat_interval_secs.saturating_mul(3))
        })
        .map(|r| r.actor_id)
        .collect();
    expired.sort_by_key(|id| *id.as_uuid());
    for actor_id in expired {
        let _ = a
            .submit_command(
                MutationCommand::RecoveryControl(Box::new(MutationCommand::ActorDeregister(
                    ActorDeregisterCommand::new(
                        a.projection().latest_sequence().saturating_add(1),
                        actor_id,
                        now,
                    ),
                ))),
                DurabilityPolicy::Immediate,
            )
            .map_err(err)?;
    }
    crate::reactivity::reconcile(a, now).map_err(err)?;
    crate::waits::recover_expired_execution(a, now).map_err(err)?;
    crate::waits::reconcile(a, now).map_err(err)?;
    crate::reactivity::reconcile(a, now).map_err(err)?;
    let retry: Vec<_> = a
        .projection()
        .run_instances()
        .filter(|r| r.state() == RunState::RetryWait)
        .cloned()
        .collect();
    let backoff = actionqueue_executor_local::FixedBackoff::new(std::time::Duration::from_secs(
        policy.retry_delay_secs,
    ));
    let promotion = actionqueue_engine::scheduler::retry_promotion::promote_retry_wait_to_ready(
        &retry, now, &backoff,
    )
    .map_err(err)?;
    for run in promotion.promoted() {
        let _ = a
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    a.projection().latest_sequence().saturating_add(1),
                    run.id(),
                    RunState::RetryWait,
                    RunState::Ready,
                    now,
                )),
                DurabilityPolicy::Immediate,
            )
            .map_err(err)?;
    }
    #[cfg(feature = "workflow")]
    crate::cron::replenish(a, &mut Default::default(), now).map_err(err)?;
    Ok(())
}
/// Capacity and lease policy applied under the same exclusive owner as acceptance.
pub fn claim_with_policy<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    host: &HostControlContext,
    request: RemoteClaim,
    now: u64,
    policy: RemotePolicy,
) -> Result<RemoteWork, ControlError> {
    actor(a, host, QueueAction::ClaimRun)?;
    policy.validate()?;
    let retry = a
        .projection()
        .get_run_instance(&request.run_id)
        .is_some_and(|r| r.current_attempt_id() == Some(request.attempt_id));
    if !retry
        && a.projection()
            .run_instances()
            .filter(|r| matches!(r.state(), RunState::Leased | RunState::Running))
            .count()
            >= policy.max_concurrent
    {
        return Err(err("dispatch capacity unavailable"));
    }
    claim(a, host, request, now, policy.lease_timeout_secs)
}
