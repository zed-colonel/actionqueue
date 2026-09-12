//! Shared authoritative eligibility and durable accepted-start sequence.
use actionqueue_core::{
    executor::ExecutorTraits,
    ids::{AttemptId, RunId},
    mutation::*,
    run::RunState,
};
use actionqueue_storage::{
    mutation::authority::*,
    recovery::reducer::{ReplayReducer, ReplayReducerError},
    wal::writer::WalWriter,
};
type Error = MutationAuthorityError<ReplayReducerError>;
/// Pure scheduling gates shared by local and remote claims. Namespace checks are
/// a separate authenticated boundary; traits confer no authority.
pub fn eligible(p: &ReplayReducer, run: RunId, traits: Option<&ExecutorTraits>, now: u64) -> bool {
    let Some(r) = p.get_run_instance(&run) else {
        return false;
    };
    let Some(task) = p.get_task(&r.task_id()) else {
        return false;
    };
    if p.is_engine_paused()
        || p.is_task_canceled(r.task_id())
        || !matches!(r.state(), RunState::Ready | RunState::Scheduled)
        || (r.state() == RunState::Scheduled && r.scheduled_at() > now)
        || !actionqueue_core::executor::matches_requirements(
            traits,
            task.constraints().required_executor_traits(),
        )
        || p.dependency_declarations().any(|(id, deps)| {
            id == r.task_id()
                && deps.iter().any(|id| {
                    p.task_terminal_status(*id)
                        != Some(actionqueue_core::continuation::TaskTerminalStatus::Succeeded)
                })
        })
        || p.budgets().any(|((id, _), b)| *id == r.task_id() && b.exhausted)
    {
        return false;
    }
    if let Some(key) = task.constraints().concurrency_key() {
        if p.key_reservations().any(|(holder, held)| holder != run && held == key) {
            return false;
        }
    }
    true
}
fn next<W: WalWriter>(a: &StorageMutationAuthority<W, ReplayReducer>) -> Result<u64, Error> {
    a.projection()
        .latest_sequence()
        .checked_add(1)
        .ok_or(Error::Validation(MutationValidationError::SequenceOverflow))
}
/// Caller holds exclusive authority ownership across all four records. Returning
/// work is permitted only after the accepted start has synced durably. Recovery
/// closes every partial sequence through the existing execution recovery path.
pub fn accept<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    run_id: RunId,
    attempt_id: AttemptId,
    owner: &str,
    now: u64,
    expiry: u64,
) -> Result<LeaseFence, Error> {
    for (from, to) in [(RunState::Ready, RunState::Leased)] {
        let _ = a.submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                next(a)?,
                run_id,
                from,
                to,
                now,
            )),
            DurabilityPolicy::Immediate,
        )?;
    }
    let _ = a.submit_command(
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
            next(a)?,
            run_id,
            owner,
            expiry,
            now,
        )),
        DurabilityPolicy::Immediate,
    )?;
    let _ = a.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            next(a)?,
            run_id,
            RunState::Leased,
            RunState::Running,
            now,
        )),
        DurabilityPolicy::Immediate,
    )?;
    let lease = a.projection().get_lease_metadata(&run_id).expect("lease accepted");
    let fence = LeaseFence::new(lease.owner().into(), lease.granted_at_sequence());
    let resume = a.projection().pending_resume(run_id).map(|c| c.context_id);
    let result = a.submit_command(
        MutationCommand::AttemptStart(AttemptStartCommand::new(
            next(a)?,
            run_id,
            attempt_id,
            now,
            fence.clone(),
            resume,
        )),
        DurabilityPolicy::Immediate,
    )?;
    if !matches!(result.applied(), AppliedMutation::AttemptStart { .. }) {
        return Err(Error::Disposition(
            actionqueue_storage::mutation::disposition::DispositionRejection::Stale,
        ));
    }
    Ok(fence)
}
