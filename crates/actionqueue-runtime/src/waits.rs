//! Handler-independent continuation service. AQ-07 owns accepted-start delivery.
use actionqueue_core::{mutation::*, run::RunState};
use actionqueue_storage::{
    mutation::{MutationAuthorityError, StorageMutationAuthority},
    recovery::reducer::{ReplayReducer, ReplayReducerError},
    wal::writer::WalWriter,
};
pub type WaitError = MutationAuthorityError<ReplayReducerError>;
pub const MATCH_BATCH: usize = 128;
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reconciliation {
    pub resolved: usize,
    pub remaining: bool,
}
fn next<W: WalWriter>(a: &StorageMutationAuthority<W, ReplayReducer>) -> Result<u64, WaitError> {
    if a.recovery_required() {
        return Err(WaitError::RecoveryRequired);
    }
    a.projection()
        .latest_sequence()
        .checked_add(1)
        .ok_or(WaitError::Wait(WaitRejection::StaleSequence))
}
/// One bounded batch, matching retained facts before deadlines. Persistent indexes retain
/// additional work; notifications are optional and a tick always checks the indexes.
pub fn reconcile_batch<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
    limit: usize,
) -> Result<Reconciliation, WaitError> {
    next(a)?;
    // A control may have committed just before a crash or between service calls.
    // Descendants and dependents must be canceled before any wait can advance.
    recover_cancellations(a, now)?;
    let limit = limit.clamp(1, MATCH_BATCH);
    let mut n = 0;
    for (signal, id) in a.projection().waits().matches(limit) {
        let run = a.projection().waits().get(id).expect("indexed").run_id;
        let _ = a.submit_command(
            actionqueue_engine::continuation::satisfy(next(a)?, run, id, signal, now),
            DurabilityPolicy::Immediate,
        )?;
        n += 1;
    }
    if a.projection().waits().matches(1).is_empty() {
        for id in a.projection().waits().due(now, limit - n) {
            let wait = a.projection().waits().get(id).expect("indexed");
            let run = wait.run_id;
            // An earlier terminal timeout in this batch may have canceled this wait.
            if wait.resolution.is_some() {
                continue;
            }
            let _ = a.submit_command(
                actionqueue_engine::continuation::timeout(next(a)?, run, id, now),
                DurabilityPolicy::Immediate,
            )?;
            n += 1;
            if a.projection().get_run_state(&run).is_some_and(|state| state.is_terminal()) {
                // Complete ordinary failure cascades before processing another deadline
                // or allowing the caller to open dispatch (including bootstrap).
                recover_cancellations(a, now)?;
            }
        }
    }
    Ok(Reconciliation {
        resolved: n,
        remaining: !a.projection().waits().matches(1).is_empty()
            || !a.projection().waits().due(now, 1).is_empty(),
    })
}
/// Bootstrap and explicit service calls drain every page before returning.
pub fn reconcile<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
) -> Result<usize, WaitError> {
    let mut count = 0;
    loop {
        let batch = reconcile_batch(a, now, MATCH_BATCH)?;
        count += batch.resolved;
        if !batch.remaining {
            return Ok(count);
        }
    }
}
/// Establishment acknowledgement is independent of subsequent reconciliation failures.
/// A caller may inspect/retry this operation, then call `reconcile`; no wake input is consumed.
pub fn establish<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    command: WaitEstablishCommand,
) -> Result<WaitOutcome, WaitError> {
    match a
        .submit_command(MutationCommand::WaitEstablish(command), DurabilityPolicy::Immediate)?
        .applied()
    {
        AppliedMutation::Wait(o) => Ok(*o),
        _ => unreachable!(),
    }
}
/// Finish ordinary interrupted execution before continuation reconciliation. Every prefix
/// remains recoverable; an absent wait establishment never implies a yielded attempt.
pub fn recover_execution<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
) -> Result<(), WaitError> {
    let mut ids: Vec<_> = a
        .projection()
        .run_instances()
        .filter(|r| matches!(r.state(), RunState::Running | RunState::Leased))
        .map(|r| r.id())
        .collect();
    ids.sort();
    for id in ids {
        recover_run(a, id, now)?;
    }
    Ok(())
}
/// Live expiry recovery is selected from durable ownership, never from a worker result.
/// The mutation owner runs this before heartbeats and dispatch. Bootstrap still recovers
/// all interrupted executions, including those whose leases have not yet elapsed.
pub(crate) fn recover_expired_execution<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
) -> Result<Vec<(actionqueue_core::ids::RunId, Option<actionqueue_core::ids::AttemptId>)>, WaitError>
{
    next(a)?;
    let mut expired: Vec<_> = a
        .projection()
        .run_instances()
        .filter(|run| matches!(run.state(), RunState::Running | RunState::Leased))
        .filter(|run| {
            a.projection().get_lease_metadata(&run.id()).is_some_and(|lease| now >= lease.expiry())
        })
        .map(|run| (run.id(), run.current_attempt_id()))
        .collect();
    expired.sort_by_key(|(run_id, _)| *run_id);
    for (id, _) in &expired {
        recover_run(a, *id, now)?;
    }
    Ok(expired)
}

// Each durable prefix uses the same restart-safe recovery path as bootstrap. Finishing
// the accepted attempt accounts one failure; releasing its lease and transitioning the
// run then applies the retry cap and concurrency reservation policy.
fn recover_run<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    id: actionqueue_core::ids::RunId,
    now: u64,
) -> Result<(), WaitError> {
    let state = *a.projection().get_run_state(&id).unwrap();
    if state == RunState::Running {
        if let Some(attempt) = a.projection().get_run_instance(&id).unwrap().current_attempt_id() {
            let _ = a.submit_command(
                MutationCommand::AttemptFinish(
                    AttemptFinishCommand::new(
                        next(a)?,
                        id,
                        attempt,
                        AttemptOutcome::failure(crate::config::EXECUTOR_INTERRUPTED),
                        now,
                    )
                    .with_recovery_origin(),
                ),
                DurabilityPolicy::Immediate,
            )?;
        }
    }
    if let Some((owner, expiry)) = a.projection().get_lease(&id).cloned() {
        let _ = a.submit_command(
            MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
                next(a)?,
                id,
                owner,
                expiry,
                now,
            )),
            DurabilityPolicy::Immediate,
        )?;
    }
    let target = if state == RunState::Leased {
        RunState::Ready
    } else if !a.projection().dispatch_has_started(id) {
        RunState::RetryWait
    } else {
        let r = a.projection().get_run_instance(&id).unwrap();
        let last =
            a.projection().get_attempt_history(&id).and_then(|h| h.last()).and_then(|a| a.result());
        if last == Some(AttemptResultKind::Success) {
            RunState::Completed
        } else if last == Some(AttemptResultKind::Suspended) {
            RunState::Suspended
        } else {
            let failures = r.failure_attempt_count() as usize;
            if failures
                < a.projection().get_task(&r.task_id()).unwrap().constraints().max_attempts()
                    as usize
            {
                RunState::RetryWait
            } else {
                RunState::Failed
            }
        }
    };
    let state = *a.projection().get_run_state(&id).unwrap();
    if state == target {
        return Ok(());
    }
    let _ = a.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            next(a)?,
            id,
            state,
            target,
            now,
        )),
        DurabilityPolicy::Immediate,
    )?;
    Ok(())
}
/// Complete legacy partial task controls and descendant/dependency cascades before matching.
/// Each task boundary is atomic; restart repeats the remaining tasks without changing winners.
pub fn recover_cancellations<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
) -> Result<(), WaitError> {
    loop {
        let mut targets = std::collections::HashSet::new();
        for task in a.projection().task_records() {
            let id = task.task_spec().id();
            let unfinished = a.projection().runs_for_task(id).any(|r| !r.state().is_terminal());
            if a.projection().is_task_canceled(id) && unfinished {
                targets.insert(id);
            }
            if !a.projection().is_task_canceled(id)
                && task
                    .task_spec()
                    .parent_task_id()
                    .is_some_and(|p| a.projection().is_task_canceled(p))
            {
                targets.insert(id);
            }
        }
        for (task, deps) in a.projection().dependency_declarations() {
            if a.projection().is_task_canceled(task) {
                continue;
            }
            let failed = deps.iter().any(|dep| {
                let runs: Vec<_> = a.projection().runs_for_task(*dep).collect();
                !runs.is_empty()
                    && runs.iter().all(|r| r.state().is_terminal())
                    && runs.iter().all(|r| r.state() != RunState::Completed)
            });
            if failed {
                targets.insert(task);
            }
        }
        if targets.is_empty() {
            return Ok(());
        }
        let mut targets: Vec<_> = targets.into_iter().collect();
        targets.sort_by_key(|id| *id.as_uuid());
        for task in targets {
            let tenant_id = a.projection().get_task(&task).expect("indexed task").tenant_id();
            let _ = a.submit_command(
                MutationCommand::Cancel(CancelCommand {
                    expected_sequence: next(a)?,
                    target: CancelTarget::Task(task),
                    tenant_id,
                    control_context: None,
                    timestamp: now,
                }),
                DurabilityPolicy::Immediate,
            )?;
        }
    }
}
