//! Shared rolling cron window maintenance under the exclusive mutation owner.
use crate::dispatch::DispatchError;
use actionqueue_core::{
    ids::TaskId,
    mutation::{DurabilityPolicy, MutationAuthority, MutationCommand, RunCreateCommand},
    run::run_instance::RunInstance,
};
use actionqueue_storage::{
    mutation::authority::StorageMutationAuthority, recovery::reducer::ReplayReducer,
    wal::writer::WalWriter,
};
pub(crate) fn replenish<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    cache: &mut actionqueue_engine::derive::cron::CronScheduleCache,
    current_time: u64,
) -> Result<(), DispatchError> {
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_engine::derive::cron::{derive_cron_cached, CRON_WINDOW_SIZE};

    // Collect cron task IDs and their policies without holding the projection borrow.
    let mut cron_tasks: Vec<(TaskId, actionqueue_core::task::run_policy::CronPolicy)> = authority
        .projection()
        .task_records()
        .filter_map(|tr| {
            if let RunPolicy::Cron(ref policy) = *tr.task_spec().run_policy() {
                Some((tr.task_spec().id(), policy.clone()))
            } else {
                None
            }
        })
        .collect();

    cron_tasks.sort_by_key(|(id, _)| *id);
    for (task_id, policy) in cron_tasks {
        // Skip canceled tasks.
        if authority.projection().is_task_canceled(task_id) {
            continue;
        }

        // Release the projection borrow before appending newly derived runs.
        let all_runs: Vec<RunInstance> =
            authority.projection().runs_for_task(task_id).cloned().collect();

        let total_derived = u32::try_from(all_runs.len()).unwrap_or(u32::MAX);
        let non_terminal_count =
            u32::try_from(all_runs.iter().filter(|r| !r.state().is_terminal()).count())
                .unwrap_or(u32::MAX);

        // Check max_occurrences cap.
        if let Some(max) = policy.max_occurrences() {
            if total_derived >= max {
                continue; // All allowed occurrences already derived.
            }
        }

        let to_derive = CRON_WINDOW_SIZE.saturating_sub(non_terminal_count);
        if to_derive == 0 {
            continue;
        }

        // Cap to_derive by remaining max_occurrences budget.
        let to_derive = if let Some(max) = policy.max_occurrences() {
            to_derive.min(max.saturating_sub(total_derived))
        } else {
            to_derive
        };
        if to_derive == 0 {
            continue;
        }

        // Find the latest scheduled_at among all existing runs for this task.
        // New occurrences are derived strictly after this timestamp, preventing
        // duplicate runs for already-scheduled time slots.
        let last_scheduled_at = all_runs
            .iter()
            .map(|r| r.scheduled_at())
            .max()
            .unwrap_or_else(|| current_time.saturating_sub(1));

        // Two-phase cache access to avoid borrow-checker conflicts:
        // Phase 1: ensure schedule is cached (mutable borrow ends after this call).
        cache.ensure(task_id, &policy);
        // Phase 2: immutable borrow of cache during derive only.
        let schedule = cache.get(task_id).expect("schedule was just ensured");
        let new_runs =
            derive_cron_cached(task_id, schedule, last_scheduled_at, current_time, to_derive)
                .map_err(DispatchError::Derivation)?;

        if new_runs.is_empty() {
            continue; // No upcoming occurrences (finite schedule exhausted).
        }

        tracing::debug!(
            %task_id,
            count = new_runs.len(),
            "cron: deriving rolling window runs"
        );

        for run in new_runs {
            let seq = authority
                .projection()
                .latest_sequence()
                .checked_add(1)
                .ok_or(DispatchError::SequenceOverflow)?;
            let _ = authority
                .submit_command(
                    MutationCommand::RunCreate(RunCreateCommand::new(seq, run)),
                    DurabilityPolicy::Immediate,
                )
                .map_err(DispatchError::Authority)?;
        }
    }

    Ok(())
}
