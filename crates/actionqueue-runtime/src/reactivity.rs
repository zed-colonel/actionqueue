//! Shared reconciliation of reactive matches durably observed by the reducer.
use actionqueue_core::mutation::*;
use actionqueue_storage::{
    mutation::authority::*, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};
/// Finish pending triggers after live execution or recovery in either host.
pub fn reconcile<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    now: u64,
) -> Result<(), MutationAuthorityError<actionqueue_storage::recovery::reducer::ReplayReducerError>>
{
    let mut ready: Vec<_> = a
        .projection()
        .subscriptions()
        .filter(|(_, s)| {
            s.canceled_at.is_none() && s.triggered_at.is_none() && s.matched_sequence.is_some()
        })
        .map(|(id, _)| *id)
        .collect();
    ready.sort_by_key(|id| *id.as_uuid());
    for id in ready {
        let _ = a.submit_command(
            MutationCommand::SubscriptionTrigger(SubscriptionTriggerCommand::new(
                a.projection().latest_sequence().saturating_add(1),
                id,
                now,
            )),
            DurabilityPolicy::Immediate,
        )?;
    }
    Ok(())
}
