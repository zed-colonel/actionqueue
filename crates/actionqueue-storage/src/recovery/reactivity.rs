//! Record reactive evidence in WAL order, never by wall-clock comparison.
use super::reducer::ReplayReducer;
use actionqueue_core::{
    ids::RunId,
    run::RunState,
    subscription::{EventFilter, SubscriptionId},
};
use std::collections::{BTreeSet, HashMap};

pub(super) enum Observation {
    Runs(BTreeSet<RunId>),
    Terminal(bool),
    Budget(u64, bool),
}
impl ReplayReducer {
    pub(super) fn subscription_observations(&self) -> HashMap<SubscriptionId, Observation> {
        self.subscriptions()
            .filter(|(_, s)| {
                s.canceled_at.is_none() && s.triggered_at.is_none() && s.matched_sequence.is_none()
            })
            .map(|(id, s)| {
                (
                    *id,
                    match s.filter {
                        EventFilter::RunStateChanged { task_id, state } => Observation::Runs(
                            self.runs_for_task(task_id)
                                .filter(|r| r.state() == state)
                                .map(|r| r.id())
                                .collect(),
                        ),
                        EventFilter::TaskCompleted { task_id } => Observation::Terminal(
                            self.task_terminal_status(task_id).is_some()
                                && self
                                    .runs_for_task(task_id)
                                    .any(|r| r.state() == RunState::Completed),
                        ),
                        EventFilter::BudgetThreshold { task_id, dimension, threshold_pct } => {
                            let b = self.get_budget(&task_id, dimension);
                            Observation::Budget(
                                b.map_or(0, |b| b.consumed),
                                b.is_some_and(|b| {
                                    b.limit > 0
                                        && (b.consumed as u128) * 100
                                            >= (b.limit as u128) * (threshold_pct as u128)
                                }),
                            )
                        }
                    },
                )
            })
            .collect()
    }
    pub(super) fn record_subscription_matches(
        &mut self,
        before: HashMap<SubscriptionId, Observation>,
        sequence: u64,
    ) {
        for (id, after) in self.subscription_observations() {
            let matched = match (before.get(&id), after) {
                (Some(Observation::Runs(old)), Observation::Runs(new)) => !new.is_subset(old),
                (Some(Observation::Terminal(false)), Observation::Terminal(true)) => true,
                (Some(Observation::Budget(old, _)), Observation::Budget(new, true)) => new > *old,
                _ => false,
            };
            if matched {
                self.subscriptions.get_mut(&id).expect("observed subscription").matched_sequence =
                    Some(sequence);
            }
        }
    }
}
