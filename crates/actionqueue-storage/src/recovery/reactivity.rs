//! Record reactive evidence in WAL order, evaluating only affected subscriptions.
#[cfg(test)]
#[path = "reactivity_tests.rs"]
mod replay_tests;

use super::reducer::ReplayReducer;
use crate::wal::event::{WalEvent, WalEventType as E};
use actionqueue_core::{
    ids::{RunId, TaskId},
    subscription::{EventFilter, SubscriptionId},
};
use std::collections::{BTreeSet, HashMap};

pub(super) enum Observation {
    Runs(BTreeSet<RunId>),
    Terminal(bool),
    Budget(u64, bool),
}
pub(super) struct SubscriptionObservations {
    runs: Vec<RunId>,
    before: HashMap<SubscriptionId, Observation>,
    #[cfg(feature = "testing")]
    tasks: BTreeSet<TaskId>,
}
fn observed_task(filter: &EventFilter) -> TaskId {
    match filter {
        EventFilter::RunStateChanged { task_id, .. }
        | EventFilter::TaskCompleted { task_id }
        | EventFilter::BudgetThreshold { task_id, .. } => *task_id,
    }
}
impl ReplayReducer {
    pub(crate) fn index_subscription(&mut self, id: SubscriptionId, filter: &EventFilter) {
        self.derived.subscriptions.entry(observed_task(filter)).or_default().insert(id);
    }
    pub(crate) fn unindex_subscription(&mut self, id: SubscriptionId) {
        if let Some(s) = self.subscriptions.get(&id) {
            let task = observed_task(&s.filter);
            if let Some(bucket) = self.derived.subscriptions.get_mut(&task) {
                bucket.remove(&id);
            }
        }
    }
    fn observation(&self, filter: &EventFilter, runs: &[RunId]) -> Observation {
        match *filter {
            EventFilter::RunStateChanged { task_id, state } => Observation::Runs(
                runs.iter()
                    .copied()
                    .filter(|id| {
                        self.get_run_instance(id)
                            .is_some_and(|r| r.task_id() == task_id && r.state() == state)
                    })
                    .collect(),
            ),
            EventFilter::TaskCompleted { task_id } => Observation::Terminal(
                self.task_terminal_status(task_id).is_some()
                    && self.derived.summaries.get(&task_id).is_some_and(|s| s.completed > 0),
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
        }
    }
    pub(super) fn subscription_observations(&self, event: &WalEvent) -> SubscriptionObservations {
        let mut tasks = BTreeSet::new();
        let mut runs = Vec::new();
        match event.event() {
            E::RunCreated { run_instance } => {
                tasks.insert(run_instance.task_id());
                runs.push(run_instance.id());
            }
            E::RunStateChanged { run_id, .. }
            | E::RunCanceled { run_id, .. }
            | E::RunSuspended { run_id, .. }
            | E::RunResumed { run_id, .. }
            | E::LeaseExpired { run_id, .. }
            | E::LeaseReleased { run_id, .. } => runs.push(*run_id),
            E::AttemptDispositionCommitted { record } => {
                runs.push(record.run_id);
                for c in &record.children {
                    tasks.insert(c.admission.task_id());
                    runs.extend(c.runs.iter().map(|r| r.id()));
                }
            }
            E::WaitSatisfied { record }
            | E::WaitTimedOut { record }
            | E::WaitCanceled { record } => runs.push(record.run_id),
            E::WaitEstablished { record } => runs.push(record.run_id),
            E::TaskCancellationCommitted { record } | E::RunCancellationCommitted { record } => {
                match record.target {
                    actionqueue_core::mutation::CancelTarget::Run(id) => runs.push(id),
                    actionqueue_core::mutation::CancelTarget::Task(id) => {
                        tasks.insert(id);
                        runs.extend(self.active_run_ids_for_task(id));
                    }
                }
            }
            E::AdmissionCommitted { record, runs: created } => {
                tasks.insert(record.task_id());
                runs.extend(created.iter().map(|r| r.id()));
            }
            E::TaskCanceled { task_id, .. }
            | E::DependencyDeclared { task_id, .. }
            | E::BudgetAllocated { task_id, .. }
            | E::BudgetConsumed { task_id, .. }
            | E::BudgetExhausted { task_id, .. }
            | E::BudgetReplenished { task_id, .. } => {
                tasks.insert(*task_id);
            }
            E::TaskCreated { task_spec, .. } => {
                tasks.insert(task_spec.id());
            }
            // Keep this exhaustive: every new event must classify its reactive effects.
            E::StoreInitialized { .. }
            | E::AcceptedAttemptStarted { .. }
            | E::AttemptClosed { .. }
            | E::AttemptStarted { .. }
            | E::AttemptFinished { .. }
            | E::LeaseAcquired { .. }
            | E::LeaseHeartbeat { .. }
            | E::EnginePaused { .. }
            | E::EngineResumed { .. }
            | E::SubscriptionCreated { .. }
            | E::SubscriptionTriggered { .. }
            | E::SubscriptionCanceled { .. }
            | E::SignalAdmitted { .. }
            | E::SignalPinned { .. }
            | E::SignalUnpinned { .. }
            | E::SignalsRetired { .. }
            | E::ActorRegistered { .. }
            | E::ActorDeregistered { .. }
            | E::ActorHeartbeat { .. }
            | E::TenantCreated { .. }
            | E::RoleAssigned { .. }
            | E::CapabilityGranted { .. }
            | E::CapabilityRevoked { .. }
            | E::LedgerEntryAppended { .. } => {}
        }
        for run in &runs {
            if let Some(r) = self.get_run_instance(run) {
                tasks.insert(r.task_id());
            }
        }
        let observations = tasks
            .iter()
            .flat_map(|task| {
                self.derived.subscriptions.get(task).into_iter().flat_map(|ids| ids.iter())
            })
            .map(|id| {
                #[cfg(feature = "testing")]
                super::work::subscription_candidate();
                (
                    *id,
                    self.observation(
                        &self.subscriptions.get(id).expect("indexed subscription").filter,
                        &runs,
                    ),
                )
            })
            .collect();
        SubscriptionObservations {
            runs,
            before: observations,
            #[cfg(feature = "testing")]
            tasks,
        }
    }
    pub(super) fn record_subscription_matches(
        &mut self,
        observations: SubscriptionObservations,
        sequence: u64,
    ) {
        // Validate the prediction against actual reducer writes, even with no subscriptions.
        // This visits only the change, so instrumented scale runs remain history independent.
        #[cfg(feature = "testing")]
        {
            let runs: BTreeSet<_> = observations.runs.iter().copied().collect();
            assert!(
                self.change.runs.iter().all(|id| runs.contains(id)),
                "changed run missing from subscription candidates"
            );
            assert!(
                self.change.tasks.iter().all(|id| observations.tasks.contains(id)),
                "changed task missing from subscription candidates"
            );
        }
        for (id, old) in observations.before {
            let s = self.subscriptions.get(&id).expect("observed subscription");
            let matched = match (old, self.observation(&s.filter, &observations.runs)) {
                (Observation::Runs(old), Observation::Runs(new)) => !new.is_subset(&old),
                (Observation::Terminal(false), Observation::Terminal(true)) => true,
                (Observation::Budget(old, _), Observation::Budget(new, true)) => new > old,
                _ => false,
            };
            if matched {
                self.unindex_subscription(id);
                self.derived.matched_subscriptions.insert(id);
                self.subscriptions.get_mut(&id).unwrap().matched_sequence = Some(sequence);
            }
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use actionqueue_core::{
        run::RunInstance,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    fn apply(p: &mut ReplayReducer, event: E) {
        p.apply(&WalEvent::new(p.latest_sequence() + 1, event)).unwrap();
    }
    fn task(p: &mut ReplayReducer) -> TaskId {
        let id = TaskId::new();
        let task_spec = TaskSpec::new(
            id,
            TaskPayload::new(vec![]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        apply(p, E::TaskCreated { task_spec, timestamp: 1 });
        id
    }
    #[test]
    #[should_panic(expected = "changed run missing from subscription candidates")]
    fn candidate_guard_detects_unobserved_run_creation_without_subscriptions() {
        let mut p = ReplayReducer::new();
        let id = task(&mut p);
        p.change = Default::default();
        let observations =
            p.subscription_observations(&WalEvent::new(2, E::EnginePaused { timestamp: 1 }));
        p.apply_run_created(&RunInstance::new_scheduled(id, 1, 1).unwrap()).unwrap();
        p.record_subscription_matches(observations, 2);
    }
    #[test]
    #[should_panic(expected = "changed task missing from subscription candidates")]
    fn candidate_guard_detects_unobserved_task_terminality_without_subscriptions() {
        let mut p = ReplayReducer::new();
        let id = task(&mut p);
        p.change = Default::default();
        let observations =
            p.subscription_observations(&WalEvent::new(2, E::EnginePaused { timestamp: 1 }));
        p.apply_task_canceled(&id, 2).unwrap();
        p.record_subscription_matches(observations, 2);
    }
    #[test]
    #[should_panic(expected = "changed task missing from subscription candidates")]
    fn candidate_guard_detects_unobserved_budget_consumption_without_subscriptions() {
        use actionqueue_core::budget::BudgetDimension;
        let mut p = ReplayReducer::new();
        let id = task(&mut p);
        apply(
            &mut p,
            E::BudgetAllocated {
                task_id: id,
                dimension: BudgetDimension::CostCents,
                limit: 100,
                timestamp: 1,
            },
        );
        p.change = Default::default();
        let observations =
            p.subscription_observations(&WalEvent::new(3, E::EnginePaused { timestamp: 1 }));
        p.apply_budget_consumed(id, BudgetDimension::CostCents, 50, 2);
        p.record_subscription_matches(observations, 3);
    }
    #[test]
    fn unrelated_subscriptions_are_not_visited_and_new_subscriptions_do_not_match_retroactively() {
        use actionqueue_core::run::RunState;
        let mut p = ReplayReducer::new();
        let watched = task(&mut p);
        let unrelated = task(&mut p);
        for _ in 0..1000 {
            apply(
                &mut p,
                E::SubscriptionCreated {
                    subscription_id: SubscriptionId::new(),
                    task_id: unrelated,
                    filter: EventFilter::RunStateChanged {
                        task_id: watched,
                        state: RunState::Scheduled,
                    },
                    timestamp: 1,
                },
            );
        }
        super::super::work::reset_projection_work();
        apply(
            &mut p,
            E::RunCreated { run_instance: RunInstance::new_scheduled(unrelated, 1, 1).unwrap() },
        );
        assert_eq!(super::super::work::projection_work().subscription_candidates, 0);
        let original = p.clone();
        apply(
            &mut p,
            E::RunCreated { run_instance: RunInstance::new_scheduled(watched, 1, 1).unwrap() },
        );
        let sequence = p.latest_sequence();
        assert_eq!(super::super::work::projection_work().subscription_candidates, 1000);
        assert!(p.subscriptions().all(|(_, s)| s.matched_sequence == Some(sequence)));
        assert!(original.subscriptions().all(|(_, s)| s.matched_sequence.is_none()));
        let late = SubscriptionId::new();
        apply(
            &mut p,
            E::SubscriptionCreated {
                subscription_id: late,
                task_id: unrelated,
                filter: EventFilter::RunStateChanged {
                    task_id: watched,
                    state: RunState::Scheduled,
                },
                timestamp: 1,
            },
        );
        assert!(p.get_subscription(&late).unwrap().matched_sequence.is_none());
    }
}
