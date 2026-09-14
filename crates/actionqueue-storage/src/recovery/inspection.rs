//! Derived exact-reference lookup. Rebuilt by admission insertion on replay and hydration.
use std::collections::{BTreeSet, HashMap};

use actionqueue_core::ids::{TaskId, TenantId};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceField {
    Trace,
    Correlation,
    Origin,
}
#[derive(Debug, Clone, Default)]
pub struct InspectionIndex {
    controls: HashMap<ControlTarget, BTreeSet<u64>>,
    attempts: HashMap<actionqueue_core::ids::AttemptId, actionqueue_core::ids::RunId>,
    history_unavailable: bool,
    tasks: HashMap<(Option<TenantId>, ReferenceField, String), BTreeSet<TaskId>>,
}
impl InspectionIndex {
    pub(crate) fn insert(&mut self, r: &crate::mutation::admission::AdmissionRecord) {
        let c = r.request().causal_context();
        for (field, value) in [
            (ReferenceField::Trace, Some(c.trace_id().as_str())),
            (ReferenceField::Correlation, Some(c.correlation_id().as_str())),
            (ReferenceField::Origin, c.origin_ref().map(|v| v.expose())),
        ] {
            if let Some(value) = value {
                self.tasks
                    .entry((r.tenant_id(), field, value.into()))
                    .or_default()
                    .insert(r.task_id());
            }
        }
    }
}
impl super::reducer::ReplayReducer {
    /// Exact, namespace-local lookup; no parsing or normalization of references.
    pub fn tasks_by_reference(
        &self,
        tenant: Option<TenantId>,
        field: ReferenceField,
        value: &str,
    ) -> impl Iterator<Item = TaskId> + '_ {
        self.inspection_index
            .tasks
            .get(&(tenant, field, value.into()))
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
    }
}

impl super::reducer::ReplayReducer {
    /// Control targets are recorded identities, never inferred from wall-clock timestamps.
    pub fn task_control_sequence(&self, task: TaskId) -> Option<u64> {
        self.control_sequences(ControlTarget::Task(task)).next_back().or_else(|| {
            self.cancellations
                .get(&actionqueue_core::mutation::CancelTarget::Task(task))
                .map(|c| c.sequence)
                .or_else(|| self.task_admission(task).map(|a| a.sequence()))
        })
    }
}

/// Recorded queue object affected by a control frame; never derived from time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlTarget {
    Task(TaskId),
    Run(actionqueue_core::ids::RunId),
    Wait(actionqueue_core::ids::WaitId),
    Signal(actionqueue_core::ids::SignalSequence),
}
impl super::reducer::ReplayReducer {
    /// False only for a standalone snapshot before retained-WAL indexes are restored.
    pub fn control_targets_available(&self) -> bool {
        !self.inspection_index.history_unavailable
    }
    pub fn control_sequences(
        &self,
        target: ControlTarget,
    ) -> impl DoubleEndedIterator<Item = u64> + '_ {
        self.inspection_index
            .controls
            .get(&target)
            .into_iter()
            .flat_map(|seqs| seqs.iter().copied())
    }
    /// Ownership is independent of any caller-supplied run identifier.
    pub fn attempt_owner(
        &self,
        attempt: actionqueue_core::ids::AttemptId,
    ) -> Option<actionqueue_core::ids::RunId> {
        self.inspection_index.attempts.get(&attempt).copied()
    }
    pub(crate) fn index_inspection_event(&mut self, event: &crate::wal::event::WalEvent) {
        use ControlTarget as T;

        use crate::wal::event::WalEventType as E;
        match event.event() {
            E::AcceptedAttemptStarted { record } => {
                self.inspection_index.attempts.insert(record.attempt_id, record.run_id);
            }
            E::AttemptStarted { run_id, attempt_id, .. } => {
                self.inspection_index.attempts.insert(*attempt_id, *run_id);
            }
            _ => {}
        }
        if event.control().is_none() {
            return;
        }
        let mut targets = match event.event() {
            E::AdmissionCommitted { record, .. } => vec![T::Task(record.task_id())],
            E::TaskCanceled { task_id, .. }
            | E::BudgetAllocated { task_id, .. }
            | E::BudgetReplenished { task_id, .. }
            | E::BudgetConsumed { task_id, .. }
            | E::SubscriptionCreated { task_id, .. } => vec![T::Task(*task_id)],
            E::RunResumed { run_id, .. }
            | E::RunSuspended { run_id, .. }
            | E::RunCanceled { run_id, .. }
            | E::LeaseHeartbeat { run_id, .. } => vec![T::Run(*run_id)],
            E::TaskCancellationCommitted { record } | E::RunCancellationCommitted { record } => {
                vec![match record.target {
                    actionqueue_core::mutation::CancelTarget::Task(id) => T::Task(id),
                    actionqueue_core::mutation::CancelTarget::Run(id) => T::Run(id),
                }]
            }
            E::WaitSatisfied { record }
            | E::WaitTimedOut { record }
            | E::WaitCanceled { record } => vec![T::Wait(record.wait_id)],
            E::SignalAdmitted { record } => vec![T::Signal(record.sequence())],
            E::SignalPinned { record } | E::SignalUnpinned { record } => self
                .signals()
                .get_signal(record.tenant_id, &record.signal_id)
                .map(|s| vec![T::Signal(s.sequence())])
                .unwrap_or_default(),
            E::SignalsRetired { record } => {
                record.sequences.iter().copied().map(T::Signal).collect()
            }
            E::AttemptDispositionCommitted { record } => vec![T::Run(record.run_id)],
            E::SubscriptionCanceled { subscription_id, .. } => self
                .subscriptions()
                .find(|(id, _)| **id == *subscription_id)
                .map(|(_, s)| vec![T::Task(s.task_id)])
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let runs: Vec<_> = targets
            .iter()
            .filter_map(|t| match t {
                T::Run(run) => Some(*run),
                T::Wait(wait) => self.waits().get(*wait).map(|w| w.run_id),
                _ => None,
            })
            .collect();
        for run in runs {
            targets.push(T::Run(run));
            if let Some(r) = self.get_run_instance(&run) {
                targets.push(T::Task(r.task_id()));
            }
        }
        for target in targets {
            self.inspection_index.controls.entry(target).or_default().insert(event.sequence());
        }
    }
    pub(crate) fn hydrate_inspection_ownership(&mut self) {
        self.inspection_index.history_unavailable = true;
        for (run, history) in &self.attempt_history {
            for attempt in history {
                self.inspection_index.attempts.insert(attempt.attempt_id(), *run);
            }
        }
    }
}
