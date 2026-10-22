//! Derived inspection indexes. Exact-reference lookup is rebuilt from admissions: by
//! admission insertion on replay and in bulk on hydration. Control-target and
//! attempt-owner associations are indexed per frame on replay; schema-10 checkpoints
//! persist them, and bootstrap and full verification restore them after hydration.
use super::collections::{BTreeSet, HashMap};

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
    pub(crate) fn remove_attempt(&mut self, attempt: actionqueue_core::ids::AttemptId) {
        self.attempts.remove(&attempt);
    }

    pub(crate) fn hydrate_references<'a>(
        &mut self,
        records: impl Iterator<Item = &'a crate::mutation::admission::AdmissionRecord>,
    ) {
        let mut buckets: std::collections::HashMap<_, Vec<TaskId>> =
            std::collections::HashMap::new();
        for r in records {
            let c = r.request().causal_context();
            for (field, value) in [
                (ReferenceField::Trace, Some(c.trace_id().as_str())),
                (ReferenceField::Correlation, Some(c.correlation_id().as_str())),
                (ReferenceField::Origin, c.origin_ref().map(|v| v.expose())),
            ] {
                if let Some(value) = value {
                    buckets
                        .entry((r.tenant_id(), field, value.to_owned()))
                        .or_default()
                        .push(r.task_id());
                }
            }
        }
        self.tasks = buckets
            .into_iter()
            .map(|(k, mut v)| {
                v.sort_unstable();
                (k, v.into_iter().collect())
            })
            .collect();
    }
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
/// Recorded queue object affected by a control frame; never derived from time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlTarget {
    Task(TaskId),
    Run(actionqueue_core::ids::RunId),
    Wait(actionqueue_core::ids::WaitId),
    Signal(actionqueue_core::ids::SignalSequence),
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
    /// Control targets are recorded identities, never inferred from wall-clock timestamps.
    pub fn task_control_sequence(&self, task: TaskId) -> Option<u64> {
        self.control_sequences(ControlTarget::Task(task)).next_back().or_else(|| {
            self.cancellations
                .get(&actionqueue_core::mutation::CancelTarget::Task(task))
                .map(|c| c.sequence)
                .or_else(|| self.task_admission(task).map(|a| a.sequence()))
        })
    }
    /// False only for a projection hydrated from a logical image whose checkpoint
    /// operation associations were not restored; its control-target sequences are
    /// then unknown.
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
                .get_subscription(subscription_id)
                .map(|s| vec![T::Task(s.task_id)])
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
        for (run, history) in self.attempt_history.iter() {
            for attempt in history {
                self.inspection_index.attempts.insert(attempt.attempt_id(), *run);
            }
        }
    }
}

/// Logical operation-history associations. Exact-reference lookup is rebuilt from admissions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct InspectionImage {
    pub controls: Vec<(u8, [u8; 16], u64, Vec<u64>)>,
    pub attempts: Vec<([u8; 16], [u8; 16])>,
}
impl InspectionIndex {
    pub(crate) fn export(&self) -> InspectionImage {
        let mut controls: Vec<_> = self
            .controls
            .iter()
            .map(|(t, seqs)| {
                let (tag, id, signal) = match t {
                    ControlTarget::Task(id) => (0, *id.as_uuid().as_bytes(), 0),
                    ControlTarget::Run(id) => (1, *id.as_uuid().as_bytes(), 0),
                    ControlTarget::Wait(id) => (2, *id.as_uuid().as_bytes(), 0),
                    ControlTarget::Signal(seq) => (3, [0; 16], seq.get()),
                };
                (tag, id, signal, seqs.iter().copied().collect())
            })
            .collect();
        controls.sort();
        let mut attempts: Vec<_> = self
            .attempts
            .iter()
            .map(|(a, r)| (*a.as_uuid().as_bytes(), *r.as_uuid().as_bytes()))
            .collect();
        attempts.sort();
        InspectionImage { controls, attempts }
    }
}
impl super::reducer::ReplayReducer {
    pub(crate) fn restore_inspection(
        &mut self,
        image: InspectionImage,
    ) -> Result<(), crate::store::StoreError> {
        use actionqueue_core::ids::*;
        let bad =
            || crate::store::StoreError::InvalidStore("invalid checkpoint inspection index".into());
        let mut possible: std::collections::HashMap<_, std::collections::HashSet<_>> =
            std::collections::HashMap::new();
        for (run, history) in self.attempt_history.iter() {
            for a in history {
                possible.entry(a.attempt_id()).or_default().insert(*run);
            }
        }
        let mut owners = Vec::new();
        let mut previous_attempt = None;
        for (attempt, run) in image.attempts {
            if previous_attempt.is_some_and(|p| p >= attempt) {
                return Err(bad());
            }
            previous_attempt = Some(attempt);
            let attempt = AttemptId::from_uuid(uuid::Uuid::from_bytes(attempt));
            let run = RunId::from_uuid(uuid::Uuid::from_bytes(run));
            if possible.get(&attempt).is_none_or(|runs| !runs.contains(&run)) {
                return Err(bad());
            }
            owners.push((attempt, run));
        }
        if owners.len() != possible.len() {
            return Err(bad());
        }
        self.inspection_index.attempts = owners.into_iter().collect();
        let known_controls: std::collections::HashSet<_> =
            self.control_history.keys().copied().collect();
        let mut controls = Vec::new();
        let mut previous = None;
        for (tag, id, signal, sequences) in image.controls {
            let key = (tag, id, signal);
            if previous.is_some_and(|p| p >= key)
                || sequences.is_empty()
                || sequences.windows(2).any(|w| w[0] >= w[1])
            {
                return Err(bad());
            }
            previous = Some(key);
            let uuid = uuid::Uuid::from_bytes(id);
            let target = match (tag, uuid.is_nil(), signal) {
                (0, false, 0) => ControlTarget::Task(TaskId::from_uuid(uuid)),
                (1, false, 0) => ControlTarget::Run(RunId::from_uuid(uuid)),
                (2, false, 0) => ControlTarget::Wait(WaitId::from_uuid(uuid)),
                (3, true, s) if s > 0 && s <= self.signals.last_sequence().get() => {
                    ControlTarget::Signal(SignalSequence::new(s))
                }
                _ => return Err(bad()),
            };
            // Historical targets need not be present in a live object table.
            for seq in &sequences {
                if *seq == 0 || *seq > self.latest_sequence || !known_controls.contains(seq) {
                    return Err(bad());
                }
            }
            controls.push((target, sequences.into_iter().collect()));
        }
        self.inspection_index.controls = controls.into_iter().collect();
        self.inspection_index.history_unavailable = false;
        Ok(())
    }
}
