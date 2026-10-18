//! Derived coordination indexes. None of these process-local fields are persisted or digested.
use super::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    reducer::ReplayReducer,
};
use actionqueue_core::{
    ids::{AttemptId, LedgerEntryId, RunId, TaskId, WaitId},
    run::{RunInstance, RunState},
    subscription::SubscriptionId,
};

/// The latest successfully applied frame's complete compound effect.
#[derive(Debug, Clone, Default)]
pub struct ProjectionChange {
    pub sequence: u64,
    pub created_tasks: BTreeSet<TaskId>,
    pub tasks: BTreeSet<TaskId>,
    pub runs: BTreeSet<RunId>,
    pub resolved_waits: BTreeSet<WaitId>,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct RunSummary {
    pub total: usize,
    pub active: usize,
    pub completed: usize,
    pub last_scheduled: u64,
}
#[derive(Debug, Clone, Default)]
pub(crate) struct DerivedIndexes {
    pub attempt_positions: HashMap<AttemptId, (RunId, usize)>,
    pub summaries: HashMap<TaskId, RunSummary>,
    pub children: HashMap<TaskId, BTreeSet<TaskId>>,
    pub dependents: HashMap<TaskId, BTreeSet<TaskId>>,
    pub key_owners: HashMap<String, RunId>,
    pub ledger_ids: HashSet<LedgerEntryId>,
    pub subscriptions: HashMap<TaskId, HashSet<SubscriptionId>>,
    pub matched_subscriptions: HashSet<SubscriptionId>,
    pub cancellation_targets: BTreeSet<TaskId>,
    // One entry per task/run, moved on change. This lets services that commit many
    // frames catch up without an event queue or scanning unchanged tasks/runs.
    task_sequence: HashMap<TaskId, u64>,
    tasks_changed: BTreeMap<u64, BTreeSet<TaskId>>,
    run_sequence: HashMap<RunId, u64>,
    runs_changed: BTreeMap<u64, BTreeSet<RunId>>,
}
impl ReplayReducer {
    /// Clones share this proof until a state-changing entry point is called.
    pub fn shares_revision_with(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.revision, &other.revision)
    }
    pub fn latest_change(&self) -> &ProjectionChange {
        &self.change
    }
    /// Latest modifications since a consumer's cursor, including multi-frame services.
    /// Space is bounded by current task count, rather than the number of mutations.
    pub fn changed_tasks_since(&self, sequence: u64) -> impl Iterator<Item = TaskId> + '_ {
        self.derived
            .tasks_changed
            .range((std::ops::Bound::Excluded(sequence), std::ops::Bound::Unbounded))
            .flat_map(|(_, ids)| ids.iter().copied())
    }
    pub fn changed_runs_since(&self, sequence: u64) -> impl Iterator<Item = RunId> + '_ {
        self.derived
            .runs_changed
            .range((std::ops::Bound::Excluded(sequence), std::ops::Bound::Unbounded))
            .flat_map(|(_, ids)| ids.iter().copied())
    }
    pub fn children_of(&self, parent: TaskId) -> impl Iterator<Item = TaskId> + '_ {
        self.derived.children.get(&parent).into_iter().flat_map(|ids| ids.iter().copied())
    }
    pub fn prerequisites_of(&self, task: TaskId) -> impl Iterator<Item = TaskId> + '_ {
        self.dependency_declarations.get(&task).into_iter().flat_map(|ids| ids.iter().copied())
    }
    pub fn key_reservation(&self, run: RunId) -> Option<&str> {
        self.key_reservations.get(&run).map(String::as_str)
    }
    pub fn matched_subscriptions(&self) -> impl Iterator<Item = SubscriptionId> + '_ {
        self.derived.matched_subscriptions.iter().copied()
    }
    pub fn cancellation_targets(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.derived.cancellation_targets.iter().copied()
    }
    pub fn has_unfinished_runs(&self, task: TaskId) -> bool {
        self.derived.summaries.get(&task).is_some_and(|s| s.active > 0)
    }
    pub(crate) fn release_key(&mut self, run: RunId) {
        if let Some(key) = self.key_reservations.remove(&run) {
            self.derived.key_owners.remove(&key);
        }
    }
    pub(crate) fn note_run_change(
        &mut self,
        run: &RunInstance,
        before: Option<RunState>,
        after: RunState,
    ) {
        self.change.runs.insert(run.id());
        self.change.tasks.insert(run.task_id());
        let s = self.derived.summaries.entry(run.task_id()).or_default();
        if let Some(before) = before {
            s.active -= usize::from(!before.is_terminal());
            s.completed -= usize::from(before == RunState::Completed);
        } else {
            s.total += 1;
        }
        s.active += usize::from(!after.is_terminal());
        s.completed += usize::from(after == RunState::Completed);
        s.last_scheduled = s.last_scheduled.max(run.scheduled_at());
    }
    pub(crate) fn finish_change(&mut self, sequence: u64) {
        self.change.sequence = sequence;
        let mut candidates = self.change.tasks.clone();
        for id in &self.change.tasks {
            if let Some(old) = self.derived.task_sequence.insert(*id, sequence) {
                let bucket = self.derived.tasks_changed.get_mut(&old).expect("task revision");
                bucket.remove(id);
                if bucket.is_empty() {
                    self.derived.tasks_changed.remove(&old);
                }
            }
            self.derived.tasks_changed.entry(sequence).or_default().insert(*id);
            candidates.extend(
                self.derived.dependents.get(id).into_iter().flat_map(|ids| ids.iter()).copied(),
            );
            candidates.extend(self.children_of(*id));
        }
        for id in &self.change.runs {
            if let Some(old) = self.derived.run_sequence.insert(*id, sequence) {
                let bucket = self.derived.runs_changed.get_mut(&old).expect("run revision");
                bucket.remove(id);
                if bucket.is_empty() {
                    self.derived.runs_changed.remove(&old);
                }
            }
            self.derived.runs_changed.entry(sequence).or_default().insert(*id);
        }
        for id in candidates {
            self.refresh_cancellation_target(id);
        }
    }
    fn refresh_cancellation_target(&mut self, id: TaskId) {
        use actionqueue_core::{
            continuation::TaskTerminalStatus as T, task::task_spec::ChildLifecyclePolicy,
        };
        let partial = self.is_task_canceled(id) && self.has_unfinished_runs(id);
        let cascade = self.get_task(&id).is_some_and(|task| {
            self.task_terminal_status(id).is_none()
                && (task.child_lifecycle_policy() == ChildLifecyclePolicy::Required
                    && task.parent_task_id().is_some_and(|p| self.is_task_canceled(p))
                    || self.prerequisites_of(id).any(|dep| {
                        self.task_terminal_status(dep).is_some_and(|s| s != T::Succeeded)
                    }))
        });
        if partial || cascade {
            self.derived.cancellation_targets.insert(id);
        } else {
            self.derived.cancellation_targets.remove(&id);
        }
    }
    /// Hydration reconstructs these indexes once, before structural validation.
    pub(crate) fn rebuild_derived(&mut self) {
        self.derived = Default::default();
        for (id, task) in self.tasks.iter() {
            if let Some(parent) = task.task_spec().parent_task_id() {
                self.derived.children.entry(parent).or_default().insert(*id);
            }
        }
        for run in self.run_instances.values() {
            let s = self.derived.summaries.entry(run.task_id()).or_default();
            s.total += 1;
            s.active += usize::from(!run.state().is_terminal());
            s.completed += usize::from(run.state() == RunState::Completed);
            s.last_scheduled = s.last_scheduled.max(run.scheduled_at());
        }
        for (id, deps) in self.dependency_declarations.iter() {
            for dep in deps {
                self.derived.dependents.entry(*dep).or_default().insert(*id);
            }
        }
        for (run, attempts) in self.attempt_history.iter() {
            for (position, attempt) in attempts.iter().enumerate() {
                self.derived.attempt_positions.insert(attempt.attempt_id(), (*run, position));
            }
        }
        self.derived.ledger_ids = self.ledger_entries.iter().map(|e| e.entry_id).collect();
        self.derived.key_owners =
            self.key_reservations.iter().map(|(id, key)| (key.clone(), *id)).collect();
        let subs: Vec<_> = self
            .subscriptions
            .iter()
            .filter(|(_, s)| {
                s.canceled_at.is_none() && s.triggered_at.is_none() && s.matched_sequence.is_none()
            })
            .map(|(id, s)| (*id, s.filter.clone()))
            .collect();
        for (id, filter) in subs {
            self.index_subscription(id, &filter);
        }
        self.derived.matched_subscriptions = self
            .subscriptions
            .iter()
            .filter(|(_, s)| {
                s.canceled_at.is_none() && s.triggered_at.is_none() && s.matched_sequence.is_some()
            })
            .map(|(id, _)| *id)
            .collect();
        self.change = ProjectionChange {
            tasks: self.tasks.keys().copied().collect(),
            runs: self.run_instances.keys().copied().collect(),
            ..Default::default()
        };
        self.finish_change(self.latest_sequence);
        self.change = Default::default();
    }
}
