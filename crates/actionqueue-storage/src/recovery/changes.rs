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

/// Coordination-relevant effects of the most recent successful
/// [`ReplayReducer::apply`].
///
/// Run creation, every run state transition (including lease close and wait
/// transitions) and run cancellation report the run and its task. Task creation and
/// cancellation, dependency declarations and budget allocation, consumption,
/// exhaustion and replenishment report the task. Frames that change neither run
/// state nor task coordination, such as lease acquisition and heartbeats, attempt
/// records, subscriptions, engine pause/resume, ledger and identity frames, report
/// only their `sequence`.
///
/// `apply` clears the change before handling a frame, so a rejected frame leaves
/// it empty with sequence 0. Hydration also leaves it empty.
#[derive(Debug, Clone, Default)]
pub struct ProjectionChange {
    /// WAL sequence of the applied frame, or 0 when the change is empty.
    pub sequence: u64,
    /// Tasks created by the frame.
    pub created_tasks: BTreeSet<TaskId>,
    /// Tasks whose coordination state changed, including the tasks of changed runs.
    pub tasks: BTreeSet<TaskId>,
    /// Runs created, transitioned or canceled by the frame.
    pub runs: BTreeSet<RunId>,
    /// Waits resolved by the frame.
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
    pub attempt_positions: HashMap<(RunId, AttemptId), usize>,
    pub active_runs: HashMap<TaskId, BTreeSet<RunId>>,
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
    /// Returns true when both projections are clones of the same revision: neither
    /// has had a state-changing entry point called since they were cloned. `true`
    /// proves equal state; `false` proves nothing.
    pub fn shares_revision_with(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.revision, &other.revision)
    }
    /// Effects of the most recent successful apply; see [`ProjectionChange`].
    pub fn latest_change(&self) -> &ProjectionChange {
        &self.change
    }
    /// Tasks whose most recent recorded change has a sequence above `sequence`, in
    /// ascending order of that change.
    ///
    /// A consumer that has applied every change up to its cursor can catch up across
    /// many frames without an event queue. Each task appears once, at its latest
    /// change, and only for the effects [`ProjectionChange::tasks`] reports.
    /// Hydration marks every task as changed at the recovered sequence. Space is
    /// bounded by task count, rather than the number of mutations.
    pub fn changed_tasks_since(&self, sequence: u64) -> impl Iterator<Item = TaskId> + '_ {
        self.derived
            .tasks_changed
            .range((std::ops::Bound::Excluded(sequence), std::ops::Bound::Unbounded))
            .flat_map(|(_, ids)| ids.iter().copied())
    }
    /// Runs whose most recent recorded change has a sequence above `sequence`, in
    /// ascending order of that change; the run counterpart of
    /// [`Self::changed_tasks_since`], for the effects [`ProjectionChange::runs`]
    /// reports.
    pub fn changed_runs_since(&self, sequence: u64) -> impl Iterator<Item = RunId> + '_ {
        self.derived
            .runs_changed
            .range((std::ops::Bound::Excluded(sequence), std::ops::Bound::Unbounded))
            .flat_map(|(_, ids)| ids.iter().copied())
    }
    pub fn children_of(&self, parent: TaskId) -> impl Iterator<Item = TaskId> + '_ {
        self.derived.children.get(&parent).into_iter().flat_map(|ids| ids.iter().copied()).inspect(
            |_| {
                #[cfg(feature = "testing")]
                super::work::child_history_visit();
            },
        )
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
    /// Only unfinished runs, excluding a task's retained terminal history.
    pub fn active_run_ids_for_task(&self, task: TaskId) -> impl Iterator<Item = RunId> + '_ {
        self.derived.active_runs.get(&task).into_iter().flat_map(|ids| ids.iter().copied())
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
        if after.is_terminal() {
            if let Some(ids) = self.derived.active_runs.get_mut(&run.task_id()) {
                ids.remove(&run.id());
                if ids.is_empty() {
                    self.derived.active_runs.remove(&run.task_id());
                }
            }
        } else {
            self.derived.active_runs.entry(run.task_id()).or_default().insert(run.id());
        }
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
            if self.is_task_canceled(*id) {
                candidates.extend(self.children_of(*id));
            }
        }
        for id in &self.change.runs {
            // Every terminal transition, including recovery and legacy controls, is
            // ordered by its WAL frame. Wall time is not a historical ordering key.
            if self.runs.get(id).is_some_and(|s| s.is_terminal()) {
                self.terminal_sequences.entry(*id).or_insert(sequence);
            }
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
        let mut summaries: std::collections::HashMap<_, RunSummary> =
            std::collections::HashMap::new();
        let mut active: std::collections::HashMap<_, Vec<_>> = std::collections::HashMap::new();
        for run in self.run_instances.values() {
            let s = summaries.entry(run.task_id()).or_default();
            s.total += 1;
            if !run.state().is_terminal() {
                active.entry(run.task_id()).or_default().push(run.id());
            }
            s.active += usize::from(!run.state().is_terminal());
            s.completed += usize::from(run.state() == RunState::Completed);
            s.last_scheduled = s.last_scheduled.max(run.scheduled_at());
        }
        self.derived.summaries = summaries.into_iter().collect();
        self.derived.active_runs =
            active.into_iter().map(|(id, runs)| (id, runs.into_iter().collect())).collect();
        for (id, deps) in self.dependency_declarations.iter() {
            for dep in deps {
                self.derived.dependents.entry(*dep).or_default().insert(*id);
            }
        }
        for (run, attempts) in self.attempt_history.iter() {
            for (position, attempt) in attempts.iter().enumerate() {
                self.derived
                    .attempt_positions
                    .entry((*run, attempt.attempt_id()))
                    .or_insert(position);
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
        // All hydrated objects begin at one known revision. Build these buckets
        // once instead of repeatedly editing the same persistent bucket per object.
        self.derived.task_sequence =
            self.tasks.keys().map(|id| (*id, self.latest_sequence)).collect();
        self.derived.run_sequence =
            self.run_instances.keys().map(|id| (*id, self.latest_sequence)).collect();
        let mut tasks: Vec<_> = self.tasks.keys().copied().collect();
        tasks.sort_unstable();
        let mut runs: Vec<_> = self.run_instances.keys().copied().collect();
        runs.sort_unstable();
        self.derived.tasks_changed.insert(self.latest_sequence, tasks.into_iter().collect());
        self.derived.runs_changed.insert(self.latest_sequence, runs.into_iter().collect());
        let tasks: Vec<_> = self.tasks.keys().copied().collect();
        for id in tasks {
            if self.dependency_declarations.is_empty()
                && self.tasks.get(&id).is_some_and(|t| t.task_spec.parent_task_id().is_none())
                && self.derived.summaries.get(&id).is_none_or(|s| s.active == 0)
            {
                continue;
            }
            self.refresh_cancellation_target(id);
        }
        self.change = Default::default();
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::{
        mutation::wait::CancelRecord,
        wal::event::{WalEvent, WalEventType as E},
    };
    use actionqueue_core::{
        mutation::CancelTarget,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    fn apply(p: &mut ReplayReducer, event: E) {
        let event = WalEvent::new(p.latest_sequence() + 1, event);
        p.apply(&event).unwrap_or_else(|error| panic!("{event:?}: {error:?}"));
    }
    #[test]
    fn legacy_repeated_attempt_identity_preserves_first_match_finish_rejection() {
        use actionqueue_core::{ids::AttemptId, mutation::AttemptResultKind};
        let mut p = ReplayReducer::new();
        let id = TaskId::new();
        let task_spec = TaskSpec::new(
            id,
            TaskPayload::new(vec![]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        apply(&mut p, E::TaskCreated { task_spec, timestamp: 1 });
        let run_instance = RunInstance::new_scheduled(id, 1, 1).unwrap();
        let run_id = run_instance.id();
        apply(&mut p, E::RunCreated { run_instance });
        let attempt_id = AttemptId::new();
        for (from, to) in [
            (RunState::Scheduled, RunState::Ready),
            (RunState::Ready, RunState::Leased),
            (RunState::Leased, RunState::Running),
        ] {
            apply(
                &mut p,
                E::RunStateChanged { run_id, previous_state: from, new_state: to, timestamp: 2 },
            );
        }
        apply(&mut p, E::AttemptStarted { run_id, attempt_id, timestamp: 3 });
        let finish = |timestamp| E::AttemptFinished {
            run_id,
            attempt_id,
            result: AttemptResultKind::Failure,
            error: Some("failure".into()),
            output: None,
            timestamp,
        };
        apply(&mut p, finish(4));
        for (from, to) in [
            (RunState::Running, RunState::RetryWait),
            (RunState::RetryWait, RunState::Ready),
            (RunState::Ready, RunState::Leased),
            (RunState::Leased, RunState::Running),
        ] {
            apply(
                &mut p,
                E::RunStateChanged { run_id, previous_state: from, new_state: to, timestamp: 5 },
            );
        }
        apply(&mut p, E::AttemptStarted { run_id, attempt_id, timestamp: 6 });
        let sequence = p.latest_sequence();
        assert_eq!(p.attempt_entry_for_run(run_id, attempt_id).unwrap().finished_at(), Some(4));
        assert_eq!(
            p.apply(&WalEvent::new(sequence + 1, finish(7))),
            Err(super::super::reducer::ReplayReducerError::CorruptedData)
        );
        assert_eq!(p.latest_sequence(), sequence);
    }

    #[test]
    fn cancellation_of_a_repeating_task_visits_only_its_unfinished_runs() {
        let mut p = ReplayReducer::new();
        let id = TaskId::new();
        let task_spec = TaskSpec::new(
            id,
            TaskPayload::new(vec![]),
            RunPolicy::repeat(1001, 1).unwrap(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        apply(&mut p, E::TaskCreated { task_spec, timestamp: 1 });
        for _ in 0..1000 {
            let run_instance = RunInstance::new_scheduled(id, 1, 1).unwrap();
            let run_id = run_instance.id();
            apply(&mut p, E::RunCreated { run_instance });
            apply(&mut p, E::RunCanceled { run_id, timestamp: 2 });
        }
        let run_instance = RunInstance::new_scheduled(id, 1, 1).unwrap();
        let active = run_instance.id();
        apply(&mut p, E::RunCreated { run_instance });
        let before = p.clone();
        super::super::work::reset_projection_work();
        let record = CancelRecord {
            target: CancelTarget::Task(id),
            tenant_id: None,
            sequence: p.latest_sequence() + 1,
            timestamp: 3,
            control_context: None,
        };
        apply(&mut p, E::TaskCancellationCommitted { record });
        assert_eq!(super::super::work::projection_work().cancellation_run_candidates, 1);
        assert_eq!(p.latest_change().runs.iter().copied().collect::<Vec<_>>(), vec![active]);
        assert_eq!(before.active_run_ids_for_task(id).collect::<Vec<_>>(), vec![active]);
        assert_eq!(p.active_run_ids_for_task(id).count(), 0);
        assert_eq!(p.run_count(), 1001);
    }
}
