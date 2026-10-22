//! Shared durable task terminality and structural coordination checks.
use actionqueue_core::{
    continuation::*,
    ids::TaskId,
    mutation::CancelTarget,
    run::{RunInstance, RunState},
    task::{run_policy::RunPolicy, task_spec::ChildLifecyclePolicy},
};

use super::reducer::ReplayReducer;
use crate::mutation::wait::{CancelRecord, WaitResolutionKind};
impl ReplayReducer {
    /// A drained cron window is not terminal while another occurrence can be derived.
    pub fn task_terminal_status(&self, id: TaskId) -> Option<TaskTerminalStatus> {
        if self.is_task_canceled(id) {
            return Some(TaskTerminalStatus::Canceled);
        }
        self.task_runs_terminal_status(id)
    }
    fn task_runs_terminal_status(&self, id: TaskId) -> Option<TaskTerminalStatus> {
        let task = self.get_task(&id)?;
        let empty = super::changes::RunSummary::default();
        let summary = self.derived.summaries.get(&id).unwrap_or(&empty);
        if summary.active > 0 {
            return None;
        }
        match task.run_policy() {
            RunPolicy::Once if summary.total == 0 => return None,
            RunPolicy::Repeat(p) if summary.total < p.count() as usize => return None,
            #[cfg(feature = "workflow")]
            RunPolicy::Cron(p) => {
                let last = if summary.total > 0 {
                    summary.last_scheduled
                } else {
                    self.get_task_record(&id).expect("task").created_at().saturating_sub(1)
                };
                if p.max_occurrences().is_none_or(|cap| summary.total < cap as usize)
                    && !p.next_occurrences_after(last, 1).is_empty()
                {
                    return None;
                }
            }
            _ => {}
        }
        Some(if summary.completed > 0 {
            TaskTerminalStatus::Succeeded
        } else {
            TaskTerminalStatus::Failed
        })
    }
    /// Required direct children must terminate before any parent run completes.
    pub fn required_children_terminal(&self, parent: TaskId) -> bool {
        self.children_of(parent)
            .filter(|id| {
                self.get_task(id)
                    .is_some_and(|t| t.child_lifecycle_policy() == ChildLifecyclePolicy::Required)
            })
            .all(|id| self.task_terminal_status(id).is_some())
    }
    /// Follow eligibility edges and required-child completion gates to detect deadlocks.
    pub(crate) fn completion_requires(&self, start: TaskId, target: TaskId) -> bool {
        let mut pending = vec![start];
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = pending.pop() {
            if id == target {
                return true;
            }
            if !seen.insert(id) {
                continue;
            }
            if let Some(deps) = self.dependency_declarations.get(&id) {
                pending.extend(deps);
            }
            pending.extend(self.children_of(id).filter(|child| {
                self.get_task(child)
                    .is_some_and(|t| t.child_lifecycle_policy() == ChildLifecyclePolicy::Required)
            }));
            for w in self
                .waits
                .active_by_task
                .get(&id)
                .into_iter()
                .flat_map(|runs| runs.iter())
                .filter_map(|run| self.waits.active(*run))
            {
                if let WaitTarget::Children { task_ids, .. } = w.spec.target() {
                    pending.extend(task_ids);
                }
            }
        }
        false
    }
    /// Select immutable evidence deterministically from authoritative state.
    pub fn child_wait_outcomes(&self, spec: &WaitSpec) -> Option<Vec<ChildOutcome>> {
        let WaitTarget::Children { task_ids, policy } = spec.target() else { return None };
        let outcomes: Vec<_> = task_ids
            .iter()
            .filter_map(|id| {
                self.task_terminal_status(*id).map(|status| ChildOutcome { task_id: *id, status })
            })
            .collect();
        if *policy == ChildWaitPolicy::AllSucceededOrAnyFailed {
            if let Some(failure) =
                outcomes.iter().find(|o| o.status != TaskTerminalStatus::Succeeded)
            {
                return Some(vec![failure.clone()]);
            }
        }
        (outcomes.len() == task_ids.len()).then_some(outcomes)
    }
    /// Reconstruct a task's terminal status as of the frame at `sequence`, as live
    /// preparation observed it before that frame. A run counts only if the frame that
    /// ended it precedes `sequence`. Dispositions, wait resolutions and committed
    /// cancellations record that frame's WAL sequence, so wall-clock timestamps, which
    /// may step backwards, never decide those runs. Recovery and legacy transitions
    /// record none: any recorded activity at or after `sequence` still proves such a
    /// run unfinished, and otherwise `legacy_timestamp`, when given, excludes a run
    /// whose last transition is later.
    pub(crate) fn task_terminal_status_at(
        &self,
        id: TaskId,
        sequence: u64,
        legacy_timestamp: Option<u64>,
    ) -> Option<TaskTerminalStatus> {
        let task_cancel = self.cancellations.get(&CancelTarget::Task(id));
        if task_cancel.is_some_and(|c| c.sequence < sequence) {
            return Some(TaskTerminalStatus::Canceled);
        }
        for run in self.runs_for_task(id) {
            if !run.state().is_terminal() {
                return None;
            }
            let (last, recorded) = self.terminal_run_evidence(run, task_cancel);
            if last >= sequence
                || !recorded && legacy_timestamp.is_some_and(|t| run.last_state_change_at() > t)
            {
                return None;
            }
        }
        self.task_runs_terminal_status(id)
    }
    /// The latest WAL sequence this image records for a terminal run, and whether the
    /// frame that ended the run is among those records. Reads only the run's own
    /// attempts and resolved waits.
    fn terminal_run_evidence(
        &self,
        run: &RunInstance,
        task_cancel: Option<&CancelRecord>,
    ) -> (u64, bool) {
        let mut last = self.dispatch_sequences.get(&run.id()).copied().unwrap_or(0);
        let mut recorded = false;
        for a in self.get_attempt_history(&run.id()).into_iter().flatten() {
            last = last.max(a.accepted_start.as_ref().map_or(0, |s| s.sequence));
            if let Some(d) = &a.disposition {
                last = last.max(d.sequence);
                recorded |= d.target_state.is_terminal();
            }
        }
        for w in self.waits.resolved_for_run(run.id()) {
            let Some(r) = &w.resolution else { continue };
            last = last.max(r.sequence);
            recorded |= match r.kind {
                WaitResolutionKind::Canceled(_) => true,
                WaitResolutionKind::Deadline => w
                    .spec
                    .deadline()
                    .is_some_and(|d| d.policy != WaitTimeoutPolicy::ResumeWithTimeout),
                _ => false,
            };
        }
        if run.state() == RunState::Canceled {
            // A run cancellation names its run; a task cancellation ended the runs
            // still active when it committed.
            let run_cancel = self.cancellations.get(&CancelTarget::Run(run.id()));
            if let Some(c) = run_cancel.or(task_cancel.filter(|_| !recorded)) {
                last = last.max(c.sequence);
                recorded = true;
            }
        }
        (last, recorded)
    }
    pub(crate) fn validate_historical_child_evidence(
        &self,
        wait: &crate::mutation::wait::WaitRecord,
        outcomes: &[ChildOutcome],
    ) -> Result<(), actionqueue_core::mutation::WaitRejection> {
        use actionqueue_core::mutation::WaitRejection as E;
        let WaitTarget::Children { task_ids, policy } = wait.spec.target() else {
            return Err(E::InvalidSignal);
        };
        let resolution = wait.resolution.as_ref().ok_or(E::InvalidState)?;
        let mut expected: Vec<_> = task_ids
            .iter()
            .filter_map(|id| {
                self.task_terminal_status_at(*id, resolution.sequence, Some(resolution.timestamp))
                    .map(|status| ChildOutcome { task_id: *id, status })
            })
            .collect();
        if *policy == ChildWaitPolicy::AllSucceededOrAnyFailed {
            if let Some(failure) =
                expected.iter().find(|o| o.status != TaskTerminalStatus::Succeeded).cloned()
            {
                expected = vec![failure];
            } else if expected.len() != task_ids.len() {
                return Err(E::InvalidSignal);
            }
        } else if expected.len() != task_ids.len() {
            return Err(E::InvalidSignal);
        }
        if expected != outcomes {
            return Err(E::InvalidSignal);
        }
        Ok(())
    }
    /// The rule live preparation applies to a Complete disposition: every Required
    /// direct child admitted before it was terminal when it committed. Only the
    /// parent's children are read. Unlike child-wait evidence, no outcome is selected
    /// from the children, so a run ended by a frame without a recorded sequence counts
    /// as terminal unless later recorded activity proves otherwise; its timestamp
    /// might have stepped backwards.
    pub(crate) fn validate_historical_completion(
        &self,
        parent: TaskId,
        sequence: u64,
        timestamp: u64,
    ) -> bool {
        self.children_of(parent)
            .filter_map(|id| self.get_task_record(&id))
            .filter(|t| {
                t.task_spec().child_lifecycle_policy() == ChildLifecyclePolicy::Required
                    && self
                        .task_admission(t.task_spec().id())
                        .map_or(t.created_at() <= timestamp, |a| a.sequence() < sequence)
            })
            .all(|t| self.task_terminal_status_at(t.task_spec().id(), sequence, None).is_some())
    }
}
