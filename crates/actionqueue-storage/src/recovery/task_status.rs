//! Shared durable task terminality and structural coordination checks.
use actionqueue_core::{
    continuation::*,
    ids::TaskId,
    mutation::CancelTarget,
    task::{run_policy::RunPolicy, task_spec::ChildLifecyclePolicy},
};

use super::reducer::ReplayReducer;
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
    /// Reconstruct terminality immediately before a WAL frame. Every run ending,
    /// including recovery and legacy transitions, has sequence evidence in schema 10.
    pub(crate) fn task_terminal_status_at(
        &self,
        id: TaskId,
        sequence: u64,
    ) -> Option<TaskTerminalStatus> {
        if self.cancellations.get(&CancelTarget::Task(id)).is_some_and(|c| c.sequence < sequence) {
            return Some(TaskTerminalStatus::Canceled);
        }
        for run in self.runs_for_task(id) {
            if self.terminal_sequences.get(&run.id()).is_none_or(|end| *end >= sequence) {
                return None;
            }
        }
        self.task_runs_terminal_status(id)
    }
    /// Cross-check auxiliary ending sequences against independently retained facts.
    pub(crate) fn validate_terminal_sequences(&self) -> bool {
        use crate::mutation::wait::WaitResolutionKind;
        for (run, end) in self.terminal_sequences.iter() {
            if self.dispatch_sequences.get(run).is_some_and(|s| s >= end) {
                return false;
            }
            for a in self.get_attempt_history(run).into_iter().flatten() {
                if a.accepted_start.as_ref().is_some_and(|s| s.sequence >= *end)
                    || a.disposition.as_ref().is_some_and(|d| {
                        d.sequence > *end || d.target_state.is_terminal() && d.sequence != *end
                    })
                {
                    return false;
                }
            }
            for w in self.waits.resolved_for_run(*run) {
                let r = w.resolution.as_ref().expect("resolved wait");
                let terminal = match r.kind {
                    WaitResolutionKind::Canceled(_) => true,
                    WaitResolutionKind::Deadline => w
                        .spec
                        .deadline()
                        .is_some_and(|d| d.policy != WaitTimeoutPolicy::ResumeWithTimeout),
                    _ => false,
                };
                if r.sequence > *end || terminal && r.sequence != *end {
                    return false;
                }
            }
        }
        true
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
                self.task_terminal_status_at(*id, resolution.sequence)
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
    /// Check Complete dispositions in WAL order, validating each Required child
    /// only at its parent's first completion after admission. A terminal task cannot
    /// become unfinished again, so later completions need not recheck that child.
    pub(crate) fn validate_historical_completions(
        &self,
        completions: &mut std::collections::HashMap<TaskId, Vec<(u64, u64)>>,
    ) -> bool {
        for (parent, records) in completions {
            records.sort_unstable_by_key(|(sequence, _)| *sequence);
            let mut admitted = Vec::new();
            let mut legacy = Vec::new();
            for id in self.children_of(*parent) {
                let t = self.get_task_record(&id).expect("indexed child");
                if t.task_spec().child_lifecycle_policy() != ChildLifecyclePolicy::Required {
                    continue;
                }
                if let Some(a) = self.task_admission(id) {
                    admitted.push((a.sequence(), id));
                } else {
                    // Legacy TaskCreated frames have no admission sequence. Retain
                    // their old timestamp eligibility rule, but check each only once.
                    legacy.push((t.created_at(), id));
                }
            }
            admitted.sort_unstable();
            legacy.sort_unstable();
            let mut admitted = admitted.into_iter().peekable();
            let mut legacy = legacy.into_iter().peekable();
            for (sequence, timestamp) in records {
                while admitted.peek().is_some_and(|(s, _)| *s < *sequence) {
                    let (_, child) = admitted.next().expect("peeked child");
                    if self.task_terminal_status_at(child, *sequence).is_none() {
                        return false;
                    }
                }
                while legacy.peek().is_some_and(|(t, _)| *t <= *timestamp) {
                    let (_, child) = legacy.next().expect("peeked legacy child");
                    if self.task_terminal_status_at(child, *sequence).is_none() {
                        return false;
                    }
                }
            }
        }
        true
    }
}
