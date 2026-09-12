//! Shared durable task terminality and structural coordination checks.
use super::reducer::ReplayReducer;
use actionqueue_core::{
    continuation::*,
    ids::TaskId,
    run::RunState,
    task::{run_policy::RunPolicy, task_spec::ChildLifecyclePolicy},
};
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
        let runs: Vec<_> = self.runs_for_task(id).collect();
        if runs.iter().any(|r| !r.state().is_terminal()) {
            return None;
        }
        match task.run_policy() {
            RunPolicy::Once if runs.is_empty() => return None,
            RunPolicy::Repeat(p) if runs.len() < p.count() as usize => return None,
            #[cfg(feature = "workflow")]
            RunPolicy::Cron(p) => {
                let last = runs.iter().map(|r| r.scheduled_at()).max().unwrap_or_else(|| {
                    self.task_records()
                        .find(|r| r.task_spec().id() == id)
                        .expect("task")
                        .created_at()
                        .saturating_sub(1)
                });
                if p.max_occurrences().is_none_or(|cap| runs.len() < cap as usize)
                    && !p.next_occurrences_after(last, 1).is_empty()
                {
                    return None;
                }
            }
            _ => {}
        }
        Some(if runs.iter().any(|r| r.state() == RunState::Completed) {
            TaskTerminalStatus::Succeeded
        } else {
            TaskTerminalStatus::Failed
        })
    }
    /// Required direct children must terminate before any parent run completes.
    pub fn required_children_terminal(&self, parent: TaskId) -> bool {
        self.task_records()
            .filter(|r| {
                r.task_spec().parent_task_id() == Some(parent)
                    && r.task_spec().child_lifecycle_policy() == ChildLifecyclePolicy::Required
            })
            .all(|r| self.task_terminal_status(r.task_spec().id()).is_some())
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
            pending.extend(
                self.task_records()
                    .filter(|t| {
                        t.task_spec().parent_task_id() == Some(id)
                            && t.task_spec().child_lifecycle_policy()
                                == ChildLifecyclePolicy::Required
                    })
                    .map(|t| t.task_spec().id()),
            );
            for w in self.waits.records().filter(|w| {
                w.resolution.is_none()
                    && self.get_run_instance(&w.run_id).is_some_and(|r| r.task_id() == id)
            }) {
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
    /// Reconstruct terminal facts at a historical resolution. Immutable attempt records
    /// and cancellation sequences disambiguate events sharing a timestamp.
    pub(crate) fn task_terminal_status_at(
        &self,
        id: TaskId,
        sequence: u64,
        timestamp: u64,
    ) -> Option<TaskTerminalStatus> {
        use actionqueue_core::mutation::CancelTarget;
        if self.cancellations.iter().any(|c| {
            c.target == CancelTarget::Task(id) && c.sequence < sequence && c.timestamp <= timestamp
        }) {
            return Some(TaskTerminalStatus::Canceled);
        }
        for run in self.runs_for_task(id) {
            if !run.state().is_terminal() || run.last_state_change_at() > timestamp {
                return None;
            }
            if self
                .get_attempt_history(&run.id())
                .into_iter()
                .flatten()
                .filter_map(|a| a.disposition.as_ref())
                .any(|d| d.target_state.is_terminal() && d.sequence >= sequence)
            {
                return None;
            }
            if run.state() == RunState::Canceled
                && self.cancellations.iter().any(|c| {
                    (c.target == CancelTarget::Run(run.id()) || c.target == CancelTarget::Task(id))
                        && c.sequence >= sequence
                })
            {
                return None;
            }
            if self
                .waits
                .records()
                .filter(|w| w.run_id == run.id())
                .filter_map(|w| w.resolution.as_ref())
                .any(|r| {
                    r.sequence >= sequence
                        && matches!(
                            r.kind,
                            crate::mutation::wait::WaitResolutionKind::Deadline
                                | crate::mutation::wait::WaitResolutionKind::Canceled(_)
                        )
                })
            {
                return None;
            }
        }
        self.task_runs_terminal_status(id)
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
                self.task_terminal_status_at(*id, resolution.sequence, resolution.timestamp)
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
    pub(crate) fn validate_historical_completion(
        &self,
        parent: TaskId,
        sequence: u64,
        timestamp: u64,
    ) -> bool {
        self.task_records()
            .filter(|t| {
                t.task_spec().parent_task_id() == Some(parent)
                    && t.task_spec().child_lifecycle_policy() == ChildLifecyclePolicy::Required
                    && self
                        .task_admission(t.task_spec().id())
                        .map_or(t.created_at() <= timestamp, |a| a.sequence() < sequence)
            })
            .all(|t| {
                self.task_terminal_status_at(t.task_spec().id(), sequence, timestamp).is_some()
            })
    }
}
