//! Shared validation for live preparation and replay. Caches are never authority.
//! Structural bounds and the digest were verified when the record was constructed.
use actionqueue_core::{
    admission::AdmissionRejection as R,
    run::{state::RunState, RunInstance},
    task::run_policy::RunPolicy,
};

use crate::{mutation::admission::AdmissionRecord, recovery::reducer::ReplayReducer};
impl ReplayReducer {
    pub(crate) fn validate_admission(
        &self,
        record: &AdmissionRecord,
        runs: &[RunInstance],
    ) -> Result<(), R> {
        let q = record.request();
        let s = q.task_spec();
        let id = s.id();
        if self.get_task(&id).is_some() {
            return Err(R::TaskIdCollision);
        }
        if self.admission(record.tenant_id(), record.key()).is_some() {
            return Err(R::TaskIdCollision);
        }
        if record.tenant_id().is_some_and(|t| !self.tenant_exists(t)) {
            return Err(R::TenantMismatch);
        }
        if record.tenant_id().is_some() && !cfg!(feature = "platform") {
            return Err(R::UnsupportedFeature);
        }
        let mut cursor = s.parent_task_id();
        let mut depth = 0;
        let mut seen = std::collections::HashSet::new();
        while let Some(parent) = cursor {
            if parent == id || !seen.insert(parent) {
                return Err(R::InvalidParent);
            }
            let spec = self.get_task(&parent).ok_or(R::InvalidParent)?;
            if spec.tenant_id() != s.tenant_id() {
                return Err(R::TenantMismatch);
            }
            if depth == 0 && self.task_terminal_status(parent).is_some() {
                return Err(R::TerminalParent);
            }
            depth += 1;
            if depth > 8 {
                return Err(R::HierarchyDepth);
            }
            cursor = spec.parent_task_id();
        }
        for dep in q.dependencies() {
            if *dep == id {
                return Err(R::DependencyCycle);
            }
            let spec = self.get_task(dep).ok_or(R::UnknownDependency)?;
            if spec.tenant_id() != s.tenant_id() {
                return Err(R::TenantMismatch);
            }
        }
        let mut pending = q.dependencies().to_vec();
        let mut visited = std::collections::HashSet::new();
        while let Some(dep) = pending.pop() {
            if dep == id {
                return Err(R::DependencyCycle);
            }
            if visited.insert(dep) {
                if let Some(deps) = self.dependency_declarations.get(&dep) {
                    pending.extend(deps);
                }
            }
        }
        if s.child_lifecycle_policy()
            == actionqueue_core::task::task_spec::ChildLifecyclePolicy::Required
        {
            if let Some(parent) = s.parent_task_id() {
                if q.dependencies().iter().any(|dep| self.completion_requires(*dep, parent)) {
                    return Err(R::DependencyCycle);
                }
            }
        }
        let expected = initial_schedule(s.run_policy(), record.timestamp())?;
        if runs.len() != expected.len()
            || runs.len() > actionqueue_core::limits::MAX_RUNS_PER_ADMISSION
        {
            return Err(R::InvalidRuns);
        }
        let mut seen = std::collections::HashSet::new();
        for (run, at) in runs.iter().zip(expected) {
            if run.task_id() != id {
                return Err(R::RunTaskMismatch);
            }
            if !seen.insert(run.id()) {
                return Err(R::DuplicateRun);
            }
            if self.get_run_instance(&run.id()).is_some() {
                return Err(R::RunIdCollision);
            }
            if run.id().as_uuid().is_nil()
                || run.state() != RunState::Scheduled
                || run.attempt_count() != 0
                || run.failure_attempt_count() != 0
                || run.current_attempt_id().is_some()
                || run.created_at() != record.timestamp()
                || run.last_state_change_at() != record.timestamp()
                || run.scheduled_at() != at
                || run.effective_priority() != 0
            {
                return Err(R::InvalidRuns);
            }
        }
        Ok(())
    }
}
fn initial_schedule(policy: &RunPolicy, timestamp: u64) -> Result<Vec<u64>, R> {
    match policy {
        RunPolicy::Once => Ok(vec![timestamp]),
        RunPolicy::Repeat(p) => (0..p.count())
            .map(|i| {
                u64::from(i)
                    .checked_mul(p.interval_secs())
                    .and_then(|offset| timestamp.checked_add(offset))
                    .ok_or(R::InvalidRuns)
            })
            .collect(),
        #[cfg(feature = "workflow")]
        RunPolicy::Cron(p) => Ok(p.next_occurrences_after(
            timestamp.saturating_sub(1),
            p.max_occurrences()
                .unwrap_or(actionqueue_core::task::run_policy::CRON_WINDOW_SIZE)
                .min(actionqueue_core::task::run_policy::CRON_WINDOW_SIZE) as usize,
        )),
    }
}
