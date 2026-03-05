//! Ready index - runs eligible to be leased.
//!
//! The ready index holds run instances in the Ready state. These runs have
//! passed their scheduled_at time and are eligible to be selected by the
//! scheduler for leasing to an executor.

use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;

/// A view of all runs in the Ready state.
///
/// This structure provides filtering and traversal over runs that are
/// eligible to be leased. Runs transition from Ready to Leased (or Canceled)
/// when selected by the scheduler.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadyIndex {
    /// The runs in Ready state
    runs: Vec<RunInstance>,
}

impl ReadyIndex {
    /// Creates a new empty ready index.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Creates a ready index from a vector of runs.
    ///
    /// All runs must be in the `Ready` state.
    pub fn from_runs(runs: Vec<RunInstance>) -> Self {
        debug_assert!(
            runs.iter().all(|r| r.state() == RunState::Ready),
            "ReadyIndex::from_runs called with non-Ready run"
        );
        Self { runs }
    }

    /// Returns all runs in the ready index.
    pub fn runs(&self) -> &[RunInstance] {
        &self.runs
    }

    /// Returns the number of runs in the ready index.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Returns true if the index contains no runs.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }
}

impl From<&[RunInstance]> for ReadyIndex {
    fn from(runs: &[RunInstance]) -> Self {
        let ready_runs: Vec<RunInstance> =
            runs.iter().filter(|run| run.state() == RunState::Ready).cloned().collect();
        Self::from_runs(ready_runs)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;

    use super::*;
    use crate::index::test_util::build_run;

    #[test]
    fn ready_index_filters_correctly() {
        let now = 1000;
        let task_id = TaskId::new();

        // Create runs in different states
        let runs = vec![
            build_run(task_id, RunState::Scheduled, 900, now, 0, None),
            build_run(task_id, RunState::Ready, 900, now, 0, None),
            build_run(task_id, RunState::Ready, 950, now, 0, None),
            build_run(task_id, RunState::Completed, 800, now, 0, None),
        ];

        let index = ReadyIndex::from(runs.as_slice());

        assert_eq!(index.len(), 2);
        assert_eq!(index.runs().len(), 2);
        assert!(index.runs().iter().all(|run| run.state() == RunState::Ready));
    }

    #[test]
    fn ready_index_is_empty() {
        let index = ReadyIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let now = 1000;
        let task_id = TaskId::new();
        let run = build_run(task_id, RunState::Ready, 900, now, 0, None);
        let index = ReadyIndex::from(std::slice::from_ref(&run));

        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn ready_index_preserves_ready_order_and_state_purity() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled = build_run(task_id, RunState::Scheduled, 800, now, 0, None);
        let ready_first = build_run(task_id, RunState::Ready, 810, now, 0, None);
        let running = build_run(
            task_id,
            RunState::Running,
            820,
            now,
            0,
            Some(actionqueue_core::ids::AttemptId::new()),
        );
        let ready_second = build_run(task_id, RunState::Ready, 830, now, 0, None);
        let canceled = build_run(task_id, RunState::Canceled, 840, now, 0, None);

        let expected_order = vec![ready_first.id().to_string(), ready_second.id().to_string()];
        let runs = vec![scheduled, ready_first, running, ready_second, canceled];
        let index = ReadyIndex::from(runs.as_slice());

        let actual_order: Vec<String> =
            index.runs().iter().map(|run| run.id().to_string()).collect();
        assert_eq!(actual_order, expected_order);
        assert!(index.runs().iter().all(|run| run.state() == RunState::Ready));
    }
}
