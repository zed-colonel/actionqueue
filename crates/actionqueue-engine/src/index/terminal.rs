//! Terminal index - runs that have completed their lifecycle.
//!
//! The terminal index holds run instances in terminal states (Completed,
//! Failed, Canceled). These runs cannot transition to any other state
//! and represent the final outcome of task execution.

use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;

/// A view of all runs in terminal states.
///
/// This structure provides filtering and traversal over runs that have
/// reached a terminal state. Terminal runs cannot transition to any other
/// state and represent completed task execution outcomes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TerminalIndex {
    /// The runs in terminal states
    runs: Vec<RunInstance>,
}

impl TerminalIndex {
    /// Creates a new empty terminal index.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Creates a terminal index from a vector of runs.
    ///
    /// All runs must be in a terminal state (`Completed`, `Failed`, or
    /// `Canceled`).
    pub fn from_runs(runs: Vec<RunInstance>) -> Self {
        debug_assert!(
            runs.iter().all(|r| r.state().is_terminal()),
            "TerminalIndex::from_runs called with non-terminal run"
        );
        Self { runs }
    }

    /// Returns all runs in the terminal index.
    pub fn runs(&self) -> &[RunInstance] {
        &self.runs
    }

    /// Returns the number of runs in the terminal index.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Returns true if the index contains no runs.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Returns only completed runs.
    pub fn completed(&self) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.state() == RunState::Completed).collect()
    }

    /// Returns only failed runs.
    pub fn failed(&self) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.state() == RunState::Failed).collect()
    }

    /// Returns only canceled runs.
    pub fn canceled(&self) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.state() == RunState::Canceled).collect()
    }
}

impl From<&[RunInstance]> for TerminalIndex {
    fn from(runs: &[RunInstance]) -> Self {
        let terminal_runs: Vec<RunInstance> =
            runs.iter().filter(|run| run.state().is_terminal()).cloned().collect();
        Self::from_runs(terminal_runs)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;

    use super::*;
    use crate::index::test_util::build_run;

    #[test]
    fn terminal_index_filters_correctly() {
        let now = 1000;
        let task_id = TaskId::new();

        // Create runs in different states
        let runs = vec![
            build_run(task_id, RunState::Ready, 900, now, 0, None),
            build_run(task_id, RunState::Completed, 900, now, 0, None),
            build_run(task_id, RunState::Failed, 950, now, 0, None),
            build_run(task_id, RunState::Canceled, 800, now, 0, None),
        ];

        let index = TerminalIndex::from(runs.as_slice());

        assert_eq!(index.len(), 3); // Completed, Failed, Canceled
        assert!(index.runs().iter().all(|run| run.state().is_terminal()));
    }

    #[test]
    fn terminal_index_breakdown() {
        let now = 1000;
        let task_id = TaskId::new();

        let runs = vec![
            build_run(task_id, RunState::Completed, 900, now, 0, None),
            build_run(task_id, RunState::Completed, 950, now, 0, None),
            build_run(task_id, RunState::Failed, 800, now, 0, None),
        ];

        let index = TerminalIndex::from(runs.as_slice());

        assert_eq!(index.completed().len(), 2);
        assert_eq!(index.failed().len(), 1);
        assert_eq!(index.canceled().len(), 0);
    }

    #[test]
    fn terminal_index_is_empty() {
        let index = TerminalIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let now = 1000;
        let task_id = TaskId::new();
        let run = build_run(task_id, RunState::Completed, 900, now, 0, None);
        let index = TerminalIndex::from(std::slice::from_ref(&run));

        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn terminal_index_preserves_order_and_state_purity() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled = build_run(task_id, RunState::Scheduled, 700, now, 0, None);
        let completed = build_run(task_id, RunState::Completed, 710, now, 0, None);
        let running = build_run(
            task_id,
            RunState::Running,
            720,
            now,
            0,
            Some(actionqueue_core::ids::AttemptId::new()),
        );
        let failed = build_run(task_id, RunState::Failed, 730, now, 0, None);
        let canceled = build_run(task_id, RunState::Canceled, 740, now, 0, None);

        let expected_order =
            vec![completed.id().to_string(), failed.id().to_string(), canceled.id().to_string()];
        let runs = vec![scheduled, completed, running, failed, canceled];
        let index = TerminalIndex::from(runs.as_slice());

        let actual_order: Vec<String> =
            index.runs().iter().map(|run| run.id().to_string()).collect();
        assert_eq!(actual_order, expected_order);
        assert!(index.runs().iter().all(|run| run.state().is_terminal()));
    }
}
