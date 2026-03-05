//! Running index - active runs that are leased or executing.
//!
//! The running index holds run instances that are actively in-flight:
//! - `Leased`: reserved for an executor but not yet started
//! - `Running`: currently executing

use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;

/// A view of all active runs (`Leased` and `Running`).
///
/// This structure provides filtering and traversal over runs that are
/// currently in-flight. Active runs transition from Running to either:
/// - RetryWait (on failure, if retries remain)
/// - Completed (on success)
/// - Failed (on failure, no retries remaining)
/// - Canceled
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunningIndex {
    /// The runs in Running state
    runs: Vec<RunInstance>,
}

impl RunningIndex {
    /// Creates a new empty running index.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Creates a running index from a vector of runs.
    ///
    /// All runs must be in the `Leased` or `Running` state.
    pub fn from_runs(runs: Vec<RunInstance>) -> Self {
        debug_assert!(
            runs.iter().all(|r| matches!(r.state(), RunState::Leased | RunState::Running)),
            "RunningIndex::from_runs called with non-Leased/Running run"
        );
        Self { runs }
    }

    /// Returns all runs in the running index.
    pub fn runs(&self) -> &[RunInstance] {
        &self.runs
    }

    /// Returns the number of runs in the running index.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Returns true if the index contains no runs.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Returns only leased runs.
    pub fn leased(&self) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.state() == RunState::Leased).collect()
    }

    /// Returns only currently executing runs.
    pub fn executing(&self) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.state() == RunState::Running).collect()
    }
}

impl From<&[RunInstance]> for RunningIndex {
    fn from(runs: &[RunInstance]) -> Self {
        let running_runs: Vec<RunInstance> = runs
            .iter()
            .filter(|run| matches!(run.state(), RunState::Leased | RunState::Running))
            .cloned()
            .collect();
        Self::from_runs(running_runs)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;

    use super::*;
    use crate::index::test_util::build_run;

    #[test]
    fn running_index_filters_correctly() {
        let now = 1000;
        let task_id = TaskId::new();

        // Create runs in different states
        let runs = vec![
            build_run(task_id, RunState::Ready, 900, now, 0, None),
            build_run(task_id, RunState::Leased, 900, now, 0, None),
            build_run(
                task_id,
                RunState::Running,
                950,
                now,
                0,
                Some(actionqueue_core::ids::AttemptId::new()),
            ),
            build_run(task_id, RunState::Completed, 800, now, 0, None),
        ];

        let index = RunningIndex::from(runs.as_slice());

        assert_eq!(index.len(), 2);
        assert_eq!(index.runs().len(), 2);
        assert_eq!(index.leased().len(), 1);
        assert_eq!(index.executing().len(), 1);
        assert!(index
            .runs()
            .iter()
            .all(|run| matches!(run.state(), RunState::Leased | RunState::Running)));
    }

    #[test]
    fn running_index_is_empty() {
        let index = RunningIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let now = 1000;
        let task_id = TaskId::new();
        let run = build_run(task_id, RunState::Leased, 900, now, 0, None);
        let index = RunningIndex::from(std::slice::from_ref(&run));

        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn running_index_preserves_active_run_order() {
        let now = 1000;
        let task_id = TaskId::new();

        let ready = build_run(task_id, RunState::Ready, 800, now, 0, None);
        let leased = build_run(task_id, RunState::Leased, 810, now, 0, None);
        let completed = build_run(task_id, RunState::Completed, 820, now, 0, None);
        let running = build_run(
            task_id,
            RunState::Running,
            830,
            now,
            0,
            Some(actionqueue_core::ids::AttemptId::new()),
        );

        let expected_order = vec![leased.id().to_string(), running.id().to_string()];
        let runs = vec![ready, leased, completed, running];
        let index = RunningIndex::from(runs.as_slice());

        let actual_order: Vec<String> =
            index.runs().iter().map(|run| run.id().to_string()).collect();
        assert_eq!(actual_order, expected_order);
    }
}
