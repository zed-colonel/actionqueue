//! Scheduled index - runs waiting to become ready.
//!
//! The scheduled index holds run instances in the Scheduled state. These runs
//! are newly derived and waiting for their scheduled_at time to pass before
//! they can transition to Ready.

use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;

/// A view of all runs in the Scheduled state.
///
/// This structure provides filtering and traversal over runs that are
/// waiting to become ready. Runs transition from Scheduled to Ready when
/// the scheduled_at timestamp has passed according to the scheduler clock.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduledIndex {
    /// The runs in Scheduled state
    runs: Vec<RunInstance>,
}

impl ScheduledIndex {
    /// Creates a new empty scheduled index.
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Creates a scheduled index from a vector of runs.
    ///
    /// All runs must be in the `Scheduled` state.
    pub fn from_runs(runs: Vec<RunInstance>) -> Self {
        debug_assert!(
            runs.iter().all(|r| r.state() == RunState::Scheduled),
            "ScheduledIndex::from_runs called with non-Scheduled run"
        );
        Self { runs }
    }

    /// Returns all runs in the scheduled index.
    pub fn runs(&self) -> &[RunInstance] {
        &self.runs
    }

    /// Returns the number of runs in the scheduled index.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Returns true if the index contains no runs.
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Filters runs that are ready to transition to the Ready state.
    ///
    /// A run is ready for promotion if its scheduled_at time has passed.
    pub fn ready_for_promotion(&self, current_time: u64) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.scheduled_at() <= current_time).collect()
    }

    /// Filters runs that are still waiting to become ready.
    ///
    /// A run is still waiting if its scheduled_at time is in the future.
    pub fn waiting(&self, current_time: u64) -> Vec<&RunInstance> {
        self.runs.iter().filter(|run| run.scheduled_at() > current_time).collect()
    }
}

impl From<&[RunInstance]> for ScheduledIndex {
    fn from(runs: &[RunInstance]) -> Self {
        let scheduled_runs: Vec<RunInstance> =
            runs.iter().filter(|run| run.state() == RunState::Scheduled).cloned().collect();
        Self::from_runs(scheduled_runs)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::run_instance::RunInstance;

    use super::*;
    use crate::index::test_util::build_run;

    #[test]
    fn scheduled_index_filters_correctly() {
        let now = 1000;
        let task_id = TaskId::new();

        let runs = vec![
            RunInstance::new_scheduled(task_id, 900, now).expect("valid scheduled run"), /* past, ready for promotion */
            RunInstance::new_scheduled(task_id, 1000, now).expect("valid scheduled run"), /* current, ready for promotion */
            RunInstance::new_scheduled(task_id, 1100, now).expect("valid scheduled run"), /* future, still waiting */
        ];

        let index = ScheduledIndex::from(runs.as_slice());

        assert_eq!(index.len(), 3);

        let ready = index.ready_for_promotion(1000);
        assert_eq!(ready.len(), 2); // 900 and 1000 are <= 1000

        let waiting = index.waiting(1000);
        assert_eq!(waiting.len(), 1); // 1100 is > 1000
        assert!(index.runs().iter().all(|run| run.state() == RunState::Scheduled));
    }

    #[test]
    fn scheduled_index_is_empty() {
        let index = ScheduledIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        let now = 1000;
        let task_id = TaskId::new();
        let run = RunInstance::new_scheduled(task_id, 1000, now).expect("valid scheduled run");
        let index = ScheduledIndex::from(std::slice::from_ref(&run));

        assert!(!index.is_empty());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn scheduled_index_preserves_order_and_state_purity() {
        let now = 1000;
        let task_id = TaskId::new();

        let ready = build_run(task_id, RunState::Ready, 700, now, 0, None);
        let scheduled_first =
            RunInstance::new_scheduled(task_id, 1100, now).expect("valid scheduled run");
        let running = build_run(
            task_id,
            RunState::Running,
            800,
            now,
            0,
            Some(actionqueue_core::ids::AttemptId::new()),
        );
        let scheduled_second =
            RunInstance::new_scheduled(task_id, 1200, now).expect("valid scheduled run");
        let terminal = build_run(task_id, RunState::Completed, 900, now, 0, None);

        let expected_order =
            vec![scheduled_first.id().to_string(), scheduled_second.id().to_string()];
        let runs = vec![ready, scheduled_first, running, scheduled_second, terminal];
        let index = ScheduledIndex::from(runs.as_slice());

        let actual_order: Vec<String> =
            index.runs().iter().map(|run| run.id().to_string()).collect();
        assert_eq!(actual_order, expected_order);
        assert!(index.runs().iter().all(|run| run.state() == RunState::Scheduled));
    }
}
