//! Default priority-then-FIFO selector for ready runs.
//!
//! This module implements the default run selection policy for v0.1:
//! - Runs are selected by priority (higher values considered higher priority)
//! - Runs with the same priority are selected in FIFO order (earlier created_at first)
//! - Runs with identical priority and created_at are selected by RunId for full tie-breaking

use actionqueue_core::run::run_instance::RunInstance;

use crate::index::ready::ReadyIndex;

/// Result of running the default selector.
///
/// This structure contains the runs that should be selected (leased) from the
/// ready index, in the order they should be selected.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct SelectionResult {
    /// Runs to be selected (leased) in order.
    selected: Vec<RunInstance>,
    /// Runs remaining in the ready index after selection.
    remaining: Vec<RunInstance>,
}

impl SelectionResult {
    /// Returns the runs selected for leasing, in order.
    pub fn selected(&self) -> &[RunInstance] {
        &self.selected
    }

    /// Consumes self and returns the selected runs.
    pub fn into_selected(self) -> Vec<RunInstance> {
        self.selected
    }

    /// Returns the runs remaining in the ready index after selection.
    pub fn remaining(&self) -> &[RunInstance] {
        &self.remaining
    }
}

/// Selector-ready input that pairs a run with a pre-resolved snapshot priority.
///
/// The scheduler should populate this structure before selection so sorting never
/// depends on mutable metadata lookups during comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyRunSelectionInput {
    /// Ready run candidate.
    run: RunInstance,
    /// Snapshot priority to use for ordering in this selection pass.
    priority_snapshot: i32,
}

impl ReadyRunSelectionInput {
    /// Returns a reference to the ready run candidate.
    pub fn run(&self) -> &RunInstance {
        &self.run
    }

    /// Returns the snapshot priority.
    pub fn priority_snapshot(&self) -> i32 {
        self.priority_snapshot
    }
}

impl ReadyRunSelectionInput {
    /// Creates selector input from a ready run and an explicit snapshot priority.
    pub fn new(run: RunInstance, priority_snapshot: i32) -> Self {
        Self { run, priority_snapshot }
    }

    /// Creates selector input from a ready run using the run's snapshot priority.
    pub fn from_ready_run(run: RunInstance) -> Self {
        let priority_snapshot = run.effective_priority();
        Self::new(run, priority_snapshot)
    }
}

/// Builds selector-ready inputs from a ready index.
///
/// This captures each run's priority snapshot at input construction time so
/// selection can be performed without metadata lookups.
pub fn ready_inputs_from_index(ready: &ReadyIndex) -> Vec<ReadyRunSelectionInput> {
    ready.runs().iter().cloned().map(ReadyRunSelectionInput::from_ready_run).collect()
}

/// Default selector that picks runs by priority (descending), FIFO (ascending created_at),
/// and finally by RunId for full ties.
///
/// This function takes selector-ready inputs and returns a selection result
/// containing runs ordered by priority (higher first), then by creation time
/// (earlier first), and finally by RunId (deterministic ordering) for runs
/// with identical priority and created_at values.
///
/// # Arguments
///
/// * `ready_runs` - Selector inputs containing run and pre-resolved priority
///
/// # Returns
///
/// A SelectionResult containing:
/// - `selected`: All ready runs ordered by priority, then FIFO, then RunId
/// - `remaining`: Always empty in this default selector, because all ready
///   runs are selected. Future selectors (e.g., N-of-M selection, capacity-
///   aware selection) may return a non-empty `remaining` set to indicate
///   runs that were eligible but not selected in this pass.
///
/// # Design Note
///
/// This is the default selector for v0.1. The selection policy may be made
/// configurable in future versions (Phase 4+).
///
/// # Determinism
///
/// The selector is fully deterministic. When two runs have identical priority
/// and created_at values (a "full tie"), they are ordered by RunId using
/// lexicographic comparison of the underlying UUID bytes. This guarantees
/// stable ordering regardless of input order.
pub fn select_ready_runs(ready_runs: &[ReadyRunSelectionInput]) -> SelectionResult {
    let mut ready_runs = ready_runs.to_vec();

    // Sort by priority (descending), then by created_at (ascending) for FIFO,
    // finally by RunId for deterministic ordering of full ties.
    ready_runs.sort_by(|a, b| {
        // Compare priority (higher priority first)
        b.priority_snapshot
            .cmp(&a.priority_snapshot)
            // Compare created_at (earlier first for FIFO)
            .then_with(|| a.run.created_at().cmp(&b.run.created_at()))
            // Final tie-breaker: RunId (deterministic UUID byte ordering)
            .then_with(|| a.run.id().cmp(&b.run.id()))
    });

    let selected = ready_runs.into_iter().map(|ready_run| ready_run.run).collect();

    SelectionResult { selected, remaining: Vec::new() }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::run::run_instance::RunInstance;

    use super::*;

    #[test]
    fn selects_by_snapshot_priority_descending_then_fifo_for_ties() {
        let now = 1000;
        let input_low = ReadyRunSelectionInput::new(
            RunInstance::new_ready(actionqueue_core::ids::TaskId::new(), now - 10, now + 30, 1)
                .expect("valid ready run"),
            1,
        );
        let input_high_later = ReadyRunSelectionInput::new(
            RunInstance::new_ready(actionqueue_core::ids::TaskId::new(), now - 10, now + 20, 5)
                .expect("valid ready run"),
            5,
        );
        let input_high_earlier = ReadyRunSelectionInput::new(
            RunInstance::new_ready(actionqueue_core::ids::TaskId::new(), now - 10, now + 10, 5)
                .expect("valid ready run"),
            5,
        );

        // Input order intentionally shuffled.
        let inputs = vec![input_low, input_high_later, input_high_earlier];

        let result = select_ready_runs(&inputs);

        let ordered_created_at: Vec<u64> =
            result.selected().iter().map(|run| run.created_at()).collect();
        assert_eq!(ordered_created_at, vec![now + 10, now + 20, now + 30]);
    }

    #[test]
    fn empty_input_returns_empty_selection() {
        let result = select_ready_runs(&[]);

        assert!(result.selected().is_empty());
        assert!(result.remaining().is_empty());
    }

    #[test]
    fn ready_inputs_from_index_captures_run_priority_snapshot() {
        let run_a = RunInstance::new_ready(actionqueue_core::ids::TaskId::new(), 100, 200, 11)
            .expect("valid ready run");
        let run_b = RunInstance::new_ready(actionqueue_core::ids::TaskId::new(), 100, 300, -2)
            .expect("valid ready run");
        let ready = ReadyIndex::from_runs(vec![run_a.clone(), run_b.clone()]);

        let inputs = ready_inputs_from_index(&ready);

        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].priority_snapshot(), run_a.effective_priority());
        assert_eq!(inputs[1].priority_snapshot(), run_b.effective_priority());
        assert_eq!(inputs[0].run().id(), run_a.id());
        assert_eq!(inputs[1].run().id(), run_b.id());
    }
}
