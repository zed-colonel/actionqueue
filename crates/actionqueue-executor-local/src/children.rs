//! Dispatch-time snapshot of child task states.
//!
//! [`ChildrenSnapshot`] is an immutable view of child task states captured by
//! the dispatch loop immediately before spawning a handler. Coordinator-pattern
//! handlers read this snapshot to determine which children are complete and what
//! steps to submit next.
//!
//! The snapshot is fully owned (no shared state, no locks), giving handlers a
//! consistent point-in-time view without any contention with the dispatch loop.

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::state::RunState;

/// Immutable snapshot of a single child task's run states.
#[derive(Debug, Clone)]
pub struct ChildState {
    task_id: TaskId,
    run_states: Vec<(RunId, RunState)>,
    all_terminal: bool,
}

impl ChildState {
    /// Creates a new child state entry.
    ///
    /// A child with an empty `run_states` vec (runs not yet derived) will have
    /// `all_terminal == false`. This is intentional: the snapshot reflects the
    /// true state — a child whose runs have not yet been derived is not
    /// complete. Callers checking [`ChildrenSnapshot::all_children_terminal`]
    /// will correctly treat such children as incomplete.
    pub fn new(task_id: TaskId, run_states: Vec<(RunId, RunState)>) -> Self {
        let all_terminal =
            !run_states.is_empty() && run_states.iter().all(|(_, s)| s.is_terminal());
        Self { task_id, run_states, all_terminal }
    }

    /// Returns the child task's identifier.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns all run instances for this child task, with their current states.
    pub fn run_states(&self) -> &[(RunId, RunState)] {
        &self.run_states
    }

    /// Returns whether ALL runs for this child are in terminal states.
    pub fn all_terminal(&self) -> bool {
        self.all_terminal
    }

    /// Returns `true` if this child has at least one run instance.
    pub fn has_runs(&self) -> bool {
        !self.run_states.is_empty()
    }
}

/// Immutable point-in-time snapshot of all child tasks' states.
///
/// Provided to Coordinator-pattern handlers so they can check progress
/// and decide what to do next without accessing shared mutable state.
#[derive(Debug, Clone, Default)]
pub struct ChildrenSnapshot {
    children: Vec<ChildState>,
}

impl ChildrenSnapshot {
    /// Creates a snapshot from a list of child states.
    pub fn new(children: Vec<ChildState>) -> Self {
        Self { children }
    }

    /// Returns all child states in this snapshot.
    pub fn children(&self) -> &[ChildState] {
        &self.children
    }

    /// Returns `true` if there are no children (empty snapshot).
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Returns the number of children in this snapshot.
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Returns `true` if all children have all runs in terminal states.
    pub fn all_children_terminal(&self) -> bool {
        !self.children.is_empty() && self.children.iter().all(|c| c.all_terminal())
    }

    /// Returns the child state for the given task, if present.
    pub fn get(&self, task_id: TaskId) -> Option<&ChildState> {
        self.children.iter().find(|c| c.task_id() == task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid() -> TaskId {
        TaskId::new()
    }

    fn rid() -> RunId {
        RunId::new()
    }

    #[test]
    fn empty_snapshot() {
        let snap = ChildrenSnapshot::default();
        assert!(snap.is_empty());
        assert!(!snap.all_children_terminal());
        assert!(snap.children().is_empty());
    }

    #[test]
    fn single_child_all_terminal() {
        let snap =
            ChildrenSnapshot::new(vec![ChildState::new(tid(), vec![(rid(), RunState::Completed)])]);
        assert!(!snap.is_empty());
        assert!(snap.all_children_terminal());
    }

    #[test]
    fn single_child_not_terminal() {
        let snap =
            ChildrenSnapshot::new(vec![ChildState::new(tid(), vec![(rid(), RunState::Running)])]);
        assert!(!snap.all_children_terminal());
    }

    #[test]
    fn multiple_children_mixed() {
        let snap = ChildrenSnapshot::new(vec![
            ChildState::new(tid(), vec![(rid(), RunState::Completed)]),
            ChildState::new(tid(), vec![(rid(), RunState::Running)]),
        ]);
        assert!(!snap.all_children_terminal());
    }

    #[test]
    fn all_children_terminal_mixed_terminal_states() {
        let snap = ChildrenSnapshot::new(vec![
            ChildState::new(tid(), vec![(rid(), RunState::Completed)]),
            ChildState::new(tid(), vec![(rid(), RunState::Failed)]),
        ]);
        assert!(snap.all_children_terminal());
    }

    #[test]
    fn get_found_and_not_found() {
        let known = tid();
        let snap =
            ChildrenSnapshot::new(vec![ChildState::new(known, vec![(rid(), RunState::Completed)])]);
        assert!(snap.get(known).is_some());
        assert!(snap.get(tid()).is_none());
    }

    #[test]
    fn child_with_no_runs_not_terminal() {
        let snap = ChildrenSnapshot::new(vec![ChildState::new(tid(), vec![])]);
        assert!(!snap.all_children_terminal());
        assert!(!snap.children()[0].all_terminal());
    }

    #[test]
    fn child_with_no_runs_reports_has_runs_false() {
        let child = ChildState::new(tid(), vec![]);
        assert!(!child.has_runs());

        let child_with_run = ChildState::new(tid(), vec![(rid(), RunState::Completed)]);
        assert!(child_with_run.has_runs());
    }
}
