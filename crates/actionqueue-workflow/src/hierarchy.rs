//! Parent-child task lifecycle coupling.
//!
//! [`HierarchyTracker`] maintains the parent → children mapping and enforces
//! lifecycle rules:
//!
//! - **Cascading cancellation**: canceling a parent cascades to all non-terminal
//!   descendants via [`HierarchyTracker::collect_cancellation_cascade`].
//! - **Orphan prevention**: [`HierarchyTracker::register_child`] rejects children
//!   of tasks already marked terminal.
//! - **Depth limit**: configurable maximum nesting depth (default 8 levels).
//!
//! # Invariants
//!
//! - Tree structure is acyclic by construction (only append, no reparenting).
//! - Terminal state is tracked via [`HierarchyTracker::mark_terminal`], called
//!   by the dispatch loop after all runs for a task reach terminal state.
//! - At bootstrap, tree registration happens before terminal marking, so
//!   orphan prevention never fires during WAL replay.

use std::collections::{HashMap, HashSet, VecDeque};

use actionqueue_core::ids::TaskId;

/// Default maximum nesting depth for task hierarchies.
pub const DEFAULT_MAX_DEPTH: usize = 8;

/// Error returned when registering a parent-child relationship fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HierarchyError {
    /// Parent task has already reached a terminal state; no new children allowed.
    OrphanPrevention {
        /// The child task that was rejected.
        child: TaskId,
        /// The terminal parent task.
        parent: TaskId,
    },
    /// Adding this child would exceed the configured maximum nesting depth.
    DepthLimitExceeded {
        /// The child task that was rejected.
        child: TaskId,
        /// The parent task that would have hosted the child.
        parent: TaskId,
        /// The depth the child would have reached.
        depth: usize,
        /// The configured maximum allowed depth.
        limit: usize,
    },
}

impl std::fmt::Display for HierarchyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HierarchyError::OrphanPrevention { child, parent } => {
                write!(
                    f,
                    "cannot register child {child} under terminal parent {parent} (orphan \
                     prevention)"
                )
            }
            HierarchyError::DepthLimitExceeded { child, parent, depth, limit } => {
                write!(
                    f,
                    "registering child {child} under {parent} would reach depth {depth} (limit: \
                     {limit})"
                )
            }
        }
    }
}

impl std::error::Error for HierarchyError {}

/// Tracks parent-child task relationships and enforces lifecycle coupling.
///
/// The tracker is **not** persisted directly. At bootstrap it is reconstructed
/// from the `parent_task_id` field present on each `TaskSpec` in the WAL, then
/// terminal status is inferred from run states in the projection.
///
/// The dispatch loop calls [`mark_terminal`][HierarchyTracker::mark_terminal]
/// whenever a task's runs all reach terminal state, keeping orphan prevention
/// accurate throughout the engine lifetime.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HierarchyTracker {
    /// parent_task_id → set of direct child task_ids.
    children: HashMap<TaskId, HashSet<TaskId>>,
    /// child_task_id → parent_task_id.
    parents: HashMap<TaskId, TaskId>,
    /// task_ids that have reached terminal state.
    terminal_tasks: HashSet<TaskId>,
    /// Maximum allowed nesting depth (root = 0).
    max_depth: usize,
}

impl HierarchyTracker {
    /// Creates a new tracker with the default maximum depth (8).
    pub fn new() -> Self {
        Self::with_max_depth(DEFAULT_MAX_DEPTH)
    }

    /// Creates a new tracker with the given maximum nesting depth.
    ///
    /// A root task is at depth 0. A direct child is at depth 1.
    /// `max_depth = 8` allows up to depth 8 (nine levels total).
    pub fn with_max_depth(max_depth: usize) -> Self {
        Self {
            children: HashMap::new(),
            parents: HashMap::new(),
            terminal_tasks: HashSet::new(),
            max_depth,
        }
    }

    /// Registers a parent-child relationship.
    ///
    /// # Errors
    ///
    /// - [`HierarchyError::OrphanPrevention`] if the parent is already terminal.
    /// - [`HierarchyError::DepthLimitExceeded`] if adding this child would
    ///   exceed the configured maximum nesting depth.
    pub fn register_child(&mut self, parent: TaskId, child: TaskId) -> Result<(), HierarchyError> {
        if self.terminal_tasks.contains(&parent) {
            return Err(HierarchyError::OrphanPrevention { child, parent });
        }

        let child_depth = self.depth(parent) + 1;
        if child_depth > self.max_depth {
            return Err(HierarchyError::DepthLimitExceeded {
                child,
                parent,
                depth: child_depth,
                limit: self.max_depth,
            });
        }

        self.parents.insert(child, parent);
        self.children.entry(parent).or_default().insert(child);
        Ok(())
    }

    /// Returns an iterator over the direct children of `parent`.
    pub fn children_of(&self, parent: TaskId) -> impl Iterator<Item = TaskId> + '_ {
        self.children.get(&parent).into_iter().flat_map(|s| s.iter().copied())
    }

    /// Returns the parent of `child`, if any.
    pub fn parent_of(&self, child: TaskId) -> Option<TaskId> {
        self.parents.get(&child).copied()
    }

    /// Returns `true` if `task_id` has any registered children.
    #[must_use]
    pub fn has_children(&self, task_id: TaskId) -> bool {
        self.children.contains_key(&task_id)
    }

    /// Returns `true` if `task_id` has been marked terminal.
    #[must_use]
    pub fn is_terminal(&self, task_id: TaskId) -> bool {
        self.terminal_tasks.contains(&task_id)
    }

    /// Marks `task_id` as having reached a terminal state.
    ///
    /// Called by the dispatch loop when all runs for a task become terminal.
    /// Once marked terminal, the task cannot accept new children.
    pub fn mark_terminal(&mut self, task_id: TaskId) {
        self.terminal_tasks.insert(task_id);
    }

    /// Returns the nesting depth of `task_id` (0 = root, no parent).
    ///
    /// Walks up the parent chain; O(depth) time. With `max_depth` capped at 8,
    /// this is effectively constant-time and caching is unnecessary.
    pub fn depth(&self, task_id: TaskId) -> usize {
        let mut depth = 0usize;
        let mut current = task_id;
        while let Some(&parent) = self.parents.get(&current) {
            depth += 1;
            current = parent;
        }
        depth
    }

    /// Removes all tracker state for a fully-terminal task and its descendants.
    ///
    /// **Safety constraint:** Only call when `task_id` AND all its descendants
    /// are terminal (i.e., `collect_cancellation_cascade(task_id)` returns empty).
    /// Violating this precondition can remove entries still needed for cascade
    /// protection or orphan prevention of in-flight tasks.
    ///
    /// Called by the dispatch loop after the hierarchy cascade for `task_id`
    /// is fully quenched (all descendants terminal).
    pub fn gc_subtree(&mut self, task_id: TaskId) {
        // Collect all task_ids in the subtree (task_id + all descendants).
        let mut to_remove = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(task_id);

        while let Some(current) = queue.pop_front() {
            to_remove.push(current);
            if let Some(children) = self.children.get(&current) {
                for &child in children {
                    queue.push_back(child);
                }
            }
        }

        for id in to_remove {
            self.children.remove(&id);
            self.parents.remove(&id);
            self.terminal_tasks.remove(&id);
        }
    }

    /// Collects all non-terminal descendants of `task_id` for cascading cancellation.
    ///
    /// Traverses the subtree in **breadth-first order** (BFS) starting from
    /// `task_id`'s direct children. Results are returned in BFS visitation
    /// order. Descendants already marked terminal
    /// (via [`mark_terminal`][HierarchyTracker::mark_terminal]) are excluded
    /// from the result, as they require no further action.
    ///
    /// Returns an empty `Vec` when `task_id` has no children or all descendants
    /// are already terminal (self-quenching: repeated calls are safe and cheap).
    pub fn collect_cancellation_cascade(&self, task_id: TaskId) -> Vec<TaskId> {
        let mut result = Vec::new();
        let mut queue = VecDeque::new();

        // Seed the queue with direct children.
        if let Some(children) = self.children.get(&task_id) {
            for &child in children {
                queue.push_back(child);
            }
        }

        while let Some(current) = queue.pop_front() {
            if self.terminal_tasks.contains(&current) {
                // Terminal descendants need no cascading; still descend their children
                // because a deeper non-terminal descendant may still need canceling.
                // (A terminal descendant may itself have had non-terminal children
                // registered before it went terminal — those children are still live.)
            } else {
                result.push(current);
            }

            // Always traverse children regardless of terminal status (to reach
            // deeper non-terminal descendants through terminal intermediaries).
            if let Some(children) = self.children.get(&current) {
                for &child in children {
                    queue.push_back(child);
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;

    use super::*;

    fn tid(n: u128) -> TaskId {
        TaskId::from_uuid(uuid::Uuid::from_u128(n))
    }

    #[test]
    fn new_tracker_has_no_children() {
        let tracker = HierarchyTracker::new();
        assert!(!tracker.has_children(tid(1)));
        assert!(tracker.parent_of(tid(2)).is_none());
        assert_eq!(tracker.depth(tid(1)), 0);
    }

    #[test]
    fn register_child_records_relationship() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        assert!(tracker.has_children(tid(1)));
        assert_eq!(tracker.parent_of(tid(2)), Some(tid(1)));
        assert_eq!(tracker.depth(tid(2)), 1);
    }

    #[test]
    fn depth_increases_down_chain() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.register_child(tid(2), tid(3)).expect("no error");
        tracker.register_child(tid(3), tid(4)).expect("no error");
        assert_eq!(tracker.depth(tid(1)), 0);
        assert_eq!(tracker.depth(tid(2)), 1);
        assert_eq!(tracker.depth(tid(3)), 2);
        assert_eq!(tracker.depth(tid(4)), 3);
    }

    #[test]
    fn orphan_prevention_rejects_child_of_terminal() {
        let mut tracker = HierarchyTracker::new();
        tracker.mark_terminal(tid(1));
        let err = tracker.register_child(tid(1), tid(2)).expect_err("should fail");
        assert!(matches!(err, HierarchyError::OrphanPrevention { child, parent }
            if child == tid(2) && parent == tid(1)));
    }

    #[test]
    fn depth_limit_rejects_excessive_nesting() {
        let mut tracker = HierarchyTracker::with_max_depth(2);
        tracker.register_child(tid(1), tid(2)).expect("depth 1 OK");
        tracker.register_child(tid(2), tid(3)).expect("depth 2 OK");
        let err = tracker.register_child(tid(3), tid(4)).expect_err("depth 3 exceeds limit 2");
        assert!(matches!(err, HierarchyError::DepthLimitExceeded { depth: 3, limit: 2, .. }));
    }

    #[test]
    fn cascade_returns_non_terminal_descendants() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.register_child(tid(1), tid(3)).expect("no error");
        tracker.register_child(tid(2), tid(4)).expect("no error");

        let cascade = tracker.collect_cancellation_cascade(tid(1));
        assert!(cascade.contains(&tid(2)));
        assert!(cascade.contains(&tid(3)));
        assert!(cascade.contains(&tid(4)));
        assert_eq!(cascade.len(), 3);
    }

    #[test]
    fn cascade_excludes_terminal_descendants() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.register_child(tid(1), tid(3)).expect("no error");
        tracker.mark_terminal(tid(2));

        let cascade = tracker.collect_cancellation_cascade(tid(1));
        assert!(!cascade.contains(&tid(2)), "tid(2) is terminal, excluded");
        assert!(cascade.contains(&tid(3)));
        assert_eq!(cascade.len(), 1);
    }

    #[test]
    fn cascade_traverses_through_terminal_intermediary() {
        // Even if a mid-chain task is terminal, its non-terminal children
        // should still be collected (they are still live descendants).
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.register_child(tid(2), tid(3)).expect("no error");
        tracker.mark_terminal(tid(2));

        let cascade = tracker.collect_cancellation_cascade(tid(1));
        assert!(!cascade.contains(&tid(2)), "terminal");
        assert!(cascade.contains(&tid(3)), "tid(3) is non-terminal leaf under terminal tid(2)");
    }

    #[test]
    fn cascade_returns_empty_for_leaf_task() {
        let tracker = HierarchyTracker::new();
        assert!(tracker.collect_cancellation_cascade(tid(99)).is_empty());
    }

    #[test]
    fn cascade_self_quenches_after_mark_terminal() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");

        let first = tracker.collect_cancellation_cascade(tid(1));
        assert_eq!(first, vec![tid(2)]);

        tracker.mark_terminal(tid(2));
        let second = tracker.collect_cancellation_cascade(tid(1));
        assert!(second.is_empty(), "self-quenches after mark_terminal");
    }

    #[test]
    fn bootstrap_registration_succeeds_with_empty_terminal_set() {
        // During bootstrap, terminal_tasks is empty, so all register_child calls
        // succeed regardless of actual terminal state — matching WAL replay semantics.
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error during bootstrap");
        // After registering, populate terminal status
        tracker.mark_terminal(tid(1));
        tracker.mark_terminal(tid(2));
        // Now orphan prevention works for new submissions
        tracker.register_child(tid(1), tid(3)).expect_err("parent is terminal");
    }

    #[test]
    fn gc_subtree_removes_terminal_subtree() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.register_child(tid(2), tid(3)).expect("no error");
        tracker.mark_terminal(tid(1));
        tracker.mark_terminal(tid(2));
        tracker.mark_terminal(tid(3));

        // Cascade is quenched (all terminal).
        assert!(tracker.collect_cancellation_cascade(tid(1)).is_empty());

        tracker.gc_subtree(tid(1));

        // All tracker state removed.
        assert!(!tracker.has_children(tid(1)));
        assert!(!tracker.has_children(tid(2)));
        assert!(tracker.parent_of(tid(2)).is_none());
        assert!(tracker.parent_of(tid(3)).is_none());
        assert!(!tracker.is_terminal(tid(1)));
        assert!(!tracker.is_terminal(tid(2)));
        assert!(!tracker.is_terminal(tid(3)));
    }

    #[test]
    fn gc_subtree_is_idempotent() {
        let mut tracker = HierarchyTracker::new();
        tracker.register_child(tid(1), tid(2)).expect("no error");
        tracker.mark_terminal(tid(1));
        tracker.mark_terminal(tid(2));

        tracker.gc_subtree(tid(1));
        tracker.gc_subtree(tid(1)); // second call must not panic
    }

    #[test]
    fn max_depth_default_is_eight() {
        let tracker = HierarchyTracker::new();
        assert_eq!(tracker.max_depth, DEFAULT_MAX_DEPTH);
    }
}
