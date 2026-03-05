//! Task dependency graph and satisfaction tracking.
//!
//! [`DependencyGate`] tracks task-to-task dependencies and gates run promotion.
//! A task with unmet prerequisites stays in `Scheduled` state even when its
//! `scheduled_at` has elapsed. When all prerequisites reach terminal success,
//! the gate marks the task eligible for promotion.
//!
//! # Invariants
//!
//! - Circular dependencies are rejected at declaration time (DFS cycle check).
//! - Dependency satisfaction is deterministic: the gate state is reconstructible
//!   from `DependencyDeclared` WAL events + run terminal states replayed via the
//!   recovery reducer. Satisfaction and failure are ephemeral projections, not
//!   independent WAL events.
//! - A failed prerequisite cascades failure to all direct and transitive dependents.

use std::collections::{HashMap, HashSet, VecDeque};

use actionqueue_core::ids::TaskId;

/// Error when a circular dependency is detected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleError {
    task_id: TaskId,
    cycle_through: TaskId,
}

impl CycleError {
    pub(crate) fn new(task_id: TaskId, cycle_through: TaskId) -> Self {
        Self { task_id, cycle_through }
    }

    /// The task whose declaration would introduce a cycle.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// The prerequisite that would close the cycle.
    pub fn cycle_through(&self) -> TaskId {
        self.cycle_through
    }
}

impl std::fmt::Display for CycleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.task_id == self.cycle_through {
            write!(f, "task {} cannot depend on itself", self.task_id)
        } else {
            write!(
                f,
                "declaring dependency from {} on {} would introduce a cycle ({} is already \
                 reachable from {})",
                self.task_id, self.cycle_through, self.task_id, self.cycle_through
            )
        }
    }
}

impl std::error::Error for CycleError {}

/// Gates task promotion based on declared inter-task dependencies.
///
/// Tasks with unsatisfied prerequisites are blocked from Scheduled → Ready
/// promotion in the dispatch loop. The gate is notified when tasks complete
/// or fail, updating eligibility accordingly.
///
/// The gate is **not** persisted directly — it is reconstructed at bootstrap
/// from WAL events stored in the recovery reducer.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DependencyGate {
    /// task_id → set of prerequisite task_ids that must complete first.
    ///
    /// Only contains tasks with declared dependencies (not all tasks).
    prerequisites: HashMap<TaskId, HashSet<TaskId>>,

    /// task_ids all of whose prerequisites have reached terminal success.
    satisfied: HashSet<TaskId>,

    /// task_ids blocked because at least one prerequisite failed/canceled.
    failed: HashSet<TaskId>,
}

impl DependencyGate {
    /// Creates an empty gate (no dependencies, all tasks implicitly eligible).
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares that `task_id` depends on all tasks in `depends_on`.
    ///
    /// # Errors
    ///
    /// Returns [`CycleError`] if adding this dependency would create a cycle
    /// in the dependency graph.
    pub fn declare(&mut self, task_id: TaskId, depends_on: Vec<TaskId>) -> Result<(), CycleError> {
        // Cycle detection: for each new prerequisite, check whether `task_id`
        // is reachable from the prerequisite (i.e., the prereq already depends
        // on `task_id` directly or transitively). If so, adding this edge
        // would close a cycle.
        for &prereq in &depends_on {
            if self.is_reachable_from(prereq, task_id) {
                return Err(CycleError::new(task_id, prereq));
            }
        }

        let entry = self.prerequisites.entry(task_id).or_default();
        for prereq in depends_on {
            entry.insert(prereq);
        }

        // Re-evaluate satisfaction now that prerequisites have changed.
        self.recompute_satisfaction(task_id);
        Ok(())
    }

    /// Checks whether declaring these dependencies would create a cycle,
    /// without modifying the gate.
    ///
    /// Use this to pre-validate before a WAL append, so that cycles are
    /// rejected before being durably persisted.
    ///
    /// # Errors
    ///
    /// Returns [`CycleError`] if any prerequisite in `depends_on` can already
    /// reach `task_id` through existing edges (i.e., adding these edges would
    /// close a cycle).
    pub fn check_cycle(&self, task_id: TaskId, depends_on: &[TaskId]) -> Result<(), CycleError> {
        for &prereq in depends_on {
            if self.is_reachable_from(prereq, task_id) {
                return Err(CycleError::new(task_id, prereq));
            }
        }
        Ok(())
    }

    /// Returns `true` if `task_id` may be promoted to Ready.
    ///
    /// A task is eligible when it has no declared dependencies, OR when all
    /// its prerequisites have been satisfied. Tasks with failed prerequisites
    /// are NOT eligible (they will be canceled by the dispatch loop instead).
    #[must_use]
    pub fn is_eligible(&self, task_id: TaskId) -> bool {
        !self.failed.contains(&task_id)
            && match self.prerequisites.get(&task_id) {
                None => true,                                 // No dependencies declared
                Some(_) => self.satisfied.contains(&task_id), // All satisfied
            }
    }

    /// Returns `true` if `task_id`'s prerequisites have permanently failed.
    ///
    /// When this returns `true`, the dispatch loop should cancel the task's
    /// non-terminal runs rather than waiting for prerequisites to be satisfied.
    #[must_use]
    pub fn is_dependency_failed(&self, task_id: TaskId) -> bool {
        self.failed.contains(&task_id)
    }

    /// Returns `true` if `task_id` has any declared prerequisites.
    #[must_use]
    pub fn has_prerequisites(&self, task_id: TaskId) -> bool {
        self.prerequisites.contains_key(&task_id)
    }

    /// Returns all task_ids that are waiting for their prerequisites to be met.
    ///
    /// Excludes tasks whose prerequisites have all been satisfied and tasks
    /// whose prerequisites have failed (those are in the `failed` set).
    pub fn waiting_task_ids(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.prerequisites
            .keys()
            .copied()
            .filter(move |&id| !self.satisfied.contains(&id) && !self.failed.contains(&id))
    }

    /// Called when a prerequisite task has reached terminal success.
    ///
    /// Returns the list of task_ids that became newly eligible as a result
    /// (all prerequisites now satisfied).
    #[must_use]
    pub fn notify_completed(&mut self, completed_task_id: TaskId) -> Vec<TaskId> {
        // Mark the completed task as satisfied (it is now eligible as a prerequisite).
        self.satisfied.insert(completed_task_id);

        // Recompute satisfaction for all tasks that declare this task as a prerequisite.
        let dependents: Vec<TaskId> = self
            .prerequisites
            .iter()
            .filter(|(_, prereqs)| prereqs.contains(&completed_task_id))
            .map(|(task_id, _)| *task_id)
            .collect();

        let mut newly_eligible = Vec::new();
        for dep in dependents {
            let was_eligible = self.is_eligible(dep);
            self.recompute_satisfaction(dep);
            if !was_eligible && self.is_eligible(dep) {
                newly_eligible.push(dep);
            }
        }
        newly_eligible
    }

    /// Called when a prerequisite task has permanently failed (all runs
    /// reached Failed or Canceled, with no successful completion).
    ///
    /// Returns the list of task_ids that are now permanently blocked
    /// (their prerequisites will never succeed).
    #[must_use]
    pub fn notify_failed(&mut self, failed_task_id: TaskId) -> Vec<TaskId> {
        // Early exit if this task was already processed as failed.
        if self.failed.contains(&failed_task_id) {
            return Vec::new();
        }

        // Mark the root as failed for idempotency on repeated calls.
        // The root itself is NOT added to newly_blocked — it is the
        // prerequisite that failed, not a dependent that became blocked.
        self.failed.insert(failed_task_id);

        let mut newly_blocked = Vec::new();
        let mut queue: VecDeque<TaskId> = VecDeque::new();
        queue.push_back(failed_task_id);

        while let Some(failed_id) = queue.pop_front() {
            // Find all tasks that depend on this failed task.
            let dependents: Vec<TaskId> = self
                .prerequisites
                .iter()
                .filter(|(_, prereqs)| prereqs.contains(&failed_id))
                .map(|(task_id, _)| *task_id)
                .filter(|task_id| !self.failed.contains(task_id))
                .collect();

            for dep in dependents {
                self.failed.insert(dep);
                self.satisfied.remove(&dep);
                newly_blocked.push(dep);
                // Cascade: dependents of the newly-failed task also fail.
                queue.push_back(dep);
            }
        }

        newly_blocked
    }

    /// Cascades the `failed` set to all transitive dependents.
    ///
    /// After bootstrap populates `force_fail` for directly-failed
    /// prerequisites, this method BFS-cascades to ensure all reachable
    /// dependents are also marked as failed.
    ///
    /// Returns the list of newly-failed (dependent) task IDs.
    #[must_use]
    pub fn propagate_failures(&mut self) -> Vec<TaskId> {
        let seeds: Vec<TaskId> = self.failed.iter().copied().collect();
        let mut newly_blocked = Vec::new();
        let mut queue: VecDeque<TaskId> = seeds.into_iter().collect();
        while let Some(failed_id) = queue.pop_front() {
            let dependents: Vec<TaskId> = self
                .prerequisites
                .iter()
                .filter(|(_, prereqs)| prereqs.contains(&failed_id))
                .map(|(task_id, _)| *task_id)
                .filter(|task_id| !self.failed.contains(task_id))
                .collect();
            for dep in dependents {
                self.failed.insert(dep);
                self.satisfied.remove(&dep);
                newly_blocked.push(dep);
                queue.push_back(dep);
            }
        }
        newly_blocked
    }

    /// Directly marks a task as satisfied (used during gate reconstruction
    /// from WAL events at bootstrap — bypasses cycle check since declarations
    /// are already validated).
    pub fn force_satisfy(&mut self, task_id: TaskId) {
        self.satisfied.insert(task_id);
        self.failed.remove(&task_id);
    }

    /// Directly marks a task as failed (used during gate reconstruction).
    pub fn force_fail(&mut self, task_id: TaskId) {
        self.failed.insert(task_id);
        self.satisfied.remove(&task_id);
    }

    /// Removes all gate state for a fully-terminal task.
    ///
    /// Called by the dispatch loop after a task reaches full terminal state and
    /// all dependency notifications have been issued. Safe to call because:
    /// - If the task completed (`satisfied`), all dependents already received
    ///   `notify_completed` and their eligibility was recomputed.
    /// - If the task failed (`failed`), all dependents already received
    ///   `notify_failed` and cascades are complete.
    /// - Removing a satisfied prerequisite from a dependent's set is safe;
    ///   the remaining prerequisites still gate eligibility correctly.
    pub fn gc_task(&mut self, task_id: TaskId) {
        // Remove from direct state sets.
        self.prerequisites.remove(&task_id);
        self.satisfied.remove(&task_id);
        self.failed.remove(&task_id);

        // Remove task_id from other tasks' prerequisite sets.
        for prereqs in self.prerequisites.values_mut() {
            prereqs.remove(&task_id);
        }
    }

    /// Re-evaluates satisfaction for `task_id` using the current `satisfied` set.
    ///
    /// Called by the dispatch loop after restoring prerequisite satisfaction state
    /// from the projection (e.g., when declaring a dependency after a prerequisite
    /// completed and was GC'd from the `satisfied` set).
    pub fn recompute_satisfaction_pub(&mut self, task_id: TaskId) {
        self.recompute_satisfaction(task_id);
    }

    // ── Private helpers ──────────────────────────────────────────────────

    /// BFS check: can we reach `target` starting from `start`?
    fn is_reachable_from(&self, start: TaskId, target: TaskId) -> bool {
        if start == target {
            return true;
        }
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            if current == target {
                return true;
            }
            if !visited.insert(current) {
                continue;
            }
            if let Some(prereqs) = self.prerequisites.get(&current) {
                for &prereq in prereqs {
                    if !visited.contains(&prereq) {
                        queue.push_back(prereq);
                    }
                }
            }
        }
        false
    }

    /// Recomputes whether `task_id` has all prerequisites satisfied and
    /// updates the `satisfied` set for `task_id` itself (not for leaf tasks).
    ///
    /// A task with declared prerequisites is "satisfied" (eligible as a future
    /// prerequisite) when ALL its own prerequisites are in the `satisfied` set.
    /// Leaf tasks (no declared prerequisites) are implicitly satisfied when they
    /// complete — they are added to `satisfied` directly by `notify_completed`.
    fn recompute_satisfaction(&mut self, task_id: TaskId) {
        if self.failed.contains(&task_id) {
            return; // Already permanently blocked.
        }
        let Some(prereqs) = self.prerequisites.get(&task_id) else {
            return; // No declared prerequisites — eligibility decided by `is_eligible`.
        };
        let prereqs: Vec<TaskId> = prereqs.iter().copied().collect();

        // A task's prerequisites are all met only if every prereq is in the
        // `satisfied` set (meaning it has completed successfully).
        let all_satisfied = prereqs.iter().all(|prereq| self.satisfied.contains(prereq));

        if all_satisfied {
            self.satisfied.insert(task_id);
        } else {
            self.satisfied.remove(&task_id);
        }
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
    fn no_dependencies_is_always_eligible() {
        let gate = DependencyGate::new();
        assert!(gate.is_eligible(tid(1)));
        assert!(!gate.has_prerequisites(tid(1)));
    }

    #[test]
    fn declared_dependency_blocks_until_completed() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        assert!(!gate.is_eligible(tid(2)));

        let newly_eligible = gate.notify_completed(tid(1));
        assert_eq!(newly_eligible, vec![tid(2)]);
        assert!(gate.is_eligible(tid(2)));
    }

    #[test]
    fn failed_prerequisite_blocks_dependent() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");

        let blocked = gate.notify_failed(tid(1));
        assert_eq!(blocked, vec![tid(2)]);
        assert!(!gate.is_eligible(tid(2)));
        assert!(gate.is_dependency_failed(tid(2)));
    }

    #[test]
    fn cascading_failure_through_chain() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        gate.declare(tid(3), vec![tid(2)]).expect("no cycle");

        let blocked = gate.notify_failed(tid(1));
        assert!(blocked.contains(&tid(2)));
        assert!(blocked.contains(&tid(3)));
        assert!(gate.is_dependency_failed(tid(3)));
    }

    #[test]
    fn cycle_detection_rejects_direct_cycle() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        let err = gate.declare(tid(1), vec![tid(2)]).expect_err("cycle should be detected");
        assert_eq!(err.task_id(), tid(1));
    }

    #[test]
    fn force_satisfy_and_fail_for_bootstrap() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        assert!(!gate.is_eligible(tid(2)));

        gate.force_satisfy(tid(2));
        assert!(gate.is_eligible(tid(2)));
    }

    #[test]
    fn check_cycle_detects_cycle_without_mutation() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        let snapshot = gate.clone();

        // check_cycle detects the cycle...
        let err = gate.check_cycle(tid(1), &[tid(2)]).expect_err("cycle");
        assert_eq!(err.task_id(), tid(1));
        assert_eq!(err.cycle_through(), tid(2));

        // ...but the gate is unchanged.
        assert_eq!(gate, snapshot);
    }

    #[test]
    fn self_loop_display_message() {
        let err = CycleError::new(tid(1), tid(1));
        let msg = err.to_string();
        assert!(
            msg.contains("cannot depend on itself"),
            "self-loop error should say 'cannot depend on itself', got: {msg}"
        );
    }

    #[test]
    fn notify_failed_idempotent_on_second_call() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");

        let first = gate.notify_failed(tid(1));
        assert_eq!(first.len(), 1);

        // Second call for same task returns empty (already processed).
        let second = gate.notify_failed(tid(1));
        assert!(second.is_empty(), "second notify_failed must return empty");
    }

    #[test]
    fn propagate_failures_cascades_transitive_chain() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        gate.declare(tid(3), vec![tid(2)]).expect("no cycle");

        // force_fail only marks tid(1), does NOT cascade.
        gate.force_fail(tid(1));
        assert!(gate.is_dependency_failed(tid(1)));
        assert!(!gate.is_dependency_failed(tid(2)), "before propagate, tid(2) not failed");
        assert!(!gate.is_dependency_failed(tid(3)), "before propagate, tid(3) not failed");

        // propagate_failures cascades to transitive dependents.
        let newly_blocked = gate.propagate_failures();
        assert!(newly_blocked.contains(&tid(2)));
        assert!(newly_blocked.contains(&tid(3)));
        assert!(gate.is_dependency_failed(tid(2)));
        assert!(gate.is_dependency_failed(tid(3)));
    }

    #[test]
    fn notify_failed_idempotent_guard_fires_on_root() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");

        let first = gate.notify_failed(tid(1));
        assert_eq!(first.len(), 1);
        assert!(gate.is_dependency_failed(tid(1)), "root should be in failed set");

        // Second call returns empty via the early-exit guard on the root.
        let second = gate.notify_failed(tid(1));
        assert!(second.is_empty(), "second call must return empty via guard");
    }

    #[test]
    fn gc_task_removes_completed_prerequisite() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        let _ = gate.notify_completed(tid(1));
        assert!(gate.is_eligible(tid(2)));

        gate.gc_task(tid(1));

        // tid(2) prerequisites are now empty (tid(1) removed), still eligible.
        assert!(gate.is_eligible(tid(2)));
        // tid(1) no longer tracked anywhere.
        assert!(!gate.has_prerequisites(tid(1)));
    }

    #[test]
    fn gc_task_removes_failed_prerequisite() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        let _ = gate.notify_failed(tid(1));
        assert!(gate.is_dependency_failed(tid(2)));

        gate.gc_task(tid(1));

        // tid(2) is still in failed set (gc_task only removes the root).
        assert!(gate.is_dependency_failed(tid(2)));
    }

    #[test]
    fn gc_task_is_idempotent() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(2), vec![tid(1)]).expect("no cycle");
        let _ = gate.notify_completed(tid(1));

        gate.gc_task(tid(1));
        gate.gc_task(tid(1)); // second call must not panic
        assert!(gate.is_eligible(tid(2)));
    }

    #[test]
    fn multiple_prerequisites_require_all_satisfied() {
        let mut gate = DependencyGate::new();
        gate.declare(tid(3), vec![tid(1), tid(2)]).expect("no cycle");

        let r1 = gate.notify_completed(tid(1));
        assert!(r1.is_empty(), "tid(3) still blocked on tid(2)");
        assert!(!gate.is_eligible(tid(3)));

        let r2 = gate.notify_completed(tid(2));
        assert_eq!(r2, vec![tid(3)]);
        assert!(gate.is_eligible(tid(3)));
    }
}
