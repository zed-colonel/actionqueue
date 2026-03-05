//! Pre-dispatch budget gate.
//!
//! The gate wraps the [`BudgetTracker`] and provides the eligibility check
//! used by the dispatch loop's step 7 (run selection). A task with any
//! exhausted budget dimension is blocked from dispatch.

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::TaskId;
use tracing;

use crate::tracker::BudgetTracker;

/// Pre-dispatch eligibility gate backed by the budget tracker.
///
/// All checks are O(dimensions) per task — linear in the number of budget
/// dimensions allocated to the task, typically very small (1-3).
#[derive(Debug)]
pub struct BudgetGate<'a> {
    tracker: &'a BudgetTracker,
}

impl<'a> BudgetGate<'a> {
    /// Creates a gate that borrows the given tracker.
    pub fn new(tracker: &'a BudgetTracker) -> Self {
        Self { tracker }
    }

    /// Returns `true` if no budget dimension is exhausted for this task.
    pub fn can_dispatch(&self, task_id: TaskId) -> bool {
        let allowed = !self.tracker.is_any_exhausted(task_id);
        if !allowed {
            tracing::debug!(%task_id, "dispatch blocked by exhausted budget");
        }
        allowed
    }

    /// Returns the (dimension, pct) pairs that have crossed the given threshold.
    ///
    /// Used by the dispatch loop to fire `BudgetThresholdCrossed` events.
    pub fn check_threshold(
        &self,
        task_id: TaskId,
        threshold_pct: u8,
    ) -> Vec<(BudgetDimension, u8)> {
        let dimensions =
            [BudgetDimension::Token, BudgetDimension::CostCents, BudgetDimension::TimeSecs];
        dimensions
            .into_iter()
            .filter_map(|dim| {
                let pct = self.tracker.threshold_pct(task_id, dim)?;
                if pct >= threshold_pct {
                    Some((dim, pct))
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::budget::BudgetDimension;
    use actionqueue_core::ids::TaskId;

    use super::BudgetGate;
    use crate::tracker::BudgetTracker;

    #[test]
    fn can_dispatch_when_no_budget_allocated() {
        let tracker = BudgetTracker::new();
        let gate = BudgetGate::new(&tracker);
        let task = TaskId::new();
        assert!(gate.can_dispatch(task));
    }

    #[test]
    fn can_dispatch_within_budget() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 1000);
        tracker.consume(task, BudgetDimension::Token, 500);
        let gate = BudgetGate::new(&tracker);
        assert!(gate.can_dispatch(task));
    }

    #[test]
    fn cannot_dispatch_when_exhausted() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.consume(task, BudgetDimension::Token, 100);
        let gate = BudgetGate::new(&tracker);
        assert!(!gate.can_dispatch(task));
    }

    #[test]
    fn check_threshold_at_zero_returns_all_allocated() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.allocate(task, BudgetDimension::CostCents, 50);
        let gate = BudgetGate::new(&tracker);
        // threshold_pct=0 means everything at or above 0% should be returned
        let crossing = gate.check_threshold(task, 0);
        assert_eq!(crossing.len(), 2);
    }

    #[test]
    fn check_threshold_at_100_only_returns_exhausted() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.consume(task, BudgetDimension::Token, 100);
        tracker.allocate(task, BudgetDimension::CostCents, 100);
        tracker.consume(task, BudgetDimension::CostCents, 50);
        let gate = BudgetGate::new(&tracker);
        let crossing = gate.check_threshold(task, 100);
        assert_eq!(crossing.len(), 1);
        assert_eq!(crossing[0].0, BudgetDimension::Token);
    }

    #[test]
    fn check_threshold_no_allocations_returns_empty() {
        let tracker = BudgetTracker::new();
        let gate = BudgetGate::new(&tracker);
        let task = TaskId::new();
        let crossing = gate.check_threshold(task, 50);
        assert!(crossing.is_empty());
    }

    #[test]
    fn check_threshold_multiple_dimensions_crossing() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.consume(task, BudgetDimension::Token, 90); // 90%
        tracker.allocate(task, BudgetDimension::CostCents, 200);
        tracker.consume(task, BudgetDimension::CostCents, 160); // 80%
        tracker.allocate(task, BudgetDimension::TimeSecs, 50);
        tracker.consume(task, BudgetDimension::TimeSecs, 20); // 40%
        let gate = BudgetGate::new(&tracker);
        let crossing = gate.check_threshold(task, 80);
        assert_eq!(crossing.len(), 2);
    }

    #[test]
    fn check_threshold_returns_crossing_dimensions() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.consume(task, BudgetDimension::Token, 85);
        let gate = BudgetGate::new(&tracker);
        let crossing = gate.check_threshold(task, 80);
        assert_eq!(crossing.len(), 1);
        assert_eq!(crossing[0].0, BudgetDimension::Token);
    }
}
