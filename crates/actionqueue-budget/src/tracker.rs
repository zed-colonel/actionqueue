//! Per-task budget state tracking.
//!
//! Tracks in-memory budget state derived from durable WAL events. The tracker
//! is the authoritative in-memory view of consumption against allocations.
//! The dispatch loop consults the tracker to gate dispatch and determine when
//! to signal suspension.

use std::collections::HashMap;

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::TaskId;
use tracing;

/// In-memory state for one (task, dimension) budget entry.
#[derive(Debug, Clone)]
pub struct BudgetState {
    /// Maximum consumption allowed before dispatch is blocked.
    pub limit: u64,
    /// Total consumption recorded so far.
    pub consumed: u64,
    /// True once consumption has reached or exceeded the limit.
    pub exhausted: bool,
}

impl BudgetState {
    fn new(limit: u64) -> Self {
        Self { limit, consumed: 0, exhausted: false }
    }
}

/// Result of a consume operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumeResult {
    /// Consumption recorded; budget has not been exhausted.
    WithinBudget,
    /// Consumption pushed total past the limit.
    Exhausted {
        /// Amount consumed beyond the limit.
        overage: u64,
    },
}

/// In-memory per-task budget tracker.
///
/// Reconstructed from WAL events at bootstrap via [`BudgetTracker::allocate`]
/// and [`BudgetTracker::consume`] calls. The dispatch loop updates the tracker
/// each tick as WorkerResults arrive.
#[derive(Debug, Default)]
pub struct BudgetTracker {
    budgets: HashMap<(TaskId, BudgetDimension), BudgetState>,
}

impl BudgetTracker {
    /// Creates an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a budget allocation for a task/dimension pair.
    ///
    /// If an allocation already exists for this pair, it is replaced (re-allocation).
    pub fn allocate(&mut self, task_id: TaskId, dimension: BudgetDimension, limit: u64) {
        tracing::debug!(%task_id, %dimension, limit, "budget allocated");
        self.budgets.insert((task_id, dimension), BudgetState::new(limit));
    }

    /// Records consumption and returns whether the budget was exhausted by this call.
    ///
    /// No-ops if no allocation exists for the (task, dimension) pair.
    pub fn consume(
        &mut self,
        task_id: TaskId,
        dimension: BudgetDimension,
        amount: u64,
    ) -> ConsumeResult {
        let Some(state) = self.budgets.get_mut(&(task_id, dimension)) else {
            return ConsumeResult::WithinBudget;
        };

        tracing::debug!(%task_id, %dimension, amount, "budget consumption recorded");
        state.consumed = state.consumed.saturating_add(amount);
        if state.consumed >= state.limit {
            let overage = state.consumed.saturating_sub(state.limit);
            state.exhausted = true;
            tracing::warn!(
                %task_id, %dimension,
                consumed = state.consumed, limit = state.limit,
                "budget exhausted"
            );
            ConsumeResult::Exhausted { overage }
        } else {
            ConsumeResult::WithinBudget
        }
    }

    /// Replenishes a budget: sets new limit and clears the exhausted flag.
    pub fn replenish(&mut self, task_id: TaskId, dimension: BudgetDimension, new_limit: u64) {
        if let Some(state) = self.budgets.get_mut(&(task_id, dimension)) {
            tracing::debug!(%task_id, %dimension, new_limit, "budget replenished");
            state.limit = new_limit;
            state.consumed = 0;
            state.exhausted = false;
        }
    }

    /// Returns true if the given dimension budget is exhausted for this task.
    pub fn is_exhausted(&self, task_id: TaskId, dimension: BudgetDimension) -> bool {
        self.budgets.get(&(task_id, dimension)).is_some_and(|s| s.exhausted)
    }

    /// Returns true if ANY dimension budget is exhausted for this task.
    pub fn is_any_exhausted(&self, task_id: TaskId) -> bool {
        [BudgetDimension::Token, BudgetDimension::CostCents, BudgetDimension::TimeSecs]
            .iter()
            .any(|&dim| self.is_exhausted(task_id, dim))
    }

    /// Returns the remaining budget for a (task, dimension) pair.
    ///
    /// Returns `None` if no allocation exists.
    pub fn remaining(&self, task_id: TaskId, dimension: BudgetDimension) -> Option<u64> {
        self.budgets.get(&(task_id, dimension)).map(|s| s.limit.saturating_sub(s.consumed))
    }

    /// Returns consumed amount as a percentage of the limit (0-100).
    ///
    /// Returns `None` if no allocation exists.
    pub fn threshold_pct(&self, task_id: TaskId, dimension: BudgetDimension) -> Option<u8> {
        self.budgets.get(&(task_id, dimension)).map(|s| {
            if s.limit == 0 {
                100
            } else {
                ((s.consumed.min(s.limit) * 100) / s.limit) as u8
            }
        })
    }

    /// Returns the state for a (task, dimension) pair.
    pub fn get(&self, task_id: TaskId, dimension: BudgetDimension) -> Option<&BudgetState> {
        self.budgets.get(&(task_id, dimension))
    }

    /// Removes all budget state for a fully-terminal task.
    ///
    /// Called by the dispatch loop after a task reaches terminal state.
    /// Safe to call because terminal tasks no longer dispatch runs that
    /// would consume budget or check exhaustion.
    pub fn gc_task(&mut self, task_id: TaskId) {
        self.budgets.retain(|&(tid, _), _| tid != task_id);
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::budget::BudgetDimension;
    use actionqueue_core::ids::TaskId;

    use super::{BudgetTracker, ConsumeResult};

    #[test]
    fn allocate_and_consume_within_budget() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 1000);
        let result = tracker.consume(task, BudgetDimension::Token, 500);
        assert_eq!(result, ConsumeResult::WithinBudget);
        assert!(!tracker.is_exhausted(task, BudgetDimension::Token));
        assert_eq!(tracker.remaining(task, BudgetDimension::Token), Some(500));
    }

    #[test]
    fn consume_exhausts_budget() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 500);
        let result = tracker.consume(task, BudgetDimension::Token, 500);
        assert_eq!(result, ConsumeResult::Exhausted { overage: 0 });
        assert!(tracker.is_exhausted(task, BudgetDimension::Token));
        assert!(tracker.is_any_exhausted(task));
    }

    #[test]
    fn consume_over_limit_reports_overage() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::CostCents, 100);
        let result = tracker.consume(task, BudgetDimension::CostCents, 150);
        assert_eq!(result, ConsumeResult::Exhausted { overage: 50 });
    }

    #[test]
    fn replenish_clears_exhausted_flag() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.consume(task, BudgetDimension::Token, 100);
        assert!(tracker.is_exhausted(task, BudgetDimension::Token));
        tracker.replenish(task, BudgetDimension::Token, 200);
        assert!(!tracker.is_exhausted(task, BudgetDimension::Token));
        assert_eq!(tracker.remaining(task, BudgetDimension::Token), Some(200));
    }

    #[test]
    fn threshold_pct_returns_correct_percentage() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 200);
        tracker.consume(task, BudgetDimension::Token, 100);
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::Token), Some(50));
    }

    #[test]
    fn no_allocation_returns_within_budget() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        let result = tracker.consume(task, BudgetDimension::Token, 100);
        assert_eq!(result, ConsumeResult::WithinBudget);
    }

    #[test]
    fn threshold_pct_zero_limit_returns_100() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 0);
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::Token), Some(100));
    }

    #[test]
    fn threshold_pct_no_consumption_returns_zero() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::Token), Some(0));
    }

    #[test]
    fn threshold_pct_full_consumption_returns_100() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::CostCents, 200);
        tracker.consume(task, BudgetDimension::CostCents, 200);
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::CostCents), Some(100));
    }

    #[test]
    fn threshold_pct_over_consumption_capped_at_100() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::TimeSecs, 100);
        tracker.consume(task, BudgetDimension::TimeSecs, 200);
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::TimeSecs), Some(100));
    }

    #[test]
    fn threshold_pct_no_allocation_returns_none() {
        let tracker = BudgetTracker::new();
        let task = TaskId::new();
        assert_eq!(tracker.threshold_pct(task, BudgetDimension::Token), None);
    }

    #[test]
    fn gc_task_removes_all_dimensions() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.allocate(task, BudgetDimension::CostCents, 200);
        tracker.gc_task(task);
        assert!(tracker.get(task, BudgetDimension::Token).is_none());
        assert!(tracker.get(task, BudgetDimension::CostCents).is_none());
    }

    #[test]
    fn gc_task_does_not_affect_other_tasks() {
        let mut tracker = BudgetTracker::new();
        let task1 = TaskId::new();
        let task2 = TaskId::new();
        tracker.allocate(task1, BudgetDimension::Token, 100);
        tracker.allocate(task2, BudgetDimension::Token, 200);
        tracker.gc_task(task1);
        assert!(tracker.get(task1, BudgetDimension::Token).is_none());
        assert!(tracker.get(task2, BudgetDimension::Token).is_some());
    }

    #[test]
    fn gc_task_is_idempotent() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.gc_task(task);
        tracker.gc_task(task); // must not panic
    }

    #[test]
    fn is_any_exhausted_checks_all_dimensions() {
        let mut tracker = BudgetTracker::new();
        let task = TaskId::new();
        tracker.allocate(task, BudgetDimension::Token, 100);
        tracker.allocate(task, BudgetDimension::CostCents, 100);
        tracker.consume(task, BudgetDimension::Token, 100);
        assert!(tracker.is_any_exhausted(task));
        assert!(!tracker.is_exhausted(task, BudgetDimension::CostCents));
    }
}
