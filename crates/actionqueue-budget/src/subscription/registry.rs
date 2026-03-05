//! In-memory subscription state registry.
//!
//! Tracks the lifecycle of event subscriptions: active, triggered, canceled.
//! The dispatch loop consults this registry to:
//! 1. Find tasks with triggered subscriptions for early promotion.
//! 2. Match new events against active subscriptions.

use std::collections::{HashMap, HashSet};

use actionqueue_core::ids::TaskId;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};

/// State record for a single subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    /// The subscribing task.
    pub task_id: TaskId,
    /// The event filter.
    pub filter: EventFilter,
    /// Set when the subscription has been triggered. One-shot: cleared after promotion.
    pub triggered: bool,
    /// Set when the subscription has been explicitly canceled.
    pub canceled: bool,
}

/// In-memory registry of event subscriptions.
///
/// Reconstructed from WAL events at bootstrap via [`SubscriptionRegistry::register`],
/// [`SubscriptionRegistry::trigger`], and [`SubscriptionRegistry::cancel`] calls.
///
/// Maintains a secondary `task_subscriptions` index for O(S) per-task lookups
/// where S is the number of subscriptions for that task (not O(N) total).
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    subscriptions: HashMap<SubscriptionId, SubscriptionEntry>,
    /// Secondary index: task_id → set of subscription_ids for that task.
    task_subscriptions: HashMap<TaskId, HashSet<SubscriptionId>>,
}

impl SubscriptionRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new active subscription.
    pub fn register(
        &mut self,
        subscription_id: SubscriptionId,
        task_id: TaskId,
        filter: EventFilter,
    ) {
        self.subscriptions.insert(
            subscription_id,
            SubscriptionEntry { task_id, filter, triggered: false, canceled: false },
        );
        self.task_subscriptions.entry(task_id).or_default().insert(subscription_id);
    }

    /// Marks a subscription as triggered (promotion eligible).
    pub fn trigger(&mut self, subscription_id: SubscriptionId) {
        if let Some(entry) = self.subscriptions.get_mut(&subscription_id) {
            entry.triggered = true;
        }
    }

    /// Marks a subscription as canceled (no longer active).
    pub fn cancel(&mut self, subscription_id: SubscriptionId) {
        if let Some(entry) = self.subscriptions.get_mut(&subscription_id) {
            entry.canceled = true;
        }
    }

    /// Returns an iterator over non-canceled, non-triggered active subscriptions.
    pub fn active_subscriptions(
        &self,
    ) -> impl Iterator<Item = (SubscriptionId, &SubscriptionEntry)> {
        self.subscriptions
            .iter()
            .filter(|(_, entry)| !entry.canceled && !entry.triggered)
            .map(|(id, entry)| (*id, entry))
    }

    /// Returns `true` if any subscription for this task has been triggered.
    ///
    /// O(S) where S = number of subscriptions for this task (via secondary index).
    pub fn is_triggered(&self, task_id: TaskId) -> bool {
        let Some(ids) = self.task_subscriptions.get(&task_id) else {
            return false;
        };
        ids.iter().any(|id| self.subscriptions.get(id).is_some_and(|e| e.triggered && !e.canceled))
    }

    /// Clears the triggered flag for all triggered subscriptions for this task.
    ///
    /// Called after the task has been promoted to Ready so the one-shot
    /// trigger is consumed and won't re-promote on the next tick.
    ///
    /// O(S) where S = number of subscriptions for this task (via secondary index).
    pub fn clear_triggered(&mut self, task_id: TaskId) {
        let Some(ids) = self.task_subscriptions.get(&task_id) else {
            return;
        };
        let ids: Vec<_> = ids.iter().copied().collect();
        for id in ids {
            if let Some(entry) = self.subscriptions.get_mut(&id) {
                if entry.triggered {
                    entry.triggered = false;
                }
            }
        }
    }

    /// Removes all subscription state for a fully-terminal task.
    ///
    /// Called by the dispatch loop after a task reaches terminal state.
    /// Removes all subscriptions (including canceled ones) from both the
    /// primary map and the secondary index.
    pub fn gc_task(&mut self, task_id: TaskId) {
        if let Some(ids) = self.task_subscriptions.remove(&task_id) {
            for id in ids {
                self.subscriptions.remove(&id);
            }
        }
    }

    /// Returns the subscription entry for the given ID.
    pub fn get(&self, subscription_id: &SubscriptionId) -> Option<&SubscriptionEntry> {
        self.subscriptions.get(subscription_id)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::subscription::{EventFilter, SubscriptionId};

    use super::SubscriptionRegistry;

    #[test]
    fn register_and_is_triggered_lifecycle() {
        let mut registry = SubscriptionRegistry::new();
        let sub_id = SubscriptionId::new();
        let task_id = TaskId::new();
        let filter = EventFilter::TaskCompleted { task_id };

        registry.register(sub_id, task_id, filter);
        assert!(!registry.is_triggered(task_id));

        registry.trigger(sub_id);
        assert!(registry.is_triggered(task_id));

        registry.clear_triggered(task_id);
        assert!(!registry.is_triggered(task_id));
    }

    #[test]
    fn cancel_removes_from_active() {
        let mut registry = SubscriptionRegistry::new();
        let sub_id = SubscriptionId::new();
        let task_id = TaskId::new();
        let filter = EventFilter::TaskCompleted { task_id };
        registry.register(sub_id, task_id, filter);

        registry.cancel(sub_id);
        assert_eq!(registry.active_subscriptions().count(), 0);
    }

    #[test]
    fn trigger_nonexistent_subscription_is_noop() {
        let mut registry = SubscriptionRegistry::new();
        let nonexistent = SubscriptionId::new();
        registry.trigger(nonexistent); // should not panic
        assert!(!registry.is_triggered(TaskId::new()));
    }

    #[test]
    fn cancel_nonexistent_subscription_is_noop() {
        let mut registry = SubscriptionRegistry::new();
        let nonexistent = SubscriptionId::new();
        registry.cancel(nonexistent); // should not panic
        assert_eq!(registry.active_subscriptions().count(), 0);
    }

    #[test]
    fn get_returns_entry() {
        let mut registry = SubscriptionRegistry::new();
        let sub_id = SubscriptionId::new();
        let task_id = TaskId::new();
        let filter = EventFilter::TaskCompleted { task_id };
        registry.register(sub_id, task_id, filter.clone());

        let entry = registry.get(&sub_id).expect("subscription should exist");
        assert_eq!(entry.task_id, task_id);
        assert_eq!(entry.filter, filter);
        assert!(!entry.triggered);
        assert!(!entry.canceled);
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let registry = SubscriptionRegistry::new();
        assert!(registry.get(&SubscriptionId::new()).is_none());
    }

    #[test]
    fn duplicate_registration_overwrites() {
        let mut registry = SubscriptionRegistry::new();
        let sub_id = SubscriptionId::new();
        let task1 = TaskId::new();
        let task2 = TaskId::new();
        registry.register(sub_id, task1, EventFilter::TaskCompleted { task_id: task1 });
        registry.register(sub_id, task2, EventFilter::TaskCompleted { task_id: task2 });

        let entry = registry.get(&sub_id).unwrap();
        assert_eq!(entry.task_id, task2);
        assert_eq!(registry.active_subscriptions().count(), 1);
    }

    #[test]
    fn is_triggered_uses_secondary_index() {
        let mut registry = SubscriptionRegistry::new();
        let task = TaskId::new();
        let sub1 = SubscriptionId::new();
        let sub2 = SubscriptionId::new();
        registry.register(sub1, task, EventFilter::TaskCompleted { task_id: task });
        registry.register(sub2, task, EventFilter::TaskCompleted { task_id: task });

        assert!(!registry.is_triggered(task));
        registry.trigger(sub1);
        assert!(registry.is_triggered(task));
        registry.clear_triggered(task);
        assert!(!registry.is_triggered(task));
    }

    #[test]
    fn gc_task_removes_subscriptions_and_index() {
        let mut registry = SubscriptionRegistry::new();
        let task = TaskId::new();
        let sub_id = SubscriptionId::new();
        registry.register(sub_id, task, EventFilter::TaskCompleted { task_id: task });

        registry.gc_task(task);

        assert!(registry.get(&sub_id).is_none());
        assert!(!registry.is_triggered(task));
        assert_eq!(registry.active_subscriptions().count(), 0);
    }

    #[test]
    fn gc_task_does_not_affect_other_tasks() {
        let mut registry = SubscriptionRegistry::new();
        let task1 = TaskId::new();
        let task2 = TaskId::new();
        let sub1 = SubscriptionId::new();
        let sub2 = SubscriptionId::new();
        registry.register(sub1, task1, EventFilter::TaskCompleted { task_id: task1 });
        registry.register(sub2, task2, EventFilter::TaskCompleted { task_id: task2 });

        registry.gc_task(task1);

        assert!(registry.get(&sub1).is_none());
        assert!(registry.get(&sub2).is_some());
        assert_eq!(registry.active_subscriptions().count(), 1);
    }

    #[test]
    fn gc_task_is_idempotent() {
        let mut registry = SubscriptionRegistry::new();
        let task = TaskId::new();
        let sub_id = SubscriptionId::new();
        registry.register(sub_id, task, EventFilter::TaskCompleted { task_id: task });
        registry.gc_task(task);
        registry.gc_task(task); // must not panic
    }

    #[test]
    fn active_subscriptions_excludes_triggered_and_canceled() {
        let mut registry = SubscriptionRegistry::new();
        let task = TaskId::new();

        let active = SubscriptionId::new();
        let triggered = SubscriptionId::new();
        let canceled = SubscriptionId::new();

        registry.register(active, task, EventFilter::TaskCompleted { task_id: task });
        registry.register(triggered, task, EventFilter::TaskCompleted { task_id: task });
        registry.register(canceled, task, EventFilter::TaskCompleted { task_id: task });

        registry.trigger(triggered);
        registry.cancel(canceled);

        let active_subs: Vec<_> = registry.active_subscriptions().collect();
        assert_eq!(active_subs.len(), 1);
        assert_eq!(active_subs[0].0, active);
    }
}
