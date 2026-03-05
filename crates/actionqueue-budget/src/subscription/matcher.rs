//! Subscription source implementation and re-exports for event matching.
//!
//! [`ActionQueueEvent`] and [`check_event`] are defined in `actionqueue-core` and
//! re-exported from `actionqueue-budget` for backward compatibility. This module
//! implements [`SubscriptionSource`] for [`SubscriptionRegistry`] so that
//! `check_event` can iterate active subscriptions.

pub use actionqueue_core::event::{check_event, ActionQueueEvent, SubscriptionSource};
use actionqueue_core::subscription::EventFilter;

use crate::subscription::registry::SubscriptionRegistry;

impl SubscriptionSource for SubscriptionRegistry {
    fn active_subscriptions_iter(
        &self,
    ) -> impl Iterator<Item = (actionqueue_core::subscription::SubscriptionId, &EventFilter)> + '_
    {
        self.active_subscriptions().map(|(id, entry)| (id, &entry.filter))
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::budget::BudgetDimension;
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::subscription::{EventFilter, SubscriptionId};

    use super::{check_event, ActionQueueEvent};
    use crate::subscription::registry::SubscriptionRegistry;

    #[test]
    fn task_completed_filter_matches_terminal_success_event() {
        let mut registry = SubscriptionRegistry::new();
        let task_a = TaskId::new();
        let task_b = TaskId::new();
        let sub = SubscriptionId::new();
        registry.register(sub, task_b, EventFilter::TaskCompleted { task_id: task_a });

        let event = ActionQueueEvent::TaskReachedTerminalSuccess { task_id: task_a };
        let matches = check_event(&event, &registry);
        assert_eq!(matches, vec![sub]);
    }

    #[test]
    fn task_completed_filter_does_not_match_wrong_task() {
        let mut registry = SubscriptionRegistry::new();
        let task_a = TaskId::new();
        let task_b = TaskId::new();
        let sub = SubscriptionId::new();
        registry.register(sub, task_b, EventFilter::TaskCompleted { task_id: task_a });

        let event = ActionQueueEvent::TaskReachedTerminalSuccess { task_id: task_b };
        assert!(check_event(&event, &registry).is_empty());
    }

    #[test]
    fn run_state_changed_filter_matches_correct_state() {
        let mut registry = SubscriptionRegistry::new();
        let task_a = TaskId::new();
        let task_b = TaskId::new();
        let sub = SubscriptionId::new();
        registry.register(
            sub,
            task_b,
            EventFilter::RunStateChanged { task_id: task_a, state: RunState::Suspended },
        );

        let event =
            ActionQueueEvent::RunChangedState { task_id: task_a, new_state: RunState::Suspended };
        assert_eq!(check_event(&event, &registry), vec![sub]);

        let wrong_state =
            ActionQueueEvent::RunChangedState { task_id: task_a, new_state: RunState::Completed };
        assert!(check_event(&wrong_state, &registry).is_empty());
    }

    #[test]
    fn budget_threshold_filter_matches_at_or_above_threshold() {
        let mut registry = SubscriptionRegistry::new();
        let task_a = TaskId::new();
        let task_b = TaskId::new();
        let sub = SubscriptionId::new();
        registry.register(
            sub,
            task_b,
            EventFilter::BudgetThreshold {
                task_id: task_a,
                dimension: BudgetDimension::Token,
                threshold_pct: 80,
            },
        );

        let at_threshold = ActionQueueEvent::BudgetThresholdCrossed {
            task_id: task_a,
            dimension: BudgetDimension::Token,
            pct: 80,
        };
        assert_eq!(check_event(&at_threshold, &registry), vec![sub]);

        let above_threshold = ActionQueueEvent::BudgetThresholdCrossed {
            task_id: task_a,
            dimension: BudgetDimension::Token,
            pct: 95,
        };
        assert_eq!(check_event(&above_threshold, &registry), vec![sub]);

        let below_threshold = ActionQueueEvent::BudgetThresholdCrossed {
            task_id: task_a,
            dimension: BudgetDimension::Token,
            pct: 79,
        };
        assert!(check_event(&below_threshold, &registry).is_empty());
    }

    #[test]
    fn custom_event_filter_matches_by_key() {
        let mut registry = SubscriptionRegistry::new();
        let task = TaskId::new();
        let sub = SubscriptionId::new();
        registry.register(
            sub,
            task,
            EventFilter::Custom { key: "caelum.thread.paused".to_string() },
        );

        let matching = ActionQueueEvent::CustomEvent { key: "caelum.thread.paused".to_string() };
        assert_eq!(check_event(&matching, &registry), vec![sub]);

        let non_matching = ActionQueueEvent::CustomEvent { key: "caelum.thread.other".to_string() };
        assert!(check_event(&non_matching, &registry).is_empty());
    }
}
