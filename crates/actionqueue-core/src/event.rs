//! System events for subscription matching and dispatch coordination.
//!
//! [`ActionQueueEvent`](crate::event::ActionQueueEvent) is the canonical event type
//! evaluated by the dispatch loop each tick. Subscriptions declare
//! [`EventFilter`](crate::subscription::EventFilter)
//! predicates that are matched against fired events to trigger task promotion.
//!
//! This module lives in `actionqueue-core` (rather than `actionqueue-budget`) so that
//! `actionqueue-actor` and other extension crates can emit events without creating
//! a circular dependency on `actionqueue-budget`.

use crate::budget::BudgetDimension;
use crate::ids::{ActorId, TaskId, TenantId};
use crate::run::state::RunState;
use crate::subscription::{EventFilter, SubscriptionId};

/// An event that occurred within the dispatch loop, evaluated against subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionQueueEvent {
    /// A task reached terminal success (all runs completed).
    TaskReachedTerminalSuccess {
        /// The task that completed.
        task_id: TaskId,
    },
    /// A run of the given task transitioned to a new state.
    RunChangedState {
        /// The owning task.
        task_id: TaskId,
        /// The new run state.
        new_state: RunState,
    },
    /// A budget dimension crossed a percentage threshold for the given task.
    BudgetThresholdCrossed {
        /// The task whose budget crossed a threshold.
        task_id: TaskId,
        /// The budget dimension.
        dimension: BudgetDimension,
        /// The percentage (0-100) at which the threshold was crossed.
        pct: u8,
    },
    /// An application-defined custom event.
    CustomEvent {
        /// The application-defined event key.
        key: String,
    },
    /// A remote actor registered with the hub.
    ActorRegistered {
        /// The actor that registered.
        actor_id: ActorId,
    },
    /// A remote actor deregistered (explicit or heartbeat timeout).
    ActorDeregistered {
        /// The actor that deregistered.
        actor_id: ActorId,
    },
    /// A remote actor's heartbeat timed out.
    ActorHeartbeatTimeout {
        /// The actor whose heartbeat timed out.
        actor_id: ActorId,
    },
    /// A ledger entry was appended in the platform layer.
    LedgerEntryAppended {
        /// The tenant whose ledger received the entry.
        tenant_id: TenantId,
        /// The ledger key (e.g. `"audit"`, `"decision"`).
        ledger_key: String,
    },
}

/// Checks a `ActionQueueEvent` against all active subscriptions in `registry`.
///
/// Returns the IDs of subscriptions whose filters match the event. Used by
/// the dispatch loop to trigger task promotions when events fire.
pub fn check_event<R>(event: &ActionQueueEvent, registry: &R) -> Vec<SubscriptionId>
where
    R: SubscriptionSource,
{
    registry
        .active_subscriptions_iter()
        .filter_map(|(id, filter)| if filter_matches(filter, event) { Some(id) } else { None })
        .collect()
}

/// Trait for types that expose active subscription filters.
///
/// Implemented by `SubscriptionRegistry` in `actionqueue-budget`. This indirection
/// keeps `actionqueue-core` free of a dependency on `actionqueue-budget`.
pub trait SubscriptionSource {
    /// Returns an iterator over (subscription_id, filter) pairs for all
    /// active (non-canceled, non-triggered) subscriptions.
    fn active_subscriptions_iter(
        &self,
    ) -> impl Iterator<Item = (SubscriptionId, &EventFilter)> + '_;
}

fn filter_matches(filter: &EventFilter, event: &ActionQueueEvent) -> bool {
    match (filter, event) {
        (
            EventFilter::TaskCompleted { task_id: filter_task },
            ActionQueueEvent::TaskReachedTerminalSuccess { task_id: event_task },
        ) => filter_task == event_task,

        (
            EventFilter::RunStateChanged { task_id: filter_task, state: filter_state },
            ActionQueueEvent::RunChangedState { task_id: event_task, new_state },
        ) => filter_task == event_task && filter_state == new_state,

        (
            EventFilter::BudgetThreshold {
                task_id: filter_task,
                dimension: filter_dim,
                threshold_pct,
            },
            ActionQueueEvent::BudgetThresholdCrossed { task_id: event_task, dimension, pct },
        ) => filter_task == event_task && filter_dim == dimension && pct >= threshold_pct,

        (
            EventFilter::Custom { key: filter_key },
            ActionQueueEvent::CustomEvent { key: event_key },
        ) => filter_key == event_key,

        _ => false,
    }
}
