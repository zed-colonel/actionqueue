//! Subscription domain types for event-driven task promotion.
//!
//! Subscriptions allow tasks to be promoted to Ready when specific events
//! occur — task completion, run state changes, budget threshold crossings,
//! or application-defined custom events. The dispatch loop evaluates active
//! subscriptions each tick and triggers those whose filters match.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use uuid::Uuid;

use crate::budget::BudgetDimension;
use crate::ids::TaskId;
use crate::run::state::RunState;

/// A unique identifier for an event subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SubscriptionId(Uuid);

impl SubscriptionId {
    /// Creates a new random SubscriptionId.
    pub fn new() -> Self {
        SubscriptionId(Uuid::new_v4())
    }

    /// Creates a SubscriptionId from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        SubscriptionId(uuid)
    }

    /// Returns the inner UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for SubscriptionId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(SubscriptionId)
    }
}

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A filter that specifies which events trigger a subscription.
///
/// Subscriptions are evaluated each tick against the set of events that
/// occurred during that tick. A matching subscription triggers the
/// subscribing task's scheduled run to be promoted to Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EventFilter {
    /// Fires when a specific task reaches terminal success (Completed).
    TaskCompleted {
        /// The task that must complete to trigger this subscription.
        task_id: TaskId,
    },
    /// Fires when any run of a specific task transitions to the given state.
    RunStateChanged {
        /// The task to observe.
        task_id: TaskId,
        /// The target run state that triggers this subscription.
        state: RunState,
    },
    /// Fires when a task's budget consumption crosses a percentage threshold.
    BudgetThreshold {
        /// The task whose budget is observed.
        task_id: TaskId,
        /// The budget dimension to observe.
        dimension: BudgetDimension,
        /// The consumption percentage (0-100) at which to trigger.
        threshold_pct: u8,
    },
    /// Fires when an application-defined event with this key is emitted.
    Custom {
        /// The application-defined event key.
        key: String,
    },
}
