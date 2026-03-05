#![forbid(unsafe_code)]
//! Budget enforcement and event subscription primitives for ActionQueue.
//!
//! This crate provides the in-memory budget tracking and event subscription
//! matching logic that Caelum uses for Cognitive Thread budget enforcement,
//! preemptibility, and reactive coordination.
//!
//! # Architecture
//!
//! The `actionqueue-budget` crate is intentionally small: it provides pure
//! in-memory data structures reconstructed from WAL-sourced events. All
//! durability is handled by `actionqueue-storage`; this crate depends only on
//! `actionqueue-core` for domain types.
//!
//! # Components
//!
//! - [`tracker::BudgetTracker`] — per-task budget state (allocations, consumption)
//! - [`gate::BudgetGate`] — pre-dispatch eligibility check
//! - [`subscription::registry::SubscriptionRegistry`] — active subscription state
//! - [`subscription::matcher::check_event`] — event-to-subscription matching

pub mod gate;
pub mod subscription;
pub mod tracker;

pub use actionqueue_core::event::{check_event, ActionQueueEvent, SubscriptionSource};
pub use gate::BudgetGate;
pub use subscription::registry::SubscriptionRegistry;
pub use tracker::{BudgetState, BudgetTracker, ConsumeResult};
