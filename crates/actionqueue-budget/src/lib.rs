#![forbid(unsafe_code)]
//! Budget tracking and pre-dispatch eligibility for ActionQueue.
//!
//! Durable state belongs to storage; the tracker mirrors its projection.
//! Internal structural subscriptions belong to `actionqueue-engine::reactivity`.
//! Signals, waits, and suspension resume do not require this crate.

pub mod gate;
pub mod tracker;

pub use gate::BudgetGate;
pub use tracker::{BudgetState, BudgetTracker, ConsumeResult};
