//! Internal structural subscriptions promote Scheduled runs only.
//! Trigger notification follows its originating mutation; rearming is in memory.
//! Durable external continuation uses signals and waits, independently.

pub mod matcher;
pub mod registry;
pub use registry::InternalSubscriptionRegistry;
