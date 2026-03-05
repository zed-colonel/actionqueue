#![forbid(unsafe_code)]
//! Remote actor management for ActionQueue v1.0.
//!
//! This crate provides pure in-memory data structures for managing remote
//! actor registrations, heartbeat monitoring, capability-based routing, and
//! department grouping. No I/O, no tokio, no storage dependencies.
//!
//! # Components
//!
//! - [`ActorRegistry`] — registered actor state with secondary tenant index
//! - [`HeartbeatMonitor`] — per-actor heartbeat tracking and timeout detection
//! - [`CapabilityRouter`] — stateless capability intersection matching
//! - [`DepartmentRegistry`] — actor-to-department grouping with reverse index

pub mod department;
pub mod heartbeat;
pub mod registry;
pub mod routing;

pub use department::DepartmentRegistry;
pub use heartbeat::HeartbeatMonitor;
pub use registry::ActorRegistry;
pub use routing::CapabilityRouter;
