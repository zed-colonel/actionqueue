#![forbid(unsafe_code)]
//! Engine utilities and abstractions for the ActionQueue system.
//!
//! This crate provides the scheduling engine for the ActionQueue system, including
//! run derivation, state management, and scheduling logic.
//!
//! # Overview
//!
//! The engine crate defines the scheduling logic for the ActionQueue system:
//!
//! - [`mod@derive`] - Run derivation from task specifications according to run policies
//! - [`index`] - Indexing utilities for run instances by state (Scheduled, Ready, Running, Terminal)
//! - [`selection`] - Run selection for executor leasing
//! - [`scheduler`] - Scheduling logic for state promotion and ordering
//! - [`time`] - Time clock abstractions for deterministic testing
//! - [`lease`] - Lease ownership and expiry models
//! - [`concurrency`] - Concurrency key gates for single-flight execution control
//!
//! # Example
//!
//! ```
//! use actionqueue_core::ids::TaskId;
//! use actionqueue_core::task::constraints::TaskConstraints;
//! use actionqueue_core::task::metadata::TaskMetadata;
//! use actionqueue_core::task::run_policy::RunPolicy;
//! use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
//! use actionqueue_engine::derive::{derive_runs, DerivationResult};
//! use actionqueue_engine::time::clock::{Clock, SystemClock};
//!
//! // Create a clock for deterministic time management
//! let clock = SystemClock::default();
//!
//! // Create a task with a "Once" run policy
//! let task_id = TaskId::new();
//! let task_spec = TaskSpec::new(
//!     task_id,
//!     TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
//!     RunPolicy::Once,
//!     TaskConstraints::default(),
//!     TaskMetadata::default(),
//! )
//! .expect("example task spec should be valid");
//!
//! // Derive runs for the task - for "Once" policy, this creates at most one run
//! let result: DerivationResult = derive_runs(
//!     &clock,
//!     task_id,
//!     task_spec.run_policy(),
//!     0,           // already_derived
//!     clock.now(), // schedule_origin
//! );
//!
//! // The result contains newly derived runs
//! // For a "Once" policy, this will be at most one run
//! let derived_runs = result.expect("derivation must succeed for valid policy").into_derived();
//!
//! // In real usage, the engine indexes would track runs by state:
//! // - Scheduled: runs waiting for their scheduled_at time
//! // - Ready: runs ready to be leased to executors
//! // - Running: runs currently being executed
//! // - Terminal: completed, failed, or canceled runs
//!
//! # // Verify the result is valid (At least one run should be derived)
//! # assert!(derived_runs.len() <= 1);
//! ```

pub mod derive;
pub mod index;
pub mod lease {
    //! Lease primitives for deterministic in-flight ownership.

    pub mod acquire;
    pub mod expiry;
    pub mod model;
}
pub mod concurrency {
    //! Concurrency key gate primitives for single-flight execution control.

    pub mod key_gate;
    pub mod lifecycle;
}
pub mod scheduler;
pub mod selection;
pub mod time;
