#![forbid(unsafe_code)]
//! Core utilities and abstractions for the ActionQueue system.
//!
//! This crate provides the core domain types for the ActionQueue task execution system,
//! including run state management, task specifications, and unique identifiers.
//!
//! # Overview
//!
//! The core crate defines the fundamental types used throughout the ActionQueue system:
//!
//! - [`ids`] - Unique identifiers for tasks, runs, and attempts
//! - [`mutation`] - Engine-facing mutation authority boundary contracts
//! - [`run`] - Run state machine and instance management
//! - [`task`] - Task specifications and constraints
//!
//! # Example
//!
//! ```
//! use actionqueue_core::ids::{RunId, TaskId};
//! use actionqueue_core::run::{RunInstance, RunState};
//!
//! // Create a new task ID
//! let task_id = TaskId::new();
//!
//! // Create a new run instance in Scheduled state
//! let run = RunInstance::new_scheduled(task_id, 1000u64, 500u64)
//!     .expect("run construction should be valid");
//!
//! // Verify the run starts in Scheduled state
//! assert_eq!(run.state(), RunState::Scheduled);
//! assert!(!run.is_terminal());
//!
//! // Create a transition to Ready state
//! use actionqueue_core::run::Transition;
//! let transition = Transition::new(RunState::Scheduled, RunState::Ready);
//! assert!(transition.is_ok());
//! ```

pub mod actor;
pub mod budget;
/// System events for subscription matching and dispatch coordination.
pub mod event;
/// Strongly-typed identifiers used across task/run/attempt domain entities.
pub mod ids;
pub mod mutation;
pub mod platform;
pub mod run;
pub mod subscription;
pub mod task;
pub mod time;
