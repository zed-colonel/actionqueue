#![forbid(unsafe_code)]
//! Workflow primitives for ActionQueue.
//!
//! This crate extends ActionQueue with the workflow capabilities needed by Manifold
//! and downstream systems: DAG task dependencies, parent-child task hierarchy,
//! dynamic task submission from handlers, and cron scheduling.
//!
pub mod children;
pub mod dag;
pub mod hierarchy;
