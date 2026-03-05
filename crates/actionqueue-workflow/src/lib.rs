#![forbid(unsafe_code)]
//! Workflow primitives for ActionQueue.
//!
//! This crate extends ActionQueue with the workflow capabilities needed by Manifold
//! and downstream systems: DAG task dependencies, parent-child task hierarchy,
//! dynamic task submission from handlers, and cron scheduling.
//!
//! # Integration
//!
//! The dispatch loop in `actionqueue-runtime` uses this crate to:
//! - Create a [`submission::SubmissionChannel`] and pass it to handlers
//!   via `ExecutorContext.submission`
//! - Build a [`children::ChildrenSnapshot`] and pass it to Coordinator handlers
//!   via `ExecutorContext.children`
//! - Process submitted [`submission::TaskSubmission`]s on each tick

pub mod children;
pub mod dag;
pub mod hierarchy;
pub mod submission;
