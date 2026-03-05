#![forbid(unsafe_code)]
//! ActionQueue runtime: dispatch loop, configuration, and embedded API.
//!
//! This crate composes the storage, engine, and executor primitives into a
//! cohesive runtime that drives the full task lifecycle from submission through
//! scheduling, leasing, execution, and completion.

pub mod config;
pub mod dispatch;
pub mod engine;
pub mod worker;
