#![forbid(unsafe_code)]
//! CLI library for the ActionQueue system.
//!
//! This crate provides the CLI infrastructure for operating ActionQueue:
//! - Argument parsing and command dispatch
//! - Daemon startup commands
//! - Task submission to the ActionQueue daemon
//! - Stats introspection from the daemon

pub mod args;
pub mod cmd;
