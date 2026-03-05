#![forbid(unsafe_code)]
//! Daemon utilities and abstractions for the ActionQueue system.
//!
//! This crate provides the daemon infrastructure for the ActionQueue system,
//! including configuration, bootstrap, HTTP introspection routes, and metrics.
//!
//! # Overview
//!
//! The daemon crate provides:
//! - [`config`] - Deterministic daemon configuration
//! - [`bootstrap`] - Bootstrap entry point for assembling runtime dependencies
//! - [`time`] - Authoritative daemon clock abstractions
//!
//! # Invariant boundaries
//!
//! All read-only introspection endpoints are enabled by default. Control
//! endpoints require explicit enablement via `enable_control` in configuration.
//! No mutation authority bypass is introduced.
//!
//! # Example
//!
//! ```no_run
//! use actionqueue_daemon::bootstrap::bootstrap;
//! use actionqueue_daemon::config::DaemonConfig;
//!
//! let config = DaemonConfig::default();
//! let state = bootstrap(config).expect("bootstrap should succeed");
//!
//! // Use state to start HTTP server at a higher level
//! // (HTTP server startup is not part of bootstrap)
//! ```

pub mod bootstrap;
pub mod config;
pub mod http;
pub mod metrics;
pub mod time;
