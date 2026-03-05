#![forbid(unsafe_code)]
//! Multi-tenant isolation, RBAC, and ledger infrastructure for ActionQueue v1.0.
//!
//! This crate provides pure in-memory data structures for the platform layer.
//! No I/O, no tokio, no storage dependencies.
//!
//! # Components
//!
//! - [`TenantRegistry`] — tenant registration and lookup
//! - [`RbacEnforcer`] — role-based capability enforcement
//! - [`AppendLedger`] — generic append-only ledger backed by WAL events

pub mod ledger;
pub mod rbac;
pub mod tenant;

pub use ledger::AppendLedger;
pub use rbac::{RbacEnforcer, RbacError};
pub use tenant::TenantRegistry;
