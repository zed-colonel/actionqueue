//! Recovery module for WAL replay and state reconstruction.
//!
//! Recovery components are replay-only reconstruction surfaces.
//! Durable lifecycle mutation commands must route through
//! [`crate::mutation::authority::StorageMutationAuthority`].

pub mod bootstrap;
pub mod reducer;
pub mod replay;

pub mod projection;

pub mod signals;
mod validation;

pub mod waits;

pub mod checkpoints;
pub mod resume;

mod dispositions;

mod task_status;
