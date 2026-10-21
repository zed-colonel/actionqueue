//! Snapshot module for state persistence and recovery.

pub mod build;
pub mod loader;
pub mod mapping;
pub mod model;
pub mod writer;

pub(crate) mod envelope;

pub(crate) mod wire_v10;

pub mod checkpoint;

#[cfg(test)]
mod checkpoint_tests;
