//! Write-Ahead Log (WAL) module.

pub mod codec;
pub mod event;
pub mod fs_reader;
pub mod fs_writer;
pub mod reader;
pub mod repair;
pub mod tail_validation;
pub mod writer;

pub use writer::{InstrumentedWalWriter, WalAppendTelemetry, WalAppendTelemetrySnapshot};
