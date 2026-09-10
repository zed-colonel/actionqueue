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

pub(crate) mod wire_v1;

mod task_v1;

pub use wire_v1::RESERVED_KINDS;

mod domain_v1;

pub(crate) mod admission_v1;
