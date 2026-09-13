//! Validated continuation contracts used by durable storage and runtime dispatch.
pub mod signal;
pub use signal::*;
pub mod wait;
pub use wait::*;
pub mod checkpoint;
pub use checkpoint::*;
pub mod resume;
pub use resume::*;
pub mod signal_admission;
pub use signal_admission::*;
pub mod signal_canonical;
pub use signal_canonical::*;
