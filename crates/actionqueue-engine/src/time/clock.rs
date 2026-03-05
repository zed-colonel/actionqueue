//! Clock trait re-exported from `actionqueue-core`.
//!
//! All clock types are now defined in `actionqueue_core::time::clock`.
//! This module re-exports them for backward compatibility.

pub use actionqueue_core::time::clock::{Clock, MockClock, SharedClock, SystemClock};
