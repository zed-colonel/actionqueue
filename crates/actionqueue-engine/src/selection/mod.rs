//! Selection utilities for ready run selection.
//!
//! This module provides run selection logic for choosing which ready runs
//! should be leased to executors.
//!
//! The default selector implements priority-then-FIFO ordering for v0.1.

pub mod default_selector;
