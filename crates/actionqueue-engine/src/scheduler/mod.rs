//! Scheduler utilities for run promotion and selection.
//!
//! This module provides scheduling logic for moving runs between states
//! and selecting runs for execution.
//!
//! The promotion module handles moving runs from Scheduled to Ready when
//! their scheduled_at time has passed.

pub mod attempt_finish;
pub mod promotion;
pub mod retry_promotion;
