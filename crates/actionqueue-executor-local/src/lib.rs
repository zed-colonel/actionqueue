#![forbid(unsafe_code)]
//! Local executor for the ActionQueue task queue.
//!
//! This crate provides the execution infrastructure for running task attempts
//! on the local node. It handles:
//!
//! - **Dispatch queue** ([`pool::DispatchQueue`]) — FIFO intake with backpressure
//! - **Attempt execution** ([`AttemptRunner`]) — Request/response pipeline with
//!   timeout enforcement and cancellation cooperation tracking
//! - **Timeout enforcement** ([`timeout::TimeoutGuard`]) — Watchdog-based timeout with
//!   panic-safe cleanup and cooperation metrics
//! - **Backoff strategies** ([`backoff::BackoffStrategy`]) — Pluggable delay computation
//!   with overflow-safe arithmetic
//!
//! # Handler Contract
//!
//! Implement [`handler::ExecutorHandler`] to define task execution logic. Handlers must be
//! `Send + Sync` and should poll the [`handler::cancellation::CancellationToken`] at bounded
//! cadence for cooperative timeout support.

pub mod attempt_runner;
pub mod backoff;
pub mod children;
pub mod handler;
pub mod pool;
pub mod timeout;
pub mod types;

pub use attempt_runner::{
    AttemptOutcomeRecord, AttemptRunner, AttemptTimer, SystemAttemptTimer, TimeoutCadencePolicy,
    TimeoutCooperation, TimeoutCooperationMetrics, TimeoutCooperationMetricsSnapshot,
    TimeoutEnforcementReport,
};
pub use backoff::{BackoffConfigError, BackoffStrategy, ExponentialBackoff, FixedBackoff};
pub use children::{ChildState, ChildrenSnapshot};
pub use handler::{
    AttemptDisposition, AttemptMetadata, ExecutorContext, ExecutorHandler, HandlerInput,
};
pub use handler::{CancellationContext, CancellationToken};
pub use pool::{DispatchQueue, DispatchQueueError};
pub use timeout::{
    classify_timeout, GuardedExecution, SystemTimeoutClock, TimeoutClassification, TimeoutClock,
    TimeoutFailure, TimeoutGuard, TimeoutReasonCode,
};
pub use types::ExecutorRequest;
