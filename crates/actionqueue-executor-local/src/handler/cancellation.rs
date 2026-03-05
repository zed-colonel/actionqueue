//! Cancellation mechanism for attempt execution.
//!
//! This module provides cooperative cancellation support for attempt execution.
//! A cancellation token can be checked by handlers to determine if execution
//! should be terminated early.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// A token that can be used to signal cancellation to an ongoing operation.
///
/// The token itself is immutable, but can be used to check if cancellation
/// has been requested. When a cancellation is requested, all handlers
/// checking this token should terminate early.
#[derive(Debug, Clone)]
#[must_use = "cancellation token should be passed to handler for cooperative timeout"]
pub struct CancellationToken {
    /// Internal flag indicating if cancellation has been requested.
    cancelled: std::sync::Arc<AtomicBool>,
    /// Internal flag indicating whether any handler poll observed cancellation.
    observed_cancelled: std::sync::Arc<AtomicBool>,
    /// Timestamp of the first cancellation request.
    cancellation_requested_at: std::sync::Arc<OnceLock<Instant>>,
    /// Timestamp of the first poll that observed cancellation.
    cancellation_observed_at: std::sync::Arc<OnceLock<Instant>>,
}

impl CancellationToken {
    /// Creates a new cancellable token that is not yet cancelled.
    pub fn new() -> Self {
        Self {
            cancelled: std::sync::Arc::new(AtomicBool::new(false)),
            observed_cancelled: std::sync::Arc::new(AtomicBool::new(false)),
            cancellation_requested_at: std::sync::Arc::new(OnceLock::new()),
            cancellation_observed_at: std::sync::Arc::new(OnceLock::new()),
        }
    }

    /// Returns `true` if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        let cancelled = self.cancelled.load(Ordering::SeqCst);
        if cancelled {
            self.observed_cancelled.store(true, Ordering::SeqCst);
            let _ = self.cancellation_observed_at.get_or_init(Instant::now);
        }
        cancelled
    }

    /// Requests cancellation for this token.
    ///
    /// This is idempotent - calling it multiple times has the same effect
    /// as calling it once.
    pub fn cancel(&self) {
        let _ = self.cancellation_requested_at.get_or_init(Instant::now);
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Returns `true` once any poll has observed cancellation as requested.
    pub fn was_cancellation_observed(&self) -> bool {
        self.observed_cancelled.load(Ordering::SeqCst)
    }

    /// Returns the first observed cancellation-poll latency from request to observation.
    pub fn cancellation_observation_latency(&self) -> Option<Duration> {
        let requested = self.cancellation_requested_at.get().copied()?;
        let observed = self.cancellation_observed_at.get().copied()?;
        Some(observed.saturating_duration_since(requested))
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// A context that provides access to a cancellation token.
///
/// This is passed to handlers to allow them to check for cancellation
/// during execution.
#[derive(Debug, Clone)]
pub struct CancellationContext {
    token: CancellationToken,
}

impl CancellationContext {
    /// Creates a new cancellation context with a fresh token.
    pub fn new() -> Self {
        Self { token: CancellationToken::new() }
    }

    /// Returns the cancellation token for this context.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Requests cancellation for this context's token.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Returns `true` once any handler poll has observed cancellation.
    pub fn was_cancellation_observed(&self) -> bool {
        self.token.was_cancellation_observed()
    }

    /// Returns the first observed cancellation-poll latency from request to observation.
    pub fn cancellation_observation_latency(&self) -> Option<Duration> {
        self.token.cancellation_observation_latency()
    }
}

impl Default for CancellationContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_token_defaults_to_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancellation_token_can_be_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_observation_is_recorded_when_polled_after_cancel() {
        let token = CancellationToken::new();
        assert!(!token.was_cancellation_observed());
        assert_eq!(token.cancellation_observation_latency(), None);

        token.cancel();
        assert!(token.is_cancelled());
        assert!(token.was_cancellation_observed());
        assert!(token.cancellation_observation_latency().is_some());
    }

    #[test]
    fn cancellation_is_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_context_creates_fresh_token() {
        let context = CancellationContext::new();
        assert!(!context.token().is_cancelled());
    }

    #[test]
    fn cancellation_context_can_cancel() {
        let context = CancellationContext::new();
        context.cancel();
        assert!(context.token().is_cancelled());
    }

    #[test]
    fn cancellation_context_reports_observation_state() {
        let context = CancellationContext::new();
        assert!(!context.was_cancellation_observed());
        assert_eq!(context.cancellation_observation_latency(), None);

        context.cancel();
        assert!(context.token().is_cancelled());
        assert!(context.was_cancellation_observed());
        assert!(context.cancellation_observation_latency().is_some());
    }
}
