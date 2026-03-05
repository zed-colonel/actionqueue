//! Daemon clock re-exports from `actionqueue-core`.
//!
//! All clock types are now defined in `actionqueue_core::time::clock`.
//! This module provides backward-compatible type aliases.

pub use actionqueue_core::time::clock::{Clock, MockClock, SharedClock, SystemClock};

/// Backward-compatible alias for [`SharedClock`].
pub type SharedDaemonClock = SharedClock;

#[cfg(test)]
mod tests {
    use super::{Clock, MockClock, SystemClock};

    #[test]
    fn fixed_clock_returns_deterministic_timestamp() {
        let clock = MockClock::new(1_700_000_000);
        assert_eq!(clock.now(), 1_700_000_000);
    }

    #[test]
    fn system_clock_now_is_non_panicking() {
        let clock = SystemClock;
        let now = clock.now();
        assert!(now > 0);
    }
}
