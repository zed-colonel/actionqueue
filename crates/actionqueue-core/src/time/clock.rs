//! Unified clock trait and implementations for the ActionQueue system.
//!
//! This module defines a single clock abstraction used across all ActionQueue
//! crates. Time is represented as a 64-bit Unix timestamp (seconds since
//! epoch).

/// A trait for obtaining the current time.
///
/// This trait abstracts time sources to enable deterministic testing.
/// Implementations should provide monotonic time progression for
/// scheduling decisions.
///
/// Time is represented as a 64-bit Unix timestamp (seconds since epoch).
pub trait Clock: Send + Sync {
    /// Returns the current time as seconds since Unix epoch.
    fn now(&self) -> u64;
}

/// A wall-clock implementation that uses the actual system time.
///
/// This implementation reads from the system clock and is suitable for
/// production use.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    /// Returns the current time as seconds since Unix epoch.
    ///
    /// # Panics
    ///
    /// Panics if the system clock reports a time before the Unix epoch
    /// (January 1, 1970). This is unreachable on all supported platforms.
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_secs()
    }
}

impl SystemClock {
    /// Returns the current time as a `std::time::SystemTime`.
    pub fn now_system_time(&self) -> std::time::SystemTime {
        std::time::SystemTime::now()
    }
}

/// A mock clock implementation for deterministic testing.
///
/// This clock allows time to be advanced explicitly, enabling deterministic
/// scheduling tests without relying on real time progression.
#[derive(Debug, Clone)]
pub struct MockClock {
    /// The current time in seconds since Unix epoch.
    current_time: u64,
}

impl MockClock {
    /// Creates a new mock clock initialized to the given time.
    pub fn new(current_time: u64) -> Self {
        Self { current_time }
    }

    /// Creates a new mock clock initialized to the current system time.
    ///
    /// # Panics
    ///
    /// Panics if the system clock reports a time before the Unix epoch
    /// (January 1, 1970). This is unreachable on all supported platforms.
    pub fn with_system_time() -> Self {
        Self::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time before Unix epoch")
                .as_secs(),
        )
    }

    /// Advances the clock by the specified number of seconds.
    pub fn advance_by(&mut self, seconds: u64) {
        self.current_time = self.current_time.saturating_add(seconds);
    }

    /// Sets the clock to a specific time.
    pub fn set(&mut self, time: u64) {
        self.current_time = time;
    }

    /// Returns the current time as a `std::time::SystemTime`.
    pub fn now_system_time(&self) -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH
            .checked_add(std::time::Duration::from_secs(self.current_time))
            .expect("time overflow")
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::with_system_time()
    }
}

impl Clock for MockClock {
    fn now(&self) -> u64 {
        self.current_time
    }
}

/// Shared clock handle type for use in multi-threaded contexts.
pub type SharedClock = std::sync::Arc<dyn Clock>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_monotonically_increasing_time() {
        let clock = SystemClock;
        let time1 = clock.now();
        let time2 = clock.now();
        assert!(time2 >= time1);
    }

    #[test]
    fn mock_clock_can_be_initialized() {
        let clock = MockClock::new(1000);
        assert_eq!(clock.now(), 1000);
    }

    #[test]
    fn mock_clock_can_be_advanced() {
        let mut clock = MockClock::new(1000);
        clock.advance_by(100);
        assert_eq!(clock.now(), 1100);
    }

    #[test]
    fn mock_clock_can_be_set() {
        let mut clock = MockClock::new(1000);
        clock.set(2000);
        assert_eq!(clock.now(), 2000);
    }

    #[test]
    fn mock_clock_system_time_conversion() {
        let clock = MockClock::new(1000);
        let system_time = clock.now_system_time();
        let duration = system_time.duration_since(std::time::UNIX_EPOCH).expect("time overflow");
        assert_eq!(duration.as_secs(), 1000);
    }
}
