//! Runtime configuration for the ActionQueue dispatch loop.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the ActionQueue runtime.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Directory for WAL and snapshot storage.
    pub data_dir: PathBuf,
    /// Backoff strategy for retry delay computation.
    pub backoff_strategy: BackoffStrategyConfig,
    /// Maximum number of concurrently executing runs.
    pub dispatch_concurrency: NonZeroUsize,
    /// Lease timeout in seconds for dispatched runs.
    pub lease_timeout_secs: u64,
    /// Interval between scheduler ticks.
    pub tick_interval: Duration,
    /// Number of WAL events between automatic snapshot writes.
    ///
    /// When `Some(n)`, the dispatch loop writes a snapshot every `n` WAL
    /// events. When `None`, automatic snapshot writing is disabled.
    pub snapshot_event_threshold: Option<u64>,
}

/// Backoff strategy configuration.
#[derive(Debug, Clone)]
pub enum BackoffStrategyConfig {
    /// Fixed interval between retries.
    Fixed {
        /// The constant delay between retries.
        interval: Duration,
    },
    /// Exponential backoff with a maximum cap.
    Exponential {
        /// Base delay for the first retry.
        base: Duration,
        /// Maximum delay cap.
        max: Duration,
    },
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::from_secs(5) },
            dispatch_concurrency: NonZeroUsize::new(4).expect("4 is non-zero"),
            lease_timeout_secs: 300,
            tick_interval: Duration::from_millis(100),
            snapshot_event_threshold: Some(10_000),
        }
    }
}

/// Errors that can occur during configuration validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The data directory path is empty.
    EmptyDataDir,
    /// The tick interval is zero.
    ZeroTickInterval,
    /// The exponential backoff base exceeds the max.
    BackoffBaseExceedsMax,
    /// The snapshot event threshold is zero.
    ZeroSnapshotEventThreshold,
    /// The lease timeout is too low for heartbeat semantics.
    LeaseTimeoutTooLow,
    /// The tick interval exceeds the maximum (60 seconds).
    TickIntervalTooHigh,
    /// The lease timeout exceeds the maximum (24 hours).
    LeaseTimeoutTooHigh,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::EmptyDataDir => write!(f, "data_dir must not be empty"),
            ConfigError::ZeroTickInterval => write!(f, "tick_interval must be greater than zero"),
            ConfigError::BackoffBaseExceedsMax => {
                write!(f, "exponential backoff base must not exceed max")
            }
            ConfigError::ZeroSnapshotEventThreshold => {
                write!(f, "snapshot_event_threshold must be >= 1 when provided")
            }
            ConfigError::LeaseTimeoutTooLow => {
                write!(f, "lease_timeout_secs must be >= 3 for correct heartbeat semantics")
            }
            ConfigError::TickIntervalTooHigh => {
                write!(f, "tick_interval must not exceed 60 seconds")
            }
            ConfigError::LeaseTimeoutTooHigh => {
                write!(f, "lease_timeout_secs must not exceed 86400 (24 hours)")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl RuntimeConfig {
    /// Validates this configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::EmptyDataDir);
        }
        if self.tick_interval.is_zero() {
            return Err(ConfigError::ZeroTickInterval);
        }
        if self.tick_interval > Duration::from_secs(60) {
            return Err(ConfigError::TickIntervalTooHigh);
        }
        if let BackoffStrategyConfig::Exponential { base, max } = &self.backoff_strategy {
            if base > max {
                return Err(ConfigError::BackoffBaseExceedsMax);
            }
        }
        if self.snapshot_event_threshold == Some(0) {
            return Err(ConfigError::ZeroSnapshotEventThreshold);
        }
        if self.lease_timeout_secs < 3 {
            return Err(ConfigError::LeaseTimeoutTooLow);
        }
        if self.lease_timeout_secs > 86_400 {
            return Err(ConfigError::LeaseTimeoutTooHigh);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        let config = RuntimeConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn empty_data_dir_is_rejected() {
        let config = RuntimeConfig { data_dir: PathBuf::from(""), ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::EmptyDataDir));
    }

    #[test]
    fn zero_tick_interval_is_rejected() {
        let config = RuntimeConfig { tick_interval: Duration::ZERO, ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::ZeroTickInterval));
    }

    #[test]
    fn zero_snapshot_event_threshold_is_rejected() {
        let config =
            RuntimeConfig { snapshot_event_threshold: Some(0), ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::ZeroSnapshotEventThreshold));
    }

    #[test]
    fn none_snapshot_event_threshold_validates() {
        let config = RuntimeConfig { snapshot_event_threshold: None, ..RuntimeConfig::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn lease_timeout_below_minimum_is_rejected() {
        let config = RuntimeConfig { lease_timeout_secs: 2, ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::LeaseTimeoutTooLow));
    }

    #[test]
    fn lease_timeout_at_minimum_validates() {
        let config = RuntimeConfig { lease_timeout_secs: 3, ..RuntimeConfig::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn tick_interval_exceeding_maximum_is_rejected() {
        let config =
            RuntimeConfig { tick_interval: Duration::from_secs(61), ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::TickIntervalTooHigh));
    }

    #[test]
    fn tick_interval_at_maximum_validates() {
        let config =
            RuntimeConfig { tick_interval: Duration::from_secs(60), ..RuntimeConfig::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn lease_timeout_exceeding_maximum_is_rejected() {
        let config = RuntimeConfig { lease_timeout_secs: 86_401, ..RuntimeConfig::default() };
        assert_eq!(config.validate(), Err(ConfigError::LeaseTimeoutTooHigh));
    }

    #[test]
    fn lease_timeout_at_maximum_validates() {
        let config = RuntimeConfig { lease_timeout_secs: 86_400, ..RuntimeConfig::default() };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn exponential_base_exceeding_max_is_rejected() {
        let config = RuntimeConfig {
            backoff_strategy: BackoffStrategyConfig::Exponential {
                base: Duration::from_secs(100),
                max: Duration::from_secs(10),
            },
            ..RuntimeConfig::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::BackoffBaseExceedsMax));
    }
}
