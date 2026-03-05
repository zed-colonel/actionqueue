//! Daemon configuration type.
//!
//! This module defines the configuration surface for the ActionQueue daemon. The
//! configuration is an operator-facing contract that must be explicit,
//! deterministic, and serializable. It does not read from environment variables
//! implicitly — defaults are stable and explicit.
//!
//! **Exception:** The tracing/logging subscriber (initialized during bootstrap,
//! not by this module) reads the `RUST_LOG` environment variable for log-level
//! filtering. This is standard Rust observability practice and does not affect
//! engine behavior, mutation authority, or any durable state.
//!
//! # Invariant boundaries
//!
//! Configuration must not introduce any mutation lane outside the validated
//! mutation authority defined in `invariant-boundaries-v0.1.md`. Configuration
//! values must be explicit and inspectable to preserve auditability required by
//! external systems.
//!
//! # Field semantics
//!
//! ## Read-only by default
//!
//! All introspection endpoints (health, ready, stats, task/run queries) are
//! enabled by default. Control endpoints require explicit enablement via
//! `enable_control`.
//!
//! ## Deterministic defaults
//!
//! The [`DaemonConfig::default`] constructor produces fixed, stable values.
//! No environment variables or runtime detection influence defaults.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

/// Daemon configuration for the ActionQueue service.
///
/// This struct defines the operator-facing configuration contract for the
/// ActionQueue daemon. It is designed to be explicit, deterministic, and
/// serializable.
///
/// # Default posture
///
/// The default configuration:
/// - Binds the HTTP API to `127.0.0.1:8787`
/// - Uses the literal string `~/.actionqueue/data` as the data directory (no expansion)
/// - Has control endpoints disabled by default
/// - Has metrics enabled on a separate port
///
/// # Invariants
///
/// - Configuration must not permit state mutation paths (no "auto-apply"
///   flags or bypass toggles).
/// - Config values must be explicit and inspectable to preserve auditability.
/// - No implicit environment reads inside constructors or default methods.
///
/// # Example
///
/// ```rust
/// use actionqueue_daemon::config::DaemonConfig;
///
/// let config = DaemonConfig::default();
/// assert!(!config.enable_control);
/// assert_eq!(config.bind_address.ip(), std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DaemonConfig {
    /// The bind address (IP and port) for the daemon HTTP server.
    ///
    /// This is the primary operational surface for read-only introspection
    /// endpoints. In v0.1, the daemon listens on localhost by default.
    ///
    /// # Default
    ///
    /// `127.0.0.1:8787`
    pub bind_address: SocketAddr,

    /// The data directory or persistence root path.
    ///
    /// This path is used for WAL files and optional snapshots. The daemon
    /// expects to have read/write permissions on this directory.
    /// The daemon expects `<data_dir>/wal/` and `<data_dir>/snapshots/` to exist or be creatable; filenames are fixed to `actionqueue.wal` and `snapshot.bin`.
    ///
    /// # Default
    ///
    /// The literal string `~/.actionqueue/data`. No expansion of `~` or environment
    /// variables occurs in [`DaemonConfig::default`]. The caller/operator is
    /// responsible for resolving the path to a concrete filesystem location if
    /// required.
    pub data_dir: PathBuf,

    /// Feature flag to enable control endpoints.
    ///
    /// When `false` (the default), control endpoints are unreachable. When
    /// `true`, control endpoints (`/api/v1/tasks/:task_id/cancel`,
    /// `/api/v1/runs/:run_id/cancel`, `/api/v1/engine/pause`,
    /// `/api/v1/engine/resume`) are registered and may be invoked.
    ///
    /// # Safety
    ///
    /// Control endpoints require explicit enablement to enforce least-privilege
    /// by default. This prevents accidental activation of mutating operations.
    ///
    /// # Default
    ///
    /// `false`
    pub enable_control: bool,

    /// The bind address for Prometheus-compatible metrics.
    ///
    /// If `None`, metrics are disabled. If `Some`, metrics are exposed on the
    /// specified socket address.
    ///
    /// # Default
    ///
    /// `Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090))`
    pub metrics_bind: Option<SocketAddr>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            data_dir: PathBuf::from("~/.actionqueue/data"),
            enable_control: false,
            metrics_bind: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090)),
        }
    }
}

/// Validation error for configuration values.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConfigError {
    /// A machine-readable error code.
    pub code: ConfigErrorCode,
    /// A human-readable error message.
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Machine-readable configuration error codes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConfigErrorCode {
    /// The bind address is invalid (e.g., port out of range).
    InvalidBindAddress,
    /// The data directory path is empty or invalid.
    InvalidDataDir,
    /// The metrics bind address is invalid.
    InvalidMetricsBind,
    /// The metrics bind address conflicts with the main bind address.
    PortConflict,
}

impl std::fmt::Display for ConfigErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigErrorCode::InvalidBindAddress => write!(f, "invalid_bind_address"),
            ConfigErrorCode::InvalidDataDir => write!(f, "invalid_data_dir"),
            ConfigErrorCode::InvalidMetricsBind => write!(f, "invalid_metrics_bind"),
            ConfigErrorCode::PortConflict => write!(f, "port_conflict"),
        }
    }
}

impl DaemonConfig {
    /// Validate the configuration values.
    ///
    /// This method performs structural validation only and is side-effect free.
    /// It does not perform any I/O or filesystem checks.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` if any field contains an invalid value.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Validate bind address
        if self.bind_address.port() == 0 {
            return Err(ConfigError {
                code: ConfigErrorCode::InvalidBindAddress,
                message: "bind address port must be non-zero".to_string(),
            });
        }

        // Validate metrics bind if present
        if let Some(ref metrics_addr) = self.metrics_bind {
            if metrics_addr.port() == 0 {
                return Err(ConfigError {
                    code: ConfigErrorCode::InvalidMetricsBind,
                    message: "metrics bind address port must be non-zero".to_string(),
                });
            }
            if *metrics_addr == self.bind_address {
                return Err(ConfigError {
                    code: ConfigErrorCode::PortConflict,
                    message: format!(
                        "metrics_bind {metrics_addr} conflicts with bind_address (same address \
                         and port)",
                    ),
                });
            }
        }

        // Validate data directory (structural check only)
        if self.data_dir.as_os_str().is_empty() {
            return Err(ConfigError {
                code: ConfigErrorCode::InvalidDataDir,
                message: "data directory path must not be empty".to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DaemonConfig::default();
        assert_eq!(config.bind_address.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.bind_address.port(), 8787);
        assert_eq!(config.data_dir, PathBuf::from("~/.actionqueue/data"));
        assert!(!config.enable_control);
        assert!(config.metrics_bind.is_some());
        let metrics_addr = config.metrics_bind.unwrap();
        assert_eq!(metrics_addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(metrics_addr.port(), 9090);
    }

    #[test]
    fn test_validate_valid_config() {
        let config = DaemonConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_port_bind_address() {
        let config = DaemonConfig {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            ..Default::default()
        };
        assert_eq!(config.validate().unwrap_err().code, ConfigErrorCode::InvalidBindAddress);
    }

    #[test]
    fn test_validate_zero_port_metrics_bind() {
        let config = DaemonConfig {
            metrics_bind: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            ..Default::default()
        };
        assert_eq!(config.validate().unwrap_err().code, ConfigErrorCode::InvalidMetricsBind);
    }

    #[test]
    fn test_validate_port_conflict() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787);
        let config =
            DaemonConfig { bind_address: addr, metrics_bind: Some(addr), ..Default::default() };
        let err = config.validate().unwrap_err();
        assert_eq!(err.code, ConfigErrorCode::PortConflict);
        assert!(err.message.contains("conflicts with bind_address"));
    }

    #[test]
    fn test_validate_different_ports_ok() {
        let config = DaemonConfig {
            bind_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            metrics_bind: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090)),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_data_dir() {
        let config = DaemonConfig { data_dir: PathBuf::new(), ..Default::default() };
        assert_eq!(config.validate().unwrap_err().code, ConfigErrorCode::InvalidDataDir);
    }

    #[test]
    fn test_config_equality() {
        let config1 = DaemonConfig::default();
        let config2 = DaemonConfig::default();
        assert_eq!(config1, config2);
    }

    #[test]
    fn test_config_cloning() {
        let config = DaemonConfig::default();
        let cloned = config.clone();
        assert_eq!(config, cloned);
    }
}
