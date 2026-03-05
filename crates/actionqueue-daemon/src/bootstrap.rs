//! Daemon bootstrap module.
//!
//! This module provides the bootstrap entry point for the ActionQueue daemon.
//! Bootstrap assembles runtime dependencies from configuration and prepares
//! the HTTP introspection surface without starting background loops or
//! binding network sockets.
//!
//! # Overview
//!
//! The bootstrap process:
//! 1. Validates configuration values
//! 2. Constructs storage and engine dependencies
//! 3. Initializes metrics registry
//! 4. Sets up router wiring entry point with control feature flag gating
//!
//! # Invariant boundaries
//!
//! Bootstrap must not introduce mutation authority bypass or direct state
//! mutation. All control routes must route through validated mutation
//! authority defined in [`actionqueue_storage::mutation::authority`].
//!
//! # Bootstrap output
//!
//! On success, bootstrap returns a [`BootstrapState`] that contains:
//!
//! - The validated configuration
//! - Metrics registry handle
//! - A concrete HTTP router built with [`crate::http::build_router()`]
//! - Shared router state (Arc-wrapped [`crate::http::RouterStateInner`])
//!
//! The router and router state are constructed deterministically from the
//! bootstrap output (config control flag, projection state, and readiness).
//! No sockets are bound and no background tasks are started.
//!
//! # Example
//!
//! ```no_run
//! use actionqueue_daemon::bootstrap::{bootstrap, BootstrapState};
//! use actionqueue_daemon::config::DaemonConfig;
//!
//! let config = DaemonConfig::default();
//! let state = bootstrap(config).expect("bootstrap should succeed");
//!
//! // Access the router and state directly from bootstrap output
//! let router = state.http_router();
//! let router_state = state.router_state();
//!
//! // Use router to start HTTP server at a higher level
//! // (HTTP server startup is not part of bootstrap)
//! ```

use std::path::PathBuf;

use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::{
    load_projection_from_storage, RecoveryBootstrapError,
};
use actionqueue_storage::recovery::reducer::ReplayReducer;

use crate::config::{ConfigError, DaemonConfig};
use crate::metrics::registry::MetricsRegistry;
use crate::time::clock::{SharedDaemonClock, SystemClock};

/// Bootstrap error types.
///
/// Typed errors distinguish configuration validation failures from dependency
/// initialization failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapError {
    /// Configuration validation failed.
    Config(ConfigError),
    /// WAL file system initialization failed.
    WalInit(String),
    /// Dependency assembly failed.
    Dependency(String),
}

impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootstrapError::Config(e) => write!(f, "config error: {e}"),
            BootstrapError::WalInit(msg) => write!(f, "WAL initialization error: {msg}"),
            BootstrapError::Dependency(msg) => write!(f, "dependency error: {msg}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl std::convert::From<ConfigError> for BootstrapError {
    fn from(err: ConfigError) -> Self {
        BootstrapError::Config(err)
    }
}

/// Ready status representation.
///
/// This struct holds the daemon readiness state derived from bootstrap completion.
/// Readiness is a deterministic boolean plus a stable reason string, derived only
/// from bootstrap state without any IO or polling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct ReadyStatus {
    /// Whether the daemon is ready.
    is_ready: bool,
    /// A static string describing the readiness state.
    reason: &'static str,
}

impl ReadyStatus {
    /// The daemon is fully operational.
    pub const REASON_READY: &'static str = "ready";

    /// Configuration was invalid during bootstrap.
    pub const REASON_CONFIG_INVALID: &'static str = "config_invalid";

    /// Bootstrap process was incomplete.
    pub const REASON_BOOTSTRAP_INCOMPLETE: &'static str = "bootstrap_incomplete";

    /// Returns a ready status indicating the daemon is fully operational.
    pub const fn ready() -> Self {
        Self { is_ready: true, reason: Self::REASON_READY }
    }

    /// Returns a not-ready status with the given reason string.
    ///
    /// Valid reasons are the documented static constants on [`ReadyStatus`]:
    /// - [`REASON_READY`](ReadyStatus::REASON_READY)
    /// - [`REASON_CONFIG_INVALID`](ReadyStatus::REASON_CONFIG_INVALID)
    /// - [`REASON_BOOTSTRAP_INCOMPLETE`](ReadyStatus::REASON_BOOTSTRAP_INCOMPLETE)
    pub const fn not_ready(reason: &'static str) -> Self {
        Self { is_ready: false, reason }
    }

    /// Returns whether the daemon is ready.
    pub const fn is_ready(&self) -> bool {
        self.is_ready
    }

    /// Returns the static reason string describing the readiness state.
    pub const fn reason(&self) -> &'static str {
        self.reason
    }
}

/// Router configuration for routing assembly.
#[derive(Debug, Clone)]
pub struct RouterConfig {
    /// Whether control endpoints are enabled.
    pub control_enabled: bool,
    /// Whether metrics endpoint exposure is enabled.
    pub metrics_enabled: bool,
}

/// Bootstrap state returned after successful bootstrap.
///
/// This struct holds all runtime handles assembled during bootstrap.
/// It does not start background loops or bind network sockets.
///
/// The concrete HTTP router and shared router state are assembled from
/// bootstrap output (config control flag, projection state, and readiness).
#[must_use]
pub struct BootstrapState {
    /// The validated daemon configuration.
    config: DaemonConfig,
    /// The metrics registry handle.
    metrics: std::sync::Arc<MetricsRegistry>,
    /// The concrete HTTP router built with http::build_router().
    http_router: axum::Router,
    /// WAL path for persistence (reconstructed on demand).
    wal_path: PathBuf,
    /// Snapshot path for persistence.
    snapshot_path: PathBuf,
    /// Replay projection state.
    projection: ReplayReducer,
    /// Shared router state (Arc-wrapped inner state).
    router_state: crate::http::RouterState,
    /// Shared authoritative daemon clock handle.
    clock: SharedDaemonClock,
    /// Ready status indicating daemon readiness derived from bootstrap state.
    ready_status: ReadyStatus,
}

impl BootstrapState {
    /// Returns the validated configuration.
    pub fn config(&self) -> &DaemonConfig {
        &self.config
    }

    /// Returns the metrics registry handle.
    pub fn metrics(&self) -> &MetricsRegistry {
        self.metrics.as_ref()
    }

    /// Returns the concrete HTTP router.
    pub fn http_router(&self) -> &axum::Router {
        &self.http_router
    }

    /// Returns the shared router state (Arc-wrapped inner state).
    pub fn router_state(&self) -> &crate::http::RouterState {
        &self.router_state
    }

    /// Returns the shared authoritative daemon clock handle.
    pub fn clock(&self) -> &SharedDaemonClock {
        &self.clock
    }

    /// Consumes self and returns the concrete router and router state as a tuple.
    pub fn into_http(self) -> (axum::Router, crate::http::RouterState) {
        (self.http_router, self.router_state)
    }

    /// Returns the WAL path.
    pub fn wal_path(&self) -> &PathBuf {
        &self.wal_path
    }

    /// Returns the snapshot path.
    pub fn snapshot_path(&self) -> &PathBuf {
        &self.snapshot_path
    }

    /// Returns the ReplayReducer projection state.
    pub fn projection(&self) -> &ReplayReducer {
        &self.projection
    }

    /// Returns the ready status indicating daemon readiness.
    pub fn ready_status(&self) -> ReadyStatus {
        self.ready_status
    }
}

/// Bootstrap entry point.
///
/// Validates configuration and assembles runtime dependencies.
/// This function is deterministic and does not start background loops
/// or bind network sockets.
///
/// # Errors
///
/// Returns [`BootstrapError::Config`] if configuration validation fails.
/// Returns [`BootstrapError::WalInit`] if WAL file initialization fails.
///
/// # Examples
///
/// ```no_run
/// use actionqueue_daemon::bootstrap::bootstrap;
/// use actionqueue_daemon::config::DaemonConfig;
///
/// let config = DaemonConfig::default();
/// let state = bootstrap(config).expect("bootstrap should succeed");
/// ```
pub fn bootstrap(config: DaemonConfig) -> Result<BootstrapState, BootstrapError> {
    // Initialize structured logging subscriber.
    // Uses RUST_LOG env var for filtering (e.g. RUST_LOG=actionqueue=debug).
    // init() is a no-op if a subscriber is already set (e.g. in tests).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Validate configuration first
    config.validate()?;

    let recovery = load_projection_from_storage(&config.data_dir).map_err(map_recovery_error)?;
    let wal_path = recovery.wal_path.clone();
    let snapshot_path = recovery.snapshot_path.clone();
    let projection = recovery.projection.clone();
    let wal_append_telemetry = recovery.wal_append_telemetry.clone();
    let recovery_observations = recovery.recovery_observations;

    let control_authority = if config.enable_control {
        Some(std::sync::Arc::new(std::sync::Mutex::new(StorageMutationAuthority::new(
            recovery.wal_writer,
            projection.clone(),
        ))))
    } else {
        None
    };

    // Create metrics registry
    let metrics =
        std::sync::Arc::new(MetricsRegistry::new(config.metrics_bind).map_err(|error| {
            BootstrapError::Dependency(format!("metrics_registry_init_failed: {error}"))
        })?);

    // Create a single authoritative daemon clock handle for router and metrics wiring.
    let clock: SharedDaemonClock = std::sync::Arc::new(SystemClock);

    // Build router state from bootstrap output
    let ready_status = ReadyStatus::ready();
    let router_config = crate::bootstrap::RouterConfig {
        control_enabled: config.enable_control,
        metrics_enabled: config.metrics_bind.is_some(),
    };
    let observability = crate::http::RouterObservability {
        metrics: metrics.clone(),
        wal_append_telemetry,
        clock: clock.clone(),
        recovery_observations,
    };
    let shared_projection = std::sync::Arc::new(std::sync::RwLock::new(projection.clone()));
    let router_state_inner = if let Some(authority) = control_authority {
        crate::http::RouterStateInner::with_control_authority(
            router_config,
            shared_projection,
            observability,
            authority,
            ready_status,
        )
    } else {
        crate::http::RouterStateInner::new(
            router_config,
            shared_projection,
            observability,
            ready_status,
        )
    };
    let router_state = std::sync::Arc::new(router_state_inner);

    // Build the concrete HTTP router using the assembly entry
    let http_router = crate::http::build_router(router_state.clone());

    // Return bootstrap state with concrete router and state handle
    Ok(BootstrapState {
        config,
        metrics,
        http_router,
        wal_path,
        snapshot_path,
        projection,
        router_state,
        clock,
        ready_status,
    })
}

fn map_recovery_error(error: RecoveryBootstrapError) -> BootstrapError {
    match error {
        RecoveryBootstrapError::WalInit(msg) => BootstrapError::WalInit(msg),
        RecoveryBootstrapError::WalRead(msg)
        | RecoveryBootstrapError::SnapshotLoad(msg)
        | RecoveryBootstrapError::WalReplay(msg)
        | RecoveryBootstrapError::SnapshotBootstrap(msg) => BootstrapError::Dependency(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_with_valid_config() {
        let config = DaemonConfig::default();
        let control_flag = config.enable_control;
        let result = bootstrap(config);

        // We expect success if data directory exists or can be created
        // The exact result may vary based on filesystem permissions
        if let Ok(state) = result {
            // Assert router state wiring
            // RouterState is Arc<RouterStateInner>, and RouterStateInner is public
            let router_state = state.router_state();

            // Access the inner fields through Arc deref
            assert!(router_state.ready_status.is_ready());
            assert_eq!(router_state.router_config.control_enabled, control_flag);
            assert_eq!(router_state.router_config.metrics_enabled, state.metrics().is_enabled());
        } else {
            // Skip assertions if bootstrap fails due to file system issues
            assert!(matches!(result, Err(BootstrapError::WalInit(_))));
        }
    }

    #[test]
    fn test_bootstrap_with_invalid_config() {
        // Test with invalid bind address (port 0)
        let config = DaemonConfig {
            bind_address: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            ..Default::default()
        };

        let result = bootstrap(config);
        assert!(matches!(result, Err(BootstrapError::Config(_))));
    }
}
