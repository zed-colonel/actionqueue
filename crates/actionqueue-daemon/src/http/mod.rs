//! HTTP route modules.
//!
//! This module provides the HTTP introspection surface for the ActionQueue daemon.
//! Routes are organized by functionality:
//!
//! - [`health`] - Liveness endpoint (`GET /healthz`)
//! - [`ready`] - Readiness endpoint (`GET /ready`)
//! - [`stats`] - Aggregate statistics endpoint (`GET /api/v1/stats`)
//! - [`tasks_list`] - Task listing endpoint (`GET /api/v1/tasks`)
//! - [`task_get`] - Single task endpoint (`GET /api/v1/tasks/:task_id`)
//! - [`runs_list`] - Run listing endpoint (`GET /api/v1/runs`)
//! - [`run_get`] - Single run endpoint (`GET /api/v1/runs/:run_id`)
//! - [`control`] - Feature-gated control endpoints (cancel + pause/resume)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};

use crate::bootstrap::{ReadyStatus, RouterConfig};
use crate::metrics::registry::MetricsRegistry;
use crate::time::clock::SharedDaemonClock;

/// Shared control mutation authority type used by control handlers.
pub type ControlMutationAuthority =
    Arc<Mutex<StorageMutationAuthority<InstrumentedWalWriter<WalFsWriter>, ReplayReducer>>>;

/// Router state shared across all HTTP handlers.
///
/// This struct holds the state that is accessible to all read-only introspection
/// endpoints (health, ready, stats). It is wrapped in `Arc` to enable cloning
/// as required by axum's router state system.
///
/// # Invariant boundaries
///
/// This state is read-only. Handlers must not mutate any fields or introduce
/// interior mutability beyond `Arc` cloning.
pub struct RouterStateInner {
    /// Router configuration for routing decisions.
    ///
    /// Used by [`build_router`] to determine which optional route sets
    /// (control endpoints, metrics) are registered.
    pub(crate) router_config: RouterConfig,

    /// Shared projection state for stats and introspection.
    ///
    /// Wrapped in `Arc<RwLock<>>` so control handlers can sync the
    /// authority's updated projection after mutations, while read handlers
    /// acquire a read lock for consistent snapshots.
    pub(crate) shared_projection: Arc<RwLock<ReplayReducer>>,

    /// Optional control mutation authority lane for feature-gated control handlers.
    pub(crate) control_authority: Option<ControlMutationAuthority>,

    /// Shared daemon-local metrics registry handle.
    pub(crate) metrics: Arc<MetricsRegistry>,

    /// Authoritative WAL append telemetry for scrape-time WAL counter updates.
    pub(crate) wal_append_telemetry: WalAppendTelemetry,

    /// Authoritative daemon clock handle used by metrics derivation paths.
    pub(crate) clock: SharedDaemonClock,

    /// Authoritative recovery observations captured during bootstrap.
    pub(crate) recovery_observations: RecoveryObservations,

    /// Idempotence guard for recovery histogram observe-once semantics.
    pub(crate) recovery_histogram_observed: AtomicBool,

    /// Ready status indicating daemon readiness derived from bootstrap state.
    pub(crate) ready_status: ReadyStatus,
}

impl std::fmt::Debug for RouterStateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterStateInner")
            .field("router_config", &self.router_config)
            .field("shared_projection", &"Arc<RwLock<ReplayReducer>>")
            .field("control_authority", &self.control_authority.is_some())
            .field("metrics_enabled", &self.metrics.is_enabled())
            .field("wal_append_telemetry", &self.wal_append_telemetry.snapshot())
            .field("clock_refcount", &Arc::strong_count(&self.clock))
            .field("recovery_observations", &self.recovery_observations)
            .field(
                "recovery_histogram_observed",
                &self.recovery_histogram_observed.load(Ordering::Relaxed),
            )
            .field("ready_status", &self.ready_status)
            .finish()
    }
}

/// Shared router state type (Arc-wrapped inner struct for Clone compatibility).
pub type RouterState = Arc<RouterStateInner>;

/// Observability dependencies grouped for router state construction.
///
/// Groups the telemetry, metrics, clock, and recovery observation handles
/// that are threaded through the daemon HTTP layer.
pub struct RouterObservability {
    /// Shared daemon-local metrics registry handle.
    pub metrics: Arc<MetricsRegistry>,
    /// Authoritative WAL append telemetry for scrape-time WAL counter updates.
    pub wal_append_telemetry: WalAppendTelemetry,
    /// Authoritative daemon clock handle used by metrics derivation paths.
    pub clock: SharedDaemonClock,
    /// Authoritative recovery observations captured during bootstrap.
    pub recovery_observations: RecoveryObservations,
}

impl RouterStateInner {
    /// Creates a new router state from bootstrap components.
    ///
    /// The `ready_status` field is derived from
    /// [`BootstrapState::ready_status()`](crate::bootstrap::BootstrapState::ready_status()).
    pub fn new(
        router_config: RouterConfig,
        shared_projection: Arc<RwLock<ReplayReducer>>,
        observability: RouterObservability,
        ready_status: ReadyStatus,
    ) -> Self {
        Self {
            router_config,
            shared_projection,
            control_authority: None,
            metrics: observability.metrics,
            wal_append_telemetry: observability.wal_append_telemetry,
            clock: observability.clock,
            recovery_observations: observability.recovery_observations,
            recovery_histogram_observed: AtomicBool::new(false),
            ready_status,
        }
    }

    /// Creates router state with control mutation authority context.
    pub fn with_control_authority(
        router_config: RouterConfig,
        shared_projection: Arc<RwLock<ReplayReducer>>,
        observability: RouterObservability,
        control_authority: ControlMutationAuthority,
        ready_status: ReadyStatus,
    ) -> Self {
        Self {
            router_config,
            shared_projection,
            control_authority: Some(control_authority),
            metrics: observability.metrics,
            wal_append_telemetry: observability.wal_append_telemetry,
            clock: observability.clock,
            recovery_observations: observability.recovery_observations,
            recovery_histogram_observed: AtomicBool::new(false),
            ready_status,
        }
    }
}

#[cfg(feature = "actor")]
pub mod actors;
pub mod control;
pub mod health;
pub mod metrics;
pub mod pagination;
#[cfg(feature = "platform")]
pub mod platform;
pub mod ready;
pub mod run_get;
pub mod runs_list;
pub mod stats;
pub mod task_get;
pub mod tasks_list;

/// Acquires a read lock on the shared projection, returning HTTP 500 on poison.
pub(crate) fn read_projection(
    state: &RouterStateInner,
) -> Result<std::sync::RwLockReadGuard<'_, ReplayReducer>, Box<axum::response::Response>> {
    state.shared_projection.read().map_err(|_| {
        tracing::error!("shared projection RwLock poisoned — read handler degraded");
        Box::new(projection_poison_response())
    })
}

/// Acquires a write lock on the shared projection, returning HTTP 500 on poison.
pub(crate) fn write_projection(
    state: &RouterStateInner,
) -> Result<std::sync::RwLockWriteGuard<'_, ReplayReducer>, Box<axum::response::Response>> {
    state.shared_projection.write().map_err(|_| {
        tracing::error!("shared projection RwLock poisoned — write handler degraded");
        Box::new(projection_poison_response())
    })
}

fn projection_poison_response() -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::Json;

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "error": "internal_error",
            "message": "shared projection lock poisoned"
        })),
    )
        .into_response()
}

/// Builds the HTTP router with all registered routes.
///
/// This function constructs an axum router and registers all read-only
/// introspection routes (health, ready, stats) with the provided state.
///
/// # Arguments
///
/// * `state` - The shared router state containing configuration and projection
///
/// # Returns
///
/// An axum Router configured with all registered routes and the shared state.
pub fn build_router(state: RouterState) -> axum::Router {
    let control_enabled = state.router_config.control_enabled;
    let metrics_enabled = state.router_config.metrics_enabled;
    let router: axum::Router<RouterState> = axum::Router::new();
    let router = health::register_routes(router);
    let router = ready::register_routes(router);
    let router = stats::register_routes(router);
    let router = tasks_list::register_routes(router);
    let router = runs_list::register_routes(router);
    let router = run_get::register_routes(router);
    let router = task_get::register_routes(router);
    let router = metrics::register_routes(router, metrics_enabled);
    let router = control::register_routes(router, control_enabled);
    #[cfg(feature = "actor")]
    let router = actors::register_routes(router);
    #[cfg(feature = "platform")]
    let router = platform::register_routes(router);
    router.with_state(state)
}
