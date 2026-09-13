//! HTTP route modules.
//!
//! This module provides the HTTP introspection surface for the ActionQueue daemon.
//! Routes are organized by functionality:
//!
//! - [`health`] - Liveness endpoint (`GET /healthz`)
//! - [`ready`] - Readiness endpoint (`GET /ready`)
//!
//! V2 operations are registered by [`api`].

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

#[cfg(feature = "actor")]
use actionqueue_core::mutation::MutationAuthority;
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
/// Mutations use the serialized authority lane. Inspection snapshots and current
/// grant checks share a single authoritative projection revision.
pub struct RouterStateInner {
    pub(crate) admission_throttle: Mutex<admission_throttle::AdmissionThrottle>,
    pub(crate) background_maintenance: bool,
    pub(crate) disclosure_policy: actionqueue_runtime::inspection::DisclosurePolicy,
    pub(crate) authority_lane: Arc<tokio::sync::Semaphore>,
    pub(crate) operational_failed: AtomicBool,
    pub(crate) host_authenticator: Option<auth::HostAuthenticator>,
    pub(crate) store_session: Option<actionqueue_storage::store::StoreSession>,
    /// Router configuration for routing decisions.
    ///
    /// Used by [`build_router`] to determine which optional route sets
    /// (control endpoints, metrics) are registered.
    pub(crate) router_config: RouterConfig,
    #[cfg(feature = "actor")]
    pub(crate) remote_policy: actionqueue_runtime::remote::RemotePolicy,
    pub(crate) maintenance_started: AtomicBool,
    pub(crate) maintenance_stopping: AtomicBool,
    pub(crate) maintenance_task: Mutex<Option<tokio::task::JoinHandle<()>>>,

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
    /// Offline adapters retain exclusive storage ownership but do not drive execution.
    pub fn without_background_maintenance(mut self) -> Self {
        self.background_maintenance = false;
        self
    }
    /// Trusted disclosure policy; request query flags do not grant this authority.
    pub fn with_disclosure_policy(
        mut self,
        policy: actionqueue_runtime::inspection::DisclosurePolicy,
    ) -> Self {
        self.disclosure_policy = policy;
        self
    }
    /// Installs the trusted host authentication hook before building the router.
    pub fn with_host_authenticator(mut self, hook: auth::HostAuthenticator) -> Self {
        self.host_authenticator = Some(hook);
        self
    }

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
            #[cfg(feature = "actor")]
            remote_policy: Default::default(),
            maintenance_started: AtomicBool::new(false),
            maintenance_stopping: AtomicBool::new(false),
            maintenance_task: Mutex::new(None),
            background_maintenance: true,
            disclosure_policy: Default::default(),
            admission_throttle: Mutex::new(Default::default()),
            authority_lane: Arc::new(tokio::sync::Semaphore::new(32)),
            operational_failed: AtomicBool::new(false),
            host_authenticator: None,
            store_session: None,
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
            #[cfg(feature = "actor")]
            remote_policy: Default::default(),
            maintenance_started: AtomicBool::new(false),
            maintenance_stopping: AtomicBool::new(false),
            maintenance_task: Mutex::new(None),
            background_maintenance: true,
            disclosure_policy: Default::default(),
            admission_throttle: Mutex::new(Default::default()),
            authority_lane: Arc::new(tokio::sync::Semaphore::new(32)),
            operational_failed: AtomicBool::new(false),
            host_authenticator: None,
            store_session: control_authority.lock().ok().and_then(|a| a.store_session().cloned()),
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
mod admission_throttle;
pub mod api;
pub mod auth;
pub mod control;
pub mod health;
pub mod maintenance;
pub mod metrics;
#[cfg(feature = "platform")]
pub mod platform;
pub mod ready;
pub mod stats;

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
        StatusCode::SERVICE_UNAVAILABLE,
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
    maintenance::start(&state);
    let control_enabled = state.router_config.control_enabled;
    let metrics_enabled = state.router_config.metrics_enabled;
    let router: axum::Router<RouterState> = axum::Router::new();
    let router = health::register_routes(router);
    let router = ready::register_routes(router);
    let router = stats::register_routes(router);
    let inspection = api::reads().route_layer(axum::middleware::from_fn_with_state(
        state.clone(),
        auth::authenticate_inspection,
    ));
    let router = router.merge(inspection);
    let router = metrics::register_routes(router, metrics_enabled);
    let controls = axum::Router::new();
    let controls = control::register_routes(controls, control_enabled);
    #[cfg(feature = "actor")]
    let controls = if control_enabled { actors::register_routes(controls) } else { controls };
    #[cfg(feature = "platform")]
    let controls = if control_enabled { platform::register_routes(controls) } else { controls };
    let controls = if control_enabled {
        controls
            .route_layer(axum::middleware::from_fn_with_state(state.clone(), api::blocking_adapter))
            .merge(api::writes())
    } else {
        controls
    };
    let controls = if control_enabled {
        controls
            .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth::authenticate))
    } else {
        controls
    };
    router
        .merge(controls)
        .layer(axum::middleware::from_fn(api::sanitize_errors))
        .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(state)
}

// The caller returns this response directly to Axum on the exceptional path.
#[allow(clippy::result_large_err)]
pub(crate) fn sync_projection<W: actionqueue_storage::wal::writer::WalWriter>(
    state: &RouterState,
    a: &StorageMutationAuthority<W, ReplayReducer>,
) -> Result<(), axum::response::Response> {
    let mut published = write_projection(state).map_err(|e| *e)?;
    // Equal revisions must describe the same authoritative facts. Different
    // revisions are expected while publishing a newly committed prefix.
    if published.latest_sequence() == a.projection().latest_sequence()
        && !matches!((published.projection_digest(), a.projection().projection_digest()), (Ok(left), Ok(right)) if left == right)
    {
        if !state.operational_failed.swap(true, Ordering::AcqRel) {
            a.telemetry().projection_mismatch();
        }
        return Err(projection_poison_response());
    }
    *published = a.projection().clone();
    Ok(())
}
#[cfg(feature = "actor")]
pub(crate) fn execute_host_mutation<W: actionqueue_storage::wal::writer::WalWriter>(
    state: &RouterState,
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    host: &actionqueue_core::control::HostControlContext,
    command: actionqueue_core::mutation::MutationCommand,
) -> Result<actionqueue_core::mutation::MutationOutcome, actionqueue_runtime::control::ServiceError>
{
    let result = a
        .submit_command(
            command.with_control(host),
            actionqueue_core::mutation::DurabilityPolicy::Immediate,
        )
        .map_err(actionqueue_runtime::control::ServiceError::Storage);
    sync_projection(state, a).map_err(|_| {
        state.operational_failed.store(true, Ordering::Release);
        actionqueue_runtime::control::ServiceError::Storage(
            actionqueue_storage::mutation::MutationAuthorityError::RecoveryRequired,
        )
    })?;
    if a.recovery_required() {
        state.operational_failed.store(true, Ordering::Release);
    }
    result
}

/// Inspection snapshots and grant checks use the same authoritative revision.
pub(crate) fn read_inspection_projection(
    state: &RouterState,
) -> Result<ReplayReducer, Box<axum::response::Response>> {
    if let Some(authority) = &state.control_authority {
        return authority
            .lock()
            .map(|a| a.projection().clone())
            .map_err(|_| Box::new(projection_poison_response()));
    }
    read_projection(state).map(|p| p.clone())
}

#[cfg(all(test, feature = "platform"))]
mod tenant_tests;
