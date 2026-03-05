//! Prometheus metrics surfaces for the daemon.
//!
//! This module provides the daemon-scoped metrics registry and registration
//! helpers used by the `/metrics` export route.

pub mod attempts;
pub mod recovery;
pub mod registry;
pub mod runs;
pub mod wal;

#[cfg_attr(not(test), allow(dead_code))]
/// Returns authoritative daemon Unix time for metrics derivation paths.
///
/// This function is the required source for scheduling-lag time derivation
/// and reads from the router state's injected authoritative clock handle.
pub(crate) fn lag_now(state: &crate::http::RouterStateInner) -> u64 {
    state.clock.now()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::WalAppendTelemetry;

    use super::lag_now;
    use crate::bootstrap::{ReadyStatus, RouterConfig};
    use crate::metrics::registry::MetricsRegistry;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    #[test]
    fn lag_now_uses_router_state_injected_clock() {
        let clock: SharedDaemonClock = Arc::new(MockClock::new(424_242));
        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = crate::http::RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: false },
            Arc::new(std::sync::RwLock::new(ReplayReducer::new())),
            crate::http::RouterObservability {
                metrics,
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock,
                recovery_observations: RecoveryObservations::zero(),
            },
            ReadyStatus::ready(),
        );

        assert_eq!(lag_now(&state), 424_242);
    }
}
