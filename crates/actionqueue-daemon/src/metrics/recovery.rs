//! Authoritative recovery metrics population from bootstrap observations.
//!
//! This updater reads immutable recovery observations captured during bootstrap
//! and populates recovery metric families deterministically on scrape.

use std::sync::atomic::Ordering;

/// Recomputes recovery metric families from authoritative bootstrap observations.
///
/// Update behavior is deterministic for each scrape:
/// - Resets and overwrites `actionqueue_recovery_events_applied_total` as an absolute value.
/// - Observes `actionqueue_recovery_time_seconds` exactly once per process lifetime.
pub fn update(state: &crate::http::RouterStateInner) {
    let collectors = state.metrics.collectors();

    collectors.recovery_events_applied_total().reset();
    collectors
        .recovery_events_applied_total()
        .inc_by(state.recovery_observations.events_applied_total as f64);

    if state
        .recovery_histogram_observed
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        collectors
            .recovery_time_seconds()
            .observe(state.recovery_observations.recovery_duration_seconds);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::WalAppendTelemetry;

    use super::update;
    use crate::bootstrap::{ReadyStatus, RouterConfig};
    use crate::metrics::registry::MetricsRegistry;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    fn build_state(
        metrics: Arc<MetricsRegistry>,
        recovery_observations: RecoveryObservations,
    ) -> crate::http::RouterStateInner {
        let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));
        crate::http::RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: true },
            Arc::new(std::sync::RwLock::new(ReplayReducer::new())),
            crate::http::RouterObservability {
                metrics,
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock,
                recovery_observations,
            },
            ReadyStatus::ready(),
        )
    }

    #[test]
    fn update_overwrites_counter_from_authoritative_total() {
        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let first_state = build_state(
            Arc::clone(&metrics),
            RecoveryObservations {
                recovery_duration_seconds: 0.25,
                events_applied_total: 11,
                snapshot_events_applied: 7,
                wal_replay_events_applied: 4,
            },
        );

        update(&first_state);
        assert_eq!(metrics.collectors().recovery_events_applied_total().get(), 11.0);

        let second_state = build_state(
            Arc::clone(&metrics),
            RecoveryObservations {
                recovery_duration_seconds: 0.5,
                events_applied_total: 3,
                snapshot_events_applied: 1,
                wal_replay_events_applied: 2,
            },
        );

        update(&second_state);
        assert_eq!(metrics.collectors().recovery_events_applied_total().get(), 3.0);
    }

    #[test]
    fn update_observes_histogram_once_per_process_lifetime() {
        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(
            Arc::clone(&metrics),
            RecoveryObservations {
                recovery_duration_seconds: 0.125,
                events_applied_total: 1,
                snapshot_events_applied: 1,
                wal_replay_events_applied: 0,
            },
        );

        let before_count = metrics.collectors().recovery_time_seconds().get_sample_count();

        update(&state);
        update(&state);

        let after_count = metrics.collectors().recovery_time_seconds().get_sample_count();
        assert_eq!(after_count, before_count + 1);
    }
}
