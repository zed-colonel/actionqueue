//! Authoritative WAL append metric population from storage telemetry.
//!
//! This updater reads append outcome totals emitted by the instrumented WAL
//! writer and overwrites daemon counters deterministically on each scrape.

/// Recomputes WAL append counters from authoritative storage telemetry.
///
/// Update behavior is deterministic for each scrape:
/// - Reads immutable append-success/failure totals from router state telemetry.
/// - Resets both Prometheus counters.
/// - Applies absolute values via `inc_by`.
pub fn update(state: &crate::http::RouterStateInner) {
    let collectors = state.metrics.collectors();
    let snapshot = state.wal_append_telemetry.snapshot();

    collectors.wal_append_total().reset();
    collectors.wal_append_failures_total().reset();

    collectors.wal_append_total().inc_by(snapshot.append_success_total as f64);
    collectors.wal_append_failures_total().inc_by(snapshot.append_failure_total as f64);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::writer::{WalWriter, WalWriterError};
    use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};

    use super::update;
    use crate::bootstrap::{ReadyStatus, RouterConfig};
    use crate::metrics::registry::MetricsRegistry;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    #[derive(Debug)]
    struct SuccessWriter;

    impl WalWriter for SuccessWriter {
        fn append(
            &mut self,
            _event: &actionqueue_storage::wal::event::WalEvent,
        ) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailureWriter;

    impl WalWriter for FailureWriter {
        fn append(
            &mut self,
            _event: &actionqueue_storage::wal::event::WalEvent,
        ) -> Result<(), WalWriterError> {
            Err(WalWriterError::IoError("append failed".to_string()))
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    fn sample_event(sequence: u64) -> actionqueue_storage::wal::event::WalEvent {
        actionqueue_storage::wal::event::WalEvent::new(
            sequence,
            actionqueue_storage::wal::event::WalEventType::EnginePaused { timestamp: sequence },
        )
    }

    fn build_state(
        metrics: Arc<MetricsRegistry>,
        telemetry: WalAppendTelemetry,
    ) -> crate::http::RouterStateInner {
        let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));
        crate::http::RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: true },
            Arc::new(std::sync::RwLock::new(ReplayReducer::new())),
            crate::http::RouterObservability {
                metrics,
                wal_append_telemetry: telemetry,
                clock,
                recovery_observations: RecoveryObservations::zero(),
            },
            ReadyStatus::ready(),
        )
    }

    #[test]
    fn update_maps_telemetry_totals_to_wal_counters_exactly() {
        let telemetry = WalAppendTelemetry::new();
        let mut success_writer = InstrumentedWalWriter::new(SuccessWriter, telemetry.clone());
        let mut failure_writer = InstrumentedWalWriter::new(FailureWriter, telemetry.clone());

        success_writer.append(&sample_event(1)).expect("append should succeed");
        let _ = failure_writer.append(&sample_event(2));
        let _ = failure_writer.append(&sample_event(3));

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(Arc::clone(&metrics), telemetry);
        update(&state);

        assert_eq!(metrics.collectors().wal_append_total().get(), 1.0);
        assert_eq!(metrics.collectors().wal_append_failures_total().get(), 2.0);
    }

    #[test]
    fn update_overwrites_prior_scrape_values_deterministically() {
        let telemetry_one = WalAppendTelemetry::new();
        let mut writer_one = InstrumentedWalWriter::new(SuccessWriter, telemetry_one.clone());
        writer_one.append(&sample_event(1)).expect("append should succeed");
        writer_one.append(&sample_event(2)).expect("append should succeed");

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));

        let first_state = build_state(Arc::clone(&metrics), telemetry_one);
        update(&first_state);
        assert_eq!(metrics.collectors().wal_append_total().get(), 2.0);
        assert_eq!(metrics.collectors().wal_append_failures_total().get(), 0.0);

        let telemetry_two = WalAppendTelemetry::new();
        let mut writer_two = InstrumentedWalWriter::new(FailureWriter, telemetry_two.clone());
        let _ = writer_two.append(&sample_event(10));

        let second_state = build_state(Arc::clone(&metrics), telemetry_two);
        update(&second_state);
        assert_eq!(metrics.collectors().wal_append_total().get(), 0.0);
        assert_eq!(metrics.collectors().wal_append_failures_total().get(), 1.0);
    }

    #[test]
    fn update_does_not_touch_recovery_histogram_collector() {
        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(Arc::clone(&metrics), WalAppendTelemetry::new());
        update(&state);

        assert_eq!(metrics.collectors().recovery_time_seconds().get_sample_count(), 0);
        assert_eq!(metrics.collectors().recovery_time_seconds().get_sample_sum(), 0.0);
    }
}
