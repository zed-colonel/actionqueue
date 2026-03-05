//! Prometheus metrics registry for daemon observability surfaces.
//!
//! This module defines the daemon-local Prometheus registry, registers the
//! required metric families for Phase 6, and provides a text-encoding export
//! helper for the HTTP `/metrics` route.

use std::net::SocketAddr;

use prometheus::core::Collector;
use prometheus::{
    Counter, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry, TextEncoder,
};

/// Bounded run-state label values for `actionqueue_runs_total{state=...}`.
pub const RUN_STATE_LABEL_VALUES: [&str; 8] =
    ["scheduled", "ready", "leased", "running", "retry_wait", "completed", "failed", "canceled"];

/// Bounded attempt-result label values for `actionqueue_attempts_total{result=...}`.
pub const ATTEMPT_RESULT_LABEL_VALUES: [&str; 3] = ["success", "failure", "timeout"];

const METRIC_RUNS_TOTAL: &str = "actionqueue_runs_total";
const HELP_RUNS_TOTAL: &str = "Total runs by current lifecycle state.";

const METRIC_RUNS_READY: &str = "actionqueue_runs_ready";
const HELP_RUNS_READY: &str = "Number of runs currently in the ready state.";

const METRIC_RUNS_RUNNING: &str = "actionqueue_runs_running";
const HELP_RUNS_RUNNING: &str = "Number of runs currently in the running state.";

const METRIC_ATTEMPTS_TOTAL: &str = "actionqueue_attempts_total";
const HELP_ATTEMPTS_TOTAL: &str = "Total attempt outcomes by result taxonomy.";

const METRIC_SCHEDULING_LAG_SECONDS: &str = "actionqueue_scheduling_lag_seconds";
const HELP_SCHEDULING_LAG_SECONDS: &str =
    "Observed scheduling lag in seconds between eligibility and dispatch.";

const METRIC_EXECUTOR_DURATION_SECONDS: &str = "actionqueue_executor_duration_seconds";
const HELP_EXECUTOR_DURATION_SECONDS: &str = "Observed executor attempt duration in seconds.";

const METRIC_WAL_APPEND_TOTAL: &str = "actionqueue_wal_append_total";
const HELP_WAL_APPEND_TOTAL: &str = "Total successful WAL append operations.";

const METRIC_WAL_APPEND_FAILURES_TOTAL: &str = "actionqueue_wal_append_failures_total";
const HELP_WAL_APPEND_FAILURES_TOTAL: &str = "Total failed WAL append operations.";

const METRIC_RECOVERY_TIME_SECONDS: &str = "actionqueue_recovery_time_seconds";
const HELP_RECOVERY_TIME_SECONDS: &str = "Observed recovery replay duration in seconds.";

const METRIC_RECOVERY_EVENTS_APPLIED_TOTAL: &str = "actionqueue_recovery_events_applied_total";
const HELP_RECOVERY_EVENTS_APPLIED_TOTAL: &str =
    "Total recovery-applied events from snapshot hydration plus WAL replay.";

/// Typed metrics registry errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsRegistryError {
    /// Collector registration failed.
    Registration { metric: &'static str, message: String },
    /// Prometheus text encoding failed.
    Encode(String),
}

impl std::fmt::Display for MetricsRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricsRegistryError::Registration { metric, message } => {
                write!(f, "metrics_registration_failed[{metric}]: {message}")
            }
            MetricsRegistryError::Encode(message) => {
                write!(f, "metrics_encode_failed: {message}")
            }
        }
    }
}

impl std::error::Error for MetricsRegistryError {}

/// HTTP-ready encoded metrics payload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct EncodedMetrics {
    /// Prometheus content type for text exposition.
    pub content_type: String,
    /// UTF-8 encoded exposition payload.
    pub body: String,
}

/// Registered collector handles for future metric update wiring.
#[derive(Debug, Clone)]
pub struct RegisteredMetrics {
    runs_total: GaugeVec,
    runs_ready: Gauge,
    runs_running: Gauge,
    attempts_total: GaugeVec,
    scheduling_lag_seconds: Histogram,
    executor_duration_seconds: Histogram,
    wal_append_total: Counter,
    wal_append_failures_total: Counter,
    recovery_time_seconds: Histogram,
    recovery_events_applied_total: Counter,
}

impl RegisteredMetrics {
    /// Returns the `actionqueue_runs_total` gauge vector.
    pub fn runs_total(&self) -> &GaugeVec {
        &self.runs_total
    }

    /// Returns the `actionqueue_runs_ready` gauge.
    pub fn runs_ready(&self) -> &Gauge {
        &self.runs_ready
    }

    /// Returns the `actionqueue_runs_running` gauge.
    pub fn runs_running(&self) -> &Gauge {
        &self.runs_running
    }

    /// Returns the `actionqueue_attempts_total` gauge vector.
    pub fn attempts_total(&self) -> &GaugeVec {
        &self.attempts_total
    }

    /// Returns the `actionqueue_scheduling_lag_seconds` histogram.
    pub fn scheduling_lag_seconds(&self) -> &Histogram {
        &self.scheduling_lag_seconds
    }

    /// Returns the `actionqueue_executor_duration_seconds` histogram.
    pub fn executor_duration_seconds(&self) -> &Histogram {
        &self.executor_duration_seconds
    }

    /// Returns the `actionqueue_wal_append_total` counter.
    pub fn wal_append_total(&self) -> &Counter {
        &self.wal_append_total
    }

    /// Returns the `actionqueue_wal_append_failures_total` counter.
    pub fn wal_append_failures_total(&self) -> &Counter {
        &self.wal_append_failures_total
    }

    /// Returns the `actionqueue_recovery_time_seconds` histogram.
    pub fn recovery_time_seconds(&self) -> &Histogram {
        &self.recovery_time_seconds
    }

    /// Returns the `actionqueue_recovery_events_applied_total` counter.
    pub fn recovery_events_applied_total(&self) -> &Counter {
        &self.recovery_events_applied_total
    }
}

/// Daemon-scoped metrics registry and collector handles.
#[derive(Debug, Clone)]
pub struct MetricsRegistry {
    metrics_bind: Option<SocketAddr>,
    registry: Registry,
    collectors: RegisteredMetrics,
}

impl MetricsRegistry {
    /// Builds a new registry instance and registers all required collector families.
    pub fn new(metrics_bind: Option<SocketAddr>) -> Result<Self, MetricsRegistryError> {
        let registry = Registry::new();

        let runs_total = GaugeVec::new(Opts::new(METRIC_RUNS_TOTAL, HELP_RUNS_TOTAL), &["state"])
            .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_RUNS_TOTAL,
            message: error.to_string(),
        })?;
        register_collector(&registry, METRIC_RUNS_TOTAL, runs_total.clone())?;

        let runs_ready =
            Gauge::with_opts(Opts::new(METRIC_RUNS_READY, HELP_RUNS_READY)).map_err(|error| {
                MetricsRegistryError::Registration {
                    metric: METRIC_RUNS_READY,
                    message: error.to_string(),
                }
            })?;
        register_collector(&registry, METRIC_RUNS_READY, runs_ready.clone())?;

        let runs_running = Gauge::with_opts(Opts::new(METRIC_RUNS_RUNNING, HELP_RUNS_RUNNING))
            .map_err(|error| MetricsRegistryError::Registration {
                metric: METRIC_RUNS_RUNNING,
                message: error.to_string(),
            })?;
        register_collector(&registry, METRIC_RUNS_RUNNING, runs_running.clone())?;

        let attempts_total =
            GaugeVec::new(Opts::new(METRIC_ATTEMPTS_TOTAL, HELP_ATTEMPTS_TOTAL), &["result"])
                .map_err(|error| MetricsRegistryError::Registration {
                    metric: METRIC_ATTEMPTS_TOTAL,
                    message: error.to_string(),
                })?;
        register_collector(&registry, METRIC_ATTEMPTS_TOTAL, attempts_total.clone())?;

        let scheduling_lag_seconds = Histogram::with_opts(HistogramOpts::new(
            METRIC_SCHEDULING_LAG_SECONDS,
            HELP_SCHEDULING_LAG_SECONDS,
        ))
        .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_SCHEDULING_LAG_SECONDS,
            message: error.to_string(),
        })?;
        register_collector(
            &registry,
            METRIC_SCHEDULING_LAG_SECONDS,
            scheduling_lag_seconds.clone(),
        )?;

        let executor_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            METRIC_EXECUTOR_DURATION_SECONDS,
            HELP_EXECUTOR_DURATION_SECONDS,
        ))
        .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_EXECUTOR_DURATION_SECONDS,
            message: error.to_string(),
        })?;
        register_collector(
            &registry,
            METRIC_EXECUTOR_DURATION_SECONDS,
            executor_duration_seconds.clone(),
        )?;

        let wal_append_total =
            Counter::with_opts(Opts::new(METRIC_WAL_APPEND_TOTAL, HELP_WAL_APPEND_TOTAL)).map_err(
                |error| MetricsRegistryError::Registration {
                    metric: METRIC_WAL_APPEND_TOTAL,
                    message: error.to_string(),
                },
            )?;
        register_collector(&registry, METRIC_WAL_APPEND_TOTAL, wal_append_total.clone())?;

        let wal_append_failures_total = Counter::with_opts(Opts::new(
            METRIC_WAL_APPEND_FAILURES_TOTAL,
            HELP_WAL_APPEND_FAILURES_TOTAL,
        ))
        .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_WAL_APPEND_FAILURES_TOTAL,
            message: error.to_string(),
        })?;
        register_collector(
            &registry,
            METRIC_WAL_APPEND_FAILURES_TOTAL,
            wal_append_failures_total.clone(),
        )?;

        let recovery_time_seconds = Histogram::with_opts(HistogramOpts::new(
            METRIC_RECOVERY_TIME_SECONDS,
            HELP_RECOVERY_TIME_SECONDS,
        ))
        .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_RECOVERY_TIME_SECONDS,
            message: error.to_string(),
        })?;
        register_collector(&registry, METRIC_RECOVERY_TIME_SECONDS, recovery_time_seconds.clone())?;

        let recovery_events_applied_total = Counter::with_opts(Opts::new(
            METRIC_RECOVERY_EVENTS_APPLIED_TOTAL,
            HELP_RECOVERY_EVENTS_APPLIED_TOTAL,
        ))
        .map_err(|error| MetricsRegistryError::Registration {
            metric: METRIC_RECOVERY_EVENTS_APPLIED_TOTAL,
            message: error.to_string(),
        })?;
        register_collector(
            &registry,
            METRIC_RECOVERY_EVENTS_APPLIED_TOTAL,
            recovery_events_applied_total.clone(),
        )?;

        for label in RUN_STATE_LABEL_VALUES {
            let _ = runs_total.with_label_values(&[label]);
        }
        for label in ATTEMPT_RESULT_LABEL_VALUES {
            let _ = attempts_total.with_label_values(&[label]);
        }

        Ok(Self {
            metrics_bind,
            registry,
            collectors: RegisteredMetrics {
                runs_total,
                runs_ready,
                runs_running,
                attempts_total,
                scheduling_lag_seconds,
                executor_duration_seconds,
                wal_append_total,
                wal_append_failures_total,
                recovery_time_seconds,
                recovery_events_applied_total,
            },
        })
    }

    /// Returns the configured metrics bind address when enabled.
    pub fn bind_address(&self) -> Option<SocketAddr> {
        self.metrics_bind
    }

    /// Returns `true` if metrics routing is enabled.
    pub fn is_enabled(&self) -> bool {
        self.metrics_bind.is_some()
    }

    /// Returns the collector handle set.
    pub fn collectors(&self) -> &RegisteredMetrics {
        &self.collectors
    }

    /// Gathers metric families from the daemon-local registry.
    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// Encodes gathered metric families to Prometheus text exposition.
    pub fn encode_text(&self) -> Result<EncodedMetrics, MetricsRegistryError> {
        let metric_families = self.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .map_err(|error| MetricsRegistryError::Encode(error.to_string()))?;
        let body = String::from_utf8(buffer).map_err(|error| {
            MetricsRegistryError::Encode(format!(
                "metrics encoding produced non-utf8 bytes: {error}"
            ))
        })?;

        Ok(EncodedMetrics { content_type: encoder.format_type().to_string(), body })
    }
}

fn register_collector<T: Collector + Clone + 'static>(
    registry: &Registry,
    metric: &'static str,
    collector: T,
) -> Result<(), MetricsRegistryError> {
    registry
        .register(Box::new(collector))
        .map_err(|error| MetricsRegistryError::Registration { metric, message: error.to_string() })
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    #[test]
    fn registry_constructor_registers_required_families_and_labels() {
        let registry =
            MetricsRegistry::new(Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090)))
                .expect("registry should initialize");

        assert!(registry.is_enabled());
        assert_eq!(
            registry.bind_address(),
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9090))
        );

        let encoded = registry.encode_text().expect("encoding should succeed");
        assert!(encoded.body.contains(METRIC_RUNS_TOTAL));
        assert!(encoded.body.contains(METRIC_RUNS_READY));
        assert!(encoded.body.contains(METRIC_RUNS_RUNNING));
        assert!(encoded.body.contains(METRIC_ATTEMPTS_TOTAL));
        assert!(encoded.body.contains(METRIC_SCHEDULING_LAG_SECONDS));
        assert!(encoded.body.contains(METRIC_EXECUTOR_DURATION_SECONDS));
        assert!(encoded.body.contains(METRIC_WAL_APPEND_TOTAL));
        assert!(encoded.body.contains(METRIC_WAL_APPEND_FAILURES_TOTAL));
        assert!(encoded.body.contains(METRIC_RECOVERY_TIME_SECONDS));
        assert!(encoded.body.contains(METRIC_RECOVERY_EVENTS_APPLIED_TOTAL));

        for label in RUN_STATE_LABEL_VALUES {
            assert!(
                encoded.body.contains(&format!("state=\"{label}\"")),
                "missing pre-seeded run-state label {label}"
            );
        }
        for label in ATTEMPT_RESULT_LABEL_VALUES {
            assert!(
                encoded.body.contains(&format!("result=\"{label}\"")),
                "missing pre-seeded attempt-result label {label}"
            );
        }
    }

    #[test]
    fn duplicate_registration_path_returns_typed_deterministic_error() {
        let registry = Registry::new();
        let gauge = Gauge::with_opts(Opts::new(
            "actionqueue_registry_duplicate_test",
            "duplicate test gauge",
        ))
        .expect("gauge should initialize");
        register_collector(&registry, "actionqueue_registry_duplicate_test", gauge.clone())
            .expect("first registration should succeed");

        let error = register_collector(&registry, "actionqueue_registry_duplicate_test", gauge)
            .expect_err("second registration should fail deterministically");

        match error {
            MetricsRegistryError::Registration { metric, .. } => {
                assert_eq!(metric, "actionqueue_registry_duplicate_test");
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }
}
