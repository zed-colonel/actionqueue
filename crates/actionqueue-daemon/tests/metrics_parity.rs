//! Phase 6 metrics parity integration tests.
//!
//! This suite is the authoritative parity proof surface for `GET /metrics`.
//! It seeds deterministic projection, WAL telemetry, and recovery observations,
//! then verifies exact metric sample correctness plus bounded label vocabularies.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_daemon::metrics::registry::{ATTEMPT_RESULT_LABEL_VALUES, RUN_STATE_LABEL_VALUES};
use actionqueue_daemon::time::clock::{MockClock, SharedDaemonClock};
use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::writer::WalWriter;
use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};
use axum::body::Body;
use axum::http::{Method, StatusCode};
use http_body_util::BodyExt;

/// Parsed sample from Prometheus text exposition.
#[derive(Debug, Clone, PartialEq)]
struct MetricSample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

/// Builds a deterministic task specification for integration state seeding.
fn build_task_spec(task_id: TaskId, priority: i32, concurrency_key: Option<&str>) -> TaskSpec {
    let constraints = TaskConstraints::new(1, None, concurrency_key.map(str::to_string))
        .expect("constraints should be valid");
    let metadata = TaskMetadata::new(vec![], priority, None);

    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        constraints,
        metadata,
    )
    .expect("task spec should be valid")
}

/// Applies a WAL event to the authoritative replay reducer.
fn apply_event(reducer: &mut ReplayReducer, sequence: u64, event: WalEventType) {
    let event = WalEvent::new(sequence, event);
    reducer.apply(&event).expect("event should apply");
}

/// Creates a deterministic scheduled run instance.
fn run_instance_scheduled(
    run_id: RunId,
    task_id: TaskId,
    scheduled_at: u64,
    created_at: u64,
) -> RunInstance {
    RunInstance::new_scheduled_with_id(run_id, task_id, scheduled_at, created_at)
        .expect("run instance should be valid")
}

/// Seeds deterministic projection state for run and attempt parity assertions.
///
/// Final run-state distribution is exactly one run in each bounded state label:
/// scheduled, ready, leased, running, retry_wait, completed, failed, canceled.
///
/// Attempt result distribution is exactly:
/// success=1, failure=1, timeout=1.
fn seeded_projection_for_metrics_parity() -> ReplayReducer {
    let mut reducer = ReplayReducer::new();

    let task_id = TaskId::from_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
    let scheduled_id = RunId::from_str("00000000-0000-0000-0000-000000000101").unwrap();
    let ready_id = RunId::from_str("00000000-0000-0000-0000-000000000102").unwrap();
    let leased_id = RunId::from_str("00000000-0000-0000-0000-000000000103").unwrap();
    let running_id = RunId::from_str("00000000-0000-0000-0000-000000000104").unwrap();
    let retry_wait_id = RunId::from_str("00000000-0000-0000-0000-000000000105").unwrap();
    let completed_id = RunId::from_str("00000000-0000-0000-0000-000000000106").unwrap();
    let failed_id = RunId::from_str("00000000-0000-0000-0000-000000000107").unwrap();
    let canceled_id = RunId::from_str("00000000-0000-0000-0000-000000000108").unwrap();

    let attempt_timeout = AttemptId::from_str("00000000-0000-0000-0000-00000000a104").unwrap();
    let attempt_success = AttemptId::from_str("00000000-0000-0000-0000-00000000a106").unwrap();
    let attempt_failure = AttemptId::from_str("00000000-0000-0000-0000-00000000a107").unwrap();

    apply_event(
        &mut reducer,
        1,
        WalEventType::TaskCreated { task_spec: build_task_spec(task_id, 7, None), timestamp: 10 },
    );

    apply_event(
        &mut reducer,
        2,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(
                scheduled_id,
                task_id,
                1_699_999_900,
                1_699_999_900,
            ),
        },
    );

    apply_event(
        &mut reducer,
        3,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(ready_id, task_id, 1_699_999_901, 1_699_999_901),
        },
    );
    apply_event(
        &mut reducer,
        4,
        WalEventType::RunStateChanged {
            run_id: ready_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 11,
        },
    );

    apply_event(
        &mut reducer,
        5,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(leased_id, task_id, 1_699_999_902, 1_699_999_902),
        },
    );
    apply_event(
        &mut reducer,
        6,
        WalEventType::RunStateChanged {
            run_id: leased_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 12,
        },
    );
    apply_event(
        &mut reducer,
        7,
        WalEventType::RunStateChanged {
            run_id: leased_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 13,
        },
    );

    apply_event(
        &mut reducer,
        8,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(running_id, task_id, 1_699_999_903, 1_699_999_903),
        },
    );
    apply_event(
        &mut reducer,
        9,
        WalEventType::RunStateChanged {
            run_id: running_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 14,
        },
    );
    apply_event(
        &mut reducer,
        10,
        WalEventType::RunStateChanged {
            run_id: running_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 15,
        },
    );
    apply_event(
        &mut reducer,
        11,
        WalEventType::RunStateChanged {
            run_id: running_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 16,
        },
    );
    apply_event(
        &mut reducer,
        12,
        WalEventType::AttemptStarted {
            run_id: running_id,
            attempt_id: attempt_timeout,
            timestamp: 17,
        },
    );
    apply_event(
        &mut reducer,
        13,
        WalEventType::AttemptFinished {
            run_id: running_id,
            attempt_id: attempt_timeout,
            result: AttemptResultKind::Timeout,
            error: Some("attempt timed out after 5s".to_string()),
            output: None,
            timestamp: 18,
        },
    );

    apply_event(
        &mut reducer,
        14,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(
                retry_wait_id,
                task_id,
                1_699_999_904,
                1_699_999_904,
            ),
        },
    );
    apply_event(
        &mut reducer,
        15,
        WalEventType::RunStateChanged {
            run_id: retry_wait_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 19,
        },
    );
    apply_event(
        &mut reducer,
        16,
        WalEventType::RunStateChanged {
            run_id: retry_wait_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 20,
        },
    );
    apply_event(
        &mut reducer,
        17,
        WalEventType::RunStateChanged {
            run_id: retry_wait_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 21,
        },
    );
    apply_event(
        &mut reducer,
        18,
        WalEventType::RunStateChanged {
            run_id: retry_wait_id,
            previous_state: RunState::Running,
            new_state: RunState::RetryWait,
            timestamp: 22,
        },
    );

    apply_event(
        &mut reducer,
        19,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(
                completed_id,
                task_id,
                1_699_999_905,
                1_699_999_905,
            ),
        },
    );
    apply_event(
        &mut reducer,
        20,
        WalEventType::RunStateChanged {
            run_id: completed_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 23,
        },
    );
    apply_event(
        &mut reducer,
        21,
        WalEventType::RunStateChanged {
            run_id: completed_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 24,
        },
    );
    apply_event(
        &mut reducer,
        22,
        WalEventType::RunStateChanged {
            run_id: completed_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 25,
        },
    );
    apply_event(
        &mut reducer,
        23,
        WalEventType::AttemptStarted {
            run_id: completed_id,
            attempt_id: attempt_success,
            timestamp: 26,
        },
    );
    apply_event(
        &mut reducer,
        24,
        WalEventType::AttemptFinished {
            run_id: completed_id,
            attempt_id: attempt_success,
            result: AttemptResultKind::Success,
            error: None,
            output: None,
            timestamp: 27,
        },
    );
    apply_event(
        &mut reducer,
        25,
        WalEventType::RunStateChanged {
            run_id: completed_id,
            previous_state: RunState::Running,
            new_state: RunState::Completed,
            timestamp: 28,
        },
    );

    apply_event(
        &mut reducer,
        26,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(failed_id, task_id, 1_699_999_906, 1_699_999_906),
        },
    );
    apply_event(
        &mut reducer,
        27,
        WalEventType::RunStateChanged {
            run_id: failed_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 29,
        },
    );
    apply_event(
        &mut reducer,
        28,
        WalEventType::RunStateChanged {
            run_id: failed_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 30,
        },
    );
    apply_event(
        &mut reducer,
        29,
        WalEventType::RunStateChanged {
            run_id: failed_id,
            previous_state: RunState::Leased,
            new_state: RunState::Running,
            timestamp: 31,
        },
    );
    apply_event(
        &mut reducer,
        30,
        WalEventType::AttemptStarted {
            run_id: failed_id,
            attempt_id: attempt_failure,
            timestamp: 32,
        },
    );
    apply_event(
        &mut reducer,
        31,
        WalEventType::AttemptFinished {
            run_id: failed_id,
            attempt_id: attempt_failure,
            result: AttemptResultKind::Failure,
            error: Some("boom".to_string()),
            output: None,
            timestamp: 33,
        },
    );
    apply_event(
        &mut reducer,
        32,
        WalEventType::RunStateChanged {
            run_id: failed_id,
            previous_state: RunState::Running,
            new_state: RunState::Failed,
            timestamp: 34,
        },
    );

    apply_event(
        &mut reducer,
        33,
        WalEventType::RunCreated {
            run_instance: run_instance_scheduled(
                canceled_id,
                task_id,
                1_699_999_907,
                1_699_999_907,
            ),
        },
    );
    apply_event(&mut reducer, 34, WalEventType::RunCanceled { run_id: canceled_id, timestamp: 35 });

    reducer
}

/// Seeds deterministic WAL append telemetry by driving a real instrumented writer.
fn seeded_wal_append_telemetry(successes: u64, failures: u64) -> WalAppendTelemetry {
    let telemetry = WalAppendTelemetry::new();
    let unique = format!(
        "actionqueue-daemon-metrics-parity-{}-{}.wal",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    );
    let wal_path = std::env::temp_dir().join(unique);
    let wal_writer = WalFsWriter::new(wal_path.clone()).expect("test wal writer should initialize");
    let mut writer = InstrumentedWalWriter::new(wal_writer, telemetry.clone());

    let mut sequence = 1u64;
    for _ in 0..successes {
        writer
            .append(&WalEvent::new(sequence, WalEventType::EnginePaused { timestamp: sequence }))
            .expect("append success seed should succeed");
        sequence = sequence.saturating_add(1);
    }

    for _ in 0..failures {
        let stale_sequence = sequence.saturating_sub(1);
        let _ = writer.append(&WalEvent::new(
            stale_sequence,
            WalEventType::EnginePaused { timestamp: sequence },
        ));
    }

    drop(writer);
    let _ = std::fs::remove_file(wal_path);
    telemetry
}

/// Builds a metrics-enabled router using deterministic authoritative sources.
fn build_metrics_router(
    projection: ReplayReducer,
    wal_append_telemetry: WalAppendTelemetry,
    recovery_observations: RecoveryObservations,
) -> axum::Router<()> {
    let metrics_registry = Arc::new(
        actionqueue_daemon::metrics::registry::MetricsRegistry::new(Some(
            std::net::SocketAddr::from(([127, 0, 0, 1], 9090)),
        ))
        .expect("test metrics registry should initialize"),
    );
    let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));

    let router_state = Arc::new(actionqueue_daemon::http::RouterStateInner::new(
        actionqueue_daemon::bootstrap::RouterConfig {
            control_enabled: false,
            metrics_enabled: true,
        },
        Arc::new(std::sync::RwLock::new(projection)),
        actionqueue_daemon::http::RouterObservability {
            metrics: metrics_registry,
            wal_append_telemetry,
            clock,
            recovery_observations,
        },
        actionqueue_daemon::bootstrap::ReadyStatus::ready(),
    ));

    actionqueue_daemon::http::build_router(router_state).with_state(())
}

/// Sends an HTTP request against the in-process router.
async fn send_request(router: &mut axum::Router<()>, path: &str) -> axum::response::Response {
    use axum::http::Request;
    use tower::Service;

    let request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Body::empty())
        .expect("request should build");
    let mut service = router.as_service::<Body>();
    service.call(request).await.expect("request should succeed")
}

/// Scrapes `/metrics` and returns the raw exposition body.
async fn scrape_metrics_body(router: &mut axum::Router<()>) -> String {
    let response = send_request(router, "/metrics").await;
    assert_eq!(response.status(), StatusCode::OK);

    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .expect("content-type header should exist")
        .to_str()
        .expect("content-type should be valid ascii")
        .to_string();
    assert_eq!(content_type, "text/plain; version=0.0.4");

    let bytes = response.into_body().collect().await.expect("body should collect").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("metrics response should be utf-8")
}

/// Parses Prometheus text exposition into structured samples.
fn parse_metrics_samples(body: &str) -> Vec<MetricSample> {
    body.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let mut parts = trimmed.split_whitespace();
            let metric_key = parts.next().expect("metric key should exist");
            let value = parts
                .next()
                .expect("metric value should exist")
                .parse::<f64>()
                .expect("metric value should parse as f64");

            let (name, labels) = parse_metric_key(metric_key);
            Some(MetricSample { name, labels, value })
        })
        .collect()
}

/// Parses `metric{label="value"}` into metric name and label map.
fn parse_metric_key(metric_key: &str) -> (String, BTreeMap<String, String>) {
    let Some(label_start) = metric_key.find('{') else {
        return (metric_key.to_string(), BTreeMap::new());
    };

    let label_end = metric_key.rfind('}').expect("labeled metric key should include closing brace");
    let name = metric_key[..label_start].to_string();
    let mut labels = BTreeMap::new();
    let labels_str = &metric_key[label_start + 1..label_end];

    if labels_str.is_empty() {
        return (name, labels);
    }

    for assignment in labels_str.split(',') {
        let mut key_value = assignment.splitn(2, '=');
        let key = key_value.next().expect("label key should exist").to_string();
        let raw_value = key_value.next().expect("label value should exist");
        let value = raw_value.trim_matches('"').to_string();
        labels.insert(key, value);
    }

    (name, labels)
}

/// Returns a sample value by exact metric name plus exact label set.
fn sample_value(samples: &[MetricSample], metric_name: &str, labels: &[(&str, &str)]) -> f64 {
    let expected_labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<BTreeMap<_, _>>();

    samples
        .iter()
        .find(|sample| sample.name == metric_name && sample.labels == expected_labels)
        .unwrap_or_else(|| {
            panic!("missing metric sample: {metric_name} with labels {expected_labels:?}")
        })
        .value
}

/// Collects all values for a label key within a metric family.
fn label_values_for_family(
    samples: &[MetricSample],
    metric_name: &str,
    label_key: &str,
) -> BTreeSet<String> {
    samples
        .iter()
        .filter(|sample| sample.name == metric_name)
        .map(|sample| {
            sample
                .labels
                .get(label_key)
                .unwrap_or_else(|| {
                    panic!("missing required label key {label_key} on {metric_name}")
                })
                .clone()
        })
        .collect()
}

/// Collects all label keys present in a metric family.
fn label_keys_for_family(samples: &[MetricSample], metric_name: &str) -> BTreeSet<String> {
    samples
        .iter()
        .filter(|sample| sample.name == metric_name)
        .flat_map(|sample| sample.labels.keys().cloned())
        .collect()
}

/// Returns true if any sample name starts with `prefix`.
fn has_metric_prefix(samples: &[MetricSample], prefix: &str) -> bool {
    samples.iter().any(|sample| sample.name.starts_with(prefix))
}

/// Builds a parity-test router with deterministic authoritative seeds.
fn seeded_parity_router() -> axum::Router<()> {
    let projection = seeded_projection_for_metrics_parity();
    let wal_append_telemetry = seeded_wal_append_telemetry(3, 2);
    let recovery_observations = RecoveryObservations {
        recovery_duration_seconds: 0.75,
        events_applied_total: 7,
        snapshot_events_applied: 5,
        wal_replay_events_applied: 2,
    };

    build_metrics_router(projection, wal_append_telemetry, recovery_observations)
}

#[tokio::test]
async fn metrics_parity_asserts_all_required_family_values_from_authoritative_seeds() {
    let mut router = seeded_parity_router();
    let body = scrape_metrics_body(&mut router).await;
    let samples = parse_metrics_samples(&body);

    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "scheduled")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "ready")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "leased")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "running")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "retry_wait")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "completed")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "failed")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_total", &[("state", "canceled")]), 1.0);

    assert_eq!(sample_value(&samples, "actionqueue_runs_ready", &[]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_runs_running", &[]), 1.0);

    assert_eq!(sample_value(&samples, "actionqueue_attempts_total", &[("result", "success")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_attempts_total", &[("result", "failure")]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_attempts_total", &[("result", "timeout")]), 1.0);

    assert_eq!(sample_value(&samples, "actionqueue_scheduling_lag_seconds_count", &[]), 4.0);
    assert_eq!(sample_value(&samples, "actionqueue_scheduling_lag_seconds_sum", &[]), 390.0);

    assert_eq!(sample_value(&samples, "actionqueue_wal_append_total", &[]), 3.0);
    assert_eq!(sample_value(&samples, "actionqueue_wal_append_failures_total", &[]), 2.0);

    assert_eq!(sample_value(&samples, "actionqueue_recovery_events_applied_total", &[]), 7.0);
    assert_eq!(sample_value(&samples, "actionqueue_recovery_time_seconds_count", &[]), 1.0);
    assert_eq!(sample_value(&samples, "actionqueue_recovery_time_seconds_sum", &[]), 0.75);
}

#[tokio::test]
async fn metrics_parity_enforces_bounded_run_and_attempt_labels_without_extras() {
    let mut router = seeded_parity_router();
    let body = scrape_metrics_body(&mut router).await;
    let samples = parse_metrics_samples(&body);

    let expected_run_states =
        RUN_STATE_LABEL_VALUES.into_iter().map(str::to_string).collect::<BTreeSet<_>>();
    let expected_attempt_results =
        ATTEMPT_RESULT_LABEL_VALUES.into_iter().map(str::to_string).collect::<BTreeSet<_>>();

    assert_eq!(
        label_values_for_family(&samples, "actionqueue_runs_total", "state"),
        expected_run_states
    );
    assert_eq!(
        label_values_for_family(&samples, "actionqueue_attempts_total", "result"),
        expected_attempt_results
    );

    assert_eq!(
        label_keys_for_family(&samples, "actionqueue_runs_total"),
        BTreeSet::from(["state".to_string()])
    );
    assert_eq!(
        label_keys_for_family(&samples, "actionqueue_attempts_total"),
        BTreeSet::from(["result".to_string()])
    );

    assert_eq!(samples.iter().filter(|sample| sample.name == "actionqueue_runs_total").count(), 8);
    assert_eq!(
        samples.iter().filter(|sample| sample.name == "actionqueue_attempts_total").count(),
        3
    );
}

#[tokio::test]
async fn metrics_parity_emits_all_required_metric_families() {
    let mut router = seeded_parity_router();
    let body = scrape_metrics_body(&mut router).await;
    let samples = parse_metrics_samples(&body);

    assert!(has_metric_prefix(&samples, "actionqueue_runs_total"));
    assert!(has_metric_prefix(&samples, "actionqueue_runs_ready"));
    assert!(has_metric_prefix(&samples, "actionqueue_runs_running"));
    assert!(has_metric_prefix(&samples, "actionqueue_attempts_total"));
    assert!(has_metric_prefix(&samples, "actionqueue_scheduling_lag_seconds_"));
    assert!(has_metric_prefix(&samples, "actionqueue_wal_append_total"));
    assert!(has_metric_prefix(&samples, "actionqueue_wal_append_failures_total"));
    assert!(has_metric_prefix(&samples, "actionqueue_recovery_time_seconds_"));
    assert!(has_metric_prefix(&samples, "actionqueue_recovery_events_applied_total"));
}
