//! P8-006 Observability acceptance proof suite.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::str::FromStr;

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::state::RunState;
use serde_json::Value;

/// Locked scenario contract for P8-006 observability acceptance proofs.
#[derive(Debug, Clone, Copy)]
struct ObservabilityScenarioSpec {
    /// Human-readable deterministic scenario label used for isolated data-dir naming.
    label: &'static str,
    /// Exact `/api/v1/stats` expectation before restart.
    expected_stats_pre_restart: StatsTruth,
    /// Exact `/api/v1/stats` expectation after restart.
    expected_stats_post_restart: StatsTruth,
    /// Exact `/metrics` expectation before restart.
    expected_metrics_pre_restart: MetricsTruth,
    /// Exact `/metrics` expectation after restart.
    expected_metrics_post_restart: MetricsTruth,
}

use support::{MetricsTruth, StatsTruth};

/// Deterministic per-row assertion contract for `/api/v1/runs`.
#[derive(Debug, Clone)]
struct RunsListTruth {
    run_id: RunId,
    task_id: String,
    expected_state: &'static str,
    expected_attempt_count: u64,
    expected_concurrency_key: Option<&'static str>,
}

/// Deterministic per-run assertion contract for `/api/v1/runs/:id`.
#[derive(Debug, Clone)]
struct RunGetTruth {
    run_id: RunId,
    task_id: String,
    expected_state: &'static str,
    expected_block_reason: Option<&'static str>,
    expected_attempt_ids: Vec<String>,
    expected_attempt_results: Vec<String>,
    expected_state_history: Vec<(Option<String>, String)>,
}

/// Canonicalized metrics fact set used for restart parity snapshots.
#[derive(Debug, Clone, PartialEq)]
struct MetricsFactSnapshot {
    run_state_values: BTreeMap<String, f64>,
    attempts_result_values: BTreeMap<String, f64>,
    runs_ready: f64,
    runs_running: f64,
    run_state_label_values: BTreeSet<String>,
    attempt_result_label_values: BTreeSet<String>,
    run_state_label_keys: BTreeSet<String>,
    attempt_result_label_keys: BTreeSet<String>,
    run_state_sample_count: usize,
    attempt_result_sample_count: usize,
    family_prefix_presence: BTreeMap<String, bool>,
}

/// Canonicalized read-surface snapshot used for restart parity assertions.
#[derive(Debug, Clone, PartialEq)]
struct ReadSurfaceSnapshot {
    runs_rows_sorted_by_run_id: Vec<Value>,
    run_get_by_run_id: BTreeMap<String, Value>,
    stats_payload: Value,
    metrics_facts: MetricsFactSnapshot,
}

/// Returns deterministic state-history truth tuples from compact string literals.
fn history_truth(entries: &[(Option<&str>, &str)]) -> Vec<(Option<String>, String)> {
    entries.iter().map(|(from, to)| (from.map(str::to_string), (*to).to_string())).collect()
}

/// Builds deterministic required-metric-family prefix set for observability assertions.
fn required_metric_family_prefixes() -> &'static [&'static str] {
    &[
        "actionqueue_runs_total",
        "actionqueue_runs_ready",
        "actionqueue_runs_running",
        "actionqueue_attempts_total",
        "actionqueue_scheduling_lag_seconds_",
        "actionqueue_wal_append_total",
        "actionqueue_wal_append_failures_total",
        "actionqueue_recovery_time_seconds_",
        "actionqueue_recovery_events_applied_total",
    ]
}

/// Asserts deterministic `/api/v1/runs` identity and lifecycle-summary truth.
async fn assert_runs_list_truth(router: &mut axum::Router<()>, expected_rows: &[RunsListTruth]) {
    let actual_rows = support::runs_list_rows_sorted_by_run_id(router).await;

    let mut expected_sorted = expected_rows.to_vec();
    expected_sorted.sort_by(|left, right| left.run_id.to_string().cmp(&right.run_id.to_string()));

    assert_eq!(
        actual_rows.len(),
        expected_sorted.len(),
        "runs row cardinality must match expected"
    );

    for (actual, expected) in actual_rows.iter().zip(expected_sorted.iter()) {
        assert_eq!(actual["run_id"], expected.run_id.to_string());
        assert_eq!(actual["task_id"], expected.task_id);
        assert_eq!(actual["state"], expected.expected_state);
        assert_eq!(actual["attempt_count"], expected.expected_attempt_count);
        match expected.expected_concurrency_key {
            Some(key) => assert_eq!(actual["concurrency_key"], key),
            None => assert!(actual["concurrency_key"].is_null(), "concurrency_key must be null"),
        }
    }
}

/// Asserts deterministic `/api/v1/runs/:id` identity, lineage, and lifecycle truth.
async fn assert_run_get_truth(router: &mut axum::Router<()>, expected_runs: &[RunGetTruth]) {
    for expected in expected_runs {
        let payload = support::run_get(router, expected.run_id).await;
        assert_eq!(payload["run_id"], expected.run_id.to_string());
        assert_eq!(payload["task_id"], expected.task_id);
        assert_eq!(payload["state"], expected.expected_state);
        assert_eq!(payload["attempt_count"], expected.expected_attempt_ids.len());

        match expected.expected_block_reason {
            Some(reason) => assert_eq!(payload["block_reason"], reason),
            None => {
                assert!(payload["block_reason"].is_null(), "unblocked run must expose null reason")
            }
        }

        let attempts = payload["attempts"].as_array().expect("attempts must be an array");
        let actual_attempt_ids = attempts
            .iter()
            .map(|entry| {
                entry["attempt_id"]
                    .as_str()
                    .expect("attempt_id should be serialized as string")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let actual_attempt_results = attempts
            .iter()
            .map(|entry| {
                entry["result"]
                    .as_str()
                    .expect("result should be present and serialized as string")
                    .to_string()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_attempt_ids, expected.expected_attempt_ids);
        assert_eq!(actual_attempt_results, expected.expected_attempt_results);

        let actual_history = payload["state_history"]
            .as_array()
            .expect("state_history should be an array")
            .iter()
            .map(|entry| {
                (
                    entry["from"].as_str().map(str::to_string),
                    entry["to"]
                        .as_str()
                        .expect("state history `to` should be serialized as string")
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_history, expected.expected_state_history);
    }
}

/// Asserts deterministic aggregate parity truth from `/api/v1/stats`.
async fn assert_stats_truth(router: &mut axum::Router<()>, expected: StatsTruth) {
    support::assert_stats_truth(router, expected).await;
}

/// Asserts deterministic metrics value parity truth from `/metrics`.
async fn assert_metrics_value_truth(router: &mut axum::Router<()>, expected: MetricsTruth) {
    support::assert_metrics_truth(router, expected).await;
}

/// Asserts bounded labels, bounded cardinality, and required metrics family presence.
async fn assert_metrics_label_bounds_and_families(router: &mut axum::Router<()>) {
    let metrics = support::get_text(router, "/metrics").await;

    let expected_run_labels: BTreeSet<String> = [
        "scheduled",
        "ready",
        "leased",
        "running",
        "retry_wait",
        "completed",
        "failed",
        "canceled",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    let expected_attempt_labels: BTreeSet<String> =
        ["success", "failure", "timeout"].into_iter().map(str::to_string).collect();
    let expected_run_keys: BTreeSet<String> = ["state"].into_iter().map(str::to_string).collect();
    let expected_attempt_keys: BTreeSet<String> =
        ["result"].into_iter().map(str::to_string).collect();

    assert_eq!(
        support::metrics_label_values(&metrics, "actionqueue_runs_total", "state"),
        expected_run_labels
    );
    assert_eq!(
        support::metrics_label_values(&metrics, "actionqueue_attempts_total", "result"),
        expected_attempt_labels
    );
    assert_eq!(support::metrics_label_keys(&metrics, "actionqueue_runs_total"), expected_run_keys);
    assert_eq!(
        support::metrics_label_keys(&metrics, "actionqueue_attempts_total"),
        expected_attempt_keys
    );

    assert_eq!(support::metrics_sample_count(&metrics, "actionqueue_runs_total"), 8);
    assert_eq!(support::metrics_sample_count(&metrics, "actionqueue_attempts_total"), 3);

    for prefix in required_metric_family_prefixes() {
        assert!(
            support::metrics_has_prefix(&metrics, prefix),
            "required metrics family prefix should exist: {prefix}"
        );
    }
}

/// Captures canonicalized metrics fact set for restart parity snapshots.
fn capture_metrics_fact_snapshot(metrics_text: &str) -> MetricsFactSnapshot {
    let run_state_values = [
        "scheduled",
        "ready",
        "leased",
        "running",
        "retry_wait",
        "completed",
        "failed",
        "canceled",
    ]
    .into_iter()
    .map(|state| {
        (
            state.to_string(),
            support::metrics_sample_value(
                metrics_text,
                "actionqueue_runs_total",
                &[("state", state)],
            ),
        )
    })
    .collect::<BTreeMap<_, _>>();

    let attempts_result_values = ["success", "failure", "timeout"]
        .into_iter()
        .map(|result| {
            (
                result.to_string(),
                support::metrics_sample_value(
                    metrics_text,
                    "actionqueue_attempts_total",
                    &[("result", result)],
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let family_prefix_presence = required_metric_family_prefixes()
        .iter()
        .map(|prefix| ((*prefix).to_string(), support::metrics_has_prefix(metrics_text, prefix)))
        .collect::<BTreeMap<_, _>>();

    MetricsFactSnapshot {
        run_state_values,
        attempts_result_values,
        runs_ready: support::metrics_sample_value(metrics_text, "actionqueue_runs_ready", &[]),
        runs_running: support::metrics_sample_value(metrics_text, "actionqueue_runs_running", &[]),
        run_state_label_values: support::metrics_label_values(
            metrics_text,
            "actionqueue_runs_total",
            "state",
        ),
        attempt_result_label_values: support::metrics_label_values(
            metrics_text,
            "actionqueue_attempts_total",
            "result",
        ),
        run_state_label_keys: support::metrics_label_keys(metrics_text, "actionqueue_runs_total"),
        attempt_result_label_keys: support::metrics_label_keys(
            metrics_text,
            "actionqueue_attempts_total",
        ),
        run_state_sample_count: support::metrics_sample_count(
            metrics_text,
            "actionqueue_runs_total",
        ),
        attempt_result_sample_count: support::metrics_sample_count(
            metrics_text,
            "actionqueue_attempts_total",
        ),
        family_prefix_presence,
    }
}

/// Captures canonicalized read-surface snapshot for restart parity comparisons.
async fn capture_read_surface_snapshot(router: &mut axum::Router<()>) -> ReadSurfaceSnapshot {
    let runs_rows_sorted_by_run_id = support::runs_list_rows_sorted_by_run_id(router).await;
    let mut run_get_by_run_id = BTreeMap::new();

    for row in &runs_rows_sorted_by_run_id {
        let run_id_text = row["run_id"]
            .as_str()
            .expect("run_id should be serialized as string in runs list")
            .to_string();
        let run_id = RunId::from_str(&run_id_text).expect("run_id from runs list should parse");
        let payload = support::run_get(router, run_id).await;
        run_get_by_run_id.insert(run_id_text, payload);
    }

    let stats_payload = support::get_json(router, "/api/v1/stats").await;
    let metrics_text = support::get_text(router, "/metrics").await;

    ReadSurfaceSnapshot {
        runs_rows_sorted_by_run_id,
        run_get_by_run_id,
        stats_payload,
        metrics_facts: capture_metrics_fact_snapshot(&metrics_text),
    }
}

/// Asserts restart parity by re-validating all required observability categories post-restart.
async fn assert_restart_parity(
    data_dir: &Path,
    expected_rows: &[RunsListTruth],
    expected_runs: &[RunGetTruth],
    expected_stats: StatsTruth,
    expected_metrics: MetricsTruth,
) -> ReadSurfaceSnapshot {
    let mut restarted_router = support::bootstrap_http_router(data_dir, true);
    assert_runs_list_truth(&mut restarted_router, expected_rows).await;
    assert_run_get_truth(&mut restarted_router, expected_runs).await;
    assert_stats_truth(&mut restarted_router, expected_stats).await;
    assert_metrics_value_truth(&mut restarted_router, expected_metrics).await;
    assert_metrics_label_bounds_and_families(&mut restarted_router).await;
    capture_read_surface_snapshot(&mut restarted_router).await
}

#[tokio::test]
async fn observability_api_identity_and_lifecycle_parity_are_operator_reconstructable() {
    let spec = ObservabilityScenarioSpec {
        label: "p8-006-ob-a-api-identity-and-lifecycle",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 3,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 1,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 3,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 1,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 2.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 2.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);

    let task_id_a_literal = "f8060000-0000-0000-0000-0000000000a1";
    let task_id_b_literal = "f8060000-0000-0000-0000-0000000000b1";
    let task_id_a = TaskId::from_str(task_id_a_literal).expect("fixed task id should parse");
    let task_id_b = TaskId::from_str(task_id_b_literal).expect("fixed task id should parse");

    let submit_a = support::submit_once_task_via_cli(task_id_a_literal, &data_dir);
    assert_eq!(submit_a["runs_created"], 1);
    let completed = support::complete_once_run_via_authority(&data_dir, task_id_a);

    let submit_b =
        support::submit_once_task_with_max_attempts_via_cli(task_id_b_literal, &data_dir, 2);
    assert_eq!(submit_b["runs_created"], 1);
    let failed = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir,
        task_id_b,
        2,
        &[
            support::AttemptOutcomePlan::RetryableFailure,
            support::AttemptOutcomePlan::RetryableFailure,
        ],
    );
    assert_eq!(failed.final_state, RunState::Failed);

    let expected_rows = vec![
        RunsListTruth {
            run_id: completed.run_id,
            task_id: task_id_a.to_string(),
            expected_state: "Completed",
            expected_attempt_count: 1,
            expected_concurrency_key: None,
        },
        RunsListTruth {
            run_id: failed.run_id,
            task_id: task_id_b.to_string(),
            expected_state: "Failed",
            expected_attempt_count: 2,
            expected_concurrency_key: None,
        },
    ];

    let expected_runs = vec![
        RunGetTruth {
            run_id: completed.run_id,
            task_id: task_id_a.to_string(),
            expected_state: "Completed",
            expected_block_reason: Some("terminal"),
            expected_attempt_ids: vec![completed.attempt_id.to_string()],
            expected_attempt_results: vec!["Success".to_string()],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "Completed"),
            ]),
        },
        RunGetTruth {
            run_id: failed.run_id,
            task_id: task_id_b.to_string(),
            expected_state: "Failed",
            expected_block_reason: Some("terminal"),
            expected_attempt_ids: vec![
                "00000000-0000-0000-0000-000000000001".to_string(),
                "00000000-0000-0000-0000-000000000002".to_string(),
            ],
            expected_attempt_results: vec!["Failure".to_string(), "Failure".to_string()],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "RetryWait"),
                (Some("RetryWait"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "Failed"),
            ]),
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_runs_list_truth(&mut router, &expected_rows).await;
    assert_run_get_truth(&mut router, &expected_runs).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_value_truth(&mut router, spec.expected_metrics_pre_restart).await;
    assert_metrics_label_bounds_and_families(&mut router).await;

    drop(router);
    let _ = assert_restart_parity(
        &data_dir,
        &expected_rows,
        &expected_runs,
        spec.expected_stats_post_restart,
        spec.expected_metrics_post_restart,
    )
    .await;
}

#[tokio::test]
async fn observability_stats_and_metrics_values_match_runtime_truth_exactly() {
    let spec = ObservabilityScenarioSpec {
        label: "p8-006-ob-b-stats-and-metrics-value-parity",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 3,
            total_runs: 3,
            attempts_total: 1,
            scheduled: 0,
            ready: 1,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 3,
            total_runs: 3,
            attempts_total: 1,
            scheduled: 0,
            ready: 1,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 1.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 1.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);

    let task_ready_literal = "f8060000-0000-0000-0000-0000000000b2";
    let task_running_literal = "f8060000-0000-0000-0000-0000000000b3";
    let task_completed_literal = "f8060000-0000-0000-0000-0000000000b4";

    let task_ready = TaskId::from_str(task_ready_literal).expect("fixed task id should parse");
    let task_running = TaskId::from_str(task_running_literal).expect("fixed task id should parse");
    let task_completed =
        TaskId::from_str(task_completed_literal).expect("fixed task id should parse");

    let submit_ready = support::submit_once_task_via_cli(task_ready_literal, &data_dir);
    assert_eq!(submit_ready["runs_created"], 1);
    let ready_run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_ready);

    let submit_running = support::submit_once_task_via_cli(task_running_literal, &data_dir);
    assert_eq!(submit_running["runs_created"], 1);
    let running_run_id =
        support::promote_single_run_to_ready_via_authority(&data_dir, task_running);
    support::transition_run_state_via_authority(
        &data_dir,
        running_run_id,
        RunState::Ready,
        RunState::Leased,
    );
    support::transition_run_state_via_authority(
        &data_dir,
        running_run_id,
        RunState::Leased,
        RunState::Running,
    );

    let submit_completed = support::submit_once_task_via_cli(task_completed_literal, &data_dir);
    assert_eq!(submit_completed["runs_created"], 1);
    let completed = support::complete_once_run_via_authority(&data_dir, task_completed);

    let expected_rows = vec![
        RunsListTruth {
            run_id: ready_run_id,
            task_id: task_ready.to_string(),
            expected_state: "Ready",
            expected_attempt_count: 0,
            expected_concurrency_key: None,
        },
        RunsListTruth {
            run_id: running_run_id,
            task_id: task_running.to_string(),
            expected_state: "Running",
            expected_attempt_count: 0,
            expected_concurrency_key: None,
        },
        RunsListTruth {
            run_id: completed.run_id,
            task_id: task_completed.to_string(),
            expected_state: "Completed",
            expected_attempt_count: 1,
            expected_concurrency_key: None,
        },
    ];

    let expected_runs = vec![
        RunGetTruth {
            run_id: ready_run_id,
            task_id: task_ready.to_string(),
            expected_state: "Ready",
            expected_block_reason: None,
            expected_attempt_ids: vec![],
            expected_attempt_results: vec![],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
            ]),
        },
        RunGetTruth {
            run_id: running_run_id,
            task_id: task_running.to_string(),
            expected_state: "Running",
            expected_block_reason: Some("running"),
            expected_attempt_ids: vec![],
            expected_attempt_results: vec![],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
            ]),
        },
        RunGetTruth {
            run_id: completed.run_id,
            task_id: task_completed.to_string(),
            expected_state: "Completed",
            expected_block_reason: Some("terminal"),
            expected_attempt_ids: vec![completed.attempt_id.to_string()],
            expected_attempt_results: vec!["Success".to_string()],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "Completed"),
            ]),
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_runs_list_truth(&mut router, &expected_rows).await;
    assert_run_get_truth(&mut router, &expected_runs).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_value_truth(&mut router, spec.expected_metrics_pre_restart).await;
    assert_metrics_label_bounds_and_families(&mut router).await;

    drop(router);
    let _ = assert_restart_parity(
        &data_dir,
        &expected_rows,
        &expected_runs,
        spec.expected_stats_post_restart,
        spec.expected_metrics_post_restart,
    )
    .await;
}

#[tokio::test]
async fn observability_metrics_enforce_bounded_labels_and_required_family_presence() {
    let spec = ObservabilityScenarioSpec {
        label: "p8-006-ob-c-bounded-labels-and-family-presence",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 1,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 1,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_literal = "f8060000-0000-0000-0000-0000000000c1";
    let task_id = TaskId::from_str(task_literal).expect("fixed task id should parse");

    let submit = support::submit_once_task_via_cli(task_literal, &data_dir);
    assert_eq!(submit["runs_created"], 1);
    let completed = support::complete_once_run_via_authority(&data_dir, task_id);

    let expected_rows = vec![RunsListTruth {
        run_id: completed.run_id,
        task_id: task_id.to_string(),
        expected_state: "Completed",
        expected_attempt_count: 1,
        expected_concurrency_key: None,
    }];
    let expected_runs = vec![RunGetTruth {
        run_id: completed.run_id,
        task_id: task_id.to_string(),
        expected_state: "Completed",
        expected_block_reason: Some("terminal"),
        expected_attempt_ids: vec![completed.attempt_id.to_string()],
        expected_attempt_results: vec!["Success".to_string()],
        expected_state_history: history_truth(&[
            (None, "Scheduled"),
            (Some("Scheduled"), "Ready"),
            (Some("Ready"), "Leased"),
            (Some("Leased"), "Running"),
            (Some("Running"), "Completed"),
        ]),
    }];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_runs_list_truth(&mut router, &expected_rows).await;
    assert_run_get_truth(&mut router, &expected_runs).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_value_truth(&mut router, spec.expected_metrics_pre_restart).await;
    assert_metrics_label_bounds_and_families(&mut router).await;

    drop(router);
    let _ = assert_restart_parity(
        &data_dir,
        &expected_rows,
        &expected_runs,
        spec.expected_stats_post_restart,
        spec.expected_metrics_post_restart,
    )
    .await;
}

#[tokio::test]
async fn observability_restart_preserves_runs_run_get_stats_and_metrics_parity() {
    let spec = ObservabilityScenarioSpec {
        label: "p8-006-ob-d-full-restart-parity",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 3,
            total_runs: 3,
            attempts_total: 2,
            scheduled: 0,
            ready: 1,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 1,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 3,
            total_runs: 3,
            attempts_total: 2,
            scheduled: 0,
            ready: 1,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 1,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 1.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 2.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 1.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 2.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);

    let task_ready_literal = "f8060000-0000-0000-0000-0000000000d1";
    let task_running_literal = "f8060000-0000-0000-0000-0000000000d2";
    let task_failed_literal = "f8060000-0000-0000-0000-0000000000d3";

    let task_ready = TaskId::from_str(task_ready_literal).expect("fixed task id should parse");
    let task_running = TaskId::from_str(task_running_literal).expect("fixed task id should parse");
    let task_failed = TaskId::from_str(task_failed_literal).expect("fixed task id should parse");

    let submit_ready = support::submit_once_task_via_cli(task_ready_literal, &data_dir);
    assert_eq!(submit_ready["runs_created"], 1);
    let ready_run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_ready);

    let submit_running = support::submit_once_task_via_cli(task_running_literal, &data_dir);
    assert_eq!(submit_running["runs_created"], 1);
    let running_run_id =
        support::promote_single_run_to_ready_via_authority(&data_dir, task_running);
    support::transition_run_state_via_authority(
        &data_dir,
        running_run_id,
        RunState::Ready,
        RunState::Leased,
    );
    support::transition_run_state_via_authority(
        &data_dir,
        running_run_id,
        RunState::Leased,
        RunState::Running,
    );

    let submit_failed =
        support::submit_once_task_with_max_attempts_via_cli(task_failed_literal, &data_dir, 2);
    assert_eq!(submit_failed["runs_created"], 1);

    let failed = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir,
        task_failed,
        2,
        &[
            support::AttemptOutcomePlan::RetryableFailure,
            support::AttemptOutcomePlan::RetryableFailure,
        ],
    );
    assert_eq!(failed.final_state, RunState::Failed);

    let expected_rows = vec![
        RunsListTruth {
            run_id: ready_run_id,
            task_id: task_ready.to_string(),
            expected_state: "Ready",
            expected_attempt_count: 0,
            expected_concurrency_key: None,
        },
        RunsListTruth {
            run_id: running_run_id,
            task_id: task_running.to_string(),
            expected_state: "Running",
            expected_attempt_count: 0,
            expected_concurrency_key: None,
        },
        RunsListTruth {
            run_id: failed.run_id,
            task_id: task_failed.to_string(),
            expected_state: "Failed",
            expected_attempt_count: 2,
            expected_concurrency_key: None,
        },
    ];

    let expected_runs = vec![
        RunGetTruth {
            run_id: ready_run_id,
            task_id: task_ready.to_string(),
            expected_state: "Ready",
            expected_block_reason: None,
            expected_attempt_ids: vec![],
            expected_attempt_results: vec![],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
            ]),
        },
        RunGetTruth {
            run_id: running_run_id,
            task_id: task_running.to_string(),
            expected_state: "Running",
            expected_block_reason: Some("running"),
            expected_attempt_ids: vec![],
            expected_attempt_results: vec![],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
            ]),
        },
        RunGetTruth {
            run_id: failed.run_id,
            task_id: task_failed.to_string(),
            expected_state: "Failed",
            expected_block_reason: Some("terminal"),
            expected_attempt_ids: vec![
                "00000000-0000-0000-0000-000000000001".to_string(),
                "00000000-0000-0000-0000-000000000002".to_string(),
            ],
            expected_attempt_results: vec!["Failure".to_string(), "Failure".to_string()],
            expected_state_history: history_truth(&[
                (None, "Scheduled"),
                (Some("Scheduled"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "RetryWait"),
                (Some("RetryWait"), "Ready"),
                (Some("Ready"), "Leased"),
                (Some("Leased"), "Running"),
                (Some("Running"), "Failed"),
            ]),
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_runs_list_truth(&mut router, &expected_rows).await;
    assert_run_get_truth(&mut router, &expected_runs).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_value_truth(&mut router, spec.expected_metrics_pre_restart).await;
    assert_metrics_label_bounds_and_families(&mut router).await;
    let pre_restart_snapshot = capture_read_surface_snapshot(&mut router).await;

    drop(router);
    let post_restart_snapshot = assert_restart_parity(
        &data_dir,
        &expected_rows,
        &expected_runs,
        spec.expected_stats_post_restart,
        spec.expected_metrics_post_restart,
    )
    .await;

    assert_eq!(post_restart_snapshot, pre_restart_snapshot);
}
