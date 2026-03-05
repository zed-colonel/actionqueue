//! P8-003 Retry-cap acceptance proof: exact attempt cap, terminal closure, and no-N+1 after restart.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;

/// Deterministic scenario contract for retry-cap acceptance proofs.
#[derive(Debug, Clone, Copy)]
struct RetryCapScenarioSpec {
    /// Human-readable scenario label used for isolated data-dir naming.
    label: &'static str,
    /// Deterministic task identifier literal.
    task_id_literal: &'static str,
    /// Hard retry cap under test.
    max_attempts: u32,
    /// Ordered planned attempt outcomes applied through authority mutation lane.
    outcomes: &'static [support::AttemptOutcomePlan],
    /// Ordered expected run-get attempt result strings.
    expected_attempt_results: &'static [&'static str],
    /// Exact expected attempt count for the run.
    expected_attempt_count: usize,
    /// Expected terminal state string in HTTP payload.
    expected_final_state: &'static str,
    /// Expected attempts metric sample for `result=success`.
    expected_success_count: f64,
    /// Expected attempts metric sample for `result=failure`.
    expected_failure_count: f64,
    /// Expected attempts metric sample for `result=timeout`.
    expected_timeout_count: f64,
}

/// Asserts the pre-execution baseline required before authority-lane orchestration.
async fn assert_pre_execution_baseline(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    expected_run_id: &str,
) {
    let stats = support::get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["total_runs"], 1);
    assert_eq!(stats["runs_by_state"]["scheduled"], 1);

    let runs = support::get_json(router, "/api/v1/runs").await;
    let task_runs = runs["runs"]
        .as_array()
        .expect("runs list should be an array")
        .iter()
        .filter(|entry| entry["task_id"] == task_id_literal)
        .collect::<Vec<_>>();
    assert_eq!(task_runs.len(), 1);
    assert_eq!(task_runs[0]["run_id"], expected_run_id);
    assert_eq!(task_runs[0]["attempt_count"], 0);
}

/// Asserts required HTTP read-surface truth for a retry-cap terminal scenario.
async fn assert_retry_cap_http_truth(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    run_id: &str,
    spec: RetryCapScenarioSpec,
) {
    let stats = support::get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["total_runs"], 1);
    assert_eq!(
        stats["runs_by_state"]["completed"],
        if spec.expected_final_state == "Completed" { 1 } else { 0 }
    );
    assert_eq!(
        stats["runs_by_state"]["failed"],
        if spec.expected_final_state == "Failed" { 1 } else { 0 }
    );
    assert_eq!(stats["runs_by_state"]["running"], 0);
    assert_eq!(stats["runs_by_state"]["ready"], 0);
    assert_eq!(stats["runs_by_state"]["scheduled"], 0);

    let runs = support::get_json(router, "/api/v1/runs").await;
    let task_runs = runs["runs"]
        .as_array()
        .expect("runs list should be an array")
        .iter()
        .filter(|entry| entry["task_id"] == task_id_literal)
        .collect::<Vec<_>>();
    assert_eq!(task_runs.len(), 1);
    assert_eq!(task_runs[0]["run_id"], run_id);
    assert_eq!(task_runs[0]["attempt_count"], spec.expected_attempt_count);
    assert_eq!(task_runs[0]["state"], spec.expected_final_state);

    let run_get_path = format!("/api/v1/runs/{run_id}");
    let run_get = support::get_json(router, &run_get_path).await;
    assert_eq!(run_get["state"], spec.expected_final_state);
    assert_eq!(run_get["attempt_count"], spec.expected_attempt_count);
    assert_eq!(run_get["block_reason"], "terminal");

    let attempts = run_get["attempts"].as_array().expect("attempts should be an array");
    assert_eq!(attempts.len(), spec.expected_attempt_count);
    let attempt_results = attempts
        .iter()
        .map(|entry| {
            entry["result"]
                .as_str()
                .expect("attempt result should be present and serialized as string")
        })
        .collect::<Vec<_>>();
    assert_eq!(attempt_results, spec.expected_attempt_results);
}

/// Asserts required attempt and run-state metrics parity samples.
async fn assert_retry_cap_metrics(router: &mut axum::Router<()>, spec: RetryCapScenarioSpec) {
    let metrics = support::get_text(router, "/metrics").await;
    assert_eq!(
        support::metrics_sample_value(
            &metrics,
            "actionqueue_attempts_total",
            &[("result", "success")]
        ),
        spec.expected_success_count
    );
    assert_eq!(
        support::metrics_sample_value(
            &metrics,
            "actionqueue_attempts_total",
            &[("result", "failure")]
        ),
        spec.expected_failure_count
    );
    assert_eq!(
        support::metrics_sample_value(
            &metrics,
            "actionqueue_attempts_total",
            &[("result", "timeout")]
        ),
        spec.expected_timeout_count
    );
    assert_eq!(support::metrics_sample_value(&metrics, "actionqueue_runs_ready", &[]), 0.0);
    assert_eq!(support::metrics_sample_value(&metrics, "actionqueue_runs_running", &[]), 0.0);
}

/// Converts expected state text into canonical run-state enum for storage assertions.
fn expected_state_enum(expected_state: &str) -> RunState {
    match expected_state {
        "Completed" => RunState::Completed,
        "Failed" => RunState::Failed,
        _ => panic!("unsupported expected_final_state '{expected_state}' in retry-cap scenario"),
    }
}

/// Runs one retry-cap scenario end-to-end with restart durability and no-N+1 proof.
async fn run_retry_cap_scenario(spec: RetryCapScenarioSpec) {
    // 1) isolated data directory
    let data_dir = support::unique_data_dir(spec.label);

    // 2) deterministic task identity
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");

    // 3) submit via CLI path with explicit cap constraint
    let submit = support::submit_once_task_with_max_attempts_via_cli(
        spec.task_id_literal,
        &data_dir,
        spec.max_attempts,
    );
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    // 4) prove exactly one run identity exists for the task
    let initial_run_id = support::single_run_id_for_task(&data_dir, task_id);

    // 5-6) baseline HTTP assertions before orchestrated execution
    let mut baseline_router = support::bootstrap_http_router(&data_dir, true);
    assert_pre_execution_baseline(
        &mut baseline_router,
        spec.task_id_literal,
        &initial_run_id.to_string(),
    )
    .await;

    // 7) execute deterministic outcomes through storage authority only
    drop(baseline_router);
    let evidence = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir,
        task_id,
        spec.max_attempts,
        spec.outcomes,
    );
    assert_eq!(evidence.run_id, initial_run_id, "run identity must remain stable");
    assert!(evidence.final_sequence > 0, "terminal sequence must be durably recorded");

    // 8) storage truth assertions immediately post execution
    let expected_state = expected_state_enum(spec.expected_final_state);
    support::assert_run_state_from_storage(&data_dir, evidence.run_id, expected_state);
    assert_eq!(evidence.final_state, expected_state);
    assert_eq!(evidence.attempt_ids.len(), spec.expected_attempt_count);
    if spec.expected_attempt_count
        == usize::try_from(spec.max_attempts).expect("max_attempts should fit")
    {
        assert_eq!(
            evidence.attempt_ids.len(),
            usize::try_from(spec.max_attempts).expect("max_attempts should fit in usize"),
            "cap-boundary scenarios must stop exactly at max_attempts"
        );
    }

    // 9-10) HTTP and metrics parity with storage truth
    let mut post_execution_router = support::bootstrap_http_router(&data_dir, true);
    assert_retry_cap_http_truth(
        &mut post_execution_router,
        spec.task_id_literal,
        &evidence.run_id.to_string(),
        spec,
    )
    .await;
    assert_retry_cap_metrics(&mut post_execution_router, spec).await;

    // 11) restart durability proof
    drop(post_execution_router);
    let post_restart_attempt_ids =
        support::attempt_ids_for_run_from_storage(&data_dir, evidence.run_id);
    assert_eq!(post_restart_attempt_ids, evidence.attempt_ids);
    let post_restart_run_id = support::single_run_id_for_task(&data_dir, task_id);
    assert_eq!(post_restart_run_id, evidence.run_id);
    support::assert_run_state_from_storage(&data_dir, evidence.run_id, expected_state);

    // 12) post-restart HTTP + metrics parity
    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_retry_cap_http_truth(
        &mut restarted_router,
        spec.task_id_literal,
        &evidence.run_id.to_string(),
        spec,
    )
    .await;
    assert_retry_cap_metrics(&mut restarted_router, spec).await;

    // 13) explicit no-N+1 proofs
    let final_attempt_ids = support::attempt_ids_for_run_from_storage(&data_dir, evidence.run_id);
    assert_eq!(
        final_attempt_ids, evidence.attempt_ids,
        "no extra attempt must appear after restart"
    );
    support::assert_task_run_count_and_ids(&data_dir, task_id, &[evidence.run_id]);

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn retry_cap_max_attempts_1_retryable_failure_no_n_plus_one_after_restart() {
    const OUTCOMES: &[support::AttemptOutcomePlan] =
        &[support::AttemptOutcomePlan::RetryableFailure];

    run_retry_cap_scenario(RetryCapScenarioSpec {
        label: "p8-003-rc-a-max-attempts-1-retryable-failure",
        task_id_literal: "44444444-4444-4444-4444-444444444444",
        max_attempts: 1,
        outcomes: OUTCOMES,
        expected_attempt_results: &["Failure"],
        expected_attempt_count: 1,
        expected_final_state: "Failed",
        expected_success_count: 0.0,
        expected_failure_count: 1.0,
        expected_timeout_count: 0.0,
    })
    .await;
}

#[tokio::test]
async fn retry_cap_max_attempts_3_retryable_failure_exact_cap_no_n_plus_one_after_restart() {
    const OUTCOMES: &[support::AttemptOutcomePlan] = &[
        support::AttemptOutcomePlan::RetryableFailure,
        support::AttemptOutcomePlan::RetryableFailure,
        support::AttemptOutcomePlan::RetryableFailure,
    ];

    run_retry_cap_scenario(RetryCapScenarioSpec {
        label: "p8-003-rc-b-max-attempts-3-retryable-failure",
        task_id_literal: "55555555-5555-5555-5555-555555555555",
        max_attempts: 3,
        outcomes: OUTCOMES,
        expected_attempt_results: &["Failure", "Failure", "Failure"],
        expected_attempt_count: 3,
        expected_final_state: "Failed",
        expected_success_count: 0.0,
        expected_failure_count: 3.0,
        expected_timeout_count: 0.0,
    })
    .await;
}

#[tokio::test]
async fn retry_cap_max_attempts_3_timeout_exact_cap_no_n_plus_one_after_restart() {
    const OUTCOMES: &[support::AttemptOutcomePlan] = &[
        support::AttemptOutcomePlan::Timeout,
        support::AttemptOutcomePlan::Timeout,
        support::AttemptOutcomePlan::Timeout,
    ];

    run_retry_cap_scenario(RetryCapScenarioSpec {
        label: "p8-003-rc-c-max-attempts-3-timeout",
        task_id_literal: "66666666-6666-6666-6666-666666666666",
        max_attempts: 3,
        outcomes: OUTCOMES,
        expected_attempt_results: &["Timeout", "Timeout", "Timeout"],
        expected_attempt_count: 3,
        expected_final_state: "Failed",
        expected_success_count: 0.0,
        expected_failure_count: 0.0,
        expected_timeout_count: 3.0,
    })
    .await;
}

#[tokio::test]
async fn retry_cap_success_completes_without_retry_under_cap_after_restart() {
    const OUTCOMES: &[support::AttemptOutcomePlan] = &[support::AttemptOutcomePlan::Success];

    run_retry_cap_scenario(RetryCapScenarioSpec {
        label: "p8-003-rc-d-success-under-cap",
        task_id_literal: "77777777-7777-7777-7777-777777777777",
        max_attempts: 3,
        outcomes: OUTCOMES,
        expected_attempt_results: &["Success"],
        expected_attempt_count: 1,
        expected_final_state: "Completed",
        expected_success_count: 1.0,
        expected_failure_count: 0.0,
        expected_timeout_count: 0.0,
    })
    .await;
}

#[tokio::test]
async fn retry_cap_terminal_failure_fails_without_retry_under_cap_after_restart() {
    const OUTCOMES: &[support::AttemptOutcomePlan] =
        &[support::AttemptOutcomePlan::TerminalFailure];

    run_retry_cap_scenario(RetryCapScenarioSpec {
        label: "p8-003-rc-e-terminal-failure-under-cap",
        task_id_literal: "88888888-8888-8888-8888-888888888888",
        max_attempts: 3,
        outcomes: OUTCOMES,
        expected_attempt_results: &["Failure"],
        expected_attempt_count: 1,
        expected_final_state: "Failed",
        expected_success_count: 0.0,
        expected_failure_count: 1.0,
        expected_timeout_count: 0.0,
    })
    .await;
}
