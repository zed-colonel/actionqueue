//! P8-001 Once acceptance proof: one run, durable completion, no redispatch.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::TaskId;

/// Asserts the required HTTP read-surface contract for the completed Once run.
async fn assert_once_read_surfaces(
    router: &mut axum::Router<()>,
    completion: &support::CompletionEvidence,
) {
    let stats = support::get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["total_tasks"], 1);
    assert_eq!(stats["total_runs"], 1);
    assert_eq!(stats["runs_by_state"]["completed"], 1);
    assert_eq!(stats["runs_by_state"]["ready"], 0);
    assert_eq!(stats["runs_by_state"]["running"], 0);
    assert_eq!(stats["runs_by_state"]["scheduled"], 0);

    let runs = support::get_json(router, "/api/v1/runs").await;
    let run_items = runs["runs"].as_array().expect("runs list should be an array");
    assert_eq!(run_items.len(), 1);
    assert_eq!(run_items[0]["run_id"], completion.run_id.to_string());
    assert_eq!(run_items[0]["state"], "Completed");
    assert_eq!(run_items[0]["attempt_count"], 1);

    let run_get_path = format!("/api/v1/runs/{}", completion.run_id);
    let run_get = support::get_json(router, &run_get_path).await;
    assert_eq!(run_get["state"], "Completed");
    assert_eq!(run_get["attempt_count"], 1);
    let attempts = run_get["attempts"].as_array().expect("attempts should be an array");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0]["attempt_id"], completion.attempt_id.to_string());
    assert_eq!(attempts[0]["result"], "Success");
    assert_eq!(run_get["block_reason"], "terminal");
}

/// Asserts the required metrics parity samples for the completed Once run.
async fn assert_once_metrics_parity(router: &mut axum::Router<()>) {
    let metrics = support::get_text(router, "/metrics").await;
    assert!(metrics.contains("actionqueue_runs_total{state=\"completed\"} 1"));
    assert!(metrics.contains("actionqueue_runs_ready 0"));
    assert!(metrics.contains("actionqueue_runs_running 0"));
    assert!(metrics.contains("actionqueue_attempts_total{result=\"success\"} 1"));
}

#[tokio::test]
async fn once_accounting_proves_one_run_and_no_redispatch_after_restart() {
    // 1) Arrange
    let data_dir = support::unique_data_dir("p8-001-once-accounting");
    let task_id_str = "11111111-1111-1111-1111-111111111111";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // 2) Seed via CLI submit
    let submit = support::submit_once_task_via_cli(task_id_str, &data_dir);
    assert_eq!(submit["runs_created"], 1);

    // 3) Durably complete run through authority lane
    let completion = support::complete_once_run_via_authority(&data_dir, task_id);
    // Once policy: task_create(1) + run_create(2) + promote(3) + lease(4) +
    // running(5) + attempt_start(6) + attempt_finish(7) + completed(8)
    assert_eq!(
        completion.final_sequence, 8,
        "Once policy completion should produce exactly 8 WAL events"
    );

    // 4) Pre-restart readback assertions + metrics parity
    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_once_read_surfaces(&mut router, &completion).await;
    assert_once_metrics_parity(&mut router).await;

    // 5) Restart proof
    drop(router);
    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_once_read_surfaces(&mut restarted_router, &completion).await;

    // 6) No-redispatch proof
    support::assert_no_scheduled_runs_left(&data_dir);
    let run_get_path = format!("/api/v1/runs/{}", completion.run_id);
    let run_get = support::get_json(&mut restarted_router, &run_get_path).await;
    assert_eq!(run_get["attempt_count"], 1);
    assert_eq!(run_get["state"], "Completed");
    let history = run_get["state_history"].as_array().expect("state_history should be an array");
    let tail = history.last().expect("state_history should not be empty");
    assert_eq!(tail["to"], "Completed");

    let _ = std::fs::remove_dir_all(&data_dir);
}
