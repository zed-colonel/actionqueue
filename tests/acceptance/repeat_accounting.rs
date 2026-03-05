//! P8-002 Repeat acceptance proof: exact-N accounting with explicit no-N+1 restart guarantees.

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use actionqueue_core::ids::{RunId, TaskId};

/// Asserts the pre-completion `/api/v1/runs` task-scoped cardinality equals expected count.
async fn assert_pre_completion_run_cardinality(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    count: u32,
) {
    let runs = support::get_json(router, "/api/v1/runs").await;
    let task_runs = runs["runs"]
        .as_array()
        .expect("runs list should be an array")
        .iter()
        .filter(|entry| entry["task_id"] == task_id_literal)
        .collect::<Vec<_>>();
    assert_eq!(task_runs.len(), usize::try_from(count).expect("count should fit in usize"));
}

/// Asserts required `/api/v1/stats`, `/api/v1/runs`, and `/api/v1/runs/:id` truths.
async fn assert_post_completion_read_surfaces(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    expected_run_ids: &[RunId],
    completions: &[support::CompletionEvidence],
    count: u32,
) {
    let stats = support::get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["total_runs"], count);
    assert_eq!(stats["runs_by_state"]["completed"], count);
    assert_eq!(stats["runs_by_state"]["scheduled"], 0);
    assert_eq!(stats["runs_by_state"]["ready"], 0);
    assert_eq!(stats["runs_by_state"]["running"], 0);

    let expected_id_set =
        expected_run_ids.iter().map(ToString::to_string).collect::<BTreeSet<String>>();
    let completion_attempt_by_run = completions
        .iter()
        .map(|evidence| (evidence.run_id.to_string(), evidence.attempt_id.to_string()))
        .collect::<BTreeMap<String, String>>();

    let runs = support::get_json(router, "/api/v1/runs").await;
    let task_runs = runs["runs"]
        .as_array()
        .expect("runs list should be an array")
        .iter()
        .filter(|entry| entry["task_id"] == task_id_literal)
        .collect::<Vec<_>>();
    assert_eq!(task_runs.len(), usize::try_from(count).expect("count should fit in usize"));

    let actual_id_set = task_runs
        .iter()
        .map(|entry| {
            assert_eq!(entry["state"], "Completed");
            assert_eq!(entry["attempt_count"], 1);
            entry["run_id"].as_str().expect("run_id should be string").to_string()
        })
        .collect::<BTreeSet<String>>();
    assert_eq!(actual_id_set, expected_id_set);

    for run_id in expected_run_ids {
        let run_get_path = format!("/api/v1/runs/{run_id}");
        let run_get = support::get_json(router, &run_get_path).await;
        assert_eq!(run_get["state"], "Completed");
        assert_eq!(run_get["attempt_count"], 1);
        assert_eq!(run_get["block_reason"], "terminal");

        let attempts = run_get["attempts"].as_array().expect("attempts should be an array");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0]["result"], "Success");

        let run_id_text = run_id.to_string();
        let expected_attempt_id = completion_attempt_by_run
            .get(&run_id_text)
            .expect("completion evidence should include each run id");
        assert_eq!(attempts[0]["attempt_id"], expected_attempt_id.as_str());
    }
}

/// Asserts required `/metrics` sample values for completed repeat scenarios.
async fn assert_post_completion_metrics(router: &mut axum::Router<()>, count: u32) {
    let metrics = support::get_text(router, "/metrics").await;
    let expected = f64::from(count);
    assert_eq!(
        support::metrics_sample_value(
            &metrics,
            "actionqueue_runs_total",
            &[("state", "completed")]
        ),
        expected
    );
    assert_eq!(support::metrics_sample_value(&metrics, "actionqueue_runs_ready", &[]), 0.0);
    assert_eq!(support::metrics_sample_value(&metrics, "actionqueue_runs_running", &[]), 0.0);
    assert_eq!(
        support::metrics_sample_value(
            &metrics,
            "actionqueue_attempts_total",
            &[("result", "success")]
        ),
        expected
    );
}

/// Executes one exact-N repeat accounting scenario with restart and no-N+1 proof.
async fn run_repeat_exact_n_scenario(
    label: &str,
    task_id_literal: &str,
    count: u32,
    interval_secs: u64,
) {
    let data_dir = support::unique_data_dir(label);
    let task_id = TaskId::from_str(task_id_literal).expect("fixed task id should parse");

    let submit =
        support::submit_repeat_task_via_cli(task_id_literal, &data_dir, count, interval_secs);
    assert_eq!(submit["runs_created"], count);
    assert_eq!(submit["run_policy"], format!("repeat:{count}:{interval_secs}"));

    let initial_run_ids = support::task_run_ids_from_storage(&data_dir, task_id);
    assert_eq!(initial_run_ids.len(), usize::try_from(count).expect("count should fit in usize"));

    let mut pre_completion_router = support::bootstrap_http_router(&data_dir, true);
    assert_pre_completion_run_cardinality(&mut pre_completion_router, task_id_literal, count).await;
    drop(pre_completion_router);

    let completions = support::complete_all_task_runs_via_authority(&data_dir, task_id);
    assert_eq!(completions.len(), usize::try_from(count).expect("count should fit in usize"));

    let mut post_completion_router = support::bootstrap_http_router(&data_dir, true);
    assert_post_completion_read_surfaces(
        &mut post_completion_router,
        task_id_literal,
        &initial_run_ids,
        &completions,
        count,
    )
    .await;
    assert_post_completion_metrics(&mut post_completion_router, count).await;

    drop(post_completion_router);
    let restarted_run_ids = support::task_run_ids_from_storage(&data_dir, task_id);
    assert_eq!(restarted_run_ids, initial_run_ids);

    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_post_completion_read_surfaces(
        &mut restarted_router,
        task_id_literal,
        &initial_run_ids,
        &completions,
        count,
    )
    .await;

    support::assert_task_run_count_and_ids(&data_dir, task_id, &initial_run_ids);
    assert_eq!(restarted_run_ids, initial_run_ids);

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn repeat_accounting_repeat_3_60_exact_n_no_n_plus_1_after_restart() {
    run_repeat_exact_n_scenario(
        "p8-002-repeat-accounting-repeat-3-60",
        "22222222-2222-2222-2222-222222222222",
        3,
        60,
    )
    .await;
}

#[tokio::test]
async fn repeat_accounting_repeat_6_30_exact_n_no_n_plus_1_after_restart() {
    run_repeat_exact_n_scenario(
        "p8-002-repeat-accounting-repeat-6-30",
        "33333333-3333-3333-3333-333333333333",
        6,
        30,
    )
    .await;
}
