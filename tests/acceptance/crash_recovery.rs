//! P8-004 Crash-recovery acceptance proof suite.

mod support;

use std::path::Path;
use std::str::FromStr;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::run::state::RunState;
use actionqueue_executor_local::ExecutorResponse;
use support::{MetricsTruth, StatsTruth};

/// Deterministic scenario contract for crash-recovery acceptance proofs.
#[derive(Debug, Clone, Copy)]
struct CrashRecoveryScenarioSpec {
    /// Human-readable scenario label used for isolated data-dir naming.
    label: &'static str,
    /// Deterministic task identifier literal.
    task_id_literal: &'static str,
    /// Hard retry cap applied to the submitted Once task.
    max_attempts: u32,
    /// Expected durable run state before restart.
    expected_pre_restart_state: &'static str,
    /// Expected durable run state after post-restart actions complete.
    expected_post_restart_state: &'static str,
    /// Expected attempt count before restart.
    expected_pre_restart_attempt_count: usize,
    /// Expected attempt count after post-restart actions complete.
    expected_post_restart_attempt_count: usize,
}

/// Submits one authority-lane run-state transition command and returns durable sequence.
fn submit_run_state_transition_via_authority(
    data_dir: &Path,
    run_id: RunId,
    previous_state: RunState,
    new_state: RunState,
) -> u64 {
    support::transition_run_state_via_authority(data_dir, run_id, previous_state, new_state)
}

/// Submits one authority-lane attempt-start command and returns durable sequence.
fn submit_attempt_start_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
) -> u64 {
    support::submit_attempt_start_via_authority(data_dir, run_id, attempt_id)
}

/// Submits one authority-lane attempt-finish command and returns durable sequence.
fn submit_attempt_finish_response_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
    response: &ExecutorResponse,
) -> u64 {
    support::submit_attempt_finish_response_via_authority(data_dir, run_id, attempt_id, response)
}

/// Asserts required read-API truth for `/api/v1/runs` and `/api/v1/runs/:id`.
#[allow(clippy::too_many_arguments)] // Test assertion helper with naturally many parameters
async fn assert_http_truth(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    run_id: RunId,
    expected_state: &str,
    expected_attempt_ids: &[String],
    expected_block_reason: Option<&str>,
) {
    let runs = support::get_json(router, "/api/v1/runs").await;
    let run_entries = runs["runs"]
        .as_array()
        .expect("runs list should be an array")
        .iter()
        .filter(|entry| entry["run_id"] == run_id.to_string())
        .collect::<Vec<_>>();
    assert_eq!(run_entries.len(), 1, "expected exactly one run-list record for scenario run");
    assert_eq!(run_entries[0]["task_id"], task_id_literal);
    assert_eq!(run_entries[0]["state"], expected_state);
    assert_eq!(
        run_entries[0]["attempt_count"],
        u64::try_from(expected_attempt_ids.len()).expect("attempt length should fit in u64")
    );

    let run_get_path = format!("/api/v1/runs/{run_id}");
    let run_get = support::get_json(router, &run_get_path).await;
    assert_eq!(run_get["run_id"], run_id.to_string());
    assert_eq!(run_get["task_id"], task_id_literal);
    assert_eq!(run_get["state"], expected_state);
    assert_eq!(
        run_get["attempt_count"],
        u64::try_from(expected_attempt_ids.len()).expect("attempt length should fit in u64")
    );

    let attempt_ids = run_get["attempts"]
        .as_array()
        .expect("attempts should be an array")
        .iter()
        .map(|entry| {
            entry["attempt_id"]
                .as_str()
                .expect("attempt_id should be serialized as string")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(attempt_ids, expected_attempt_ids);

    match expected_block_reason {
        Some(reason) => assert_eq!(run_get["block_reason"], reason),
        None => assert!(run_get["block_reason"].is_null(), "ready run should be unblocked"),
    }
}

/// Asserts required aggregate parity truth from `/api/v1/stats`.
async fn assert_stats_truth(router: &mut axum::Router<()>, expected: StatsTruth) {
    support::assert_stats_truth(router, expected).await;
}

/// Asserts required aggregate parity truth from `/metrics`.
async fn assert_metrics_truth(router: &mut axum::Router<()>, expected: MetricsTruth) {
    support::assert_metrics_truth(router, expected).await;
}

/// Asserts post-restart parity before post-restart mutation actions are applied.
async fn assert_restart_parity(
    router: &mut axum::Router<()>,
    task_id_literal: &str,
    checkpoint: &support::CrashRecoveryCheckpoint,
    expected_pre_restart_state: &str,
) {
    let expected_attempt_ids = attempt_ids_to_strings(&checkpoint.pre_restart_attempt_ids);
    assert_http_truth(
        router,
        task_id_literal,
        checkpoint.run_id,
        expected_pre_restart_state,
        &expected_attempt_ids,
        block_reason_for_state(expected_pre_restart_state),
    )
    .await;

    let post_restart_attempt_ids = support::run_get_attempt_ids(router, checkpoint.run_id).await;
    assert_eq!(
        post_restart_attempt_ids, expected_attempt_ids,
        "pre-restart attempt lineage must be preserved exactly after restart"
    );

    let stats = support::get_json(router, "/api/v1/stats").await;
    assert_eq!(stats["latest_sequence"], checkpoint.pre_restart_sequence);
    assert_eq!(
        stats["attempts_total"],
        u64::try_from(checkpoint.pre_restart_attempt_ids.len())
            .expect("pre-restart attempt count should fit in u64")
    );
}

/// Returns canonical API block-reason expectation for a given state string.
fn block_reason_for_state(state: &str) -> Option<&'static str> {
    match state {
        "Scheduled" => Some("scheduled"),
        "Ready" => None,
        "Leased" => Some("leased"),
        "Running" => Some("running"),
        "RetryWait" => Some("retry_wait"),
        "Completed" | "Failed" | "Canceled" => Some("terminal"),
        _ => panic!("unsupported state '{state}' in crash-recovery acceptance"),
    }
}

/// Converts ordered attempt IDs into ordered stable string values.
fn attempt_ids_to_strings(ids: &[AttemptId]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

#[tokio::test]
async fn crash_recovery_completed_run_remains_terminal_no_redispatch_after_restart() {
    let spec = CrashRecoveryScenarioSpec {
        label: "p8-004-cr-a-durable-completion-present",
        task_id_literal: "91000000-0000-0000-0000-000000000001",
        max_attempts: 1,
        expected_pre_restart_state: "Completed",
        expected_post_restart_state: "Completed",
        expected_pre_restart_attempt_count: 1,
        expected_post_restart_attempt_count: 1,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");

    let submit =
        support::submit_once_task_with_constraints_via_cli(spec.task_id_literal, &data_dir, None);
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    let completion = support::complete_once_run_via_authority(&data_dir, task_id);
    assert!(completion.final_sequence > 0, "completion should durably advance WAL sequence");

    let checkpoint = support::capture_checkpoint(&data_dir, completion.run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::Completed);
    assert_eq!(
        checkpoint.pre_restart_attempt_ids.len(),
        spec.expected_pre_restart_attempt_count,
        "checkpoint attempt count must match scenario spec"
    );

    let expected_attempt_ids = attempt_ids_to_strings(&checkpoint.pre_restart_attempt_ids);

    let mut pre_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_truth(
        &mut pre_restart_router,
        spec.task_id_literal,
        checkpoint.run_id,
        spec.expected_pre_restart_state,
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut pre_restart_router,
        StatsTruth {
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
    )
    .await;
    assert_metrics_truth(
        &mut pre_restart_router,
        MetricsTruth {
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
    )
    .await;

    drop(pre_restart_router);

    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_restart_parity(
        &mut post_restart_router,
        spec.task_id_literal,
        &checkpoint,
        spec.expected_pre_restart_state,
    )
    .await;
    assert_http_truth(
        &mut post_restart_router,
        spec.task_id_literal,
        checkpoint.run_id,
        spec.expected_post_restart_state,
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut post_restart_router,
        StatsTruth {
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
    )
    .await;
    assert_metrics_truth(
        &mut post_restart_router,
        MetricsTruth {
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
    )
    .await;

    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(checkpoint.run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[checkpoint.run_id]);
    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut post_restart_router, checkpoint.run_id).await;
    assert_eq!(post_restart_attempt_ids, expected_attempt_ids);
    assert_eq!(
        post_restart_attempt_ids.len(),
        spec.expected_post_restart_attempt_count,
        "post-restart attempt count must match scenario spec"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn crash_recovery_absent_completion_allows_recovery_path_with_stable_runid_and_attempt_lineage(
) {
    let spec = CrashRecoveryScenarioSpec {
        label: "p8-004-cr-b-durable-completion-absent",
        task_id_literal: "91000000-0000-0000-0000-000000000002",
        max_attempts: 2,
        expected_pre_restart_state: "RetryWait",
        expected_post_restart_state: "Completed",
        expected_pre_restart_attempt_count: 1,
        expected_post_restart_attempt_count: 2,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");
    let attempt_id_1 = AttemptId::from_str("00000000-0000-0000-0000-00000000b001")
        .expect("deterministic attempt id should parse");
    let attempt_id_2 = AttemptId::from_str("00000000-0000-0000-0000-00000000b002")
        .expect("deterministic attempt id should parse");

    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    let run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_id);

    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_1);
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_1,
        &ExecutorResponse::RetryableFailure {
            error: "cr-b attempt-1 retryable failure".to_string(),
        },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::RetryWait,
    );

    let checkpoint = support::capture_checkpoint(&data_dir, run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::RetryWait);
    assert_eq!(
        checkpoint.pre_restart_attempt_ids,
        vec![attempt_id_1],
        "pre-restart lineage must contain exactly attempt-1"
    );
    assert_eq!(
        checkpoint.pre_restart_attempt_ids.len(),
        spec.expected_pre_restart_attempt_count,
        "checkpoint attempt count must match scenario spec"
    );

    let expected_pre_attempt_ids = vec![attempt_id_1.to_string()];
    let mut pre_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_truth(
        &mut pre_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_pre_restart_state,
        &expected_pre_attempt_ids,
        Some("retry_wait"),
    )
    .await;
    assert_stats_truth(
        &mut pre_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 1,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 1,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut pre_restart_router,
        MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 1.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 1.0,
            attempts_timeout: 0.0,
        },
    )
    .await;

    drop(pre_restart_router);

    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_restart_parity(
        &mut post_restart_router,
        spec.task_id_literal,
        &checkpoint,
        spec.expected_pre_restart_state,
    )
    .await;

    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::RetryWait,
        RunState::Ready,
    );
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_2);
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_2,
        &ExecutorResponse::Success { output: None },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::Completed,
    );

    drop(post_restart_router);
    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);

    let expected_post_attempt_ids = vec![attempt_id_1.to_string(), attempt_id_2.to_string()];
    assert_http_truth(
        &mut post_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_post_restart_state,
        &expected_post_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut post_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 2,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut post_restart_router,
        MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 1.0,
            attempts_failure: 1.0,
            attempts_timeout: 0.0,
        },
    )
    .await;

    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(checkpoint.run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut post_restart_router, run_id).await;
    assert_eq!(post_restart_attempt_ids, expected_post_attempt_ids);
    assert_eq!(
        post_restart_attempt_ids.len(),
        spec.expected_post_restart_attempt_count,
        "post-restart attempt count must match scenario spec"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn crash_recovery_active_lease_at_crash_closes_to_ready_without_phantom_terminal_state() {
    let spec = CrashRecoveryScenarioSpec {
        label: "p8-004-cr-c-active-lease-at-crash",
        task_id_literal: "91000000-0000-0000-0000-000000000003",
        max_attempts: 1,
        expected_pre_restart_state: "Leased",
        expected_post_restart_state: "Ready",
        expected_pre_restart_attempt_count: 0,
        expected_post_restart_attempt_count: 0,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");

    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);

    let run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_id);
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    let expected_owner = "p8-004-cr-c-owner";
    let expected_expiry = 17_000_u64;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        expected_owner,
        expected_expiry,
    );

    let checkpoint = support::capture_checkpoint(&data_dir, run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::Leased);
    assert_eq!(checkpoint.pre_restart_attempt_ids.len(), spec.expected_pre_restart_attempt_count);

    let mut pre_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_truth(
        &mut pre_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_pre_restart_state,
        &[],
        Some("leased"),
    )
    .await;
    let pre_restart_lease = support::current_lease_from_run_get(&mut pre_restart_router, run_id)
        .await
        .expect("lease evidence should be present before restart in CR-C");
    assert_eq!(pre_restart_lease.owner, expected_owner);
    assert_eq!(pre_restart_lease.expiry, expected_expiry);
    assert!(pre_restart_lease.updated_at >= pre_restart_lease.acquired_at);

    assert_stats_truth(
        &mut pre_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 0,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut pre_restart_router,
        MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    )
    .await;

    drop(pre_restart_router);

    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_restart_parity(
        &mut post_restart_router,
        spec.task_id_literal,
        &checkpoint,
        spec.expected_pre_restart_state,
    )
    .await;
    let post_restart_lease = support::current_lease_from_run_get(&mut post_restart_router, run_id)
        .await
        .expect("lease evidence should remain present immediately after restart in CR-C");
    assert_eq!(post_restart_lease.owner, expected_owner);
    assert_eq!(post_restart_lease.expiry, expected_expiry);

    support::lease_release_for_run_via_authority(
        &data_dir,
        run_id,
        &post_restart_lease.owner,
        post_restart_lease.expiry,
    );

    drop(post_restart_router);
    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);

    assert_http_truth(
        &mut post_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_post_restart_state,
        &[],
        None,
    )
    .await;
    assert!(
        support::current_lease_from_run_get(&mut post_restart_router, run_id).await.is_none(),
        "lease metadata should clear after lease close"
    );
    assert_stats_truth(
        &mut post_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 0,
            scheduled: 0,
            ready: 1,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut post_restart_router,
        MetricsTruth {
            scheduled: 0.0,
            ready: 1.0,
            leased: 0.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    )
    .await;

    let run_get =
        support::get_json(&mut post_restart_router, &format!("/api/v1/runs/{run_id}")).await;
    assert_ne!(run_get["state"], "Completed");
    assert_ne!(run_get["state"], "Failed");
    assert_ne!(run_get["state"], "Canceled");

    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(checkpoint.run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);
    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut post_restart_router, run_id).await;
    assert!(post_restart_attempt_ids.is_empty());
    assert_eq!(post_restart_attempt_ids.len(), spec.expected_post_restart_attempt_count);

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// T-1: Crash during Running state — WAL replay preserves Running state, attempt lineage
/// is intact, and the run can be manually recovered to Ready (uncertainty clause path).
///
/// Scenario: Submit a Once task with max_attempts=2, drive it through
/// Scheduled→Ready→Leased→Running with an AttemptStart, simulate crash
/// (drop authority), re-bootstrap from WAL, and verify:
///   - Running state is durably preserved (WAL replay is deterministic)
///   - Attempt lineage (1 started, 0 finished) is intact
///   - RunId is stable across restart
///   - The run can be recovered to Ready via Running→RetryWait→Ready path
///     (the uncertainty clause recovery path used by the dispatch loop)
#[tokio::test]
async fn crash_recovery_running_state_preserves_lineage_and_allows_recovery() {
    let spec = CrashRecoveryScenarioSpec {
        label: "t1-cr-running-state-crash",
        task_id_literal: "91000000-0000-0000-0000-000000000005",
        max_attempts: 2,
        expected_pre_restart_state: "Running",
        expected_post_restart_state: "Completed",
        expected_pre_restart_attempt_count: 1,
        expected_post_restart_attempt_count: 2,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");
    let attempt_id_1 = AttemptId::from_str("00000000-0000-0000-0000-00000000e001")
        .expect("deterministic attempt id should parse");
    let attempt_id_2 = AttemptId::from_str("00000000-0000-0000-0000-00000000e002")
        .expect("deterministic attempt id should parse");

    // Submit a Once task with max_attempts=2.
    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    // Drive: Scheduled→Ready→Leased→Running + AttemptStart
    let run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_id);
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_1);

    // Capture checkpoint in Running state before crash.
    let checkpoint = support::capture_checkpoint(&data_dir, run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::Running);
    assert_eq!(
        checkpoint.pre_restart_attempt_ids,
        vec![attempt_id_1],
        "pre-restart lineage must contain exactly attempt-1"
    );
    assert_eq!(checkpoint.pre_restart_attempt_ids.len(), spec.expected_pre_restart_attempt_count);

    // Verify pre-restart Running state via HTTP.
    let mut pre_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_truth(
        &mut pre_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_pre_restart_state,
        &[attempt_id_1.to_string()],
        Some("running"),
    )
    .await;
    assert_stats_truth(
        &mut pre_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 1,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
    )
    .await;

    // Simulate crash: drop router (and all state).
    drop(pre_restart_router);

    // Re-bootstrap from WAL. Running state is preserved (WAL replay is deterministic).
    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);

    // Post-restart: run should still be in Running state (deterministic WAL replay).
    assert_restart_parity(
        &mut post_restart_router,
        spec.task_id_literal,
        &checkpoint,
        spec.expected_pre_restart_state,
    )
    .await;

    // Attempt lineage should be preserved across restart.
    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut post_restart_router, run_id).await;
    assert_eq!(
        post_restart_attempt_ids,
        vec![attempt_id_1.to_string()],
        "attempt lineage (attempt-1 started) must be preserved across restart"
    );
    assert_eq!(
        post_restart_attempt_ids.len(),
        spec.expected_pre_restart_attempt_count,
        "post-restart attempt count must match pre-restart count"
    );

    // Run should not be in any terminal state after restart.
    let run_get_path = format!("/api/v1/runs/{run_id}");
    let run_get = support::get_json(&mut post_restart_router, &run_get_path).await;
    assert_ne!(run_get["state"], "Completed");
    assert_ne!(run_get["state"], "Failed");
    assert_ne!(run_get["state"], "Canceled");

    // RunId stability across restart.
    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(checkpoint.run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    drop(post_restart_router);

    // Uncertainty clause recovery: manually recover Running→RetryWait→Ready,
    // then complete via a second attempt (simulating what the dispatch loop does).
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_1,
        &ExecutorResponse::RetryableFailure { error: "t1-crash-recovery-uncertainty".to_string() },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::RetryWait,
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::RetryWait,
        RunState::Ready,
    );
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_2);
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_2,
        &ExecutorResponse::Success { output: None },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::Completed,
    );

    // Verify final state after full recovery path.
    let mut final_router = support::bootstrap_http_router(&data_dir, true);
    let expected_attempt_ids = vec![attempt_id_1.to_string(), attempt_id_2.to_string()];
    assert_http_truth(
        &mut final_router,
        spec.task_id_literal,
        run_id,
        spec.expected_post_restart_state,
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut final_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 2,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
    )
    .await;

    let final_attempt_ids = support::run_get_attempt_ids(&mut final_router, run_id).await;
    assert_eq!(final_attempt_ids, expected_attempt_ids);
    assert_eq!(
        final_attempt_ids.len(),
        spec.expected_post_restart_attempt_count,
        "post-recovery attempt count must match scenario spec"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn crash_recovery_between_attempt_lease_expiry_preserves_lineage_and_allows_next_attempt_after_restart(
) {
    let spec = CrashRecoveryScenarioSpec {
        label: "p8-004-cr-d-between-attempt-lease-expiry",
        task_id_literal: "91000000-0000-0000-0000-000000000004",
        max_attempts: 2,
        expected_pre_restart_state: "Leased",
        expected_post_restart_state: "Completed",
        expected_pre_restart_attempt_count: 1,
        expected_post_restart_attempt_count: 2,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");
    let attempt_id_1 = AttemptId::from_str("00000000-0000-0000-0000-00000000d001")
        .expect("deterministic attempt id should parse");
    let attempt_id_2 = AttemptId::from_str("00000000-0000-0000-0000-00000000d002")
        .expect("deterministic attempt id should parse");

    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);

    let run_id = support::promote_single_run_to_ready_via_authority(&data_dir, task_id);

    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_1);
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_1,
        &ExecutorResponse::Timeout { timeout_secs: 3 },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::RetryWait,
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::RetryWait,
        RunState::Ready,
    );

    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    let lease_owner_attempt_2 = "p8-004-cr-d-owner-attempt-2";
    let lease_expiry_attempt_2 = 29_000_u64;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        lease_owner_attempt_2,
        lease_expiry_attempt_2,
    );

    let checkpoint = support::capture_checkpoint(&data_dir, run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::Leased);
    assert_eq!(checkpoint.pre_restart_attempt_ids, vec![attempt_id_1]);
    assert_eq!(
        checkpoint.pre_restart_attempt_ids.len(),
        spec.expected_pre_restart_attempt_count,
        "checkpoint attempt count must match scenario spec"
    );

    let mut pre_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_truth(
        &mut pre_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_pre_restart_state,
        &[attempt_id_1.to_string()],
        Some("leased"),
    )
    .await;
    let pre_restart_lease = support::current_lease_from_run_get(&mut pre_restart_router, run_id)
        .await
        .expect("lease evidence should be present before restart in CR-D");
    assert_eq!(pre_restart_lease.owner, lease_owner_attempt_2);
    assert_eq!(pre_restart_lease.expiry, lease_expiry_attempt_2);
    assert_stats_truth(
        &mut pre_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 1,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 0,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut pre_restart_router,
        MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 0.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 1.0,
        },
    )
    .await;

    drop(pre_restart_router);

    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);
    assert_restart_parity(
        &mut post_restart_router,
        spec.task_id_literal,
        &checkpoint,
        spec.expected_pre_restart_state,
    )
    .await;

    let post_restart_lease = support::current_lease_from_run_get(&mut post_restart_router, run_id)
        .await
        .expect("lease evidence should remain present immediately after restart in CR-D");
    support::lease_expire_for_run_via_authority(
        &data_dir,
        run_id,
        &post_restart_lease.owner,
        post_restart_lease.expiry,
    );

    drop(post_restart_router);
    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);

    assert_http_truth(
        &mut post_restart_router,
        spec.task_id_literal,
        run_id,
        "Ready",
        &[attempt_id_1.to_string()],
        None,
    )
    .await;
    assert!(
        support::current_lease_from_run_get(&mut post_restart_router, run_id).await.is_none(),
        "lease metadata must clear after lease-expire close"
    );

    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        "p8-004-cr-d-owner-attempt-2b",
        31_000,
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Leased,
        RunState::Running,
    );
    submit_attempt_start_via_authority(&data_dir, run_id, attempt_id_2);
    submit_attempt_finish_response_via_authority(
        &data_dir,
        run_id,
        attempt_id_2,
        &ExecutorResponse::Success { output: None },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::Completed,
    );

    drop(post_restart_router);
    let mut post_restart_router = support::bootstrap_http_router(&data_dir, true);

    let expected_attempt_ids = vec![attempt_id_1.to_string(), attempt_id_2.to_string()];
    assert_http_truth(
        &mut post_restart_router,
        spec.task_id_literal,
        run_id,
        spec.expected_post_restart_state,
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut post_restart_router,
        StatsTruth {
            total_tasks: 1,
            total_runs: 1,
            attempts_total: 2,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 0,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
    )
    .await;
    assert_metrics_truth(
        &mut post_restart_router,
        MetricsTruth {
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
            attempts_timeout: 1.0,
        },
    )
    .await;

    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(checkpoint.run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut post_restart_router, run_id).await;
    assert_eq!(post_restart_attempt_ids, expected_attempt_ids);
    assert_ne!(attempt_id_1, attempt_id_2, "attempt-2 must use a distinct AttemptId");
    assert_eq!(
        post_restart_attempt_ids.len(),
        spec.expected_post_restart_attempt_count,
        "post-restart attempt count must match scenario spec"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
