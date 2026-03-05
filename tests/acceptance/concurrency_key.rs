//! P8-005 Concurrency-key acceptance proof suite.

mod support;

use std::path::Path;
use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_engine::concurrency::key_gate::{ConcurrencyKey, KeyGate};

/// Deterministic scenario contract for concurrency-key acceptance proofs.
#[derive(Debug, Clone, Copy)]
struct ConcurrencyScenarioSpec {
    /// Human-readable scenario label used for isolated data-dir naming.
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

/// Deterministic per-run assertion contract for `/api/v1/runs` and `/api/v1/runs/:id`.
#[derive(Debug, Clone, Copy)]
struct RunRowTruth<'a> {
    run_id: actionqueue_core::ids::RunId,
    expected_state: &'a str,
    expected_concurrency_key: Option<&'a str>,
    expected_attempt_count: u64,
}

/// Builds a deterministic once-run handle and advances it to `Leased`.
fn seed_once_run_to_leased(
    data_dir: &Path,
    task_id_literal: &str,
    concurrency_key: Option<&str>,
) -> support::ConcurrencyRunHandle {
    let submit = support::submit_once_task_with_concurrency_key_via_cli(
        task_id_literal,
        data_dir,
        concurrency_key,
    );
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    let task_id = TaskId::from_str(task_id_literal).expect("fixed task id should parse");
    let run_id = support::promote_task_run_to_ready_via_authority(data_dir, task_id);
    support::transition_run_state_via_authority(
        data_dir,
        run_id,
        RunState::Ready,
        RunState::Leased,
    );

    support::ConcurrencyRunHandle {
        task_id,
        run_id,
        concurrency_key: concurrency_key.map(str::to_string),
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

/// Asserts deterministic run identity/state/concurrency-key truth from read surfaces.
async fn assert_run_rows_truth(router: &mut axum::Router<()>, expected_rows: &[RunRowTruth<'_>]) {
    let runs = support::get_json(router, "/api/v1/runs").await;
    let rows = runs["runs"].as_array().expect("runs list should be an array");
    assert_eq!(rows.len(), expected_rows.len(), "scenario must expose exactly expected run count");

    for expected in expected_rows {
        let row = support::runs_list_entry_by_run_id(router, expected.run_id).await;
        assert_eq!(row["run_id"], expected.run_id.to_string());
        assert_eq!(row["state"], expected.expected_state);
        assert_eq!(row["attempt_count"], expected.expected_attempt_count);
        support::assert_runs_list_concurrency_key(
            router,
            expected.run_id,
            expected.expected_concurrency_key,
        )
        .await;

        let run_get_path = format!("/api/v1/runs/{}", expected.run_id);
        let run_get = support::get_json(router, &run_get_path).await;
        assert_eq!(run_get["run_id"], expected.run_id.to_string());
        assert_eq!(run_get["state"], expected.expected_state);
        assert_eq!(run_get["attempt_count"], expected.expected_attempt_count);
    }
}

/// Asserts full restart parity for run rows, stats, and metrics against scenario expectations.
async fn assert_restart_parity(
    data_dir: &Path,
    spec: ConcurrencyScenarioSpec,
    expected_rows: &[RunRowTruth<'_>],
) {
    let mut restarted_router = support::bootstrap_http_router(data_dir, true);
    assert_run_rows_truth(&mut restarted_router, expected_rows).await;
    assert_stats_truth(&mut restarted_router, spec.expected_stats_post_restart).await;
    assert_metrics_truth(&mut restarted_router, spec.expected_metrics_post_restart).await;
}

#[tokio::test]
async fn concurrency_key_same_key_exclusion_blocks_second_run_from_running() {
    let spec = ConcurrencyScenarioSpec {
        label: "p8-005-ck-a-same-key-exclusion",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let run_1 = seed_once_run_to_leased(
        &data_dir,
        "a5a50000-0000-0000-0000-000000000001",
        Some("ck-a-shared"),
    );
    let run_2 = seed_once_run_to_leased(
        &data_dir,
        "a5a50000-0000-0000-0000-000000000002",
        Some("ck-a-shared"),
    );
    assert_ne!(run_1.run_id, run_2.run_id, "scenario runs must remain identity-distinct");

    let mut key_gate = KeyGate::new();
    let run_1_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_1.run_id,
        run_1.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_1_enter.from, RunState::Leased);
    assert_eq!(run_1_enter.to, RunState::Running);
    assert_eq!(run_1_enter.gate_outcome, "Acquired");
    assert!(run_1_enter.transition_applied);

    let run_2_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_2.run_id,
        run_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_2_enter.from, RunState::Leased);
    assert_eq!(run_2_enter.to, RunState::Running);
    assert_eq!(run_2_enter.gate_outcome, "KeyOccupied");
    assert!(!run_2_enter.transition_applied);

    let key = ConcurrencyKey::new("ck-a-shared");
    assert!(key_gate.is_key_occupied(&key));
    assert_eq!(key_gate.key_holder(&key), Some(run_1.run_id));

    let expected_rows = vec![
        RunRowTruth {
            run_id: run_1.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("ck-a-shared"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_2.run_id,
            expected_state: "Leased",
            expected_concurrency_key: Some("ck-a-shared"),
            expected_attempt_count: 0,
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_run_rows_truth(&mut router, &expected_rows).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_truth(&mut router, spec.expected_metrics_pre_restart).await;

    drop(router);
    assert_restart_parity(&data_dir, spec, &expected_rows).await;
}

#[tokio::test]
async fn concurrency_key_different_keys_can_run_concurrently_without_false_blocking() {
    let spec = ConcurrencyScenarioSpec {
        label: "p8-005-ck-b-different-key-overlap",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 2,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 2,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 2.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 2.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let run_a =
        seed_once_run_to_leased(&data_dir, "b5b50000-0000-0000-0000-000000000001", Some("key-a"));
    let run_b =
        seed_once_run_to_leased(&data_dir, "b5b50000-0000-0000-0000-000000000002", Some("key-b"));
    assert_ne!(run_a.run_id, run_b.run_id, "scenario runs must remain identity-distinct");

    let mut key_gate = KeyGate::new();
    let run_a_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_a.run_id,
        run_a.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_a_enter.gate_outcome, "Acquired");
    assert!(run_a_enter.transition_applied);

    let run_b_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_b.run_id,
        run_b.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_b_enter.gate_outcome, "Acquired");
    assert!(run_b_enter.transition_applied);

    let key_a = ConcurrencyKey::new("key-a");
    let key_b = ConcurrencyKey::new("key-b");
    assert_eq!(key_gate.key_holder(&key_a), Some(run_a.run_id));
    assert_eq!(key_gate.key_holder(&key_b), Some(run_b.run_id));

    let expected_rows = vec![
        RunRowTruth {
            run_id: run_a.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("key-a"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_b.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("key-b"),
            expected_attempt_count: 0,
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_run_rows_truth(&mut router, &expected_rows).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_truth(&mut router, spec.expected_metrics_pre_restart).await;

    drop(router);
    assert_restart_parity(&data_dir, spec, &expected_rows).await;
}

#[tokio::test]
async fn concurrency_key_release_enables_waiting_same_key_run_to_acquire_and_run() {
    let spec = ConcurrencyScenarioSpec {
        label: "p8-005-ck-c-release-then-reacquire",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 1,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 1.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let run_1 = seed_once_run_to_leased(
        &data_dir,
        "c5c50000-0000-0000-0000-000000000001",
        Some("ck-c-shared"),
    );
    let run_2 = seed_once_run_to_leased(
        &data_dir,
        "c5c50000-0000-0000-0000-000000000002",
        Some("ck-c-shared"),
    );

    let mut key_gate = KeyGate::new();
    let run_1_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_1.run_id,
        run_1.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_1_enter.gate_outcome, "Acquired");
    assert!(run_1_enter.transition_applied);

    let run_2_first_attempt = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_2.run_id,
        run_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_2_first_attempt.gate_outcome, "KeyOccupied");
    assert!(!run_2_first_attempt.transition_applied);

    let run_1_exit = support::evaluate_and_apply_running_exit_with_key_gate(
        &data_dir,
        run_1.run_id,
        run_1.concurrency_key.as_deref(),
        RunState::Completed,
        &mut key_gate,
    );
    assert_eq!(run_1_exit.from, RunState::Running);
    assert_eq!(run_1_exit.to, RunState::Completed);
    assert_eq!(run_1_exit.gate_outcome, "Released");
    assert!(run_1_exit.transition_applied);

    let run_2_second_attempt = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_2.run_id,
        run_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_2_second_attempt.gate_outcome, "Acquired");
    assert!(run_2_second_attempt.transition_applied);

    let key = ConcurrencyKey::new("ck-c-shared");
    assert_eq!(key_gate.key_holder(&key), Some(run_2.run_id));

    let expected_rows = vec![
        RunRowTruth {
            run_id: run_1.run_id,
            expected_state: "Completed",
            expected_concurrency_key: Some("ck-c-shared"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_2.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("ck-c-shared"),
            expected_attempt_count: 0,
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_run_rows_truth(&mut router, &expected_rows).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_truth(&mut router, spec.expected_metrics_pre_restart).await;

    drop(router);
    assert_restart_parity(&data_dir, spec, &expected_rows).await;
}

#[tokio::test]
async fn concurrency_key_absent_runs_do_not_occupy_key_gate_and_do_not_interfere_with_keyed_runs() {
    let spec = ConcurrencyScenarioSpec {
        label: "p8-005-ck-d-no-key-non-interference",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 4,
            total_runs: 4,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 3,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 4,
            total_runs: 4,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 1,
            running: 3,
            retry_wait: 0,
            completed: 0,
            failed: 0,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 3.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 1.0,
            running: 3.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 0.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let run_key_1 = seed_once_run_to_leased(
        &data_dir,
        "d5d50000-0000-0000-0000-000000000001",
        Some("shared-key"),
    );
    let run_key_2 = seed_once_run_to_leased(
        &data_dir,
        "d5d50000-0000-0000-0000-000000000002",
        Some("shared-key"),
    );
    let run_none_1 =
        seed_once_run_to_leased(&data_dir, "d5d50000-0000-0000-0000-000000000003", None);
    let run_none_2 =
        seed_once_run_to_leased(&data_dir, "d5d50000-0000-0000-0000-000000000004", None);

    let mut key_gate = KeyGate::new();

    let key_1_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_key_1.run_id,
        run_key_1.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(key_1_enter.gate_outcome, "Acquired");
    assert!(key_1_enter.transition_applied);

    let key_2_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_key_2.run_id,
        run_key_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(key_2_enter.gate_outcome, "KeyOccupied");
    assert!(!key_2_enter.transition_applied);

    let none_1_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_none_1.run_id,
        run_none_1.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(none_1_enter.gate_outcome, "NoAction");
    assert!(none_1_enter.transition_applied);

    let none_2_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_none_2.run_id,
        run_none_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(none_2_enter.gate_outcome, "NoAction");
    assert!(none_2_enter.transition_applied);

    let shared_key = ConcurrencyKey::new("shared-key");
    assert!(key_gate.is_key_occupied(&shared_key));
    assert_eq!(key_gate.key_holder(&shared_key), Some(run_key_1.run_id));

    let expected_rows = vec![
        RunRowTruth {
            run_id: run_key_1.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("shared-key"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_key_2.run_id,
            expected_state: "Leased",
            expected_concurrency_key: Some("shared-key"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_none_1.run_id,
            expected_state: "Running",
            expected_concurrency_key: None,
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_none_2.run_id,
            expected_state: "Running",
            expected_concurrency_key: None,
            expected_attempt_count: 0,
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_run_rows_truth(&mut router, &expected_rows).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_truth(&mut router, spec.expected_metrics_pre_restart).await;

    drop(router);
    assert_restart_parity(&data_dir, spec, &expected_rows).await;
}

/// End-to-end proof that two tasks sharing a concurrency key are never concurrently
/// Running — the dispatch loop serializes their execution through the key gate.
#[tokio::test]
async fn concurrency_key_e2e_serialized_execution_via_engine() {
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_engine::time::clock::MockClock;
    use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
    use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
    use actionqueue_runtime::engine::ActionQueueEngine;

    let data_dir = support::unique_data_dir("p8-005-ck-e2e-serialized");

    // Track concurrent execution — if two handlers with the same concurrency key
    // run at the same time, the max concurrent count will exceed 1.
    let concurrent_count = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    #[derive(Debug, Clone)]
    struct ConcurrencyTracker {
        concurrent: Arc<AtomicUsize>,
        max: Arc<AtomicUsize>,
    }

    impl ExecutorHandler for ConcurrencyTracker {
        fn execute(&self, ctx: ExecutorContext) -> HandlerOutput {
            let _input = ctx.input;
            let prev = self.concurrent.fetch_add(1, Ordering::SeqCst);
            let current = prev + 1;
            let mut old_max = self.max.load(Ordering::SeqCst);
            while current > old_max {
                match self.max.compare_exchange_weak(
                    old_max,
                    current,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => old_max = actual,
                }
            }
            std::thread::sleep(Duration::from_millis(5));
            self.concurrent.fetch_sub(1, Ordering::SeqCst);
            HandlerOutput::Success { output: None, consumption: vec![] }
        }
    }

    let handler = ConcurrencyTracker {
        concurrent: Arc::clone(&concurrent_count),
        max: Arc::clone(&max_concurrent),
    };

    let config = RuntimeConfig {
        data_dir: data_dir.clone(),
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(4).expect("4 is non-zero"),
        lease_timeout_secs: 300,
        tick_interval: Duration::from_millis(10),
        ..RuntimeConfig::default()
    };

    let clock = MockClock::new(1_000_000);
    let engine = ActionQueueEngine::new(config, handler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap should succeed");

    // Submit two tasks sharing the SAME concurrency key.
    let shared_key = "e2e-serialize-key";
    for _ in 0..2 {
        let constraints =
            TaskConstraints::new(1, None, Some(shared_key.to_string())).expect("valid constraints");
        let spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"ck-e2e".to_vec()),
            RunPolicy::Once,
            constraints,
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit should succeed");
    }

    let summary = boot.run_until_idle().await.expect("run_until_idle should succeed");

    // Both tasks dispatched and completed.
    assert_eq!(summary.total_dispatched, 2);
    assert_eq!(summary.total_completed, 2);

    // All runs terminal.
    for run in boot.projection().run_instances() {
        assert!(
            run.state().is_terminal(),
            "run {} should be terminal, got {:?}",
            run.id(),
            run.state()
        );
    }

    // Key invariant: max concurrent was never > 1, proving serialized execution.
    let observed_max = max_concurrent.load(Ordering::SeqCst);
    assert_eq!(
        observed_max, 1,
        "concurrency key should serialize execution — max concurrent was {observed_max}, expected \
         1"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[tokio::test]
async fn concurrency_key_released_on_failure_allows_waiting_run_to_acquire() {
    let spec = ConcurrencyScenarioSpec {
        label: "p8-005-ck-f-release-on-failure",
        expected_stats_pre_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 1,
            canceled: 0,
        },
        expected_stats_post_restart: StatsTruth {
            total_tasks: 2,
            total_runs: 2,
            attempts_total: 0,
            scheduled: 0,
            ready: 0,
            leased: 0,
            running: 1,
            retry_wait: 0,
            completed: 0,
            failed: 1,
            canceled: 0,
        },
        expected_metrics_pre_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
        expected_metrics_post_restart: MetricsTruth {
            scheduled: 0.0,
            ready: 0.0,
            leased: 0.0,
            running: 1.0,
            retry_wait: 0.0,
            completed: 0.0,
            failed: 1.0,
            canceled: 0.0,
            attempts_success: 0.0,
            attempts_failure: 0.0,
            attempts_timeout: 0.0,
        },
    };

    let data_dir = support::unique_data_dir(spec.label);
    let run_1 = seed_once_run_to_leased(
        &data_dir,
        "c6c60000-0000-0000-0000-000000000001",
        Some("ck-f-shared"),
    );
    let run_2 = seed_once_run_to_leased(
        &data_dir,
        "c6c60000-0000-0000-0000-000000000002",
        Some("ck-f-shared"),
    );

    let mut key_gate = KeyGate::new();
    // Run 1 acquires the key.
    let run_1_enter = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_1.run_id,
        run_1.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_1_enter.gate_outcome, "Acquired");
    assert!(run_1_enter.transition_applied);

    // Run 2 is blocked by the occupied key.
    let run_2_first_attempt = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_2.run_id,
        run_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_2_first_attempt.gate_outcome, "KeyOccupied");
    assert!(!run_2_first_attempt.transition_applied);

    // Run 1 transitions to Failed (not Completed) — key must be released.
    let run_1_exit = support::evaluate_and_apply_running_exit_with_key_gate(
        &data_dir,
        run_1.run_id,
        run_1.concurrency_key.as_deref(),
        RunState::Failed,
        &mut key_gate,
    );
    assert_eq!(run_1_exit.from, RunState::Running);
    assert_eq!(run_1_exit.to, RunState::Failed);
    assert_eq!(run_1_exit.gate_outcome, "Released");
    assert!(run_1_exit.transition_applied);

    // Run 2 should now acquire the key.
    let run_2_second_attempt = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_2.run_id,
        run_2.concurrency_key.as_deref(),
        &mut key_gate,
    );
    assert_eq!(run_2_second_attempt.gate_outcome, "Acquired");
    assert!(run_2_second_attempt.transition_applied);

    let key = ConcurrencyKey::new("ck-f-shared");
    assert_eq!(key_gate.key_holder(&key), Some(run_2.run_id));

    let expected_rows = vec![
        RunRowTruth {
            run_id: run_1.run_id,
            expected_state: "Failed",
            expected_concurrency_key: Some("ck-f-shared"),
            expected_attempt_count: 0,
        },
        RunRowTruth {
            run_id: run_2.run_id,
            expected_state: "Running",
            expected_concurrency_key: Some("ck-f-shared"),
            expected_attempt_count: 0,
        },
    ];

    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_run_rows_truth(&mut router, &expected_rows).await;
    assert_stats_truth(&mut router, spec.expected_stats_pre_restart).await;
    assert_metrics_truth(&mut router, spec.expected_metrics_pre_restart).await;

    drop(router);
    assert_restart_parity(&data_dir, spec, &expected_rows).await;
}
