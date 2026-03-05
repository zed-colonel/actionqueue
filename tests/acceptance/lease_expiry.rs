//! P6-004 Time-based lease expiry acceptance proof suite.
//!
//! These tests verify the complete lease expiry lifecycle using a controllable
//! `MockClock`. The test scenarios prove:
//!
//! - A lease is Active when MockClock time is before the expiry timestamp.
//! - A lease is Expired when MockClock time is at or past the expiry timestamp.
//! - An expired lease transitions the run from Leased back to Ready (re-eligibility).
//! - A re-eligible run can complete a full execution lifecycle after lease expiry.
//! - Lease expiry state is durable across restart boundaries.

mod support;

use std::path::Path;
use std::str::FromStr;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::run::state::RunState;
use actionqueue_engine::lease::expiry::{evaluate_expires_at, ExpiryResult};
use actionqueue_engine::lease::model::LeaseExpiry;
use actionqueue_engine::time::clock::{Clock, MockClock};
use actionqueue_executor_local::ExecutorResponse;
use support::{MetricsTruth, StatsTruth};

// ---------------------------------------------------------------------------
// Deterministic scenario spec types
// ---------------------------------------------------------------------------

/// Deterministic scenario contract for lease-expiry acceptance proofs.
#[derive(Debug, Clone, Copy)]
struct LeaseExpiryScenarioSpec {
    /// Human-readable scenario label used for isolated data-dir naming.
    label: &'static str,
    /// Deterministic task identifier literal.
    task_id_literal: &'static str,
    /// Hard retry cap for the submitted Once task.
    max_attempts: u32,
    /// Lease owner identity used for the initial lease.
    lease_owner: &'static str,
    /// MockClock starting time (before lease acquisition).
    clock_start: u64,
    /// Lease duration in seconds (added to clock_start for expiry).
    lease_duration_secs: u64,
}

// ---------------------------------------------------------------------------
// Authority-lane helpers (local to this test file)
// ---------------------------------------------------------------------------

fn submit_run_state_transition_via_authority(
    data_dir: &Path,
    run_id: RunId,
    previous_state: RunState,
    new_state: RunState,
) -> u64 {
    support::transition_run_state_via_authority(data_dir, run_id, previous_state, new_state)
}

fn submit_attempt_start_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
) -> u64 {
    support::submit_attempt_start_via_authority(data_dir, run_id, attempt_id)
}

fn submit_attempt_finish_response_via_authority(
    data_dir: &Path,
    run_id: RunId,
    attempt_id: AttemptId,
    response: &ExecutorResponse,
) -> u64 {
    support::submit_attempt_finish_response_via_authority(data_dir, run_id, attempt_id, response)
}

// ---------------------------------------------------------------------------
// HTTP / metrics assertion helpers
// ---------------------------------------------------------------------------

async fn assert_stats_truth(router: &mut axum::Router<()>, expected: StatsTruth) {
    support::assert_stats_truth(router, expected).await;
}

async fn assert_metrics_truth(router: &mut axum::Router<()>, expected: MetricsTruth) {
    support::assert_metrics_truth(router, expected).await;
}

#[allow(clippy::too_many_arguments)] // Test assertion helper with naturally many parameters
async fn assert_http_run_truth(
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

// ---------------------------------------------------------------------------
// Test A: MockClock-driven lease expiry detection proves Active -> Expired
//         boundary with exact-second precision.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_mock_clock_proves_active_then_expired_boundary() {
    let spec = LeaseExpiryScenarioSpec {
        label: "p6-004-le-a-mock-clock-boundary",
        task_id_literal: "e4e40000-0000-0000-0000-000000000001",
        max_attempts: 1,
        lease_owner: "p6-004-le-a-owner",
        clock_start: 10_000,
        lease_duration_secs: 30,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");

    // 1) Submit a Once task
    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    // 2) Promote Scheduled -> Ready, then transition Ready -> Leased
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);

    // 3) Acquire a lease with a deterministic expiry derived from MockClock
    let mut clock = MockClock::new(spec.clock_start);
    let lease_expiry_timestamp = clock.now() + spec.lease_duration_secs;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        spec.lease_owner,
        lease_expiry_timestamp,
    );

    // 4) Verify lease is Active at clock_start (well before expiry)
    let expiry = LeaseExpiry::at(lease_expiry_timestamp);
    let result_before = evaluate_expires_at(clock.now(), expiry);
    assert_eq!(
        result_before,
        ExpiryResult::Active,
        "lease must be Active when MockClock is at {}, expiry at {}",
        clock.now(),
        lease_expiry_timestamp
    );
    assert!(!result_before.can_reenter_eligibility());

    // 5) Advance clock to 1 second before expiry -- still Active
    clock.advance_by(spec.lease_duration_secs - 1);
    let result_1s_before = evaluate_expires_at(clock.now(), expiry);
    assert_eq!(
        result_1s_before,
        ExpiryResult::Active,
        "lease must be Active 1 second before expiry at {}",
        clock.now()
    );
    assert!(!result_1s_before.can_reenter_eligibility());

    // 6) Advance clock to exact expiry -- Expired
    clock.advance_by(1);
    assert_eq!(clock.now(), lease_expiry_timestamp, "clock should be exactly at lease expiry");
    let result_at_expiry = evaluate_expires_at(clock.now(), expiry);
    assert_eq!(
        result_at_expiry,
        ExpiryResult::Expired,
        "lease must be Expired when MockClock equals expiry timestamp"
    );
    assert!(result_at_expiry.can_reenter_eligibility());

    // 7) Advance clock past expiry -- still Expired
    clock.advance_by(100);
    let result_past_expiry = evaluate_expires_at(clock.now(), expiry);
    assert_eq!(
        result_past_expiry,
        ExpiryResult::Expired,
        "lease must remain Expired well past expiry timestamp"
    );
    assert!(result_past_expiry.can_reenter_eligibility());

    // 8) Verify lease metadata is visible via HTTP before expiry detection
    let mut router = support::bootstrap_http_router(&data_dir, true);
    let lease_evidence = support::current_lease_from_run_get(&mut router, run_id)
        .await
        .expect("lease metadata should be present while run is Leased");
    assert_eq!(lease_evidence.owner, spec.lease_owner);
    assert_eq!(lease_evidence.expiry, lease_expiry_timestamp);

    assert_http_run_truth(&mut router, spec.task_id_literal, run_id, "Leased", &[], Some("leased"))
        .await;

    assert_stats_truth(
        &mut router,
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
        &mut router,
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
    drop(router);

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test B: Lease expiry triggers re-eligibility (Leased -> Ready) and the run
//         completes successfully after re-dispatch.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_triggers_reeligibility_and_run_completes_after_redispatch() {
    let spec = LeaseExpiryScenarioSpec {
        label: "p6-004-le-b-reeligibility-complete",
        task_id_literal: "e4e40000-0000-0000-0000-000000000002",
        max_attempts: 2,
        lease_owner: "p6-004-le-b-owner",
        clock_start: 20_000,
        lease_duration_secs: 60,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");
    let attempt_id_1 = AttemptId::from_str("00000000-0000-0000-0000-0000e4e40001")
        .expect("deterministic attempt id should parse");

    // 1) Submit and advance to Leased with explicit lease
    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);

    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);

    let mut clock = MockClock::new(spec.clock_start);
    let lease_expiry_timestamp = clock.now() + spec.lease_duration_secs;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        spec.lease_owner,
        lease_expiry_timestamp,
    );

    // 2) Confirm lease is Active at current clock time
    let expiry = LeaseExpiry::at(lease_expiry_timestamp);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Active);

    // 3) Advance clock past expiry to trigger detection
    clock.advance_by(spec.lease_duration_secs + 10);
    assert_eq!(
        evaluate_expires_at(clock.now(), expiry),
        ExpiryResult::Expired,
        "MockClock advanced past expiry must detect lease as expired"
    );

    // 4) Apply lease-expire command via authority (simulating dispatch loop detection)
    support::lease_expire_for_run_via_authority(
        &data_dir,
        run_id,
        spec.lease_owner,
        lease_expiry_timestamp,
    );

    // 5) Verify the run is now Ready (re-eligible) and lease is cleared
    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(&mut router, spec.task_id_literal, run_id, "Ready", &[], None).await;
    assert!(
        support::current_lease_from_run_get(&mut router, run_id).await.is_none(),
        "lease metadata must be cleared after lease-expire"
    );
    assert_stats_truth(
        &mut router,
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
        &mut router,
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
    drop(router);

    // 6) Complete the run through a full execution lifecycle after re-eligibility
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    let re_lease_owner = "p6-004-le-b-owner-redispatch";
    let re_lease_expiry = clock.now() + spec.lease_duration_secs;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        re_lease_owner,
        re_lease_expiry,
    );
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
        &ExecutorResponse::Success { output: None },
    );
    submit_run_state_transition_via_authority(
        &data_dir,
        run_id,
        RunState::Running,
        RunState::Completed,
    );

    // 7) Verify terminal Completed state via HTTP surfaces
    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut router,
        spec.task_id_literal,
        run_id,
        "Completed",
        &[attempt_id_1.to_string()],
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut router,
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
        &mut router,
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

    // 8) Restart durability proof
    drop(router);
    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut restarted_router,
        spec.task_id_literal,
        run_id,
        "Completed",
        &[attempt_id_1.to_string()],
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut restarted_router,
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
        &mut restarted_router,
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

    // 9) No-extra-run proof
    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test C: Lease expiry with retry -- first attempt expires, second attempt
//         succeeds after re-dispatch. Proves attempt lineage continuity.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_preserves_attempt_lineage_across_reeligibility_and_retry() {
    let spec = LeaseExpiryScenarioSpec {
        label: "p6-004-le-c-expiry-retry-lineage",
        task_id_literal: "e4e40000-0000-0000-0000-000000000003",
        max_attempts: 3,
        lease_owner: "p6-004-le-c-owner",
        clock_start: 50_000,
        lease_duration_secs: 45,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");
    let attempt_id_1 = AttemptId::from_str("00000000-0000-0000-0000-0000e4e40010")
        .expect("deterministic attempt id should parse");
    let attempt_id_2 = AttemptId::from_str("00000000-0000-0000-0000-0000e4e40020")
        .expect("deterministic attempt id should parse");

    // 1) Submit with max_attempts=3
    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);

    // 2) First dispatch: promote, transition through lifecycle, fail with retryable
    //    failure. No explicit lease-acquire for the first dispatch cycle -- this
    //    matches the retry-cap test pattern where lease WAL events are not the
    //    focus. The dispatch loop internally tracks the lease in memory without
    //    necessarily releasing it via WAL before the retry transition.
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
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
            error: "p6-004-le-c retryable failure attempt 1".to_string(),
        },
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

    let mut clock = MockClock::new(spec.clock_start);

    // 3) Second dispatch: lease is acquired but expires before completion
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    clock.advance_by(10); // advance a bit before second lease
    let lease_2_expiry = clock.now() + spec.lease_duration_secs;
    let lease_2_owner = "p6-004-le-c-owner-attempt-2";
    support::lease_acquire_for_run_via_authority(&data_dir, run_id, lease_2_owner, lease_2_expiry);

    // Verify lease is Active
    let expiry_2 = LeaseExpiry::at(lease_2_expiry);
    assert_eq!(evaluate_expires_at(clock.now(), expiry_2), ExpiryResult::Active);

    // Advance clock past lease expiry (simulating worker hang or crash)
    clock.advance_by(spec.lease_duration_secs + 5);
    assert_eq!(
        evaluate_expires_at(clock.now(), expiry_2),
        ExpiryResult::Expired,
        "second lease must be detected as expired"
    );

    // Apply lease-expire to restore re-eligibility
    support::lease_expire_for_run_via_authority(&data_dir, run_id, lease_2_owner, lease_2_expiry);

    // Verify run is back to Ready with attempt-1 lineage preserved
    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut router,
        spec.task_id_literal,
        run_id,
        "Ready",
        &[attempt_id_1.to_string()],
        None,
    )
    .await;
    assert!(
        support::current_lease_from_run_get(&mut router, run_id).await.is_none(),
        "lease must be cleared after expire"
    );
    drop(router);

    // 4) Third dispatch (second actual attempt): succeed this time
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);
    clock.advance_by(5);
    let lease_3_expiry = clock.now() + spec.lease_duration_secs;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        "p6-004-le-c-owner-attempt-3",
        lease_3_expiry,
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

    // 5) Verify terminal state with full attempt lineage
    let expected_attempt_ids = vec![attempt_id_1.to_string(), attempt_id_2.to_string()];
    let mut router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut router,
        spec.task_id_literal,
        run_id,
        "Completed",
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut router,
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
        &mut router,
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

    // 6) Restart durability proof
    drop(router);
    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut restarted_router,
        spec.task_id_literal,
        run_id,
        "Completed",
        &expected_attempt_ids,
        Some("terminal"),
    )
    .await;
    assert_stats_truth(
        &mut restarted_router,
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

    // 7) Identity stability proof
    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    // 8) Attempt lineage proof
    let post_restart_attempt_ids =
        support::run_get_attempt_ids(&mut restarted_router, run_id).await;
    assert_eq!(
        post_restart_attempt_ids, expected_attempt_ids,
        "attempt lineage must survive lease-expiry, re-dispatch, and restart"
    );
    assert_ne!(attempt_id_1, attempt_id_2, "distinct attempts must have distinct IDs");

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test D: Deterministic evaluation proves boundary-exact lease expiry at the
//         second granularity using MockClock set/advance operations.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_deterministic_boundary_proof_with_mock_clock_set_and_advance() {
    // This test exercises the MockClock set() and advance_by() methods to
    // confirm that the lease expiry evaluation is purely a function of
    // (current_time, expires_at) with no hidden wall-clock dependency.

    let mut clock = MockClock::new(0);
    let expiry = LeaseExpiry::at(1000);

    // At time 0: Active
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Active);

    // At time 500: Active
    clock.set(500);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Active);

    // At time 999: Active
    clock.set(999);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Active);
    assert!(!evaluate_expires_at(clock.now(), expiry).can_reenter_eligibility());

    // At time 1000: Expired (boundary)
    clock.advance_by(1);
    assert_eq!(clock.now(), 1000);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Expired);
    assert!(evaluate_expires_at(clock.now(), expiry).can_reenter_eligibility());

    // At time 1001: still Expired
    clock.advance_by(1);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Expired);

    // Time reversal via set() does not re-activate (set doesn't validate monotonicity)
    // -- this proves the evaluation is purely positional
    clock.set(500);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Active);

    // Back to past-expiry
    clock.set(2000);
    assert_eq!(evaluate_expires_at(clock.now(), expiry), ExpiryResult::Expired);

    // Verify system_time conversion is correct at each step
    clock.set(1000);
    let system_time = clock.now_system_time();
    let duration = system_time.duration_since(std::time::UNIX_EPOCH).expect("time overflow");
    assert_eq!(duration.as_secs(), 1000);
}

// ---------------------------------------------------------------------------
// Test E: Lease expiry on a Leased run is durable across restart -- the run
//         remains Ready after restart without phantom regression to Leased.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_durability_across_restart_without_phantom_regression() {
    let spec = LeaseExpiryScenarioSpec {
        label: "p6-004-le-e-restart-durability",
        task_id_literal: "e4e40000-0000-0000-0000-000000000005",
        max_attempts: 1,
        lease_owner: "p6-004-le-e-owner",
        clock_start: 100_000,
        lease_duration_secs: 120,
    };

    let data_dir = support::unique_data_dir(spec.label);
    let task_id = TaskId::from_str(spec.task_id_literal).expect("fixed task id should parse");

    // 1) Submit and advance to Leased with lease
    let submit = support::submit_once_task_with_constraints_via_cli(
        spec.task_id_literal,
        &data_dir,
        Some(&format!("{{\"max_attempts\":{}}}", spec.max_attempts)),
    );
    assert_eq!(submit["runs_created"], 1);

    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
    submit_run_state_transition_via_authority(&data_dir, run_id, RunState::Ready, RunState::Leased);

    let mut clock = MockClock::new(spec.clock_start);
    let lease_expiry_timestamp = clock.now() + spec.lease_duration_secs;
    support::lease_acquire_for_run_via_authority(
        &data_dir,
        run_id,
        spec.lease_owner,
        lease_expiry_timestamp,
    );

    // 2) Confirm Leased state before expiry
    let checkpoint = support::capture_checkpoint(&data_dir, run_id);
    assert_eq!(checkpoint.pre_restart_state, RunState::Leased);

    let mut pre_router = support::bootstrap_http_router(&data_dir, true);
    let lease_evidence = support::current_lease_from_run_get(&mut pre_router, run_id)
        .await
        .expect("lease metadata should be present while run is Leased");
    assert_eq!(lease_evidence.owner, spec.lease_owner);
    assert_eq!(lease_evidence.expiry, lease_expiry_timestamp);
    drop(pre_router);

    // 3) Advance clock past expiry and submit lease-expire
    clock.advance_by(spec.lease_duration_secs + 30);
    assert_eq!(
        evaluate_expires_at(clock.now(), LeaseExpiry::at(lease_expiry_timestamp)),
        ExpiryResult::Expired
    );

    support::lease_expire_for_run_via_authority(
        &data_dir,
        run_id,
        spec.lease_owner,
        lease_expiry_timestamp,
    );

    // 4) Verify Ready state before restart
    let mut post_expire_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(
        &mut post_expire_router,
        spec.task_id_literal,
        run_id,
        "Ready",
        &[],
        None,
    )
    .await;
    assert!(
        support::current_lease_from_run_get(&mut post_expire_router, run_id).await.is_none(),
        "lease metadata must be cleared after lease-expire"
    );
    assert_stats_truth(
        &mut post_expire_router,
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
    drop(post_expire_router);

    // 5) Restart and verify Ready state is durable (no phantom Leased regression)
    let mut restarted_router = support::bootstrap_http_router(&data_dir, true);
    assert_http_run_truth(&mut restarted_router, spec.task_id_literal, run_id, "Ready", &[], None)
        .await;
    assert!(
        support::current_lease_from_run_get(&mut restarted_router, run_id).await.is_none(),
        "lease metadata must remain cleared after restart"
    );
    assert_stats_truth(
        &mut restarted_router,
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
        &mut restarted_router,
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

    // 6) Run identity stability
    let stable_run_id = support::single_run_id_for_task(&data_dir, task_id);
    support::assert_run_id_stability(run_id, stable_run_id);
    support::assert_no_new_task_runs(&data_dir, task_id, &[run_id]);

    let _ = std::fs::remove_dir_all(&data_dir);
}
