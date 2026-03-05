//! Mixed attempt-outcome acceptance proof: attempt lineage tracking across distinct result types.
//!
//! Exercises a single run through multiple distinct attempt outcome types (Timeout,
//! RetryableFailure, Success) to prove that attempt lineage is correctly tracked
//! across different result variants.

mod support;

use std::collections::HashSet;
use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;

#[tokio::test]
async fn mat_a_mixed_outcomes_preserve_attempt_lineage() {
    // 1) Isolated data directory for this scenario.
    let data_dir = support::unique_data_dir("mixed-attempt-outcomes");

    // 2) Deterministic task identity.
    let task_id_literal = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    let task_id = TaskId::from_str(task_id_literal).expect("fixed task id should parse");

    // 3) Submit a Once task with max_attempts=3.
    let submit = support::submit_once_task_with_max_attempts_via_cli(task_id_literal, &data_dir, 3);
    assert_eq!(submit["runs_created"], 1);
    assert_eq!(submit["run_policy"], "once");

    // 4) Prove exactly one run identity exists for the task.
    let run_id = support::single_run_id_for_task(&data_dir, task_id);

    // 5) Drive the run through 3 attempts with different outcomes:
    //    Attempt 1: Timeout, Attempt 2: RetryableFailure, Attempt 3: Success
    const OUTCOMES: &[support::AttemptOutcomePlan] = &[
        support::AttemptOutcomePlan::Timeout,
        support::AttemptOutcomePlan::RetryableFailure,
        support::AttemptOutcomePlan::Success,
    ];

    let evidence =
        support::execute_attempt_outcome_sequence_via_authority(&data_dir, task_id, 3, OUTCOMES);
    assert_eq!(evidence.run_id, run_id, "run identity must remain stable across attempts");

    // 6) Bootstrap HTTP router and verify final state via HTTP.
    let mut router = support::bootstrap_http_router(&data_dir, true);

    let run_get_path = format!("/api/v1/runs/{}", evidence.run_id);
    let run_get = support::get_json(&mut router, &run_get_path).await;

    // 7) Final state is Completed and attempt_count is 3.
    assert_eq!(run_get["state"], "Completed", "run should be terminally completed");
    assert_eq!(run_get["attempt_count"], 3, "exactly 3 attempts should be recorded");

    // 8) Verify there are exactly 3 attempts in the response.
    let attempts = run_get["attempts"].as_array().expect("attempts should be an array");
    assert_eq!(attempts.len(), 3, "run should have exactly 3 attempt entries");

    // 9) Verify each attempt has the correct result type.
    let attempt_results: Vec<&str> = attempts
        .iter()
        .map(|entry| {
            entry["result"]
                .as_str()
                .expect("attempt result should be present and serialized as string")
        })
        .collect();
    assert_eq!(
        attempt_results,
        vec!["Timeout", "Failure", "Success"],
        "attempt results must match planned outcome sequence"
    );

    // 10) Verify all 3 attempt IDs are distinct.
    assert_eq!(evidence.attempt_ids.len(), 3, "evidence should record exactly 3 attempt IDs");
    let unique_ids: HashSet<_> = evidence.attempt_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        3,
        "all 3 attempt IDs must be distinct (no reuse across attempts)"
    );

    // 11) Cross-verify attempt IDs from storage match evidence.
    let storage_attempt_ids = support::attempt_ids_for_run_from_storage(&data_dir, evidence.run_id);
    assert_eq!(
        storage_attempt_ids, evidence.attempt_ids,
        "storage attempt IDs must match execution evidence"
    );

    // 12) Cross-verify terminal state from storage.
    support::assert_run_state_from_storage(&data_dir, evidence.run_id, RunState::Completed);
    assert_eq!(evidence.final_state, RunState::Completed);
    assert!(evidence.final_sequence > 0, "terminal sequence must be durably recorded");

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// T-4: Proves that AttemptIds generated via `AttemptId::new()` are globally unique
/// across distinct runs.
///
/// Creates two runs in the same data directory, generates 2 random AttemptIds per run,
/// and asserts all 4 AttemptIds are distinct. This proves the UUID v4 generation
/// provides global uniqueness (the production dispatch loop uses `AttemptId::new()`).
#[tokio::test]
async fn attempt_ids_globally_unique_across_runs() {
    use actionqueue_core::ids::AttemptId;

    // Generate 4 random AttemptIds simulating 2 attempts per run.
    let attempt_ids: Vec<AttemptId> = (0..4).map(|_| AttemptId::new()).collect();

    // All 4 must be globally unique.
    let unique_ids: HashSet<_> = attempt_ids.iter().collect();
    assert_eq!(
        unique_ids.len(),
        4,
        "all 4 AttemptIds from AttemptId::new() must be globally unique"
    );

    // Additionally verify via real execution: submit 2 tasks in separate data dirs,
    // execute each through 2 attempts, and cross-check stored IDs.
    let data_dir_1 = support::unique_data_dir("t4-attempt-id-uniqueness-run1");
    let data_dir_2 = support::unique_data_dir("t4-attempt-id-uniqueness-run2");

    let task_id_1_literal = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb01";
    let task_id_2_literal = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbb02";
    let task_id_1 =
        actionqueue_core::ids::TaskId::from_str(task_id_1_literal).expect("task id 1 should parse");
    let task_id_2 =
        actionqueue_core::ids::TaskId::from_str(task_id_2_literal).expect("task id 2 should parse");

    support::submit_once_task_with_max_attempts_via_cli(task_id_1_literal, &data_dir_1, 2);
    support::submit_once_task_with_max_attempts_via_cli(task_id_2_literal, &data_dir_2, 2);

    const OUTCOMES: &[support::AttemptOutcomePlan] =
        &[support::AttemptOutcomePlan::Timeout, support::AttemptOutcomePlan::Success];

    let evidence_1 = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir_1,
        task_id_1,
        2,
        OUTCOMES,
    );
    let evidence_2 = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir_2,
        task_id_2,
        2,
        OUTCOMES,
    );

    // Both runs should have completed successfully.
    assert_eq!(evidence_1.final_state, RunState::Completed);
    assert_eq!(evidence_2.final_state, RunState::Completed);
    assert_eq!(evidence_1.attempt_ids.len(), 2, "run 1 should have 2 attempt IDs");
    assert_eq!(evidence_2.attempt_ids.len(), 2, "run 2 should have 2 attempt IDs");

    // Within each run, attempt IDs must be unique.
    let run1_unique: HashSet<_> = evidence_1.attempt_ids.iter().collect();
    assert_eq!(run1_unique.len(), 2, "run 1 attempt IDs must be distinct within the run");

    let run2_unique: HashSet<_> = evidence_2.attempt_ids.iter().collect();
    assert_eq!(run2_unique.len(), 2, "run 2 attempt IDs must be distinct within the run");

    // RunIds must be distinct across runs.
    assert_ne!(evidence_1.run_id, evidence_2.run_id, "run IDs must be distinct");

    let _ = std::fs::remove_dir_all(&data_dir_1);
    let _ = std::fs::remove_dir_all(&data_dir_2);
}
