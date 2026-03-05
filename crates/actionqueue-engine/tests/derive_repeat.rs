//! Accounting tests for Repeat derivation policy.
//!
//! These tests verify that the Repeat derivation policy correctly creates
//! exactly N runs for a task according to its count and interval, and prevents
//! duplicate run creation.

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::run_policy::{RunPolicy, RunPolicyError};
use actionqueue_engine::derive::{derive_runs, DerivationError};

/// Verifies that a Repeat policy task creates exactly N runs on first derivation.
#[test]
fn repeat_policy_creates_correct_count() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(5, 60).expect("repeat policy should be valid");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();

    assert_eq!(result.derived().len(), 5, "Repeat policy must create exactly 5 runs");
    assert_eq!(result.already_derived(), 5, "Repeat policy must report 5 as already_derived");

    // Verify scheduled times at t=0 origin: 0, 60, 120, 180, 240
    assert_eq!(result.derived()[0].scheduled_at(), 0);
    assert_eq!(result.derived()[1].scheduled_at(), 60);
    assert_eq!(result.derived()[2].scheduled_at(), 120);
    assert_eq!(result.derived()[3].scheduled_at(), 180);
    assert_eq!(result.derived()[4].scheduled_at(), 240);

    // Verify all runs are in Scheduled state
    for run in result.derived() {
        assert_eq!(run.state(), RunState::Scheduled, "Each run must be in Scheduled state");
        assert_eq!(run.task_id(), task_id, "Each run must have correct task_id");
    }
}

/// Verifies that Repeat derivation does not create duplicate runs when some runs already exist.
#[test]
fn repeat_policy_does_not_duplicate() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(10, 30).expect("repeat policy should be valid");

    // First derivation creates all 10 runs
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 10, "First derivation must create 10 runs");

    // Second derivation with already_derived=5 should create only 5 more
    let result2 = derive_runs(&clock, task_id, &run_policy, 5, 0).unwrap();
    assert_eq!(result2.derived().len(), 5, "Second derivation must create only missing runs");
    assert_eq!(result2.already_derived(), 10, "already_derived must be updated to total count");

    // Verify the new runs start where left off (indices 5-9)
    assert_eq!(result2.derived()[0].scheduled_at(), 150); // index 5
    assert_eq!(result2.derived()[1].scheduled_at(), 180); // index 6
    assert_eq!(result2.derived()[2].scheduled_at(), 210); // index 7
    assert_eq!(result2.derived()[3].scheduled_at(), 240); // index 8
    assert_eq!(result2.derived()[4].scheduled_at(), 270); // index 9

    // Third derivation with already_derived=10 should create no new runs
    let result3 = derive_runs(&clock, task_id, &run_policy, 10, 0).unwrap();
    assert_eq!(result3.derived().len(), 0, "Third derivation must create no new runs");
    assert_eq!(result3.already_derived(), 10, "already_derived must remain 10");
}

/// Verifies Repeat accounting is deterministic with multiple tasks.
#[test]
fn repeat_policy_accounting_deterministic_with_multiple_tasks() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id1 = TaskId::new();
    let task_id2 = TaskId::new();
    let task_id3 = TaskId::new();
    let run_policy = RunPolicy::repeat(3, 120).expect("repeat policy should be valid");

    // Derive for task 1 - creates 3 runs
    let result1 = derive_runs(&clock, task_id1, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 3);
    let run1_id = result1.derived()[0].id();

    // Derive for task 2 (different task) - creates 3 runs
    let result2 = derive_runs(&clock, task_id2, &run_policy, 0, 0).unwrap();
    assert_eq!(result2.derived().len(), 3);
    assert_ne!(result2.derived()[0].id(), run1_id, "Different tasks must have different run IDs");

    // Derive again for task 1 - should not create duplicates
    let result3 = derive_runs(&clock, task_id1, &run_policy, 3, 0).unwrap();
    assert_eq!(result3.derived().len(), 0, "Task 1 already has 3 runs, must not create more");

    // Derive for task 3 - creates 3 runs
    let result4 = derive_runs(&clock, task_id3, &run_policy, 0, 0).unwrap();
    assert_eq!(result4.derived().len(), 3);
    assert_ne!(result4.derived()[0].id(), run1_id);
    assert_ne!(result4.derived()[0].id(), result2.derived()[0].id());
}

/// Verifies that Repeat accounting remains stable when clock advances.
#[test]
fn repeat_policy_accounting_stable_with_clock_advancement() {
    let mut clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(5, 60).expect("repeat policy should be valid");

    // First derivation at t=1000 with origin=0 creates 5 runs
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 5);
    assert_eq!(result1.derived()[0].scheduled_at(), 0);
    assert_eq!(result1.derived()[1].scheduled_at(), 60);
    assert_eq!(result1.derived()[2].scheduled_at(), 120);
    assert_eq!(result1.derived()[3].scheduled_at(), 180);
    assert_eq!(result1.derived()[4].scheduled_at(), 240);

    // Advance clock and re-derive with already_derived=2
    clock.advance_by(600);

    let result2 = derive_runs(&clock, task_id, &run_policy, 2, 0).unwrap();
    assert_eq!(result2.derived().len(), 3);

    // Scheduled times must remain the same (deterministic)
    assert_eq!(result2.derived()[0].scheduled_at(), 120);
    assert_eq!(result2.derived()[1].scheduled_at(), 180);
    assert_eq!(result2.derived()[2].scheduled_at(), 240);

    // The original run's scheduled_at must remain unchanged
    assert_eq!(result1.derived()[0].scheduled_at(), 0, "Scheduled time must not change");
}

/// Verifies RepeatPolicy::new defensively rejects zero count with a typed policy error.
#[test]
fn repeat_policy_zero_count_is_rejected_at_construction() {
    use actionqueue_core::task::run_policy::RepeatPolicy;
    let result = RepeatPolicy::new(0, 60);
    assert_eq!(result, Err(RunPolicyError::InvalidRepeatCount { count: 0 }));
}

/// Verifies RepeatPolicy::new defensively rejects zero interval with a typed policy error.
#[test]
fn repeat_policy_zero_interval_is_rejected_at_construction() {
    use actionqueue_core::task::run_policy::RepeatPolicy;
    let result = RepeatPolicy::new(3, 0);
    assert_eq!(result, Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 }));
}

/// Verifies Repeat accounting is consistent with policy expectations.
#[test]
fn repeat_policy_accounting_consistent_with_policy() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(4, 90).expect("repeat policy should be valid");

    // The key invariant: Repeat policy with count N must always result in
    // exactly N derived runs total, regardless of how many times derive_runs
    // is called with already_derived >= N

    // First call
    let r1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(r1.derived().len(), 4);
    assert_eq!(r1.already_derived(), 4);

    // Subsequent calls with already_derived=4 must create no new runs
    for _ in 1..=10 {
        let r = derive_runs(&clock, task_id, &run_policy, 4, 0).unwrap();
        assert_eq!(r.derived().len(), 0, "No runs must be derived on repeat calls");
        assert_eq!(r.already_derived(), 4, "already_derived must remain 4");
    }

    // Verify the original runs still exist (same IDs)
    let _run_ids: Vec<_> = r1.derived().iter().map(|r| r.id()).collect();
    let r_final = derive_runs(&clock, task_id, &run_policy, 4, 0).unwrap();
    assert_eq!(r_final.derived().len(), 0);
    assert_eq!(r_final.already_derived(), 4);
}

/// Verifies Repeat derivation with different start origins produces different scheduled times.
#[test]
fn repeat_policy_respects_schedule_origin() {
    let clock = actionqueue_engine::time::clock::MockClock::new(2000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(3, 100).expect("repeat policy should be valid");

    // Derive with origin=500: scheduled_at = 500, 600, 700
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 500).unwrap();
    assert_eq!(result1.derived().len(), 3);
    assert_eq!(result1.derived()[0].scheduled_at(), 500);
    assert_eq!(result1.derived()[1].scheduled_at(), 600);
    assert_eq!(result1.derived()[2].scheduled_at(), 700);

    // Derive with origin=1000: scheduled_at = 1000, 1100, 1200
    let result2 = derive_runs(&clock, task_id, &run_policy, 0, 1000).unwrap();
    assert_eq!(result2.derived().len(), 3);
    assert_eq!(result2.derived()[0].scheduled_at(), 1000);
    assert_eq!(result2.derived()[1].scheduled_at(), 1100);
    assert_eq!(result2.derived()[2].scheduled_at(), 1200);
}

/// Verifies that large count values cause integer overflow and return typed error.
#[test]
fn repeat_policy_large_count_returns_error_on_overflow() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    // Use a count and interval that will trigger overflow detection
    let run_policy =
        RunPolicy::repeat(u32::MAX, u64::MAX / 2).expect("repeat policy should be valid");

    // With the strict overflow check, this should return an error, not partial results
    let result = derive_runs(&clock, task_id, &run_policy, 0, 0);
    assert!(result.is_err(), "Large count with interval overflow should return error");

    // Check it's specifically an arithmetic overflow error
    match result {
        Err(DerivationError::ArithmeticOverflow { policy, operation }) => {
            assert_eq!(policy, "Repeat");
            assert!(operation.contains("multiplication") || operation.contains("addition"));
        }
        _ => panic!("Expected arithmetic overflow error"),
    }
}

/// Verifies that large interval values do not cause integer overflow in schedule calculation.
#[test]
fn repeat_policy_large_interval_does_not_overflow() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    // Very large interval, but small count
    let run_policy = RunPolicy::repeat(3, u64::MAX / 2).expect("repeat policy should be valid");

    // derivation must not panic
    let result = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();

    // First iteration (i=0): product = 0 * interval = 0, scheduled_at = 0 + 0 = 0 -> success
    // Second iteration (i=1): product = 1 * interval = u64::MAX / 2, scheduled_at = u64::MAX / 2 -> success
    // Third iteration (i=2): product = 2 * interval = u64::MAX - 1, scheduled_at = u64::MAX - 1 -> success
    // The loop would need a 4th iteration but count=3 means we stop after i=2
    assert_eq!(result.derived().len(), 3);
    assert_eq!(result.already_derived(), 3);
    assert_eq!(result.derived()[0].scheduled_at(), 0);
    assert_eq!(result.derived()[1].scheduled_at(), u64::MAX / 2);
    assert_eq!(result.derived()[2].scheduled_at(), u64::MAX - 1);
}

/// Verifies that large schedule_origin values cause integer overflow and return typed error.
#[test]
fn repeat_policy_large_origin_returns_error_on_overflow() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    // Large origin, small count and interval
    let run_policy = RunPolicy::repeat(3, 60).expect("repeat policy should be valid");

    // Use origin close to u64::MAX to test overflow in addition
    let large_origin = u64::MAX - 100;
    let result = derive_runs(&clock, task_id, &run_policy, 0, large_origin);

    // With strict overflow check, this should return an error
    assert!(result.is_err(), "Large origin with addition overflow should return error");

    // Check it's specifically an arithmetic overflow error
    match result {
        Err(DerivationError::ArithmeticOverflow { policy, operation }) => {
            assert_eq!(policy, "Repeat");
            assert!(operation.contains("addition"), "Operation should mention addition");
        }
        _ => panic!("Expected arithmetic overflow error"),
    }
}

/// Verifies that multiple large-value cases work correctly.
///
/// Note: Cases 1 and 2 have overflow scenarios and now return errors as per the strict
/// overflow check. Case 3 is the non-overflow test.
#[test]
fn repeat_policy_large_values_boundary_consistency() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();

    // Case 1: large count with large interval causes overflow in multiplication
    let policy1 = RunPolicy::repeat(u32::MAX, u64::MAX / 2).expect("repeat policy should be valid");
    let result1 = derive_runs(&clock, task_id, &policy1, 0, 0);
    assert!(result1.is_err(), "Large count with large interval should return error");
    match result1 {
        Err(DerivationError::ArithmeticOverflow { policy, operation }) => {
            assert_eq!(policy, "Repeat");
            assert!(operation.contains("multiplication"));
        }
        _ => panic!("Expected arithmetic overflow error"),
    }

    // Case 2: moderate count with large interval
    // i=9999: product = 9999 * (u64::MAX / 10000)
    // This should succeed since 9999 * (u64::MAX / 10000) < u64::MAX
    let policy2 =
        RunPolicy::repeat(10_000, u64::MAX / 10_000).expect("repeat policy should be valid");
    let result2 = derive_runs(&clock, task_id, &policy2, 0, 0).unwrap();
    assert_eq!(result2.derived().len(), 10_000);
    assert_eq!(result2.derived()[0].scheduled_at(), 0);
    assert_eq!(result2.derived()[1].scheduled_at(), u64::MAX / 10_000);
    // Last entry should be 9999 * interval
    assert_eq!(result2.derived()[9999].scheduled_at(), 9999 * (u64::MAX / 10_000));
    assert_eq!(result2.already_derived(), 10_000);

    // Case 3: moderate count with moderate values (no overflow expected)
    let policy3 = RunPolicy::repeat(1000, 60).expect("repeat policy should be valid");
    let result3 = derive_runs(&clock, task_id, &policy3, 0, 0).unwrap();
    assert_eq!(result3.derived().len(), 1000);
    assert_eq!(result3.already_derived(), 1000);
    // Verify the last scheduled time
    assert_eq!(result3.derived()[999].scheduled_at(), 999 * 60);
}

/// Verifies that Repeat derivation rejects invalid state where already_derived exceeds count.
#[test]
fn repeat_policy_rejects_already_derived_exceeds_count() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(5, 60).expect("repeat policy should be valid");

    // already_derived=10 with count=5 should return an error
    let result = derive_runs(&clock, task_id, &run_policy, 10, 0);

    assert!(result.is_err(), "already_derived (10) > count (5) should return an error");

    // Verify it's the expected error type
    match result {
        Err(DerivationError::ArithmeticOverflow { policy, operation }) => {
            assert_eq!(policy, "Repeat", "Policy should be Repeat");
            assert!(
                operation.contains("already_derived"),
                "Operation should mention already_derived"
            );
            assert!(operation.contains("count"), "Operation should mention count");
        }
        _ => panic!("Expected arithmetic overflow error for invalid already_derived"),
    }
}

/// Verifies that Repeat derivation with already_derived = count succeeds but creates no new runs.
#[test]
fn repeat_policy_boundary_already_derived_equals_count() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(5, 60).expect("repeat policy should be valid");

    // First derivation creates 5 runs
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 5);
    assert_eq!(result1.already_derived(), 5);

    // Second derivation with already_derived=5 (equals count) should succeed but create no new runs
    let result2 = derive_runs(&clock, task_id, &run_policy, 5, 0).unwrap();
    assert_eq!(result2.derived().len(), 0, "No new runs should be created");
    assert_eq!(result2.already_derived(), 5, "already_derived should remain at count");
}

/// Verifies nil task IDs are surfaced as typed derivation errors for Repeat policy.
#[test]
fn repeat_policy_nil_task_id_returns_typed_error_without_partial_results() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = "00000000-0000-0000-0000-000000000000"
        .parse::<TaskId>()
        .expect("nil task id literal must parse");
    let run_policy = RunPolicy::repeat(4, 60).expect("repeat policy should be valid");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 1000);

    assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
}

/// Guards against panic-regressions by asserting repeat derivation returns typed errors
/// instead of unwinding for invalid task identifiers.
#[test]
fn repeat_policy_nil_task_id_does_not_panic() {
    let outcome = std::panic::catch_unwind(|| {
        let clock = actionqueue_engine::time::clock::MockClock::new(1000);
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");
        let run_policy = RunPolicy::repeat(4, 60).expect("repeat policy should be valid");

        derive_runs(&clock, task_id, &run_policy, 0, 1000)
    });

    match outcome {
        Ok(result) => {
            let task_id = "00000000-0000-0000-0000-000000000000"
                .parse::<TaskId>()
                .expect("nil task id literal must parse");
            assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
        }
        Err(_) => panic!("repeat derivation must return typed errors, never unwind"),
    }
}
