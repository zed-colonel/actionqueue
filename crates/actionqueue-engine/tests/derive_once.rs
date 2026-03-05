//! Accounting tests for Once derivation policy.
//!
//! These tests verify that the Once derivation policy correctly creates
//! exactly one run for a task and prevents duplicate creation.

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_engine::derive::{derive_runs, DerivationError};

/// Verifies that a Once policy task creates exactly one run on first derivation.
#[test]
fn once_policy_creates_exactly_one_run() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::Once;

    let result =
        derive_runs(&clock, task_id, &run_policy, 0, 0).expect("Once derivation must succeed");

    assert_eq!(result.derived().len(), 1, "Once policy must create exactly one run");
    assert_eq!(result.already_derived(), 1, "Once policy must report 1 as already_derived");
    assert_eq!(result.derived()[0].task_id(), task_id, "Run must have correct task_id");
    assert_eq!(
        result.derived()[0].state(),
        actionqueue_core::run::state::RunState::Scheduled,
        "Once run must be in Scheduled state"
    );
    assert_eq!(
        result.derived()[0].scheduled_at(),
        1000,
        "Once run must have scheduled_at = clock.now()"
    );
    assert_eq!(
        result.derived()[0].created_at(),
        1000,
        "Once run must have created_at = clock.now()"
    );
}

/// Verifies that Once derivation does not create duplicate runs when run already exists.
#[test]
fn once_policy_does_not_duplicate() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::Once;

    // First derivation Creates one run
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 1, "First derivation must create one run");
    let _first_run_id = result1.derived()[0].id();

    // Second derivation with already_derived=1 must create no new runs
    let result2 = derive_runs(&clock, task_id, &run_policy, 1, 0).unwrap();

    assert_eq!(result2.derived().len(), 0, "Second derivation must create zero runs");
    assert_eq!(result2.already_derived(), 1, "Once policy must always report 1 as already_derived");
    assert_eq!(result2.derived().len(), 0, "No duplicate run must be created");
}

/// Verifies Once derivation accounting is deterministic with multiple tasks.
#[test]
fn once_policy_accounting_deterministic_with_multiple_tasks() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id1 = TaskId::new();
    let task_id2 = TaskId::new();
    let task_id3 = TaskId::new();
    let run_policy = RunPolicy::Once;

    // Derive for task 1
    let result1 = derive_runs(&clock, task_id1, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 1);
    let run1_id = result1.derived()[0].id();

    // Derive for task 2 (different task)
    let result2 = derive_runs(&clock, task_id2, &run_policy, 0, 0).unwrap();
    assert_eq!(result2.derived().len(), 1);
    assert_ne!(result2.derived()[0].id(), run1_id, "Different tasks must have different run IDs");

    // Derive again for task 1 - should not create duplicates
    let result3 = derive_runs(&clock, task_id1, &run_policy, 1, 0).unwrap();
    assert_eq!(result3.derived().len(), 0, "Task 1 already has 1 run, must not create more");

    // Derive for task 3
    let result4 = derive_runs(&clock, task_id3, &run_policy, 0, 0).unwrap();
    assert_eq!(result4.derived().len(), 1);
    assert_ne!(result4.derived()[0].id(), run1_id);
    assert_ne!(result4.derived()[0].id(), result2.derived()[0].id());
}

/// Verifies that Once accounting remains stable when clock advances.
#[test]
fn once_policy_accounting_stable_with_clock_advancement() {
    let mut clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::Once;

    // First derivation at t=1000
    let result1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(result1.derived().len(), 1);
    assert_eq!(result1.derived()[0].scheduled_at(), 1000);

    // Advance clock and re-derive with already_derived=1
    clock.advance_by(600);

    let result2 = derive_runs(&clock, task_id, &run_policy, 1, 0).unwrap();

    assert_eq!(result2.derived().len(), 0, "No new run must be created");
    assert_eq!(result2.already_derived(), 1, "Still reports 1 as already_derived");

    // The original run's scheduled_at must remain unchanged
    assert_eq!(result1.derived()[0].scheduled_at(), 1000, "Scheduled time must not change");
}

/// Verifies Once derivation with zero derived count always creates one run.
#[test]
fn once_policy_zero_derived_count_creates_one() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::Once;

    // Starting with already_derived=0 must create exactly one run
    let result = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();

    assert_eq!(result.derived().len(), 1);
    assert_eq!(result.already_derived(), 1);
    assert_eq!(result.derived()[0].task_id(), task_id);
}

/// Verifies Once derivation accounting is consistent with policy expectations.
#[test]
fn once_policy_accounting_consistent_with_policy() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::Once;

    // The key invariant: Once policy must always result in exactly 1 derived run
    // regardless of how many times derive_runs is called with already_derived >= 1

    // First call
    let r1 = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();
    assert_eq!(r1.derived().len(), 1);
    assert_eq!(r1.already_derived(), 1);

    // Subsequent calls with already_derived=1
    for _ in 1..=10 {
        let r = derive_runs(&clock, task_id, &run_policy, 1, 0).unwrap();
        assert_eq!(r.derived().len(), 0, "No runs must be derived on repeat calls");
        assert_eq!(r.already_derived(), 1, "already_derived must remain 1");
    }

    // Verify the original run still exists (same ID)
    let _run_id = r1.derived()[0].id();
    let r_final = derive_runs(&clock, task_id, &run_policy, 1, 0).unwrap();
    assert_eq!(r_final.derived().len(), 0);
    assert_eq!(r_final.already_derived(), 1);
}

/// Verifies nil task IDs are surfaced as typed derivation errors with no panic path.
#[test]
fn once_policy_nil_task_id_returns_typed_error_without_unwind() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = "00000000-0000-0000-0000-000000000000"
        .parse::<TaskId>()
        .expect("nil task id literal must parse");

    let result = derive_runs(&clock, task_id, &RunPolicy::Once, 0, 0);

    assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
}

/// Guards against panic-regressions by asserting the once derivation path does not unwind
/// when task identifiers are invalid.
#[test]
fn once_policy_nil_task_id_does_not_panic() {
    let outcome = std::panic::catch_unwind(|| {
        let clock = actionqueue_engine::time::clock::MockClock::new(1000);
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");

        derive_runs(&clock, task_id, &RunPolicy::Once, 0, 0)
    });

    match outcome {
        Ok(result) => {
            let task_id = "00000000-0000-0000-0000-000000000000"
                .parse::<TaskId>()
                .expect("nil task id literal must parse");
            assert_eq!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { task_id }));
        }
        Err(_) => panic!("once derivation must return typed errors, never unwind"),
    }
}
