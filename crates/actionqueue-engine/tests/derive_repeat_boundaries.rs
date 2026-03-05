//! Boundary tests for Repeat derivation policy edge cases.
//!
//! T-6: These tests exercise degenerate and extreme Repeat policy parameters
//! to verify that derivation handles boundary conditions correctly:
//!
//! 1. `Repeat(1, 60)` — degenerate single-repeat produces exactly 1 run
//! 2. `Repeat(2, u64::MAX)` — second run's scheduled_at would overflow
//! 3. `Repeat(0, _)` — rejected at both validation and derivation time

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::run_policy::{RunPolicy, RunPolicyError};
use actionqueue_engine::derive::{derive_runs, DerivationError};

// ---------------------------------------------------------------------------
// 1. Degenerate single-repeat: Repeat(1, 60) must derive exactly 1 run
// ---------------------------------------------------------------------------

/// Repeat(1, interval) is the degenerate case where the repeat policy collapses
/// to the same cardinality as Once. Derivation must produce exactly 1 run at
/// the schedule origin (index 0, so scheduled_at = origin + 0 * interval = origin).
#[test]
fn repeat_1_derives_exactly_one_run() {
    let clock = actionqueue_engine::time::clock::MockClock::new(5000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(1, 60).expect("Repeat(1, 60) is a valid policy");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 1000).unwrap();

    assert_eq!(result.derived().len(), 1, "Repeat(1, _) must derive exactly 1 run");
    assert_eq!(result.already_derived(), 1, "already_derived must equal the policy count");
    assert_eq!(
        result.derived()[0].scheduled_at(),
        1000,
        "Single run must be scheduled at the origin"
    );
    assert_eq!(result.derived()[0].task_id(), task_id);
    assert_eq!(result.derived()[0].state(), RunState::Scheduled);
}

/// Re-deriving after the single run already exists must produce zero new runs.
#[test]
fn repeat_1_does_not_create_duplicates() {
    let clock = actionqueue_engine::time::clock::MockClock::new(5000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(1, 60).expect("Repeat(1, 60) is a valid policy");

    let first = derive_runs(&clock, task_id, &run_policy, 0, 1000).unwrap();
    assert_eq!(first.derived().len(), 1);

    let second = derive_runs(&clock, task_id, &run_policy, 1, 1000).unwrap();
    assert_eq!(second.derived().len(), 0, "No new runs when already_derived == count");
    assert_eq!(second.already_derived(), 1);
}

/// Repeat(1, _) with different interval values must always yield the same
/// single-run result because only index 0 is ever computed (product = 0).
#[test]
fn repeat_1_interval_value_is_irrelevant() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();

    // Small interval
    let small = RunPolicy::repeat(1, 1).expect("valid");
    let r_small = derive_runs(&clock, task_id, &small, 0, 500).unwrap();
    assert_eq!(r_small.derived().len(), 1);
    assert_eq!(r_small.derived()[0].scheduled_at(), 500);

    // Large interval — still only index 0 so no overflow risk
    let large = RunPolicy::repeat(1, u64::MAX).expect("valid");
    let r_large = derive_runs(&clock, task_id, &large, 0, 500).unwrap();
    assert_eq!(r_large.derived().len(), 1);
    assert_eq!(r_large.derived()[0].scheduled_at(), 500);
}

// ---------------------------------------------------------------------------
// 2. Repeat(2, u64::MAX) — overflow on the second run's scheduled_at
// ---------------------------------------------------------------------------

/// With count=2 and interval=u64::MAX, the second run would require
/// scheduled_at = origin + 1 * u64::MAX, which overflows for any origin > 0.
/// Derivation must return a typed ArithmeticOverflow error.
#[test]
fn repeat_2_max_interval_overflows_with_nonzero_origin() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(2, u64::MAX).expect("Repeat(2, u64::MAX) is a valid policy");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 1);

    assert!(result.is_err(), "origin=1 + 1*u64::MAX must overflow");
    match result {
        Err(DerivationError::ArithmeticOverflow { ref policy, .. }) => {
            assert_eq!(policy, "Repeat");
        }
        other => panic!("Expected ArithmeticOverflow, got: {other:?}"),
    }
}

/// With origin=0, Repeat(2, u64::MAX) means scheduled_at for index 1 is
/// 0 + 1 * u64::MAX = u64::MAX, which is representable. This must succeed.
#[test]
fn repeat_2_max_interval_succeeds_with_zero_origin() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(2, u64::MAX).expect("Repeat(2, u64::MAX) is a valid policy");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 0).unwrap();

    assert_eq!(result.derived().len(), 2);
    assert_eq!(result.derived()[0].scheduled_at(), 0, "index 0: origin + 0 * interval = 0");
    assert_eq!(
        result.derived()[1].scheduled_at(),
        u64::MAX,
        "index 1: origin + 1 * u64::MAX = u64::MAX"
    );
}

/// Repeat(3, u64::MAX) must overflow because index 2 requires
/// 2 * u64::MAX which exceeds u64 in the multiplication step.
#[test]
fn repeat_3_max_interval_overflows_on_multiplication() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(3, u64::MAX).expect("Repeat(3, u64::MAX) is a valid policy");

    let result = derive_runs(&clock, task_id, &run_policy, 0, 0);

    assert!(result.is_err(), "index 2: 2 * u64::MAX must overflow multiplication");
    match result {
        Err(DerivationError::ArithmeticOverflow { ref policy, ref operation }) => {
            assert_eq!(policy, "Repeat");
            assert!(
                operation.contains("multiplication"),
                "Error should identify the multiplication overflow, got: {operation}"
            );
        }
        other => panic!("Expected ArithmeticOverflow with multiplication, got: {other:?}"),
    }
}

/// Partially-derived Repeat(2, u64::MAX) with already_derived=1 must still
/// succeed because only index 1 needs to be computed (which is representable
/// at origin 0).
#[test]
fn repeat_2_max_interval_partial_derivation_succeeds() {
    let clock = actionqueue_engine::time::clock::MockClock::new(1000);
    let task_id = TaskId::new();
    let run_policy = RunPolicy::repeat(2, u64::MAX).expect("Repeat(2, u64::MAX) is a valid policy");

    let result = derive_runs(&clock, task_id, &run_policy, 1, 0).unwrap();

    assert_eq!(result.derived().len(), 1, "Only index 1 should be newly derived");
    assert_eq!(result.derived()[0].scheduled_at(), u64::MAX);
    assert_eq!(result.already_derived(), 2);
}

// ---------------------------------------------------------------------------
// 3. Repeat(0, _) — rejected at validation time
// ---------------------------------------------------------------------------

/// The RunPolicy::repeat() constructor must reject count=0.
#[test]
fn repeat_0_rejected_by_constructor() {
    let result = RunPolicy::repeat(0, 60);
    assert_eq!(
        result,
        Err(RunPolicyError::InvalidRepeatCount { count: 0 }),
        "RunPolicy::repeat(0, _) must fail with InvalidRepeatCount"
    );
}

/// RepeatPolicy::new(0, _) must be rejected at construction time.
#[test]
fn repeat_0_rejected_by_newtype_constructor() {
    use actionqueue_core::task::run_policy::RepeatPolicy;
    let result = RepeatPolicy::new(0, 60);
    assert_eq!(
        result,
        Err(RunPolicyError::InvalidRepeatCount { count: 0 }),
        "RepeatPolicy::new must reject count=0"
    );
}

/// Repeat(0, 0) must be rejected (count=0 takes precedence in error reporting).
#[test]
fn repeat_0_count_0_interval_rejected() {
    let result = RunPolicy::repeat(0, 0);
    assert_eq!(
        result,
        Err(RunPolicyError::InvalidRepeatCount { count: 0 }),
        "count=0 should be checked before interval_secs=0"
    );
}

/// Verify that Repeat(0, _) with various interval values is always rejected.
#[test]
fn repeat_0_rejected_regardless_of_interval() {
    for interval in [1, 60, 3600, u64::MAX] {
        let result = RunPolicy::repeat(0, interval);
        assert_eq!(
            result,
            Err(RunPolicyError::InvalidRepeatCount { count: 0 }),
            "Repeat(0, {interval}) must be rejected"
        );
    }
}
