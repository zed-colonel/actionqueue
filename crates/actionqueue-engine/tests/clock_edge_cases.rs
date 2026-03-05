//! Clock edge case tests (T-5).
//!
//! These tests verify correct behavior at temporal boundary conditions:
//!
//! 1. **Lease expiry at exact boundary:** `current_time == expiry_at` must be expired.
//! 2. **Far-future scheduling:** A run with `scheduled_at` far in the future stays Scheduled.
//! 3. **MockClock at u64::MAX - 1:** Scheduling arithmetic near the u64 ceiling uses
//!    checked/saturating operations and returns typed errors instead of panicking.

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_engine::derive::derive_runs;
use actionqueue_engine::lease::expiry::{evaluate, evaluate_expires_at, ExpiryResult};
use actionqueue_engine::lease::model::{Lease, LeaseExpiry, LeaseOwner};
use actionqueue_engine::scheduler::promotion::promote_scheduled_to_ready;
use actionqueue_engine::time::clock::{Clock, MockClock};

// ---------------------------------------------------------------------------
// 1. Lease expiry at exact boundary
// ---------------------------------------------------------------------------

/// When `current_time` equals `expiry_at` exactly, the lease must be considered
/// expired and the run must be eligible for re-entry to the Ready state.
#[test]
fn lease_expired_at_exact_boundary() {
    let expiry_time = 5000_u64;
    let expiry = LeaseExpiry::at(expiry_time);

    let result = evaluate_expires_at(expiry_time, expiry);

    assert_eq!(
        result,
        ExpiryResult::Expired,
        "lease must be expired when current_time == expiry_at"
    );
    assert!(result.is_expired(), "is_expired() must return true at exact boundary");
    assert!(result.can_reenter_eligibility(), "expired lease must allow re-entry to eligibility");
}

/// Full Lease struct evaluation at exact expiry boundary produces the same
/// result as the raw timestamp evaluation.
#[test]
fn lease_struct_expired_at_exact_boundary() {
    let expiry_time = 5000_u64;
    let run_id = RunId::new();
    let owner = LeaseOwner::new("worker-edge");
    let lease = Lease::new(run_id, owner, LeaseExpiry::at(expiry_time));

    let result = evaluate(expiry_time, &lease);

    assert_eq!(
        result,
        ExpiryResult::Expired,
        "Lease struct evaluation must agree with raw evaluation at exact boundary"
    );
}

/// One tick before the boundary the lease must still be active.
#[test]
fn lease_active_one_tick_before_boundary() {
    let expiry_time = 5000_u64;
    let expiry = LeaseExpiry::at(expiry_time);

    let result = evaluate_expires_at(expiry_time - 1, expiry);

    assert_eq!(result, ExpiryResult::Active, "lease must be active one second before expiry_at");
    assert!(!result.is_expired(), "is_expired() must return false one tick before boundary");
}

/// One tick after the boundary the lease must be expired (not just at the boundary).
#[test]
fn lease_expired_one_tick_after_boundary() {
    let expiry_time = 5000_u64;
    let expiry = LeaseExpiry::at(expiry_time);

    let result = evaluate_expires_at(expiry_time + 1, expiry);

    assert_eq!(result, ExpiryResult::Expired, "lease must be expired one second after expiry_at");
}

/// Lease expiry at t=0 boundary: both current_time and expiry are zero.
#[test]
fn lease_expired_at_zero_boundary() {
    let result = evaluate_expires_at(0, LeaseExpiry::at(0));

    assert_eq!(
        result,
        ExpiryResult::Expired,
        "lease must be expired when both current_time and expiry_at are zero"
    );
}

/// Lease expiry at u64::MAX boundary: both current_time and expiry equal u64::MAX.
#[test]
fn lease_expired_at_u64_max_boundary() {
    let result = evaluate_expires_at(u64::MAX, LeaseExpiry::at(u64::MAX));

    assert_eq!(
        result,
        ExpiryResult::Expired,
        "lease must be expired when both current_time and expiry_at are u64::MAX"
    );
}

// ---------------------------------------------------------------------------
// 2. Far-future scheduling
// ---------------------------------------------------------------------------

/// A run scheduled 1 billion seconds into the future must remain in the
/// Scheduled state and must NOT be promoted to Ready.
#[test]
fn far_future_run_stays_scheduled() {
    let now = 1_000_000_u64;
    let far_future_offset = 1_000_000_000_u64;
    let task_id = TaskId::new();

    let scheduled_at = now + far_future_offset;

    let run =
        actionqueue_core::run::run_instance::RunInstance::new_scheduled(task_id, scheduled_at, now)
            .expect("valid scheduled run");

    assert_eq!(run.state(), RunState::Scheduled);
    assert_eq!(run.scheduled_at(), scheduled_at);

    // Build a ScheduledIndex and attempt promotion at current time
    let index = actionqueue_engine::index::scheduled::ScheduledIndex::from_runs(vec![run]);

    let result =
        promote_scheduled_to_ready(&index, now).expect("promotion evaluation must succeed");

    assert!(
        result.promoted().is_empty(),
        "far-future run must NOT be promoted when current_time is {now}"
    );
    assert_eq!(
        result.remaining_scheduled().len(),
        1,
        "far-future run must remain in remaining_scheduled"
    );
    assert_eq!(
        result.remaining_scheduled()[0].state(),
        RunState::Scheduled,
        "remaining run must still be in Scheduled state"
    );
    assert_eq!(
        result.remaining_scheduled()[0].scheduled_at(),
        scheduled_at,
        "scheduled_at must be preserved"
    );
}

/// Even after advancing the clock significantly (but not enough), the far-future
/// run must still not be promoted.
#[test]
fn far_future_run_stays_scheduled_after_partial_advance() {
    let now = 1_000_000_u64;
    let far_future_offset = 1_000_000_000_u64;
    let task_id = TaskId::new();
    let scheduled_at = now + far_future_offset;

    let run =
        actionqueue_core::run::run_instance::RunInstance::new_scheduled(task_id, scheduled_at, now)
            .expect("valid scheduled run");

    let index = actionqueue_engine::index::scheduled::ScheduledIndex::from_runs(vec![run]);

    // Advance halfway — still not enough
    let halfway = now + (far_future_offset / 2);
    let result =
        promote_scheduled_to_ready(&index, halfway).expect("promotion evaluation must succeed");

    assert!(result.promoted().is_empty(), "run must not be promoted at halfway point ({halfway})");
    assert_eq!(result.remaining_scheduled().len(), 1);
}

/// A far-future run IS promoted once the clock reaches the exact scheduled_at time.
#[test]
fn far_future_run_promoted_at_exact_scheduled_at() {
    let now = 1_000_000_u64;
    let far_future_offset = 1_000_000_000_u64;
    let task_id = TaskId::new();
    let scheduled_at = now + far_future_offset;

    let run =
        actionqueue_core::run::run_instance::RunInstance::new_scheduled(task_id, scheduled_at, now)
            .expect("valid scheduled run");

    let index = actionqueue_engine::index::scheduled::ScheduledIndex::from_runs(vec![run]);

    // Advance exactly to scheduled_at
    let result = promote_scheduled_to_ready(&index, scheduled_at)
        .expect("promotion evaluation must succeed");

    assert_eq!(
        result.promoted().len(),
        1,
        "run must be promoted when current_time reaches scheduled_at"
    );
    assert_eq!(result.promoted()[0].state(), RunState::Ready);
    assert!(result.remaining_scheduled().is_empty());
}

/// Derive a Once run at a far-future clock time; the run must be scheduled at that time.
#[test]
fn far_future_clock_derive_once_schedules_at_clock_time() {
    let far_future = 1_000_000_000_u64 + 1_000_000_u64;
    let clock = MockClock::new(far_future);
    let task_id = TaskId::new();

    let result =
        derive_runs(&clock, task_id, &RunPolicy::Once, 0, 0).expect("Once derivation must succeed");

    assert_eq!(result.derived().len(), 1);
    assert_eq!(
        result.derived()[0].scheduled_at(),
        far_future,
        "Once-derived run must be scheduled at the far-future clock time"
    );
}

// ---------------------------------------------------------------------------
// 3. MockClock at u64::MAX - 1 and near-overflow arithmetic
// ---------------------------------------------------------------------------

/// MockClock can be set to u64::MAX - 1 and reports the correct value.
#[test]
fn mock_clock_near_max_reports_correct_time() {
    let clock = MockClock::new(u64::MAX - 1);
    assert_eq!(clock.now(), u64::MAX - 1);
}

/// MockClock::advance_by(1) from u64::MAX - 1 reaches u64::MAX exactly.
#[test]
fn mock_clock_advance_to_exact_max() {
    let mut clock = MockClock::new(u64::MAX - 1);
    clock.advance_by(1);
    assert_eq!(clock.now(), u64::MAX, "advancing by 1 from MAX-1 must reach exactly u64::MAX");
}

/// MockClock::set(u64::MAX) must work without panic.
#[test]
fn mock_clock_set_to_max() {
    let mut clock = MockClock::new(0);
    clock.set(u64::MAX);
    assert_eq!(clock.now(), u64::MAX);
}

/// Derive a Once run with the clock at u64::MAX - 1.
/// The run should be scheduled at u64::MAX - 1 without overflow.
#[test]
fn derive_once_at_near_max_clock() {
    let clock = MockClock::new(u64::MAX - 1);
    let task_id = TaskId::new();

    let result = derive_runs(&clock, task_id, &RunPolicy::Once, 0, 0)
        .expect("Once derivation at near-max clock must succeed");

    assert_eq!(result.derived().len(), 1);
    assert_eq!(
        result.derived()[0].scheduled_at(),
        u64::MAX - 1,
        "run scheduled_at must equal clock time (u64::MAX - 1)"
    );
}

/// Repeat derivation with schedule_origin near u64::MAX must fail with a typed
/// ArithmeticOverflow error instead of panicking from wrapping addition.
#[test]
fn repeat_derivation_near_max_returns_arithmetic_overflow() {
    let clock = MockClock::new(u64::MAX - 1);
    let task_id = TaskId::new();
    let policy = RunPolicy::repeat(3, 60).expect("valid repeat policy");

    // schedule_origin is near MAX, so origin + (index * interval) overflows
    let result = derive_runs(&clock, task_id, &policy, 0, u64::MAX - 10);

    assert!(result.is_err(), "repeat derivation near u64::MAX must fail with overflow error");
    match result {
        Err(actionqueue_engine::derive::DerivationError::ArithmeticOverflow { .. }) => {
            // Expected: typed overflow error, no panic
        }
        other => panic!("expected ArithmeticOverflow, got: {other:?}"),
    }
}

/// Repeat derivation with an interval that would overflow the multiplication step
/// must also return a typed error.
#[test]
fn repeat_derivation_multiplication_overflow_returns_typed_error() {
    let clock = MockClock::new(1000);
    let task_id = TaskId::new();
    let policy = RunPolicy::repeat(u32::MAX, u64::MAX).expect("valid repeat policy");

    let result = derive_runs(&clock, task_id, &policy, 0, 0);

    assert!(result.is_err(), "repeat derivation with u32::MAX count * u64::MAX interval must fail");
    match result {
        Err(actionqueue_engine::derive::DerivationError::ArithmeticOverflow { .. }) => {
            // Expected: multiplication overflow detected
        }
        other => panic!("expected ArithmeticOverflow, got: {other:?}"),
    }
}

/// Promotion at u64::MAX clock time: a run scheduled at u64::MAX must be promoted
/// (scheduled_at <= current_time is satisfied).
#[test]
fn promotion_at_u64_max_clock_time() {
    let task_id = TaskId::new();

    let run = actionqueue_core::run::run_instance::RunInstance::new_scheduled(
        task_id,
        u64::MAX,
        u64::MAX,
    )
    .expect("valid scheduled run at u64::MAX");

    let index = actionqueue_engine::index::scheduled::ScheduledIndex::from_runs(vec![run]);

    let result =
        promote_scheduled_to_ready(&index, u64::MAX).expect("promotion at u64::MAX must succeed");

    assert_eq!(
        result.promoted().len(),
        1,
        "run scheduled at u64::MAX must be promoted when clock is u64::MAX"
    );
    assert_eq!(result.promoted()[0].state(), RunState::Ready);
}

/// A run scheduled at u64::MAX must NOT be promoted when the clock is u64::MAX - 1.
#[test]
fn run_at_max_not_promoted_when_clock_one_tick_before() {
    let task_id = TaskId::new();

    let run = actionqueue_core::run::run_instance::RunInstance::new_scheduled(
        task_id,
        u64::MAX,
        u64::MAX - 1,
    )
    .expect("valid scheduled run at u64::MAX");

    let index = actionqueue_engine::index::scheduled::ScheduledIndex::from_runs(vec![run]);

    let result = promote_scheduled_to_ready(&index, u64::MAX - 1).expect("promotion must succeed");

    assert!(
        result.promoted().is_empty(),
        "run at u64::MAX must NOT be promoted when clock is u64::MAX - 1"
    );
    assert_eq!(result.remaining_scheduled().len(), 1);
}

/// Lease expiry evaluation at u64::MAX - 1 with expiry at u64::MAX must be Active.
#[test]
fn lease_active_at_near_max_before_expiry() {
    let result = evaluate_expires_at(u64::MAX - 1, LeaseExpiry::at(u64::MAX));

    assert_eq!(
        result,
        ExpiryResult::Active,
        "lease must be active when current_time (MAX-1) < expiry_at (MAX)"
    );
}

/// Lease expiry evaluation at u64::MAX with expiry at u64::MAX must be Expired.
#[test]
fn lease_expired_at_max_boundary() {
    let result = evaluate_expires_at(u64::MAX, LeaseExpiry::at(u64::MAX));

    assert_eq!(
        result,
        ExpiryResult::Expired,
        "lease must be expired when current_time == expiry_at == u64::MAX"
    );
}
