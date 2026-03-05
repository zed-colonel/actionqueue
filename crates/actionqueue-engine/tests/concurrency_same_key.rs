//! Same-key non-overlap tests.
//!
//! These tests verify that the concurrency key gate enforces single-flight
//! execution for runs with the same concurrency key:
//! - A key can be acquired when it is free
//! - A second run attempting to acquire the same key is blocked
//! - The blocking run receives clear information about which run holds the key

use actionqueue_core::ids::RunId;
use actionqueue_core::run::RunInstance;
use actionqueue_core::run::RunState;
use actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy;
use actionqueue_engine::concurrency::key_gate::{
    AcquireResult, ConcurrencyKey, KeyGate, ReleaseResult,
};
use actionqueue_engine::concurrency::lifecycle::{
    evaluate_state_transition, KeyLifecycleContext, LifecycleResult,
};

/// Verifies that a key can be acquired when it is free.
#[test]
fn key_can_be_acquired_when_free() {
    let mut key_gate = KeyGate::new();
    let key = ConcurrencyKey::new("my-key");
    let run_id = RunId::new();

    let result = key_gate.acquire(key.clone(), run_id);

    match result {
        AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
            assert_eq!(acquired_key, key);
            assert_eq!(acquired_run_id, run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected key to be acquired"),
    }
}

/// Verifies that a second run cannot acquire the same key while it is held.
#[test]
fn second_run_cannot_acquire_same_key_while_first_holds_it() {
    let mut key_gate = KeyGate::new();
    let key = ConcurrencyKey::new("my-key");
    let first_run_id = RunId::new();
    let second_run_id = RunId::new();

    // First run acquires the key
    let acquire_first = key_gate.acquire(key.clone(), first_run_id);
    match acquire_first {
        AcquireResult::Acquired { key: _, run_id } => {
            assert_eq!(run_id, first_run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    // Second run attempts to acquire the same key
    let acquire_second = key_gate.acquire(key, second_run_id);

    match acquire_second {
        AcquireResult::Occupied { key: occupied_key, holder_run_id } => {
            assert_eq!(occupied_key.as_str(), "my-key");
            assert_eq!(holder_run_id, first_run_id);
        }
        AcquireResult::Acquired { .. } => panic!("Expected key to be occupied"),
    }
}

/// Verifies that the same run can acquire the same key multiple times (idempotent).
#[test]
fn same_run_can_acquire_same_key_multiple_times() {
    let mut key_gate = KeyGate::new();
    let key = ConcurrencyKey::new("my-key");
    let run_id = RunId::new();

    // First acquisition
    let result_first = key_gate.acquire(key.clone(), run_id);
    match result_first {
        AcquireResult::Acquired { key: _, run_id: acquired_run_id } => {
            assert_eq!(acquired_run_id, run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    // Same run acquires again (should still succeed - idempotent)
    let result_second = key_gate.acquire(key, run_id);
    match result_second {
        AcquireResult::Acquired { key: _, run_id: acquired_run_id } => {
            assert_eq!(acquired_run_id, run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected repeated acquisition to succeed"),
    }
}

/// Verifies that release clears the key for other runs to acquire.
#[test]
fn release_allows_other_runs_to_acquire_key() {
    let mut key_gate = KeyGate::new();
    let key = ConcurrencyKey::new("my-key");
    let first_run_id = RunId::new();
    let second_run_id = RunId::new();

    // First run acquires the key
    let acquire_first = key_gate.acquire(key.clone(), first_run_id);
    match acquire_first {
        AcquireResult::Acquired { key: _, run_id } => {
            assert_eq!(run_id, first_run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    // First run releases the key
    let release_result = key_gate.release(key.clone(), first_run_id);
    match release_result {
        ReleaseResult::Released { key: released_key } => {
            assert_eq!(released_key, key);
        }
        ReleaseResult::NotHeld { .. } => {
            panic!("Expected release to succeed");
        }
    }

    // Second run can now acquire the key
    let acquire_second = key_gate.acquire(key, second_run_id);
    match acquire_second {
        AcquireResult::Acquired { key: _, run_id } => {
            assert_eq!(run_id, second_run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected key to be free after release"),
    }
}

/// Verifies that release from a different run does not clear the key.
#[test]
fn release_from_different_run_does_not_clear_key() {
    let mut key_gate = KeyGate::new();
    let key = ConcurrencyKey::new("my-key");
    let first_run_id = RunId::new();
    let second_run_id = RunId::new();

    // First run acquires the key
    let acquire_first = key_gate.acquire(key.clone(), first_run_id);
    match acquire_first {
        AcquireResult::Acquired { key: _, run_id } => {
            assert_eq!(run_id, first_run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    // Second run attempts to release (should fail)
    let release_result = key_gate.release(key, second_run_id);
    match release_result {
        ReleaseResult::Released { .. } => {
            panic!("Expected release from different run to fail");
        }
        ReleaseResult::NotHeld { key: _, attempting_run_id } => {
            assert_eq!(attempting_run_id, second_run_id);
        }
    }

    // First run should still hold the key
    let acquire_duplicate = key_gate.acquire(ConcurrencyKey::new("my-key"), first_run_id);
    match acquire_duplicate {
        AcquireResult::Acquired { .. } => {}
        AcquireResult::Occupied { .. } => panic!("Expected same run to still hold key"),
    }
}

/// Verifies that same-key exclusion contract is enforced in a deterministic scheduling scenario.
///
/// This test simulates a realistic scheduling scenario where multiple runs with the
/// same concurrency key are ready to execute, but only one can run at a time.
/// The key is acquired when a run transitions to Running state.
#[test]
fn same_key_exclusion_contract_test() {
    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    // Create two runs with the same concurrency key
    let run1 = RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 = RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    let concurrency_key = Some("shared-key".to_string());

    // First run transitions to Running - should acquire the key
    let result1 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key.clone(),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result1, LifecycleResult::Acquired { .. }),
        "First run should acquire key when entering Running"
    );
    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new("shared-key")));

    // Second run transitions to Running - should fail to acquire the key
    let result2 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key.clone(),
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result2, LifecycleResult::KeyOccupied { .. }),
        "Second run should observe key occupied when entering Running"
    );

    // Key should still be held by first run
    assert_eq!(key_gate.key_holder(&ConcurrencyKey::new("shared-key")), Some(run1.id()));
}

/// Verifies that key release enables re-acquisition by another run.
///
/// This demonstrates the contract that when a run completes (transitions from Running to
/// a terminal state), it releases its concurrency key, allowing other runs with the same
/// key to execute.
#[test]
fn key_release_enables_reacquisition_contract_test() {
    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    let run1 = RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 = RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    let concurrency_key = Some("reacquisition-key".to_string());

    // First run acquires key
    let _ = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key.clone(),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new("reacquisition-key")));

    // First run completes (transitions from Running to terminal)
    let _ = evaluate_state_transition(
        RunState::Running,
        RunState::Completed,
        KeyLifecycleContext::new(
            concurrency_key.clone(),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    // Key should now be free
    assert!(!key_gate.is_key_occupied(&ConcurrencyKey::new("reacquisition-key")));

    // Second run can now acquire the key
    let result = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key,
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result, LifecycleResult::Acquired { .. }),
        "Second run should acquire key after first run releases it"
    );
    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new("reacquisition-key")));
}

/// Verifies that runs without a concurrency key can execute concurrently.
///
/// This demonstrates that the concurrency key is optional, and runs without a key
/// do not block each other from executing.
#[test]
fn no_key_runs_can_execute_concurrently() {
    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    let run1 = RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 = RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    // No concurrency key defined
    let concurrency_key = None;

    // First run transitions to Running - no key to acquire
    let result1 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key.clone(),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(matches!(result1, LifecycleResult::NoAction { .. }), "No action when no key defined");

    // Second run transitions to Running - also no key
    let result2 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            concurrency_key,
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(matches!(result2, LifecycleResult::NoAction { .. }), "No action when no key defined");

    // Key gate should be empty - no keys tracked
    assert!(!key_gate.is_key_occupied(&ConcurrencyKey::new("nonexistent")));
}

/// Integration test simulating a complete scheduling cycle with same-key exclusion.
///
/// This test demonstrates the complete life cycle of runs with a concurrency key:
/// 1. Multiple runs are ready to execute
/// 2. First run acquires the key when entering Running
/// 3. Second run is blocked from acquiring the same key
/// 4. First run releases the key on completion
/// 5. Second run can now acquire the key
#[test]
fn same_key_scheduling_cycle() {
    use actionqueue_core::run::RunInstance;

    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    // Create three runs with the same concurrency key
    let run1 = RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 = RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");
    let run3 = RunInstance::new_scheduled(task_id, 1000, 502).expect("valid run");

    let concurrency_key = "cycle-key";

    // Phase 1: run1 enters Running - acquires key
    let phase1 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(concurrency_key.to_string()),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase1, LifecycleResult::Acquired { .. }));

    // run2 and run3 cannot acquire the key
    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new(concurrency_key)));
    assert_eq!(key_gate.key_holder(&ConcurrencyKey::new(concurrency_key)), Some(run1.id()));

    // Phase 2: run1 completes, releases key
    let phase2 = evaluate_state_transition(
        RunState::Running,
        RunState::Completed,
        KeyLifecycleContext::new(
            Some(concurrency_key.to_string()),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase2, LifecycleResult::Released { .. }));

    // Key is now free
    assert!(!key_gate.is_key_occupied(&ConcurrencyKey::new(concurrency_key)));

    // Phase 3: run2 enters Running - acquires key
    let phase3 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(concurrency_key.to_string()),
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase3, LifecycleResult::Acquired { .. }));
    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new(concurrency_key)));
    assert_eq!(key_gate.key_holder(&ConcurrencyKey::new(concurrency_key)), Some(run2.id()));

    // Phase 4: run2 completes, releases key
    let phase4 = evaluate_state_transition(
        RunState::Running,
        RunState::Completed,
        KeyLifecycleContext::new(
            Some(concurrency_key.to_string()),
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase4, LifecycleResult::Released { .. }));
    assert!(!key_gate.is_key_occupied(&ConcurrencyKey::new(concurrency_key)));

    // Phase 5: run3 enters Running - acquires key
    let phase5 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(concurrency_key.to_string()),
            run3.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase5, LifecycleResult::Acquired { .. }));
    assert!(key_gate.is_key_occupied(&ConcurrencyKey::new(concurrency_key)));
    assert_eq!(key_gate.key_holder(&ConcurrencyKey::new(concurrency_key)), Some(run3.id()));
}
