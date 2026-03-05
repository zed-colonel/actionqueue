//! Different-key overlap tests.
//!
//! These tests verify that distinct concurrency keys do not interfere with one
//! another and can overlap as intended:
//! - Different keys can be acquired simultaneously by different runs
//! - A run can hold multiple different keys
//! - Releasing one key doesn't affect another key

use actionqueue_core::ids::RunId;
use actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy;
use actionqueue_engine::concurrency::key_gate::{AcquireResult, ConcurrencyKey, KeyGate};

/// Verifies that different keys can be acquired simultaneously by different runs.
#[test]
fn different_keys_can_be_acquired_simultaneously() {
    let mut key_gate = KeyGate::new();
    let key1 = ConcurrencyKey::new("key-1");
    let key2 = ConcurrencyKey::new("key-2");
    let run1_id = RunId::new();
    let run2_id = RunId::new();

    // First run acquires key-1
    let result1 = key_gate.acquire(key1.clone(), run1_id);
    match result1 {
        AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
            assert_eq!(acquired_key, key1);
            assert_eq!(acquired_run_id, run1_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    // Second run acquires key-2 (different key)
    let result2 = key_gate.acquire(key2.clone(), run2_id);
    match result2 {
        AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
            assert_eq!(acquired_key, key2);
            assert_eq!(acquired_run_id, run2_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected second acquisition to succeed"),
    }
}

/// Verifies that a run can acquire multiple different keys.
#[test]
fn same_run_can_acquire_multiple_different_keys() {
    let mut key_gate = KeyGate::new();
    let key1 = ConcurrencyKey::new("key-1");
    let key2 = ConcurrencyKey::new("key-2");
    let key3 = ConcurrencyKey::new("key-3");
    let run_id = RunId::new();

    // Same run acquires three different keys
    let result1 = key_gate.acquire(key1.clone(), run_id);
    match result1 {
        AcquireResult::Acquired { key: acquired_key, .. } => {
            assert_eq!(acquired_key, key1);
        }
        AcquireResult::Occupied { .. } => panic!("Expected first acquisition to succeed"),
    }

    let result2 = key_gate.acquire(key2.clone(), run_id);
    match result2 {
        AcquireResult::Acquired { key: acquired_key, .. } => {
            assert_eq!(acquired_key, key2);
        }
        AcquireResult::Occupied { .. } => panic!("Expected second acquisition to succeed"),
    }

    let result3 = key_gate.acquire(key3.clone(), run_id);
    match result3 {
        AcquireResult::Acquired { key: acquired_key, .. } => {
            assert_eq!(acquired_key, key3);
        }
        AcquireResult::Occupied { .. } => panic!("Expected third acquisition to succeed"),
    }
}

/// Verifies that releasing one key doesn't affect another key held by the same run.
#[test]
fn releasing_one_key_does_not_affect_other_key_held_by_same_run() {
    let mut key_gate = KeyGate::new();
    let key1 = ConcurrencyKey::new("key-1");
    let key2 = ConcurrencyKey::new("key-2");
    let run_id = RunId::new();

    // Same run acquires both keys
    let _ = key_gate.acquire(key1.clone(), run_id);
    let _ = key_gate.acquire(key2.clone(), run_id);

    // Release key-1
    let release_result1 = key_gate.release(key1.clone(), run_id);
    match release_result1 {
        actionqueue_engine::concurrency::key_gate::ReleaseResult::Released {
            key: released_key,
        } => {
            assert_eq!(released_key, key1);
        }
        actionqueue_engine::concurrency::key_gate::ReleaseResult::NotHeld { .. } => {
            panic!("Expected release to succeed");
        }
    }

    // key-1 should be free
    assert!(!key_gate.is_key_occupied(&key1));
    assert_eq!(key_gate.key_holder(&key1), None);

    // key-2 should still be held by the same run
    assert!(key_gate.is_key_occupied(&key2));
    assert_eq!(key_gate.key_holder(&key2), Some(run_id));

    // A different run should now be able to acquire key-1
    let different_run_id = RunId::new();
    let result = key_gate.acquire(key1, different_run_id);
    match result {
        AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
            assert_eq!(acquired_key, ConcurrencyKey::new("key-1"));
            assert_eq!(acquired_run_id, different_run_id);
        }
        AcquireResult::Occupied { .. } => panic!("Expected key-1 to be free after release"),
    }

    // key-2 should still be held by the original run
    assert!(key_gate.is_key_occupied(&key2));
    assert_eq!(key_gate.key_holder(&key2), Some(run_id));
}

/// Verifies that releasing one key doesn't affect key held by a different run.
#[test]
fn releasing_one_key_does_not_affect_key_held_by_different_run() {
    let mut key_gate = KeyGate::new();
    let key1 = ConcurrencyKey::new("key-1");
    let key2 = ConcurrencyKey::new("key-2");
    let run1_id = RunId::new();
    let run2_id = RunId::new();

    // Different runs acquire different keys
    let _ = key_gate.acquire(key1.clone(), run1_id);
    let _ = key_gate.acquire(key2.clone(), run2_id);

    // run1 releases key-1 (clone key1 before moving it)
    let key1_clone = key1.clone();
    let release_result1 = key_gate.release(key1_clone, run1_id);
    match release_result1 {
        actionqueue_engine::concurrency::key_gate::ReleaseResult::Released { .. } => {}
        actionqueue_engine::concurrency::key_gate::ReleaseResult::NotHeld { .. } => {
            panic!("Expected release to succeed");
        }
    }

    // key-1 should be free
    assert!(!key_gate.is_key_occupied(&key1));

    // key-2 should still be held by run2
    assert!(key_gate.is_key_occupied(&key2));
    assert_eq!(key_gate.key_holder(&key2), Some(run2_id));
}

/// Verifies that acquiring a new key doesn't affect keys already held by other runs.
#[test]
fn acquiring_new_key_does_not_affect_existing_key_held_by_different_run() {
    let mut key_gate = KeyGate::new();
    let key1 = ConcurrencyKey::new("key-1");
    let key2 = ConcurrencyKey::new("key-2");
    let key3 = ConcurrencyKey::new("key-3");
    let run1_id = RunId::new();
    let run2_id = RunId::new();

    // run1 acquires key-1
    let _ = key_gate.acquire(key1.clone(), run1_id);

    // run2 acquires key-2 (different key)
    let _ = key_gate.acquire(key2.clone(), run2_id);

    // run1 acquires key-3 (another different key)
    let _ = key_gate.acquire(key3.clone(), run1_id);

    // All keys should be occupied
    assert!(key_gate.is_key_occupied(&key1));
    assert!(key_gate.is_key_occupied(&key2));
    assert!(key_gate.is_key_occupied(&key3));

    // Verify holders
    assert_eq!(key_gate.key_holder(&key1), Some(run1_id));
    assert_eq!(key_gate.key_holder(&key2), Some(run2_id));
    assert_eq!(key_gate.key_holder(&key3), Some(run1_id));
}

/// Verifies that runs with different concurrency keys can execute in parallel.
///
/// This test demonstrates the parallelism contract: runs with different concurrency
/// keys can execute simultaneously because their keys do not interfere with each other.
#[test]
fn different_keys_parallelism_contract_test() {
    use actionqueue_core::run::RunState;
    use actionqueue_engine::concurrency::lifecycle::{
        evaluate_state_transition, KeyLifecycleContext, LifecycleResult,
    };

    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    // Create two runs with different concurrency keys
    let run1 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    let key1 = "key-1";
    let key2 = "key-2";

    // Both runs enter Running state - each acquires its own key
    let result1 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result1, LifecycleResult::Acquired { .. }),
        "First run should acquire its key when entering Running"
    );
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));

    let result2 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key2.to_string()),
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result2, LifecycleResult::Acquired { .. }),
        "Second run should acquire different key when entering Running"
    );

    // Both keys should be occupied
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)));

    // Each key should be held by a different run
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)),
        Some(run1.id())
    );
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)),
        Some(run2.id())
    );
}

/// Verifies that releasing one key does not prevent the other key from being used.
///
/// This test demonstrates that key release is scoped to that specific key and does
/// not affect other keys held by the same or different runs.
#[test]
fn key_release_is_scoped_to_key_contract_test() {
    use actionqueue_core::run::RunState;
    use actionqueue_engine::concurrency::lifecycle::{
        evaluate_state_transition, KeyLifecycleContext, LifecycleResult,
    };

    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    let run1 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run2 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    let key1 = "scoped-key-1";
    let key2 = "scoped-key-2";

    // Both runs acquire their respective keys
    let _ = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    let _ = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key2.to_string()),
            run2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    // Both keys are occupied
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)));

    // run1 completes - releases key1
    let _ = evaluate_state_transition(
        RunState::Running,
        RunState::Completed,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    // key1 is now free
    assert!(!key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));

    // key2 is still occupied by run2
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)));
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)),
        Some(run2.id())
    );

    // A new run can now acquire key1
    let run3 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 502).expect("valid run");
    let result = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run3.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );

    assert!(
        matches!(result, LifecycleResult::Acquired { .. }),
        "Run3 should be able to acquire key1 after run1 releases it"
    );
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)),
        Some(run3.id())
    );
}

/// Integration test demonstrating mixed concurrency scenarios with same and different keys.
///
/// This test simulates a realistic scenario where:
/// - Multiple runs with the same key compete for the key (same-key exclusion)
/// - Runs with different keys execute in parallel (different-key parallelism)
#[test]
fn mixed_concurrency_scenario_test() {
    use actionqueue_core::run::RunState;
    use actionqueue_engine::concurrency::lifecycle::{
        evaluate_state_transition, KeyLifecycleContext, LifecycleResult,
    };

    let mut key_gate = KeyGate::new();
    let task_id = actionqueue_core::ids::TaskId::new();

    // Group A: Two runs with the same key (key-1) - they cannot run in parallel
    let run_a1 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 500).expect("valid run");
    let run_a2 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 501).expect("valid run");

    // Group B: Two runs with the same key (key-2) - they cannot run in parallel
    let run_b1 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 502).expect("valid run");
    let run_b2 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 503).expect("valid run");

    // Group C: One run with key-1 - same group as A, but different run
    let run_c1 =
        actionqueue_core::run::RunInstance::new_scheduled(task_id, 1000, 504).expect("valid run");

    let key1 = "group-1-key";
    let key2 = "group-2-key";

    // Phase 1: run_a1 enters Running - acquires key1
    let phase1 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run_a1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase1, LifecycleResult::Acquired { .. }));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)),
        Some(run_a1.id())
    );

    // Phase 2: run_a2 enters Running - key1 is occupied, acquire fails
    let phase2 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run_a2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase2, LifecycleResult::KeyOccupied { .. }));

    // Phase 3: run_b1 enters Running with key2 - acquires successfully (different key)
    let phase3 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key2.to_string()),
            run_b1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase3, LifecycleResult::Acquired { .. }));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)));
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)),
        Some(run_b1.id())
    );

    // Phase 4: run_b2 enters Running with key2 - key2 is occupied, acquire fails
    let phase4 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key2.to_string()),
            run_b2.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase4, LifecycleResult::KeyOccupied { .. }));

    // Phase 5: run_c1 enters Running with key1 - key1 is occupied by run_a1, acquire fails
    let phase5 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run_c1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase5, LifecycleResult::KeyOccupied { .. }));

    // Phase 6: run_a1 completes - releases key1
    let phase6 = evaluate_state_transition(
        RunState::Running,
        RunState::Completed,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run_a1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase6, LifecycleResult::Released { .. }));
    assert!(!key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key2)));

    // Phase 7: run_c1 enters Running with key1 - now succeeds (run_a1 released it)
    let phase7 = evaluate_state_transition(
        RunState::Leased,
        RunState::Running,
        KeyLifecycleContext::new(
            Some(key1.to_string()),
            run_c1.id(),
            &mut key_gate,
            ConcurrencyKeyHoldPolicy::default(),
        ),
    );
    assert!(matches!(phase7, LifecycleResult::Acquired { .. }));
    assert!(key_gate
        .is_key_occupied(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)));
    assert_eq!(
        key_gate.key_holder(&actionqueue_engine::concurrency::key_gate::ConcurrencyKey::new(key1)),
        Some(run_c1.id())
    );
}
