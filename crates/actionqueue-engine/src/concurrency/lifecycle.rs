//! Concurrency key lifecycle policy for run execution boundaries.
//!
//! This module implements the concurrency key acquire and release policy:
//! - Keys are acquired when a run transitions to Running state (execution starts).
//! - Keys are released when a run transitions to a terminal state.
//!
//! The lifecycle logic uses the key gate contracts from `key_gate` module
//! and does not add unrelated policy behavior.

use actionqueue_core::ids::RunId;
use actionqueue_core::run::RunState;
use actionqueue_core::task::constraints::ConcurrencyKeyHoldPolicy;

use crate::concurrency::key_gate::{AcquireResult, KeyGate, ReleaseResult};

/// Result of a key lifecycle operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum LifecycleResult {
    /// Key was acquired successfully.
    Acquired {
        /// The concurrency key that was acquired.
        key: String,
        /// The run that now holds the key.
        run_id: RunId,
    },
    /// Key acquisition failed because the key is occupied by another run.
    KeyOccupied {
        /// The concurrency key that is occupied.
        key: String,
        /// The run that currently holds the key.
        holder_run_id: RunId,
    },
    /// Key was released successfully.
    Released {
        /// The concurrency key that was released.
        key: String,
    },
    /// No action was taken (either no key or key already free).
    NoAction {
        /// The key that was attempted to release (if any).
        key: Option<String>,
    },
}

/// Attempts to acquire the concurrency key for a run.
///
/// Returns `LifecycleResult::Acquired` if the key is free or already held by
/// the same run. Returns `LifecycleResult::KeyOccupied` if another run holds it.
///
/// The key is obtained from the task constraints. Returns `NoAction` if no
/// concurrency key is defined.
pub fn acquire_key(key: Option<String>, run_id: RunId, key_gate: &mut KeyGate) -> LifecycleResult {
    match key {
        Some(key_str) => {
            let concurrency_key = crate::concurrency::key_gate::ConcurrencyKey::new(key_str);
            match key_gate.acquire(concurrency_key, run_id) {
                AcquireResult::Acquired { key, run_id } => {
                    LifecycleResult::Acquired { key: key.as_str().to_string(), run_id }
                }
                AcquireResult::Occupied { key, holder_run_id } => {
                    LifecycleResult::KeyOccupied { key: key.as_str().to_string(), holder_run_id }
                }
            }
        }
        None => LifecycleResult::NoAction { key: None },
    }
}

/// Attempts to release the concurrency key for a run.
///
/// Returns `LifecycleResult::Released` if the key was held by the run.
/// Returns `LifecycleResult::NoAction` if no key was defined or the key
/// was not held by the run.
pub fn release_key(key: Option<String>, run_id: RunId, key_gate: &mut KeyGate) -> LifecycleResult {
    match key {
        Some(key_str) => {
            let concurrency_key = crate::concurrency::key_gate::ConcurrencyKey::new(key_str);
            match key_gate.release(concurrency_key, run_id) {
                ReleaseResult::Released { key } => {
                    LifecycleResult::Released { key: key.as_str().to_string() }
                }
                ReleaseResult::NotHeld { key, .. } => {
                    LifecycleResult::NoAction { key: Some(key.as_str().to_string()) }
                }
            }
        }
        None => LifecycleResult::NoAction { key: None },
    }
}

/// Context for evaluating concurrency key lifecycle during a state transition.
pub struct KeyLifecycleContext<'a> {
    /// Optional concurrency key associated with the run.
    concurrency_key: Option<String>,
    /// The run undergoing the state transition.
    run_id: RunId,
    /// Mutable reference to the key gate managing concurrency slots.
    key_gate: &'a mut KeyGate,
    /// Policy controlling key behavior during retry transitions.
    hold_policy: ConcurrencyKeyHoldPolicy,
}

impl<'a> KeyLifecycleContext<'a> {
    /// Creates a new key lifecycle context.
    pub fn new(
        concurrency_key: Option<String>,
        run_id: RunId,
        key_gate: &'a mut KeyGate,
        hold_policy: ConcurrencyKeyHoldPolicy,
    ) -> Self {
        Self { concurrency_key, run_id, key_gate, hold_policy }
    }

    /// Returns the optional concurrency key.
    pub fn concurrency_key(&self) -> Option<&str> {
        self.concurrency_key.as_deref()
    }

    /// Returns the run identifier.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the hold policy.
    pub fn hold_policy(&self) -> ConcurrencyKeyHoldPolicy {
        self.hold_policy
    }
}

/// Determines the appropriate key lifecycle action based on a state transition.
///
/// When a run transitions to Running, the concurrency key should be acquired.
/// When a run transitions to a terminal state (Completed, Failed, Canceled),
/// the concurrency key should be released.
///
/// The `hold_policy` parameter controls behavior when transitioning from
/// Running to RetryWait:
/// - [`ConcurrencyKeyHoldPolicy::HoldDuringRetry`]: the key is retained
///   (no release) so no other run with the same key can start during retry.
/// - [`ConcurrencyKeyHoldPolicy::ReleaseOnRetry`]: the key is released,
///   allowing other runs to acquire it while this run waits for retry.
pub fn evaluate_state_transition(
    from: RunState,
    to: RunState,
    ctx: KeyLifecycleContext<'_>,
) -> LifecycleResult {
    let KeyLifecycleContext { concurrency_key, run_id, key_gate, hold_policy } = ctx;
    tracing::debug!(%run_id, ?from, ?to, "concurrency key lifecycle evaluated");

    // Key is acquired when entering Running state
    if from != RunState::Running && to == RunState::Running {
        return acquire_key(concurrency_key, run_id, key_gate);
    }

    // When leaving Running for RetryWait, consult the hold policy
    if from == RunState::Running && to == RunState::RetryWait {
        return match hold_policy {
            ConcurrencyKeyHoldPolicy::HoldDuringRetry => LifecycleResult::NoAction { key: None },
            ConcurrencyKeyHoldPolicy::ReleaseOnRetry => {
                release_key(concurrency_key, run_id, key_gate)
            }
        };
    }

    // When leaving Running for Suspended, follow the same hold policy as RetryWait.
    // The run is paused and may resume, so the key behaviour mirrors retry semantics.
    if from == RunState::Running && to == RunState::Suspended {
        return match hold_policy {
            ConcurrencyKeyHoldPolicy::HoldDuringRetry => LifecycleResult::NoAction { key: None },
            ConcurrencyKeyHoldPolicy::ReleaseOnRetry => {
                release_key(concurrency_key, run_id, key_gate)
            }
        };
    }

    // Key is released when leaving Running state for any other non-Running state
    if from == RunState::Running && to != RunState::Running {
        return release_key(concurrency_key, run_id, key_gate);
    }

    // When a Suspended run reaches a terminal state (e.g. cascade cancellation),
    // release the key unconditionally. The key may have been held during suspend
    // under HoldDuringRetry policy and must be freed to avoid permanent key leak.
    if from == RunState::Suspended && to.is_terminal() {
        return release_key(concurrency_key, run_id, key_gate);
    }

    // No key lifecycle action needed for other transitions
    LifecycleResult::NoAction { key: None }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::RunId;

    use super::*;

    #[test]
    fn acquire_key_succeeds_when_key_is_free() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();
        let key = Some("my-key".to_string());

        let result = acquire_key(key, run_id, &mut key_gate);

        match result {
            LifecycleResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
                assert_eq!(acquired_key, "my-key");
                assert_eq!(acquired_run_id, run_id);
            }
            _ => panic!("Expected acquire to succeed"),
        }
    }

    #[test]
    fn acquire_key_returns_no_action_when_no_key_defined() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        let result = acquire_key(None, run_id, &mut key_gate);

        assert_eq!(result, LifecycleResult::NoAction { key: None });
    }

    #[test]
    fn acquire_key_fails_when_key_is_occupied() {
        let mut key_gate = KeyGate::new();
        let holder_run_id = RunId::new();
        let requesting_run_id = RunId::new();
        let key = Some("my-key".to_string());

        // First run acquires the key
        let _ = acquire_key(key.clone(), holder_run_id, &mut key_gate);

        // Second run tries to acquire the same key
        let result = acquire_key(key, requesting_run_id, &mut key_gate);

        match result {
            LifecycleResult::KeyOccupied { key: occupied_key, holder_run_id: occupied_holder } => {
                assert_eq!(occupied_key, "my-key");
                assert_eq!(occupied_holder, holder_run_id);
            }
            _ => panic!("Expected key to be occupied"),
        }
    }

    #[test]
    fn release_key_succeeds_when_key_is_held() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();
        let key = Some("my-key".to_string());

        // Acquire the key first
        let _ = acquire_key(key.clone(), run_id, &mut key_gate);

        // Release the key
        let result = release_key(key, run_id, &mut key_gate);

        match result {
            LifecycleResult::Released { key: released_key } => {
                assert_eq!(released_key, "my-key");
            }
            _ => panic!("Expected release to succeed"),
        }
    }

    #[test]
    fn release_key_returns_no_action_when_no_key_defined() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        let result = release_key(None, run_id, &mut key_gate);

        assert_eq!(result, LifecycleResult::NoAction { key: None });
    }

    #[test]
    fn release_key_returns_no_action_when_key_not_held() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();
        let key = Some("my-key".to_string());

        let result = release_key(key, run_id, &mut key_gate);

        assert_eq!(result, LifecycleResult::NoAction { key: Some("my-key".to_string()) });
    }

    #[test]
    fn evaluate_transition_acquires_key_when_entering_running() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        let result = evaluate_state_transition(
            RunState::Leased,
            RunState::Running,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::default(),
            ),
        );

        match result {
            LifecycleResult::Acquired { key, run_id: acquired_run_id } => {
                assert_eq!(key, "my-key");
                assert_eq!(acquired_run_id, run_id);
            }
            _ => panic!("Expected key to be acquired"),
        }
    }

    #[test]
    fn evaluate_transition_releases_key_when_entering_terminal_state() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        // First acquire the key
        let _ = acquire_key(Some("my-key".to_string()), run_id, &mut key_gate);

        // Then evaluate transition to terminal state
        let result = evaluate_state_transition(
            RunState::Running,
            RunState::Completed,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::default(),
            ),
        );

        match result {
            LifecycleResult::Released { key } => {
                assert_eq!(key, "my-key");
            }
            _ => panic!("Expected key to be released"),
        }
    }

    #[test]
    fn evaluate_transition_no_action_for_non_key_transitions() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        // Evaluate transition that doesn't trigger key lifecycle
        let result = evaluate_state_transition(
            RunState::Scheduled,
            RunState::Ready,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::default(),
            ),
        );

        assert_eq!(result, LifecycleResult::NoAction { key: None });
    }

    #[test]
    fn evaluate_transition_holds_key_during_retry_with_default_policy() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        // First transition to Running should acquire
        let first = evaluate_state_transition(
            RunState::Leased,
            RunState::Running,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::HoldDuringRetry,
            ),
        );

        // Transition from Running to RetryWait with HoldDuringRetry should NOT release
        let second = evaluate_state_transition(
            RunState::Running,
            RunState::RetryWait,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::HoldDuringRetry,
            ),
        );

        assert!(matches!(first, LifecycleResult::Acquired { .. }));
        assert!(matches!(second, LifecycleResult::NoAction { .. }));
    }

    #[test]
    fn evaluate_transition_releases_key_on_retry_with_release_policy() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        // First transition to Running should acquire
        let first = evaluate_state_transition(
            RunState::Leased,
            RunState::Running,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::ReleaseOnRetry,
            ),
        );

        // Transition from Running to RetryWait with ReleaseOnRetry should release
        let second = evaluate_state_transition(
            RunState::Running,
            RunState::RetryWait,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::ReleaseOnRetry,
            ),
        );

        assert!(matches!(first, LifecycleResult::Acquired { .. }));
        assert!(matches!(second, LifecycleResult::Released { .. }));
    }

    #[test]
    fn evaluate_transition_releases_key_on_suspended_to_canceled_with_hold_policy() {
        let mut key_gate = KeyGate::new();
        let run_id = RunId::new();

        // Acquire key (simulating Running state entry)
        let _ = acquire_key(Some("my-key".to_string()), run_id, &mut key_gate);

        // Running→Suspended with HoldDuringRetry should NOT release
        let suspend_result = evaluate_state_transition(
            RunState::Running,
            RunState::Suspended,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::HoldDuringRetry,
            ),
        );
        assert!(
            matches!(suspend_result, LifecycleResult::NoAction { .. }),
            "HoldDuringRetry must not release key on suspend"
        );

        // Suspended→Canceled must release regardless of hold policy
        let cancel_result = evaluate_state_transition(
            RunState::Suspended,
            RunState::Canceled,
            KeyLifecycleContext::new(
                Some("my-key".to_string()),
                run_id,
                &mut key_gate,
                ConcurrencyKeyHoldPolicy::HoldDuringRetry,
            ),
        );
        assert!(
            matches!(cancel_result, LifecycleResult::Released { .. }),
            "Suspended→Canceled must release concurrency key"
        );
    }
}
