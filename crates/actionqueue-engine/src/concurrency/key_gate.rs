//! Concurrency key gate for single-flight execution control.
//!
//! This module defines typed concurrency key primitives and the gate logic
//! that prevents runs with the same concurrency key from executing in parallel.
//!
//! # Overview
//!
//! A concurrency key is an optional string attached to a task that ensures
//! only one run with that key can be in the Running state at a time.
//!
//! ## Key States
//!
//! - **Free**: No run currently holds the key.
//! - **Occupied**: A run is currently holding the key and executing.
//!
//! ## Gate Operations
//!
//! - **Acquire**: Attempts to acquire a key for a run. Returns `AcquireResult::Acquired`
//!   if the key is free, or `AcquireResult::Occupied` if another run holds it.
//! - **Release**: Releases a key, making it available for other runs.
//! - **Query**: Checking whether a key is currently occupied.

use std::fmt::{Display, Formatter};

use actionqueue_core::ids::RunId;

/// A typed concurrency key that ensures runs with the same key don't run in parallel.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConcurrencyKey(String);

impl ConcurrencyKey {
    /// Creates a new concurrency key from a string.
    ///
    /// In debug builds, panics if the value is empty.
    pub fn new(key: impl Into<String>) -> Self {
        let value = key.into();
        assert!(!value.is_empty(), "ConcurrencyKey must not be empty");
        Self(value)
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for ConcurrencyKey {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for ConcurrencyKey {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}

impl Display for ConcurrencyKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Result of a concurrency key acquisition attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum AcquireResult {
    /// The key was successfully acquired for the run.
    Acquired {
        /// The concurrency key that was acquired.
        key: ConcurrencyKey,
        /// The run that now holds the key.
        run_id: RunId,
    },
    /// The key is already occupied by another run.
    Occupied {
        /// The concurrency key that is occupied.
        key: ConcurrencyKey,
        /// The run that currently holds the key.
        holder_run_id: RunId,
    },
}

/// Result of a concurrency key release attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum ReleaseResult {
    /// The key was successfully released.
    Released {
        /// The concurrency key that was released.
        key: ConcurrencyKey,
    },
    /// The key was not held by the specified run, or was already free.
    NotHeld {
        /// The concurrency key that was attempted to release.
        key: ConcurrencyKey,
        /// The run that attempted to release the key.
        attempting_run_id: RunId,
    },
}

/// A key gate state that tracks which run (if any) holds each concurrency key.
#[derive(Debug, Clone, Default)]
pub struct KeyGate {
    /// Maps each concurrency key to the run that currently holds it.
    occupied_keys: std::collections::HashMap<ConcurrencyKey, RunId>,
}

impl KeyGate {
    /// Creates a new empty key gate.
    ///
    /// Empty initialization is safe post-recovery because the dispatch loop is
    /// single-threaded and acquires concurrency keys at dispatch time (step 6 of
    /// the tick cycle), not at recovery time. Runs that were in-flight when a
    /// crash occurred recover to the Ready state via the uncertainty clause and
    /// must re-acquire their concurrency keys before being dispatched again.
    /// This means there is no window where a recovered run could bypass the
    /// key gate.
    pub fn new() -> Self {
        Self { occupied_keys: std::collections::HashMap::new() }
    }

    /// Attempts to acquire a concurrency key for a run.
    ///
    /// Returns `AcquireResult::Acquired` if the key is free or already held by
    /// the same run. Returns `AcquireResult::Occupied` if another run holds the key.
    pub fn acquire(&mut self, key: ConcurrencyKey, run_id: RunId) -> AcquireResult {
        if let Some(holder) = self.occupied_keys.get(&key) {
            if *holder == run_id {
                // Already held by this run
                tracing::debug!(%key, %run_id, "concurrency key re-acquired by same run");
                AcquireResult::Acquired { key, run_id }
            } else {
                // Key is held by another run
                tracing::debug!(%key, %run_id, holder = %holder, "concurrency key occupied");
                AcquireResult::Occupied { key, holder_run_id: *holder }
            }
        } else {
            // Key is free, acquire it
            tracing::debug!(%key, %run_id, "concurrency key acquired");
            self.occupied_keys.insert(key.clone(), run_id);
            AcquireResult::Acquired { key, run_id }
        }
    }

    /// Attempts to release a concurrency key from a run.
    ///
    /// Returns `ReleaseResult::Released` if the key was held by the specified run.
    /// Returns `ReleaseResult::NotHeld` if the key was not held by the run
    /// or was already free.
    pub fn release(&mut self, key: ConcurrencyKey, run_id: RunId) -> ReleaseResult {
        match self.occupied_keys.get(&key) {
            Some(holder) if *holder == run_id => {
                self.occupied_keys.remove(&key);
                tracing::debug!(%key, %run_id, "concurrency key released");
                ReleaseResult::Released { key }
            }
            Some(_holder) => {
                // Key is held by a different run
                ReleaseResult::NotHeld { key, attempting_run_id: run_id }
            }
            None => {
                // Key is already free
                ReleaseResult::NotHeld { key, attempting_run_id: run_id }
            }
        }
    }

    /// Queries whether a concurrency key is currently occupied.
    pub fn is_key_occupied(&self, key: &ConcurrencyKey) -> bool {
        self.occupied_keys.contains_key(key)
    }

    /// Returns the run that currently holds the key, if any.
    pub fn key_holder(&self, key: &ConcurrencyKey) -> Option<RunId> {
        self.occupied_keys.get(key).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_succeeds_when_key_is_free() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let run_id = RunId::new();

        let result = gate.acquire(key.clone(), run_id);

        match result {
            AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
                assert_eq!(acquired_key, key);
                assert_eq!(acquired_run_id, run_id);
            }
            AcquireResult::Occupied { .. } => panic!("Expected acquisition to succeed"),
        }

        assert!(gate.is_key_occupied(&key));
        assert_eq!(gate.key_holder(&key), Some(run_id));
    }

    #[test]
    fn acquire_fails_when_key_is_occupied_by_different_run() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let holder_run_id = RunId::new();
        let requesting_run_id = RunId::new();

        // First run acquires the key
        let _ = gate.acquire(key.clone(), holder_run_id);

        // Second run tries to acquire the same key
        let result = gate.acquire(key.clone(), requesting_run_id);

        match result {
            AcquireResult::Occupied { key: occupied_key, holder_run_id: occupied_holder } => {
                assert_eq!(occupied_key, key);
                assert_eq!(occupied_holder, holder_run_id);
            }
            AcquireResult::Acquired { .. } => panic!("Expected acquisition to fail"),
        }

        // The holder should remain unchanged
        assert_eq!(gate.key_holder(&key), Some(holder_run_id));
    }

    #[test]
    fn acquire_succeeds_when_same_run_reacquires_key() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let run_id = RunId::new();

        // First acquisition
        let _ = gate.acquire(key.clone(), run_id);

        // Same run tries to acquire again (should still succeed, re-entrant)
        let result = gate.acquire(key.clone(), run_id);

        match result {
            AcquireResult::Acquired { key: acquired_key, run_id: acquired_run_id } => {
                assert_eq!(acquired_key, key);
                assert_eq!(acquired_run_id, run_id);
            }
            AcquireResult::Occupied { .. } => panic!("Expected re-acquisition to succeed"),
        }

        assert!(gate.is_key_occupied(&key));
        assert_eq!(gate.key_holder(&key), Some(run_id));
    }

    #[test]
    fn release_releases_key_held_by_same_run() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let run_id = RunId::new();

        // Acquire the key
        let _ = gate.acquire(key.clone(), run_id);

        // Release the key
        let result = gate.release(key.clone(), run_id);

        match result {
            ReleaseResult::Released { key: released_key } => {
                assert_eq!(released_key, key);
            }
            ReleaseResult::NotHeld { .. } => panic!("Expected release to succeed"),
        }

        assert!(!gate.is_key_occupied(&key));
        assert_eq!(gate.key_holder(&key), None);
    }

    #[test]
    fn release_fails_when_key_held_by_different_run() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let holder_run_id = RunId::new();
        let attempting_run_id = RunId::new();

        // Different run acquires the key
        let _ = gate.acquire(key.clone(), holder_run_id);

        // Different run tries to release
        let result = gate.release(key.clone(), attempting_run_id);

        match result {
            ReleaseResult::NotHeld { key: released_key, attempting_run_id: attempted_run_id } => {
                assert_eq!(released_key, key);
                assert_eq!(attempted_run_id, attempting_run_id);
            }
            ReleaseResult::Released { .. } => panic!("Expected release to fail"),
        }

        // The key should still be held by the original holder
        assert!(gate.is_key_occupied(&key));
        assert_eq!(gate.key_holder(&key), Some(holder_run_id));
    }

    #[test]
    fn release_has_no_effect_when_key_is_free() {
        let mut gate = KeyGate::new();
        let key = ConcurrencyKey::new("my-key");
        let run_id = RunId::new();

        // Try to release a free key
        let result = gate.release(key.clone(), run_id);

        match result {
            ReleaseResult::NotHeld { .. } => {}
            ReleaseResult::Released { .. } => panic!("Expected release to have no effect"),
        }

        assert!(!gate.is_key_occupied(&key));
    }

    #[test]
    fn different_keys_can_be_occupied_simultaneously() {
        let mut gate = KeyGate::new();
        let key1 = ConcurrencyKey::new("key-1");
        let key2 = ConcurrencyKey::new("key-2");
        let run1_id = RunId::new();
        let run2_id = RunId::new();

        // Acquire different keys
        let _ = gate.acquire(key1.clone(), run1_id);
        let _ = gate.acquire(key2.clone(), run2_id);

        // Both keys should be occupied
        assert!(gate.is_key_occupied(&key1));
        assert!(gate.is_key_occupied(&key2));
        assert_eq!(gate.key_holder(&key1), Some(run1_id));
        assert_eq!(gate.key_holder(&key2), Some(run2_id));
    }
}
