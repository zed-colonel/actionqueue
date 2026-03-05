//! Run instance - a specific scheduled occurrence of a task.

use crate::ids::{AttemptId, RunId, TaskId};
use crate::run::state::RunState;
use crate::run::transitions::is_valid_transition;

/// Typed validation errors returned by [`RunInstance`] constructors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunInstanceConstructionError {
    /// The supplied [`RunId`] is nil/empty and therefore invalid.
    InvalidRunId {
        /// Rejected run identifier.
        run_id: RunId,
    },
    /// The supplied [`TaskId`] is nil/empty and therefore invalid.
    InvalidTaskId {
        /// Rejected task identifier.
        task_id: TaskId,
    },
    /// A `Ready` run cannot be created with a future schedule instant.
    ReadyScheduledAtAfterCreatedAt {
        /// Run identifier associated with the failed construction.
        run_id: RunId,
        /// Requested scheduled timestamp.
        scheduled_at: u64,
        /// Requested creation timestamp.
        created_at: u64,
    },
}

impl std::fmt::Display for RunInstanceConstructionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRunId { run_id } => {
                write!(f, "invalid run_id for run construction: {run_id}")
            }
            Self::InvalidTaskId { task_id } => {
                write!(f, "invalid task_id for run construction: {task_id}")
            }
            Self::ReadyScheduledAtAfterCreatedAt { run_id, scheduled_at, created_at } => {
                write!(
                    f,
                    "run {run_id} cannot be created in Ready with scheduled_at ({scheduled_at}) > \
                     created_at ({created_at})"
                )
            }
        }
    }
}

impl std::error::Error for RunInstanceConstructionError {}

/// A run instance represents a specific scheduled occurrence of a task.
///
/// Each run is uniquely identified by a RunId and is associated with a TaskId.
/// The lifecycle of a run progresses through canonical states according to the run policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[must_use]
pub struct RunInstance {
    /// Unique identifier for this run instance.
    id: RunId,

    /// The task that this run belongs to.
    task_id: TaskId,

    /// The current state of this run.
    state: RunState,

    /// The attempt that is currently active (if any).
    /// This is set when the run transitions to Running state.
    current_attempt_id: Option<AttemptId>,

    /// The number of attempts made so far.
    attempt_count: u32,

    /// The time at which this run was created.
    created_at: u64,

    /// The time at which this run should become Ready.
    /// For "Once" policy, this is when the run is created.
    /// For "Repeat" policy, this is computed based on the start time and interval.
    scheduled_at: u64,

    /// The snapshot priority used for selection ordering.
    /// This is set when the run transitions to Ready state.
    effective_priority: i32,

    /// Timestamp of the most recent state change, used by retry backoff
    /// to compute when a run entered RetryWait. Defaults to `created_at`.
    #[cfg_attr(feature = "serde", serde(default))]
    last_state_change_at: u64,
}

impl RunInstance {
    /// Creates a new run instance in Scheduled state.
    pub fn new_scheduled(
        task_id: TaskId,
        scheduled_at: u64,
        created_at: u64,
    ) -> Result<Self, RunInstanceConstructionError> {
        Self::new_scheduled_with_id(RunId::new(), task_id, scheduled_at, created_at)
    }

    /// Creates a new run instance in Scheduled state with an explicit identifier.
    ///
    /// This constructor is primarily intended for deterministic replay/bootstrap
    /// flows that must preserve a durable [`RunId`].
    pub fn new_scheduled_with_id(
        id: RunId,
        task_id: TaskId,
        scheduled_at: u64,
        created_at: u64,
    ) -> Result<Self, RunInstanceConstructionError> {
        Self::validate_identifiers(id, task_id)?;

        Ok(Self {
            id,
            task_id,
            state: RunState::Scheduled,
            current_attempt_id: None,
            attempt_count: 0,
            created_at,
            scheduled_at,
            effective_priority: 0,
            last_state_change_at: created_at,
        })
    }

    /// Creates a new ready run with the specified effective priority.
    pub fn new_ready(
        task_id: TaskId,
        scheduled_at: u64,
        created_at: u64,
        effective_priority: i32,
    ) -> Result<Self, RunInstanceConstructionError> {
        Self::new_ready_with_id(RunId::new(), task_id, scheduled_at, created_at, effective_priority)
    }

    /// Creates a new ready run with an explicit identifier and effective priority.
    pub fn new_ready_with_id(
        id: RunId,
        task_id: TaskId,
        scheduled_at: u64,
        created_at: u64,
        effective_priority: i32,
    ) -> Result<Self, RunInstanceConstructionError> {
        Self::validate_identifiers(id, task_id)?;

        if scheduled_at > created_at {
            return Err(RunInstanceConstructionError::ReadyScheduledAtAfterCreatedAt {
                run_id: id,
                scheduled_at,
                created_at,
            });
        }

        Ok(Self {
            id,
            task_id,
            state: RunState::Ready,
            current_attempt_id: None,
            attempt_count: 0,
            created_at,
            scheduled_at,
            effective_priority,
            last_state_change_at: created_at,
        })
    }

    /// Validates identifier-level construction invariants.
    fn validate_identifiers(
        id: RunId,
        task_id: TaskId,
    ) -> Result<(), RunInstanceConstructionError> {
        if id.as_uuid().is_nil() {
            return Err(RunInstanceConstructionError::InvalidRunId { run_id: id });
        }

        if task_id.as_uuid().is_nil() {
            return Err(RunInstanceConstructionError::InvalidTaskId { task_id });
        }

        Ok(())
    }

    /// Returns true if this run is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Returns this run's identifier.
    pub fn id(&self) -> RunId {
        self.id
    }

    /// Returns the owning task identifier for this run.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Returns the current lifecycle state for this run.
    pub fn state(&self) -> RunState {
        self.state
    }

    /// Returns the currently active attempt identifier, if one exists.
    pub fn current_attempt_id(&self) -> Option<AttemptId> {
        self.current_attempt_id
    }

    /// Returns the number of started attempts for this run.
    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    /// Returns the run creation timestamp.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Returns the timestamp at which this run becomes eligible for readiness.
    pub fn scheduled_at(&self) -> u64 {
        self.scheduled_at
    }

    /// Returns the effective priority snapshot currently associated with this run.
    pub fn effective_priority(&self) -> i32 {
        self.effective_priority
    }

    /// Returns the timestamp of the most recent state change.
    pub fn last_state_change_at(&self) -> u64 {
        self.last_state_change_at
    }

    /// Records the timestamp of a state change (called by the reducer after transitions).
    ///
    /// # Invariants
    ///
    /// In production, the supplied `timestamp` must be monotonically non-decreasing
    /// relative to the current `last_state_change_at`. The reducer is the sole caller
    /// and is expected to feed wall-clock timestamps from durably ordered WAL events.
    /// Violation of this invariant will not cause incorrect behavior (the field is
    /// simply overwritten), but it indicates a clock or sequencing anomaly that should
    /// be investigated.
    pub fn record_state_change_at(&mut self, timestamp: u64) {
        self.last_state_change_at = timestamp;
    }

    /// Applies a validated lifecycle transition.
    ///
    /// Transition legality is enforced via the canonical transition table.
    /// Leaving `Running` while an active attempt is still open is rejected.
    pub fn transition_to(&mut self, new_state: RunState) -> Result<(), RunInstanceError> {
        if !is_valid_transition(self.state, new_state) {
            return Err(RunInstanceError::InvalidTransition {
                run_id: self.id,
                from: self.state,
                to: new_state,
            });
        }

        if self.state == RunState::Running
            && new_state != RunState::Running
            && new_state != RunState::Canceled
            && self.current_attempt_id.is_some()
        {
            return Err(RunInstanceError::AttemptInProgress {
                run_id: self.id,
                active_attempt_id: self.current_attempt_id.expect("checked is_some above"),
            });
        }

        if new_state != RunState::Running {
            self.current_attempt_id = None;
        }
        self.state = new_state;
        Ok(())
    }

    /// Promotes this run into the `Ready` state with transition validation.
    pub fn promote_to_ready(&mut self) -> Result<(), RunInstanceError> {
        self.transition_to(RunState::Ready)
    }

    /// Promotes this run into the `Ready` state and snapshots its effective priority.
    pub fn promote_to_ready_with_priority(
        &mut self,
        effective_priority: i32,
    ) -> Result<(), RunInstanceError> {
        self.transition_to(RunState::Ready)?;
        self.effective_priority = effective_priority;
        Ok(())
    }

    /// Updates the ready-state effective priority snapshot.
    pub fn set_effective_priority(
        &mut self,
        effective_priority: i32,
    ) -> Result<(), RunInstanceError> {
        if self.state != RunState::Ready {
            return Err(RunInstanceError::PriorityMutationRequiresReady {
                run_id: self.id,
                current_state: self.state,
            });
        }

        self.effective_priority = effective_priority;
        Ok(())
    }

    /// Records the start of a new attempt for a run currently in `Running`.
    pub fn start_attempt(&mut self, attempt_id: AttemptId) -> Result<(), RunInstanceError> {
        if self.state != RunState::Running {
            return Err(RunInstanceError::AttemptStartRequiresRunning {
                run_id: self.id,
                current_state: self.state,
            });
        }

        if self.current_attempt_id.is_some() {
            return Err(RunInstanceError::AttemptAlreadyActive {
                run_id: self.id,
                active_attempt_id: self.current_attempt_id.expect("checked is_some above"),
            });
        }

        self.attempt_count = self
            .attempt_count
            .checked_add(1)
            .ok_or(RunInstanceError::AttemptCountOverflow { run_id: self.id })?;
        self.current_attempt_id = Some(attempt_id);
        Ok(())
    }

    /// Records completion of the current active attempt.
    pub fn finish_attempt(&mut self, attempt_id: AttemptId) -> Result<(), RunInstanceError> {
        if self.state != RunState::Running {
            return Err(RunInstanceError::AttemptFinishRequiresRunning {
                run_id: self.id,
                current_state: self.state,
            });
        }

        match self.current_attempt_id {
            Some(active_attempt_id) if active_attempt_id == attempt_id => {
                self.current_attempt_id = None;
                Ok(())
            }
            Some(active_attempt_id) => Err(RunInstanceError::AttemptOwnershipMismatch {
                run_id: self.id,
                expected_attempt_id: active_attempt_id,
                actual_attempt_id: attempt_id,
            }),
            None => Err(RunInstanceError::NoActiveAttempt { run_id: self.id }),
        }
    }
}

/// Typed lifecycle and mutation errors for [`RunInstance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunInstanceError {
    /// The requested transition is not allowed by the canonical transition table.
    InvalidTransition {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Current source state.
        from: RunState,
        /// Requested target state.
        to: RunState,
    },
    /// Attempt-related transition blocked because an attempt is still active.
    AttemptInProgress {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Active attempt that must be finished before state exit.
        active_attempt_id: AttemptId,
    },
    /// Attempt start was requested while not in the `Running` state.
    AttemptStartRequiresRunning {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Current state that rejected attempt start.
        current_state: RunState,
    },
    /// Attempt finish was requested while not in the `Running` state.
    AttemptFinishRequiresRunning {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Current state that rejected attempt finish.
        current_state: RunState,
    },
    /// Attempt start was requested but a different active attempt already exists.
    AttemptAlreadyActive {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Currently active attempt.
        active_attempt_id: AttemptId,
    },
    /// Attempt finish did not match the currently active attempt.
    AttemptOwnershipMismatch {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Expected active attempt id.
        expected_attempt_id: AttemptId,
        /// Supplied attempt id.
        actual_attempt_id: AttemptId,
    },
    /// Attempt finish was requested without any active attempt.
    NoActiveAttempt {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
    },
    /// Attempt counter overflowed `u32`.
    AttemptCountOverflow {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
    },
    /// Priority snapshots can only be mutated while the run is in `Ready`.
    PriorityMutationRequiresReady {
        /// Run identifier associated with the failed mutation.
        run_id: RunId,
        /// Current state that rejected priority mutation.
        current_state: RunState,
    },
}

impl std::fmt::Display for RunInstanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunInstanceError::InvalidTransition { run_id, from, to } => {
                write!(f, "invalid run transition for {run_id}: {from:?} -> {to:?}")
            }
            RunInstanceError::AttemptInProgress { run_id, active_attempt_id } => {
                write!(
                    f,
                    "run {run_id} cannot leave Running while attempt {active_attempt_id} is active"
                )
            }
            RunInstanceError::AttemptStartRequiresRunning { run_id, current_state } => {
                write!(f, "run {run_id} cannot start attempt in state {current_state:?}")
            }
            RunInstanceError::AttemptFinishRequiresRunning { run_id, current_state } => {
                write!(f, "run {run_id} cannot finish attempt in state {current_state:?}")
            }
            RunInstanceError::AttemptAlreadyActive { run_id, active_attempt_id } => {
                write!(f, "run {run_id} already has active attempt {active_attempt_id}")
            }
            RunInstanceError::AttemptOwnershipMismatch {
                run_id,
                expected_attempt_id,
                actual_attempt_id,
            } => {
                write!(
                    f,
                    "run {run_id} attempt mismatch: expected {expected_attempt_id}, got \
                     {actual_attempt_id}"
                )
            }
            RunInstanceError::NoActiveAttempt { run_id } => {
                write!(f, "run {run_id} has no active attempt")
            }
            RunInstanceError::AttemptCountOverflow { run_id } => {
                write!(f, "run {run_id} attempt counter overflow")
            }
            RunInstanceError::PriorityMutationRequiresReady { run_id, current_state } => {
                write!(
                    f,
                    "run {run_id} priority can only be updated in Ready state (current: \
                     {current_state:?})"
                )
            }
        }
    }
}

impl std::error::Error for RunInstanceError {}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for RunInstance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RunInstanceWire {
            id: RunId,
            task_id: TaskId,
            state: RunState,
            current_attempt_id: Option<AttemptId>,
            attempt_count: u32,
            created_at: u64,
            scheduled_at: u64,
            effective_priority: i32,
            #[serde(default)]
            last_state_change_at: u64,
        }

        let wire = RunInstanceWire::deserialize(deserializer)?;

        // Validate identifiers
        if wire.id.as_uuid().is_nil() {
            return Err(serde::de::Error::custom("run_id must not be nil"));
        }
        if wire.task_id.as_uuid().is_nil() {
            return Err(serde::de::Error::custom("task_id must not be nil"));
        }

        // Validate state/attempt consistency
        if wire.state != RunState::Running && wire.current_attempt_id.is_some() {
            return Err(serde::de::Error::custom(
                "active attempt_id is only valid in Running state",
            ));
        }
        if wire.state.is_terminal() && wire.current_attempt_id.is_some() {
            return Err(serde::de::Error::custom("terminal state cannot have an active attempt"));
        }

        // Validate schedule causality for Ready state
        if wire.state == RunState::Ready && wire.scheduled_at > wire.created_at {
            return Err(serde::de::Error::custom(format!(
                "Ready state requires scheduled_at ({}) <= created_at ({})",
                wire.scheduled_at, wire.created_at,
            )));
        }

        Ok(RunInstance {
            id: wire.id,
            task_id: wire.task_id,
            state: wire.state,
            current_attempt_id: wire.current_attempt_id,
            attempt_count: wire.attempt_count,
            created_at: wire.created_at,
            scheduled_at: wire.scheduled_at,
            effective_priority: wire.effective_priority,
            last_state_change_at: wire.last_state_change_at,
        })
    }
}

impl RunInstance {
    /// Restores attempt state during snapshot bootstrap.
    ///
    /// During snapshot bootstrap, runs are created via `new_scheduled_with_id`
    /// (which initializes `attempt_count=0` and `current_attempt_id=None`) then
    /// replayed through state transitions. However, `start_attempt()`/`finish_attempt()`
    /// are never called during bootstrap replay. This method directly sets the
    /// attempt count and active attempt ID from the snapshot's attempt history.
    ///
    /// This is a bootstrap-only method — normal operation uses `start_attempt()`
    /// and `finish_attempt()` which enforce state-machine invariants.
    pub fn restore_attempt_state_for_bootstrap(
        &mut self,
        count: u32,
        active_attempt: Option<AttemptId>,
    ) {
        self.attempt_count = count;
        self.current_attempt_id = active_attempt;
    }
}

#[cfg(test)]
impl RunInstance {
    /// Sets the attempt count to an arbitrary value for testing overflow paths.
    pub(crate) fn set_attempt_count_for_testing(&mut self, count: u32) {
        self.attempt_count = count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{AttemptId, TaskId};

    #[test]
    fn start_attempt_at_u32_max_returns_overflow_error() {
        let mut run =
            RunInstance::new_scheduled(TaskId::new(), 1_000, 1_000).expect("valid scheduled run");
        run.transition_to(RunState::Ready).unwrap();
        run.transition_to(RunState::Leased).unwrap();
        run.transition_to(RunState::Running).unwrap();
        run.set_attempt_count_for_testing(u32::MAX);

        let result = run.start_attempt(AttemptId::new());

        assert!(result.is_err(), "start_attempt at u32::MAX should fail");
        assert_eq!(
            result.unwrap_err(),
            RunInstanceError::AttemptCountOverflow { run_id: run.id() },
        );
    }
}
