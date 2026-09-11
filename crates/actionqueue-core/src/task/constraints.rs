//! Task constraints that enforce execution limits and behavior.

use super::safety::SafetyLevel;
use crate::executor::{ExecutorTraitError, ExecutorTraits};

/// Typed validation errors for [`TaskConstraints`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskConstraintsError {
    /// The configured attempt cap is invalid.
    InvalidMaxAttempts {
        /// The rejected `max_attempts` value.
        max_attempts: u32,
    },
    /// An empty string was provided as a concurrency key.
    EmptyConcurrencyKey,
    /// A timeout of zero seconds was provided.
    ZeroTimeout,
    /// A routing label or trait count violates its bounds.
    InvalidExecutorTrait(ExecutorTraitError),
}

impl std::fmt::Display for TaskConstraintsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskConstraintsError::InvalidMaxAttempts { max_attempts } => {
                write!(f, "invalid max_attempts value: {max_attempts} (must be >= 1)")
            }
            TaskConstraintsError::EmptyConcurrencyKey => {
                write!(f, "concurrency key must not be an empty string")
            }
            TaskConstraintsError::ZeroTimeout => {
                write!(f, "timeout_secs must not be zero")
            }
            TaskConstraintsError::InvalidExecutorTrait(error) => {
                write!(f, "invalid executor traits: {error}")
            }
        }
    }
}

impl std::error::Error for TaskConstraintsError {}

/// Policy for concurrency key behavior during retry wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConcurrencyKeyHoldPolicy {
    /// Hold the concurrency key during retry (default - current behavior).
    #[default]
    HoldDuringRetry,
    /// Release the concurrency key when entering RetryWait.
    ReleaseOnRetry,
}

/// Constraints that control how a task's runs are executed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TaskConstraints {
    /// Maximum number of attempts for each run.
    /// Default: 1
    max_attempts: u32,
    /// Execution timeout in seconds. If `None`, no timeout.
    timeout_secs: Option<u64>,
    /// Optional concurrency key. Runs with the same key never run concurrently.
    concurrency_key: Option<String>,
    /// Policy for concurrency key behavior during retry wait.
    #[cfg_attr(feature = "serde", serde(default))]
    concurrency_key_hold_policy: ConcurrencyKeyHoldPolicy,
    concurrency_key_wait_policy: ConcurrencyKeyWaitPolicy,
    /// Safety level classification for the task's side-effect characteristics.
    #[cfg_attr(feature = "serde", serde(default))]
    safety_level: SafetyLevel,
    /// Required executor traits for dispatching this task.
    ///
    /// Remote actors declare executor traits; exact subset matching determines
    /// routing eligibility. These labels grant no authorization.
    ///
    /// When `Some`, the list must be non-empty. When `None`, any executor can
    /// handle the task.
    #[cfg_attr(feature = "serde", serde(default))]
    required_executor_traits: Option<ExecutorTraits>,
}

impl TaskConstraints {
    /// Creates constraints with validation of invariant-sensitive fields.
    pub fn new(
        max_attempts: u32,
        timeout_secs: Option<u64>,
        concurrency_key: Option<String>,
    ) -> Result<Self, TaskConstraintsError> {
        let constraints = Self {
            max_attempts,
            timeout_secs,
            concurrency_key,
            concurrency_key_hold_policy: ConcurrencyKeyHoldPolicy::default(),
            concurrency_key_wait_policy: ConcurrencyKeyWaitPolicy::default(),
            safety_level: SafetyLevel::default(),
            required_executor_traits: None,
        };
        constraints.validate()?;
        Ok(constraints)
    }

    /// Validates this constraints object against core invariants.
    pub fn validate(&self) -> Result<(), TaskConstraintsError> {
        if self.max_attempts == 0 {
            return Err(TaskConstraintsError::InvalidMaxAttempts { max_attempts: 0 });
        }
        if self.timeout_secs == Some(0) {
            return Err(TaskConstraintsError::ZeroTimeout);
        }
        if let Some(ref key) = self.concurrency_key {
            if key.is_empty() {
                return Err(TaskConstraintsError::EmptyConcurrencyKey);
            }
        }
        Ok(())
    }

    /// Returns the maximum number of attempts allowed for each run.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the optional per-attempt timeout in seconds.
    pub fn timeout_secs(&self) -> Option<u64> {
        self.timeout_secs
    }

    /// Returns the optional concurrency key.
    pub fn concurrency_key(&self) -> Option<&str> {
        self.concurrency_key.as_deref()
    }

    /// Sets the attempt cap with invariant validation.
    pub fn set_max_attempts(&mut self, max_attempts: u32) -> Result<(), TaskConstraintsError> {
        if max_attempts == 0 {
            return Err(TaskConstraintsError::InvalidMaxAttempts { max_attempts });
        }

        self.max_attempts = max_attempts;
        Ok(())
    }

    /// Sets the optional timeout in seconds.
    ///
    /// # Errors
    ///
    /// Returns [`TaskConstraintsError::ZeroTimeout`] if `timeout_secs` is `Some(0)`.
    pub fn set_timeout_secs(
        &mut self,
        timeout_secs: Option<u64>,
    ) -> Result<(), TaskConstraintsError> {
        if timeout_secs == Some(0) {
            return Err(TaskConstraintsError::ZeroTimeout);
        }
        self.timeout_secs = timeout_secs;
        Ok(())
    }

    /// Sets the optional concurrency key.
    ///
    /// # Errors
    ///
    /// Returns [`TaskConstraintsError::EmptyConcurrencyKey`] if the key is `Some("")`.
    pub fn set_concurrency_key(
        &mut self,
        concurrency_key: Option<String>,
    ) -> Result<(), TaskConstraintsError> {
        if let Some(ref key) = concurrency_key {
            if key.is_empty() {
                return Err(TaskConstraintsError::EmptyConcurrencyKey);
            }
        }
        self.concurrency_key = concurrency_key;
        Ok(())
    }

    /// Returns the concurrency key hold policy.
    pub fn concurrency_key_hold_policy(&self) -> ConcurrencyKeyHoldPolicy {
        self.concurrency_key_hold_policy
    }

    /// Sets the concurrency key hold policy.
    pub fn set_concurrency_key_hold_policy(&mut self, policy: ConcurrencyKeyHoldPolicy) {
        self.concurrency_key_hold_policy = policy;
    }

    /// Returns the safety level classification for this task.
    pub fn safety_level(&self) -> SafetyLevel {
        self.safety_level
    }

    /// Sets the safety level classification.
    pub fn set_safety_level(&mut self, safety_level: SafetyLevel) {
        self.safety_level = safety_level;
    }

    /// Returns the concurrency-key policy for continuation waits.
    ///
    pub fn concurrency_key_wait_policy(&self) -> ConcurrencyKeyWaitPolicy {
        self.concurrency_key_wait_policy
    }

    /// Sets the durable continuation key policy.
    pub fn set_concurrency_key_wait_policy(&mut self, policy: ConcurrencyKeyWaitPolicy) {
        self.concurrency_key_wait_policy = policy;
    }

    /// Returns the required executor traits, if any.
    ///
    /// These requirements select executors; they grant no queue permission.
    pub fn required_executor_traits(&self) -> Option<&ExecutorTraits> {
        self.required_executor_traits.as_ref()
    }

    /// Attaches required executor traits, returning the modified constraints.
    ///
    /// The list must be non-empty; an empty vec is rejected at validation.
    pub fn with_required_executor_traits(
        mut self,
        traits: Vec<String>,
    ) -> Result<Self, TaskConstraintsError> {
        self.set_required_executor_traits(Some(traits))?;
        Ok(self)
    }

    /// Sets routing requirements atomically, rejecting invalid or empty lists.
    pub fn set_required_executor_traits(
        &mut self,
        traits: Option<Vec<String>>,
    ) -> Result<(), TaskConstraintsError> {
        let validated = traits
            .map(|values| {
                ExecutorTraits::new(values).map_err(TaskConstraintsError::InvalidExecutorTrait)
            })
            .transpose()?;
        self.required_executor_traits = validated;
        Ok(())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for TaskConstraints {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TaskConstraintsWire {
            max_attempts: u32,
            timeout_secs: Option<u64>,
            concurrency_key: Option<String>,
            #[serde(default)]
            concurrency_key_hold_policy: ConcurrencyKeyHoldPolicy,
            #[serde(default)]
            concurrency_key_wait_policy: ConcurrencyKeyWaitPolicy,
            #[serde(default)]
            safety_level: SafetyLevel,
            #[serde(default)]
            required_executor_traits: Option<ExecutorTraits>,
        }

        let wire = TaskConstraintsWire::deserialize(deserializer)?;
        let constraints = TaskConstraints {
            max_attempts: wire.max_attempts,
            timeout_secs: wire.timeout_secs,
            concurrency_key: wire.concurrency_key,
            concurrency_key_hold_policy: wire.concurrency_key_hold_policy,
            concurrency_key_wait_policy: wire.concurrency_key_wait_policy,
            safety_level: wire.safety_level,
            required_executor_traits: wire.required_executor_traits,
        };
        constraints.validate().map_err(serde::de::Error::custom)?;
        Ok(constraints)
    }
}

#[cfg(feature = "testing")]
impl TaskConstraints {
    /// Creates constraints bypassing validation — for testing only.
    ///
    /// This allows constructing constraints with values (e.g., zero timeout)
    /// that are useful in test fixtures but rejected by production validation.
    pub fn new_for_testing(
        max_attempts: u32,
        timeout_secs: Option<u64>,
        concurrency_key: Option<String>,
    ) -> Self {
        Self {
            max_attempts,
            timeout_secs,
            concurrency_key,
            concurrency_key_hold_policy: ConcurrencyKeyHoldPolicy::default(),
            concurrency_key_wait_policy: ConcurrencyKeyWaitPolicy::default(),
            safety_level: SafetyLevel::default(),
            required_executor_traits: None,
        }
    }
}

impl Default for TaskConstraints {
    fn default() -> Self {
        Self::new(1, None, None).expect("default TaskConstraints must be valid")
    }
}

/// Concurrency-key policy for a continuation wait (ADR-009).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConcurrencyKeyWaitPolicy {
    /// Release at Awaiting and reacquire through ordinary eligibility on wake.
    #[default]
    ReleaseWhileAwaiting,
    /// Keep the key until the continuation resolves.
    HoldWhileAwaiting,
}

impl ConcurrencyKeyWaitPolicy {
    /// Whether entering Awaiting releases the held concurrency key.
    pub fn releases_while_awaiting(self) -> bool {
        match self {
            Self::ReleaseWhileAwaiting => true,
            Self::HoldWhileAwaiting => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskConstraints, TaskConstraintsError};

    #[test]
    fn rejects_zero_max_attempts_at_construction() {
        let result = TaskConstraints::new(0, Some(10), None);
        assert_eq!(result, Err(TaskConstraintsError::InvalidMaxAttempts { max_attempts: 0 }));
    }

    #[test]
    fn set_max_attempts_rejects_zero_without_mutation() {
        let mut constraints = TaskConstraints::default();
        let original = constraints.clone();

        let result = constraints.set_max_attempts(0);
        assert_eq!(result, Err(TaskConstraintsError::InvalidMaxAttempts { max_attempts: 0 }));
        assert_eq!(constraints, original);
    }

    #[test]
    fn rejects_empty_concurrency_key_at_construction() {
        let result = TaskConstraints::new(1, None, Some(String::new()));
        assert_eq!(result, Err(TaskConstraintsError::EmptyConcurrencyKey));
    }

    #[test]
    fn accepts_none_concurrency_key() {
        let result = TaskConstraints::new(1, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn accepts_valid_concurrency_key() {
        let result = TaskConstraints::new(1, None, Some("valid-key".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().concurrency_key(), Some("valid-key"));
    }

    #[test]
    fn set_concurrency_key_rejects_empty() {
        let mut constraints = TaskConstraints::default();
        let result = constraints.set_concurrency_key(Some(String::new()));
        assert_eq!(result, Err(TaskConstraintsError::EmptyConcurrencyKey));
        assert_eq!(constraints.concurrency_key(), None); // unchanged
    }

    #[test]
    fn set_concurrency_key_accepts_valid() {
        let mut constraints = TaskConstraints::default();
        constraints.set_concurrency_key(Some("key".to_string())).unwrap();
        assert_eq!(constraints.concurrency_key(), Some("key"));
    }

    #[test]
    fn set_concurrency_key_accepts_none() {
        let mut constraints = TaskConstraints::new(1, None, Some("key".to_string())).unwrap();
        constraints.set_concurrency_key(None).unwrap();
        assert_eq!(constraints.concurrency_key(), None);
    }

    #[test]
    fn rejects_zero_timeout_at_construction() {
        let result = TaskConstraints::new(1, Some(0), None);
        assert_eq!(result, Err(TaskConstraintsError::ZeroTimeout));
    }

    #[test]
    fn set_timeout_secs_rejects_zero() {
        let mut constraints = TaskConstraints::default();
        let result = constraints.set_timeout_secs(Some(0));
        assert_eq!(result, Err(TaskConstraintsError::ZeroTimeout));
        assert_eq!(constraints.timeout_secs(), None); // unchanged
    }

    #[test]
    fn set_timeout_secs_accepts_none() {
        let mut constraints = TaskConstraints::new(1, Some(30), None).unwrap();
        constraints.set_timeout_secs(None).unwrap();
        assert_eq!(constraints.timeout_secs(), None);
    }

    #[test]
    fn set_timeout_secs_accepts_positive_value() {
        let mut constraints = TaskConstraints::default();
        constraints.set_timeout_secs(Some(60)).unwrap();
        assert_eq!(constraints.timeout_secs(), Some(60));
    }

    #[test]
    fn accepts_valid_timeout_at_construction() {
        let result = TaskConstraints::new(1, Some(30), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().timeout_secs(), Some(30));
    }

    #[test]
    fn with_required_executor_traits_rejects_empty_vec() {
        let constraints = TaskConstraints::default();
        let result = constraints.with_required_executor_traits(vec![]);
        assert_eq!(
            result,
            Err(TaskConstraintsError::InvalidExecutorTrait(
                crate::executor::ExecutorTraitError::Collection { count: 0 }
            ))
        );
    }

    #[test]
    fn with_required_executor_traits_accepts_non_empty_vec() {
        let constraints = TaskConstraints::default();
        let result = constraints.with_required_executor_traits(vec!["gpu".to_string()]);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().required_executor_traits(),
            Some(&crate::executor::ExecutorTraits::new(vec!["gpu".to_string()]).unwrap())
        );
    }

    #[test]
    fn validate_passes_for_default_and_valid_executor_traits() {
        // Verify validate() passes on default constraints (no executor traits)
        // and on constraints with valid non-empty executor trait entries.
        let mut constraints = TaskConstraints::default();
        assert!(constraints.validate().is_ok());
        constraints = constraints.with_required_executor_traits(vec!["cap1".to_string()]).unwrap();
        assert!(constraints.validate().is_ok());
    }

    #[test]
    fn with_required_executor_traits_rejects_empty_string_entry() {
        let constraints = TaskConstraints::default();
        let result =
            constraints.with_required_executor_traits(vec!["gpu".to_string(), String::new()]);
        assert_eq!(
            result,
            Err(TaskConstraintsError::InvalidExecutorTrait(
                crate::executor::ExecutorTraitError::Label {
                    index: 1,
                    source: crate::bounded::BoundedValueError::Empty,
                }
            ))
        );
    }
}
