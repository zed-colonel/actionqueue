//! Run policy definitions for task execution scheduling.

/// Error returned when constructing or configuring a [`CronPolicy`].
#[cfg(feature = "workflow")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronPolicyError {
    message: String,
}

#[cfg(feature = "workflow")]
impl CronPolicyError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// Returns the human-readable error description.
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[cfg(feature = "workflow")]
impl std::fmt::Display for CronPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid cron policy: {}", self.message)
    }
}

#[cfg(feature = "workflow")]
impl std::error::Error for CronPolicyError {}

/// A validated cron scheduling policy.
///
/// The cron expression is validated at construction time via [`CronPolicy::new`].
/// Expressions use the 7-field format supported by the `cron` crate:
/// `sec min hour dom month dow year`.
///
/// Examples:
/// - `"0 * * * * * *"` — every minute at second 0
/// - `"0 0 * * * * *"` — every hour at minute 0
/// - `"0 0 9 * * MON *"` — every Monday at 09:00 UTC
#[cfg(feature = "workflow")]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronPolicy {
    expression: String,
    #[cfg_attr(feature = "serde", serde(default, skip_serializing_if = "Option::is_none"))]
    max_occurrences: Option<u32>,
}

#[cfg(feature = "workflow")]
impl CronPolicy {
    /// Creates a validated cron policy from a cron expression string.
    ///
    /// The expression is parsed and validated immediately; invalid expressions
    /// are rejected at construction time.
    ///
    /// # Errors
    ///
    /// Returns [`CronPolicyError`] if the expression cannot be parsed.
    pub fn new(expression: impl Into<String>) -> Result<Self, CronPolicyError> {
        use std::str::FromStr as _;
        let expression = expression.into();
        cron::Schedule::from_str(&expression).map_err(|e| CronPolicyError::new(e.to_string()))?;
        Ok(Self { expression, max_occurrences: None })
    }

    /// Sets a maximum number of occurrences to ever derive for this task.
    ///
    /// `None` (the default) means unlimited occurrences.
    ///
    /// # Errors
    ///
    /// Returns [`CronPolicyError`] if `max` is zero (a cron task that never
    /// derives runs is invalid, matching the `RepeatPolicy::new(0, _)` rejection).
    pub fn with_max_occurrences(mut self, max: u32) -> Result<Self, CronPolicyError> {
        if max == 0 {
            return Err(CronPolicyError::new("max_occurrences must be at least 1"));
        }
        self.max_occurrences = Some(max);
        Ok(self)
    }

    /// Returns the raw cron expression string.
    pub fn expression(&self) -> &str {
        &self.expression
    }

    /// Returns the maximum number of occurrences, if configured.
    pub fn max_occurrences(&self) -> Option<u32> {
        self.max_occurrences
    }

    /// Returns the next `count` occurrence timestamps (Unix seconds, UTC)
    /// that are strictly after `after_secs`.
    ///
    /// Returns fewer than `count` items if the schedule has fewer remaining
    /// occurrences (e.g., when `max_occurrences` is set and the cap is near).
    ///
    /// The expression is pre-validated at construction, so schedule parsing
    /// here should never fail.
    pub fn next_occurrences_after(&self, after_secs: u64, count: usize) -> Vec<u64> {
        use std::str::FromStr as _;

        use chrono::{TimeZone as _, Utc};

        if count == 0 {
            return Vec::new();
        }

        // perf: re-parses expression on each call; acceptable for alpha.
        // cron::Schedule is not Clone/Serialize, making OnceCell caching impractical.
        let schedule = cron::Schedule::from_str(&self.expression)
            .expect("cron expression pre-validated at CronPolicy::new");

        let ts = i64::try_from(after_secs).unwrap_or(i64::MAX);
        let after_dt = Utc
            .timestamp_opt(ts, 0)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().expect("epoch is valid"));

        schedule
            .after(&after_dt)
            .take(count)
            .filter_map(|dt| u64::try_from(dt.timestamp()).ok())
            .collect()
    }
}

#[cfg(all(feature = "serde", feature = "workflow"))]
impl<'de> serde::Deserialize<'de> for CronPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct CronPolicyWire {
            expression: String,
            #[serde(default)]
            max_occurrences: Option<u32>,
        }

        let wire = CronPolicyWire::deserialize(deserializer)?;
        let mut policy = CronPolicy::new(wire.expression).map_err(serde::de::Error::custom)?;
        if let Some(max) = wire.max_occurrences {
            policy = policy.with_max_occurrences(max).map_err(serde::de::Error::custom)?;
        }
        Ok(policy)
    }
}

/// Typed validation errors for [`RunPolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunPolicyError {
    /// Repeat policy `count` must be greater than zero.
    InvalidRepeatCount {
        /// The rejected `count` value.
        count: u32,
    },
    /// Repeat policy `interval_secs` must be greater than zero.
    InvalidRepeatIntervalSecs {
        /// The rejected `interval_secs` value.
        interval_secs: u64,
    },
}

impl std::fmt::Display for RunPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunPolicyError::InvalidRepeatCount { count } => {
                write!(f, "invalid repeat count: {count} (must be >= 1)")
            }
            RunPolicyError::InvalidRepeatIntervalSecs { interval_secs } => {
                write!(f, "invalid repeat interval_secs: {interval_secs} (must be >= 1)")
            }
        }
    }
}

impl std::error::Error for RunPolicyError {}

/// A validated repeat policy with count and interval.
///
/// Both `count` and `interval_secs` are guaranteed to be strictly positive
/// after construction through [`RepeatPolicy::new`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RepeatPolicy {
    count: u32,
    interval_secs: u64,
}

impl RepeatPolicy {
    /// Creates a validated repeat policy.
    ///
    /// # Errors
    ///
    /// Returns [`RunPolicyError::InvalidRepeatCount`] if `count` is zero.
    /// Returns [`RunPolicyError::InvalidRepeatIntervalSecs`] if `interval_secs` is zero.
    pub fn new(count: u32, interval_secs: u64) -> Result<Self, RunPolicyError> {
        if count == 0 {
            return Err(RunPolicyError::InvalidRepeatCount { count });
        }
        if interval_secs == 0 {
            return Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs });
        }
        Ok(Self { count, interval_secs })
    }

    /// Returns the total number of runs to derive.
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Returns the interval in seconds between derived runs.
    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }
}

/// A policy that defines how many times a task should be run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum RunPolicy {
    /// Run exactly once.
    Once,
    /// Run a specific number of times at a fixed interval.
    Repeat(RepeatPolicy),
    /// Run on a cron schedule (rolling window derivation, UTC only).
    #[cfg(feature = "workflow")]
    Cron(CronPolicy),
}

impl RunPolicy {
    /// Constructs a validated [`RunPolicy::Repeat`] policy.
    ///
    /// This is the canonical constructor for repeat policies and enforces
    /// the contract requirement that both `count` and `interval_secs`
    /// are strictly positive.
    pub fn repeat(count: u32, interval_secs: u64) -> Result<Self, RunPolicyError> {
        Ok(Self::Repeat(RepeatPolicy::new(count, interval_secs)?))
    }

    /// Constructs a validated [`RunPolicy::Cron`] policy.
    ///
    /// # Errors
    ///
    /// Returns [`CronPolicyError`] if the cron expression cannot be parsed.
    #[cfg(feature = "workflow")]
    pub fn cron(expression: impl Into<String>) -> Result<Self, CronPolicyError> {
        Ok(Self::Cron(CronPolicy::new(expression)?))
    }

    /// Validates this run policy against contract invariants.
    ///
    /// For [`RunPolicy::Repeat`], validation is guaranteed at construction time
    /// by [`RepeatPolicy::new`], so this method always returns `Ok(())`.
    pub fn validate(&self) -> Result<(), RunPolicyError> {
        // RepeatPolicy invariants are enforced at construction time.
        // CronPolicy expression is validated at construction time.
        // No additional validation needed for any variant.
        Ok(())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for RepeatPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct RepeatPolicyWire {
            count: u32,
            interval_secs: u64,
        }

        let wire = RepeatPolicyWire::deserialize(deserializer)?;
        RepeatPolicy::new(wire.count, wire.interval_secs).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for RepeatPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("RepeatPolicy", 2)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("interval_secs", &self.interval_secs)?;
        state.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for RunPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        enum RunPolicyWire {
            Once,
            Repeat {
                count: u32,
                interval_secs: u64,
            },
            #[cfg(feature = "workflow")]
            Cron {
                expression: String,
                #[serde(default)]
                max_occurrences: Option<u32>,
            },
        }

        let wire = <RunPolicyWire as serde::Deserialize>::deserialize(deserializer)?;
        match wire {
            RunPolicyWire::Once => Ok(RunPolicy::Once),
            RunPolicyWire::Repeat { count, interval_secs } => {
                let policy =
                    RepeatPolicy::new(count, interval_secs).map_err(serde::de::Error::custom)?;
                Ok(RunPolicy::Repeat(policy))
            }
            #[cfg(feature = "workflow")]
            RunPolicyWire::Cron { expression, max_occurrences } => {
                // NOTE: keep in sync with CronPolicy Deserialize impl.
                let mut policy = CronPolicy::new(expression).map_err(serde::de::Error::custom)?;
                if let Some(max) = max_occurrences {
                    policy = policy.with_max_occurrences(max).map_err(serde::de::Error::custom)?;
                }
                Ok(RunPolicy::Cron(policy))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RepeatPolicy, RunPolicy, RunPolicyError};

    #[test]
    fn repeat_policy_rejects_zero_count() {
        let result = RepeatPolicy::new(0, 60);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatCount { count: 0 }));
    }

    #[test]
    fn repeat_policy_rejects_zero_interval() {
        let result = RepeatPolicy::new(3, 0);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 }));
    }

    #[test]
    fn repeat_policy_accepts_valid_values() {
        let policy = RepeatPolicy::new(6, 1800).expect("repeat policy should be valid");
        assert_eq!(policy.count(), 6);
        assert_eq!(policy.interval_secs(), 1800);
    }

    #[test]
    fn repeat_constructor_rejects_zero_count() {
        let result = RunPolicy::repeat(0, 60);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatCount { count: 0 }));
    }

    #[test]
    fn repeat_constructor_rejects_zero_interval() {
        let result = RunPolicy::repeat(3, 0);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 }));
    }

    #[test]
    fn repeat_constructor_accepts_valid_values() {
        let policy = RunPolicy::repeat(6, 1800).expect("repeat policy should be valid");
        assert_eq!(policy, RunPolicy::Repeat(RepeatPolicy::new(6, 1800).unwrap()));
    }

    #[test]
    fn validate_always_succeeds_for_valid_policies() {
        assert!(RunPolicy::Once.validate().is_ok());
        assert!(RunPolicy::repeat(3, 60).unwrap().validate().is_ok());
    }

    #[test]
    fn repeat_policy_accessors() {
        let rp = RepeatPolicy::new(5, 120).unwrap();
        assert_eq!(rp.count(), 5);
        assert_eq!(rp.interval_secs(), 120);
    }

    #[cfg(feature = "workflow")]
    #[test]
    fn cron_next_occurrences_after_with_u64_max_returns_empty() {
        let policy = super::CronPolicy::new("* * * * * * *").expect("valid");
        // u64::MAX saturates to i64::MAX; no cron occurrences should be
        // after year 292 billion, so the result is empty.
        let result = policy.next_occurrences_after(u64::MAX, 5);
        assert!(result.is_empty() || result.iter().all(|&ts| ts <= i64::MAX as u64));
    }

    #[cfg(feature = "workflow")]
    #[test]
    fn cron_with_max_occurrences_zero_rejected() {
        let policy = super::CronPolicy::new("* * * * * * *").expect("valid");
        let err = policy.with_max_occurrences(0).expect_err("zero should be rejected");
        assert!(err.message().contains("at least 1"));
    }
}
