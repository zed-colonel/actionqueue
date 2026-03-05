//! Cron derivation — rolling window run creation from cron expressions.
//!
//! Unlike `Once` (1 run) and `Repeat` (N runs), cron derivation is incremental.
//! The dispatch loop maintains a rolling window of non-terminal runs. As runs
//! complete, [`derive_cron`] produces the next batch of occurrences.
//!
//! # Window model
//!
//! `derive_cron` creates up to `count` scheduled runs whose `scheduled_at`
//! timestamps are the next cron occurrences strictly after `after_secs`. The
//! caller is responsible for passing `count` as the gap between the current
//! non-terminal run count and the desired window size.
//!
//! # UTC only
//!
//! All occurrences are computed in UTC. The `CronPolicy` expression uses the
//! 7-field format (`sec min hour dom month dow year`) supported by the `cron`
//! crate. Example: `"0 * * * * * *"` fires at second 0 of every minute.

use std::collections::HashMap;
use std::str::FromStr as _;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::task::run_policy::CronPolicy;
use chrono::{TimeZone as _, Utc};

use super::{map_run_construction_error, DerivationError, DerivationSuccess};
use crate::time::clock::Clock;

/// Per-task cache of parsed `cron::Schedule` objects.
///
/// Parsing a cron expression is comparatively expensive. The dispatch loop
/// owns one instance of this cache and passes it into derivation calls so
/// each expression is parsed at most once per task lifetime.
///
/// Two-phase API to avoid borrow-checker conflicts with other `self` fields:
/// 1. Call [`ensure`](Self::ensure) (mutable borrow, ends before next step).
/// 2. Call [`get`](Self::get) (immutable borrow, for passing to derivation).
#[derive(Default)]
pub struct CronScheduleCache {
    cache: HashMap<TaskId, cron::Schedule>,
}

impl CronScheduleCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ensures the schedule for `task_id` is cached, parsing it if absent.
    ///
    /// This is a pure mutation — it does not return a reference, so the
    /// mutable borrow of the cache ends when this call returns.
    pub fn ensure(&mut self, task_id: TaskId, policy: &CronPolicy) {
        self.cache.entry(task_id).or_insert_with(|| {
            cron::Schedule::from_str(policy.expression())
                .expect("cron expression pre-validated at CronPolicy::new")
        });
    }

    /// Returns the cached schedule for `task_id`, or `None` if not yet cached.
    pub fn get(&self, task_id: TaskId) -> Option<&cron::Schedule> {
        self.cache.get(&task_id)
    }

    /// Removes the cached schedule for a task (called at GC time).
    pub fn remove(&mut self, task_id: TaskId) {
        self.cache.remove(&task_id);
    }
}

/// Derives the next `count` cron runs for a task using a pre-parsed schedule.
///
/// Like [`derive_cron`] but avoids re-parsing the cron expression by using
/// the caller-supplied `schedule`. The dispatch loop caches schedules in a
/// [`CronScheduleCache`] and passes them here.
pub fn derive_cron_cached(
    task_id: TaskId,
    schedule: &cron::Schedule,
    after_secs: u64,
    created_at: u64,
    count: u32,
) -> Result<Vec<RunInstance>, DerivationError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let ts = i64::try_from(after_secs).unwrap_or(i64::MAX);
    let after_dt = Utc
        .timestamp_opt(ts, 0)
        .single()
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().expect("epoch is valid"));

    let occurrences: Vec<u64> = schedule
        .after(&after_dt)
        .take(count as usize)
        .filter_map(|dt| u64::try_from(dt.timestamp()).ok())
        .collect();

    let mut result = Vec::with_capacity(occurrences.len());
    for scheduled_at in occurrences {
        let run = RunInstance::new_scheduled(task_id, scheduled_at, created_at)
            .map_err(|source| map_run_construction_error(task_id, source))?;
        result.push(run);
    }

    Ok(result)
}

/// Default number of future cron runs to maintain as a pre-scheduled window.
pub const CRON_WINDOW_SIZE: u32 = 5;

/// Derives the next `count` cron runs for a task.
///
/// Finds cron occurrences strictly after `after_secs` (a Unix timestamp) and
/// creates a [`RunInstance`] for each, using `created_at` as the creation
/// timestamp.
///
/// Returns an empty `Vec` when `count == 0` or when the cron schedule has
/// fewer than `count` upcoming occurrences (open-ended schedules always
/// have more).
///
/// # Errors
///
/// Returns [`DerivationError::RunConstructionFailed`] if a run cannot be
/// constructed (e.g., nil task ID) for any occurrence.
pub fn derive_cron(
    task_id: TaskId,
    policy: &CronPolicy,
    after_secs: u64,
    created_at: u64,
    count: u32,
) -> Result<Vec<RunInstance>, DerivationError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let occurrences = policy.next_occurrences_after(after_secs, count as usize);

    let mut result = Vec::with_capacity(occurrences.len());
    for scheduled_at in occurrences {
        let run = RunInstance::new_scheduled(task_id, scheduled_at, created_at)
            .map_err(|source| map_run_construction_error(task_id, source))?;
        result.push(run);
    }

    Ok(result)
}

/// Derives the initial window of cron runs for a newly submitted task.
///
/// Equivalent to calling [`derive_cron`] with
/// `after_secs = schedule_origin.saturating_sub(1)` (ensures the schedule_origin
/// itself is included in the derivation window) and `count = CRON_WINDOW_SIZE`.
/// Integrated into the standard [`super::derive_runs`] dispatch for cron policies.
pub(super) fn derive_cron_initial(
    clock: &impl Clock,
    task_id: TaskId,
    policy: &CronPolicy,
    already_derived: u32,
    schedule_origin: u64,
) -> Result<DerivationSuccess, DerivationError> {
    // If runs have already been derived (e.g. re-submission after WAL replay),
    // don't derive more here; the rolling window in derive_cron_runs handles it.
    if already_derived > 0 {
        return Ok(DerivationSuccess::new(Vec::new(), already_derived));
    }

    // Cap at max_occurrences if set.
    let window = if let Some(max) = policy.max_occurrences() {
        CRON_WINDOW_SIZE.min(max)
    } else {
        CRON_WINDOW_SIZE
    };

    // Derive the first window of occurrences from schedule_origin onwards.
    let after_secs = schedule_origin.saturating_sub(1);
    let runs = derive_cron(task_id, policy, after_secs, clock.now(), window)?;
    let new_total = runs.len() as u32;
    Ok(DerivationSuccess::new(runs, new_total))
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::task::run_policy::CronPolicy;

    use super::*;
    use crate::time::clock::MockClock;

    fn every_minute_policy() -> CronPolicy {
        // 7-field: sec min hour dom month dow year
        CronPolicy::new("0 * * * * * *").expect("valid cron expression")
    }

    fn every_second_policy() -> CronPolicy {
        CronPolicy::new("* * * * * * *").expect("valid cron expression")
    }

    #[test]
    fn derive_cron_returns_empty_for_count_zero() {
        let task_id = TaskId::new();
        let policy = every_minute_policy();
        let result = derive_cron(task_id, &policy, 0, 1000, 0).expect("should not error");
        assert!(result.is_empty());
    }

    #[test]
    fn derive_cron_returns_expected_count() {
        let task_id = TaskId::new();
        let policy = every_second_policy();
        // Start from Unix second 1000, ask for 3 runs
        let result = derive_cron(task_id, &policy, 1000, 999, 3).expect("should not error");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn derive_cron_runs_are_strictly_after_after_secs() {
        let task_id = TaskId::new();
        let policy = every_second_policy();
        let after_secs = 1000u64;
        let result = derive_cron(task_id, &policy, after_secs, 999, 5).expect("should not error");
        for run in &result {
            assert!(
                run.scheduled_at() > after_secs,
                "scheduled_at {} must be > after_secs {}",
                run.scheduled_at(),
                after_secs
            );
        }
    }

    #[test]
    fn derive_cron_runs_are_monotonically_increasing() {
        let task_id = TaskId::new();
        let policy = every_second_policy();
        let result = derive_cron(task_id, &policy, 0, 0, 5).expect("should not error");
        let times: Vec<u64> = result.iter().map(|r| r.scheduled_at()).collect();
        for i in 1..times.len() {
            assert!(
                times[i] > times[i - 1],
                "occurrence {} ({}) must be after {} ({})",
                i,
                times[i],
                i - 1,
                times[i - 1]
            );
        }
    }

    #[test]
    fn derive_cron_initial_creates_window_on_first_call() {
        let clock = MockClock::new(1_000);
        let task_id = TaskId::new();
        let policy = every_second_policy();

        let result =
            derive_cron_initial(&clock, task_id, &policy, 0, 1_000).expect("should not error");
        assert_eq!(result.derived().len(), CRON_WINDOW_SIZE as usize);
        assert_eq!(result.already_derived(), CRON_WINDOW_SIZE);
    }

    #[test]
    fn derive_cron_initial_is_noop_when_already_derived() {
        let clock = MockClock::new(1_000);
        let task_id = TaskId::new();
        let policy = every_second_policy();

        // already_derived = 5 → no new runs
        let result =
            derive_cron_initial(&clock, task_id, &policy, 5, 1_000).expect("should not error");
        assert!(result.derived().is_empty());
        assert_eq!(result.already_derived(), 5);
    }

    #[test]
    fn derive_cron_initial_respects_max_occurrences() {
        let clock = MockClock::new(1_000);
        let task_id = TaskId::new();
        let policy =
            CronPolicy::new("* * * * * * *").expect("valid").with_max_occurrences(3).expect("ok");

        let result =
            derive_cron_initial(&clock, task_id, &policy, 0, 1_000).expect("should not error");
        // max_occurrences = 3 < CRON_WINDOW_SIZE (5): only 3 runs created
        assert_eq!(result.derived().len(), 3);
        assert_eq!(result.already_derived(), 3);
    }

    #[test]
    fn derive_cron_rejects_nil_task_id() {
        let task_id = "00000000-0000-0000-0000-000000000000"
            .parse::<TaskId>()
            .expect("nil task id literal must parse");
        let policy = every_second_policy();

        let result = derive_cron(task_id, &policy, 0, 0, 1);
        assert!(matches!(result, Err(DerivationError::InvalidTaskIdForRunConstruction { .. })));
    }
}
