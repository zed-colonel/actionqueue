//! Run metrics population from authoritative projection and daemon clock.
//!
//! This module updates run-focused Prometheus metric families from the current
//! authoritative in-memory projection and the router state's injected daemon
//! clock. It does not introduce inferred counters or ad hoc wall-clock reads.

use actionqueue_core::run::state::RunState;

/// Recomputes run-focused metric families from the authoritative projection.
///
/// Update behavior is deterministic for each scrape:
/// - Recounts all run states from `ReplayReducer::run_instances()`.
/// - Overwrites `actionqueue_runs_total{state=...}` gauge samples.
/// - Overwrites aggregate gauges `actionqueue_runs_ready` and
///   `actionqueue_runs_running`.
/// - Observes scheduling lag histogram samples for lag-eligible states only.
pub fn update(state: &crate::http::RouterStateInner) {
    let collectors = state.metrics.collectors();
    let mut counts = RunStateCounts::default();

    let now_unix_seconds = super::lag_now(state);

    let projection = match state.shared_projection.read() {
        Ok(guard) => guard,
        Err(_) => {
            tracing::error!("shared projection RwLock poisoned — skipping run metrics update");
            return;
        }
    };
    for run in projection.run_instances() {
        let run_state = run.state();
        counts.increment(run_state);

        if is_lag_eligible(run_state) {
            let lag_seconds = now_unix_seconds.saturating_sub(run.scheduled_at());
            collectors.scheduling_lag_seconds().observe(lag_seconds as f64);
        }
    }

    for run_state in ALL_RUN_STATES {
        collectors
            .runs_total()
            .with_label_values(&[state_label(run_state)])
            .set(counts.get(run_state) as f64);
    }

    collectors.runs_ready().set(counts.get(RunState::Ready) as f64);
    collectors.runs_running().set(counts.get(RunState::Running) as f64);
}

const ALL_RUN_STATES: [RunState; 8] = [
    RunState::Scheduled,
    RunState::Ready,
    RunState::Leased,
    RunState::Running,
    RunState::RetryWait,
    RunState::Completed,
    RunState::Failed,
    RunState::Canceled,
];

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RunStateCounts {
    scheduled: u64,
    ready: u64,
    leased: u64,
    running: u64,
    retry_wait: u64,
    completed: u64,
    failed: u64,
    canceled: u64,
}

impl RunStateCounts {
    fn increment(&mut self, state: RunState) {
        match state {
            RunState::Scheduled => self.scheduled += 1,
            RunState::Ready => self.ready += 1,
            RunState::Leased => self.leased += 1,
            RunState::Running => self.running += 1,
            RunState::RetryWait => self.retry_wait += 1,
            RunState::Suspended => {}
            RunState::Completed => self.completed += 1,
            RunState::Failed => self.failed += 1,
            RunState::Canceled => self.canceled += 1,
        }
    }

    fn get(&self, state: RunState) -> u64 {
        match state {
            RunState::Scheduled => self.scheduled,
            RunState::Ready => self.ready,
            RunState::Leased => self.leased,
            RunState::Running => self.running,
            RunState::RetryWait => self.retry_wait,
            RunState::Suspended => 0,
            RunState::Completed => self.completed,
            RunState::Failed => self.failed,
            RunState::Canceled => self.canceled,
        }
    }
}

fn state_label(state: RunState) -> &'static str {
    match state {
        RunState::Scheduled => "scheduled",
        RunState::Ready => "ready",
        RunState::Leased => "leased",
        RunState::Running => "running",
        RunState::RetryWait => "retry_wait",
        RunState::Suspended => "suspended",
        RunState::Completed => "completed",
        RunState::Failed => "failed",
        RunState::Canceled => "canceled",
    }
}

fn is_lag_eligible(state: RunState) -> bool {
    matches!(state, RunState::Ready | RunState::Leased | RunState::Running | RunState::RetryWait)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_core::ids::{RunId, TaskId};
    use actionqueue_core::run::run_instance::RunInstance;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::event::{WalEvent, WalEventType};
    use actionqueue_storage::wal::WalAppendTelemetry;

    use super::{is_lag_eligible, state_label, update};
    use crate::bootstrap::{ReadyStatus, RouterConfig};
    use crate::metrics::registry::MetricsRegistry;
    use crate::time::clock::{MockClock, SharedDaemonClock};

    fn build_task_spec(task_id: TaskId) -> TaskSpec {
        TaskSpec::new(
            task_id,
            TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("task spec should be valid")
    }

    fn apply_event(reducer: &mut ReplayReducer, sequence: u64, event: WalEventType) {
        let event = WalEvent::new(sequence, event);
        reducer.apply(&event).expect("event should apply");
    }

    fn run_instance_scheduled(run_id: RunId, task_id: TaskId, scheduled_at: u64) -> RunInstance {
        RunInstance::new_scheduled_with_id(run_id, task_id, scheduled_at, scheduled_at)
            .expect("run instance should be valid")
    }

    #[allow(clippy::too_many_arguments)] // Test helper with naturally many parameters
    fn seed_run_state(
        reducer: &mut ReplayReducer,
        sequence: &mut u64,
        run_id: RunId,
        task_id: TaskId,
        target_state: actionqueue_core::run::state::RunState,
        scheduled_at: u64,
    ) {
        fn transition_state(
            reducer: &mut ReplayReducer,
            sequence: &mut u64,
            run_id: RunId,
            from: actionqueue_core::run::state::RunState,
            to: actionqueue_core::run::state::RunState,
        ) {
            apply_event(
                reducer,
                *sequence,
                WalEventType::RunStateChanged {
                    run_id,
                    previous_state: from,
                    new_state: to,
                    timestamp: *sequence + 1_000,
                },
            );
            *sequence += 1;
        }

        apply_event(
            reducer,
            *sequence,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_id, task_id, scheduled_at),
            },
        );
        *sequence += 1;

        use actionqueue_core::run::state::RunState;
        match target_state {
            RunState::Scheduled => {}
            RunState::Ready => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready)
            }
            RunState::Leased => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
            }
            RunState::Running => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
                transition_state(reducer, sequence, run_id, RunState::Leased, RunState::Running);
            }
            RunState::RetryWait => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
                transition_state(reducer, sequence, run_id, RunState::Leased, RunState::Running);
                transition_state(reducer, sequence, run_id, RunState::Running, RunState::RetryWait);
            }
            RunState::Completed => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
                transition_state(reducer, sequence, run_id, RunState::Leased, RunState::Running);
                transition_state(reducer, sequence, run_id, RunState::Running, RunState::Completed);
            }
            RunState::Failed => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
                transition_state(reducer, sequence, run_id, RunState::Leased, RunState::Running);
                transition_state(reducer, sequence, run_id, RunState::Running, RunState::Failed);
            }
            RunState::Canceled => {
                apply_event(
                    reducer,
                    *sequence,
                    WalEventType::RunCanceled { run_id, timestamp: *sequence + 1_000 },
                );
                *sequence += 1;
            }
            RunState::Suspended => {
                transition_state(reducer, sequence, run_id, RunState::Scheduled, RunState::Ready);
                transition_state(reducer, sequence, run_id, RunState::Ready, RunState::Leased);
                transition_state(reducer, sequence, run_id, RunState::Leased, RunState::Running);
                transition_state(reducer, sequence, run_id, RunState::Running, RunState::Suspended);
            }
        }
    }

    fn build_state(
        projection: ReplayReducer,
        metrics: Arc<MetricsRegistry>,
        now_unix_seconds: u64,
    ) -> crate::http::RouterStateInner {
        let clock: SharedDaemonClock = Arc::new(MockClock::new(now_unix_seconds));
        crate::http::RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: false },
            Arc::new(std::sync::RwLock::new(projection)),
            crate::http::RouterObservability {
                metrics,
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock,
                recovery_observations: RecoveryObservations::zero(),
            },
            ReadyStatus::ready(),
        )
    }

    fn run_total_value(
        metrics: &MetricsRegistry,
        state: actionqueue_core::run::state::RunState,
    ) -> f64 {
        metrics.collectors().runs_total().with_label_values(&[state_label(state)]).get()
    }

    #[test]
    fn state_to_label_mapping_covers_all_run_states() {
        use actionqueue_core::run::state::RunState;

        assert_eq!(state_label(RunState::Scheduled), "scheduled");
        assert_eq!(state_label(RunState::Ready), "ready");
        assert_eq!(state_label(RunState::Leased), "leased");
        assert_eq!(state_label(RunState::Running), "running");
        assert_eq!(state_label(RunState::RetryWait), "retry_wait");
        assert_eq!(state_label(RunState::Completed), "completed");
        assert_eq!(state_label(RunState::Failed), "failed");
        assert_eq!(state_label(RunState::Canceled), "canceled");
    }

    #[test]
    fn lag_eligibility_includes_only_non_terminal_readiness_path_states() {
        use actionqueue_core::run::state::RunState;

        assert!(!is_lag_eligible(RunState::Scheduled));
        assert!(is_lag_eligible(RunState::Ready));
        assert!(is_lag_eligible(RunState::Leased));
        assert!(is_lag_eligible(RunState::Running));
        assert!(is_lag_eligible(RunState::RetryWait));
        assert!(!is_lag_eligible(RunState::Completed));
        assert!(!is_lag_eligible(RunState::Failed));
        assert!(!is_lag_eligible(RunState::Canceled));
    }

    #[test]
    fn update_overwrites_gauges_for_repeated_projection_snapshots() {
        use actionqueue_core::run::state::RunState;

        let task_id = TaskId::new();
        let mut projection_one = ReplayReducer::new();
        apply_event(
            &mut projection_one,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );
        let mut sequence = 2;
        seed_run_state(
            &mut projection_one,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Ready,
            100,
        );
        seed_run_state(
            &mut projection_one,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Completed,
            100,
        );

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state_one = build_state(projection_one, Arc::clone(&metrics), 200);
        update(&state_one);

        assert_eq!(run_total_value(&metrics, RunState::Ready), 1.0);
        assert_eq!(run_total_value(&metrics, RunState::Completed), 1.0);

        let mut projection_two = ReplayReducer::new();
        apply_event(
            &mut projection_two,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );
        let mut sequence = 2;
        seed_run_state(
            &mut projection_two,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Running,
            150,
        );

        let state_two = build_state(projection_two, Arc::clone(&metrics), 300);
        update(&state_two);

        assert_eq!(run_total_value(&metrics, RunState::Ready), 0.0);
        assert_eq!(run_total_value(&metrics, RunState::Completed), 0.0);
        assert_eq!(run_total_value(&metrics, RunState::Running), 1.0);
    }

    #[test]
    fn ready_and_running_aggregates_match_state_counts() {
        use actionqueue_core::run::state::RunState;

        let task_id = TaskId::new();
        let mut projection = ReplayReducer::new();
        apply_event(
            &mut projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );

        let mut sequence = 2;
        seed_run_state(&mut projection, &mut sequence, RunId::new(), task_id, RunState::Ready, 100);
        seed_run_state(&mut projection, &mut sequence, RunId::new(), task_id, RunState::Ready, 100);
        seed_run_state(
            &mut projection,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Running,
            100,
        );
        seed_run_state(
            &mut projection,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Running,
            100,
        );
        seed_run_state(
            &mut projection,
            &mut sequence,
            RunId::new(),
            task_id,
            RunState::Scheduled,
            100,
        );

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(projection, Arc::clone(&metrics), 300);
        update(&state);

        assert_eq!(metrics.collectors().runs_ready().get(), 2.0);
        assert_eq!(metrics.collectors().runs_running().get(), 2.0);
        assert_eq!(run_total_value(&metrics, RunState::Ready), 2.0);
        assert_eq!(run_total_value(&metrics, RunState::Running), 2.0);
    }

    #[test]
    fn lag_observation_uses_saturating_subtraction() {
        use actionqueue_core::run::state::RunState;

        let task_id = TaskId::new();
        let mut projection = ReplayReducer::new();
        apply_event(
            &mut projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );

        let mut sequence = 2;
        seed_run_state(&mut projection, &mut sequence, RunId::new(), task_id, RunState::Ready, 200);

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(projection, Arc::clone(&metrics), 100);
        update(&state);

        assert_eq!(metrics.collectors().scheduling_lag_seconds().get_sample_count(), 1);
        assert_eq!(metrics.collectors().scheduling_lag_seconds().get_sample_sum(), 0.0);
    }

    #[test]
    fn lag_observation_uses_router_state_clock_value() {
        use actionqueue_core::run::state::RunState;

        let task_id = TaskId::new();
        let mut projection = ReplayReducer::new();
        apply_event(
            &mut projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );
        let mut sequence = 2;
        seed_run_state(&mut projection, &mut sequence, RunId::new(), task_id, RunState::Ready, 40);

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));

        let state_at_50 = build_state(projection.clone(), Arc::clone(&metrics), 50);
        update(&state_at_50);
        let sample_count_after_first =
            metrics.collectors().scheduling_lag_seconds().get_sample_count();
        let sample_sum_after_first = metrics.collectors().scheduling_lag_seconds().get_sample_sum();

        let state_at_90 = build_state(projection, Arc::clone(&metrics), 90);
        update(&state_at_90);
        let sample_count_after_second =
            metrics.collectors().scheduling_lag_seconds().get_sample_count();
        let sample_sum_after_second =
            metrics.collectors().scheduling_lag_seconds().get_sample_sum();

        assert_eq!(sample_count_after_first, 1);
        assert_eq!(sample_sum_after_first, 10.0);
        assert_eq!(sample_count_after_second, 2);
        assert_eq!(sample_sum_after_second, 60.0);
    }
}
