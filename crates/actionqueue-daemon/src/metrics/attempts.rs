//! Attempt metrics population from authoritative projection.
//!
//! This updater derives `actionqueue_attempts_total{result=...}` gauge directly
//! from replay projection attempt history, using `.set()` for deterministic
//! absolute values per scrape.

use actionqueue_core::mutation::AttemptResultKind;

/// Recomputes attempt outcome gauges from authoritative projection truth.
pub fn update(state: &crate::http::RouterStateInner) {
    let collectors = state.metrics.collectors();
    let counts = count_attempt_results(state);

    collectors.attempts_total().with_label_values(&["success"]).set(counts.success as f64);
    collectors.attempts_total().with_label_values(&["failure"]).set(counts.failure as f64);
    collectors.attempts_total().with_label_values(&["timeout"]).set(counts.timeout as f64);
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct AttemptResultCounts {
    success: u64,
    failure: u64,
    timeout: u64,
}

fn count_attempt_results(state: &crate::http::RouterStateInner) -> AttemptResultCounts {
    let mut counts = AttemptResultCounts::default();

    let projection = match state.shared_projection.read() {
        Ok(guard) => guard,
        Err(_) => {
            tracing::error!("shared projection RwLock poisoned — skipping attempt metrics update");
            return AttemptResultCounts::default();
        }
    };
    for run in projection.run_instances() {
        if let Some(attempts) = projection.get_attempt_history(&run.id()) {
            for attempt in attempts {
                match attempt.result() {
                    Some(AttemptResultKind::Success) => counts.success += 1,
                    Some(AttemptResultKind::Failure) => counts.failure += 1,
                    Some(AttemptResultKind::Timeout) => counts.timeout += 1,
                    Some(AttemptResultKind::Suspended) => {}
                    None => {}
                }
            }
        }
    }

    counts
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actionqueue_core::ids::{AttemptId, RunId, TaskId};
    use actionqueue_core::mutation::AttemptResultKind;
    use actionqueue_core::run::run_instance::RunInstance;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::RunPolicy;
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_storage::recovery::bootstrap::RecoveryObservations;
    use actionqueue_storage::recovery::reducer::ReplayReducer;
    use actionqueue_storage::wal::event::{WalEvent, WalEventType};
    use actionqueue_storage::wal::WalAppendTelemetry;

    use super::update;
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

    fn run_instance_scheduled(run_id: RunId, task_id: TaskId, at: u64) -> RunInstance {
        RunInstance::new_scheduled_with_id(run_id, task_id, at, at)
            .expect("run instance should be valid")
    }

    fn transition_to_running(reducer: &mut ReplayReducer, run_id: RunId, mut sequence: u64) -> u64 {
        apply_event(
            reducer,
            sequence,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: sequence + 1_000,
            },
        );
        sequence += 1;
        apply_event(
            reducer,
            sequence,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Ready,
                new_state: RunState::Leased,
                timestamp: sequence + 1_000,
            },
        );
        sequence += 1;
        apply_event(
            reducer,
            sequence,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Leased,
                new_state: RunState::Running,
                timestamp: sequence + 1_000,
            },
        );
        sequence + 1
    }

    fn build_state(
        projection: ReplayReducer,
        metrics: Arc<MetricsRegistry>,
    ) -> crate::http::RouterStateInner {
        let clock: SharedDaemonClock = Arc::new(MockClock::new(1_700_000_000));
        crate::http::RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: true },
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

    #[test]
    fn update_maps_success_failure_timeout_and_ignores_unfinished_attempts() {
        let task_id = TaskId::new();
        let mut projection = ReplayReducer::new();
        apply_event(
            &mut projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );

        // Success run
        let run_success = RunId::new();
        let attempt_success = AttemptId::new();
        apply_event(
            &mut projection,
            2,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_success, task_id, 10),
            },
        );
        let mut seq = transition_to_running(&mut projection, run_success, 3);
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptStarted {
                run_id: run_success,
                attempt_id: attempt_success,
                timestamp: 2_000,
            },
        );
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptFinished {
                run_id: run_success,
                attempt_id: attempt_success,
                result: AttemptResultKind::Success,
                error: None,
                output: None,
                timestamp: 2_100,
            },
        );

        // Failure run
        let run_failure = RunId::new();
        let attempt_failure = AttemptId::new();
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_failure, task_id, 11),
            },
        );
        seq = transition_to_running(&mut projection, run_failure, seq + 1);
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptStarted {
                run_id: run_failure,
                attempt_id: attempt_failure,
                timestamp: 3_000,
            },
        );
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptFinished {
                run_id: run_failure,
                attempt_id: attempt_failure,
                result: AttemptResultKind::Failure,
                error: Some("boom".to_string()),
                output: None,
                timestamp: 3_100,
            },
        );

        // Timeout run
        let run_timeout = RunId::new();
        let attempt_timeout = AttemptId::new();
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_timeout, task_id, 12),
            },
        );
        seq = transition_to_running(&mut projection, run_timeout, seq + 1);
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptStarted {
                run_id: run_timeout,
                attempt_id: attempt_timeout,
                timestamp: 4_000,
            },
        );
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptFinished {
                run_id: run_timeout,
                attempt_id: attempt_timeout,
                result: AttemptResultKind::Timeout,
                error: Some("attempt timed out after 5s".to_string()),
                output: None,
                timestamp: 4_100,
            },
        );

        // Unfinished attempt should not count
        let run_unfinished = RunId::new();
        let attempt_unfinished = AttemptId::new();
        seq += 1;
        apply_event(
            &mut projection,
            seq,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_unfinished, task_id, 13),
            },
        );
        seq = transition_to_running(&mut projection, run_unfinished, seq + 1);
        apply_event(
            &mut projection,
            seq,
            WalEventType::AttemptStarted {
                run_id: run_unfinished,
                attempt_id: attempt_unfinished,
                timestamp: 5_000,
            },
        );

        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));
        let state = build_state(projection, Arc::clone(&metrics));
        update(&state);

        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["success"]).get(),
            1.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["failure"]).get(),
            1.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["timeout"]).get(),
            1.0
        );
    }

    #[test]
    fn update_overwrites_attempt_counters_when_projection_counts_decrease() {
        let task_id = TaskId::new();
        let metrics =
            Arc::new(MetricsRegistry::new(None).expect("metrics registry should initialize"));

        // First snapshot: success=2, failure=1, timeout=1
        let mut first_projection = ReplayReducer::new();
        apply_event(
            &mut first_projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );

        let run_success_a = RunId::new();
        let run_success_b = RunId::new();
        let run_failure = RunId::new();
        let run_timeout = RunId::new();

        let mut seq = 2;
        for (run_id, result, error) in [
            (run_success_a, AttemptResultKind::Success, None),
            (run_success_b, AttemptResultKind::Success, None),
            (run_failure, AttemptResultKind::Failure, Some("synthetic failure".to_string())),
            (
                run_timeout,
                AttemptResultKind::Timeout,
                Some("attempt timed out after 3s".to_string()),
            ),
        ] {
            let attempt_id = AttemptId::new();
            apply_event(
                &mut first_projection,
                seq,
                WalEventType::RunCreated {
                    run_instance: run_instance_scheduled(run_id, task_id, 10 + seq),
                },
            );
            seq = transition_to_running(&mut first_projection, run_id, seq + 1);
            apply_event(
                &mut first_projection,
                seq,
                WalEventType::AttemptStarted { run_id, attempt_id, timestamp: 1_000 + seq },
            );
            seq += 1;
            apply_event(
                &mut first_projection,
                seq,
                WalEventType::AttemptFinished {
                    run_id,
                    attempt_id,
                    result,
                    error,
                    output: None,
                    timestamp: 2_000 + seq,
                },
            );
            seq += 1;
        }

        let first_state = build_state(first_projection, Arc::clone(&metrics));
        update(&first_state);
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["success"]).get(),
            2.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["failure"]).get(),
            1.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["timeout"]).get(),
            1.0
        );

        // Second snapshot in same process: success=1, failure=0, timeout=0
        let mut second_projection = ReplayReducer::new();
        apply_event(
            &mut second_projection,
            1,
            WalEventType::TaskCreated { task_spec: build_task_spec(task_id), timestamp: 1 },
        );
        let run_success_only = RunId::new();
        let attempt_success_only = AttemptId::new();
        apply_event(
            &mut second_projection,
            2,
            WalEventType::RunCreated {
                run_instance: run_instance_scheduled(run_success_only, task_id, 99),
            },
        );
        let next_seq = transition_to_running(&mut second_projection, run_success_only, 3);
        apply_event(
            &mut second_projection,
            next_seq,
            WalEventType::AttemptStarted {
                run_id: run_success_only,
                attempt_id: attempt_success_only,
                timestamp: 3_000,
            },
        );
        apply_event(
            &mut second_projection,
            next_seq + 1,
            WalEventType::AttemptFinished {
                run_id: run_success_only,
                attempt_id: attempt_success_only,
                result: AttemptResultKind::Success,
                error: None,
                output: None,
                timestamp: 3_100,
            },
        );

        let second_state = build_state(second_projection, Arc::clone(&metrics));
        update(&second_state);

        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["success"]).get(),
            1.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["failure"]).get(),
            0.0
        );
        assert_eq!(
            metrics.collectors().attempts_total().with_label_values(&["timeout"]).get(),
            0.0
        );
    }
}
