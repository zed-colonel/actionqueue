//! Cron scheduling: runs derived on schedule, bounded by max_occurrences.
//!
//! Proves that:
//! 1. A cron task derives an initial window of min(CRON_WINDOW_SIZE, max_occurrences) runs.
//! 2. When max_occurrences is set, no additional runs are derived beyond the cap.
//! 3. When the clock advances, all derived runs become eligible and complete.

mod support;

#[cfg(feature = "workflow")]
mod wf {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::state::RunState;
    use actionqueue_core::task::constraints::TaskConstraints;
    use actionqueue_core::task::metadata::TaskMetadata;
    use actionqueue_core::task::run_policy::{CronPolicy, RunPolicy};
    use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
    use actionqueue_engine::derive::cron::CRON_WINDOW_SIZE;
    use actionqueue_engine::time::clock::Clock;
    use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
    use actionqueue_runtime::config::RuntimeConfig;
    use actionqueue_runtime::engine::ActionQueueEngine;

    #[derive(Debug)]
    struct InstantSuccessHandler;

    impl ExecutorHandler for InstantSuccessHandler {
        fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
            HandlerOutput::Success { output: None, consumption: vec![] }
        }
    }

    /// A clock backed by a shared atomic counter, advanceable from outside the engine.
    #[derive(Clone)]
    struct AdvancableClock(Arc<AtomicU64>);

    impl AdvancableClock {
        fn new(t: u64) -> Self {
            Self(Arc::new(AtomicU64::new(t)))
        }

        fn advance(&self, by: u64) {
            self.0.fetch_add(by, Ordering::Release);
        }
    }

    impl Clock for AdvancableClock {
        fn now(&self) -> u64 {
            self.0.load(Ordering::Acquire)
        }
    }

    fn engine_config(data_dir: &std::path::Path) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            snapshot_event_threshold: None,
            ..RuntimeConfig::default()
        }
    }

    // ── Initial window equals min(CRON_WINDOW_SIZE, max_occurrences) ─────────

    #[test]
    fn cron_initial_window_equals_cron_window_size_for_unbounded_policy() {
        let data_dir = super::support::unique_data_dir("wf-cron-window");
        let task_id = TaskId::new();
        let clock = AdvancableClock::new(1000);

        let engine = ActionQueueEngine::new(engine_config(&data_dir), InstantSuccessHandler);
        let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap must succeed");

        // Unbounded cron: no max_occurrences limit.
        let cron_policy = CronPolicy::new("* * * * * * *").expect("valid cron expression");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"cron".to_vec()),
            RunPolicy::Cron(cron_policy),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");

        eng.submit_task(spec).expect("submit cron task");

        // Immediately after submit, initial derivation creates CRON_WINDOW_SIZE runs.
        let initial_run_count = eng.projection().run_ids_for_task(task_id).len();
        assert_eq!(
            initial_run_count, CRON_WINDOW_SIZE as usize,
            "initial derivation must create exactly CRON_WINDOW_SIZE={CRON_WINDOW_SIZE} runs"
        );

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn cron_initial_window_capped_at_max_occurrences() {
        let data_dir = super::support::unique_data_dir("wf-cron-cap");
        let task_id = TaskId::new();
        let max_occurrences = 3u32;
        assert!(
            max_occurrences < CRON_WINDOW_SIZE,
            "test requires max_occurrences < CRON_WINDOW_SIZE"
        );

        let clock = AdvancableClock::new(1000);
        let engine = ActionQueueEngine::new(engine_config(&data_dir), InstantSuccessHandler);
        let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap must succeed");

        let cron_policy = CronPolicy::new("* * * * * * *")
            .expect("valid cron expression")
            .with_max_occurrences(max_occurrences)
            .expect("valid max_occurrences");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"cron-capped".to_vec()),
            RunPolicy::Cron(cron_policy),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");

        eng.submit_task(spec).expect("submit cron task");

        let initial_run_count = eng.projection().run_ids_for_task(task_id).len();
        assert_eq!(
            initial_run_count, max_occurrences as usize,
            "initial derivation must be capped at max_occurrences={max_occurrences}, not \
             CRON_WINDOW_SIZE={CRON_WINDOW_SIZE}"
        );

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ── Bounded cron runs all complete when clock advances ───────────────────

    #[tokio::test]
    async fn cron_bounded_runs_complete_after_clock_advances() {
        let data_dir = super::support::unique_data_dir("wf-cron-complete");
        let task_id = TaskId::new();
        let max_occurrences = 3u32;

        let clock = AdvancableClock::new(1000);
        let engine = ActionQueueEngine::new(engine_config(&data_dir), InstantSuccessHandler);
        let mut eng = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap must succeed");

        let cron_policy = CronPolicy::new("* * * * * * *")
            .expect("valid cron expression")
            .with_max_occurrences(max_occurrences)
            .expect("valid max_occurrences");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"cron-bounded".to_vec()),
            RunPolicy::Cron(cron_policy),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");

        eng.submit_task(spec).expect("submit cron task");

        // 3 runs were derived at t=1001, 1002, 1003 (strictly after after_secs=999).
        let initial_count = eng.projection().run_ids_for_task(task_id).len();
        assert_eq!(
            initial_count, max_occurrences as usize,
            "must derive max_occurrences={max_occurrences} runs initially"
        );

        // Advance clock past the last scheduled run and tick until all complete.
        clock.advance(10); // clock=1010, all 3 runs (1001, 1002, 1003) now eligible.

        for _ in 0..20 {
            let _ = eng.tick().await.expect("tick must succeed");
            let run_ids = eng.projection().run_ids_for_task(task_id);
            let all_done = run_ids
                .iter()
                .all(|id| eng.projection().get_run_state(id) == Some(&RunState::Completed));
            if all_done {
                break;
            }
        }

        // All 3 cron runs must be Completed.
        let run_ids = eng.projection().run_ids_for_task(task_id);
        assert_eq!(
            run_ids.len(),
            max_occurrences as usize,
            "no extra runs must be derived beyond max_occurrences"
        );
        for run_id in &run_ids {
            assert_eq!(
                eng.projection().get_run_state(run_id),
                Some(&RunState::Completed),
                "every bounded cron run must complete"
            );
        }

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // ── Cron rolling window replenishment after completions ──────────────────

    /// After initial window runs complete, the dispatch loop derives new runs
    /// to maintain the rolling window (top-up behavior for unbounded cron tasks).
    #[tokio::test]
    async fn cron_window_replenishes_after_completions() {
        let data_dir = super::support::unique_data_dir("wf-cron-topup");
        let task_id = TaskId::new();

        let clock = AdvancableClock::new(1000);
        let engine = ActionQueueEngine::new(engine_config(&data_dir), InstantSuccessHandler);
        let mut eng = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap must succeed");

        // Unbounded cron — should maintain a rolling window of CRON_WINDOW_SIZE.
        let cron_policy = CronPolicy::new("* * * * * * *").expect("valid cron expression");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"cron-topup".to_vec()),
            RunPolicy::Cron(cron_policy),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("valid spec");

        eng.submit_task(spec).expect("submit cron task");

        let initial_count = eng.projection().run_ids_for_task(task_id).len();
        assert_eq!(
            initial_count, CRON_WINDOW_SIZE as usize,
            "initial window must be CRON_WINDOW_SIZE"
        );

        // Advance clock past the initial window and tick until all initial runs complete.
        clock.advance(CRON_WINDOW_SIZE as u64 + 5);
        let _ = eng.run_until_idle().await.expect("first run_until_idle");

        // After completing the initial runs, the dispatch loop should derive new runs
        // to replenish the window. Total runs = initial completed + new window.
        let total_runs = eng.projection().run_ids_for_task(task_id).len();
        assert!(
            total_runs > CRON_WINDOW_SIZE as usize,
            "after initial window completes, new runs must be derived to replenish the window; \
             got {total_runs} total runs (expected > {CRON_WINDOW_SIZE})"
        );

        eng.shutdown().expect("shutdown must succeed");
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
