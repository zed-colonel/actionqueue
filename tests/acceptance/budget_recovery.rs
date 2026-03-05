//! 7G Budget-recovery acceptance proof.
//!
//! Verifies that budget state (allocation and consumption) survives WAL recovery.
//! After allocating a budget, running one consuming attempt, dropping the engine,
//! and re-bootstrapping from the same data directory, the recovered projection
//! must reflect the same budget allocation and consumption as before the crash.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
use actionqueue_core::ids::TaskId;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("7g-budget-recovery-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir should be creatable");
    dir
}

/// Always fails terminally, consuming 300 tokens per attempt.
/// Using TerminalFailure ensures exactly one attempt (no retries).
#[derive(Debug)]
struct ThreeHundredTokenHandler;

impl ExecutorHandler for ThreeHundredTokenHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::TerminalFailure {
            error: "budget-consume-300".to_string(),
            consumption: vec![BudgetConsumption::new(BudgetDimension::Token, 300)],
        }
    }
}

fn make_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(1).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

/// Budget allocation and partial consumption survive WAL recovery.
///
/// Scenario:
/// 1. Bootstrap engine, allocate 1000-token budget.
/// 2. Run until idle: one terminal-failure attempt consumes 300 tokens.
/// 3. Verify 300 consumed before crash.
/// 4. Drop engine (simulates crash / clean stop).
/// 5. Re-bootstrap from the same data directory.
/// 6. Verify: limit=1000, consumed=300, not exhausted.
#[tokio::test]
async fn budget_state_survives_wal_recovery() {
    let dir = data_dir("wal-recovery");
    let task_id = TaskId::new();

    // ---- Phase 1: allocate, consume 300 tokens via terminal failure, then drop ----
    {
        let clock = MockClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), ThreeHundredTokenHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap phase 1");

        // max_attempts=1 with TerminalFailure → exactly one dispatch, then Failed.
        let constraints = TaskConstraints::new(1, None, None).expect("valid constraints");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"recovery-test".to_vec()),
            RunPolicy::Once,
            constraints,
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");

        // Allocate 1000 tokens.
        boot.allocate_budget(task_id, BudgetDimension::Token, 1000).expect("allocate budget");

        // run_until_idle: dispatches once (TerminalFailure, 300 tokens), run → Failed.
        let _summary = boot.run_until_idle().await.expect("run_until_idle phase 1");

        // Verify 300 tokens consumed and recorded before crash.
        let budget_pre =
            boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
        assert_eq!(budget_pre.limit, 1000, "limit must be 1000 before crash");
        assert_eq!(
            budget_pre.consumed, 300,
            "consumed must be 300 before crash, got {}",
            budget_pre.consumed
        );
        assert!(!budget_pre.exhausted, "budget must not be exhausted (300 < 1000)");

        // Drop bootstrapped engine (simulates crash / shutdown).
        boot.shutdown().expect("shutdown phase 1");
    }

    // ---- Phase 2: re-bootstrap and verify WAL recovery restores budget state ----
    {
        let clock = MockClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), ThreeHundredTokenHandler);
        let boot = engine.bootstrap_with_clock(clock).expect("bootstrap phase 2");

        let budget_recovered =
            boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
        assert_eq!(budget_recovered.limit, 1000, "recovered limit must match original allocation");
        assert_eq!(
            budget_recovered.consumed, 300,
            "recovered consumption must reflect the 300 tokens consumed before crash"
        );
        assert!(
            !budget_recovered.exhausted,
            "budget must not be exhausted after recovery (300 < 1000)"
        );

        boot.shutdown().expect("shutdown phase 2");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Budget replenishment resets consumed to 0 and survives WAL recovery.
///
/// Scenario:
/// 1. Allocate 1000-token budget, consume 1000 (exhaust), replenish to 2000.
/// 2. Drop engine (simulates crash / clean stop).
/// 3. Re-bootstrap and verify: limit=2000, consumed=0, not exhausted.
#[tokio::test]
async fn budget_replenishment_survives_wal_recovery() {
    let dir = data_dir("replenish-recovery");
    let task_id = TaskId::new();

    // ---- Phase 1: allocate, exhaust, replenish, then drop ----
    {
        let clock = MockClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), ThreeHundredTokenHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap phase 1");

        // max_attempts=5 so we get multiple dispatches before exhaustion.
        let constraints = TaskConstraints::new(5, None, None).expect("valid constraints");
        let spec = TaskSpec::new(
            task_id,
            TaskPayload::new(b"replenish-recovery".to_vec()),
            RunPolicy::Once,
            constraints,
            TaskMetadata::default(),
        )
        .expect("valid spec");
        boot.submit_task(spec).expect("submit");

        // Allocate 1000 tokens — 300 per attempt means exhausted after 4 attempts (1200 > 1000).
        boot.allocate_budget(task_id, BudgetDimension::Token, 1000).expect("allocate budget");

        // Run until idle: dispatches until budget exhausted or run fails.
        let _summary = boot.run_until_idle().await.expect("run_until_idle phase 1");

        let budget_pre =
            boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
        assert!(budget_pre.consumed > 0, "some tokens must have been consumed");

        // Replenish with a fresh 2000-token budget.
        boot.replenish_budget(task_id, BudgetDimension::Token, 2000).expect("replenish");

        let budget_after =
            boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
        assert_eq!(budget_after.limit, 2000, "limit must be 2000 after replenishment");
        assert_eq!(budget_after.consumed, 0, "consumed must be 0 after replenishment");
        assert!(!budget_after.exhausted, "must not be exhausted after replenishment");

        boot.shutdown().expect("shutdown phase 1");
    }

    // ---- Phase 2: re-bootstrap and verify replenishment state persisted ----
    {
        let clock = MockClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), ThreeHundredTokenHandler);
        let boot = engine.bootstrap_with_clock(clock).expect("bootstrap phase 2");

        let budget_recovered =
            boot.projection().get_budget(&task_id, BudgetDimension::Token).expect("budget");
        assert_eq!(
            budget_recovered.limit, 2000,
            "recovered limit must be 2000 (replenished value)"
        );
        assert_eq!(
            budget_recovered.consumed, 0,
            "recovered consumed must be 0 (replenishment resets consumption)"
        );
        assert!(!budget_recovered.exhausted, "budget must not be exhausted after recovery");

        boot.shutdown().expect("shutdown phase 2");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
