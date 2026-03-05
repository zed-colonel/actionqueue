//! 8D Department routing acceptance proof.
//!
//! Verifies that actors can be grouped into departments and that
//! department membership is tracked correctly.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_actor::DepartmentRegistry;
use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::{ActorId, DepartmentId};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8d-dept-routing-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

struct NoopHandler;

impl ExecutorHandler for NoopHandler {
    fn execute(&self, _ctx: ExecutorContext) -> HandlerOutput {
        HandlerOutput::success()
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

/// 5 actors in "engineering" department — all registered and grouped.
#[tokio::test]
async fn five_actors_in_engineering_department() {
    let dir = data_dir("eng-dept");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let dept = DepartmentId::new("engineering").expect("valid dept");

    let mut actor_ids = Vec::new();
    for i in 0..5 {
        let id = ActorId::new();
        actor_ids.push(id);
        let caps = ActorCapabilities::new(vec!["compute".to_string()]).expect("caps");
        let reg = ActorRegistration::new(id, format!("worker-{i}"), caps, 30)
            .with_department(dept.clone());
        boot.register_actor(reg).expect("register");
    }

    // All 5 actors should be active.
    for id in &actor_ids {
        assert!(boot.actor_registry().is_active(*id), "worker must be active");
    }
}

/// DepartmentRegistry groups actors correctly.
#[test]
fn department_registry_groups_actors() {
    let mut registry = DepartmentRegistry::new();
    let dept = DepartmentId::new("engineering").expect("valid dept");

    let mut actor_ids = Vec::new();
    for _ in 0..5 {
        let id = ActorId::new();
        actor_ids.push(id);
        registry.assign(id, dept.clone());
    }

    let members = registry.actors_in_department(&dept);
    assert_eq!(members.len(), 5, "all 5 actors must be in engineering");
    for id in &actor_ids {
        assert!(members.contains(id), "each actor must be in the department");
    }
}

/// Actors in different departments don't interfere.
#[test]
fn actors_in_separate_departments_are_isolated() {
    let mut registry = DepartmentRegistry::new();
    let eng = DepartmentId::new("engineering").expect("valid");
    let ops = DepartmentId::new("ops").expect("valid");

    let eng_actor = ActorId::new();
    let ops_actor = ActorId::new();
    registry.assign(eng_actor, eng.clone());
    registry.assign(ops_actor, ops.clone());

    assert!(registry.actors_in_department(&eng).contains(&eng_actor));
    assert!(!registry.actors_in_department(&eng).contains(&ops_actor));
    assert!(registry.actors_in_department(&ops).contains(&ops_actor));
    assert!(!registry.actors_in_department(&ops).contains(&eng_actor));
}
