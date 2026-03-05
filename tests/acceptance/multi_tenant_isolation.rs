//! 8E Multi-tenant isolation acceptance proof.
//!
//! Verifies that tenants can be created and actors are scoped to their tenant.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::{ActorId, TenantId};
use actionqueue_core::platform::TenantRegistration;
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8e-tenant-{label}-{}-{n}", std::process::id()));
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

/// Two tenants can be created; actors scoped to their tenant.
#[tokio::test]
async fn two_tenants_created_and_actors_scoped() {
    let dir = data_dir("two-tenants");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let tenant_alpha = TenantId::new();
    let tenant_beta = TenantId::new();

    boot.create_tenant(TenantRegistration::new(tenant_alpha, "Alpha Corp")).expect("tenant alpha");
    boot.create_tenant(TenantRegistration::new(tenant_beta, "Beta Corp")).expect("tenant beta");

    assert!(boot.tenant_registry().exists(tenant_alpha), "alpha must exist");
    assert!(boot.tenant_registry().exists(tenant_beta), "beta must exist");
    assert!(!boot.tenant_registry().exists(TenantId::new()), "unknown tenant must not exist");

    let caps = ActorCapabilities::new(vec!["work".to_string()]).expect("caps");
    let actor_a = ActorId::new();
    let actor_b = ActorId::new();

    boot.register_actor(
        ActorRegistration::new(actor_a, "alpha-worker", caps.clone(), 30).with_tenant(tenant_alpha),
    )
    .expect("register alpha actor");
    boot.register_actor(
        ActorRegistration::new(actor_b, "beta-worker", caps, 30).with_tenant(tenant_beta),
    )
    .expect("register beta actor");

    // Actor A is in tenant Alpha.
    assert_eq!(boot.actor_registry().get(actor_a).and_then(|r| r.tenant_id()), Some(tenant_alpha));
    // Actor B is in tenant Beta.
    assert_eq!(boot.actor_registry().get(actor_b).and_then(|r| r.tenant_id()), Some(tenant_beta));

    // Active actors for tenant Alpha: only actor A.
    let alpha_actors = boot.actor_registry().active_actors_for_tenant(tenant_alpha);
    assert_eq!(alpha_actors.len(), 1);
    assert!(alpha_actors.contains(&actor_a));

    // Active actors for tenant Beta: only actor B.
    let beta_actors = boot.actor_registry().active_actors_for_tenant(tenant_beta);
    assert_eq!(beta_actors.len(), 1);
    assert!(beta_actors.contains(&actor_b));
}

/// Tenant name is preserved in registry.
#[tokio::test]
async fn tenant_name_preserved() {
    let dir = data_dir("name");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let id = TenantId::new();
    boot.create_tenant(TenantRegistration::new(id, "Acme Digital Corporation")).expect("create");

    let reg = boot.tenant_registry().get(id).expect("tenant exists");
    assert_eq!(reg.name(), "Acme Digital Corporation");
}
