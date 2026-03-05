//! 8F RBAC enforcement acceptance proof.
//!
//! Verifies role assignment and capability enforcement.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::{ActorId, TenantId};
use actionqueue_core::platform::{Capability, Role, TenantRegistration};
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8f-rbac-{label}-{}-{n}", std::process::id()));
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

/// Full triad: Operator gets CanSubmit, Auditor gets CanReview, Gatekeeper gets CanExecute.
#[tokio::test]
async fn triad_rbac_enforcement() {
    let dir = data_dir("triad");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let tenant = TenantId::new();
    boot.create_tenant(TenantRegistration::new(tenant, "Digicorp")).expect("tenant");

    let operator_id = ActorId::new();
    let auditor_id = ActorId::new();
    let gatekeeper_id = ActorId::new();
    let caps = ActorCapabilities::new(vec!["work".to_string()]).expect("caps");

    boot.register_actor(
        ActorRegistration::new(operator_id, "operator", caps.clone(), 30).with_tenant(tenant),
    )
    .expect("reg operator");
    boot.register_actor(
        ActorRegistration::new(auditor_id, "auditor", caps.clone(), 30).with_tenant(tenant),
    )
    .expect("reg auditor");
    boot.register_actor(
        ActorRegistration::new(gatekeeper_id, "gatekeeper", caps, 30).with_tenant(tenant),
    )
    .expect("reg gatekeeper");

    // Assign roles.
    boot.assign_role(operator_id, Role::Operator, tenant).expect("assign operator");
    boot.assign_role(auditor_id, Role::Auditor, tenant).expect("assign auditor");
    boot.assign_role(gatekeeper_id, Role::Gatekeeper, tenant).expect("assign gatekeeper");

    // Grant capabilities.
    boot.grant_capability(operator_id, Capability::CanSubmit, tenant).expect("grant CanSubmit");
    boot.grant_capability(auditor_id, Capability::CanReview, tenant).expect("grant CanReview");
    boot.grant_capability(gatekeeper_id, Capability::CanExecute, tenant).expect("grant CanExecute");

    // Verify roles.
    assert_eq!(boot.rbac().role_of(operator_id, tenant), Some(&Role::Operator));
    assert_eq!(boot.rbac().role_of(auditor_id, tenant), Some(&Role::Auditor));
    assert_eq!(boot.rbac().role_of(gatekeeper_id, tenant), Some(&Role::Gatekeeper));

    // Verify capabilities: each actor has exactly their granted capability.
    assert!(boot.rbac().has_capability(operator_id, &Capability::CanSubmit, tenant));
    assert!(!boot.rbac().has_capability(operator_id, &Capability::CanExecute, tenant));
    assert!(!boot.rbac().has_capability(operator_id, &Capability::CanReview, tenant));

    assert!(boot.rbac().has_capability(auditor_id, &Capability::CanReview, tenant));
    assert!(!boot.rbac().has_capability(auditor_id, &Capability::CanExecute, tenant));

    assert!(boot.rbac().has_capability(gatekeeper_id, &Capability::CanExecute, tenant));
    assert!(!boot.rbac().has_capability(gatekeeper_id, &Capability::CanSubmit, tenant));

    // check_permission: operator cannot execute privileged task.
    assert!(
        boot.rbac().check_permission(operator_id, &Capability::CanExecute, tenant).is_err(),
        "operator must not be able to execute"
    );
    assert!(
        boot.rbac().check_permission(operator_id, &Capability::CanSubmit, tenant).is_ok(),
        "operator must be able to submit"
    );
    assert!(
        boot.rbac().check_permission(gatekeeper_id, &Capability::CanExecute, tenant).is_ok(),
        "gatekeeper must be able to execute"
    );
}

/// Actor with no role gets rejected by check_permission.
#[tokio::test]
async fn no_role_rejected_by_check_permission() {
    let dir = data_dir("no-role");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let tenant = TenantId::new();
    boot.create_tenant(TenantRegistration::new(tenant, "Corp")).expect("tenant");

    let actor_id = ActorId::new();
    let caps = ActorCapabilities::new(vec!["work".to_string()]).expect("caps");
    boot.register_actor(ActorRegistration::new(actor_id, "worker", caps, 30).with_tenant(tenant))
        .expect("reg");

    // No role assigned → any permission check fails.
    let result = boot.rbac().check_permission(actor_id, &Capability::CanSubmit, tenant);
    assert!(
        matches!(result, Err(actionqueue_platform::RbacError::NoRoleAssigned { .. })),
        "expected NoRoleAssigned error"
    );
}
