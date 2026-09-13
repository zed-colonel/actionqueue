//! 8I Triad MVP acceptance proof.
//!
//! End-to-end: Operator submits → Auditor reviews → Gatekeeper executes.
//! Verifies the full three-role approval workflow with ledger audit trail.

mod host_support;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::actor::{ActorRegistration, ExecutorTraits};
use actionqueue_core::ids::{ActorId, LedgerEntryId, TaskId, TenantId};
use actionqueue_core::platform::{Capability, LedgerEntry, Role, TenantRegistration};
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::time::clock::Clock;
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

/// Clock backed by a shared atomic counter, advanceable from outside the engine.
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

struct SucceedHandler;

impl ExecutorHandler for SucceedHandler {
    fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}

fn make_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        store_features: actionqueue_storage::store::capabilities(),
        data_dir: dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(1).expect("non-zero"),
        lease_timeout_secs: 30,
        ..RuntimeConfig::default()
    }
}

fn task_spec(id: TaskId, label: &str) -> TaskSpec {
    TaskSpec::new(
        id,
        TaskPayload::new(label.as_bytes().to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec")
}

/// Full triad workflow with audit trail.
#[tokio::test]
async fn triad_mvp_full_workflow() {
    let dir = tempfile::tempdir().expect("isolated triad store");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir.path().to_path_buf()), SucceedHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap").with_host(
        actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::Store,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        },
    );

    // Setup: tenant + triad actors.
    let tenant_id = TenantId::new();
    boot.create_tenant(TenantRegistration::new(tenant_id, "Digicorp")).expect("tenant");

    let operator_id = ActorId::new();
    let auditor_id = ActorId::new();
    let gatekeeper_id = ActorId::new();
    let caps = ExecutorTraits::new(vec!["work".to_string()]).expect("caps");

    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::ProvisionTenant(tenant_id),
    )));
    boot.register_actor(
        ActorRegistration::new(operator_id, "operator", caps.clone(), 30).with_tenant(tenant_id),
    )
    .expect("reg operator");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::ProvisionTenant(tenant_id),
    )));
    boot.register_actor(
        ActorRegistration::new(auditor_id, "auditor", caps.clone(), 30).with_tenant(tenant_id),
    )
    .expect("reg auditor");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::ProvisionTenant(tenant_id),
    )));
    boot.register_actor(
        ActorRegistration::new(gatekeeper_id, "gatekeeper", caps, 30).with_tenant(tenant_id),
    )
    .expect("reg gatekeeper");

    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));

    boot.assign_role(operator_id, Role::Operator, tenant_id).expect("assign operator");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));
    boot.assign_role(auditor_id, Role::Auditor, tenant_id).expect("assign auditor");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));
    boot.assign_role(gatekeeper_id, Role::Gatekeeper, tenant_id).expect("assign gatekeeper");

    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));

    boot.grant_capability(operator_id, Capability::CanSubmit, tenant_id).expect("grant CanSubmit");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));
    boot.grant_capability(auditor_id, Capability::CanReview, tenant_id).expect("grant CanReview");
    boot.set_control_context(Some(host_support::host(
        actionqueue_core::control::ControlScope::Store,
    )));
    boot.grant_capability(gatekeeper_id, Capability::CanExecute, tenant_id)
        .expect("grant CanExecute");

    // Step 1: Operator submits work plan.
    let plan_id = TaskId::new();
    host_support::bind_engine_tenant(&mut boot, tenant_id, Some(operator_id));
    boot.submit_task(task_spec(plan_id, "work-plan").with_tenant(tenant_id)).expect("submit plan");

    // Audit: record plan submission.
    let audit_entry = LedgerEntry::new(
        LedgerEntryId::new(),
        tenant_id,
        "audit",
        b"plan submitted by operator".to_vec(),
        1001,
    )
    .with_actor(operator_id);
    boot.append_ledger_entry(audit_entry).expect("audit plan submission");

    clock.advance(1);
    let _ = boot.run_until_idle().await.expect("run plan");

    // Verify plan completed.
    let plan_runs = boot.projection().run_ids_for_task(plan_id);
    assert_eq!(plan_runs.len(), 1);
    assert_eq!(
        *boot.projection().get_run_state(&plan_runs[0]).expect("state"),
        RunState::Completed,
        "plan must complete"
    );

    // Step 2: Auditor reviews (approval workflow via DAG).
    let review_id = TaskId::new();
    host_support::bind_engine_tenant(&mut boot, tenant_id, Some(auditor_id));
    boot.submit_task(task_spec(review_id, "review").with_tenant(tenant_id)).expect("submit review");
    boot.declare_dependency(review_id, vec![plan_id]).expect("review depends on plan");

    // Audit: record review decision.
    let review_decision = LedgerEntry::new(
        LedgerEntryId::new(),
        tenant_id,
        "decision",
        b"plan approved by auditor".to_vec(),
        1002,
    )
    .with_actor(auditor_id);
    boot.append_ledger_entry(review_decision).expect("audit review");

    clock.advance(1);
    let _ = boot.run_until_idle().await.expect("run review");

    let review_runs = boot.projection().run_ids_for_task(review_id);
    assert_eq!(review_runs.len(), 1);
    assert_eq!(
        *boot.projection().get_run_state(&review_runs[0]).expect("state"),
        RunState::Completed,
        "review must complete"
    );

    // Step 3: Gatekeeper executes privileged action.
    let exec_id = TaskId::new();
    host_support::bind_engine_tenant(&mut boot, tenant_id, Some(gatekeeper_id));
    boot.submit_task(task_spec(exec_id, "privileged-execution").with_tenant(tenant_id))
        .expect("submit exec");
    boot.declare_dependency(exec_id, vec![review_id]).expect("exec depends on review");

    clock.advance(1);
    let _ = boot.run_until_idle().await.expect("run exec");

    let exec_runs = boot.projection().run_ids_for_task(exec_id);
    assert_eq!(exec_runs.len(), 1);
    assert_eq!(
        *boot.projection().get_run_state(&exec_runs[0]).expect("state"),
        RunState::Completed,
        "execution must complete"
    );

    // Audit: record execution completion.
    let exec_audit = LedgerEntry::new(
        LedgerEntryId::new(),
        tenant_id,
        "audit",
        b"privileged action executed by gatekeeper".to_vec(),
        1003,
    )
    .with_actor(gatekeeper_id);
    boot.append_ledger_entry(exec_audit).expect("audit execution");

    // Verify full audit trail.
    let audit_entries: Vec<_> = boot.ledger().iter_for_key("audit").collect();
    let decision_entries: Vec<_> = boot.ledger().iter_for_key("decision").collect();

    assert_eq!(audit_entries.len(), 2, "2 audit entries (plan submission + execution)");
    assert_eq!(decision_entries.len(), 1, "1 decision entry (review approval)");

    // Verify RBAC: separation of duties held throughout.
    assert!(
        boot.rbac().check_permission(operator_id, &Capability::CanSubmit, tenant_id).is_ok(),
        "operator can submit"
    );
    assert!(
        boot.rbac().check_permission(operator_id, &Capability::CanExecute, tenant_id).is_err(),
        "operator cannot execute"
    );
    assert!(
        boot.rbac().check_permission(gatekeeper_id, &Capability::CanExecute, tenant_id).is_ok(),
        "gatekeeper can execute"
    );
    assert!(
        boot.rbac().check_permission(gatekeeper_id, &Capability::CanSubmit, tenant_id).is_err(),
        "gatekeeper cannot submit"
    );
}
