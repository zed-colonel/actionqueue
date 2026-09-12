//! 8B Executor trait matching acceptance proof.
//!
//! Verifies that actors can only claim tasks matching their executor trait set.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_actor::ExecutorTraitRouter;
use actionqueue_core::actor::{ActorRegistration, ExecutorTraits};
use actionqueue_core::ids::ActorId;
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("8b-cap-match-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("data dir");
    dir
}

struct NoopHandler;

impl ExecutorHandler for NoopHandler {
    fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
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

/// Actor with only "compute" cannot handle tasks requiring "compute" + "review".
#[tokio::test]
async fn compute_only_actor_cannot_handle_compute_review_task() {
    let dir = data_dir("compute-only");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap").with_host(
        actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        },
    );

    let actor_a = ActorId::new();
    let actor_b = ActorId::new();

    let caps_a = ExecutorTraits::new(vec!["compute".to_string()]).expect("caps");
    let caps_b =
        ExecutorTraits::new(vec!["compute".to_string(), "review".to_string()]).expect("caps");

    boot.register_actor(ActorRegistration::new(actor_a, "actor-a", caps_a, 30)).expect("reg");
    boot.register_actor(ActorRegistration::new(actor_b, "actor-b", caps_b, 30)).expect("reg");

    // Task X requires compute+review.
    let required_x =
        ExecutorTraits::new(vec!["compute".to_string(), "review".to_string()]).unwrap();
    // Task Y requires compute only.
    let required_y = ExecutorTraits::new(vec!["compute".to_string()]).unwrap();

    // Actor A (compute only): can handle Y but not X.
    let a_caps = ExecutorTraits::new(vec!["compute".to_string()]).unwrap();
    assert!(!ExecutorTraitRouter::can_handle(&a_caps, Some(&required_x)), "A cannot handle X");
    assert!(ExecutorTraitRouter::can_handle(&a_caps, Some(&required_y)), "A can handle Y");

    // Actor B (compute+review): can handle both X and Y.
    let b_caps = ExecutorTraits::new(vec!["compute".to_string(), "review".to_string()]).unwrap();
    assert!(ExecutorTraitRouter::can_handle(&b_caps, Some(&required_x)), "B can handle X");
    assert!(ExecutorTraitRouter::can_handle(&b_caps, Some(&required_y)), "B can handle Y");

    // Both actors are registered and active.
    assert!(boot.actor_registry().is_active(actor_a));
    assert!(boot.actor_registry().is_active(actor_b));
}

/// ExecutorTraitRouter.eligible_actors correctly filters by required executor traits.
#[test]
fn eligible_actors_filters_by_executor_traits() {
    let actor_a = ActorId::new();
    let actor_b = ActorId::new();
    let actor_c = ActorId::new();

    let a_caps_vec = ExecutorTraits::new(vec!["compute".to_string()]).unwrap();
    let b_caps_vec =
        ExecutorTraits::new(vec!["compute".to_string(), "review".to_string()]).unwrap();
    let c_caps_vec = ExecutorTraits::new(vec!["review".to_string()]).unwrap();

    let actors = vec![(actor_a, &a_caps_vec), (actor_b, &b_caps_vec), (actor_c, &c_caps_vec)];

    let required_compute_review =
        ExecutorTraits::new(vec!["compute".to_string(), "review".to_string()]).unwrap();
    let eligible = ExecutorTraitRouter::eligible_actors(&actors, Some(&required_compute_review));

    assert_eq!(eligible.len(), 1, "only actor_b qualifies");
    assert!(eligible.contains(&actor_b));

    let required_compute = ExecutorTraits::new(vec!["compute".to_string()]).unwrap();
    let eligible2 = ExecutorTraitRouter::eligible_actors(&actors, Some(&required_compute));
    assert_eq!(eligible2.len(), 2, "actors a and b qualify for compute-only");
    assert!(eligible2.contains(&actor_a));
    assert!(eligible2.contains(&actor_b));
}

/// Empty required_executor_traits matches all actors.
#[test]
fn no_requirements_matches_all_actors() {
    let actor_a = ActorId::new();
    let a_caps = ExecutorTraits::new(vec!["anything".to_string()]).unwrap();
    let actors = vec![(actor_a, &a_caps)];
    let eligible = ExecutorTraitRouter::eligible_actors(&actors, None);
    assert_eq!(eligible.len(), 1);
}

/// Routing labels, even when named like permissions, never grant queue authority.
#[test]
fn executor_traits_grant_no_rbac_permission() {
    use actionqueue_core::platform::Capability;
    use actionqueue_platform::rbac::RbacEnforcer;
    let actor = ActorId::new();
    let traits = ExecutorTraits::new(vec!["admin".into(), "approve".into()]).unwrap();
    assert!(ExecutorTraitRouter::can_handle(&traits, Some(&traits)));
    let rbac = RbacEnforcer::new();
    let tenant = actionqueue_core::ids::TenantId::new();
    assert!(!rbac.has_capability(actor, &Capability::CanSubmit, tenant));
    assert!(rbac.role_of(actor, tenant).is_none());
}

/// Actual dispatch must apply the same exact-subset test as remote routing.
#[tokio::test]
async fn local_claim_requires_explicit_matching_traits() {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::task::{
        constraints::TaskConstraints, metadata::TaskMetadata, run_policy::RunPolicy,
        task_spec::TaskSpec,
    };
    for offered in
        [None, Some(vec!["compute".into()]), Some(vec!["compute".into(), "review".into()])]
    {
        let mut config = make_config(data_dir("local-claims"));
        config.local_executor_traits = offered.map(|values| ExecutorTraits::new(values).unwrap());
        let expected =
            config.local_executor_traits.as_ref().is_some_and(|t| t.as_slice().len() == 2);
        let mut boot = ActionQueueEngine::new(config, NoopHandler)
            .bootstrap_with_clock(MockClock::new(1000))
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
        let task = TaskSpec::new(
            TaskId::new(),
            actionqueue_core::task::task_spec::TaskPayload::new(vec![]),
            RunPolicy::Once,
            TaskConstraints::default()
                .with_required_executor_traits(vec!["compute".into(), "review".into()])
                .unwrap(),
            TaskMetadata::default(),
        )
        .unwrap();
        boot.submit_task(task).unwrap();
        assert_eq!(boot.tick().await.unwrap().dispatched, usize::from(expected));
    }
}

#[cfg(feature = "platform")]
#[tokio::test]
async fn actor_tenant_replacement_is_rejected_without_append() {
    use actionqueue_core::ids::TenantId;
    let mut config = make_config(data_dir("tenant-replacement"));
    config.store_features = actionqueue_storage::store::capabilities();
    let mut boot = ActionQueueEngine::new(config, NoopHandler)
        .bootstrap_with_clock(MockClock::new(1000))
        .unwrap()
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::Store,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
    let id = ActorId::new();
    let traits = ExecutorTraits::new(vec!["compute".into()]).unwrap();
    let tenant = TenantId::new();
    boot.create_tenant(actionqueue_core::platform::TenantRegistration::new(tenant, "tenant"))
        .unwrap();
    boot.set_control_context(Some(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::ProvisionTenant(tenant),
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    }));
    boot.register_actor(
        ActorRegistration::new(id, "first", traits.clone(), 30).with_tenant(tenant),
    )
    .unwrap();
    let sequence = boot.projection().latest_sequence();
    assert!(boot
        .register_actor(
            ActorRegistration::new(id, "second", traits, 30).with_tenant(TenantId::new())
        )
        .is_err());
    assert_eq!(boot.projection().latest_sequence(), sequence);
    assert_eq!(boot.actor_registry().get(id).unwrap().tenant_id(), Some(tenant));
}
