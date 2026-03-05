//! 8B Capability matching acceptance proof.
//!
//! Verifies that actors can only claim tasks matching their capability set.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use actionqueue_actor::CapabilityRouter;
use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::ActorId;
use actionqueue_engine::time::clock::MockClock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8b-cap-match-{label}-{}-{n}", std::process::id()));
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

/// Actor with only "compute" cannot handle tasks requiring "compute" + "review".
#[tokio::test]
async fn compute_only_actor_cannot_handle_compute_review_task() {
    let dir = data_dir("compute-only");
    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let actor_a = ActorId::new();
    let actor_b = ActorId::new();

    let caps_a = ActorCapabilities::new(vec!["compute".to_string()]).expect("caps");
    let caps_b =
        ActorCapabilities::new(vec!["compute".to_string(), "review".to_string()]).expect("caps");

    boot.register_actor(ActorRegistration::new(actor_a, "actor-a", caps_a, 30)).expect("reg");
    boot.register_actor(ActorRegistration::new(actor_b, "actor-b", caps_b, 30)).expect("reg");

    // Task X requires compute+review.
    let required_x = vec!["compute".to_string(), "review".to_string()];
    // Task Y requires compute only.
    let required_y = vec!["compute".to_string()];

    // Actor A (compute only): can handle Y but not X.
    let a_caps = vec!["compute".to_string()];
    assert!(!CapabilityRouter::can_handle(&a_caps, &required_x), "A cannot handle X");
    assert!(CapabilityRouter::can_handle(&a_caps, &required_y), "A can handle Y");

    // Actor B (compute+review): can handle both X and Y.
    let b_caps = vec!["compute".to_string(), "review".to_string()];
    assert!(CapabilityRouter::can_handle(&b_caps, &required_x), "B can handle X");
    assert!(CapabilityRouter::can_handle(&b_caps, &required_y), "B can handle Y");

    // Both actors are registered and active.
    assert!(boot.actor_registry().is_active(actor_a));
    assert!(boot.actor_registry().is_active(actor_b));
}

/// CapabilityRouter.eligible_actors correctly filters by required capabilities.
#[test]
fn eligible_actors_filters_by_capabilities() {
    let actor_a = ActorId::new();
    let actor_b = ActorId::new();
    let actor_c = ActorId::new();

    let a_caps_vec = vec!["compute".to_string()];
    let b_caps_vec = vec!["compute".to_string(), "review".to_string()];
    let c_caps_vec = vec!["review".to_string()];

    let actors = vec![
        (actor_a, a_caps_vec.as_slice()),
        (actor_b, b_caps_vec.as_slice()),
        (actor_c, c_caps_vec.as_slice()),
    ];

    let required_compute_review = vec!["compute".to_string(), "review".to_string()];
    let eligible = CapabilityRouter::eligible_actors(&actors, &required_compute_review);

    assert_eq!(eligible.len(), 1, "only actor_b qualifies");
    assert!(eligible.contains(&actor_b));

    let required_compute = vec!["compute".to_string()];
    let eligible2 = CapabilityRouter::eligible_actors(&actors, &required_compute);
    assert_eq!(eligible2.len(), 2, "actors a and b qualify for compute-only");
    assert!(eligible2.contains(&actor_a));
    assert!(eligible2.contains(&actor_b));
}

/// Empty required_capabilities matches all actors.
#[test]
fn no_requirements_matches_all_actors() {
    let actor_a = ActorId::new();
    let a_caps = vec!["anything".to_string()];
    let actors = vec![(actor_a, a_caps.as_slice())];
    let required_none: Vec<String> = vec![];
    let eligible = CapabilityRouter::eligible_actors(&actors, &required_none);
    assert_eq!(eligible.len(), 1);
}
