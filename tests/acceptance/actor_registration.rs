//! 8A Actor registration and heartbeat timeout acceptance proof.
//!
//! Verifies:
//! 1. Actors can be registered with the engine.
//! 2. Heartbeats keep actors alive.
//! 3. Actors time out when heartbeats stop.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::ActorId;
use actionqueue_engine::time::clock::Clock;
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler, HandlerOutput};
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

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn data_dir(label: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("8a-actor-reg-{label}-{}-{n}", std::process::id()));
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

fn make_registration(actor_id: ActorId) -> ActorRegistration {
    let caps =
        ActorCapabilities::new(vec!["compute".to_string(), "review".to_string()]).expect("caps");
    ActorRegistration::new(actor_id, "test-actor", caps, 10)
}

/// Actor registers successfully and is active.
#[tokio::test]
async fn actor_registers_and_is_active() {
    let dir = data_dir("register");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let actor_id = ActorId::new();
    boot.register_actor(make_registration(actor_id)).expect("register");

    assert!(boot.actor_registry().is_active(actor_id), "actor must be active after registration");
    assert_eq!(boot.actor_registry().identity(actor_id), Some("test-actor"));
}

/// Actor is deregistered when explicitly removed.
#[tokio::test]
async fn actor_deregisters_explicitly() {
    let dir = data_dir("deregister");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

    let actor_id = ActorId::new();
    boot.register_actor(make_registration(actor_id)).expect("register");
    boot.deregister_actor(actor_id).expect("deregister");

    assert!(!boot.actor_registry().is_active(actor_id), "actor must be inactive after deregister");
}

/// Actor heartbeat timeout: ticking past the timeout window deregisters the actor.
#[tokio::test]
async fn actor_heartbeat_timeout_deregisters_actor() {
    let dir = data_dir("timeout");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap");

    let actor_id = ActorId::new();
    // interval=10s, multiplier=3 (default) → timeout=30s.
    boot.register_actor(make_registration(actor_id)).expect("register");
    assert!(boot.actor_registry().is_active(actor_id));

    // Advance clock past heartbeat timeout (registered at t=1000, timeout at t=1030).
    clock.advance(31); // t=1031 > t=1030

    // Tick to detect and process timeout.
    let _ = boot.tick().await.expect("tick");

    assert!(
        !boot.actor_registry().is_active(actor_id),
        "actor must be deregistered after heartbeat timeout"
    );
}

/// Actor heartbeat resets the timeout window.
#[tokio::test]
async fn actor_heartbeat_resets_timeout() {
    let dir = data_dir("reset");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap");

    let actor_id = ActorId::new();
    boot.register_actor(make_registration(actor_id)).expect("register");

    // Advance to t=1025 and send a heartbeat.
    clock.advance(25);
    boot.actor_heartbeat(actor_id).expect("heartbeat");

    // Advance to t=1029 (25 more seconds, new timeout = 1025+30=1055 > 1029).
    clock.advance(4);
    let _ = boot.tick().await.expect("tick");

    // Actor must still be active.
    assert!(boot.actor_registry().is_active(actor_id), "actor must survive after fresh heartbeat");
}
