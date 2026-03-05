//! 8C Remote actor crash detection acceptance proof.
//!
//! Verifies that when an actor stops sending heartbeats, the engine
//! auto-deregisters the actor after the timeout threshold.

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
        .join(format!("8c-actor-crash-{label}-{}-{n}", std::process::id()));
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

/// Actor that stops sending heartbeats is auto-deregistered at timeout.
#[tokio::test]
async fn actor_crash_detected_at_timeout() {
    let dir = data_dir("crash");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap");

    let actor_id = ActorId::new();
    let caps = ActorCapabilities::new(vec!["work".to_string()]).expect("caps");
    // interval=10s, multiplier=3 → timeout=30s
    let reg = ActorRegistration::new(actor_id, "crashed-actor", caps, 10);
    boot.register_actor(reg).expect("register");

    assert!(boot.actor_registry().is_active(actor_id), "actor must be active initially");

    // Simulate crash: no heartbeats sent. Advance clock past timeout.
    clock.advance(35); // t=1035 > registered_at(1000) + timeout(30) = 1030

    // Tick engine — should detect timeout and deregister.
    let _ = boot.tick().await.expect("tick");

    assert!(
        !boot.actor_registry().is_active(actor_id),
        "actor must be deregistered after heartbeat timeout (simulated crash)"
    );
}

/// Multiple actors: only the crashed one is deregistered.
#[tokio::test]
async fn only_crashed_actor_deregistered() {
    let dir = data_dir("partial-crash");
    let clock = AdvancableClock::new(1000);
    let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
    let mut boot = engine.bootstrap_with_clock(clock.clone()).expect("bootstrap");

    let crashed_id = ActorId::new();
    let alive_id = ActorId::new();

    let caps = ActorCapabilities::new(vec!["work".to_string()]).expect("caps");
    boot.register_actor(ActorRegistration::new(crashed_id, "crashed", caps.clone(), 10))
        .expect("register crashed");
    boot.register_actor(ActorRegistration::new(alive_id, "alive", caps, 10))
        .expect("register alive");

    // Advance to t=1025 and send a heartbeat for the alive actor only.
    clock.advance(25);
    boot.actor_heartbeat(alive_id).expect("heartbeat for alive");

    // Advance past crashed actor's timeout but not alive actor's new timeout.
    // Crashed: timeout at t=1030. Alive: heartbeat at t=1025, new timeout at t=1055.
    clock.advance(10); // t=1035 > crashed timeout(1030), < alive timeout(1055)

    let _ = boot.tick().await.expect("tick");

    assert!(!boot.actor_registry().is_active(crashed_id), "crashed actor deregistered");
    assert!(boot.actor_registry().is_active(alive_id), "alive actor remains active");
}
