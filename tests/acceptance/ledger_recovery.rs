//! 8H Ledger recovery acceptance proof.
//!
//! Verifies that ledger entries survive WAL recovery.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::ids::{LedgerEntryId, TenantId};
use actionqueue_core::platform::{LedgerEntry, TenantRegistration};
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

    #[allow(dead_code)]
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
        .join(format!("8h-ledger-{label}-{}-{n}", std::process::id()));
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

/// Ledger entries survive WAL recovery (crash → restart → entries present).
#[tokio::test]
async fn ledger_entries_survive_wal_recovery() {
    let dir = data_dir("recovery");
    let tenant_id = TenantId::new();

    let entry_ids: Vec<LedgerEntryId> = (0..5).map(|_| LedgerEntryId::new()).collect();

    // Phase 1: write 5 ledger entries.
    {
        let clock = AdvancableClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), NoopHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

        boot.create_tenant(TenantRegistration::new(tenant_id, "Recovery Corp")).expect("tenant");

        for (i, &entry_id) in entry_ids.iter().enumerate() {
            let key = if i % 2 == 0 { "audit" } else { "decision" };
            let entry = LedgerEntry::new(
                entry_id,
                tenant_id,
                key,
                format!("payload-{i}").into_bytes(),
                1000 + i as u64,
            );
            boot.append_ledger_entry(entry).expect("append entry");
        }

        assert_eq!(boot.ledger().len(), 5, "5 entries before shutdown");
        boot.shutdown().expect("shutdown");
    }

    // Phase 2: fresh bootstrap — all entries must be recovered.
    {
        let clock = AdvancableClock::new(2000);
        let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
        let boot = engine.bootstrap_with_clock(clock).expect("bootstrap after crash");

        assert_eq!(boot.ledger().len(), 5, "all 5 ledger entries must survive WAL recovery");

        // Verify entries can be queried by key.
        let audit_count = boot.ledger().iter_for_key("audit").count();
        let decision_count = boot.ledger().iter_for_key("decision").count();
        assert_eq!(audit_count, 3, "3 audit entries (indices 0,2,4)");
        assert_eq!(decision_count, 2, "2 decision entries (indices 1,3)");

        // Verify entries can be queried by tenant.
        let tenant_count = boot.ledger().iter_for_tenant(tenant_id).count();
        assert_eq!(tenant_count, 5, "all entries belong to the tenant");

        // Verify specific entries by ID.
        for &entry_id in &entry_ids {
            assert!(
                boot.ledger().entry_by_id(entry_id).is_some(),
                "entry {entry_id} must be recoverable by ID"
            );
        }
    }
}

/// Ledger entry payloads round-trip correctly.
#[tokio::test]
async fn ledger_entry_payload_roundtrip() {
    let dir = data_dir("payload");
    let tenant_id = TenantId::new();

    let payload = b"important audit data with unicode: \xc3\xa9l\xc3\xa8ve".to_vec();
    let entry_id = LedgerEntryId::new();

    {
        let clock = AdvancableClock::new(1000);
        let engine = ActionQueueEngine::new(make_config(dir.clone()), NoopHandler);
        let mut boot = engine.bootstrap_with_clock(clock).expect("bootstrap");
        boot.create_tenant(TenantRegistration::new(tenant_id, "Corp")).expect("tenant");
        let entry = LedgerEntry::new(entry_id, tenant_id, "audit", payload.clone(), 1000);
        boot.append_ledger_entry(entry).expect("append");
        boot.shutdown().expect("shutdown");
    }

    {
        let clock = AdvancableClock::new(2000);
        let engine = ActionQueueEngine::new(make_config(dir), NoopHandler);
        let boot = engine.bootstrap_with_clock(clock).expect("bootstrap");

        let recovered = boot.ledger().entry_by_id(entry_id).expect("entry present");
        assert_eq!(recovered.payload(), payload.as_slice(), "payload must match exactly");
        assert_eq!(recovered.ledger_key(), "audit");
        assert_eq!(recovered.tenant_id(), tenant_id);
    }
}
