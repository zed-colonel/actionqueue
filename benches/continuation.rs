//! Reproducible measurements; elapsed time is evidence, never a public throughput claim.
#[path = "../tests/conformance/harness/engine.rs"]
mod engine;
#[path = "../tests/conformance/harness/package.rs"]
mod package;
use actionqueue_core::{
    disposition::AttemptDisposition, task::run_policy::RunPolicy, time::clock::MockClock,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use actionqueue_storage::wal::{event::WalEventType, fs_reader::WalFsReader, reader::WalReader};
use engine::{Embedded, Step};
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _ctx: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}

use std::time::Instant;

use serde_json::json;
fn timed<T>(f: impl FnOnce() -> T) -> (T, u128) {
    let t = Instant::now();
    let value = f();
    (value, t.elapsed().as_nanos())
}
fn main() {
    package::validate(&package::root()).unwrap();
    let input: serde_json::Value = serde_json::from_slice(
        &std::fs::read(package::root().join("performance/workloads.json")).unwrap(),
    )
    .unwrap();
    let mut results = Vec::new();
    for (index, n) in
        input["sizes"].as_array().unwrap().iter().map(|v| v.as_u64().unwrap()).enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut normal = ActionQueueEngine::new(
            RuntimeConfig { data_dir: dir.path().join("normal"), ..Default::default() },
            Complete,
        )
        .bootstrap_with_clock(MockClock::new(1000))
        .unwrap()
        .with_host(engine::host());
        let requests: Vec<_> = (1..=n)
            .map(|id| {
                let q = engine::reference_request(id);
                let mut t = q.task_spec().clone();
                t.set_run_policy(RunPolicy::Once).unwrap();
                engine::reference_with_spec(&q, t)
            })
            .collect();
        let (_, admission_ns) = timed(|| {
            for q in requests {
                normal.ensure_task(q).unwrap();
            }
        });
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let (_, plain_completion_ns) = timed(|| {
            let _ = runtime.block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(30), normal.run_until_idle())
                    .await
                    .unwrap()
                    .unwrap()
            });
        });
        assert!(normal
            .projection()
            .run_instances()
            .all(|r| r.state() == actionqueue_core::run::RunState::Completed));
        normal.shutdown().unwrap();
        let mut unmatched = Embedded::new(&dir.path().join("unmatched"));
        let (_, unmatched_signal_ns) = timed(|| {
            for signal in 1..=n {
                unmatched.apply_step(&Step::Signal { signal });
            }
        });
        assert_eq!(unmatched.a().projection().signals().statistics().retained, n as usize);
        let mut d = Embedded::new(&path);
        let (_, start_ns) = timed(|| {
            for task in 1..=n {
                d.apply_step(&Step::Start { task });
            }
        });
        let (_, wait_ns) = timed(|| {
            for task in 1..=n {
                d.apply_step(&Step::Wait { task, deadline: None });
            }
        });
        assert_eq!(d.a().projection().waits().active_count(), n as usize);
        let (_, fanout_ns) = timed(|| {
            d.apply_step(&Step::Signal { signal: 1 });
        });
        assert_eq!(d.a().projection().waits().active_count(), 0);
        assert_eq!(
            d.a().projection().waits().records().filter(|w| w.resolution.is_some()).count(),
            n as usize
        );
        // Completed match indexes must not retain unrelated historical candidates.
        assert!(d.a().projection().waits().matches(128).is_empty());
        let (_, completion_ns) = timed(|| {
            for task in 1..=n {
                d.apply_step(&Step::Finish { task, success: true });
            }
        });
        let (_, snapshot_ns) = timed(|| {
            d.apply_step(&Step::Snapshot);
        });
        let wal_bytes = std::fs::metadata(d.a().store_session().unwrap().wal_path()).unwrap().len();
        let snapshot_bytes =
            std::fs::metadata(d.a().store_session().unwrap().snapshot_path()).unwrap().len();
        let mut child_batch = Embedded::new(&dir.path().join("compound"));
        child_batch.apply_step(&Step::Start { task: 1 });
        let batch = input["compound_children"][index].as_u64().unwrap();
        child_batch.apply_step(&Step::Fanout { task: 1, children: (2..batch + 2).collect() });
        assert_eq!(child_batch.a().projection().task_count(), batch as usize + 1);
        assert_eq!(child_batch.a().projection().waits().active_count(), 1);
        let mut reader =
            WalFsReader::for_session(child_batch.a().store_session().unwrap()).unwrap();
        let mut compound = Vec::new();
        while let Some(event) = reader.read_next().unwrap() {
            if matches!(event.event(), WalEventType::AttemptDispositionCommitted { .. }) {
                compound.push(event);
            }
        }
        let (compound_bytes, serialization_ns) = timed(|| {
            compound
                .iter()
                .map(|e| {
                    let bytes = actionqueue_storage::wal::codec::encode(e).unwrap().len();
                    assert!(bytes <= actionqueue_core::limits::MAX_ADMISSION_RECORD_BYTES);
                    bytes
                })
                .sum::<usize>()
        });
        assert_eq!(compound.len(), 1);
        if index == 0 {
            let mut rejected = Embedded::new(&dir.path().join("oversized"));
            rejected.apply_step(&Step::Start { task: 1 });
            rejected.assert_rejected_fanout(
                (2..input["rejected_compound_children"].as_u64().unwrap() + 2).collect(),
            );
        }
        let digest = d.a().projection().projection_digest().unwrap();
        drop(d);
        let (recovered, recovery_ns) = timed(|| Embedded::reopen(&path));
        assert_eq!(recovered.a().projection().projection_digest().unwrap(), digest);
        results.push(json!({"tasks":n,"compound_children":batch,"ordinary_admission_ns":admission_ns,"ordinary_ready_selection_handler_dispatch_completion_ns":plain_completion_ns,"unmatched_signal_admission_ns":unmatched_signal_ns,"compound_serialization_ns":serialization_ns,"compound_serialization_bytes":compound_bytes,"admission_ready_and_accepted_start_ns":start_ns,"unmatched_wait_ns":wait_ns,"matching_fanout_ns":fanout_ns,"resumed_completion_ns":completion_ns,"snapshot_build_ns":snapshot_ns,"snapshot_bytes":snapshot_bytes,"wal_bytes":wal_bytes,"snapshot_tail_recovery_ns":recovery_ns}));
    }
    let rustc = std::process::Command::new("rustc").arg("-Vv").output().unwrap();
    let report = json!({"schema_version":1,"workload_hash":package::hash(&std::fs::read(package::root().join("performance/workloads.json")).unwrap()),"rustc":String::from_utf8_lossy(&rustc.stdout),"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"debug_assertions":cfg!(debug_assertions),"parallelism":std::thread::available_parallelism().map(|n|n.get()).unwrap_or(1),"features":{"workflow":cfg!(feature="workflow"),"budget":cfg!(feature="budget"),"actor":cfg!(feature="actor"),"platform":cfg!(feature="platform")},"measurements":results,"limitations":["Full projection preparation and full retained-WAL verification are included.","Elapsed-time measurements are informational; no machine-independent throughput gate."]});
    let path = std::env::var_os("AQ_PERFORMANCE_REPORT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("aq-cont-1-performance.json"));
    std::fs::write(&path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    println!("{}", path.display());
}
