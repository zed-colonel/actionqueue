//! Runtime coordination costs, including duplicate admissions and cancellation cascades.
#[path = "support/surface_history.rs"]
mod surface_history;
use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::OpaqueRef,
    causal::{CausalContext, ControlMutationContext},
    control::{ControlScope, HostControlContext},
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
    mutation::{CancelCommand, CancelTarget},
    task::{
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
    time::clock::MockClock,
};
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::BackoffStrategyConfig,
    dispatch::{DispatchConfig, DispatchLoop},
};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::work,
    wal::{fs_writer::WalFsWriter, writer::WalWriter},
};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
fn request() -> EnsureTaskRequest {
    let task_id = TaskId::new();
    EnsureTaskRequest::new(
        AdmissionKey::new(task_id.to_string()).unwrap(),
        TaskSpec::new(
            task_id,
            TaskPayload::new(vec![1, 2, 3]),
            RunPolicy::Once,
            Default::default(),
            Default::default(),
        )
        .unwrap(),
        vec![],
        CausalContext::new(
            TraceId::new("surface").unwrap(),
            CorrelationId::new("surface").unwrap(),
        ),
        None,
    )
    .unwrap()
}
fn measured(f: impl FnOnce()) -> serde_json::Value {
    work::reset_projection_work();
    let at = Instant::now();
    f();
    let ns = at.elapsed().as_nanos();
    let w = work::projection_work();
    assert!(w.record_copies <= 64 * 100, "record copying must be bounded per operation");
    serde_json::json!({"operations":100,"mean_ns_per_operation":ns as f64/100.,"shallow_projection_clones":w.shallow_projection_clones,"record_copies":w.record_copies,"subscription_candidates":w.subscription_candidates,"cancellation_run_candidates":w.cancellation_run_candidates})
}
fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .map(|arg| arg.parse().expect("supply sizes as integer arguments"))
        .collect();
    assert!(!sizes.is_empty());
    let mut results = Vec::new();
    for n in sizes {
        let dir = tempfile::tempdir().unwrap();
        let p = surface_history::history(n);
        let a = StorageMutationAuthority::new(
            WalFsWriter::new_raw_for_test(dir.path().join("surface.wal")).unwrap(),
            p,
        )
        .with_host(HostControlContext {
            actor_id: None,
            scope: ControlScope::SingleTenant,
            attribution: ControlMutationContext::new(OpaqueRef::new("surface").unwrap()),
        });
        let mut d = DispatchLoop::new(
            a,
            Complete,
            MockClock::new(1000),
            DispatchConfig::new(
                BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
                1,
                30,
                None,
                None,
            ),
        )
        .unwrap();
        let requests: Vec<_> = (0..100).map(|_| request()).collect();
        let admission = measured(|| {
            for q in &requests {
                assert!(d.ensure_task(q.clone()).unwrap().is_created());
            }
        });
        let duplicates = measured(|| {
            for q in &requests {
                assert!(!black_box(d.ensure_task(q.clone()).unwrap()).is_created());
            }
        });
        let cancellation = measured(|| {
            for q in requests {
                d.cancel(CancelCommand {
                    expected_sequence: d.projection().latest_sequence() + 1,
                    target: CancelTarget::Task(q.task_spec().id()),
                    tenant_id: None,
                    control_context: None,
                    timestamp: 1001,
                })
                .unwrap();
            }
        });
        let (writer, _) = d.into_authority().into_parts();
        writer.close().unwrap();
        results.push(serde_json::json!({"retained_terminal_runs":n,"admission":admission,"duplicates":duplicates,"cancellation":cancellation}));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({"schema_version":1,"results":results}))
            .unwrap()
    );
}
