//! ActionQueue-owned two-store reference workload, not certification of a downstream repository.
#[path = "harness/engine.rs"]
mod engine;
use actionqueue_core::{
    admission::EnsureTaskOutcome, continuation::*, data_ref::DataRef,
    disposition::AttemptDisposition, ids::*, run::RunState, task::run_policy::RunPolicy,
    time::clock::MockClock,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
    inspection::{DisclosurePolicy, Query},
    views::TraceNode,
};
#[derive(Clone)]
struct Handler {
    coordinator: bool,
}
impl ExecutorHandler for Handler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        if !self.coordinator {
            return AttemptDisposition::complete(Some(
                DataRef::from_bytes(b"unverified answer".to_vec()).unwrap(),
            ));
        }
        if let Some(resume) = ctx.input.resume_context {
            assert!(matches!(resume.wake, WakeReason::Signal { .. }));
            assert!(resume.checkpoint.is_some());
            return AttemptDisposition::complete(None);
        }
        AttemptDisposition::awaiting(
            WaitSpec::new(
                WaitId::new(),
                engine::reference_filter(),
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::After(SignalSequence::new(0)),
                None,
            )
            .unwrap(),
            Some(CheckpointRef {
                checkpoint_id: CheckpointId::new(),
                created_by_attempt: ctx.input.attempt_id,
                data: DataRef::from_bytes(b"external effect requires reconciliation".to_vec())
                    .unwrap(),
            }),
        )
    }
}
fn boot(path: &std::path::Path, coordinator: bool) -> BootstrappedEngine<Handler, MockClock> {
    ActionQueueEngine::new(
        RuntimeConfig { data_dir: path.into(), ..Default::default() },
        Handler { coordinator },
    )
    .bootstrap_with_clock(MockClock::new(1000))
    .unwrap()
    .with_host(engine::host())
}
async fn idle(b: &mut BootstrappedEngine<Handler, MockClock>) {
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), b.run_until_idle())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn two_stores_lost_admission_duplicate_callback_early_signal_and_uncertain_effect() {
    for early in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let local_path = dir.path().join("local");
        let remote_path = dir.path().join("remote");
        let mut local = boot(&local_path, true);
        let mut remote = boot(&remote_path, false);
        let request = |n| {
            let q = engine::reference_request(n);
            let mut t = q.task_spec().clone();
            t.set_run_policy(RunPolicy::Once).unwrap();
            engine::reference_with_spec(&q, t)
        };
        let l = request(1);
        let r = request(2);
        local.ensure_task(l.clone()).unwrap();
        // Producer loses the successful admission response; neither side depends on it.
        remote.ensure_task(r.clone()).unwrap();
        if !early {
            idle(&mut local).await;
            assert_eq!(
                local.projection().run_instances().next().unwrap().state(),
                RunState::Awaiting
            );
        }
        local.shutdown().unwrap();
        remote.shutdown().unwrap();
        let mut local = boot(&local_path, true);
        let mut remote = boot(&remote_path, false);
        assert!(matches!(
            remote.ensure_task(r.clone()).unwrap(),
            EnsureTaskOutcome::AlreadyExists { .. }
        ));
        let mut changed = r.task_spec().clone();
        changed
            .set_payload(actionqueue_core::task::task_spec::TaskPayload::new(b"changed".to_vec()));
        let before = remote.projection().projection_digest().unwrap();
        assert_eq!(
            remote.ensure_task(engine::reference_with_spec(&r, changed)).unwrap_err().code(),
            "conflict"
        );
        assert_eq!(remote.projection().projection_digest().unwrap(), before);
        if !early {
            assert_eq!(
                local.projection().run_instances().next().unwrap().state(),
                RunState::Awaiting
            );
        }
        idle(&mut remote).await;
        let remote_run = remote.projection().run_instances().next().unwrap();
        assert_eq!(remote_run.state(), RunState::Completed);
        let output = remote.projection().get_attempt_history(&remote_run.id()).unwrap()[0]
            .output_ref()
            .unwrap();
        // Application-owned evaluation rejects this answer. It has no queue mutation authority.
        let verifier_accepts =
            |output: &DataRef| matches!(output, DataRef::Inline(data) if data.bytes() == b"4");
        assert!(!verifier_accepts(output));
        assert_eq!(remote_run.state(), RunState::Completed);
        let producer =
            remote.projection().get_attempt_history(&remote_run.id()).unwrap()[0].attempt_id();
        // Concrete task/run/attempt links are store-local. Cross-store provenance
        // uses the contract's opaque external causation reference.
        let causation = actionqueue_core::causal::CausationLink::new(
            None,
            None,
            None,
            Some(
                actionqueue_core::bounded::OpaqueRef::new(format!(
                    "remote-store/{}/{}/{}",
                    r.task_spec().id(),
                    remote_run.id(),
                    producer
                ))
                .unwrap(),
            ),
        )
        .unwrap();
        let callback = AdmitSignalRequest::new(
            SignalId::new("signal/1").unwrap(),
            SignalNamespace::new("remote").unwrap(),
            SignalKind::new("complete").unwrap(),
            Some(l.causal_context().correlation_id().clone()),
            Some(causation.clone()),
            Some(actionqueue_core::bounded::OpaqueRef::new("worldinterface-owned-output").unwrap()),
            Some(output.clone()),
            None,
            None,
        )
        .unwrap();
        let first = local.admit_signal(callback.clone()).unwrap();
        let duplicate = local.admit_signal(callback.clone()).unwrap();
        assert_eq!(first.sequence(), duplicate.sequence());
        assert!(matches!(duplicate, AdmitSignalOutcome::AlreadyExists { .. }));
        idle(&mut local).await;
        let run = local.projection().run_instances().next().unwrap();
        assert_eq!(run.state(), RunState::Completed);
        assert_eq!(run.attempt_count(), 2);
        assert_eq!(run.failure_attempt_count(), 0);
        let contexts: Vec<_> = local
            .projection()
            .get_attempt_history(&run.id())
            .unwrap()
            .iter()
            .filter_map(|a| local.projection().attempt_resume(run.id(), a.attempt_id()))
            .collect();
        assert_eq!(contexts.len(), 1);
        assert_eq!(
            local
                .projection()
                .signals()
                .get_signal(None, &SignalId::new("signal/1").unwrap())
                .unwrap()
                .envelope()
                .causation,
            Some(causation.clone())
        );
        let digest = local.projection().projection_digest().unwrap();
        local.shutdown().unwrap();
        remote.shutdown().unwrap();
        let mut local = boot(&local_path, true);
        assert_eq!(local.projection().projection_digest().unwrap(), digest);
        local.admit_signal(callback).unwrap();
        assert_eq!(
            local
                .projection()
                .signals()
                .get_signal(None, &SignalId::new("signal/1").unwrap())
                .unwrap()
                .envelope()
                .causation,
            Some(causation)
        );
        idle(&mut local).await;
        assert_eq!(local.projection().projection_digest().unwrap(), digest);
        local.shutdown().unwrap();
    }
}
/// Admits one coordinator task whose first attempt establishes a wait.
fn once_request(n: u64) -> actionqueue_core::admission::EnsureTaskRequest {
    let q = engine::reference_request(n);
    let mut once = q.task_spec().clone();
    once.set_run_policy(RunPolicy::Once).unwrap();
    engine::reference_with_spec(&q, once)
}
/// The bound host alone never discloses references; every embedded inspection
/// convenience reads the same structural views the inspector does, and resolving a
/// wait through the embedded surface settles it without a signal and keeps the
/// run resumable until the run itself is canceled.
#[tokio::test]
async fn embedded_disclosure_and_wait_resolution_use_the_bound_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = boot(dir.path(), true);
    let q = once_request(1);
    let task = q.task_spec().id();
    let key = q.admission_key().clone();
    let trace_id = q.causal_context().trace_id().to_string();
    b.ensure_task(q).unwrap();
    idle(&mut b).await;
    let run = b.projection().run_instances().next().unwrap().id();
    let wait = b.projection().waits().active(run).unwrap().spec.wait_id();
    assert_eq!(b.get_admission(&key).unwrap().task_id, task);
    assert_eq!(b.get_task(task).unwrap().id, task);
    let run_view = b.get_run(run).unwrap();
    assert_eq!(run_view.state, RunState::Awaiting);
    let attempt = run_view.attempts.items[0].attempt_id;
    assert_eq!(b.get_attempt(run, attempt).unwrap().attempt_id, attempt);
    let wait_view = b.get_wait(wait).unwrap();
    assert_eq!((wait_view.run_id, wait_view.attempt_id), (run, attempt));
    let checkpoint = wait_view.checkpoint_id.unwrap();
    assert_eq!(b.get_checkpoint(checkpoint).unwrap().attempt_id, attempt);
    let waits = b.list_waits(&Query::default()).unwrap();
    assert_eq!(waits.items.iter().map(|w| w.wait_id).collect::<Vec<_>>(), vec![wait]);
    let trace = b.trace(&Query { trace_id: Some(trace_id), ..Default::default() }).unwrap();
    let kinds: Vec<_> = trace
        .nodes
        .items
        .iter()
        .map(|n| match n {
            TraceNode::Task(_) => "task",
            TraceNode::Run(_) => "run",
            TraceNode::Attempt(_) => "attempt",
            TraceNode::Checkpoint(_) => "checkpoint",
            TraceNode::Wait(_) => "wait",
            TraceNode::Signal(_) => "signal",
        })
        .collect();
    assert_eq!(kinds, ["task", "run", "attempt", "checkpoint", "wait"]);
    let redacted = serde_json::to_string(&b.inspector().unwrap().get_task(task).unwrap()).unwrap();
    assert!(b.inspector_with_disclosure(DisclosurePolicy::default()).is_err());
    let disclosed =
        b.inspector_with_disclosure(DisclosurePolicy { allow_references: true }).unwrap();
    assert_ne!(serde_json::to_string(&disclosed.get_task(task).unwrap()).unwrap(), redacted);
    b.resolve_wait(run, wait).unwrap();
    assert!(b.projection().waits().active(run).is_none());
    let record = b.projection().waits().records().find(|w| w.run_id == run).unwrap();
    assert!(matches!(
        record.resolution.as_ref().unwrap().kind,
        actionqueue_storage::mutation::wait::WaitResolutionKind::Control(_)
    ));
    assert!(b.projection().pending_resume(run).is_some());
    b.cancel_run(run).unwrap();
    assert_eq!(b.get_run(run).unwrap().state, RunState::Canceled);
    b.shutdown().unwrap();
}
/// Canceling a wait through the embedded surface is a control resolution that
/// never wakes the run; a later signal is admitted and listed but matches nothing.
#[tokio::test]
async fn embedded_wait_cancellation_and_signal_listing_use_the_bound_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = boot(dir.path(), true);
    b.ensure_task(once_request(1)).unwrap();
    idle(&mut b).await;
    let run = b.projection().run_instances().next().unwrap().id();
    let wait = b.projection().waits().active(run).unwrap().spec.wait_id();
    b.cancel_wait(run, wait).unwrap();
    assert!(b.projection().waits().active(run).is_none());
    assert!(b.projection().pending_resume(run).is_none());
    let state = b.get_run(run).unwrap().state;
    assert!(state.is_terminal(), "{state:?}");
    let AdmitSignalOutcome::Admitted { signal_id, sequence } =
        b.admit_signal(engine::reference_signal(1)).unwrap()
    else {
        panic!("fresh signal");
    };
    assert_eq!(b.get_signal(&signal_id).unwrap().sequence, sequence);
    let listed = b.list_signals(&Query::default()).unwrap();
    assert_eq!(listed.items.iter().map(|s| s.sequence).collect::<Vec<_>>(), vec![sequence]);
    assert!(b.get_wait(wait).unwrap().resolution.unwrap().signal_sequence.is_none());
    b.shutdown().unwrap();
}
