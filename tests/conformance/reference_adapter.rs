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
        let callback = engine::reference_signal(1);
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
        let digest = local.projection().projection_digest().unwrap();
        local.shutdown().unwrap();
        remote.shutdown().unwrap();
        let mut local = boot(&local_path, true);
        assert_eq!(local.projection().projection_digest().unwrap(), digest);
        local.admit_signal(callback).unwrap();
        idle(&mut local).await;
        assert_eq!(local.projection().projection_digest().unwrap(), digest);
        local.shutdown().unwrap();
    }
}
