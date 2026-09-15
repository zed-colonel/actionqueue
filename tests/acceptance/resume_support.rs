include!("wait_support.rs");
use actionqueue_core::data_ref::{DataRef, InlineData};
use actionqueue_storage::{
    recovery::reducer::ReplayReducer,
    snapshot::writer::{SnapshotFsWriter, SnapshotWriter},
};
fn checkpoint(a: &s::Authority, run: RunId, bytes: &[u8]) -> CheckpointRef {
    CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        created_by_attempt: a
            .projection()
            .get_run_instance(&run)
            .unwrap()
            .current_attempt_id()
            .unwrap(),
        data: DataRef::Inline(
            InlineData::new(
                None,
                bytes.to_vec(),
                ContentHash::new(HashAlgorithm::Sha256, sha2::Sha256::digest(bytes).to_vec())
                    .unwrap(),
            )
            .unwrap(),
        ),
    }
}
fn wake(a: &mut s::Authority) -> (RunId, ResumeContext) {
    let r = running(a, 1, None, false);
    let mut c = command(a, r, spec(WaitId::new(), None));
    c.checkpoint = Some(checkpoint(a, r, b"private continuation"));
    establish(a, c).unwrap();
    let _ = s::submit(a, s::envelope(1, 25)).unwrap();
    reconcile(a, 30).unwrap();
    (r, a.projection().pending_resume(r).unwrap())
}
fn lease(a: &mut s::Authority, r: RunId, at: u64) {
    transition(a, r, RunState::Leased, at);
    commit!(
        a,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(seq(a), r, "worker", at + 1000, at))
    );
    transition(a, r, RunState::Running, at);
}
fn start_command(a: &s::Authority, r: RunId, at: u64) -> AttemptStartCommand {
    let l = a.projection().get_lease_metadata(&r).unwrap();
    AttemptStartCommand::new(
        seq(a),
        r,
        AttemptId::new(),
        at,
        LeaseFence::new(l.owner().into(), l.granted_at_sequence()),
        a.projection().pending_resume(r).map(|c| c.context_id),
    )
}
fn start(a: &mut s::Authority, r: RunId, at: u64) -> AttemptId {
    let c = start_command(a, r, at);
    let id = c.attempt_id();
    let result = apply(a, MutationCommand::AttemptStart(c));
    assert!(matches!(result.applied(), AppliedMutation::AttemptStart { .. }));
    id
}
fn parity(a: &s::Authority) {
    let session = a.store_session().unwrap();
    let live = a.projection().projection_digest().unwrap();
    use actionqueue_storage::wal::{fs_reader::WalFsReader, reader::WalReader};
    let mut reader = WalFsReader::for_session(session).unwrap();
    let mut replay = ReplayReducer::new();
    while let Some(e) = reader.read_next().unwrap() {
        replay.apply(&e).unwrap();
    }
    assert_eq!(live, replay.projection_digest().unwrap());
    assert_eq!(
        live,
        recover_read_only(session, RepairPolicy::Strict)
            .unwrap()
            .projection
            .projection_digest()
            .unwrap()
    );
    let snapshot = build_snapshot_from_projection(a.projection(), 0).unwrap();
    let mut writer = SnapshotFsWriter::new(session).unwrap();
    writer.write(&snapshot).unwrap();
    writer.close().unwrap();
    assert_eq!(
        live,
        recover_read_only(session, RepairPolicy::Strict)
            .unwrap()
            .projection
            .projection_digest()
            .unwrap()
    );
}
#[derive(Clone)]
struct Recording(std::sync::Arc<Mutex<Vec<actionqueue_executor_local::handler::HandlerInput>>>);
impl actionqueue_executor_local::handler::ExecutorHandler for Recording {
    fn execute(
        &self,
        c: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_executor_local::handler::AttemptDisposition {
        self.0.lock().unwrap().push(c.input);
        actionqueue_core::disposition::AttemptDisposition::complete(None)
    }
}
fn observe(
    p: &ReplayReducer,
    r: RunId,
    id: AttemptId,
) -> actionqueue_executor_local::handler::HandlerInput {
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let runner =
        actionqueue_executor_local::attempt_runner::AttemptRunner::new(Recording(seen.clone()));
    let run = p.get_run_instance(&r).unwrap();
    let task = p.get_task(&run.task_id()).unwrap();
    let _ = runner.run_attempt(actionqueue_executor_local::types::ExecutorRequest {
            lease_fence: actionqueue_core::mutation::LeaseFence::new("test".into(), 1),
            failure_attempt_count: run.failure_attempt_count(),
        run_id: r,
        attempt_id: id,
        payload: task.task_payload().bytes().to_vec(),
        constraints: task.constraints().clone(),
        attempt_number: run.attempt_count(),
        resume_context: p.attempt_resume(r, id),
        causal_context: p
            .task_admission(run.task_id())
            .map(|a| a.request().causal_context().clone()),

        children: None,
        cancellation_context: None,
    });
    let input = seen.lock().unwrap().pop().unwrap();
    input
}

fn resume_dir() -> tempfile::TempDir { tempfile::tempdir().unwrap() }
