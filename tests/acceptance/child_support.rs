include!("resume_support.rs");
use actionqueue_core::{admission::AdmissionPlan, disposition::*, task::run_policy::RunPolicy};
use actionqueue_storage::mutation::disposition::DispositionRejection;
fn child(n: u64, deps: Vec<TaskId>, policy: ChildLifecyclePolicy, parent: TaskId) -> ChildAdmission {
    let mut spec = admission_support::request(n).task_spec().clone();
    spec.set_run_policy(RunPolicy::Once).unwrap();
    ChildAdmission::new(AdmissionKey::new(format!("local/{n}")).unwrap(),
        spec.with_parent_policy(parent, policy), deps, Default::default()).unwrap()
}
fn parent(a: &s::Authority, run: RunId) -> TaskId { a.projection().get_run_instance(&run).unwrap().task_id() }
fn child_disposition(a: &s::Authority, run: RunId, children: Vec<ChildAdmission>, targets: Vec<TaskId>, policy: ChildWaitPolicy) -> AttemptDisposition {
    AttemptDisposition::new(DispositionOutcome::Awaiting, DispositionParts {
        wait: Some(WaitSpec::children(WaitId::new(), targets, policy, None).unwrap()),
        checkpoint: Some(checkpoint(a, run, b"retained child IDs and next batch")),
        child_admissions: children, ..Default::default()
    }).unwrap()
}
fn proposal(a: &s::Authority, run: RunId, d: AttemptDisposition, now: u64) -> AttemptDispositionCommitCommand {
    let expected = command(a, run, spec(WaitId::new(), None)).expected;
    let plans = d.child_admissions().iter().map(|c| {
        let q = a.projection().disposition_child_request(run, expected.attempt_id(), c).unwrap();
        let digest = q.digest().unwrap();
        if a.projection().admission(q.task_spec().tenant_id(), q.admission_key()).is_some() {
            AdmissionPlan::new(q, vec![], digest).unwrap()
        } else { actionqueue_engine::admission::plan_admission(q, digest, now).unwrap() }
    }).collect();
    AttemptDispositionCommitCommand::new(expected, d, now).with_children(plans)
}
fn put(a: &mut s::Authority, run: RunId, d: AttemptDisposition, now: u64) {
    let c = proposal(a, run, d, now);
    let _ = apply(a, MutationCommand::AttemptDispositionCommit(c));
}
fn execute_run(a: &mut s::Authority, run: RunId, d: AttemptDisposition, now: u64) {
    transition(a, run, RunState::Ready, now);
    lease(a, run, now);
    start(a, run, now);
    put(a, run, d, now);
}
fn finish_child(a: &mut s::Authority, id: TaskId, success: bool, now: u64) {
    let runs: Vec<_> = a.projection().runs_for_task(id).filter(|r| !r.state().is_terminal()).map(|r| r.id()).collect();
    for run in runs {
        execute_run(a, run, if success { AttemptDisposition::complete(None) } else { AttemptDisposition::terminal_failure(BoundedError::new("child_failed").unwrap()) }, now);
    }
}
/// Completed work is immutable history: cancel is rejected and nothing is appended.
fn reject_terminal_cancel(a: &mut s::Authority, target: CancelTarget, now: u64) {
    let before = a.projection().projection_digest().unwrap();
    let sequence = seq(a);
    let tenant_id = match target {
        CancelTarget::Task(id) => a.projection().get_task(&id).unwrap().tenant_id(),
        CancelTarget::Run(id) => a.projection().get_task(&a.projection().get_run_instance(&id).unwrap().task_id()).unwrap().tenant_id(),
    };
    let c = MutationCommand::Cancel(CancelCommand { expected_sequence: sequence, target, tenant_id, control_context: None, timestamp: now });
    assert!(matches!(
        a.submit_command(fixture_control(c), DurabilityPolicy::Immediate),
        Err(actionqueue_storage::mutation::MutationAuthorityError::Wait(WaitRejection::AlreadyTerminal))
    ));
    assert_eq!(a.projection().latest_sequence() + 1, sequence);
    assert_eq!(a.projection().projection_digest().unwrap(), before);
}
fn control_task(a: &mut s::Authority, id: TaskId, now: u64) {
    let _ = apply(a, MutationCommand::Cancel(CancelCommand { expected_sequence: seq(a), target: CancelTarget::Task(id), tenant_id: a.projection().get_task(&id).unwrap().tenant_id(), control_context: None, timestamp: now }));
}
fn reject_unchanged(a: &mut s::Authority, run: RunId, d: AttemptDisposition) {
    let before = a.projection().projection_digest().unwrap();
    let c = proposal(a, run, d, 20);
    assert!(a.submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate).is_err());
    assert_eq!(a.projection().projection_digest().unwrap(), before);
}
