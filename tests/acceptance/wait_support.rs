use sha2::Digest;
mod admission_support;
mod signal_support;
use actionqueue_core::{
    bounded::*,
    causal::*,
    continuation::*,
    ids::*,
    mutation::*,
    run::RunState,
    task::{constraints::*, task_spec::*},
    time::clock::MockClock,
};
use actionqueue_runtime::waits::*;
use actionqueue_storage::{
    mutation::MutationAuthorityError,
    recovery::bootstrap::recover_read_only,
    snapshot::build::build_snapshot_from_projection,
    store::{open_store, OpenOptions},
    wal::repair::RepairPolicy,
};
use signal_support as s;
use std::sync::Mutex;
static FAULTS: Mutex<()> = Mutex::new(());
fn seq(a: &s::Authority) -> u64 {
    a.projection().latest_sequence() + 1
}
fn apply(a: &mut s::Authority, c: MutationCommand) -> MutationOutcome {
    a.submit_command(c, DurabilityPolicy::Immediate).unwrap()
}
macro_rules! commit {
    ($a:expr,$c:expr) => {{
        let c = $c;
        apply($a, c).sequence()
    }};
}
fn running(a: &mut s::Authority, n: u64, key: Option<&str>, hold: bool) -> RunId {
    running_scoped(a,n,key,hold,None)
}
fn running_scoped(a:&mut s::Authority,n:u64,key:Option<&str>,hold:bool,tenant:Option<TenantId>)->RunId {
    let q = admission_support::request(n);
    let t = q.task_spec();
    let mut constraints = TaskConstraints::new(3, None, key.map(str::to_owned)).unwrap();
    if hold {
        constraints.set_concurrency_key_wait_policy(ConcurrencyKeyWaitPolicy::HoldWhileAwaiting);
    }
    let spec = TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        t.run_policy().clone(),
        constraints,
        t.metadata().clone(),
    )
    .unwrap();
    let spec=if let Some(t)=tenant {spec.with_tenant(t)}else{spec};
    let q = admission_support::with_spec(&q, spec);
    let _ = admission_support::ensure(a, q, 10).unwrap();
    let run = a.projection().runs_for_task(t.id()).next().unwrap().id();
    transition(a, run, RunState::Ready, 11);
    transition(a, run, RunState::Leased, 12);
    commit!(
        a,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(seq(a), run, "worker", 1000, 12))
    );
    transition(a, run, RunState::Running, 13);
    commit!(
        a,
        MutationCommand::AttemptStart(AttemptStartCommand::new(seq(a), run, AttemptId::new(), 13, a.projection().get_lease_metadata(&run).map(|l| actionqueue_core::mutation::LeaseFence::new(l.owner().into(), l.granted_at_sequence())).unwrap_or_else(|| actionqueue_core::mutation::LeaseFence::new("missing".into(), 0)), a.projection().pending_resume(run).map(|c| c.context_id)))
    );
    run
}
fn transition(a: &mut s::Authority, r: RunId, to: RunState, now: u64) {
    let from = *a.projection().get_run_state(&r).unwrap();
    commit!(
        a,
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            seq(a),
            r,
            from,
            to,
            now
        ))
    );
}
fn spec(id: WaitId, deadline: Option<WaitDeadline>) -> WaitSpec {
    WaitSpec::new(
        id,
        s::filter(),
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        deadline,
    )
    .unwrap()
}
fn command(a: &s::Authority, run: RunId, w: WaitSpec) -> WaitEstablishCommand {
    let attempt = a.projection().get_run_instance(&run).unwrap().current_attempt_id().unwrap();
    let lease = a.projection().get_lease_metadata(&run).unwrap();
    WaitEstablishCommand {
        expected: AttemptCommitExpectation::new(
            seq(a),
            run,
            attempt,
            RunState::Running,
            LeaseFence::new(LeaseOwner::new("worker"), lease.granted_at_sequence()),
        ),
        wait: w,
        checkpoint: None,
        timestamp: 20,
    }
}
fn establish_wait(a: &mut s::Authority, r: RunId, w: WaitSpec) -> WaitEstablishCommand {
    let c = command(a, r, w);
    establish(a, c.clone()).unwrap();
    c
}
#[allow(dead_code)]
fn satisfy(
    a: &mut s::Authority,
    r: RunId,
    w: WaitId,
    signal: u64,
) -> Result<MutationOutcome, WaitError> {
    a.submit_command(
        MutationCommand::WaitSatisfy(WaitSatisfyCommand::new(
            seq(a),
            r,
            w,
            SignalSequence::new(signal),
            30,
        )),
        DurabilityPolicy::Immediate,
    )
}
fn timeout(
    a: &mut s::Authority,
    r: RunId,
    w: WaitId,
    now: u64,
) -> Result<MutationOutcome, WaitError> {
    a.submit_command(
        MutationCommand::WaitTimeout(WaitTimeoutCommand::new(seq(a), r, w, now)),
        DurabilityPolicy::Immediate,
    )
}
fn cancel(a: &mut s::Authority, r: RunId) {
    commit!(
        a,
        MutationCommand::Cancel(CancelCommand {
            expected_sequence: seq(a),
            target: CancelTarget::Run(r),
            tenant_id: None,
            control_context: None,
            timestamp: 31
        })
    );
}
fn assert_invariants(a: &s::Authority) {
    for run in a.projection().run_instances() {
        let active = a.projection().waits().active(run.id());
        assert_eq!(active.is_some(), run.state() == RunState::Awaiting);
        if active.is_some() {
            assert!(a.projection().get_lease(&run.id()).is_none());
            assert!(run.current_attempt_id().is_none());
        }
        if a.projection().pending_resume(run.id()).is_some() {
            assert_eq!(run.state(), RunState::Ready);
        }
        if run.state().is_terminal() {
            assert!(active.is_none());
            assert!(a.projection().pending_resume(run.id()).is_none());
        }
    }
}
