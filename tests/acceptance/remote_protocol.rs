//! AQ-11 remote protocol services, exercising durable results and retransmission.
#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_actor::protocol::*;
use actionqueue_core::{
    actor::{ActorRegistration, ExecutorTraits},
    disposition::AttemptDisposition,
    disposition_digest::*,
};
use actionqueue_runtime::{control::*, remote};
fn host(id: ActorId) -> HostControlContext {
    HostControlContext {
        actor_id: Some(id),
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("host/test").unwrap()),
    }
}
fn setup(path: &std::path::Path) -> (s::Authority, HostControlContext, RunId) {
    let mut a = open_store(
        path,
        OpenOptions::Initialize {
            features: actionqueue_storage::store::capabilities()
                .into_iter()
                .filter(|f| f != "platform")
                .collect(),
        },
    )
    .unwrap()
    .into_authority()
    .unwrap()
    .with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    });
    let id = ActorId::new();
    commit!(
        &mut a,
        MutationCommand::ActorRegister(ActorRegisterCommand::new(
            seq(&a),
            ActorRegistration::new(
                id,
                "display-is-not-owner",
                ExecutorTraits::new(vec!["compute".into()]).unwrap(),
                30
            ),
            10
        ))
    );
    let _ = admission_support::ensure(&mut a, admission_support::request(1), 10).unwrap();
    let r = a.projection().runs_for_task(admission_support::id(1)).next().unwrap().id();
    (a, host(id), r)
}
fn request(r: RunId) -> RemoteClaim {
    RemoteClaim {
        protocol_version: PROTOCOL_VERSION,
        contract_revision: CONTRACT_REVISION.into(),
        run_id: r,
        attempt_id: AttemptId::new(),
    }
}
fn result(w: &remote::RemoteWork, d: AttemptDisposition) -> RemoteAttemptResult {
    RemoteAttemptResult {
        protocol_version: PROTOCOL_VERSION,
        contract_revision: CONTRACT_REVISION.into(),
        run_id: w.run_id,
        attempt_id: w.attempt_id,
        lease_fence: w.lease_fence.clone(),
        disposition_digest: disposition_digest(&d),
        disposition: d,
    }
}
#[test]
fn canonical_known_answer() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../conformance/aq-cont-1/disposition-v1-vector.json"
    ))
    .unwrap();
    let hex = |b: &[u8]| b.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let d = AttemptDisposition::complete(None);
    assert_eq!(hex(&canonical_disposition(&d)), fixture["canonical_hex"]);
    assert_eq!(hex(&disposition_digest(&d).0), fixture["sha256"]);
    for name in ["compound", "output"] {
        let d: AttemptDisposition =
            serde_json::from_value(fixture[name]["disposition"].clone()).unwrap();
        assert_eq!(hex(&canonical_disposition(&d)), fixture[name]["canonical_hex"], "{name}");
        assert_eq!(hex(&disposition_digest(&d).0), fixture[name]["sha256"], "{name}");
    }
}
#[test]
fn accepted_claim_and_result_retries_are_append_free_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let q = request(r);
    let w = remote::claim(&mut a, &h, q.clone(), 11, 30).unwrap();
    let before = seq(&a);
    assert_eq!(remote::claim(&mut a, &h, q.clone(), 12, 30).unwrap().lease_fence, w.lease_fence);
    assert_eq!(before, seq(&a));
    assert_eq!(w.lease_fence.owner(), &lease_owner(h.actor_id.unwrap()));
    let d = result(&w, AttemptDisposition::complete(None));
    remote::submit_result(&mut a, &h, d.clone(), 13).unwrap();
    let before = seq(&a);
    remote::submit_result(&mut a, &h, d.clone(), 90).unwrap();
    assert_eq!(seq(&a), before);
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    remote::submit_result(&mut a, &h, d.clone(), 90).unwrap();
    assert_eq!(seq(&a), before);
    let changed =
        result(&w, AttemptDisposition::terminal_failure(BoundedError::new("changed").unwrap()));
    assert!(remote::submit_result(&mut a, &h, changed, 90).is_err());
    assert_eq!(seq(&a), before);
}
#[test]
fn rejected_envelopes_leave_all_state_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let w = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../conformance/aq-cont-1/disposition-v1-vector.json"
    ))
    .unwrap();
    let mut proposal = fixture["compound"]["disposition"].clone();
    proposal["checkpoint"]["created_by_attempt"] = serde_json::json!(w.attempt_id);
    #[cfg(not(feature = "workflow"))]
    {
        proposal["child_admissions"] = serde_json::json!([]);
    }
    #[cfg(not(feature = "budget"))]
    {
        proposal["consumption"] = serde_json::json!([]);
    }
    #[cfg(feature = "budget")]
    for dimension in [
        actionqueue_core::budget::BudgetDimension::Token,
        actionqueue_core::budget::BudgetDimension::CostCents,
        actionqueue_core::budget::BudgetDimension::TimeSecs,
    ] {
        let c = MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
            seq(&a),
            w.task_id,
            dimension,
            100,
            11,
        ));
        let _ = execute_mutation(&mut a, &h, c).unwrap();
    }
    let good = result(&w, serde_json::from_value(proposal).unwrap());
    let before = a.projection().projection_digest().unwrap();
    for case in 0..6 {
        let mut d = good.clone();
        match case {
            0 => d.protocol_version += 1,
            1 => d.contract_revision = "unsupported".into(),
            2 => d.attempt_id = AttemptId::new(),
            3 => d.lease_fence = LeaseFence::new(d.lease_fence.owner().clone(), 0),
            4 => d.disposition_digest.0[0] ^= 1,
            _ => {
                d.lease_fence = LeaseFence::new("other".into(), d.lease_fence.granted_at_sequence())
            }
        }
        assert!(remote::submit_result(&mut a, &h, d, 12).is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
    assert!(remote::submit_result(&mut a, &host(ActorId::new()), good.clone(), 12).is_err());
    assert!(remote::submit_result(&mut a, &h, good.clone(), 41).is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    assert!(remote::renew(
        &mut a,
        &h,
        r,
        w.attempt_id,
        LeaseFence::new(w.lease_fence.owner().clone(), 0),
        12,
        50
    )
    .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    remote::renew(&mut a, &h, r, w.attempt_id, w.lease_fence.clone(), 12, 50).unwrap();
    assert_eq!(a.projection().get_lease_metadata(&r).unwrap().expiry(), 50);
    // The same complete proposal is valid under the accepted identity. This
    // proves rejection guards protect effects that would otherwise commit.
    remote::submit_result(&mut a, &h, good, 13).unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Awaiting));
    assert_eq!(a.projection().signals().statistics().retained, 1);
    assert!(a.projection().waits().active(r).unwrap().checkpoint.is_some());
    parity(&a);
}
#[test]
fn remote_checkpoint_and_resume_assignment_survive_lost_responses() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let w = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
    let cp = checkpoint(&a, r, b"remote checkpoint");
    let wait = spec(WaitId::new(), None);
    remote::submit_result(
        &mut a,
        &h,
        result(&w, AttemptDisposition::awaiting(wait, Some(cp.clone()))),
        12,
    )
    .unwrap();
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    let _ = s::submit(&mut a, s::envelope(1, 13)).unwrap();
    reconcile(&mut a, 14).unwrap();
    let q = request(r);
    let next = remote::claim(&mut a, &h, q.clone(), 14, 30).unwrap();
    assert_eq!(next.resume_context.as_ref().unwrap().checkpoint, Some(cp));
    let before = seq(&a);
    assert_eq!(next.resume_context, remote::claim(&mut a, &h, q, 15, 30).unwrap().resume_context);
    assert_eq!(seq(&a), before);
    parity(&a);
}

#[test]
fn actual_remote_claim_enforces_traits_pause_order_and_deregistration() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let actor = h.actor_id.unwrap();
    let reg = ActorRegistration::new(
        actor,
        "same-actor",
        ExecutorTraits::new(vec!["unrelated".into()]).unwrap(),
        30,
    );
    commit!(&mut a, MutationCommand::ActorRegister(ActorRegisterCommand::new(seq(&a), reg, 10)));
    // A second task explicitly requires a different trait.
    let original = admission_support::request(9);
    let t = original.task_spec();
    let required = TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        actionqueue_core::task::run_policy::RunPolicy::Once,
        TaskConstraints::default().with_required_executor_traits(vec!["compute".into()]).unwrap(),
        t.metadata().clone(),
    )
    .unwrap();
    let _ =
        admission_support::ensure(&mut a, admission_support::with_spec(&original, required), 10)
            .unwrap();
    let target = a.projection().runs_for_task(t.id()).next().unwrap().id();
    let before = seq(&a);
    assert!(!remote::claimable(&a, &h, 11).unwrap().contains(&target));
    assert!(remote::claim(&mut a, &h, request(target), 11, 30).is_err());
    assert_eq!(seq(&a), before);
    commit!(&mut a, MutationCommand::EnginePause(EnginePauseCommand::new(seq(&a), 11)));
    let before = seq(&a);
    assert!(remote::claim(&mut a, &h, request(r), 11, 30).is_err());
    assert_eq!(seq(&a), before);
    commit!(&mut a, MutationCommand::EngineResume(EngineResumeCommand::new(seq(&a), 12)));
    let w = remote::claim(&mut a, &h, request(r), 12, 30).unwrap();
    commit!(
        &mut a,
        MutationCommand::ActorDeregister(ActorDeregisterCommand::new(seq(&a), actor, 13))
    );
    let before = seq(&a);
    assert!(remote::submit_result(&mut a, &h, result(&w, AttemptDisposition::complete(None)), 14)
        .is_err());
    assert_eq!(seq(&a), before);
}

#[test]
fn remote_fifo_uses_creation_order_even_when_scheduling_times_differ() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, _) = setup(dir.path()); // three runs created at 10, due at 10/17/24
    let q = admission_support::request(2);
    let t = q.task_spec();
    let once = TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        actionqueue_core::task::run_policy::RunPolicy::Once,
        t.constraints().clone(),
        t.metadata().clone(),
    )
    .unwrap();
    let _ = admission_support::ensure(&mut a, admission_support::with_spec(&q, once), 15).unwrap();
    let newest = a.projection().runs_for_task(t.id()).next().unwrap().id();
    let eligible = remote::claimable(&a, &h, 30).unwrap();
    assert_eq!(eligible.len(), 4);
    assert_eq!(eligible.last(), Some(&newest));
    let before = seq(&a);
    assert!(remote::claim(&mut a, &h, request(newest), 30, 30).is_err());
    assert_eq!(seq(&a), before);
}

struct FailAt<W> {
    inner: W,
    remaining: usize,
}
impl<W: actionqueue_storage::wal::writer::WalWriter> actionqueue_storage::wal::writer::WalWriter
    for FailAt<W>
{
    fn store_session(&self) -> Option<&actionqueue_storage::store::StoreSession> {
        self.inner.store_session()
    }
    fn fence(&mut self) {
        self.inner.fence();
    }
    fn recovery_required(&self) -> bool {
        self.inner.recovery_required()
    }
    fn append(
        &mut self,
        e: &actionqueue_storage::wal::event::WalEvent,
    ) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
        if self.remaining == 0 {
            return Err(actionqueue_storage::wal::writer::WalWriterError::IoError(
                "claim prefix crash".into(),
            ));
        }
        self.remaining -= 1;
        self.inner.append(e)
    }
    fn flush(&mut self) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
        self.inner.flush()
    }
    fn close(self) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
        self.inner.close()
    }
}
#[test]
fn every_partial_remote_claim_prefix_recovers_without_stranded_ownership() {
    for prefix in 0..=5 {
        let dir = tempfile::tempdir().unwrap();
        let (a, h, r) = setup(dir.path());
        let q = request(r);
        let (writer, p) = a.into_parts();
        let mut a = actionqueue_storage::mutation::StorageMutationAuthority::new(
            FailAt { inner: writer, remaining: prefix },
            p,
        );
        let result = remote::claim(&mut a, &h, q, 11, 30);
        assert_eq!(result.is_ok(), prefix == 5);
        drop(a);
        let mut a = s::reopen(dir.path());
        actionqueue_runtime::waits::recover_execution(&mut a, 12).unwrap();
        remote::maintain(
            &mut a,
            13,
            remote::RemotePolicy { retry_delay_secs: 0, ..Default::default() },
        )
        .unwrap();
        assert!(a.projection().get_lease(&r).is_none());
        assert!(a.projection().get_run_instance(&r).unwrap().current_attempt_id().is_none());
        if prefix == 5 {
            assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Failed));
            assert_eq!(a.projection().get_run_instance(&r).unwrap().failure_attempt_count(), 1);
        } else {
            let work = remote::claim(&mut a, &h, request(r), 14, 30).unwrap();
            assert_eq!(work.failure_attempt_count, 0);
        }
        parity(&a);
    }
}
#[test]
fn local_and_remote_claims_serialize_one_accepted_attempt() {
    for _ in 0..8 {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, h, r) = setup(dir.path());
        transition(&mut a, r, RunState::Ready, 11);
        let a = std::sync::Arc::new(Mutex::new(a));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let remote_a = a.clone();
        let remote_barrier = barrier.clone();
        let remote = std::thread::spawn(move || {
            remote_barrier.wait();
            remote::claim(&mut remote_a.lock().unwrap(), &h, request(r), 12, 30).is_ok()
        });
        barrier.wait();
        let local = {
            let mut a = a.lock().unwrap();
            if actionqueue_runtime::claim::eligible(a.projection(), r, None, 12) {
                actionqueue_runtime::claim::accept(&mut a, r, AttemptId::new(), "local", 12, 42)
                    .is_ok()
            } else {
                false
            }
        };
        assert_ne!(local, remote.join().unwrap());
        let a = a.lock().unwrap();
        assert_eq!(a.projection().get_attempt_history(&r).unwrap().len(), 1);
        parity(&a);
    }
}
#[test]
fn renewal_result_and_cancellation_races_preserve_the_accepted_fence() {
    for renew_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, h, r) = setup(dir.path());
        let w = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
        if renew_first {
            remote::renew(&mut a, &h, r, w.attempt_id, w.lease_fence.clone(), 12, 60).unwrap();
        }
        remote::submit_result(&mut a, &h, result(&w, AttemptDisposition::complete(None)), 13)
            .unwrap();
        let before = a.projection().projection_digest().unwrap();
        assert!(remote::renew(&mut a, &h, r, w.attempt_id, w.lease_fence.clone(), 14, 70).is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let w = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
    execute_control(
        &mut a,
        &h,
        ControlOperation::Cancel(CancelTarget::Run(r)),
        &MockClock::new(12),
    )
    .unwrap();
    let before = a.projection().projection_digest().unwrap();
    assert!(remote::renew(&mut a, &h, r, w.attempt_id, w.lease_fence.clone(), 13, 60).is_err());
    assert!(remote::submit_result(&mut a, &h, result(&w, AttemptDisposition::complete(None)), 13)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    parity(&a);
}

#[cfg(feature = "budget")]
#[test]
fn remote_maintenance_repairs_result_subscription_gap_without_waking_waits() {
    use actionqueue_core::{
        budget::BudgetDimension,
        subscription::*,
        task::{run_policy::RunPolicy, task_spec::*},
    };
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let work = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
    let task = TaskId::new();
    let task_spec = TaskSpec::new(
        task,
        TaskPayload::new(vec![]),
        RunPolicy::repeat(2, 10000).unwrap(),
        Default::default(),
        Default::default(),
    )
    .unwrap();
    execute_control(
        &mut a,
        &h,
        ControlOperation::AdmitTask(
            actionqueue_core::admission::EnsureTaskRequest::for_task(task_spec, vec![]).unwrap(),
        ),
        &MockClock::new(11),
    )
    .unwrap();
    let future = a.projection().runs_for_task(task).find(|r| r.scheduled_at() > 1000).unwrap().id();
    let waiting = running(&mut a, 9, None, false);
    let c = command(&a, waiting, spec_wait());
    establish(&mut a, c).unwrap();
    let waiting_task = a.projection().get_run_instance(&waiting).unwrap().task_id();
    let mut ids = Vec::new();
    for (target, filter) in [
        (task, EventFilter::TaskCompleted { task_id: work.task_id }),
        (
            waiting_task,
            EventFilter::RunStateChanged { task_id: work.task_id, state: RunState::Completed },
        ),
    ] {
        let id = SubscriptionId::new();
        let c = MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
            seq(&a),
            id,
            target,
            filter,
            11,
        ));
        let _ = execute_mutation(&mut a, &h, c).unwrap();
        ids.push(id);
    }
    remote::submit_result(&mut a, &h, result(&work, AttemptDisposition::complete(None)), 21)
        .unwrap();
    let siblings: Vec<_> = a
        .projection()
        .runs_for_task(work.task_id)
        .filter(|x| x.id() != r)
        .map(|x| x.id())
        .collect();
    for id in siblings {
        execute_control(
            &mut a,
            &h,
            ControlOperation::Cancel(CancelTarget::Run(id)),
            &MockClock::new(21),
        )
        .unwrap();
    }
    // Crash after the atomic result and before secondary reactivity writes.
    assert!(ids.iter().all(|id| a
        .projection()
        .get_subscription(id)
        .unwrap()
        .triggered_at
        .is_none()));
    drop(a);
    let mut a = s::reopen(dir.path());
    remote::maintain(&mut a, 22, remote::RemotePolicy::default()).unwrap();
    assert!(
        ids.iter().all(|id| a.projection().get_subscription(id).unwrap().triggered_at.is_some()),
        "subs={:?}, run={:?}, status={:?}",
        a.projection().subscriptions().collect::<Vec<_>>(),
        a.projection().get_run_instance(&r),
        a.projection().task_terminal_status(work.task_id)
    );
    assert!(remote::claimable(&a, &h, 22).unwrap().contains(&future));
    assert_eq!(a.projection().get_run_state(&waiting), Some(&RunState::Awaiting));
    let before = seq(&a);
    remote::maintain(&mut a, 22, remote::RemotePolicy::default()).unwrap();
    assert_eq!(seq(&a), before);
    parity(&a);
    fn spec_wait() -> WaitSpec {
        spec(WaitId::new(), None)
    }
}
