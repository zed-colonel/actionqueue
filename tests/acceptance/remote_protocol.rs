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
    // AQ-11: claimed work carries the admission's namespace and opaque causal context.
    assert_eq!(w.tenant_id, None);
    assert_eq!(&w.causal_context, admission_support::request(1).causal_context());
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

#[cfg(feature = "workflow")]
#[test]
fn remote_cron_replenishes_beyond_initial_window_across_restart_and_stops_on_cancel() {
    use actionqueue_core::task::run_policy::{CronPolicy, RunPolicy};
    for bounded in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (mut a, h, _) = setup(dir.path());
        execute_control(
            &mut a,
            &h,
            ControlOperation::Cancel(CancelTarget::Task(admission_support::id(1))),
            &MockClock::new(10),
        )
        .unwrap();
        let policy = CronPolicy::new("* * * * * * *").unwrap();
        let policy = if bounded { policy.with_max_occurrences(8).unwrap() } else { policy };
        let task = TaskId::new();
        let spec = TaskSpec::new(
            task,
            TaskPayload::new(vec![]),
            RunPolicy::Cron(policy),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        execute_control(
            &mut a,
            &h,
            ControlOperation::AdmitTask(
                actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
            ),
            &MockClock::new(10),
        )
        .unwrap();
        assert_eq!(a.projection().runs_for_task(task).count(), 5);
        for n in 0..8 {
            let now = 11 + n;
            remote::maintain(&mut a, now, Default::default()).unwrap();
            let run = remote::claimable(&a, &h, now).unwrap()[0];
            let w = remote::claim(&mut a, &h, request(run), now, 30).unwrap();
            remote::submit_result(&mut a, &h, result(&w, AttemptDisposition::complete(None)), now)
                .unwrap();
            if n == 4 {
                // Restart before maintenance, with a durable result to replenish.
                parity(&a);
                drop(a);
                a = s::reopen(dir.path());
            }
            remote::maintain(&mut a, now, Default::default()).unwrap();
        }
        assert_eq!(
            a.projection().runs_for_task(task).filter(|r| r.state() == RunState::Completed).count(),
            8
        );
        assert_eq!(
            a.projection().runs_for_task(task).filter(|r| !r.state().is_terminal()).count(),
            if bounded { 0 } else { 5 }
        );
        if !bounded {
            execute_control(
                &mut a,
                &h,
                ControlOperation::Cancel(CancelTarget::Task(task)),
                &MockClock::new(20),
            )
            .unwrap();
            parity(&a);
            drop(a);
            a = s::reopen(dir.path());
            let before = seq(&a);
            remote::maintain(&mut a, 21, Default::default()).unwrap();
            assert_eq!(seq(&a), before);
            assert!(a.projection().runs_for_task(task).all(|r| r.state().is_terminal()));
        }
        parity(&a);
    }
}

#[cfg(feature = "budget")]
#[tokio::test]
async fn equal_timestamp_subscriptions_obey_wal_order_after_replay_and_snapshot() {
    use actionqueue_core::{subscription::*, task::run_policy::RunPolicy};
    for before in [false, true] {
        for snapshot in [false, true] {
            for daemon in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let (mut a, h, r) = setup(dir.path());
                let work = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
                let target = TaskId::new();
                let spec = TaskSpec::new(
                    target,
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
                        actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![])
                            .unwrap(),
                    ),
                    &MockClock::new(11),
                )
                .unwrap();
                let future = a
                    .projection()
                    .runs_for_task(target)
                    .find(|r| r.scheduled_at() > 1000)
                    .unwrap()
                    .id();
                // Retire siblings first so this completion also makes the source terminal.
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
                        &MockClock::new(20),
                    )
                    .unwrap();
                }
                let ids = [SubscriptionId::new(), SubscriptionId::new()];
                let subscribe = |a: &mut s::Authority| {
                    for (id, filter) in ids.into_iter().zip([
                        EventFilter::TaskCompleted { task_id: work.task_id },
                        EventFilter::RunStateChanged {
                            task_id: work.task_id,
                            state: RunState::Completed,
                        },
                    ]) {
                        let c = MutationCommand::SubscriptionCreate(
                            SubscriptionCreateCommand::new(seq(a), id, target, filter, 21),
                        );
                        let _ = execute_mutation(a, &h, c).unwrap();
                    }
                };
                if before {
                    subscribe(&mut a);
                }
                remote::submit_result(
                    &mut a,
                    &h,
                    result(&work, AttemptDisposition::complete(None)),
                    21,
                )
                .unwrap();
                if !before {
                    subscribe(&mut a);
                }
                if snapshot {
                    parity(&a);
                }
                drop(a);
                a = s::reopen(dir.path());
                if daemon {
                    remote::maintain(&mut a, 22, Default::default()).unwrap();
                } else {
                    let c = MutationCommand::EnginePause(EnginePauseCommand::new(seq(&a), 22));
                    let _ =
                        execute_mutation(&mut a, &s::host_support::host(ControlScope::Store), c)
                            .unwrap();
                    let mut dispatch = actionqueue_runtime::dispatch::DispatchLoop::new(
                        a,
                        Recording(Default::default()),
                        MockClock::new(22),
                        actionqueue_runtime::dispatch::DispatchConfig::new(
                            actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
                                interval: std::time::Duration::from_secs(1),
                            },
                            1,
                            30,
                            None,
                            None,
                        ),
                    )
                    .unwrap();
                    let _ = dispatch.tick().await.unwrap();
                    a = dispatch.into_authority();
                    // Re-enable selection without changing subscription evidence.
                    let c = MutationCommand::EngineResume(EngineResumeCommand::new(seq(&a), 22));
                    let _ =
                        execute_mutation(&mut a, &s::host_support::host(ControlScope::Store), c)
                            .unwrap();
                }
                for id in ids {
                    assert_eq!(
                        a.projection().get_subscription(&id).unwrap().triggered_at.is_some(),
                        before
                    );
                }
                assert_eq!(remote::claimable(&a, &h, 22).unwrap().contains(&future), before);
                parity(&a);
            }
        }
    }
}

#[cfg(feature = "platform")]
#[test]
fn platform_remote_operations_reject_missing_cross_tenant_and_revoked_principals() {
    use actionqueue_core::platform::*;
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open_platform(dir.path());
    let admin = s::host_support::host(ControlScope::Store);
    let mut hosts = Vec::new();
    for name in ["owner", "other"] {
        let tenant = TenantId::new();
        let c = MutationCommand::TenantCreate(TenantCreateCommand::new(
            seq(&a),
            TenantRegistration::new(tenant, name),
            1,
        ));
        let _ = execute_mutation(&mut a, &admin, c).unwrap();
        let h = s::host_support::tenant(&mut a, tenant).unwrap();
        hosts.push(h);
    }
    let h = hosts.remove(0);
    let other = hosts.remove(0);
    let permission =
        |a: &mut s::Authority, h: &HostControlContext, action: QueueAction, grant: bool| {
            let ControlScope::Tenant(tenant) = h.scope else { panic!() };
            let c = if grant {
                MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
                    seq(a),
                    h.actor_id.unwrap(),
                    action.permission(),
                    tenant,
                    10,
                ))
            } else {
                MutationCommand::CapabilityRevoke(CapabilityRevokeCommand::new(
                    seq(a),
                    h.actor_id.unwrap(),
                    action.permission(),
                    tenant,
                    10,
                ))
            };
            let _ = execute_mutation(a, &admin, c).unwrap();
        };
    for owner in [&h, &other] {
        for action in [
            QueueAction::InspectClaimable,
            QueueAction::ClaimRun,
            QueueAction::RenewLease,
            QueueAction::SubmitResult,
        ] {
            permission(&mut a, owner, action, true);
        }
    }
    let ControlScope::Tenant(tenant) = h.scope else { panic!() };
    let q = admission_support::request(401);
    let q = admission_support::with_spec(&q, q.task_spec().clone().with_tenant(tenant));
    execute_control(&mut a, &h, ControlOperation::AdmitTask(q.clone()), &MockClock::new(10))
        .unwrap();
    let run = a.projection().runs_for_task(q.task_spec().id()).next().unwrap().id();
    let missing = HostControlContext { actor_id: None, ..h.clone() };
    let before = a.projection().projection_digest().unwrap();
    assert!(remote::claimable(&a, &missing, 11).is_err());
    assert!(remote::claimable(&a, &other, 11).unwrap().is_empty());
    for rejected in [&missing, &other] {
        assert!(remote::claim(&mut a, rejected, request(run), 11, 30).is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
    permission(&mut a, &h, QueueAction::ClaimRun, false);
    let before = a.projection().projection_digest().unwrap();
    assert!(remote::claim(&mut a, &h, request(run), 11, 30).is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    permission(&mut a, &h, QueueAction::ClaimRun, true);
    let w = remote::claim(&mut a, &h, request(run), 11, 30).unwrap();
    assert_eq!(w.tenant_id, Some(tenant));
    assert_eq!(&w.causal_context, q.causal_context());
    for action in [QueueAction::RenewLease, QueueAction::SubmitResult] {
        let invoke = |a: &mut s::Authority, h: &HostControlContext| {
            if action == QueueAction::RenewLease {
                remote::renew(a, h, run, w.attempt_id, w.lease_fence.clone(), 12, 45)
            } else {
                remote::submit_result(a, h, result(&w, AttemptDisposition::complete(None)), 12)
            }
        };
        let before = a.projection().projection_digest().unwrap();
        let bytes = std::fs::read(a.store_session().unwrap().wal_path()).unwrap();
        for rejected in [&missing, &other] {
            assert!(invoke(&mut a, rejected).is_err());
            assert_eq!(before, a.projection().projection_digest().unwrap());
            assert_eq!(bytes, std::fs::read(a.store_session().unwrap().wal_path()).unwrap());
        }
        permission(&mut a, &h, action, false);
        let before = a.projection().projection_digest().unwrap();
        assert!(invoke(&mut a, &h).is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
        permission(&mut a, &h, action, true);
        invoke(&mut a, &h).unwrap();
    }
    parity(&a);
    drop(a);
    a = s::reopen(dir.path());
    let before = a.projection().projection_digest().unwrap();
    assert!(remote::submit_result(
        &mut a,
        &other,
        result(&w, AttemptDisposition::complete(None)),
        13
    )
    .is_err());
    remote::submit_result(&mut a, &h, result(&w, AttemptDisposition::complete(None)), 13).unwrap();
    assert_eq!(before, a.projection().projection_digest().unwrap());
}

// F-017: due Scheduled work and persisted Ready snapshots share priority/FIFO order.
#[test]
fn remote_priority_matches_local_promotion_before_and_after_recovery() {
    for snapshot in [false, true] {
        for ready in [None, Some(2), Some(3)] {
            let dir = tempfile::tempdir().unwrap();
            let (mut a, h, _) = setup(dir.path());
            let mut runs = Vec::new();
            for (n, priority, at) in [(2, 1, 10), (3, 100, 11)] {
                let q = admission_support::request(n);
                let t = q.task_spec();
                let task = TaskSpec::new(
                    t.id(),
                    t.task_payload().clone(),
                    actionqueue_core::task::run_policy::RunPolicy::Once,
                    t.constraints().clone(),
                    actionqueue_core::task::metadata::TaskMetadata::new(vec![], priority, None),
                )
                .unwrap();
                admission_support::ensure(&mut a, admission_support::with_spec(&q, task), at)
                    .unwrap();
                let run = a.projection().runs_for_task(t.id()).next().unwrap().id();
                if ready == Some(n) {
                    transition(&mut a, run, RunState::Ready, 12);
                }
                runs.push(run);
            }
            let expected = vec![runs[1], runs[0]];
            let ordered = |a: &s::Authority| {
                remote::claimable(a, &h, 12)
                    .unwrap()
                    .into_iter()
                    .filter(|r| runs.contains(r))
                    .collect::<Vec<_>>()
            };
            assert_eq!(ordered(&a), expected);
            if snapshot {
                parity(&a);
            }
            drop(a);
            let mut a = s::reopen(dir.path());
            actionqueue_runtime::remote::maintain(&mut a, 12, Default::default()).unwrap();
            assert_eq!(ordered(&a), expected);
            let before = seq(&a);
            assert!(remote::claim(&mut a, &h, request(runs[0]), 12, 30).is_err());
            assert_eq!(seq(&a), before);
            let work = remote::claim(&mut a, &h, request(runs[1]), 12, 30).unwrap();
            assert_eq!(work.run_id, runs[1]);
            assert_eq!(
                a.projection().get_run_instance(&runs[1]).unwrap().effective_priority(),
                100
            );
        }
    }
}

// F-022: remote rejection leaves ownership with the executor, which can retry
// after capacity is released; it must not use local terminal-failure fallback.
#[test]
fn remote_wait_capacity_rejection_preserves_attempt_for_retry() {
    use actionqueue_core::limits::ContinuationLimits;
    let dir = tempfile::tempdir().unwrap();
    let (mut a, h, r) = setup(dir.path());
    let w = remote::claim(&mut a, &h, request(r), 11, 30).unwrap();
    let d = result(&w, AttemptDisposition::awaiting(spec(WaitId::new(), None), None));
    a.set_continuation_limits(ContinuationLimits { active_waits: 0, ..Default::default() });
    let digest = a.projection().projection_digest().unwrap();
    let error = remote::submit_result(&mut a, &h, d.clone(), 12).unwrap_err();
    assert!(error.to_string().contains("WaitCapacity"), "{error}");
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Running));
    assert!(a.projection().get_lease_metadata(&r).is_some());
    assert!(a.projection().get_attempt_history(&r).unwrap()[0].finished_at().is_none());
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    a.set_continuation_limits(ContinuationLimits { active_waits: 1, ..Default::default() });
    remote::submit_result(&mut a, &h, d.clone(), 13).unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Awaiting));
    assert!(a.projection().get_lease_metadata(&r).is_none());
    assert_eq!(a.projection().waits().active_count(), 1);
    let sequence = seq(&a);
    remote::submit_result(&mut a, &h, d, 14).unwrap();
    assert_eq!(seq(&a), sequence);
    parity(&a);
}
