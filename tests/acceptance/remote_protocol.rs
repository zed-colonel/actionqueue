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
    let mut a = open_store(path, OpenOptions::Initialize { features: vec!["actor".into()] })
        .unwrap()
        .into_authority()
        .unwrap();
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
    let good = result(&w, AttemptDisposition::complete(None));
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
    assert!(remote::submit_result(&mut a, &h, good, 41).is_err());
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
