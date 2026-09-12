#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_runtime::inspection::{DisclosurePolicy, Inspector, Query};
fn inspect_host() -> actionqueue_core::control::HostControlContext {
    actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("inspector").unwrap()),
    }
}
fn encoded_trace(p: &ReplayReducer) -> serde_json::Value {
    let h = inspect_host();
    let i = Inspector::new(p, &h, false, Default::default(), false, 40).unwrap();
    serde_json::to_value(i.trace(&Query::default()).unwrap()).unwrap()
}
#[test]
fn aq_dd_007_011_013_018_resume_history_redaction_and_replay_inspection() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    let cp = context.checkpoint.as_ref().unwrap().checkpoint_id;
    lease(&mut a, r, 31);
    let attempt = start(&mut a, r, 32);
    let h = inspect_host();
    let i = Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    let view = i.get_attempt(r, attempt).unwrap();
    assert_eq!(view.resume.unwrap().checkpoint_id, Some(cp));
    assert_eq!(view.assignment.unwrap().context_id, context.context_id);
    let trace = encoded_trace(a.projection());
    let text = serde_json::to_string(&trace).unwrap();
    for secret in ["private continuation", "payload", "locator"] {
        if secret == "payload" {
            continue;
        }
        assert!(!text.contains(secret));
    }
    for field in ["winner", "score", "saturation", "acceptance", "binding_constraint"] {
        assert!(!text.contains(&format!("\"{field}\"")));
    }
    assert!(trace["edges"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["kind"] == "consumed_checkpoint"));
    parity(&a);
    let recovered =
        recover_read_only(a.store_session().unwrap(), RepairPolicy::Strict).unwrap().projection;
    assert_eq!(trace, encoded_trace(&recovered));
    // A host must independently permit disclosure; a flag alone fails closed.
    assert!(
        Inspector::new(a.projection(), &h, false, DisclosurePolicy::default(), true, 40).is_err()
    );
    let disclosed = Inspector::new(
        a.projection(),
        &h,
        false,
        DisclosurePolicy { allow_references: true },
        true,
        40,
    )
    .unwrap();
    assert!(serde_json::to_string(
        &disclosed.get_task(a.projection().get_run_instance(&r).unwrap().task_id()).unwrap()
    )
    .unwrap()
    .contains("disclosed"));
}
#[test]
fn physical_retry_edges_keep_original_wake_identity() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    lease(&mut a, r, 31);
    let first = start(&mut a, r, 32);
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq(&a),
            r,
            first,
            AttemptOutcome::failure("ERROR_CANARY"),
            33
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1031, 33))
    );
    transition(&mut a, r, RunState::RetryWait, 33);
    transition(&mut a, r, RunState::Ready, 34);
    lease(&mut a, r, 35);
    let second = start(&mut a, r, 36);
    let h = inspect_host();
    let i = Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    let v = i.get_attempt(r, second).unwrap();
    let assignment = v.assignment.unwrap();
    assert_eq!(assignment.previous_attempt_id, Some(first));
    assert_eq!(assignment.context_id, context.context_id);
    let trace = encoded_trace(a.projection());
    assert!(!trace.to_string().contains("ERROR_CANARY"));
    assert!(trace["edges"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["kind"] == "previous_attempt"));
}
