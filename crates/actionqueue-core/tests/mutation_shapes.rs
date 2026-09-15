use actionqueue_core::bounded::OpaqueRef;
use actionqueue_core::causal::ControlMutationContext;
use actionqueue_core::continuation::{SignalEnvelope, SignalKind, SignalNamespace};
use actionqueue_core::ids::*;
use actionqueue_core::mutation::{
    LeaseFence, LeaseOwner, SignalAdmitCommand, WaitCancelCommand, WaitSatisfyCommand,
    WaitTimeoutCommand,
};
#[test]
fn commands_preserve_expectations_without_executing() {
    let run = RunId::new();
    let wait = WaitId::new();
    let signal = SignalId::new("signal").unwrap();
    let satisfy = WaitSatisfyCommand::new(1, run, wait, SignalSequence::new(3), 10);
    assert_eq!(satisfy.wait_id(), wait);
    assert_eq!(satisfy.signal_sequence(), SignalSequence::new(3));
    let timeout = WaitTimeoutCommand::new(2, run, wait, 10);
    assert_eq!(timeout.run_id(), run);
    let context = ControlMutationContext::new(OpaqueRef::new("host").unwrap());
    let cancel = WaitCancelCommand::new(3, run, wait, context.clone(), 10);
    assert_eq!(cancel.control_context(), &context);
    let fence = LeaseFence::new(LeaseOwner::new("worker"), 9);
    assert_eq!(fence.granted_at_sequence(), 9);
    let envelope = SignalEnvelope {
        signal_id: signal,
        tenant_id: None,
        namespace: SignalNamespace::new("n").unwrap(),
        kind: SignalKind::new("k").unwrap(),
        correlation_id: Some(CorrelationId::new("c").unwrap()),
        causation: None,
        source_ref: Some(OpaqueRef::new("s").unwrap()),
        payload: None,
        payload_hash: None,
        occurred_at: None,
        received_at: 9,
        control_context: None,
    };
    let admit = SignalAdmitCommand::new(4, envelope.clone());
    assert_eq!(admit.envelope(), &envelope);
}
