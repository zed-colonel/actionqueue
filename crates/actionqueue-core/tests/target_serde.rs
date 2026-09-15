#![cfg(feature = "serde")]
use actionqueue_core::actor::ActorRegistration;
use actionqueue_core::admission::{AdmissionDigest, EnsureTaskOutcome};
use actionqueue_core::bounded::*;
use actionqueue_core::causal::*;
use actionqueue_core::continuation::*;
use actionqueue_core::data_ref::*;
use actionqueue_core::executor::*;
use actionqueue_core::ids::*;

mod common;
use common::round;
fn reference() -> OpaqueRef {
    OpaqueRef::new("opaque-reference").unwrap()
}
fn hash() -> ContentHash {
    common::hash_filled(17)
}
#[test]
fn every_new_identifier_and_bounded_value_round_trips() {
    round(WaitId::new());
    round(CheckpointId::new());
    round(AdmissionKey::new("admission").unwrap());
    round(SignalId::new("signal").unwrap());
    round(TraceId::new("trace").unwrap());
    round(CorrelationId::new("corr").unwrap());
    round(SignalSequence::new(u64::MAX));
    round(reference());
    round(BoundedCode::new("error.code").unwrap());
    round(BoundedMessage::new("human detail\n").unwrap());
    round(ContentType::new("text/plain; charset=utf-8").unwrap());
    round(hash());
    round(HashAlgorithm::Sha256);
    round(BoundedError {
        code: BoundedCode::new("failed").unwrap(),
        message: BoundedMessage::new("detail").unwrap(),
    });
    round(ExecutorTrait::new("compute").unwrap());
    round(ExecutorTraits::new(vec!["compute".into()]).unwrap());
    round(SignalNamespace::new("external").unwrap());
    round(SignalKind::new("ready").unwrap());
    round(AdmissionDigest::new(hash()));
}
#[test]
fn complete_causal_and_control_contexts_round_trip() {
    let link = CausationLink::new(
        Some(TaskId::new()),
        Some(RunId::new()),
        Some(AttemptId::new()),
        Some(reference()),
    )
    .unwrap();
    let causal =
        CausalContext::new(TraceId::new("trace").unwrap(), CorrelationId::new("corr").unwrap())
            .with_causation(link)
            .with_submitting_principal_ref(reference())
            .with_requesting_actor_ref(reference())
            .with_purpose_ref(reference())
            .with_authorization_ref(reference())
            .with_identity_context_ref(reference())
            .with_signed_statement_ref(reference())
            .with_proof_context_ref(reference())
            .with_origin_ref(reference());
    round(causal);
    round(CausalOverride {
        correlation_id: Some(CorrelationId::new("fork").unwrap()),
        requesting_actor_ref: Some(reference()),
        origin_ref: Some(reference()),
    });
    round(
        ControlMutationContext::new(reference())
            .with_host_session_ref(reference())
            .with_request_id(reference())
            .with_reason_code(BoundedCode::new("resume").unwrap()),
    );
}
#[test]
fn external_data_signal_and_all_resolution_variants_round_trip() {
    let data = ExternalDataRef {
        scheme: DataScheme::new("git+https").unwrap(),
        locator: reference(),
        hash: hash(),
        size_bytes: Some(123),
        content_type: Some(ContentType::new("text/plain; charset=utf-8").unwrap()),
    };
    round(data.clone());
    round(DataRef::External(data.clone()));
    let proposal = SignalProposal {
        signal_id: SignalId::new("signal").unwrap(),
        namespace: SignalNamespace::new("external").unwrap(),
        kind: SignalKind::new("ready").unwrap(),
        correlation_id: CorrelationId::new("corr").unwrap(),
        payload: Some(DataRef::External(data)),
        payload_hash: Some(hash()),
        occurred_at: Some(1),
    };
    round(proposal.clone());
    let envelope = SignalEnvelope {
        signal_id: proposal.signal_id.clone(),
        tenant_id: Some(TenantId::new()),
        namespace: proposal.namespace,
        kind: proposal.kind,
        correlation_id: Some(proposal.correlation_id),
        causation: Some(CausationLink::new(None, None, None, Some(reference())).unwrap()),
        source_ref: Some(reference()),
        payload: proposal.payload,
        payload_hash: proposal.payload_hash,
        occurred_at: proposal.occurred_at,
        received_at: 2,
        control_context: Some(ControlMutationContext::new(reference())),
    };
    round(envelope.clone());
    round(WakeReason::Signal {
        wait_id: WaitId::new(),
        signal_sequence: SignalSequence::new(3),
        envelope: Box::new(envelope),
    });
    round(WakeReason::Deadline { wait_id: WaitId::new(), deadline_at: 4 });
    round(WakeReason::ControlResolution {
        wait_id: WaitId::new(),
        control_context: ControlMutationContext::new(reference()),
    });
    round(WakeReason::AdministrativeResume {
        control_context: Some(ControlMutationContext::new(reference())),
    });
    for policy in [
        WaitTimeoutPolicy::ResumeWithTimeout,
        WaitTimeoutPolicy::FailRun { code: BoundedCode::new("expired").unwrap() },
        WaitTimeoutPolicy::CancelRun,
    ] {
        round(policy);
    }
    round(WaitMatchPolicy::FirstMatch);
    round(SignalEligibility::AnyRetained);
    round(SignalEligibility::After(SignalSequence::new(0)));
}
#[test]
fn ensure_outcomes_and_actor_registration_round_trip() {
    let id = TaskId::new();
    let key = AdmissionKey::new("key").unwrap();
    let digest = AdmissionDigest::new(hash());
    round(EnsureTaskOutcome::Created {
        sequence: 1,
        task_id: id,
        admission_key: key.clone(),
        digest: digest.clone(),
    });
    round(EnsureTaskOutcome::AlreadyExists {
        sequence: 1,
        task_id: id,
        admission_key: key,
        digest,
    });
    let registration = ActorRegistration::new(
        ActorId::new(),
        "actor",
        ExecutorTraits::new(vec!["compute".into()]).unwrap(),
        30,
    );
    round(registration.clone());
    let mut invalid = serde_json::to_value(registration).unwrap();
    invalid["executor_traits"] = serde_json::json!([]);
    assert!(serde_json::from_value::<ActorRegistration>(invalid).is_err());
}
