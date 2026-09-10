use actionqueue_core::admission::*;
use actionqueue_core::bounded::*;
use actionqueue_core::causal::*;
use actionqueue_core::continuation::*;
use actionqueue_core::data_ref::*;
use actionqueue_core::disposition::*;
use actionqueue_core::ids::*;
use actionqueue_core::run::RunInstance;
use actionqueue_core::task::{
    constraints::TaskConstraints,
    metadata::TaskMetadata,
    run_policy::RunPolicy,
    task_spec::{TaskPayload, TaskSpec},
};
fn hash() -> ContentHash {
    ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap()
}
fn task() -> TaskSpec {
    TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(vec![]),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap()
}
fn request(
    spec: TaskSpec,
    dependencies: Vec<TaskId>,
) -> Result<EnsureTaskRequest, AdmissionRejection> {
    EnsureTaskRequest::new(
        AdmissionKey::new("key").unwrap(),
        spec,
        dependencies,
        CausalContext::new(TraceId::new("trace").unwrap(), CorrelationId::new("corr").unwrap()),
        None,
    )
}
fn child() -> ChildAdmission {
    ChildAdmission::new(
        AdmissionKey::new("child").unwrap(),
        task(),
        vec![],
        CausalOverride::default(),
    )
    .unwrap()
}
fn signal() -> SignalProposal {
    SignalProposal {
        signal_id: SignalId::new("signal").unwrap(),
        namespace: SignalNamespace::new("external").unwrap(),
        kind: SignalKind::new("ready").unwrap(),
        correlation_id: CorrelationId::new("corr").unwrap(),
        payload: None,
        payload_hash: None,
        occurred_at: None,
    }
}
fn wait() -> WaitSpec {
    WaitSpec::new(
        WaitId::new(),
        SignalFilter {
            tenant_id: None,
            namespace: SignalNamespace::new("external").unwrap(),
            kind: SignalKind::new("ready").unwrap(),
            correlation_id: None,
            source_ref: None,
        },
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        None,
    )
    .unwrap()
}
fn checkpoint() -> CheckpointRef {
    CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        data: DataRef::Inline(InlineData::new(None, vec![], hash()).unwrap()),
        created_by_attempt: AttemptId::new(),
    }
}
fn outcomes() -> Vec<AttemptOutcome> {
    let error = BoundedError {
        code: BoundedCode::new("failed").unwrap(),
        message: BoundedMessage::new("detail").unwrap(),
    };
    vec![
        AttemptOutcome::Complete,
        AttemptOutcome::Awaiting,
        AttemptOutcome::Suspended { reason: None },
        AttemptOutcome::RetryableFailure { error: error.clone() },
        AttemptOutcome::TerminalFailure { error: error.clone() },
        AttemptOutcome::Timeout { error },
    ]
}
#[test]
fn admission_checks_ownership_dependencies_and_counts() {
    let spec = task();
    let id = spec.id();
    let dep = TaskId::new();
    assert!(request(spec.clone(), vec![id]).is_err());
    assert!(request(spec.clone().with_parent(id), vec![]).is_err());
    assert!(request(spec.clone(), vec![dep; 65]).is_err());
    let req = request(spec.clone(), vec![dep, dep]).unwrap();
    assert_eq!(req.dependencies(), &[dep]);
    let wrong = RunInstance::new_scheduled(TaskId::new(), 0, 0).unwrap();
    assert!(AdmissionPlan::new(req.clone(), vec![wrong], AdmissionDigest::new(hash())).is_err());
    let run = RunInstance::new_scheduled(id, 0, 0).unwrap();
    assert!(AdmissionPlan::new(req.clone(), vec![run.clone(); 65], AdmissionDigest::new(hash()))
        .is_err());
    assert!(AdmissionPlan::new(req, vec![run], AdmissionDigest::new(hash())).is_ok());
    assert!(ChildAdmission::new(
        AdmissionKey::new("c").unwrap(),
        spec,
        vec![id],
        CausalOverride::default()
    )
    .is_err());
}
#[test]
fn all_disposition_combinations_are_checked() {
    for (index, outcome) in outcomes().into_iter().enumerate() {
        for has_output in [false, true] {
            for has_checkpoint in [false, true] {
                for has_wait in [false, true] {
                    for has_children in [false, true] {
                        let parts = DispositionParts {
                            output: has_output.then(|| checkpoint().data),
                            checkpoint: has_checkpoint.then(checkpoint),
                            wait: has_wait.then(wait),
                            child_admissions: if has_children { vec![child()] } else { vec![] },
                            ..Default::default()
                        };
                        let valid = match index {
                            0 => !has_checkpoint && !has_wait,
                            1 => has_wait,
                            2 => !has_wait,
                            _ => !has_children && !has_wait,
                        };
                        assert_eq!(
                            AttemptDisposition::new(outcome.clone(), parts).is_ok(),
                            valid,
                            "outcome {index}"
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn disposition_collection_ceilings_apply() {
    for count in [64, 65] {
        let p = DispositionParts { child_admissions: vec![child(); count], ..Default::default() };
        assert_eq!(AttemptDisposition::new(AttemptOutcome::Complete, p).is_ok(), count == 64);
    }
    for count in [32, 33] {
        let p = DispositionParts { emitted_signals: vec![signal(); count], ..Default::default() };
        assert_eq!(AttemptDisposition::new(AttemptOutcome::Complete, p).is_ok(), count == 32);
    }
}
#[cfg(feature = "serde")]
#[test]
fn admission_and_disposition_deserialization_preserves_invariants() {
    fn round<T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
        v: T,
    ) {
        assert_eq!(postcard::from_bytes::<T>(&postcard::to_allocvec(&v).unwrap()).unwrap(), v);
        assert_eq!(serde_json::from_str::<T>(&serde_json::to_string(&v).unwrap()).unwrap(), v);
    }
    let spec = task();
    let run = RunInstance::new_scheduled(spec.id(), 0, 0).unwrap();
    let req = request(spec, vec![]).unwrap();
    round(req.clone());
    round(child());
    let plan = AdmissionPlan::new(req, vec![run], AdmissionDigest::new(hash())).unwrap();
    round(plan.clone());
    let mut invalid = serde_json::to_value(plan).unwrap();
    invalid["runs"][0]["task_id"] = serde_json::to_value(TaskId::new()).unwrap();
    assert!(serde_json::from_value::<AdmissionPlan>(invalid).is_err());
    let disposition = AttemptDisposition::new(
        AttemptOutcome::Awaiting,
        DispositionParts {
            wait: Some(wait()),
            checkpoint: Some(checkpoint()),
            child_admissions: vec![child()],
            emitted_signals: vec![signal()],
            ..Default::default()
        },
    )
    .unwrap();
    round(disposition.clone());
    let mut invalid = serde_json::to_value(disposition).unwrap();
    invalid["wait"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<AttemptDisposition>(invalid).is_err());
}

#[test]
fn compound_command_shapes_preserve_commit_expectations() {
    use actionqueue_core::mutation::{
        AdmissionCommitCommand, AttemptCommitExpectation, AttemptDispositionCommitCommand,
        LeaseFence, LeaseOwner,
    };
    use actionqueue_core::run::RunState;
    let spec = task();
    let run = RunInstance::new_scheduled(spec.id(), 0, 0).unwrap();
    let run_id = run.id();
    let plan =
        AdmissionPlan::new(request(spec, vec![]).unwrap(), vec![run], AdmissionDigest::new(hash()))
            .unwrap();
    let context = ControlMutationContext::new(OpaqueRef::new("host").unwrap());
    let command = AdmissionCommitCommand::new(1, plan.clone(), Some(context), 2);
    assert_eq!(command.plan(), &plan);
    let attempt = AttemptId::new();
    let fence = LeaseFence::new(LeaseOwner::new("worker"), 3);
    let expected =
        AttemptCommitExpectation::new(4, run_id, attempt, RunState::Running, fence.clone());
    let disposition =
        AttemptDisposition::new(AttemptOutcome::Complete, DispositionParts::default()).unwrap();
    let command = AttemptDispositionCommitCommand::new(expected, disposition.clone(), 5);
    assert_eq!(command.expected_sequence(), 4);
    assert_eq!(command.run_id(), run_id);
    assert_eq!(command.attempt_id(), attempt);
    assert_eq!(command.expected_state(), RunState::Running);
    assert_eq!(command.expected_lease(), &fence);
    assert_eq!(command.disposition(), &disposition);
    assert_eq!(command.timestamp(), 5);
}
