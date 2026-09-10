use actionqueue_core::bounded::OpaqueRef;
use actionqueue_core::causal::{CausationLink, ControlMutationContext};
use actionqueue_core::continuation::*;
use actionqueue_core::data_ref::InlineData;
use actionqueue_core::ids::*;

mod common;
use common::hash;
fn filter() -> SignalFilter {
    SignalFilter {
        tenant_id: None,
        namespace: SignalNamespace::new("external").unwrap(),
        kind: SignalKind::new("ready").unwrap(),
        correlation_id: None,
        source_ref: None,
    }
}
fn envelope() -> SignalEnvelope {
    SignalEnvelope {
        signal_id: SignalId::new("s1").unwrap(),
        tenant_id: None,
        namespace: SignalNamespace::new("external").unwrap(),
        kind: SignalKind::new("ready").unwrap(),
        correlation_id: Some(CorrelationId::new("c1").unwrap()),
        causation: None,
        source_ref: Some(OpaqueRef::new("source").unwrap()),
        payload: None,
        payload_hash: None,
        occurred_at: None,
        received_at: 10,
        control_context: None,
    }
}
#[test]
fn causation_requires_a_source_and_consistent_hierarchy() {
    let task = TaskId::new();
    let run = RunId::new();
    let attempt = AttemptId::new();
    for has_task in [false, true] {
        for has_run in [false, true] {
            for has_attempt in [false, true] {
                for has_external in [false, true] {
                    let result = CausationLink::new(
                        has_task.then_some(task),
                        has_run.then_some(run),
                        has_attempt.then_some(attempt),
                        has_external.then(|| OpaqueRef::new("external").unwrap()),
                    );
                    let has_source = has_task || has_external;
                    let hierarchy_valid = (!has_run || has_task) && (!has_attempt || has_run);
                    let valid = has_source && hierarchy_valid;
                    assert_eq!(result.is_ok(), valid);
                }
            }
        }
    }
}
#[test]
fn exhaustive_filter_table_uses_exact_tenant_and_optional_equality() {
    let tenant = TenantId::new();
    let other = TenantId::new();
    for filter_tenant in [None, Some(tenant), Some(other)] {
        for signal_tenant in [None, Some(tenant), Some(other)] {
            for namespace_matches in [false, true] {
                for kind_matches in [false, true] {
                    for correlation_mode in 0..3 {
                        for source_mode in 0..3 {
                            let mut f = filter();
                            let mut e = envelope();
                            f.tenant_id = filter_tenant;
                            e.tenant_id = signal_tenant;
                            if !namespace_matches {
                                e.namespace = SignalNamespace::new("other").unwrap();
                            }
                            if !kind_matches {
                                e.kind = SignalKind::new("other").unwrap();
                            }
                            f.correlation_id = match correlation_mode {
                                0 => None,
                                1 => e.correlation_id.clone(),
                                _ => Some(CorrelationId::new("other").unwrap()),
                            };
                            f.source_ref = match source_mode {
                                0 => None,
                                1 => e.source_ref.clone(),
                                _ => Some(OpaqueRef::new("other").unwrap()),
                            };
                            assert_eq!(
                                f.matches(&e),
                                filter_tenant == signal_tenant
                                    && namespace_matches
                                    && kind_matches
                                    && correlation_mode != 2
                                    && source_mode != 2
                            );
                        }
                    }
                }
            }
        }
    }
}
#[test]
fn wait_and_signal_grammars_reject_unsafe_breadth() {
    for s in ["", "*", "a/b", "A", ".a", "a b"] {
        assert!(SignalNamespace::new(s).is_err());
        assert!(SignalKind::new(s).is_err());
    }
    assert!(SignalNamespace::new("a".repeat(64)).is_ok());
    assert!(SignalNamespace::new("a".repeat(65)).is_err());
    assert!(SignalKind::new("a".repeat(64)).is_ok());
    assert!(SignalKind::new("a".repeat(65)).is_err());
    assert!(WaitSpec::new(
        WaitId::new(),
        filter(),
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::AnyRetained,
        None
    )
    .is_err());
    assert!(WaitSpec::new(
        WaitId::new(),
        filter(),
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        None
    )
    .is_ok());
    let mut f = filter();
    f.correlation_id = Some(CorrelationId::new("c").unwrap());
    assert!(WaitSpec::new(
        WaitId::new(),
        f,
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::AnyRetained,
        None
    )
    .is_ok());
}
#[test]
fn inline_data_enforces_ceiling() {
    assert!(InlineData::new(None, vec![0; 65536], hash()).is_ok());
    assert!(InlineData::new(None, vec![0; 65537], hash()).is_err());
}
#[test]
fn resume_identifies_wait_for_every_wake_kind() {
    let id = WaitId::new();
    let control = ControlMutationContext::new(OpaqueRef::new("host").unwrap());
    let wakes = [
        WakeReason::Signal {
            wait_id: id,
            signal_sequence: SignalSequence::new(1),
            envelope: Box::new(envelope()),
        },
        WakeReason::Deadline { wait_id: id, deadline_at: 1 },
        WakeReason::ControlResolution { wait_id: id, control_context: control },
    ];
    for wake in wakes {
        assert_eq!(wake.wait_id(), Some(id));
    }
    assert_eq!(WakeReason::AdministrativeResume { control_context: None }.wait_id(), None);
}
#[cfg(feature = "serde")]
#[test]
fn target_types_round_trip_and_validate_on_decode() {
    use actionqueue_core::bounded::BoundedCode;
    use actionqueue_core::causal::CausalContext;
    use actionqueue_core::data_ref::DataRef;
    use common::round;
    let link = CausationLink::new(Some(TaskId::new()), None, None, None).unwrap();
    round(link.clone());
    round(
        CausalContext::new(
            TraceId::new("trace").unwrap(),
            CorrelationId::new("correlation").unwrap(),
        )
        .with_causation(link)
        .with_origin_ref(OpaqueRef::new("origin").unwrap()),
    );
    round(
        ControlMutationContext::new(OpaqueRef::new("caller").unwrap())
            .with_reason_code(BoundedCode::new("resume").unwrap()),
    );
    round(envelope());
    round(filter());
    let wait = WaitSpec::new(
        WaitId::new(),
        filter(),
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        Some(WaitDeadline { at: 42, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
    )
    .unwrap();
    round(wait.clone());
    let checkpoint = CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        data: DataRef::Inline(InlineData::new(None, vec![1, 2], hash()).unwrap()),
        created_by_attempt: AttemptId::new(),
    };
    round(checkpoint.clone());
    round(ResumeContext {
        checkpoint: Some(checkpoint),
        wake: WakeReason::Deadline { wait_id: wait.wait_id(), deadline_at: 42 },
        resumed_at: 43,
    });
    let mut invalid = serde_json::to_value(wait).unwrap();
    invalid["eligible_from"] = serde_json::json!("AnyRetained");
    assert!(serde_json::from_value::<WaitSpec>(invalid).is_err());
    assert!(serde_json::from_str::<CausationLink>(r#"{"parent_task_id":null,"parent_run_id":null,"parent_attempt_id":null,"external_ref":null}"#).is_err());
    let mut invalid = serde_json::to_value(InlineData::new(None, vec![], hash()).unwrap()).unwrap();
    invalid["bytes"] = serde_json::json!(vec![0u8; 65537]);
    assert!(serde_json::from_value::<InlineData>(invalid).is_err());
}

#[test]
fn optional_signal_attribution_matches_only_present_exact_values() {
    for has_correlation in [false, true] {
        for has_source in [false, true] {
            let mut signal = envelope();
            if !has_correlation {
                signal.correlation_id = None;
            }
            if !has_source {
                signal.source_ref = None;
            }
            let mut expected = filter();
            assert!(expected.matches(&signal));
            expected.correlation_id = envelope().correlation_id;
            assert_eq!(expected.matches(&signal), has_correlation);
            expected.correlation_id = None;
            expected.source_ref = envelope().source_ref;
            assert_eq!(expected.matches(&signal), has_source);
            expected.correlation_id = envelope().correlation_id;
            assert_eq!(expected.matches(&signal), has_correlation && has_source);

            #[cfg(feature = "serde")]
            {
                let json = serde_json::to_value(&signal).unwrap();
                assert_eq!(serde_json::from_value::<SignalEnvelope>(json).unwrap(), signal);
                let bytes = postcard::to_allocvec(&signal).unwrap();
                assert_eq!(postcard::from_bytes::<SignalEnvelope>(&bytes).unwrap(), signal);
            }
        }
    }
    #[cfg(feature = "serde")]
    {
        let mut json = serde_json::to_value(envelope()).unwrap();
        json.as_object_mut().unwrap().remove("correlation_id");
        json.as_object_mut().unwrap().remove("source_ref");
        let decoded = serde_json::from_value::<SignalEnvelope>(json).unwrap();
        assert_eq!(decoded.correlation_id, None);
        assert_eq!(decoded.source_ref, None);
    }
}
