use actionqueue_core::bounded::{
    BoundedCode, BoundedMessage, BoundedValueError, ContentHash, ContentType, DataScheme,
    HashAlgorithm, OpaqueRef,
};
use actionqueue_core::executor::{ExecutorTrait, ExecutorTraitError, ExecutorTraits};
use actionqueue_core::ids::{
    AdmissionKey, CheckpointId, CorrelationId, SignalId, SignalSequence, TraceId, WaitId,
};

#[test]
fn bounded_text_rejects_invalid_values_at_byte_boundaries() {
    for value in ["", "a\n", "a\0"] {
        assert!(OpaqueRef::new(value).is_err());
    }
    assert!(OpaqueRef::new("é".repeat(256)).is_ok());
    assert!(OpaqueRef::new("é".repeat(257)).is_err());
    let secret = OpaqueRef::new("private-reference").unwrap();
    assert_eq!(secret.expose(), "private-reference");
    assert!(!secret.to_string().contains("private-reference"));
    for value in ["", "A", "-a", "a b", "a/b", "é"] {
        assert!(BoundedCode::new(value).is_err());
    }
    assert!(BoundedCode::new("a".repeat(64)).is_ok());
    assert!(BoundedCode::new("a".repeat(65)).is_err());
    assert!(BoundedMessage::new("a".repeat(2048)).is_ok());
    assert!(BoundedMessage::new("a".repeat(2049)).is_err());
    for value in ["", "text/\nplain", "text/\0plain"] {
        assert!(ContentType::new(value).is_err());
    }
    assert!(ContentType::new("x".repeat(128)).is_ok());
    assert!(ContentType::new("x".repeat(129)).is_err());
}

#[test]
fn hash_is_structural_and_length_checked() {
    for length in [0, 31, 33] {
        assert!(ContentHash::new(HashAlgorithm::Sha256, vec![0; length]).is_err());
    }
    let hash = ContentHash::new(HashAlgorithm::Sha256, vec![0xab; 32]).unwrap();
    assert_eq!(hash.to_string(), format!("sha-256:{}", "ab".repeat(32)));
}

#[test]
fn executor_traits_are_bounded_canonical_sets() {
    for value in ["", "a b", "a\n", "a\u{a0}b"] {
        assert!(ExecutorTrait::new(value).is_err());
    }
    assert!(ExecutorTrait::new("a".repeat(128)).is_ok());
    assert!(ExecutorTrait::new("a".repeat(129)).is_err());
    assert!(ExecutorTraits::new(vec![]).is_err());
    assert!(ExecutorTraits::new((0..65).map(|i| i.to_string()).collect()).is_err());
    let traits = ExecutorTraits::new(vec!["z".into(), "a".into(), "z".into()]).unwrap();
    assert_eq!(traits.as_slice().iter().map(ExecutorTrait::as_str).collect::<Vec<_>>(), ["a", "z"]);
    assert!(traits.satisfies(&ExecutorTraits::new(vec!["a".into()]).unwrap()));
    assert!(!traits.satisfies(&ExecutorTraits::new(vec!["b".into()]).unwrap()));
}

#[test]
fn executor_trait_errors_identify_collection_and_label_failures() {
    use actionqueue_core::task::constraints::{TaskConstraints, TaskConstraintsError};

    let cases = [
        (vec![], ExecutorTraitError::Collection { count: 0 }, "collection must not be empty"),
        (
            (0..65).map(|i| i.to_string()).collect(),
            ExecutorTraitError::Collection { count: 65 },
            "received 65",
        ),
        (
            vec!["same".into(); 65],
            ExecutorTraitError::Collection { count: 65 },
            "before deduplication",
        ),
        (
            vec!["valid".into(), String::new()],
            ExecutorTraitError::Label { index: 1, source: BoundedValueError::Empty },
            "label at index 1",
        ),
        (
            vec!["x".repeat(129)],
            ExecutorTraitError::Label { index: 0, source: BoundedValueError::TooLarge },
            "maximum 128 bytes",
        ),
        (
            vec!["valid".into(), "invalid label".into()],
            ExecutorTraitError::Label { index: 1, source: BoundedValueError::InvalidCharacter },
            "no whitespace or control characters",
        ),
    ];
    for (values, expected, diagnostic) in cases {
        let error = ExecutorTraits::new(values.clone()).unwrap_err();
        assert_eq!(error, expected);
        assert!(error.to_string().contains(diagnostic));
        let constraints_error =
            TaskConstraints::default().with_required_executor_traits(values.clone()).unwrap_err();
        assert_eq!(constraints_error, TaskConstraintsError::InvalidExecutorTrait(expected));
        assert!(constraints_error.to_string().contains(&error.to_string()));

        #[cfg(feature = "serde")]
        {
            let json = serde_json::to_string(&values).unwrap();
            let decode_error = serde_json::from_str::<ExecutorTraits>(&json).unwrap_err();
            assert!(decode_error.to_string().contains(&error.to_string()));
            let bytes = postcard::to_allocvec(&values).unwrap();
            assert!(postcard::from_bytes::<ExecutorTraits>(&bytes).is_err());
        }
    }
    // The input ceiling includes duplicates, but a bounded input is canonicalized.
    let traits = ExecutorTraits::new(vec!["same".into(); 64]).unwrap();
    assert_eq!(traits.as_slice().len(), 1);
}

#[test]
fn identifiers_round_trip_and_enforce_limits() {
    macro_rules! bounded_id {
        ($id:ty, $limit:expr) => {{
            assert!(<$id>::new("").is_err());
            assert!(<$id>::new("x\n").is_err());
            assert!(<$id>::new("x".repeat($limit)).is_ok());
            assert!(<$id>::new("x".repeat($limit + 1)).is_err());
            let id = <$id>::new("opaque value").unwrap();
            assert_eq!(id.to_string().parse::<$id>().unwrap(), id);
        }};
    }
    bounded_id!(AdmissionKey, 256);
    bounded_id!(SignalId, 256);
    bounded_id!(TraceId, 128);
    bounded_id!(CorrelationId, 256);
    let wait = WaitId::new();
    assert_eq!(wait.to_string().parse::<WaitId>().unwrap(), wait);
    let checkpoint = CheckpointId::new();
    assert_eq!(checkpoint.to_string().parse::<CheckpointId>().unwrap(), checkpoint);
    let sequence = SignalSequence::new(u64::MAX);
    assert_eq!(sequence.to_string().parse::<SignalSequence>().unwrap(), sequence);
    assert!(sequence > SignalSequence::new(0));
}

#[cfg(feature = "serde")]
#[test]
fn deserialization_cannot_bypass_bounded_validation() {
    assert!(serde_json::from_str::<OpaqueRef>(r#""""#).is_err());
    assert!(serde_json::from_str::<BoundedCode>(r#""UPPER""#).is_err());
    assert!(serde_json::from_str::<ExecutorTraits>("[]").is_err());
    assert!(serde_json::from_str::<ExecutorTraits>(r#"["a b"]"#).is_err());
    assert!(serde_json::from_str::<ContentHash>(r#"{"algorithm":"Sha256","bytes":[]}"#).is_err());
    let traits = ExecutorTraits::new(vec!["z".into(), "a".into()]).unwrap();
    assert_eq!(serde_json::to_string(&traits).unwrap(), r#"["a","z"]"#);
    let old = vec!["a".to_string(), "z".to_string()];
    assert_eq!(postcard::to_allocvec(&traits).unwrap(), postcard::to_allocvec(&old).unwrap());
}

#[test]
fn opaque_content_types_and_schemes_allow_parameters_and_uri_punctuation() {
    assert!(ContentType::new("text/plain; charset=utf-8").is_ok());
    assert!(ContentType::new("opaque type").is_ok());
    for scheme in ["git+https", "custom resolver", "Mixed.Case-1"] {
        assert!(DataScheme::new(scheme).is_ok());
    }
    for scheme in ["", "git\nhttps", "git\0https"] {
        assert!(DataScheme::new(scheme).is_err());
    }
    assert!(DataScheme::new("x".repeat(64)).is_ok());
    assert!(DataScheme::new("x".repeat(65)).is_err());
    // Messages deliberately permit empty text and controls such as newlines.
    assert!(BoundedMessage::new("").is_ok());
    assert!(BoundedMessage::new("first\nsecond").is_ok());
}

#[test]
fn continuation_key_release_respects_both_policies() {
    use actionqueue_core::task::constraints::{ConcurrencyKeyWaitPolicy, TaskConstraints};
    assert!(ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting.releases_while_awaiting());
    assert!(!ConcurrencyKeyWaitPolicy::HoldWhileAwaiting.releases_while_awaiting());
    assert_eq!(
        TaskConstraints::default().concurrency_key_wait_policy(),
        ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting
    );
}
