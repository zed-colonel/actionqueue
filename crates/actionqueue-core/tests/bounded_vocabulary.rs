use actionqueue_core::bounded::{
    BoundedCode, BoundedMessage, ContentHash, ContentType, HashAlgorithm, OpaqueRef,
};
use actionqueue_core::executor::{ExecutorTrait, ExecutorTraits};
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
    for value in ["", "text/ plain", "text/\nplain", "text/\u{a0}plain"] {
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
