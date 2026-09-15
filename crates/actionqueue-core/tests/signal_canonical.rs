use actionqueue_core::{
    bounded::{ContentHash, HashAlgorithm},
    continuation::*,
    data_ref::{DataRef, InlineData},
};
#[cfg(feature = "serde")]
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[test]
#[cfg(feature = "serde")]
fn independent_signal_vectors_cover_options_scope_inline_external_and_hash_only() {
    let vectors: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/aq-cont-1/signal-v1-vector.json"))
            .unwrap();
    for v in vectors["cases"].as_array().unwrap() {
        let mut envelope: SignalEnvelope = serde_json::from_value(v["envelope"].clone()).unwrap();
        let canonical = CanonicalSignalV1::new(&envelope).unwrap();
        assert_eq!(hex(canonical.bytes()), v["canonical_hex"].as_str().unwrap());
        assert_eq!(hex(canonical.digest().bytes()), v["sha256"].as_str().unwrap());
        envelope.received_at = u64::MAX;
        envelope.control_context = Some(actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("another/session").unwrap(),
        ));
        assert_eq!(CanonicalSignalV1::new(&envelope).unwrap(), canonical);
        envelope.payload_hash =
            effective_payload_hash(envelope.payload.as_ref(), envelope.payload_hash.as_ref())
                .unwrap();
        assert_eq!(CanonicalSignalV1::new(&envelope).unwrap(), canonical);
    }
}
#[test]
fn inline_hard_limit_and_hash_verification() {
    use sha2::{Digest, Sha256};
    let bytes = vec![255; actionqueue_core::limits::MAX_INLINE_DATA_BYTES];
    let hash = ContentHash::new(HashAlgorithm::Sha256, Sha256::digest(&bytes).to_vec()).unwrap();
    let data = DataRef::Inline(InlineData::new(None, bytes.clone(), hash.clone()).unwrap());
    assert_eq!(effective_payload_hash(Some(&data), None).unwrap(), Some(hash.clone()));
    let mut too_large = bytes;
    too_large.push(255);
    assert!(InlineData::new(None, too_large, hash.clone()).is_err());
    let bad = DataRef::Inline(InlineData::new(None, vec![], hash).unwrap());
    assert_eq!(effective_payload_hash(Some(&bad), None).unwrap_err(), SignalRejection::InvalidHash);
}
#[test]
fn canonical_digest_covers_every_external_representation_field() {
    use actionqueue_core::bounded::*;
    use actionqueue_core::data_ref::ExternalDataRef;
    use actionqueue_core::ids::{SignalId, TenantId};

    // Construct through the domain API so canonicalization remains covered without serde.
    let hash = ContentHash::new(HashAlgorithm::Sha256, vec![1; 32]).unwrap();
    let original = SignalEnvelope {
        signal_id: SignalId::new("event/external").unwrap(),
        tenant_id: Some(TenantId::new()),
        namespace: SignalNamespace::new("remote").unwrap(),
        kind: SignalKind::new("complete").unwrap(),
        correlation_id: None,
        causation: None,
        source_ref: None,
        payload: Some(DataRef::External(ExternalDataRef {
            scheme: DataScheme::new("blob").unwrap(),
            locator: OpaqueRef::new("opaque/object").unwrap(),
            hash: hash.clone(),
            size_bytes: Some(u64::MAX),
            content_type: Some(ContentType::new("application/octet-stream").unwrap()),
        })),
        payload_hash: Some(hash),
        occurred_at: Some(100),
        received_at: 42,
        control_context: None,
    };
    let digest = CanonicalSignalV1::new(&original).unwrap().digest();
    let mut retry = original.clone();
    retry.received_at = u64::MAX;
    retry.control_context = Some(actionqueue_core::causal::ControlMutationContext::new(
        OpaqueRef::new("another/session").unwrap(),
    ));
    retry.payload_hash = None;
    assert_eq!(CanonicalSignalV1::new(&retry).unwrap().digest(), digest);
    for field in 0..7 {
        let mut e = original.clone();
        let Some(DataRef::External(d)) = &mut e.payload else { unreachable!() };
        match field {
            0 => d.scheme = DataScheme::new("other").unwrap(),
            1 => d.locator = OpaqueRef::new("another/locator").unwrap(),
            2 => {
                d.hash = ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap();
                e.payload_hash = None;
            }
            3 => d.size_bytes = None,
            4 => d.content_type = None,
            5 => e.tenant_id = None,
            _ => e.signal_id = actionqueue_core::ids::SignalId::new("another/id").unwrap(),
        }
        assert_ne!(CanonicalSignalV1::new(&e).unwrap().digest(), digest, "field {field}");
    }
}
