use actionqueue_core::{bounded::*, data_ref::*, limits::*};
use sha2::{Digest, Sha256};
fn inline(bytes: &[u8]) -> DataRef {
    DataRef::Inline(
        InlineData::new(
            None,
            bytes.to_vec(),
            ContentHash::new(HashAlgorithm::Sha256, Sha256::digest(bytes).to_vec()).unwrap(),
        )
        .unwrap(),
    )
}
#[test]
fn inline_hash_boundaries_and_redaction() {
    for size in [0, 16 * 1024, 32 * 1024, MAX_INLINE_DATA_BYTES] {
        let data = inline(&vec![71; size]);
        data.validate().unwrap();
        assert_eq!(data.verify_bytes(b"different"), Err(DataValidationError::HashMismatch));
        #[cfg(feature = "serde")]
        {
            let decoded: DataRef =
                serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
            assert_eq!(data, decoded);
            decoded.validate().unwrap();
        }
    }
    assert!(InlineData::new(
        None,
        vec![0; MAX_INLINE_DATA_BYTES + 1],
        ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap()
    )
    .is_err());
    let bad = DataRef::Inline(
        InlineData::new(
            None,
            b"SECRET-CHECKPOINT".to_vec(),
            ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap(),
        )
        .unwrap(),
    );
    assert_eq!(bad.validate(), Err(DataValidationError::HashMismatch));
    assert!(!format!("{bad:?}").contains("SECRET-CHECKPOINT"));
    assert!(!format!("{bad:?}").contains("83, 69, 67"));
}
#[test]
fn external_verification_is_pure_and_size_is_optional() {
    for size_bytes in [None, Some(7)] {
        let data = DataRef::External(ExternalDataRef {
            scheme: DataScheme::new("blob").unwrap(),
            locator: OpaqueRef::new("SECRET-LOCATOR").unwrap(),
            hash: ContentHash::new(HashAlgorithm::Sha256, Sha256::digest(b"payload").to_vec())
                .unwrap(),
            size_bytes,
            content_type: None,
        });
        data.validate().unwrap();
        data.verify_bytes(b"payload").unwrap();
        assert_eq!(data.verify_bytes(b"changed"), Err(DataValidationError::HashMismatch));
        assert!(data.verify_bytes(b"").is_err());
        assert!(!format!("{data:?}").contains("SECRET-LOCATOR"));
        #[cfg(feature = "serde")]
        assert_eq!(data, serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap());
    }
}
