//! Helpers shared by the core integration tests.
#![allow(dead_code)]

use actionqueue_core::bounded::{ContentHash, HashAlgorithm};

/// A structurally valid SHA-256 digest whose 32 bytes are all `byte`.
pub fn hash_filled(byte: u8) -> ContentHash {
    ContentHash::new(HashAlgorithm::Sha256, vec![byte; 32]).unwrap()
}

/// A structurally valid all-zero SHA-256 digest.
pub fn hash() -> ContentHash {
    hash_filled(0)
}

/// Asserts that `value` survives a JSON and a postcard round trip unchanged.
///
/// Every durable type keeps its codec assertions here so a future codec change
/// is checked in one place.
#[cfg(feature = "serde")]
pub fn round<T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug>(
    value: T,
) {
    let json = serde_json::to_string(&value).unwrap();
    assert_eq!(serde_json::from_str::<T>(&json).unwrap(), value);
    let binary = postcard::to_allocvec(&value).unwrap();
    assert_eq!(postcard::from_bytes::<T>(&binary).unwrap(), value);
}
