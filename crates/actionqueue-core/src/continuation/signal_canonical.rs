//! CanonicalSignalV1: explicit tagged fields, independent of serde and Rust layouts.
use sha2::{Digest, Sha256};

use super::{effective_payload_hash, SignalEnvelope, SignalRejection};
use crate::{bounded::ContentHash, data_ref::DataRef};
/// SHA-256 of domain-separated CanonicalSignalV1 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalDigest([u8; 32]);
impl SignalDigest {
    /// Parses a supported digest; unknown canonical versions fail closed.
    pub fn versioned(version: u32, bytes: [u8; 32]) -> Result<Self, SignalRejection> {
        if version != 1 {
            return Err(SignalRejection::InvalidEnvelope);
        }
        Ok(Self(bytes))
    }
    /// Canonical format version.
    pub const fn canonical_version(&self) -> u32 {
        1
    }
    /// SHA-256 bytes.
    pub fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}
/// Owned canonical representation with fixed field order and explicit tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSignalV1(Vec<u8>);
struct Encoder(Vec<u8>);
impl Encoder {
    fn byte(&mut self, n: u8) {
        self.0.push(n);
    }
    fn u64(&mut self, n: u64) {
        self.0.extend(n.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u64(b.len() as u64);
        self.0.extend(b);
    }
    fn text(&mut self, s: &str) {
        self.bytes(s.as_bytes());
    }
    fn uuid(&mut self, id: &uuid::Uuid) {
        self.0.extend(id.as_bytes());
    }
    fn option<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        self.byte(u8::from(v.is_some()));
        if let Some(v) = v {
            f(self, v);
        }
    }
    fn hash(&mut self, h: &ContentHash) {
        self.byte(1);
        self.bytes(h.bytes());
    }
}
impl CanonicalSignalV1 {
    /// Includes scope and producer content; excludes receipt, sequence and control context.
    pub fn new(s: &SignalEnvelope) -> Result<Self, SignalRejection> {
        let hash = effective_payload_hash(s.payload.as_ref(), s.payload_hash.as_ref())?;
        let mut e = Encoder(b"AQ-CONT-1\0signal\0".to_vec());
        e.0.extend(1u32.to_le_bytes());
        e.option(s.tenant_id, |e, id| e.uuid(id.as_uuid()));
        e.text(s.signal_id.as_str());
        e.text(s.namespace.as_str());
        e.text(s.kind.as_str());
        e.option(s.correlation_id.as_ref(), |e, id| e.text(id.as_str()));
        e.option(s.causation.as_ref(), |e, c| {
            e.option(c.parent_task_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(c.parent_run_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(c.parent_attempt_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(c.external_ref(), |e, v| e.text(v.expose()));
        });
        e.option(s.source_ref.as_ref(), |e, v| e.text(v.expose()));
        e.option(s.payload.as_ref(), |e, p| match p {
            DataRef::Inline(d) => {
                e.byte(0);
                e.option(d.content_type(), |e, v| e.text(v.as_str()));
                e.bytes(d.bytes());
                e.hash(d.hash());
            }
            DataRef::External(d) => {
                e.byte(1);
                e.text(d.scheme.as_str());
                e.text(d.locator.expose());
                e.hash(&d.hash);
                e.option(d.size_bytes, Encoder::u64);
                e.option(d.content_type.as_ref(), |e, v| e.text(v.as_str()));
            }
        });
        e.option(hash.as_ref(), Encoder::hash);
        e.option(s.occurred_at, Encoder::u64);
        Ok(Self(e.0))
    }
    /// Independent implementations can compare these bytes against published vectors.
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    /// Tagged canonical digest.
    pub fn digest(&self) -> SignalDigest {
        SignalDigest(Sha256::digest(&self.0).into())
    }
}
