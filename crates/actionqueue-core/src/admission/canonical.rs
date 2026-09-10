//! ADR-002 CanonicalAdmissionV1. No serde output or Rust discriminants are hashed.
use super::{AdmissionDigest, AdmissionRejection, EnsureTaskRequest};
use crate::bounded::{ContentHash, HashAlgorithm};
use crate::task::{
    constraints::{ConcurrencyKeyHoldPolicy, ConcurrencyKeyWaitPolicy},
    run_policy::RunPolicy,
    safety::SafetyLevel,
};
use sha2::{Digest, Sha256};

/// Owned, bounded canonical bytes. Collection ordering has already been normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAdmissionV1(Vec<u8>);
struct Encoder(Vec<u8>);
impl Encoder {
    fn byte(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend(v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend(v.to_le_bytes());
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u64(v.len() as u64);
        self.0.extend(v);
    }
    fn text(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
    fn option<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        self.byte(u8::from(v.is_some()));
        if let Some(v) = v {
            f(self, v);
        }
    }
    fn uuid(&mut self, v: &uuid::Uuid) {
        self.0.extend(v.as_bytes());
    }
}
impl CanonicalAdmissionV1 {
    /// Validates bounds on borrowed input before copying or sorting any field.
    pub fn new(r: &EnsureTaskRequest) -> Result<Self, AdmissionRejection> {
        let s = r.task_spec();
        crate::limits::AdmissionLimits::default().validate_spec(s, r.dependencies().len())?;
        let mut e = Encoder(b"AQ-CONT-1\0admission\0".to_vec());
        e.u32(1);
        e.uuid(s.id().as_uuid());
        e.bytes(s.task_payload().bytes());
        e.option(s.task_payload().content_type(), Encoder::text);
        match s.run_policy() {
            RunPolicy::Once => e.byte(0),
            RunPolicy::Repeat(p) => {
                e.byte(1);
                e.u32(p.count());
                e.u64(p.interval_secs());
            }
            #[cfg(feature = "workflow")]
            RunPolicy::Cron(p) => {
                e.byte(2);
                e.text(p.expression());
                e.option(p.max_occurrences(), Encoder::u32);
            }
        }
        let c = s.constraints();
        e.u32(c.max_attempts());
        e.option(c.timeout_secs(), Encoder::u64);
        e.option(c.concurrency_key(), Encoder::text);
        e.byte(match c.concurrency_key_hold_policy() {
            ConcurrencyKeyHoldPolicy::HoldDuringRetry => 0,
            ConcurrencyKeyHoldPolicy::ReleaseOnRetry => 1,
        });
        e.byte(match c.concurrency_key_wait_policy() {
            ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting => 0,
            ConcurrencyKeyWaitPolicy::HoldWhileAwaiting => 1,
        });
        e.byte(match c.safety_level() {
            SafetyLevel::Pure => 0,
            SafetyLevel::Idempotent => 1,
            SafetyLevel::Transactional => 2,
        });
        e.option(c.required_executor_traits(), |e, traits| {
            e.u64(traits.as_slice().len() as u64);
            for value in traits.as_slice() {
                e.text(value.as_str());
            }
        });
        e.0.extend(s.metadata().priority().to_le_bytes());
        e.option(s.metadata().description(), Encoder::text);
        let mut tags: Vec<_> = s.metadata().tags().iter().map(String::as_str).collect();
        tags.sort_unstable();
        tags.dedup();
        e.u64(tags.len() as u64);
        for tag in tags {
            e.text(tag);
        }
        e.option(s.parent_task_id(), |e, id| e.uuid(id.as_uuid()));
        e.u64(r.dependencies().len() as u64);
        for id in r.dependencies() {
            e.uuid(id.as_uuid());
        }
        e.option(s.tenant_id(), |e, id| e.uuid(id.as_uuid()));
        let c = r.causal_context();
        e.text(c.trace_id().as_str());
        e.text(c.correlation_id().as_str());
        e.option(c.causation(), |e, link| {
            e.option(link.parent_task_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(link.parent_run_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(link.parent_attempt_id(), |e, id| e.uuid(id.as_uuid()));
            e.option(link.external_ref(), |e, v| e.text(v.expose()));
        });
        for value in [
            c.submitting_principal_ref(),
            c.requesting_actor_ref(),
            c.purpose_ref(),
            c.authorization_ref(),
            c.identity_context_ref(),
            c.signed_statement_ref(),
            c.proof_context_ref(),
            c.origin_ref(),
        ] {
            e.option(value, |e, v| e.text(v.expose()));
        }
        Ok(Self(e.0))
    }
    /// Canonical v1 bytes, suitable for independent implementations and vectors.
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    /// Computes SHA-256 with its explicit canonical version and algorithm tag.
    pub fn digest(&self) -> Result<AdmissionDigest, AdmissionRejection> {
        Ok(AdmissionDigest::new(
            ContentHash::new(HashAlgorithm::Sha256, Sha256::digest(&self.0).to_vec())
                .expect("SHA-256 output length"),
        ))
    }
}
