//! CanonicalDispositionV1. Integers are little endian, strings/bytes have u64
//! lengths, options have 0/1 tags. Collections preserve proposal order. Field
//! tags and enum tags below are protocol constants, never Rust discriminants.
use sha2::{Digest, Sha256};

use crate::{
    continuation::*,
    data_ref::DataRef,
    disposition::{AttemptDisposition, DispositionOutcome},
};

/// SHA-256 digest of the versioned canonical disposition bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DispositionDigest(pub [u8; 32]);

use crate::canonical::Encoder;
impl Encoder {
    fn data(&mut self, value: &DataRef) {
        match value {
            DataRef::Inline(d) => {
                self.byte(0);
                self.option(d.content_type(), |e, v| e.text(v.as_str()));
                self.bytes(d.bytes());
                self.hash(d.hash());
            }
            DataRef::External(d) => {
                self.byte(1);
                self.text(d.scheme.as_str());
                self.text(d.locator.expose());
                self.hash(&d.hash);
                self.option(d.size_bytes, Self::u64);
                self.option(d.content_type.as_ref(), |e, v| e.text(v.as_str()));
            }
        }
    }
    fn error(&mut self, value: &crate::bounded::BoundedError) {
        self.text(value.code.as_str());
        self.text(value.message.as_str());
    }
    fn wait(&mut self, value: &WaitSpec) {
        self.uuid(value.wait_id().as_uuid());
        match value.target() {
            WaitTarget::Signal {
                filter,
                match_policy: WaitMatchPolicy::FirstMatch,
                eligible_from,
            } => {
                self.byte(0);
                self.option(filter.tenant_id, |e, v| e.uuid(v.as_uuid()));
                self.text(filter.namespace.as_str());
                self.text(filter.kind.as_str());
                self.option(filter.correlation_id.as_ref(), |e, v| e.text(v.as_str()));
                self.option(filter.source_ref.as_ref(), |e, v| e.text(v.expose()));
                self.byte(0);
                match eligible_from {
                    SignalEligibility::AnyRetained => self.byte(0),
                    SignalEligibility::After(s) => {
                        self.byte(1);
                        self.u64(s.get());
                    }
                }
            }
            WaitTarget::Children { task_ids, policy } => {
                self.byte(1);
                self.u64(task_ids.len() as u64);
                for id in task_ids {
                    self.uuid(id.as_uuid());
                }
                self.byte(match policy {
                    ChildWaitPolicy::AllTerminal => 0,
                    ChildWaitPolicy::AllSucceededOrAnyFailed => 1,
                });
            }
        }
        self.option(value.deadline(), |e, d| {
            e.u64(d.at);
            match &d.policy {
                WaitTimeoutPolicy::ResumeWithTimeout => e.byte(0),
                WaitTimeoutPolicy::FailRun { code } => {
                    e.byte(1);
                    e.text(code.as_str());
                }
                WaitTimeoutPolicy::CancelRun => e.byte(2),
            }
        });
    }
}
/// Encodes every producer-proposed field, excluding transport/server commit data.
pub fn canonical_disposition(disposition: &AttemptDisposition) -> Vec<u8> {
    let mut e = Encoder(b"AQ-CONT-1\0disposition\0".to_vec());
    e.0.extend(1u32.to_le_bytes());
    e.byte(1);
    match disposition.outcome() {
        DispositionOutcome::Complete => e.byte(0),
        DispositionOutcome::RetryableFailure { error } => {
            e.byte(1);
            e.error(error);
        }
        DispositionOutcome::TerminalFailure { error } => {
            e.byte(2);
            e.error(error);
        }
        DispositionOutcome::Timeout { error } => {
            e.byte(3);
            e.error(error);
        }
        DispositionOutcome::Suspended { reason } => {
            e.byte(4);
            e.option(reason.as_ref(), |e, v| e.text(v.as_str()));
        }
        DispositionOutcome::Awaiting => e.byte(5),
    }
    e.byte(2);
    e.option(disposition.output(), Encoder::data);
    e.byte(3);
    e.option(disposition.checkpoint(), |e, c| {
        e.uuid(c.checkpoint_id.as_uuid());
        e.data(&c.data);
        e.uuid(c.created_by_attempt.as_uuid());
    });
    e.byte(4);
    e.option(disposition.wait(), Encoder::wait);
    e.byte(5);
    e.u64(disposition.child_admissions().len() as u64);
    for c in disposition.child_admissions() {
        e.text(c.admission_key().as_str());
        e.bytes(&crate::admission::canonical::task_bytes(c.task_spec(), c.dependencies()));
        e.byte(match c.task_spec().child_lifecycle_policy() {
            crate::task::task_spec::ChildLifecyclePolicy::Required => 0,
            crate::task::task_spec::ChildLifecyclePolicy::Detached => 1,
        });
        let overrides = c.causal_override();
        e.option(overrides.correlation_id.as_ref(), |e, v| e.text(v.as_str()));
        e.option(overrides.requesting_actor_ref.as_ref(), |e, v| e.text(v.expose()));
        e.option(overrides.origin_ref.as_ref(), |e, v| e.text(v.expose()));
    }
    e.byte(6);
    e.u64(disposition.emitted_signals().len() as u64);
    for s in disposition.emitted_signals() {
        e.text(s.signal_id.as_str());
        e.text(s.namespace.as_str());
        e.text(s.kind.as_str());
        e.text(s.correlation_id.as_str());
        e.option(s.payload.as_ref(), Encoder::data);
        e.option(s.payload_hash.as_ref(), Encoder::hash);
        e.option(s.occurred_at, Encoder::u64);
    }
    e.byte(7);
    e.u64(disposition.consumption().len() as u64);
    for c in disposition.consumption() {
        use crate::budget::BudgetDimension;
        e.byte(match c.dimension {
            BudgetDimension::Token => 0,
            BudgetDimension::CostCents => 1,
            BudgetDimension::TimeSecs => 2,
        });
        e.u64(c.amount);
    }
    e.0
}
/// Computes the digest without incidental serialization or enum discriminants.
pub fn disposition_digest(disposition: &AttemptDisposition) -> DispositionDigest {
    DispositionDigest(Sha256::digest(canonical_disposition(disposition)).into())
}
