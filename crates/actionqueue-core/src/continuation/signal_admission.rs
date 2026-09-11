//! Producer requests, host attestation, and typed durable signal results.
use super::{SignalEnvelope, SignalKind, SignalNamespace};
use crate::{
    bounded::{ContentHash, OpaqueRef},
    causal::{CausationLink, ControlMutationContext},
    data_ref::DataRef,
    ids::{CorrelationId, SignalId, SignalSequence, TenantId},
};
use sha2::{Digest, Sha256};

/// The authenticated host assigns scope and attribution; references grant no permission.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SignalIngressContext {
    /// Unscoped or platform tenant namespace.
    pub tenant_id: Option<TenantId>,
    /// Attribution of the first successful admission.
    pub control_context: Option<ControlMutationContext>,
}
/// Validated producer-controlled fields. Receipt time and scope are assigned separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmitSignalRequest {
    signal_id: SignalId,
    namespace: SignalNamespace,
    kind: SignalKind,
    correlation_id: Option<CorrelationId>,
    causation: Option<CausationLink>,
    source_ref: Option<OpaqueRef>,
    payload: Option<DataRef>,
    payload_hash: Option<ContentHash>,
    occurred_at: Option<u64>,
}
impl AdmitSignalRequest {
    /// Validates inline content and normalizes the effective payload hash.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        signal_id: SignalId,
        namespace: SignalNamespace,
        kind: SignalKind,
        correlation_id: Option<CorrelationId>,
        causation: Option<CausationLink>,
        source_ref: Option<OpaqueRef>,
        payload: Option<DataRef>,
        payload_hash: Option<ContentHash>,
        occurred_at: Option<u64>,
    ) -> Result<Self, SignalRejection> {
        let payload_hash = effective_payload_hash(payload.as_ref(), payload_hash.as_ref())?;
        Ok(Self {
            signal_id,
            namespace,
            kind,
            correlation_id,
            causation,
            source_ref,
            payload,
            payload_hash,
            occurred_at,
        })
    }
    /// Builds the store envelope from host-owned fields.
    pub fn envelope(&self, ingress: &SignalIngressContext, received_at: u64) -> SignalEnvelope {
        SignalEnvelope {
            signal_id: self.signal_id.clone(),
            tenant_id: ingress.tenant_id,
            namespace: self.namespace.clone(),
            kind: self.kind.clone(),
            correlation_id: self.correlation_id.clone(),
            causation: self.causation.clone(),
            source_ref: self.source_ref.clone(),
            payload: self.payload.clone(),
            payload_hash: self.payload_hash.clone(),
            occurred_at: self.occurred_at,
            received_at,
            control_context: ingress.control_context.clone(),
        }
    }
}
/// Hard payload validation, shared by direct commands, canonicalization and replay.
pub fn effective_payload_hash(
    payload: Option<&DataRef>,
    supplied: Option<&ContentHash>,
) -> Result<Option<ContentHash>, SignalRejection> {
    let hash = match payload {
        Some(DataRef::Inline(data)) => {
            if data.bytes().len() > crate::limits::MAX_INLINE_DATA_BYTES {
                return Err(SignalRejection::TooLarge);
            }
            if Sha256::digest(data.bytes()).as_slice() != data.hash().bytes() {
                return Err(SignalRejection::InvalidHash);
            }
            Some(data.hash())
        }
        Some(DataRef::External(data)) => Some(&data.hash),
        None => None,
    };
    if hash.zip(supplied).is_some_and(|(a, b)| a != b) {
        return Err(SignalRejection::InvalidHash);
    }
    Ok(hash.or(supplied).cloned())
}
/// Successful admission or exact retry of an immutable identity, including retired records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitSignalOutcome {
    /// A new durable signal.
    Admitted { signal_id: SignalId, sequence: SignalSequence },
    /// The original durable signal.
    AlreadyExists { signal_id: SignalId, sequence: SignalSequence },
}
impl AdmitSignalOutcome {
    /// Global signal sequence (not WAL sequence).
    pub fn sequence(&self) -> SignalSequence {
        match self {
            Self::Admitted { sequence, .. } | Self::AlreadyExists { sequence, .. } => *sequence,
        }
    }
}
/// Rejections are definitive and happen before append; storage uncertainty is separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalRejection {
    /// Same tenant/id, different canonical producer content.
    Conflict,
    /// Invalid canonical envelope, ordering or transition.
    InvalidEnvelope,
    /// Inline bytes, effective hash or canonical digest mismatch.
    InvalidHash,
    /// Unknown tenant or local ancestry owned by another tenant.
    TenantMismatch,
    /// Local causation task/run/attempt does not exist or ancestry is inconsistent.
    InvalidCausation,
    /// Platform profile or signal projection unavailable.
    UnsupportedFeature,
    /// Hard or configurable size exceeded.
    TooLarge,
    /// Resident identities, bytes or pins would exceed capacity.
    Capacity,
    /// Signal or WAL sequence cannot advance.
    SequenceExhausted,
    /// Signal operations require immediate durability.
    ImmediateDurabilityRequired,
    /// Target signal does not exist in this tenant.
    NotFound,
    /// Retirement proposal became protected or is too young/recent.
    Protected,
    /// Retired signals cannot be pinned or reactivated.
    Retired,
}
impl std::fmt::Display for SignalRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "signal rejected: {self:?}")
    }
}
impl std::error::Error for SignalRejection {}

crate::bounded::bounded_text!(
    /// Stable identity for an independent retention pin.
    SignalPinId, crate::limits::MAX_SIGNAL_ID_BYTES, crate::bounded::TextGrammar::Opaque
);
