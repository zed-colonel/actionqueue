//! Exact structural signal matching; no payload predicates or wildcard syntax.
use crate::bounded::{ContentHash, OpaqueRef};
use crate::causal::{CausationLink, ControlMutationContext};
use crate::data_ref::DataRef;
use crate::ids::{CorrelationId, SignalId, TenantId};
crate::bounded::bounded_text!(/// Bounded lowercase signal namespace.
    SignalNamespace, crate::limits::MAX_SIGNAL_NAMESPACE_BYTES, crate::bounded::TextGrammar::LowercaseCode);
crate::bounded::bounded_text!(/// Bounded lowercase signal kind.
    SignalKind, crate::limits::MAX_SIGNAL_KIND_BYTES, crate::bounded::TextGrammar::LowercaseCode);
/// Structural SignalEnvelope; all text components validate on construction and decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignalEnvelope {
    /// Signal id.
    pub signal_id: SignalId,
    /// Tenant id.
    pub tenant_id: Option<TenantId>,
    /// Namespace.
    pub namespace: SignalNamespace,
    /// Kind.
    pub kind: SignalKind,
    /// Correlation id.
    pub correlation_id: Option<CorrelationId>,
    /// Causation.
    pub causation: Option<CausationLink>,
    /// Source ref.
    pub source_ref: Option<OpaqueRef>,
    /// Payload.
    pub payload: Option<DataRef>,
    /// Payload hash.
    pub payload_hash: Option<ContentHash>,
    /// Occurred at.
    pub occurred_at: Option<u64>,
    /// Received at.
    pub received_at: u64,
    /// Control context.
    pub control_context: Option<ControlMutationContext>,
}
/// Structural SignalProposal; all text components validate on construction and decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignalProposal {
    /// Signal id.
    pub signal_id: SignalId,
    /// Namespace.
    pub namespace: SignalNamespace,
    /// Kind.
    pub kind: SignalKind,
    /// Correlation id.
    pub correlation_id: CorrelationId,
    /// Payload.
    pub payload: Option<DataRef>,
    /// Payload hash.
    pub payload_hash: Option<ContentHash>,
    /// Occurred at.
    pub occurred_at: Option<u64>,
}
/// Structural SignalFilter; all text components validate on construction and decode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SignalFilter {
    /// Tenant id.
    pub tenant_id: Option<TenantId>,
    /// Namespace.
    pub namespace: SignalNamespace,
    /// Kind.
    pub kind: SignalKind,
    /// Correlation id.
    pub correlation_id: Option<CorrelationId>,
    /// Source ref.
    pub source_ref: Option<OpaqueRef>,
}
impl SignalFilter {
    /// Exact tenant, namespace and kind; absent optional filters are unconstrained.
    /// An unscoped tenant (`None`) never matches a scoped tenant (`Some`).
    pub fn matches(&self, signal: &SignalEnvelope) -> bool {
        self.tenant_id == signal.tenant_id
            && self.namespace == signal.namespace
            && self.kind == signal.kind
            && self
                .correlation_id
                .as_ref()
                .is_none_or(|id| Some(id) == signal.correlation_id.as_ref())
            && self
                .source_ref
                .as_ref()
                .is_none_or(|source| Some(source) == signal.source_ref.as_ref())
    }
}
