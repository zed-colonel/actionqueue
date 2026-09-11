//! Immutable signal proposals. The authority repeats all checks before append.
use crate::{
    causal::ControlMutationContext,
    continuation::{SignalEnvelope, SignalPinId},
    ids::{SignalId, SignalSequence, TenantId},
};
/// New signal proposal; the store assigns its independent signal sequence at commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalAdmitCommand {
    expected_sequence: u64,
    envelope: Box<SignalEnvelope>,
}
impl SignalAdmitCommand {
    /// Expected WAL sequence and host-attributed envelope.
    pub fn new(expected_sequence: u64, envelope: SignalEnvelope) -> Self {
        Self { expected_sequence, envelope: Box::new(envelope) }
    }
    /// Expected WAL sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Proposed envelope.
    pub fn envelope(&self) -> &SignalEnvelope {
        &self.envelope
    }
}
/// Independent pin acquisition/release, scoped by tenant and signal identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalPinCommand {
    /// Expected WAL sequence (ignored for an exact no-op).
    pub expected_sequence: u64,
    /// Target tenant.
    pub tenant_id: Option<TenantId>,
    /// Target identity.
    pub signal_id: SignalId,
    /// Stable pin owner identity.
    pub pin_id: SignalPinId,
    /// Store receipt time for this operation.
    pub timestamp: u64,
    /// Host-attested control attribution.
    pub control_context: Option<ControlMutationContext>,
}
/// Bounded retirement proposal; protection and policy are rechecked under ownership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireSignalsCommand {
    /// Expected WAL sequence.
    pub expected_sequence: u64,
    /// Tenant namespace for every target.
    pub tenant_id: Option<TenantId>,
    /// Strictly ordered unique signal sequences.
    pub sequences: Vec<SignalSequence>,
    /// Store time used for age eligibility and durable retirement attribution.
    pub timestamp: u64,
    /// Host-attested attribution.
    pub control_context: Option<ControlMutationContext>,
}
