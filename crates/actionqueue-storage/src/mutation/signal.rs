//! Immutable signal facts and durable retention state; wire DTOs are storage-owned.
use std::collections::BTreeMap;

use actionqueue_core::{
    causal::ControlMutationContext,
    continuation::*,
    ids::{SignalId, SignalSequence, TenantId},
};
/// Attribution and WAL order of a retention transition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    try_from = "crate::wal::signal_v1::ControlStampV1",
    into = "crate::wal::signal_v1::ControlStampV1"
)]
pub struct SignalControlRecord {
    /// Committing WAL sequence.
    pub wal_sequence: u64,
    /// Host receipt time; replay does not consult a clock.
    pub timestamp: u64,
    /// Original host-attested attribution.
    pub control_context: Option<ControlMutationContext>,
}
/// Signal identity/content remain available after retirement from matching.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    try_from = "crate::wal::signal_v1::SignalRecordV1",
    into = "crate::wal::signal_v1::SignalRecordV1"
)]
pub struct SignalRecord {
    pub(crate) envelope: SignalEnvelope,
    pub(crate) digest: SignalDigest,
    pub(crate) sequence: SignalSequence,
    pub(crate) wal_sequence: u64,
    pub(crate) retirement: Option<SignalControlRecord>,
    pub(crate) pins: BTreeMap<SignalPinId, SignalControlRecord>,
}
impl SignalRecord {
    /// Constructs a hard-validated immutable fact for admission or replay.
    pub fn new(
        mut envelope: SignalEnvelope,
        sequence: SignalSequence,
        wal_sequence: u64,
    ) -> Result<Self, SignalRejection> {
        if sequence.get() == 0 || wal_sequence == 0 {
            return Err(SignalRejection::InvalidEnvelope);
        }
        envelope.payload_hash =
            effective_payload_hash(envelope.payload.as_ref(), envelope.payload_hash.as_ref())?;
        let digest = CanonicalSignalV1::new(&envelope)?.digest();
        Ok(Self {
            envelope,
            digest,
            sequence,
            wal_sequence,
            retirement: None,
            pins: BTreeMap::new(),
        })
    }
    /// Original content and attribution.
    pub fn envelope(&self) -> &SignalEnvelope {
        &self.envelope
    }
    /// Versioned canonical digest.
    pub fn digest(&self) -> &SignalDigest {
        &self.digest
    }
    /// Global signal order.
    pub fn sequence(&self) -> SignalSequence {
        self.sequence
    }
    /// Admitting WAL sequence, independent of signal order.
    pub fn wal_sequence(&self) -> u64 {
        self.wal_sequence
    }
    /// Durable retirement facts, if no longer matchable.
    pub fn retirement(&self) -> Option<&SignalControlRecord> {
        self.retirement.as_ref()
    }
    /// Independent explicit pins.
    pub fn pins(&self) -> &BTreeMap<SignalPinId, SignalControlRecord> {
        &self.pins
    }
    /// Whether structural matching may consider this signal.
    pub fn is_retained(&self) -> bool {
        self.retirement.is_none()
    }
    pub(crate) fn outcome(&self, admitted: bool) -> AdmitSignalOutcome {
        let signal_id = self.envelope.signal_id.clone();
        let sequence = self.sequence;
        if admitted {
            AdmitSignalOutcome::Admitted { signal_id, sequence }
        } else {
            AdmitSignalOutcome::AlreadyExists { signal_id, sequence }
        }
    }
    pub(crate) fn resolve(
        &self,
        digest: &SignalDigest,
    ) -> Result<AdmitSignalOutcome, SignalRejection> {
        if digest == &self.digest {
            Ok(self.outcome(false))
        } else {
            Err(SignalRejection::Conflict)
        }
    }
    /// Encoded immutable admission bytes, independent of current pin/retirement state.
    pub fn encoded_bytes(&self) -> Result<usize, SignalRejection> {
        self.encoded_bytes_with_control(None)
    }
    /// Complete immutable admission frame size, including host attribution.
    pub fn encoded_bytes_with_control(
        &self,
        control: Option<&actionqueue_core::control::ControlAttribution>,
    ) -> Result<usize, SignalRejection> {
        let mut record = self.clone();
        record.retirement = None;
        record.pins.clear();
        let event = crate::wal::event::WalEvent::new(
            self.wal_sequence,
            crate::wal::event::WalEventType::SignalAdmitted { record },
        );
        let event =
            if let Some(control) = control { event.with_control(control.clone()) } else { event };
        crate::wal::codec::encode(&event).map(|v| v.len()).map_err(|_| SignalRejection::TooLarge)
    }
}
/// Durable pin transition. The two WAL kinds distinguish acquisition and release.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "crate::wal::signal_v1::PinV1", into = "crate::wal::signal_v1::PinV1")]
pub struct SignalPinRecord {
    /// Target scope.
    pub tenant_id: Option<TenantId>,
    /// Target signal identity.
    pub signal_id: SignalId,
    /// Independent pin identity.
    pub pin_id: SignalPinId,
    /// Transition order, time and attribution.
    pub control: SignalControlRecord,
}
/// A bounded, ordered retirement transition.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "crate::wal::signal_v1::RetiredV1", into = "crate::wal::signal_v1::RetiredV1")]
pub struct SignalsRetiredRecord {
    /// Target scope.
    pub tenant_id: Option<TenantId>,
    /// Strictly increasing target sequences.
    pub sequences: Vec<SignalSequence>,
    /// Transition order, time and attribution.
    pub control: SignalControlRecord,
}
