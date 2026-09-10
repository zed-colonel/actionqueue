//! Signal commit proposal, wired into mutation authority in AQ-05.
use crate::continuation::SignalEnvelope;
/// Pure SignalAdmitCommand proposal; no mutation authority implementation yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalAdmitCommand {
    expected_sequence: u64,
    envelope: SignalEnvelope,
}
impl SignalAdmitCommand {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(expected_sequence: u64, envelope: SignalEnvelope) -> Self {
        Self { expected_sequence, envelope }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns envelope.
    pub fn envelope(&self) -> &SignalEnvelope {
        &self.envelope
    }
}
