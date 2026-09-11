//! Timestamp/sequence-only retention arithmetic. No signal routing fields are read.
pub use actionqueue_core::limits::SignalRetentionPolicy;
/// Uses the same conservative arithmetic as authoritative commit validation.
pub fn eligible(
    policy: SignalRetentionPolicy,
    received_at: u64,
    sequence: u64,
    last_sequence: u64,
    now: u64,
    protected: bool,
) -> bool {
    policy.permits(received_at, sequence, last_sequence, now, protected)
}
