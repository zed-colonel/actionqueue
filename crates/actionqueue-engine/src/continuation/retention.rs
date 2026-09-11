//! Timestamp/sequence-only retention arithmetic. No signal routing fields are read.
pub use actionqueue_core::limits::{SignalRetentionCandidate, SignalRetentionPolicy};
/// Uses the same conservative arithmetic as authoritative commit validation.
pub fn eligible(
    policy: SignalRetentionPolicy,
    candidate: SignalRetentionCandidate,
    last_sequence: u64,
    now: u64,
) -> bool {
    policy.permits(candidate, last_sequence, now)
}
