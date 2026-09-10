//! Bounded caller-supplied identifier.
crate::bounded::bounded_text!(/// Opaque caller-supplied identity, compared by exact equality.
SignalId, crate::limits::MAX_SIGNAL_ID_BYTES, 0);
