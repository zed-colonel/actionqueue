//! Bounded caller-supplied identifier.
crate::bounded::bounded_text!(/// Opaque caller-supplied identity, compared by exact equality.
CorrelationId, crate::limits::MAX_CORRELATION_ID_BYTES, crate::bounded::TextGrammar::Opaque);
