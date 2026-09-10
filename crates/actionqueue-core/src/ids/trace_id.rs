//! Bounded caller-supplied identifier.
crate::bounded::bounded_text!(/// Opaque caller-supplied identity, compared by exact equality.
TraceId, crate::limits::MAX_TRACE_ID_BYTES, crate::bounded::TextGrammar::Opaque);
