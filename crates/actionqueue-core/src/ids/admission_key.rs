//! Bounded caller-supplied identifier.
crate::bounded::bounded_text!(
    /// Opaque caller-supplied identity, compared by exact equality.
    AdmissionKey,
    crate::limits::MAX_ADMISSION_KEY_BYTES,
    crate::bounded::TextGrammar::Opaque
);
