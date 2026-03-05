//! Safety level classification for task execution.

/// Classification of a task's side-effect safety characteristics.
///
/// This classification informs the engine about the safety guarantees
/// of a task's handler, enabling appropriate retry and scheduling decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SafetyLevel {
    /// The task produces no side effects. Safe to retry unconditionally.
    #[default]
    Pure,
    /// The task may produce side effects but is safe to retry
    /// (applying the same operation multiple times has the same effect as once).
    Idempotent,
    /// The task produces non-idempotent side effects. Retrying may cause
    /// duplicate effects. Use with caution.
    Transactional,
}
