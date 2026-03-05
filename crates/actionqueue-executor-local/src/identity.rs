//! Executor identity abstraction for lease ownership.
//!
//! [`ExecutorIdentity`] abstracts "who is executing this run" beyond the
//! hardcoded `"runtime"` string used in lease acquire and heartbeat commands.
//!
//! In v0.x, only [`LocalExecutorIdentity`] is used. In v1.0 (Sprint 4),
//! remote actors will provide their own identity via this trait, allowing
//! the dispatch loop to issue leases on their behalf without code changes.

/// Identity of the executor acquiring and heartbeating leases.
///
/// Implemented by the dispatch loop's configured executor. In the local
/// case, this always returns `"runtime"`. Remote actors in Sprint 4 will
/// provide their registered actor identity string instead.
pub trait ExecutorIdentity: Send + Sync {
    /// Returns the identity string used as the lease owner.
    fn identity(&self) -> &str;
}

/// The local executor identity — used when ActionQueue runs all work in-process.
///
/// Returns `"runtime"` as the lease owner, matching the previous hardcoded value.
#[derive(Debug, Clone, Default)]
pub struct LocalExecutorIdentity;

impl ExecutorIdentity for LocalExecutorIdentity {
    fn identity(&self) -> &str {
        "runtime"
    }
}
