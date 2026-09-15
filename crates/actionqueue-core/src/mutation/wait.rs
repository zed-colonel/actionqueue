//! Handler-independent continuation mutations. Storage owns validation and winner selection.
use super::AttemptCommitExpectation;
use crate::{
    causal::ControlMutationContext,
    continuation::{CheckpointRef, WaitSpec},
    ids::{RunId, TaskId, TenantId, WaitId},
};
/// Atomic attempt yield, lease release, and wait establishment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitEstablishCommand {
    pub expected: AttemptCommitExpectation,
    pub wait: WaitSpec,
    pub checkpoint: Option<CheckpointRef>,
    pub timestamp: u64,
}
/// Explicit host-attested wake, separate from administrative suspension resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitResolveCommand {
    pub expected_sequence: u64,
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub tenant_id: Option<TenantId>,
    pub control_context: ControlMutationContext,
    pub timestamp: u64,
}
/// A compound cancellation target. Task cancellation includes every owned nonterminal run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CancelTarget {
    Run(RunId),
    Task(TaskId),
}
/// Host cancellation, with scope attested at ingress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelCommand {
    pub expected_sequence: u64,
    pub target: CancelTarget,
    pub tenant_id: Option<TenantId>,
    pub control_context: Option<ControlMutationContext>,
    pub timestamp: u64,
}
/// Definitive pre-append continuation rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitRejection {
    InvalidIdentity,
    NotFound,
    TenantMismatch,
    StaleSequence,
    StaleAttempt,
    StaleLease,
    TaskCanceled,
    /// The target task or run already reached its outcome; completed work is immutable history.
    AlreadyTerminal,
    InvalidState,
    ActiveWaitExists,
    /// The store or tenant active-wait creation quota is exhausted.
    Capacity,
    WaitAlreadyResolved,
    InvalidSignal,
    NotDue,
    InvalidCheckpoint,
    TooLarge,
    ImmediateDurabilityRequired,
    UnsupportedFeature,
    ConflictingKeyOwnership,
}
impl std::fmt::Display for WaitRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "continuation rejected: {self:?}")
    }
}
impl std::error::Error for WaitRejection {}
/// A retry reports the original commit sequence and makes no append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    Established { wait_id: WaitId, sequence: u64 },
    AlreadyEstablished { wait_id: WaitId, sequence: u64 },
    Resolved { wait_id: WaitId, sequence: u64 },
    AlreadyResolved { wait_id: WaitId, sequence: u64 },
}
impl WaitOutcome {
    pub fn sequence(self) -> u64 {
        match self {
            Self::Established { sequence, .. }
            | Self::AlreadyEstablished { sequence, .. }
            | Self::Resolved { sequence, .. }
            | Self::AlreadyResolved { sequence, .. } => sequence,
        }
    }
}
