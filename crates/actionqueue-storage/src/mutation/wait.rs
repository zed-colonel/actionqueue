//! Immutable continuation history. Wire layout is owned by storage, independently of core.
use actionqueue_core::{
    causal::ControlMutationContext,
    continuation::{CheckpointRef, WaitSpec},
    ids::*,
    mutation::CancelTarget,
};
/// The immutable winner; signals are non-consuming observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitResolutionKind {
    Signal(SignalSequence),
    Deadline,
    Control(ControlMutationContext),
    Canceled(Option<ControlMutationContext>),
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "crate::wal::wait_v1::ResolutionV1", into = "crate::wal::wait_v1::ResolutionV1")]
pub struct WaitResolution {
    pub run_id: RunId,
    pub wait_id: WaitId,
    pub sequence: u64,
    pub timestamp: u64,
    pub kind: WaitResolutionKind,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "crate::wal::wait_v1::WaitV1", into = "crate::wal::wait_v1::WaitV1")]
pub struct WaitRecord {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub lease_owner: String,
    pub lease_granted_at_sequence: u64,
    pub sequence: u64,
    pub timestamp: u64,
    pub spec: WaitSpec,
    pub checkpoint: Option<CheckpointRef>,
    pub resolution: Option<WaitResolution>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "crate::wal::wait_v1::CancelV1", into = "crate::wal::wait_v1::CancelV1")]
pub struct CancelRecord {
    pub target: CancelTarget,
    pub tenant_id: Option<TenantId>,
    pub control_context: Option<ControlMutationContext>,
    pub sequence: u64,
    pub timestamp: u64,
}
/// Prepared operation, including append-free retries.
#[derive(Debug)]
pub enum WaitPreparation {
    Event(Box<crate::wal::event::WalEvent>, actionqueue_core::mutation::AppliedMutation),
    Noop(actionqueue_core::mutation::MutationOutcome),
}
