//! Immutable checkpoint reference.
use crate::data_ref::DataRef;
use crate::ids::{AttemptId, CheckpointId};
/// Checkpoint created by one attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CheckpointRef {
    /// Checkpoint identity.
    pub checkpoint_id: CheckpointId,
    /// Immutable data reference.
    pub data: DataRef,
    /// Creating attempt.
    pub created_by_attempt: AttemptId,
}
