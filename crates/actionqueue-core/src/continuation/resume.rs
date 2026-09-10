//! Structural reason and data delivered to a resumed attempt.
use super::{CheckpointRef, SignalEnvelope};
use crate::causal::ControlMutationContext;
use crate::ids::{SignalId, SignalSequence, WaitId};
/// Data supplied on resumption.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeContext {
    /// Latest checkpoint, if any.
    pub checkpoint: Option<CheckpointRef>,
    /// Durable wake reason.
    pub wake: WakeReason,
    /// Resumption timestamp.
    pub resumed_at: u64,
}
impl ResumeContext {
    /// Wait resolved by this resumption, if any.
    pub fn wait_id(&self) -> Option<WaitId> {
        self.wake.wait_id()
    }
}
/// The cause of a resumption.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WakeReason {
    /// A durable matching signal.
    Signal {
        /// Wait identity.
        wait_id: WaitId,
        /// Signal identity.
        signal_id: SignalId,
        /// Store sequence.
        signal_sequence: SignalSequence,
        /// Full signal.
        envelope: Box<SignalEnvelope>,
    },
    /// Wait deadline elapsed.
    Deadline {
        /// Wait identity.
        wait_id: WaitId,
        /// Deadline timestamp.
        deadline_at: u64,
    },
    /// Host-attested wait resolution.
    ControlResolution {
        /// Wait identity.
        wait_id: WaitId,
        /// Host context.
        control_context: ControlMutationContext,
    },
    /// Administrative resumption without a wait.
    AdministrativeResume {
        /// Host context, if supplied.
        control_context: Option<ControlMutationContext>,
    },
}
impl WakeReason {
    /// Wait resolved by this wake, if any.
    pub fn wait_id(&self) -> Option<WaitId> {
        match self {
            Self::Signal { wait_id, .. }
            | Self::Deadline { wait_id, .. }
            | Self::ControlResolution { wait_id, .. } => Some(*wait_id),
            Self::AdministrativeResume { .. } => None,
        }
    }
}
