//! Structural reason and data delivered to a resumed attempt.
use super::{CheckpointRef, SignalEnvelope};
use crate::causal::ControlMutationContext;
use crate::ids::{SignalSequence, WaitId};
/// Data supplied on resumption.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeContext {
    /// Store-local WAL identity of the original wake.
    pub context_id: ResumeContextId,
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
        /// Store sequence.
        signal_sequence: SignalSequence,
        /// Full signal, including its identity.
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

/// Stable store-local identity: WAL sequence of the wake that created this context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeContextId(pub u64);
/// Why this physical attempt receives the original wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ResumeDelivery {
    /// First accepted attempt for this wake.
    Initial,
    /// Previous handler attempt failed or timed out.
    Retry,
    /// Previous accepted attempt was interrupted by executor loss.
    Recovery,
}
/// Immutable delivery lineage stored with the accepted attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResumeAssignment {
    /// Original wake identity.
    pub context_id: ResumeContextId,
    /// Previous physical recipient, if any.
    pub previous_attempt_id: Option<crate::ids::AttemptId>,
    /// Initial, retry or recovery delivery.
    pub delivery: ResumeDelivery,
}
/// Durable origin of an attempt closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AttemptFinishOrigin {
    /// Handler/executor supplied the result.
    #[default]
    Executor,
    /// Restart closed an interrupted accepted attempt.
    Recovery,
}
