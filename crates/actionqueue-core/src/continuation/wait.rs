//! Exactly one wait per awaiting run (ADR-007).
use super::SignalFilter;
use crate::bounded::BoundedCode;
use crate::ids::{SignalSequence, TaskId, WaitId};
/// Deterministic signal selection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WaitMatchPolicy {
    /// Select the earliest eligible matching signal without consuming it.
    FirstMatch,
}
/// Signal eligibility is store order, never wall-clock order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SignalEligibility {
    /// Any retained signal; requires correlation.
    AnyRetained,
    /// Strictly later than the supplied store cursor.
    After(SignalSequence),
}
/// Deadline action.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WaitTimeoutPolicy {
    /// Resume with a timeout wake.
    ResumeWithTimeout,
    /// Fail with a bounded classification.
    FailRun {
        /// Failure code.
        code: BoundedCode,
    },
    /// Cancel the run.
    CancelRun,
}
/// Absolute deadline and deterministic resolution policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WaitDeadline {
    /// Deadline timestamp.
    pub at: u64,
    /// Resolution policy.
    pub policy: WaitTimeoutPolicy,
}
/// Retained history matching requires a correlation filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitSpecError;
impl std::fmt::Display for WaitSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "invalid wait: retained signals require correlation; child targets require 1..=64 \
             non-nil IDs",
        )
    }
}
impl std::error::Error for WaitSpecError {}
/// Bounded child target policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ChildWaitPolicy {
    /// Return all terminal outcomes.
    AllTerminal,
    /// Return all successes or the lowest-ID terminal failure/cancellation witness.
    AllSucceededOrAnyFailed,
}
/// The durable fact a continuation observes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub enum WaitTarget {
    /// A retained external signal.
    Signal { filter: SignalFilter, match_policy: WaitMatchPolicy, eligible_from: SignalEligibility },
    /// Direct children, sorted and bounded by the validated WaitSpec constructor.
    Children { task_ids: Vec<TaskId>, policy: ChildWaitPolicy },
}
/// Validated single continuation wait.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "WaitWire"))]
pub struct WaitSpec {
    wait_id: WaitId,
    target: WaitTarget,
    deadline: Option<WaitDeadline>,
}
impl WaitSpec {
    /// Rejects broad matching against retained history.
    pub fn new(
        wait_id: WaitId,
        filter: SignalFilter,
        match_policy: WaitMatchPolicy,
        eligible_from: SignalEligibility,
        deadline: Option<WaitDeadline>,
    ) -> Result<Self, WaitSpecError> {
        if eligible_from == SignalEligibility::AnyRetained && filter.correlation_id.is_none() {
            return Err(WaitSpecError);
        }
        Ok(Self {
            wait_id,
            target: WaitTarget::Signal { filter, match_policy, eligible_from },
            deadline,
        })
    }
    /// Constructs a nonempty target set, rejecting oversized input before normalization.
    pub fn children(
        wait_id: WaitId,
        mut task_ids: Vec<TaskId>,
        policy: ChildWaitPolicy,
        deadline: Option<WaitDeadline>,
    ) -> Result<Self, WaitSpecError> {
        if task_ids.is_empty()
            || task_ids.len() > crate::limits::MAX_CHILD_WAIT_TARGETS
            || task_ids.iter().any(|id| id.is_nil())
        {
            return Err(WaitSpecError);
        }
        task_ids.sort();
        task_ids.dedup();
        Ok(Self { wait_id, target: WaitTarget::Children { task_ids, policy }, deadline })
    }
    /// Wait identity.
    pub fn wait_id(&self) -> WaitId {
        self.wait_id
    }
    /// Typed target.
    pub fn target(&self) -> &WaitTarget {
        &self.target
    }
    /// Signal filter, when this is a signal wait.
    pub fn filter(&self) -> Option<&SignalFilter> {
        match &self.target {
            WaitTarget::Signal { filter, .. } => Some(filter),
            _ => None,
        }
    }
    /// Signal policy, when this is a signal wait.
    pub fn match_policy(&self) -> Option<&WaitMatchPolicy> {
        match &self.target {
            WaitTarget::Signal { match_policy, .. } => Some(match_policy),
            _ => None,
        }
    }
    /// Signal eligibility, when this is a signal wait.
    pub fn eligible_from(&self) -> Option<&SignalEligibility> {
        match &self.target {
            WaitTarget::Signal { eligible_from, .. } => Some(eligible_from),
            _ => None,
        }
    }
    /// Optional deadline.
    pub fn deadline(&self) -> Option<&WaitDeadline> {
        self.deadline.as_ref()
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitWire {
    wait_id: WaitId,
    target: WaitTarget,
    deadline: Option<WaitDeadline>,
}
#[cfg(feature = "serde")]
impl TryFrom<WaitWire> for WaitSpec {
    type Error = WaitSpecError;
    fn try_from(w: WaitWire) -> Result<Self, Self::Error> {
        match w.target {
            WaitTarget::Signal { filter, match_policy, eligible_from } => {
                Self::new(w.wait_id, filter, match_policy, eligible_from, w.deadline)
            }
            WaitTarget::Children { task_ids, policy } => {
                Self::children(w.wait_id, task_ids, policy, w.deadline)
            }
        }
    }
}
