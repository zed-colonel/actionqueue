//! Exactly one wait per awaiting run (ADR-007).
use super::SignalFilter;
use crate::bounded::BoundedCode;
use crate::ids::{SignalSequence, WaitId};
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
        f.write_str("AnyRetained requires a correlation filter")
    }
}
impl std::error::Error for WaitSpecError {}
/// Validated single continuation wait.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "WaitWire"))]
pub struct WaitSpec {
    wait_id: WaitId,
    filter: SignalFilter,
    match_policy: WaitMatchPolicy,
    eligible_from: SignalEligibility,
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
        Ok(Self { wait_id, filter, match_policy, eligible_from, deadline })
    }
    /// Wait identity.
    pub fn wait_id(&self) -> WaitId {
        self.wait_id
    }
    /// Exact structural filter.
    pub fn filter(&self) -> &SignalFilter {
        &self.filter
    }
    /// Match policy.
    pub fn match_policy(&self) -> &WaitMatchPolicy {
        &self.match_policy
    }
    /// Eligible store history.
    pub fn eligible_from(&self) -> &SignalEligibility {
        &self.eligible_from
    }
    /// Optional deadline.
    pub fn deadline(&self) -> Option<&WaitDeadline> {
        self.deadline.as_ref()
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct WaitWire {
    wait_id: WaitId,
    filter: SignalFilter,
    match_policy: WaitMatchPolicy,
    eligible_from: SignalEligibility,
    deadline: Option<WaitDeadline>,
}
#[cfg(feature = "serde")]
impl TryFrom<WaitWire> for WaitSpec {
    type Error = WaitSpecError;
    fn try_from(w: WaitWire) -> Result<Self, Self::Error> {
        Self::new(w.wait_id, w.filter, w.match_policy, w.eligible_from, w.deadline)
    }
}
