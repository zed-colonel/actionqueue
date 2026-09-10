//! Admission commit proposal, wired into mutation authority in AQ-04.
use crate::admission::AdmissionPlan;
use crate::causal::ControlMutationContext;
/// Pure AdmissionCommitCommand proposal; no mutation authority implementation yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionCommitCommand {
    expected_sequence: u64,
    plan: AdmissionPlan,
    control_context: Option<ControlMutationContext>,
    timestamp: u64,
}
impl AdmissionCommitCommand {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(
        expected_sequence: u64,
        plan: AdmissionPlan,
        control_context: Option<ControlMutationContext>,
        timestamp: u64,
    ) -> Self {
        Self { expected_sequence, plan, control_context, timestamp }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns plan.
    pub fn plan(&self) -> &AdmissionPlan {
        &self.plan
    }
    /// Returns control context.
    pub fn control_context(&self) -> &Option<ControlMutationContext> {
        &self.control_context
    }
    /// Returns timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
