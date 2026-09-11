//! Wait resolution commands, validated and committed by the storage authority.
use crate::causal::ControlMutationContext;
use crate::ids::{RunId, SignalSequence, TenantId, WaitId};
/// Resolve a wait using its earliest eligible durable signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitSatisfyCommand {
    expected_sequence: u64,
    run_id: RunId,
    wait_id: WaitId,
    signal_sequence: SignalSequence,
    timestamp: u64,
}
impl WaitSatisfyCommand {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(
        expected_sequence: u64,
        run_id: RunId,
        wait_id: WaitId,
        signal_sequence: SignalSequence,
        timestamp: u64,
    ) -> Self {
        Self { expected_sequence, run_id, wait_id, signal_sequence, timestamp }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns run id.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns wait id.
    pub fn wait_id(&self) -> WaitId {
        self.wait_id
    }
    /// Returns signal sequence.
    pub fn signal_sequence(&self) -> SignalSequence {
        self.signal_sequence
    }
    /// Returns timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
/// Resolve a due deadline using the policy stored with the wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitTimeoutCommand {
    expected_sequence: u64,
    run_id: RunId,
    wait_id: WaitId,
    timestamp: u64,
}
impl WaitTimeoutCommand {
    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(expected_sequence: u64, run_id: RunId, wait_id: WaitId, timestamp: u64) -> Self {
        Self { expected_sequence, run_id, wait_id, timestamp }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns run id.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns wait id.
    pub fn wait_id(&self) -> WaitId {
        self.wait_id
    }
    /// Returns timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
/// Cancel one identified wait and its owning run with host attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitCancelCommand {
    expected_sequence: u64,
    run_id: RunId,
    wait_id: WaitId,
    control_context: ControlMutationContext,
    tenant_id: Option<TenantId>,
    timestamp: u64,
}
impl WaitCancelCommand {
    /// Attests the target tenant at ingress.
    pub fn with_tenant(mut self, tenant: Option<TenantId>) -> Self {
        self.tenant_id = tenant;
        self
    }
    /// Host-attested tenant scope.
    pub fn tenant_id(&self) -> Option<TenantId> {
        self.tenant_id
    }

    /// Constructs an immutable proposal. Store checks occur at commit.
    pub fn new(
        expected_sequence: u64,
        run_id: RunId,
        wait_id: WaitId,
        control_context: ControlMutationContext,
        timestamp: u64,
    ) -> Self {
        Self { expected_sequence, run_id, wait_id, control_context, tenant_id: None, timestamp }
    }
    /// Returns expected sequence.
    pub fn expected_sequence(&self) -> u64 {
        self.expected_sequence
    }
    /// Returns run id.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns wait id.
    pub fn wait_id(&self) -> WaitId {
        self.wait_id
    }
    /// Returns control context.
    pub fn control_context(&self) -> &ControlMutationContext {
        &self.control_context
    }
    /// Returns timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
