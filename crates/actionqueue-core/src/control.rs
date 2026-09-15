//! Trusted host control vocabulary. Neither scope nor identity is inferred from
//! request payloads, routing labels, caller references, or causal references.
use crate::{
    causal::ControlMutationContext,
    ids::{ActorId, TenantId},
};

/// Explicit namespace authenticated by the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ControlScope {
    /// Host-authorized administration of the whole store.
    Store,
    /// Host-authorized actor provisioning in one named tenant; grants no execution authority.
    ProvisionTenant(TenantId),
    /// One explicit tenant namespace.
    Tenant(TenantId),
    /// Explicit namespace of a store without the platform profile.
    SingleTenant,
}
/// Authentication result constructed only by trusted Rust host code. This type
/// deliberately has no Deserialize implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostControlContext {
    /// Authenticated queue actor, when applicable.
    pub actor_id: Option<ActorId>,
    /// Host-attested namespace; Store is explicit host administrative authority.
    pub scope: ControlScope,
    /// Bounded attribution, never an authorization credential.
    pub attribution: ControlMutationContext,
}
/// Queue permissions, independent of executor routing traits and external authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum QueueAction {
    /// Admit a task.
    AdmitTask,
    /// Inspect tasks and runs.
    InspectTask,
    /// Admit a signal.
    AdmitSignal,
    /// Inspect a signal.
    InspectSignal,
    /// Pin, unpin, or retire signals.
    RetainSignal,
    /// Cancel a task.
    CancelTask,
    /// Cancel a run.
    CancelRun,
    /// Suspend execution.
    SuspendRun,
    /// Resume execution.
    ResumeRun,
    /// Inspect waits.
    InspectWait,
    /// Explicitly resolve waits.
    ResolveWait,
    /// Cancel waits.
    CancelWait,
    /// Register an actor.
    RegisterActor,
    /// Deregister an actor.
    DeregisterActor,
    /// Report actor liveness.
    HeartbeatActor,
    /// Inspect eligible execution work.
    InspectClaimable,
    /// Claim execution work.
    ClaimRun,
    /// Renew an execution lease.
    RenewLease,
    /// Submit a fenced result.
    SubmitResult,
    /// Change budget allocation.
    ManageBudget,
    /// Change internal subscriptions.
    ManageSubscription,
    /// Append a ledger entry.
    AppendLedger,
    /// Inspect ledgers.
    InspectLedger,
    /// Pause dispatch throughout the store.
    PauseEngine,
    /// Resume dispatch throughout the store.
    ResumeEngine,
    /// Create tenants.
    ManageTenant,
    /// Change roles or grants.
    ManagePermission,
}
impl QueueAction {
    /// Actions that no tenant-local grant can authorize.
    pub fn requires_store(self) -> bool {
        matches!(
            self,
            Self::PauseEngine | Self::ResumeEngine | Self::ManageTenant | Self::ManagePermission
        )
    }
    /// Explicit action-to-permission mapping. Legacy broad capabilities never
    /// implicitly grant newly introduced queue actions.
    pub fn permission(self) -> crate::platform::Capability {
        crate::platform::Capability::Queue(self)
    }
}

/// Rejection at the trusted control boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    /// Principal lacks an explicit, current permission.
    Unauthorized,
    /// Attested namespace does not match the target/store profile.
    Scope,
    /// Target does not exist within the authorized namespace.
    NotFound,
    /// Invalid operation or uncertain durable mutation.
    Mutation(String),
}
impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "control rejected: {self:?}")
    }
}
impl std::error::Error for ControlError {}

/// Durable host attribution attached to the same WAL frame as the mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ControlAttribution {
    /// Authenticated queue actor, if supplied by the host.
    pub actor_id: Option<ActorId>,
    /// Explicit authenticated namespace.
    pub scope: ControlScope,
    /// Bounded caller/session/request fields.
    pub context: ControlMutationContext,
}
impl From<&HostControlContext> for ControlAttribution {
    fn from(host: &HostControlContext) -> Self {
        Self { actor_id: host.actor_id, scope: host.scope, context: host.attribution.clone() }
    }
}
