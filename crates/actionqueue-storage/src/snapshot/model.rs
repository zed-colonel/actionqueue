//! Snapshot model types for state persistence.
//!
//! This module defines the data structures used for snapshot persistence and recovery.
//! Snapshots provide a point-in-time view of the ActionQueue state that can be used to
//! accelerate recovery by reducing the amount of WAL that needs to be replayed.

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{ActorId, LedgerEntryId, RunId, TaskId, TenantId};
use actionqueue_core::platform::{Capability, Role};
use actionqueue_core::run::run_instance::RunInstance as CoreRunInstance;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};
use actionqueue_core::task::task_spec::TaskSpec;

use crate::recovery::reducer::{AttemptHistoryEntry, LeaseMetadata, RunStateHistoryEntry};

/// A versioned snapshot of the ActionQueue state.
///
/// Snapshots capture the state of the ActionQueue system at a point in time and are used
/// to accelerate recovery by providing a starting state for WAL replay. The snapshot
/// format is versioned to support future evolution of the data structures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Snapshot {
    /// The format version of this snapshot.
    ///
    /// This version is used to ensure compatibility between the snapshot writer and
    /// loader. When the snapshot format changes in a breaking way, this version
    /// number should be incremented.
    pub version: u32,
    /// The timestamp when this snapshot was taken (Unix epoch seconds).
    ///
    /// This timestamp represents the point-in-time at which the snapshot was captured.
    pub timestamp: u64,
    /// Metadata about the snapshot for versioning and compatibility.
    pub metadata: SnapshotMetadata,
    /// The tasks known at the time of the snapshot.
    pub tasks: Vec<SnapshotTask>,
    /// The runs known at the time of the snapshot.
    pub runs: Vec<SnapshotRun>,
    /// Engine control projection at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub engine: SnapshotEngineControl,
    /// Dependency declarations at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub dependency_declarations: Vec<SnapshotDependencyDeclaration>,
    /// Budget allocations and consumption records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub budgets: Vec<SnapshotBudget>,
    /// Subscription state records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub subscriptions: Vec<SnapshotSubscription>,
    /// Actor registration records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub actors: Vec<SnapshotActor>,
    /// Tenant records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub tenants: Vec<SnapshotTenant>,
    /// Role assignment records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub role_assignments: Vec<SnapshotRoleAssignment>,
    /// Capability grant records at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub capability_grants: Vec<SnapshotCapabilityGrant>,
    /// Ledger entries at snapshot time.
    #[cfg_attr(feature = "serde", serde(default))]
    pub ledger_entries: Vec<SnapshotLedgerEntry>,
}

/// Snapshot representation of engine control projection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotEngineControl {
    /// Whether scheduling and dispatch are paused.
    pub paused: bool,
    /// Timestamp of most recent pause event, if any.
    pub paused_at: Option<u64>,
    /// Timestamp of most recent resume event, if any.
    pub resumed_at: Option<u64>,
}

/// Snapshot representation of a dependency declaration.
///
/// Records that a task's promotion is gated on the successful completion
/// of all prerequisite tasks listed in `depends_on`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotDependencyDeclaration {
    /// The task whose promotion is gated.
    pub task_id: TaskId,
    /// The prerequisite task identifiers (must all complete first).
    pub depends_on: Vec<TaskId>,
    /// Timestamp when the declaration was captured (snapshot time).
    pub declared_at: u64,
}

/// Metadata about a snapshot for versioning and compatibility.
///
/// This structure provides extensibility for future snapshot versions while
/// maintaining backward compatibility with existing data.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotMetadata {
    /// The schema version of the snapshot format.
    ///
    /// This version is incremented when breaking changes are made to the snapshot
    /// format. The snapshot loader uses this to determine compatibility.
    pub schema_version: u32,
    /// The WAL sequence number at the time of the snapshot.
    ///
    /// This is the last sequence number that was applied to the state captured
    /// in this snapshot. WAL replay should start from sequence number + 1.
    pub wal_sequence: u64,
    /// The number of tasks included in this snapshot.
    pub task_count: u64,
    /// The number of runs included in this snapshot.
    pub run_count: u64,
}

/// A task in the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotTask {
    /// The task specification.
    pub task_spec: TaskSpec,
    /// The timestamp when the task was created (Unix epoch seconds).
    pub created_at: u64,
    /// The timestamp when the task was last updated (Unix epoch seconds), if any.
    pub updated_at: Option<u64>,
    /// The timestamp when the task was canceled (Unix epoch seconds), if canceled.
    #[cfg_attr(feature = "serde", serde(default))]
    pub canceled_at: Option<u64>,
}

/// A run in the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotRun {
    /// The canonical run payload.
    ///
    /// Snapshot persistence intentionally embeds the core
    /// [`RunInstance`](actionqueue_core::run::run_instance::RunInstance) to keep
    /// run-shape semantics anchored to the single contract source.
    pub run_instance: CoreRunInstance,
    /// Deterministic run state history entries.
    pub state_history: Vec<SnapshotRunStateHistoryEntry>,
    /// Deterministic attempt lineage entries.
    pub attempts: Vec<SnapshotAttemptHistoryEntry>,
    /// Deterministic lease metadata snapshot, if any.
    pub lease: Option<SnapshotLeaseMetadata>,
}

impl SnapshotRun {
    /// Returns the run identifier of the embedded canonical payload.
    pub fn run_id(&self) -> RunId {
        self.run_instance.id()
    }
}

/// Snapshot representation of a run state history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotRunStateHistoryEntry {
    /// The previous state, if any.
    pub from: Option<actionqueue_core::run::state::RunState>,
    /// The new state recorded by the WAL event.
    pub to: actionqueue_core::run::state::RunState,
    /// The timestamp associated with the transition.
    pub timestamp: u64,
}

impl From<RunStateHistoryEntry> for SnapshotRunStateHistoryEntry {
    fn from(entry: RunStateHistoryEntry) -> Self {
        Self { from: entry.from, to: entry.to, timestamp: entry.timestamp }
    }
}

/// Snapshot representation of an attempt history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotAttemptHistoryEntry {
    /// The attempt identifier.
    pub attempt_id: actionqueue_core::ids::AttemptId,
    /// The timestamp when the attempt started.
    pub started_at: u64,
    /// The timestamp when the attempt finished, if finished.
    pub finished_at: Option<u64>,
    /// Canonical attempt result taxonomy, if finished.
    pub result: Option<actionqueue_core::mutation::AttemptResultKind>,
    /// Optional error message when the attempt failed.
    pub error: Option<String>,
    /// Optional opaque output bytes produced by the handler on success.
    #[cfg_attr(feature = "serde", serde(default))]
    pub output: Option<Vec<u8>>,
}

impl From<AttemptHistoryEntry> for SnapshotAttemptHistoryEntry {
    fn from(entry: AttemptHistoryEntry) -> Self {
        Self {
            attempt_id: entry.attempt_id,
            started_at: entry.started_at,
            finished_at: entry.finished_at,
            result: entry.result,
            error: entry.error,
            output: entry.output,
        }
    }
}

/// Snapshot representation of lease metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotLeaseMetadata {
    /// Lease owner string.
    pub owner: String,
    /// Lease expiry timestamp.
    pub expiry: u64,
    /// Timestamp when the lease was acquired.
    pub acquired_at: u64,
    /// Timestamp when the lease was last updated.
    pub updated_at: u64,
}

impl From<LeaseMetadata> for SnapshotLeaseMetadata {
    fn from(metadata: LeaseMetadata) -> Self {
        Self {
            owner: metadata.owner,
            expiry: metadata.expiry,
            acquired_at: metadata.acquired_at,
            updated_at: metadata.updated_at,
        }
    }
}

/// Snapshot representation of a budget allocation and consumption record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotBudget {
    /// The task whose budget this record covers.
    pub task_id: actionqueue_core::ids::TaskId,
    /// The budget dimension.
    pub dimension: BudgetDimension,
    /// The maximum amount allowed before dispatch is blocked.
    pub limit: u64,
    /// The total amount consumed so far.
    pub consumed: u64,
    /// The timestamp when the budget was allocated.
    pub allocated_at: u64,
    /// Whether the budget has been exhausted.
    pub exhausted: bool,
}

/// Snapshot representation of a subscription state record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotSubscription {
    /// The subscription identifier.
    pub subscription_id: SubscriptionId,
    /// The subscribing task identifier.
    pub task_id: actionqueue_core::ids::TaskId,
    /// The event filter for this subscription.
    pub filter: EventFilter,
    /// The timestamp when the subscription was created.
    pub created_at: u64,
    /// The timestamp when the subscription was last triggered, if any.
    pub triggered_at: Option<u64>,
    /// The timestamp when the subscription was canceled, if any.
    pub canceled_at: Option<u64>,
}

/// Snapshot representation of a registered actor.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotActor {
    pub actor_id: ActorId,
    pub identity: String,
    pub capabilities: Vec<String>,
    pub department: Option<String>,
    pub heartbeat_interval_secs: u64,
    pub tenant_id: Option<TenantId>,
    pub registered_at: u64,
    pub last_heartbeat_at: Option<u64>,
    pub deregistered_at: Option<u64>,
}

/// Snapshot representation of a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotTenant {
    pub tenant_id: TenantId,
    pub name: String,
    pub created_at: u64,
}

/// Snapshot representation of a role assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotRoleAssignment {
    pub actor_id: ActorId,
    pub role: Role,
    pub tenant_id: TenantId,
    pub assigned_at: u64,
}

/// Snapshot representation of a capability grant.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotCapabilityGrant {
    pub actor_id: ActorId,
    pub capability: Capability,
    pub tenant_id: TenantId,
    pub granted_at: u64,
    pub revoked_at: Option<u64>,
}

/// Snapshot representation of a ledger entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SnapshotLedgerEntry {
    pub entry_id: LedgerEntryId,
    pub tenant_id: TenantId,
    pub ledger_key: String,
    pub actor_id: Option<ActorId>,
    pub payload: Vec<u8>,
    pub timestamp: u64,
}
