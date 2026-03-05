//! WAL event types for the ActionQueue system.
//!
//! This module defines the append-only event types that make up the Write-Ahead Log (WAL).
//! Each event represents a durable state change in the system and is idempotent across replay.

use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{ActorId, AttemptId, LedgerEntryId, RunId, TaskId, TenantId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::platform::{Capability, Role};
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};
use actionqueue_core::task::task_spec::TaskSpec;

/// A uniquely identifiable WAL event.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WalEvent {
    /// Monotonically increasing sequence number for the event.
    sequence: u64,
    /// The type and payload of this event.
    event: WalEventType,
}

impl WalEvent {
    /// Creates a new WAL event.
    pub fn new(sequence: u64, event: WalEventType) -> Self {
        Self { sequence, event }
    }

    /// Returns the monotonically increasing sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns a reference to the event type and payload.
    pub fn event(&self) -> &WalEventType {
        &self.event
    }
}

/// The type of WAL event that occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum WalEventType {
    /// A new task definition has been persisted.
    TaskCreated {
        /// The task specification that was created.
        task_spec: TaskSpec,
        /// The timestamp when the task was created.
        ///
        /// Unix epoch seconds; source-of-truth for task created_at.
        timestamp: u64,
    },

    /// A new run instance has been created for a task.
    RunCreated {
        /// The run instance that was created.
        run_instance: actionqueue_core::run::run_instance::RunInstance,
    },

    /// A run instance transitioned to a new state.
    RunStateChanged {
        /// The run that changed state.
        run_id: RunId,
        /// The previous state (before the transition).
        previous_state: RunState,
        /// The new state (after the transition).
        new_state: RunState,
        /// The timestamp of the state change.
        timestamp: u64,
    },

    /// An attempt to execute a run has started.
    AttemptStarted {
        /// The run that the attempt belongs to.
        run_id: RunId,
        /// The attempt that started.
        attempt_id: AttemptId,
        /// The timestamp when the attempt started.
        timestamp: u64,
    },

    /// An attempt to execute a run has finished.
    AttemptFinished {
        /// The run that the attempt belongs to.
        run_id: RunId,
        /// The attempt that finished.
        attempt_id: AttemptId,
        /// Canonical attempt result taxonomy.
        result: AttemptResultKind,
        /// Optional error message if the attempt failed.
        error: Option<String>,
        /// Optional opaque output bytes produced by the handler.
        ///
        /// Populated from `HandlerOutput::Success { output }` via the executor
        /// response chain. Stored in the WAL for recovery and projection queries.
        // NOTE: #[serde(default)] is inert for postcard (non-self-describing format).
        // Retained for documentation symmetry with the snapshot model.
        #[cfg_attr(feature = "serde", serde(default))]
        output: Option<Vec<u8>>,
        /// The timestamp when the attempt finished.
        timestamp: u64,
    },

    /// A task has been canceled.
    TaskCanceled {
        /// The task that was canceled.
        task_id: TaskId,
        /// The timestamp when the task was canceled.
        timestamp: u64,
    },

    /// A run has been canceled.
    RunCanceled {
        /// The run that was canceled.
        run_id: RunId,
        /// The timestamp when the run was canceled.
        timestamp: u64,
    },

    /// A lease was acquired for a run.
    LeaseAcquired {
        /// The run that the lease belongs to.
        run_id: RunId,
        /// The worker that acquired the lease.
        owner: String,
        /// The expiry timestamp of the lease.
        expiry: u64,
        /// The timestamp when the lease was acquired.
        timestamp: u64,
    },

    /// A lease heartbeat (renewal) was recorded.
    LeaseHeartbeat {
        /// The run that the lease belongs to.
        run_id: RunId,
        /// The worker that sent the heartbeat.
        owner: String,
        /// The new expiry timestamp after heartbeat.
        expiry: u64,
        /// The timestamp when the heartbeat was recorded.
        timestamp: u64,
    },

    /// A lease expired (either by time or manual release).
    LeaseExpired {
        /// The run that the lease belonged to.
        run_id: RunId,
        /// The worker that held the lease.
        owner: String,
        /// The expiry timestamp of the lease.
        expiry: u64,
        /// The timestamp when the lease expired.
        timestamp: u64,
    },

    /// A lease was released before expiry.
    LeaseReleased {
        /// The run that the lease belonged to.
        run_id: RunId,
        /// The worker that released the lease.
        owner: String,
        /// The expiry timestamp of the lease at release.
        expiry: u64,
        /// The timestamp when the lease was released.
        timestamp: u64,
    },

    /// Engine scheduling and dispatch has been paused.
    EnginePaused {
        /// The timestamp when engine pause was recorded.
        timestamp: u64,
    },

    /// Engine scheduling and dispatch has been resumed.
    EngineResumed {
        /// The timestamp when engine resume was recorded.
        timestamp: u64,
    },

    /// Task dependency declarations have been durably recorded.
    ///
    /// The named task will not be promoted to Ready until all tasks in
    /// `depends_on` have reached terminal success.
    ///
    /// Dependency satisfaction and failure are **not** WAL events — they are
    /// ephemeral projections derived at bootstrap from `DependencyDeclared`
    /// events + run terminal states. This is consistent with the architectural
    /// principle that in-memory indices are ephemeral projections, not
    /// independent durable state.
    DependencyDeclared {
        /// The task whose promotion is gated.
        task_id: TaskId,
        /// The prerequisite task identifiers (must all complete first).
        depends_on: Vec<TaskId>,
        /// The timestamp when the dependency was declared.
        timestamp: u64,
    },

    /// A run has been suspended (e.g. by budget exhaustion).
    RunSuspended {
        /// The run that was suspended.
        run_id: RunId,
        /// Optional human-readable suspension reason.
        reason: Option<String>,
        /// The timestamp when the run was suspended.
        timestamp: u64,
    },

    /// A suspended run has been resumed.
    RunResumed {
        /// The run that was resumed.
        run_id: RunId,
        /// The timestamp when the run was resumed.
        timestamp: u64,
    },

    /// A budget has been allocated for a task dimension.
    BudgetAllocated {
        /// The task receiving the budget allocation.
        task_id: TaskId,
        /// The budget dimension being allocated.
        dimension: BudgetDimension,
        /// The maximum amount allowed before dispatch is blocked.
        limit: u64,
        /// The timestamp when the budget was allocated.
        timestamp: u64,
    },

    /// Resource consumption has been recorded against a task budget.
    BudgetConsumed {
        /// The task whose budget was consumed.
        task_id: TaskId,
        /// The budget dimension being consumed.
        dimension: BudgetDimension,
        /// The amount consumed in this event.
        amount: u64,
        /// The timestamp when consumption was recorded.
        timestamp: u64,
    },

    /// Reserved for WAL v4 enum ordering. Exhaustion is a derived projection
    /// from `BudgetConsumed` events (consumed >= limit). This variant is never
    /// appended to the WAL on disk but may be synthesized during snapshot
    /// bootstrap for reducer replay. Must be retained to preserve postcard
    /// deserialization order.
    BudgetExhausted {
        /// The task whose budget was exhausted.
        task_id: TaskId,
        /// The exhausted budget dimension.
        dimension: BudgetDimension,
        /// The timestamp when exhaustion was recorded.
        timestamp: u64,
    },

    /// A task budget has been replenished with a new limit.
    BudgetReplenished {
        /// The task whose budget was replenished.
        task_id: TaskId,
        /// The budget dimension being replenished.
        dimension: BudgetDimension,
        /// The new limit after replenishment.
        new_limit: u64,
        /// The timestamp when replenishment was recorded.
        timestamp: u64,
    },

    /// An event subscription has been created for a task.
    SubscriptionCreated {
        /// The subscription identifier.
        subscription_id: SubscriptionId,
        /// The subscribing task.
        task_id: TaskId,
        /// The event filter that governs when this subscription fires.
        filter: EventFilter,
        /// The timestamp when the subscription was created.
        timestamp: u64,
    },

    /// A subscription has been triggered by a matching event.
    SubscriptionTriggered {
        /// The subscription that was triggered.
        subscription_id: SubscriptionId,
        /// The timestamp when the subscription was triggered.
        timestamp: u64,
    },

    /// A subscription has been canceled.
    SubscriptionCanceled {
        /// The subscription that was canceled.
        subscription_id: SubscriptionId,
        /// The timestamp when the subscription was canceled.
        timestamp: u64,
    },

    // ── WAL v5: Actor events (discriminants 23-25) ─────────────────────────
    /// A remote actor has registered with the hub.
    ActorRegistered {
        actor_id: ActorId,
        identity: String,
        capabilities: Vec<String>,
        department: Option<String>,
        heartbeat_interval_secs: u64,
        tenant_id: Option<TenantId>,
        timestamp: u64,
    },

    /// A remote actor has deregistered (explicit or heartbeat timeout).
    ActorDeregistered { actor_id: ActorId, timestamp: u64 },

    /// A remote actor sent a heartbeat.
    ActorHeartbeat { actor_id: ActorId, timestamp: u64 },

    // ── WAL v5: Platform events (discriminants 26-31) ──────────────────────
    /// An organizational tenant was created.
    TenantCreated { tenant_id: TenantId, name: String, timestamp: u64 },

    /// A role was assigned to an actor within a tenant.
    RoleAssigned { actor_id: ActorId, role: Role, tenant_id: TenantId, timestamp: u64 },

    /// A capability was granted to an actor within a tenant.
    CapabilityGranted {
        actor_id: ActorId,
        capability: Capability,
        tenant_id: TenantId,
        timestamp: u64,
    },

    /// A capability grant was revoked.
    CapabilityRevoked {
        actor_id: ActorId,
        capability: Capability,
        tenant_id: TenantId,
        timestamp: u64,
    },

    /// A ledger entry was appended.
    LedgerEntryAppended {
        entry_id: LedgerEntryId,
        tenant_id: TenantId,
        ledger_key: String,
        actor_id: Option<ActorId>,
        payload: Vec<u8>,
        timestamp: u64,
    },
}
