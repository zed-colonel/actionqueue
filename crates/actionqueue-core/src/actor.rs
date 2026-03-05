//! Actor domain types for remote actor registration and heartbeat coordination.
//!
//! Remote actors are Caelum Vessels (or other clients) that register with the
//! Org ActionQueue hub, claim tasks by capability, and report execution results.
//! This module defines the pure domain types; storage, routing, and heartbeat
//! monitoring logic lives in `actionqueue-actor`.

use crate::ids::{ActorId, DepartmentId, TenantId};

/// Declared capabilities of a remote actor.
///
/// Capabilities are free-form strings (e.g. `"compute"`, `"review"`,
/// `"approve"`). The dispatch loop uses capability intersection to decide
/// which actors are eligible to claim a given task.
///
/// # Invariants
///
/// - The capability list must be non-empty.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorCapabilities {
    capabilities: Vec<String>,
}

impl ActorCapabilities {
    /// Creates a validated `ActorCapabilities` from a list of capability strings.
    ///
    /// # Errors
    ///
    /// Returns an error string if the list is empty or any entry is empty.
    pub fn new(capabilities: Vec<String>) -> Result<Self, String> {
        if capabilities.is_empty() {
            return Err("actor must declare at least one capability".to_string());
        }
        for cap in &capabilities {
            if cap.is_empty() {
                return Err("capability string must be non-empty".to_string());
            }
        }
        Ok(ActorCapabilities { capabilities })
    }

    /// Returns the capability strings.
    pub fn as_slice(&self) -> &[String] {
        &self.capabilities
    }

    /// Returns `true` if this set contains all capabilities in `required`.
    pub fn satisfies(&self, required: &[String]) -> bool {
        required.iter().all(|r| self.capabilities.iter().any(|c| c == r))
    }
}

/// Actor registration record.
///
/// Represents a remote actor's registration with the Org ActionQueue hub.
/// All fields are private with validated constructors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorRegistration {
    actor_id: ActorId,
    /// Human-readable identity string used as the WAL lease owner.
    identity: String,
    capabilities: ActorCapabilities,
    department: Option<DepartmentId>,
    heartbeat_interval_secs: u64,
    tenant_id: Option<TenantId>,
}

impl ActorRegistration {
    /// Creates a new actor registration with required fields.
    ///
    /// Optional fields (`department`, `tenant_id`) can be set via builder methods.
    ///
    /// # Panics
    ///
    /// Panics if `identity` is empty or `heartbeat_interval_secs` is 0.
    pub fn new(
        actor_id: ActorId,
        identity: impl Into<String>,
        capabilities: ActorCapabilities,
        heartbeat_interval_secs: u64,
    ) -> Self {
        let identity = identity.into();
        assert!(!identity.is_empty(), "actor identity must be non-empty");
        assert!(heartbeat_interval_secs > 0, "heartbeat_interval_secs must be > 0");
        ActorRegistration {
            actor_id,
            identity,
            capabilities,
            department: None,
            heartbeat_interval_secs,
            tenant_id: None,
        }
    }

    /// Attaches a department identifier, returning the modified registration.
    pub fn with_department(mut self, department: DepartmentId) -> Self {
        self.department = Some(department);
        self
    }

    /// Attaches a tenant identifier, returning the modified registration.
    pub fn with_tenant(mut self, tenant_id: TenantId) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// Returns the actor identifier.
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    /// Returns the actor identity string (used as WAL lease owner).
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Returns the actor's declared capabilities.
    pub fn capabilities(&self) -> &ActorCapabilities {
        &self.capabilities
    }

    /// Returns the actor's department, if any.
    pub fn department(&self) -> Option<&DepartmentId> {
        self.department.as_ref()
    }

    /// Returns the expected heartbeat interval in seconds.
    pub fn heartbeat_interval_secs(&self) -> u64 {
        self.heartbeat_interval_secs
    }

    /// Returns the actor's tenant, if any.
    pub fn tenant_id(&self) -> Option<TenantId> {
        self.tenant_id
    }
}

/// Heartbeat timeout policy for a remote actor.
///
/// Timeout = `interval_secs × timeout_multiplier`. The hub declares an actor
/// dead when it has not received a heartbeat for this duration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HeartbeatPolicy {
    interval_secs: u64,
    /// Timeout multiplier (timeout = interval × multiplier). Default: 3.
    timeout_multiplier: u32,
}

impl HeartbeatPolicy {
    /// Default timeout multiplier (3×interval).
    pub const DEFAULT_MULTIPLIER: u32 = 3;

    /// Creates a new heartbeat policy.
    ///
    /// # Panics
    ///
    /// Panics if `interval_secs == 0` or `timeout_multiplier == 0`.
    pub fn new(interval_secs: u64, timeout_multiplier: u32) -> Self {
        assert!(interval_secs > 0, "heartbeat interval_secs must be > 0");
        assert!(timeout_multiplier > 0, "timeout_multiplier must be > 0");
        HeartbeatPolicy { interval_secs, timeout_multiplier }
    }

    /// Creates a heartbeat policy with the default 3× multiplier.
    pub fn with_default_multiplier(interval_secs: u64) -> Self {
        Self::new(interval_secs, Self::DEFAULT_MULTIPLIER)
    }

    /// Returns the heartbeat interval in seconds.
    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    /// Returns the timeout multiplier.
    pub fn timeout_multiplier(&self) -> u32 {
        self.timeout_multiplier
    }

    /// Returns the effective timeout duration: `interval_secs × timeout_multiplier`.
    pub fn timeout_secs(&self) -> u64 {
        self.interval_secs.saturating_mul(self.timeout_multiplier as u64)
    }
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self::new(30, Self::DEFAULT_MULTIPLIER)
    }
}
