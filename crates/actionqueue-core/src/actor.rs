//! Actor domain types for remote actor registration and heartbeat coordination.
//!
//! Remote workers register executor traits, claim eligible tasks, and report results.
//! This module defines the pure domain types; storage, routing, and heartbeat
//! monitoring logic lives in `actionqueue-actor`.

use crate::ids::{ActorId, DepartmentId, TenantId};

pub use crate::executor::ExecutorTraits;

/// Actor registration record.
///
/// Represents a remote actor's registration with the ActionQueue service.
/// All fields are private with validated constructors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorRegistration {
    actor_id: ActorId,
    /// Human-readable identity string used as the WAL lease owner.
    identity: String,
    executor_traits: ExecutorTraits,
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
        executor_traits: ExecutorTraits,
        heartbeat_interval_secs: u64,
    ) -> Self {
        let identity = identity.into();
        assert!(!identity.is_empty(), "actor identity must be non-empty");
        assert!(heartbeat_interval_secs > 0, "heartbeat_interval_secs must be > 0");
        ActorRegistration {
            actor_id,
            identity,
            executor_traits,
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

    /// Returns the actor's declared executor traits.
    pub fn executor_traits(&self) -> &ExecutorTraits {
        &self.executor_traits
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
