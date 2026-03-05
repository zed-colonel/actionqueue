//! Platform domain types for multi-tenant isolation, RBAC, and ledger entries.
//!
//! These types are consumed by `actionqueue-platform` (for enforcement) and
//! `actionqueue-storage` (for WAL events and snapshot persistence). They live in
//! `actionqueue-core` so all crates share a single canonical definition.

use crate::ids::{ActorId, LedgerEntryId, TenantId};

/// Organizational role for an actor within a tenant.
///
/// Roles map to capability sets via the RBAC enforcer. The `Custom` variant
/// allows org-specific extension beyond the standard triad.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Role {
    /// Operator: submits work plans, manages tasks.
    Operator,
    /// Auditor: reviews plans, produces approval/rejection decisions.
    Auditor,
    /// Gatekeeper: executes privileged actions after approval.
    Gatekeeper,
    /// Extension role. The inner string must be non-empty.
    Custom(String),
}

impl Role {
    /// Creates a validated custom role.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is empty.
    pub fn custom(name: impl Into<String>) -> Result<Self, String> {
        let name = name.into();
        if name.is_empty() {
            return Err("custom role name must be non-empty".to_string());
        }
        Ok(Role::Custom(name))
    }
}

/// Typed permission for an actor within a tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Capability {
    /// Actor may submit new tasks.
    CanSubmit,
    /// Actor may execute (claim and complete) tasks.
    CanExecute,
    /// Actor may review tasks and produce approval/rejection outcomes.
    CanReview,
    /// Actor may approve proposed actions.
    CanApprove,
    /// Actor may cancel tasks and runs.
    CanCancel,
    /// Extension capability. The inner string must be non-empty.
    Custom(String),
}

impl Capability {
    /// Creates a validated custom capability.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is empty.
    pub fn custom(name: impl Into<String>) -> Result<Self, String> {
        let name = name.into();
        if name.is_empty() {
            return Err("custom capability name must be non-empty".to_string());
        }
        Ok(Capability::Custom(name))
    }
}

/// Tenant registration record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TenantRegistration {
    tenant_id: TenantId,
    name: String,
}

impl TenantRegistration {
    /// Creates a new tenant registration.
    ///
    /// # Panics
    ///
    /// Panics if `name` is empty.
    pub fn new(tenant_id: TenantId, name: impl Into<String>) -> Self {
        let name = name.into();
        assert!(!name.is_empty(), "tenant name must be non-empty");
        TenantRegistration { tenant_id, name }
    }

    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Returns the tenant name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Generic ledger entry for append-only platform ledgers.
///
/// Ledger keys identify the logical ledger (e.g. `"audit"`, `"decision"`,
/// `"relationship"`, `"incident"`, `"reality"`). The payload is opaque bytes
/// whose schema is defined by the consumer (Caelum, Digicorp).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LedgerEntry {
    entry_id: LedgerEntryId,
    tenant_id: TenantId,
    /// The logical ledger name (e.g. `"audit"`, `"decision"`).
    ledger_key: String,
    /// The actor that produced this entry, if any.
    actor_id: Option<ActorId>,
    /// Opaque payload bytes. Schema is consumer-defined.
    payload: Vec<u8>,
    /// Unix epoch seconds when this entry was recorded.
    timestamp: u64,
}

impl LedgerEntry {
    /// Creates a new ledger entry.
    ///
    /// `actor_id` can be set via [`with_actor`](Self::with_actor) after construction.
    ///
    /// # Panics
    ///
    /// Panics if `ledger_key` is empty.
    pub fn new(
        entry_id: LedgerEntryId,
        tenant_id: TenantId,
        ledger_key: impl Into<String>,
        payload: Vec<u8>,
        timestamp: u64,
    ) -> Self {
        let ledger_key = ledger_key.into();
        assert!(!ledger_key.is_empty(), "ledger_key must be non-empty");
        LedgerEntry { entry_id, tenant_id, ledger_key, actor_id: None, payload, timestamp }
    }

    /// Attaches an actor identifier, returning the modified entry.
    pub fn with_actor(mut self, actor_id: ActorId) -> Self {
        self.actor_id = Some(actor_id);
        self
    }

    /// Returns the entry identifier.
    pub fn entry_id(&self) -> LedgerEntryId {
        self.entry_id
    }

    /// Returns the tenant identifier.
    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Returns the logical ledger key.
    pub fn ledger_key(&self) -> &str {
        &self.ledger_key
    }

    /// Returns the actor identifier, if any.
    pub fn actor_id(&self) -> Option<ActorId> {
        self.actor_id
    }

    /// Returns the opaque payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the entry timestamp (Unix epoch seconds).
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}
