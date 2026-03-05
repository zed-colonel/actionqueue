//! Role-based access control enforcement.

use std::collections::{HashMap, HashSet};

use actionqueue_core::ids::{ActorId, TenantId};
use actionqueue_core::platform::{Capability, Role};
use tracing;

/// Error returned when RBAC enforcement rejects an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RbacError {
    /// Actor has no role assigned in the given tenant.
    NoRoleAssigned { actor_id: ActorId, tenant_id: TenantId },
    /// Actor lacks the required capability in the given tenant.
    MissingCapability { actor_id: ActorId, capability: Capability, tenant_id: TenantId },
}

impl std::fmt::Display for RbacError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RbacError::NoRoleAssigned { actor_id, tenant_id } => {
                write!(f, "actor {actor_id} has no role in tenant {tenant_id}")
            }
            RbacError::MissingCapability { actor_id, capability, tenant_id } => {
                write!(f, "actor {actor_id} lacks capability {capability:?} in tenant {tenant_id}")
            }
        }
    }
}

impl std::error::Error for RbacError {}

/// In-memory RBAC enforcer.
///
/// Reconstructed from WAL events at bootstrap via role assignment and
/// capability grant/revoke calls.
#[derive(Default)]
pub struct RbacEnforcer {
    /// (actor_id, tenant_id) → Role
    role_assignments: HashMap<(ActorId, TenantId), Role>,
    /// (actor_id, tenant_id) → Set of granted capabilities
    capability_grants: HashMap<(ActorId, TenantId), HashSet<String>>,
}

impl RbacEnforcer {
    /// Creates an empty enforcer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assigns a role to an actor within a tenant.
    pub fn assign_role(&mut self, actor_id: ActorId, role: Role, tenant_id: TenantId) {
        tracing::debug!(%actor_id, ?role, %tenant_id, "role assigned");
        self.role_assignments.insert((actor_id, tenant_id), role);
    }

    /// Grants a capability to an actor within a tenant.
    pub fn grant_capability(
        &mut self,
        actor_id: ActorId,
        capability: Capability,
        tenant_id: TenantId,
    ) {
        tracing::debug!(%actor_id, ?capability, %tenant_id, "capability granted");
        self.capability_grants
            .entry((actor_id, tenant_id))
            .or_default()
            .insert(capability_key(&capability));
    }

    /// Revokes a capability from an actor within a tenant.
    pub fn revoke_capability(
        &mut self,
        actor_id: ActorId,
        capability: &Capability,
        tenant_id: TenantId,
    ) {
        tracing::debug!(%actor_id, ?capability, %tenant_id, "capability revoked");
        if let Some(caps) = self.capability_grants.get_mut(&(actor_id, tenant_id)) {
            caps.remove(&capability_key(capability));
        }
    }

    /// Returns `true` if the actor has the given capability in the tenant.
    pub fn has_capability(
        &self,
        actor_id: ActorId,
        capability: &Capability,
        tenant_id: TenantId,
    ) -> bool {
        self.capability_grants
            .get(&(actor_id, tenant_id))
            .is_some_and(|caps| caps.contains(&capability_key(capability)))
    }

    /// Returns the role assigned to an actor in a tenant, if any.
    pub fn role_of(&self, actor_id: ActorId, tenant_id: TenantId) -> Option<&Role> {
        self.role_assignments.get(&(actor_id, tenant_id))
    }

    /// Returns `Ok(())` if the actor has the capability, `Err(RbacError)` otherwise.
    pub fn check_permission(
        &self,
        actor_id: ActorId,
        capability: &Capability,
        tenant_id: TenantId,
    ) -> Result<(), RbacError> {
        if self.role_of(actor_id, tenant_id).is_none() {
            let err = RbacError::NoRoleAssigned { actor_id, tenant_id };
            tracing::warn!(%actor_id, %tenant_id, "permission denied: no role assigned");
            return Err(err);
        }
        if !self.has_capability(actor_id, capability, tenant_id) {
            tracing::warn!(
                %actor_id, ?capability, %tenant_id,
                "permission denied: missing capability"
            );
            return Err(RbacError::MissingCapability {
                actor_id,
                capability: capability.clone(),
                tenant_id,
            });
        }
        Ok(())
    }
}

fn capability_key(cap: &Capability) -> String {
    match cap {
        Capability::CanSubmit => "CanSubmit".to_string(),
        Capability::CanExecute => "CanExecute".to_string(),
        Capability::CanReview => "CanReview".to_string(),
        Capability::CanApprove => "CanApprove".to_string(),
        Capability::CanCancel => "CanCancel".to_string(),
        Capability::Custom(s) => format!("Custom:{s}"),
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{ActorId, TenantId};
    use actionqueue_core::platform::{Capability, Role};

    use super::RbacEnforcer;

    #[test]
    fn assign_role_and_grant_capability() {
        let mut enforcer = RbacEnforcer::new();
        let actor = ActorId::new();
        let tenant = TenantId::new();

        enforcer.assign_role(actor, Role::Operator, tenant);
        enforcer.grant_capability(actor, Capability::CanSubmit, tenant);

        assert_eq!(enforcer.role_of(actor, tenant), Some(&Role::Operator));
        assert!(enforcer.has_capability(actor, &Capability::CanSubmit, tenant));
        assert!(enforcer.check_permission(actor, &Capability::CanSubmit, tenant).is_ok());
    }

    #[test]
    fn check_permission_rejects_missing_role() {
        let enforcer = RbacEnforcer::new();
        let actor = ActorId::new();
        let tenant = TenantId::new();
        let result = enforcer.check_permission(actor, &Capability::CanSubmit, tenant);
        assert!(matches!(result, Err(super::RbacError::NoRoleAssigned { .. })));
    }

    #[test]
    fn check_permission_rejects_missing_capability() {
        let mut enforcer = RbacEnforcer::new();
        let actor = ActorId::new();
        let tenant = TenantId::new();
        enforcer.assign_role(actor, Role::Operator, tenant);
        // Role assigned but no capabilities granted.
        let result = enforcer.check_permission(actor, &Capability::CanExecute, tenant);
        assert!(matches!(result, Err(super::RbacError::MissingCapability { .. })));
    }

    #[test]
    fn revoke_removes_capability() {
        let mut enforcer = RbacEnforcer::new();
        let actor = ActorId::new();
        let tenant = TenantId::new();
        enforcer.assign_role(actor, Role::Operator, tenant);
        enforcer.grant_capability(actor, Capability::CanSubmit, tenant);
        enforcer.revoke_capability(actor, &Capability::CanSubmit, tenant);
        assert!(!enforcer.has_capability(actor, &Capability::CanSubmit, tenant));
    }
}
