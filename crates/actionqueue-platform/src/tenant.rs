//! In-memory tenant registry.

use std::collections::HashMap;

use actionqueue_core::ids::TenantId;
use actionqueue_core::platform::TenantRegistration;
use tracing;

/// In-memory projection of organizational tenants.
///
/// Reconstructed from WAL events at bootstrap via [`TenantRegistry::register`].
#[derive(Default)]
pub struct TenantRegistry {
    tenants: HashMap<TenantId, TenantRegistration>,
}

impl TenantRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a tenant.
    pub fn register(&mut self, registration: TenantRegistration) {
        let tenant_id = registration.tenant_id();
        tracing::debug!(%tenant_id, "tenant registered");
        self.tenants.insert(tenant_id, registration);
    }

    /// Returns `true` if the tenant exists in the registry.
    pub fn exists(&self, tenant_id: TenantId) -> bool {
        self.tenants.contains_key(&tenant_id)
    }

    /// Returns the tenant registration, if known.
    pub fn get(&self, tenant_id: TenantId) -> Option<&TenantRegistration> {
        self.tenants.get(&tenant_id)
    }

    /// Returns an iterator over all registered tenants.
    pub fn all(&self) -> impl Iterator<Item = &TenantRegistration> {
        self.tenants.values()
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TenantId;
    use actionqueue_core::platform::TenantRegistration;

    use super::TenantRegistry;

    #[test]
    fn register_and_exists() {
        let mut reg = TenantRegistry::new();
        let id = TenantId::new();
        reg.register(TenantRegistration::new(id, "Acme Corp"));
        assert!(reg.exists(id));
        assert_eq!(reg.get(id).map(|r| r.name()), Some("Acme Corp"));
    }

    #[test]
    fn unknown_tenant_does_not_exist() {
        let reg = TenantRegistry::new();
        assert!(!reg.exists(TenantId::new()));
    }
}
