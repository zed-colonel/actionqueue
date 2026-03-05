//! In-memory registry of registered remote actors.

use std::collections::{HashMap, HashSet};

use actionqueue_core::actor::ActorRegistration;
use actionqueue_core::ids::{ActorId, TenantId};
use tracing;

/// Entry in the actor registry.
struct ActorEntry {
    registration: ActorRegistration,
    active: bool,
}

/// In-memory projection of registered remote actors.
///
/// Reconstructed from WAL events at bootstrap via [`ActorRegistry::register`]
/// and [`ActorRegistry::deregister`] calls. Maintains a secondary index of
/// actors per tenant for O(N_tenant) lookups.
#[derive(Default)]
pub struct ActorRegistry {
    actors: HashMap<ActorId, ActorEntry>,
    /// Secondary index: tenant_id → set of actor_ids in that tenant.
    actors_by_tenant: HashMap<TenantId, HashSet<ActorId>>,
}

impl ActorRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an actor. If an actor with this ID already exists, it is replaced.
    pub fn register(&mut self, registration: ActorRegistration) {
        let actor_id = registration.actor_id();
        tracing::debug!(%actor_id, "actor registered");
        if let Some(tenant_id) = registration.tenant_id() {
            self.actors_by_tenant.entry(tenant_id).or_default().insert(actor_id);
        }
        self.actors.insert(actor_id, ActorEntry { registration, active: true });
    }

    /// Marks an actor as deregistered (inactive).
    pub fn deregister(&mut self, actor_id: ActorId) {
        tracing::debug!(%actor_id, "actor deregistered");
        if let Some(entry) = self.actors.get_mut(&actor_id) {
            entry.active = false;
        }
    }

    /// Returns `true` if the actor is registered and active.
    pub fn is_active(&self, actor_id: ActorId) -> bool {
        self.actors.get(&actor_id).is_some_and(|e| e.active)
    }

    /// Returns the actor registration, if known.
    pub fn get(&self, actor_id: ActorId) -> Option<&ActorRegistration> {
        self.actors.get(&actor_id).map(|e| &e.registration)
    }

    /// Returns the identity string for the actor (used as WAL lease owner).
    pub fn identity(&self, actor_id: ActorId) -> Option<&str> {
        self.actors.get(&actor_id).map(|e| e.registration.identity())
    }

    /// Returns all active actor IDs for the given tenant.
    pub fn active_actors_for_tenant(&self, tenant_id: TenantId) -> Vec<ActorId> {
        let Some(ids) = self.actors_by_tenant.get(&tenant_id) else {
            return Vec::new();
        };
        ids.iter().copied().filter(|&id| self.is_active(id)).collect()
    }

    /// Returns an iterator over all actor IDs (active and deregistered).
    pub fn all_actor_ids(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.actors.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
    use actionqueue_core::ids::{ActorId, TenantId};

    use super::ActorRegistry;

    fn make_registration(actor_id: ActorId) -> ActorRegistration {
        let caps = ActorCapabilities::new(vec!["compute".to_string()]).unwrap();
        ActorRegistration::new(actor_id, "test-actor", caps, 30)
    }

    #[test]
    fn register_and_is_active() {
        let mut registry = ActorRegistry::new();
        let id = ActorId::new();
        registry.register(make_registration(id));
        assert!(registry.is_active(id));
        assert!(registry.get(id).is_some());
    }

    #[test]
    fn deregister_marks_inactive() {
        let mut registry = ActorRegistry::new();
        let id = ActorId::new();
        registry.register(make_registration(id));
        registry.deregister(id);
        assert!(!registry.is_active(id));
        // Entry still exists (for history), just inactive.
        assert!(registry.get(id).is_some());
    }

    #[test]
    fn active_actors_for_tenant_filters_correctly() {
        let mut registry = ActorRegistry::new();
        let tenant = TenantId::new();
        let active_id = ActorId::new();
        let deregistered_id = ActorId::new();
        let other_tenant_id = ActorId::new();

        let caps = ActorCapabilities::new(vec!["c".to_string()]).unwrap();
        registry
            .register(ActorRegistration::new(active_id, "a", caps.clone(), 30).with_tenant(tenant));
        registry.register(
            ActorRegistration::new(deregistered_id, "b", caps.clone(), 30).with_tenant(tenant),
        );
        registry.register(ActorRegistration::new(other_tenant_id, "c", caps, 30));

        registry.deregister(deregistered_id);

        let active = registry.active_actors_for_tenant(tenant);
        assert_eq!(active.len(), 1);
        assert!(active.contains(&active_id));
    }

    #[test]
    fn identity_returns_lease_owner_string() {
        let mut registry = ActorRegistry::new();
        let id = ActorId::new();
        let caps = ActorCapabilities::new(vec!["c".to_string()]).unwrap();
        registry.register(ActorRegistration::new(id, "caelum-vessel-1", caps, 30));
        assert_eq!(registry.identity(id), Some("caelum-vessel-1"));
    }
}
