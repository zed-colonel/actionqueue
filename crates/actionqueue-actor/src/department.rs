//! Actor-to-department grouping with reverse index.

use std::collections::{HashMap, HashSet};

use actionqueue_core::ids::{ActorId, DepartmentId};

/// Groups actors by department for department-targeted task routing.
///
/// The dispatch loop uses this to route tasks that target a specific
/// department (e.g., `"engineering"`) to only the actors in that group.
/// First-come-first-serve is enforced via lease atomicity at the WAL level.
#[derive(Default)]
pub struct DepartmentRegistry {
    departments: HashMap<DepartmentId, HashSet<ActorId>>,
    /// Reverse index: actor_id → their department.
    actor_departments: HashMap<ActorId, DepartmentId>,
}

impl DepartmentRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assigns an actor to a department. If the actor was in another
    /// department, they are removed from the previous one.
    pub fn assign(&mut self, actor_id: ActorId, dept: DepartmentId) {
        // Remove from previous department.
        if let Some(prev_dept) = self.actor_departments.remove(&actor_id) {
            if let Some(members) = self.departments.get_mut(&prev_dept) {
                members.remove(&actor_id);
            }
        }
        self.departments.entry(dept.clone()).or_default().insert(actor_id);
        self.actor_departments.insert(actor_id, dept);
    }

    /// Removes an actor from their department.
    pub fn remove(&mut self, actor_id: ActorId) {
        if let Some(dept) = self.actor_departments.remove(&actor_id) {
            if let Some(members) = self.departments.get_mut(&dept) {
                members.remove(&actor_id);
            }
        }
    }

    /// Returns the set of actor IDs in the given department.
    pub fn actors_in_department(&self, dept: &DepartmentId) -> &HashSet<ActorId> {
        self.departments.get(dept).map_or(&EMPTY_SET, |s| s)
    }

    /// Returns the department of the given actor, if assigned.
    pub fn department_of(&self, actor_id: ActorId) -> Option<&DepartmentId> {
        self.actor_departments.get(&actor_id)
    }
}

static EMPTY_SET: std::sync::LazyLock<HashSet<ActorId>> = std::sync::LazyLock::new(HashSet::new);

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{ActorId, DepartmentId};

    use super::DepartmentRegistry;

    fn dept(name: &str) -> DepartmentId {
        DepartmentId::new(name).unwrap()
    }

    #[test]
    fn assign_and_lookup() {
        let mut reg = DepartmentRegistry::new();
        let actor = ActorId::new();
        let engineering = dept("engineering");
        reg.assign(actor, engineering.clone());
        assert!(reg.actors_in_department(&engineering).contains(&actor));
        assert_eq!(reg.department_of(actor), Some(&engineering));
    }

    #[test]
    fn reassign_moves_actor() {
        let mut reg = DepartmentRegistry::new();
        let actor = ActorId::new();
        let eng = dept("engineering");
        let ops = dept("ops");
        reg.assign(actor, eng.clone());
        reg.assign(actor, ops.clone());
        assert!(!reg.actors_in_department(&eng).contains(&actor));
        assert!(reg.actors_in_department(&ops).contains(&actor));
        assert_eq!(reg.department_of(actor), Some(&ops));
    }

    #[test]
    fn remove_clears_actor() {
        let mut reg = DepartmentRegistry::new();
        let actor = ActorId::new();
        let eng = dept("engineering");
        reg.assign(actor, eng.clone());
        reg.remove(actor);
        assert!(!reg.actors_in_department(&eng).contains(&actor));
        assert!(reg.department_of(actor).is_none());
    }

    #[test]
    fn empty_department_returns_empty_set() {
        let reg = DepartmentRegistry::new();
        let eng = dept("engineering");
        assert!(reg.actors_in_department(&eng).is_empty());
    }
}
