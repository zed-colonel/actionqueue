//! Capability-based task routing.

use actionqueue_core::ids::ActorId;

/// Stateless capability intersection matcher for task → actor routing.
///
/// The dispatch loop uses this to filter which actors are eligible to
/// claim a task based on its `required_capabilities`.
pub struct CapabilityRouter;

impl CapabilityRouter {
    /// Returns `true` if `actor_capabilities` contains ALL entries in `required`.
    ///
    /// An empty `required` slice matches any actor (no capability requirements).
    pub fn can_handle(actor_capabilities: &[String], required: &[String]) -> bool {
        required.iter().all(|r| actor_capabilities.iter().any(|c| c == r))
    }

    /// Filters a list of `(actor_id, capabilities)` pairs to those eligible
    /// to handle a task with `required_capabilities`.
    pub fn eligible_actors(actors: &[(ActorId, &[String])], required: &[String]) -> Vec<ActorId> {
        actors
            .iter()
            .filter(|(_, caps)| Self::can_handle(caps, required))
            .map(|(id, _)| *id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::ActorId;

    use super::CapabilityRouter;

    fn caps(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn can_handle_all_required_present() {
        let actor = caps(&["compute", "review", "approve"]);
        assert!(CapabilityRouter::can_handle(&actor, &caps(&["compute", "review"])));
    }

    #[test]
    fn can_handle_missing_requirement() {
        let actor = caps(&["compute"]);
        assert!(!CapabilityRouter::can_handle(&actor, &caps(&["compute", "review"])));
    }

    #[test]
    fn can_handle_empty_required_always_matches() {
        let actor = caps(&[]);
        assert!(CapabilityRouter::can_handle(&actor, &[]));
    }

    #[test]
    fn eligible_actors_filters_correctly() {
        let a = ActorId::new();
        let b = ActorId::new();
        let c = ActorId::new();
        let a_caps = caps(&["compute", "review"]);
        let b_caps = caps(&["compute"]);
        let c_caps = caps(&["review"]);

        let actors = vec![(a, a_caps.as_slice()), (b, b_caps.as_slice()), (c, c_caps.as_slice())];
        let required = caps(&["compute", "review"]);
        let eligible = CapabilityRouter::eligible_actors(&actors, &required);

        assert_eq!(eligible.len(), 1);
        assert!(eligible.contains(&a));
    }
}
