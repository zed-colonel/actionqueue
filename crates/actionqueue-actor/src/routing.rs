//! Executor trait routing grants no queue RBAC or downstream resource authority.
use actionqueue_core::executor::ExecutorTraits;
use actionqueue_core::ids::ActorId;
/// Stateless exact subset matcher for task-to-worker routing.
pub struct ExecutorTraitRouter;
impl ExecutorTraitRouter {
    /// Absent requirements match any actor; otherwise every trait must be present.
    pub fn can_handle(actor_traits: &ExecutorTraits, required: Option<&ExecutorTraits>) -> bool {
        required.is_none_or(|required| actor_traits.satisfies(required))
    }
    /// Selects actors by routing traits only.
    pub fn eligible_actors(
        actors: &[(ActorId, &ExecutorTraits)],
        required: Option<&ExecutorTraits>,
    ) -> Vec<ActorId> {
        actors
            .iter()
            .filter(|(_, traits)| Self::can_handle(traits, required))
            .map(|(id, _)| *id)
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_subset_and_absent_requirements() {
        let a = ExecutorTraits::new(vec!["compute".into()]).unwrap();
        let b = ExecutorTraits::new(vec!["review".into(), "compute".into()]).unwrap();
        assert!(ExecutorTraitRouter::can_handle(&b, Some(&a)));
        assert!(!ExecutorTraitRouter::can_handle(&a, Some(&b)));
        assert!(ExecutorTraitRouter::can_handle(&a, None));
        let first = ActorId::new();
        let second = ActorId::new();
        assert_eq!(
            ExecutorTraitRouter::eligible_actors(&[(first, &a), (second, &b)], Some(&b)),
            [second]
        );
    }
}
