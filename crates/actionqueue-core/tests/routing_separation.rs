use actionqueue_core::bounded::OpaqueRef;
use actionqueue_core::causal::{CausalContext, ControlMutationContext};
use actionqueue_core::executor::{ExecutorTrait, ExecutorTraits};
use actionqueue_core::platform::{Capability, Role};
use actionqueue_core::task::constraints::{ConcurrencyKeyWaitPolicy, TaskConstraints};

// If a From implementation appears, the inferred marker becomes ambiguous and
// this test file fails to compile. No external compile-test dependency is needed.
macro_rules! assert_no_conversion {
    ($target:ty, $source:ty) => {
        const _: fn() = || {
            trait AmbiguousIfImpl<Marker> {
                fn check() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<()> for T {}
            struct Conversion;
            impl<T: ?Sized + From<$source>> AmbiguousIfImpl<Conversion> for T {}
            let _ = <$target as AmbiguousIfImpl<_>>::check;
        };
    };
}
assert_no_conversion!(ExecutorTrait, Capability);
assert_no_conversion!(ExecutorTrait, Role);
assert_no_conversion!(ExecutorTraits, Capability);
assert_no_conversion!(ExecutorTraits, Role);
assert_no_conversion!(OpaqueRef, Capability);
assert_no_conversion!(OpaqueRef, Role);
assert_no_conversion!(CausalContext, Capability);
assert_no_conversion!(CausalContext, Role);
assert_no_conversion!(ControlMutationContext, Capability);
assert_no_conversion!(ControlMutationContext, Role);

#[test]
fn constraint_setter_is_atomic_and_wait_policy_defaults_to_release() {
    let mut constraints =
        TaskConstraints::default().with_required_executor_traits(vec!["compute".into()]).unwrap();
    let before = constraints.clone();
    assert!(constraints.set_required_executor_traits(Some(vec![" ".into()])).is_err());
    assert_eq!(constraints, before);
    constraints.set_required_executor_traits(None).unwrap();
    assert!(constraints.required_executor_traits().is_none());
    assert_eq!(ConcurrencyKeyWaitPolicy::default(), ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting);
}
#[cfg(feature = "serde")]
#[test]
fn policies_and_constraints_round_trip() {
    for policy in [
        ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting,
        ConcurrencyKeyWaitPolicy::HoldWhileAwaiting,
    ] {
        assert_eq!(
            postcard::from_bytes::<ConcurrencyKeyWaitPolicy>(
                &postcard::to_allocvec(&policy).unwrap()
            )
            .unwrap(),
            policy
        );
    }
    let constraints =
        TaskConstraints::default().with_required_executor_traits(vec!["compute".into()]).unwrap();
    assert_eq!(
        postcard::from_bytes::<TaskConstraints>(&postcard::to_allocvec(&constraints).unwrap())
            .unwrap(),
        constraints
    );
}
