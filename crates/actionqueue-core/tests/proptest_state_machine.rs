//! Property-based tests for state machine transition invariants.

use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::is_valid_transition;
use proptest::prelude::*;

fn arb_run_state() -> impl Strategy<Value = RunState> {
    prop_oneof![
        Just(RunState::Scheduled),
        Just(RunState::Ready),
        Just(RunState::Leased),
        Just(RunState::Running),
        Just(RunState::RetryWait),
        Just(RunState::Completed),
        Just(RunState::Failed),
        Just(RunState::Canceled),
    ]
}

fn config() -> ProptestConfig {
    ProptestConfig::with_cases(256)
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn terminal_states_have_no_valid_outgoing_transitions(
        to in arb_run_state(),
    ) {
        let terminal_states = [RunState::Completed, RunState::Failed, RunState::Canceled];
        for from in &terminal_states {
            prop_assert!(
                !is_valid_transition(*from, to),
                "terminal state {:?} should have no valid transition to {:?}",
                from,
                to,
            );
        }
    }

    #[test]
    fn valid_transition_implies_from_differs_from_to(
        from in arb_run_state(),
        to in arb_run_state(),
    ) {
        if is_valid_transition(from, to) {
            prop_assert_ne!(
                from, to,
                "valid transition must go to a different state, but got {:?} -> {:?}",
                from, to,
            );
        }
    }

    #[test]
    fn no_backward_transitions_from_terminal_states(
        from in prop_oneof![
            Just(RunState::Completed),
            Just(RunState::Failed),
            Just(RunState::Canceled),
        ],
        to in arb_run_state(),
    ) {
        prop_assert!(
            !is_valid_transition(from, to),
            "no transition should succeed from terminal state {:?} to {:?}",
            from,
            to,
        );
    }
}
