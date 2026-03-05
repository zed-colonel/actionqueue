//! Tests for run state transition validation.

use actionqueue_core::run::state::RunState;
use actionqueue_core::run::transitions::{
    is_valid_transition, valid_transitions, RunTransitionError, Transition,
};

/// Test that terminal states have no valid outward transitions
#[test]
fn terminal_states_have_no_outgoing_transitions() {
    let terminal_states = [RunState::Completed, RunState::Failed, RunState::Canceled];

    let all_states = [
        RunState::Scheduled,
        RunState::Ready,
        RunState::Leased,
        RunState::Running,
        RunState::RetryWait,
        RunState::Completed,
        RunState::Failed,
        RunState::Canceled,
    ];

    for terminal in terminal_states {
        for state in all_states.iter() {
            // Terminal states should not be able to transition to any state (including themselves)
            assert!(
                !is_valid_transition(terminal, *state),
                "Terminal state {terminal:?} should not be able to transition to {state:?}"
            );
        }
    }
}

/// Test valid forward transitions
#[test]
fn valid_transitions_are_allowed() {
    let valid_transitions = vec![
        (RunState::Scheduled, RunState::Ready),
        (RunState::Ready, RunState::Leased),
        (RunState::Ready, RunState::Canceled),
        (RunState::Leased, RunState::Running),
        (RunState::Leased, RunState::Ready),
        (RunState::Leased, RunState::Canceled),
        (RunState::Running, RunState::RetryWait),
        (RunState::Running, RunState::Completed),
        (RunState::Running, RunState::Failed),
        (RunState::Running, RunState::Canceled),
        (RunState::RetryWait, RunState::Ready),
        (RunState::RetryWait, RunState::Failed),
        (RunState::RetryWait, RunState::Canceled),
    ];

    for (from, to) in valid_transitions {
        assert!(
            is_valid_transition(from, to),
            "Transition from {from:?} to {to:?} should be valid"
        );
        assert!(
            is_valid_transition(from, to),
            "is_valid_transition should return true for {from:?} -> {to:?}"
        );
    }
}

/// Test that backward transitions are rejected
#[test]
fn backward_transitions_are_rejected() {
    let backward_transitions = vec![
        (RunState::Ready, RunState::Scheduled),
        (RunState::Running, RunState::Leased),
        (RunState::Running, RunState::Ready),
        (RunState::RetryWait, RunState::Running),
        (RunState::RetryWait, RunState::Leased),
        (RunState::Completed, RunState::Running),
        (RunState::Failed, RunState::Running),
        (RunState::Canceled, RunState::Running),
    ];

    for (from, to) in backward_transitions {
        assert!(
            !is_valid_transition(from, to),
            "Backward transition from {from:?} to {to:?} should be rejected"
        );
        assert!(
            !is_valid_transition(from, to),
            "is_valid_transition should return false for {from:?} -> {to:?}"
        );
    }
}

/// Test the Transition struct constructor
#[test]
fn transition_constructor() {
    let valid = Transition::new(RunState::Scheduled, RunState::Ready);
    assert!(valid.is_ok());
    let t = valid.expect("scheduled -> ready transition should be valid");
    assert_eq!(t.from(), RunState::Scheduled);
    assert_eq!(t.to(), RunState::Ready);

    let invalid = Transition::new(RunState::Ready, RunState::Scheduled);
    assert_eq!(
        invalid,
        Err(RunTransitionError::InvalidTransition {
            from: RunState::Ready,
            to: RunState::Scheduled,
        })
    );
}

/// Test valid_transitions function
#[test]
fn valid_transitions_function() {
    let from_states = [
        RunState::Scheduled,
        RunState::Ready,
        RunState::Leased,
        RunState::Running,
        RunState::RetryWait,
    ];

    for from in from_states {
        let transitions = valid_transitions(from);
        assert!(!transitions.is_empty(), "Expected some valid transitions from {from:?}");

        // All returned transitions should be valid
        for to in transitions.iter() {
            assert!(
                is_valid_transition(from, *to),
                "valid_transitions returned invalid transition {from:?} -> {to:?}"
            );
        }
    }
}

/// Test that terminal states return no valid transitions
#[test]
fn terminal_return_no_transitions() {
    let terminal_states = [RunState::Completed, RunState::Failed, RunState::Canceled];

    for terminal in terminal_states {
        let transitions = valid_transitions(terminal);
        assert!(
            transitions.is_empty(),
            "Terminal state {terminal:?} should have no valid transitions"
        );
    }
}

/// Test that states can transition to Ready (multiple paths)
#[test]
fn multiple_paths_to_ready() {
    // Scheduled -> Ready
    assert!(is_valid_transition(RunState::Scheduled, RunState::Ready));

    // Leased -> Ready (lease expired)
    assert!(is_valid_transition(RunState::Leased, RunState::Ready));

    // RetryWait -> Ready (backoff complete)
    assert!(is_valid_transition(RunState::RetryWait, RunState::Ready));
}

/// Test thatRunning can transition to multiple states
#[test]
fn running_has_multiple_outcomes() {
    let from = RunState::Running;
    let valid_outcomes =
        vec![RunState::RetryWait, RunState::Completed, RunState::Failed, RunState::Canceled];

    for to in valid_outcomes {
        assert!(is_valid_transition(from, to), "Running should be able to transition to {to:?}");
    }
}

/// Test that Ready can transition to multiple states
#[test]
fn ready_has_multiple_outcomes() {
    let from = RunState::Ready;
    let valid_outcomes = vec![RunState::Leased, RunState::Canceled];

    for to in valid_outcomes {
        assert!(is_valid_transition(from, to), "Ready should be able to transition to {to:?}");
    }
}

/// Test that Canceled can be reached from any non-terminal state
#[test]
fn canceled_is_universally_reachable() {
    let from_states = [
        RunState::Scheduled,
        RunState::Ready,
        RunState::Leased,
        RunState::Running,
        RunState::RetryWait,
    ];

    for from in from_states {
        assert!(
            is_valid_transition(from, RunState::Canceled),
            "State {from:?} should be able to transition to Canceled"
        );
    }
}
