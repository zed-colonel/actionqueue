use actionqueue_core::ids::{AttemptId, TaskId};
use actionqueue_core::run::{
    is_valid_transition, transition_rejection, RunInstance, RunInstanceError, RunState,
    RunTransitionRejection,
};
use proptest::prelude::*;
use RunState::*;

const STATES: [RunState; 10] = [
    Scheduled, Ready, Leased, Running, RetryWait, Suspended, Completed, Failed, Canceled, Awaiting,
];
const EDGES: [(RunState, RunState); 21] = [
    (Scheduled, Ready),
    (Scheduled, Canceled),
    (Ready, Leased),
    (Ready, Canceled),
    (Leased, Running),
    (Leased, Ready),
    (Leased, Canceled),
    (Running, RetryWait),
    (Running, Suspended),
    (Running, Completed),
    (Running, Failed),
    (Running, Canceled),
    (RetryWait, Ready),
    (RetryWait, Failed),
    (RetryWait, Canceled),
    (Suspended, Ready),
    (Suspended, Canceled),
    (Running, Awaiting),
    (Awaiting, Ready),
    (Awaiting, Failed),
    (Awaiting, Canceled),
];
#[test]
fn all_hundred_pairs_have_precise_classification() {
    use RunTransitionRejection::*;
    for from in STATES {
        for to in STATES {
            let expected = if EDGES.contains(&(from, to)) {
                None
            } else if from.is_terminal() {
                Some(TerminalStateIsFinal)
            } else if to == Awaiting && from != Running {
                Some(AwaitingRequiresRunning)
            } else if from == Awaiting {
                Some(AwaitingResolvesViaReadyOnly)
            } else {
                Some(NotInTransitionTable)
            };
            assert_eq!(transition_rejection(from, to), expected, "{from} -> {to}");
            assert_eq!(is_valid_transition(from, to), expected.is_none());
        }
    }
    assert!(!Awaiting.is_terminal());
    assert_eq!(Awaiting.to_string(), "awaiting");
}
#[test]
fn awaiting_requires_finished_attempt() {
    let mut run = RunInstance::new_scheduled(TaskId::new(), 0, 0).unwrap();
    for state in [Ready, Leased, Running] {
        run.transition_to(state).unwrap();
    }
    let id = AttemptId::new();
    run.start_attempt(id).unwrap();
    assert!(matches!(run.transition_to(Awaiting), Err(RunInstanceError::AttemptInProgress { .. })));
    run.finish_attempt(id).unwrap();
    run.transition_to(Awaiting).unwrap();
    run.transition_to(Ready).unwrap();
}
proptest! {
    #[test]
    fn awaiting_only_originates_from_running(from in 0..10usize) {
        prop_assert_eq!(is_valid_transition(STATES[from], Awaiting), STATES[from] == Running);
    }
    #[test]
    fn random_sequences_never_revive_terminal_runs(sequence in proptest::collection::vec(0..10usize, 0..300)) {
        let mut run = RunInstance::new_scheduled(TaskId::new(), 0, 0).unwrap();
        for index in sequence {
            let previous = run.state();
            let _ = run.transition_to(STATES[index]);
            if previous.is_terminal() { prop_assert_eq!(run.state(), previous); }
        }
    }
}
#[cfg(feature = "serde")]
#[test]
fn postcard_discriminants_are_frozen() {
    use actionqueue_core::mutation::{AttemptOutcome, AttemptResultKind};
    for (index, state) in STATES.into_iter().enumerate() {
        assert_eq!(postcard::to_allocvec(&state).unwrap(), [index as u8]);
        assert_eq!(postcard::from_bytes::<RunState>(&[index as u8]).unwrap(), state);
    }
    let results = [
        AttemptResultKind::Success,
        AttemptResultKind::Failure,
        AttemptResultKind::Timeout,
        AttemptResultKind::Suspended,
        AttemptResultKind::Awaiting,
    ];
    for (index, result) in results.into_iter().enumerate() {
        assert_eq!(postcard::to_allocvec(&result).unwrap(), [index as u8]);
        assert_eq!(postcard::from_bytes::<AttemptResultKind>(&[index as u8]).unwrap(), result);
    }
    assert!(AttemptOutcome::from_raw_parts(AttemptResultKind::Awaiting, None, None).is_ok());
    assert!(AttemptOutcome::from_raw_parts(
        AttemptResultKind::Awaiting,
        Some("error".into()),
        None
    )
    .is_err());
}
