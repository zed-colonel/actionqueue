//! Run module - run state machine and instance management.

pub mod run_instance;
pub mod state;
pub mod transitions;

pub use run_instance::{RunInstance, RunInstanceConstructionError, RunInstanceError};
pub use state::RunState;
pub use transitions::{is_valid_transition, valid_transitions, RunTransitionError, Transition};
