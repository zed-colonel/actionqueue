//! Pure continuation proposal ordering. Storage performs final validation.
use actionqueue_core::{
    ids::{RunId, SignalSequence, WaitId},
    mutation::*,
};
/// Propose one indexed signal match.
pub fn satisfy(
    sequence: u64,
    run_id: RunId,
    wait_id: WaitId,
    signal: SignalSequence,
    now: u64,
) -> MutationCommand {
    MutationCommand::WaitSatisfy(WaitSatisfyCommand::new(sequence, run_id, wait_id, signal, now))
}
/// Propose a deadline only after all currently retained matches have been considered.
pub fn timeout(sequence: u64, run_id: RunId, wait_id: WaitId, now: u64) -> MutationCommand {
    MutationCommand::WaitTimeout(WaitTimeoutCommand::new(sequence, run_id, wait_id, now))
}
