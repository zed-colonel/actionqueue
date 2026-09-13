//! Runtime planning followed by one authoritative compound commit.
use actionqueue_core::{admission::AdmissionPlan, disposition::AttemptDisposition, mutation::*};
use actionqueue_storage::{
    mutation::{
        disposition::DispositionRejection, MutationAuthorityError, StorageMutationAuthority,
    },
    recovery::reducer::{ReplayReducer, ReplayReducerError},
    wal::writer::WalWriter,
};
type Error = MutationAuthorityError<ReplayReducerError>;
/// A rejected handler proposal closes as a minimal terminal failure. Uncertain writes
/// and stale ownership never enter the fallback path.
pub fn commit<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    expected: AttemptCommitExpectation,
    disposition: AttemptDisposition,
    now: u64,
) -> Result<(), Error> {
    commit_proposal(a, expected, disposition, now, None, true)
}
/// Remote proposals never become terminal handler failures on rejection.
pub fn commit_remote<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    expected: AttemptCommitExpectation,
    disposition: AttemptDisposition,
    now: u64,
    remote: RemoteResultExpectation,
) -> Result<(), Error> {
    commit_proposal(a, expected, disposition, now, Some(remote), false)
}
// Preserve the explicit dependencies of this existing boundary API.
#[allow(clippy::too_many_arguments)]
fn commit_proposal<W: WalWriter>(
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
    expected: AttemptCommitExpectation,
    disposition: AttemptDisposition,
    now: u64,
    remote: Option<RemoteResultExpectation>,
    fallback: bool,
) -> Result<(), Error> {
    if a.recovery_required() {
        return Err(Error::RecoveryRequired);
    }
    let mut command = AttemptDispositionCommitCommand::new(expected.clone(), disposition, now);
    if let Some(remote) = remote {
        command = command.with_remote(remote);
        if actionqueue_storage::mutation::control::validate_remote(a, &command)
            .map_err(|_| Error::Disposition(DispositionRejection::Stale))?
            .is_some()
        {
            return Ok(());
        }
    }
    a.projection().validate_disposition_fence(&command).map_err(Error::Disposition)?;
    let plans = command
        .disposition()
        .child_admissions()
        .iter()
        .map(|child| {
            let request = a
                .projection()
                .disposition_child_request(command.run_id(), command.attempt_id(), child)
                .map_err(Error::Disposition)?;
            let digest =
                request.digest().map_err(|_| Error::Disposition(DispositionRejection::Invalid))?;
            if a.projection()
                .admission(request.task_spec().tenant_id(), request.admission_key())
                .is_some()
            {
                AdmissionPlan::new(request, vec![], digest)
                    .map_err(|_| Error::Disposition(DispositionRejection::Invalid))
            } else {
                actionqueue_engine::admission::plan_admission(request, digest, now)
                    .map_err(|_| Error::Disposition(DispositionRejection::Invalid))
            }
        })
        .collect::<Result<Vec<_>, Error>>();
    let result = match plans {
        Ok(plans) => {
            command = command.with_children(plans);
            a.submit_command(
                MutationCommand::AttemptDispositionCommit(command),
                DurabilityPolicy::Immediate,
            )
            .map(|_| ())
        }
        Err(e) => Err(e),
    };
    match result {
        Err(Error::Disposition(
            reason @ (DispositionRejection::Invalid
            | DispositionRejection::TooLarge
            | DispositionRejection::UnsupportedFeature
            | DispositionRejection::ChildrenNonterminal
            | DispositionRejection::InvalidChildWait),
        )) if fallback => {
            let failure = AttemptDisposition::terminal_failure(
                crate::config::disposition_rejection_error(reason),
            );
            let _ = a.submit_command(
                MutationCommand::AttemptDispositionCommit(AttemptDispositionCommitCommand::new(
                    expected, failure, now,
                )),
                DurabilityPolicy::Immediate,
            )?;
            Ok(())
        }
        other => other,
    }
}
