//! Handler-independent durable signal service. No budget/subscription registry is involved.
use actionqueue_core::{
    continuation::*,
    ids::{SignalId, SignalSequence},
    mutation::*,
    time::clock::Clock,
};
use actionqueue_storage::{
    mutation::authority::*,
    recovery::reducer::{ReplayReducer, ReplayReducerError},
    wal::writer::WalWriter,
};
/// Definitive rejection or uncertain storage result requiring recovery.
#[derive(Debug)]
pub enum SignalAdmissionError {
    /// Admission committed, but continuation reconciliation requires recovery/retry.
    Matching { outcome: AdmitSignalOutcome, source: MutationAuthorityError<ReplayReducerError> },
    /// No append took place.
    Rejected(SignalRejection),
    /// Storage uncertainty, including a fenced authority.
    Storage(MutationAuthorityError<ReplayReducerError>),
}
impl std::fmt::Display for SignalAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(e) => write!(f, "{e}"),
            Self::Storage(e) => write!(f, "{e}"),
            Self::Matching { source, .. } => {
                write!(f, "signal admitted; matching failed: {source}")
            }
        }
    }
}
impl std::error::Error for SignalAdmissionError {}
impl From<MutationAuthorityError<ReplayReducerError>> for SignalAdmissionError {
    fn from(e: MutationAuthorityError<ReplayReducerError>) -> Self {
        match e {
            MutationAuthorityError::Signal(e) => Self::Rejected(e),
            e => Self::Storage(e),
        }
    }
}
fn next<W: WalWriter>(
    authority: &StorageMutationAuthority<W, ReplayReducer>,
) -> Result<u64, SignalAdmissionError> {
    if authority.recovery_required() {
        return Err(SignalAdmissionError::Storage(MutationAuthorityError::RecoveryRequired));
    }
    authority
        .projection()
        .latest_sequence()
        .checked_add(1)
        .ok_or(SignalAdmissionError::Rejected(SignalRejection::SequenceExhausted))
}
/// Exact retries never consult the clock or replace original receipt/control fields.
pub fn admit_signal<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    request: AdmitSignalRequest,
    ingress: SignalIngressContext,
    clock: &impl Clock,
) -> Result<AdmitSignalOutcome, SignalAdmissionError> {
    let mut envelope = request.envelope(&ingress, 0);
    if let Some(outcome) = authority.lookup_signal(&envelope)? {
        return Ok(outcome);
    }
    let sequence = next(authority)?;
    envelope.received_at = clock.now();
    let result = authority.submit_command(
        MutationCommand::SignalAdmit(SignalAdmitCommand::new(sequence, envelope)),
        DurabilityPolicy::Immediate,
    )?;
    match result.applied() {
        AppliedMutation::Signal(outcome) => {
            let outcome = outcome.clone();
            crate::waits::reconcile(authority, clock.now()).map_err(|source| {
                SignalAdmissionError::Matching { outcome: outcome.clone(), source }
            })?;
            Ok(outcome)
        }
        _ => unreachable!("signal result"),
    }
}
fn pin_command<W: WalWriter>(
    authority: &StorageMutationAuthority<W, ReplayReducer>,
    signal_id: SignalId,
    pin_id: SignalPinId,
    ingress: SignalIngressContext,
    clock: &impl Clock,
) -> SignalPinCommand {
    // Saturated expected sequence still permits the authority's idempotent no-op path.
    let expected_sequence = authority.projection().latest_sequence().saturating_add(1);
    SignalPinCommand {
        expected_sequence,
        tenant_id: ingress.tenant_id,
        signal_id,
        pin_id,
        timestamp: clock.now(),
        control_context: ingress.control_context,
    }
}
fn retention_result(outcome: MutationOutcome) -> Result<usize, SignalAdmissionError> {
    match outcome.applied() {
        AppliedMutation::SignalRetention { changed } => Ok(*changed),
        _ => unreachable!("retention result"),
    }
}
/// Acquires an independent pin. Repeating the same pin is a no-op.
pub fn pin_signal<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    signal_id: SignalId,
    pin_id: SignalPinId,
    ingress: SignalIngressContext,
    clock: &impl Clock,
) -> Result<usize, SignalAdmissionError> {
    let command = pin_command(authority, signal_id, pin_id, ingress, clock);
    retention_result(
        authority
            .submit_command(MutationCommand::SignalPin(command), DurabilityPolicy::Immediate)?,
    )
}
/// Releases only the named pin. A missing pin is a no-op.
pub fn unpin_signal<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    signal_id: SignalId,
    pin_id: SignalPinId,
    ingress: SignalIngressContext,
    clock: &impl Clock,
) -> Result<usize, SignalAdmissionError> {
    let command = pin_command(authority, signal_id, pin_id, ingress, clock);
    retention_result(
        authority
            .submit_command(MutationCommand::SignalUnpin(command), DurabilityPolicy::Immediate)?,
    )
}
/// Explicit bounded retirement; proposals are rechecked against current pins and policy.
pub fn retire_signals<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    sequences: Vec<SignalSequence>,
    ingress: SignalIngressContext,
    clock: &impl Clock,
) -> Result<usize, SignalAdmissionError> {
    let c = RetireSignalsCommand {
        expected_sequence: authority.projection().latest_sequence().saturating_add(1),
        tenant_id: ingress.tenant_id,
        sequences,
        timestamp: clock.now(),
        control_context: ingress.control_context,
    };
    retention_result(
        authority.submit_command(MutationCommand::RetireSignals(c), DurabilityPolicy::Immediate)?,
    )
}
