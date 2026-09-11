#![allow(dead_code)]
use actionqueue_core::{continuation::*, ids::*, mutation::*, time::clock::MockClock};
use actionqueue_runtime::signals::{admit_signal, SignalAdmissionError};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::{bootstrap::recover_read_only, reducer::ReplayReducer},
    store::{capabilities, open_store, OpenOptions},
    wal::{fs_writer::WalFsWriter, repair::RepairPolicy},
};
pub type Authority = StorageMutationAuthority<WalFsWriter, ReplayReducer>;
pub fn open(path: &std::path::Path) -> Authority {
    let session = open_store(path, OpenOptions::Initialize { features: capabilities() }).unwrap();
    session.into_authority().unwrap()
}
pub fn reopen(path: &std::path::Path) -> Authority {
    let session = open_store(path, OpenOptions::ReadWrite).unwrap();
    let projection = recover_read_only(&session, RepairPolicy::TruncatePartial).unwrap().projection;
    Authority::new(
        WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).unwrap(),
        projection,
    )
}
pub fn id(n: u64) -> SignalId {
    SignalId::new(format!("signal/{n}")).unwrap()
}
pub fn request(n: u64) -> AdmitSignalRequest {
    AdmitSignalRequest::new(
        id(n),
        SignalNamespace::new("remote").unwrap(),
        SignalKind::new("complete").unwrap(),
        Some(CorrelationId::new("job/1").unwrap()),
        None,
        None,
        None,
        None,
        Some(3),
    )
    .unwrap()
}
pub fn request_from(e: &SignalEnvelope) -> Result<AdmitSignalRequest, SignalRejection> {
    AdmitSignalRequest::new(
        e.signal_id.clone(),
        e.namespace.clone(),
        e.kind.clone(),
        e.correlation_id.clone(),
        e.causation.clone(),
        e.source_ref.clone(),
        e.payload.clone(),
        e.payload_hash.clone(),
        e.occurred_at,
    )
}
pub fn admit(
    a: &mut Authority,
    n: u64,
    at: u64,
) -> Result<AdmitSignalOutcome, SignalAdmissionError> {
    admit_signal(a, request(n), Default::default(), &MockClock::new(at))
}
pub fn submit(
    a: &mut Authority,
    e: SignalEnvelope,
) -> Result<
    MutationOutcome,
    actionqueue_storage::mutation::MutationAuthorityError<
        actionqueue_storage::recovery::reducer::ReplayReducerError,
    >,
> {
    let seq = a.projection().latest_sequence().saturating_add(1);
    a.submit_command(
        MutationCommand::SignalAdmit(SignalAdmitCommand::new(seq, e)),
        DurabilityPolicy::Immediate,
    )
}
pub fn envelope(n: u64, at: u64) -> SignalEnvelope {
    request(n).envelope(&Default::default(), at)
}
pub fn filter() -> SignalFilter {
    SignalFilter {
        tenant_id: None,
        namespace: SignalNamespace::new("remote").unwrap(),
        kind: SignalKind::new("complete").unwrap(),
        correlation_id: None,
        source_ref: None,
    }
}
pub fn sequences(a: &Authority, f: &SignalFilter, after: u64) -> Vec<u64> {
    a.projection()
        .signals()
        .retained_candidates(f, SignalSequence::new(after), 1000)
        .iter()
        .map(|r| r.sequence().get())
        .collect()
}
