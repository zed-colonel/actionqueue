#![allow(dead_code)]
use actionqueue_core::{
    admission::{AdmissionPlan, EnsureTaskRequest},
    causal::CausalContext,
    ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
    mutation::AdmissionCommitCommand,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::{bootstrap::recover_read_only, reducer::ReplayReducer},
    store::{capabilities, open_store, OpenOptions},
    wal::{fs_writer::WalFsWriter, repair::RepairPolicy},
};
pub type Authority = StorageMutationAuthority<WalFsWriter, ReplayReducer>;
pub fn id(n: u64) -> TaskId {
    format!("00000000-0000-0000-0000-{n:012x}").parse().unwrap()
}
pub fn spec(n: u64) -> TaskSpec {
    TaskSpec::new(
        id(n),
        TaskPayload::new(vec![0, 1, 255]),
        RunPolicy::repeat(3, 7).unwrap(),
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap()
}
pub fn request(n: u64) -> EnsureTaskRequest {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/aq-cont-1/developmental-admission.json"
    ))
    .unwrap();
    let text = |key: &str| fixture[key].as_str().unwrap();
    let opaque = |key: &str| actionqueue_core::bounded::OpaqueRef::new(text(key)).unwrap();
    EnsureTaskRequest::new(
        AdmissionKey::new(format!("{}/{n}", text("key_prefix"))).unwrap(),
        spec(n),
        vec![],
        CausalContext::new(
            TraceId::new(text("trace_id")).unwrap(),
            CorrelationId::new(text("correlation_id")).unwrap(),
        )
        .with_origin_ref(opaque("origin_ref"))
        .with_purpose_ref(opaque("purpose_ref"))
        .with_authorization_ref(opaque("authorization_ref")),
        None,
    )
    .unwrap()
}
pub fn open(path: &std::path::Path) -> Authority {
    let session = open_store(path, OpenOptions::Initialize { features: capabilities() }).unwrap();
    let projection = recover_read_only(&session, RepairPolicy::Strict).unwrap().projection;
    Authority::new(WalFsWriter::new(session).unwrap(), projection)
}
pub fn command(q: EnsureTaskRequest, sequence: u64, timestamp: u64) -> AdmissionCommitCommand {
    let control = q.control_context().cloned();
    let plan =
        actionqueue_engine::admission::plan_admission(q.clone(), q.digest().unwrap(), timestamp)
            .unwrap();
    AdmissionCommitCommand::new(sequence, plan, control, timestamp)
}
pub fn with_spec(q: &EnsureTaskRequest, s: TaskSpec) -> EnsureTaskRequest {
    EnsureTaskRequest::new(
        q.admission_key().clone(),
        s,
        q.dependencies().to_vec(),
        q.causal_context().clone(),
        q.control_context().cloned(),
    )
    .unwrap()
}
pub fn with_causal(q: &EnsureTaskRequest, c: CausalContext) -> EnsureTaskRequest {
    EnsureTaskRequest::new(
        q.admission_key().clone(),
        q.task_spec().clone(),
        q.dependencies().to_vec(),
        c,
        q.control_context().cloned(),
    )
    .unwrap()
}
pub fn with_dependencies(q: &EnsureTaskRequest, dependencies: Vec<TaskId>) -> EnsureTaskRequest {
    EnsureTaskRequest::new(
        q.admission_key().clone(),
        q.task_spec().clone(),
        dependencies,
        q.causal_context().clone(),
        q.control_context().cloned(),
    )
    .unwrap()
}
pub fn plan_with_runs(
    q: EnsureTaskRequest,
    runs: Vec<actionqueue_core::run::RunInstance>,
) -> AdmissionPlan {
    let digest = q.digest().unwrap();
    AdmissionPlan::new(q, runs, digest).unwrap()
}
pub fn ensure(
    a: &mut Authority,
    q: EnsureTaskRequest,
    at: u64,
) -> Result<
    actionqueue_core::admission::EnsureTaskOutcome,
    actionqueue_runtime::admission::AdmissionError,
> {
    actionqueue_runtime::admission::ensure_task(
        a,
        q,
        &actionqueue_core::time::clock::MockClock::new(at),
    )
}
pub fn image(a: &Authority) -> actionqueue_storage::snapshot::model::Snapshot {
    let mut image =
        actionqueue_storage::snapshot::build::build_snapshot_from_projection(a.projection(), 0)
            .unwrap();
    image.tasks.sort_by_key(|t| *t.task_spec.id().as_uuid());
    image.runs.sort_by_key(|r| r.run_instance.id());
    image.admissions.sort_by_key(|r| *r.task_id().as_uuid());
    image.dependency_declarations.sort_by_key(|d| *d.task_id.as_uuid());
    image
}
