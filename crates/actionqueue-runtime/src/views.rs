//! Transport-independent inspection schemas. No view contains executable data or locators.
use actionqueue_core::{
    admission::AdmissionDigest, bounded::*, continuation::*, ids::*, mutation::AttemptResultKind,
    run::RunState,
};
use serde::Serialize;

/// Absence and withheld values are distinct, including in human rendering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum ReferenceView {
    Absent,
    Redacted,
    Disclosed(String),
}
impl ReferenceView {
    pub fn new(value: Option<&str>, disclose: bool) -> Self {
        match value {
            None => Self::Absent,
            Some(v) if disclose => Self::Disclosed(v.into()),
            Some(_) => Self::Redacted,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Representation {
    Inline,
    External,
}
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataSummary {
    pub representation: Representation,
    pub content_hash: ContentHash,
    pub content_type: Option<String>,
    pub size_bytes: Option<u64>,
}
impl DataSummary {
    pub fn bytes(bytes: &[u8], content_type: Option<&str>) -> Self {
        use sha2::Digest;
        Self {
            representation: Representation::Inline,
            content_hash: ContentHash::new(
                HashAlgorithm::Sha256,
                sha2::Sha256::digest(bytes).to_vec(),
            )
            .expect("SHA-256"),
            content_type: content_type.map(str::to_owned),
            size_bytes: Some(bytes.len() as u64),
        }
    }
}
impl From<&actionqueue_core::data_ref::DataRef> for DataSummary {
    fn from(data: &actionqueue_core::data_ref::DataRef) -> Self {
        use actionqueue_core::data_ref::DataRef;
        match data {
            DataRef::Inline(d) => Self {
                representation: Representation::Inline,
                content_hash: d.hash().clone(),
                content_type: d.content_type().map(|v| v.as_str().into()),
                size_bytes: Some(d.bytes().len() as u64),
            },
            DataRef::External(d) => Self {
                representation: Representation::External,
                content_hash: d.hash.clone(),
                content_type: d.content_type.as_ref().map(|v| v.as_str().into()),
                size_bytes: d.size_bytes,
            },
        }
    }
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CausalView {
    pub causation: Option<CausationView>,
    pub trace_id: ReferenceView,
    pub correlation_id: ReferenceView,
    pub origin_ref: ReferenceView,
    pub submitting_principal_ref: ReferenceView,
    pub requesting_actor_ref: ReferenceView,
    pub purpose_ref: ReferenceView,
    pub authorization_ref: ReferenceView,
    pub identity_context_ref: ReferenceView,
    pub signed_statement_ref: ReferenceView,
    pub proof_context_ref: ReferenceView,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AdmissionView {
    pub control: Option<ControlView>,
    pub key: AdmissionKey,
    pub task_id: TaskId,
    pub digest: AdmissionDigest,
    pub sequence: u64,
    pub timestamp: u64,
    pub dependencies: Vec<TaskId>,
    pub causal: CausalView,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TaskView {
    pub last_control: Option<ControlView>,
    pub id: TaskId,
    pub payload: DataSummary,
    pub priority: i32,
    pub constraints: actionqueue_core::task::constraints::TaskConstraints,
    pub run_policy: actionqueue_core::task::run_policy::RunPolicy,
    pub parent_task_id: Option<TaskId>,
    pub created_at: u64,
    pub canceled_at: Option<u64>,
    pub admission: Option<AdmissionView>,
    pub budgets: Vec<BudgetView>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct BudgetView {
    pub dimension: actionqueue_core::budget::BudgetDimension,
    pub limit: u64,
    pub consumed: u64,
    pub exhausted: bool,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct RunView {
    pub resume_context_visible: bool,
    pub pending_resume_context: Option<ResumeView>,
    pub continuation_visible: bool,
    pub concurrency_key: Option<String>,
    pub block_reason: Option<&'static str>,
    #[cfg(feature = "serde")]
    pub state_history: Page<StateTransitionView>,
    #[cfg(feature = "serde")]
    pub attempts: Page<AttemptView>,
    pub run_id: RunId,
    pub task_id: TaskId,
    pub state: RunState,
    pub scheduled_at: u64,
    pub created_at: u64,
    pub attempt_count: u32,
    pub failure_attempt_count: u32,
    pub current_attempt_id: Option<AttemptId>,
    pub lease: Option<LeaseView>,
    pub gates: Vec<crate::claim::EligibilityReason>,
    pub executor_evaluated: bool,
    pub active_wait_id: Option<WaitId>,
    pub last_wait_id: Option<WaitId>,
    pub pending_resume: Option<ResumeAssignment>,
}
#[derive(Debug, Clone, Serialize)]
pub struct LeaseView {
    pub updated_at: u64,
    pub owner: ReferenceView,
    pub expiry: u64,
    pub acquired_at: u64,
    pub granted_at_sequence: u64,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AttemptView {
    pub previous_attempt_id: Option<AttemptId>,
    pub continuation_visible: bool,
    pub signal_links_visible: bool,
    pub admitted_children: Vec<TaskId>,
    pub emitted_signals: Vec<SignalSequence>,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub result: Option<AttemptResultKind>,
    pub finish_origin: AttemptFinishOrigin,
    pub accepted_start_sequence: Option<u64>,
    pub assignment: Option<ResumeAssignment>,
    pub error: ReferenceView,
    pub output: Option<DataSummary>,
    pub resume: Option<ResumeView>,
    pub checkpoints: Vec<CheckpointId>,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ResumeView {
    pub wake: WakeView,
    pub control: Option<ControlView>,
    pub checkpoint_data: Option<DataSummary>,
    pub context_id: ResumeContextId,
    pub wait_id: Option<WaitId>,
    pub checkpoint_id: Option<CheckpointId>,
    pub signal_sequence: Option<SignalSequence>,
    pub resumed_at: u64,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", rename_all = "snake_case"))]
pub enum WakeView {
    Signal { data: Option<DataSummary> },
    Children { outcomes: Vec<ChildOutcome> },
    Deadline { deadline_at: u64 },
    ControlResolution,
    AdministrativeResume,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct WaitView {
    pub wait_id: WaitId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub sequence: u64,
    pub timestamp: u64,
    pub target: WaitTargetView,
    pub deadline: Option<WaitDeadline>,
    pub checkpoint_id: Option<CheckpointId>,
    pub resolution: Option<ResolutionView>,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum WaitTargetView {
    Signal {
        namespace: SignalNamespace,
        kind: SignalKind,
        correlation_id: ReferenceView,
        source_ref: ReferenceView,
        eligible_from: SignalEligibility,
        match_policy: WaitMatchPolicy,
    },
    Children {
        task_ids: Vec<TaskId>,
        policy: ChildWaitPolicy,
    },
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ResolutionView {
    pub signal_visible: bool,
    pub control: Option<ControlView>,
    pub sequence: u64,
    pub timestamp: u64,
    pub reason: ResolutionReason,
    pub signal_sequence: Option<SignalSequence>,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionReason {
    Signal,
    Children,
    Deadline,
    Control,
    Canceled,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SignalView {
    pub control: Option<ControlView>,
    pub causation: Option<CausationView>,
    pub signal_id: SignalId,
    pub sequence: SignalSequence,
    pub wal_sequence: u64,
    pub namespace: SignalNamespace,
    pub kind: SignalKind,
    pub correlation_id: ReferenceView,
    pub source_ref: ReferenceView,
    pub data: Option<DataSummary>,
    pub payload_hash: Option<ContentHash>,
    pub received_at: u64,
    pub occurred_at: Option<u64>,
    pub retained: bool,
    pub pin_count: Option<usize>,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CheckpointView {
    pub checkpoint_id: CheckpointId,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub sequence: u64,
    pub data: DataSummary,
}
#[derive(Debug, Clone, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub revision: u64,
}
/// Structural trace notice, also used by CLI rendering.
pub const TRACE_NOTICE: &str = "Opaque references are attribution only. Queue outcomes describe execution; application-level judgments belong to the caller.";
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", content = "view", rename_all = "snake_case"))]
pub enum TraceNode {
    Task(TaskView),
    Run(RunView),
    Attempt(AttemptView),
    Wait(WaitView),
    Signal(SignalView),
    Checkpoint(CheckpointView),
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TraceView {
    pub notice: &'static str,
    pub nodes: Page<TraceNode>,
    pub edges: Page<TraceEdge>,
    pub different_fields: Vec<SchedulingField>,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingField {
    Priority,
    Constraints,
    RunPolicy,
    Budget,
    RunSchedule,
    WaitDeadline,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct CausationView {
    pub task_links_visible: bool,
    pub task_id: Option<TaskId>,
    pub run_id: Option<RunId>,
    pub attempt_id: Option<AttemptId>,
    pub external_ref: ReferenceView,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ControlView {
    pub sequence: u64,
    pub actor_id: Option<ActorId>,
    pub scope: actionqueue_core::control::ControlScope,
    pub caller_ref: ReferenceView,
    pub host_session_ref: ReferenceView,
    pub request_id: ReferenceView,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(tag = "kind", content = "id", rename_all = "snake_case"))]
pub enum NodeIdentity {
    Task(TaskId),
    Run(RunId),
    Attempt { run_id: RunId, attempt_id: AttemptId },
    Wait(WaitId),
    Signal(SignalSequence),
    Checkpoint(CheckpointId),
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    ScheduledRun,
    PhysicalAttempt,
    Parent,
    Dependency,
    EstablishedWait,
    ObservedSignal,
    ProducedCheckpoint,
    ConsumedCheckpoint,
    PreviousAttempt,
    AdmittedChild,
    EmittedSignal,
}
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct TraceEdge {
    pub from: NodeIdentity,
    pub to: NodeIdentity,
    pub kind: EdgeKind,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StateTransitionView {
    pub from: Option<RunState>,
    pub to: RunState,
    pub timestamp: u64,
}

/// History availability is explicit when only a standalone snapshot was supplied.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ControlHistoryView {
    pub available: bool,
    pub entries: Page<ControlView>,
}
