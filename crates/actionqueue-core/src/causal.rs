//! Fixed, bounded causal attribution. `correlation_id` and `origin_ref` are
//! opaque attribution compared by equality only: never dereferenced, never read
//! by scheduling, budget, or authority code. References grant no permission.
use crate::bounded::{BoundedCode, OpaqueRef};
use crate::ids::{AttemptId, CorrelationId, RunId, TaskId, TraceId};
/// Immutable structural attribution, with validated component values.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CausalContext {
    trace_id: TraceId,
    correlation_id: CorrelationId,
    causation: Option<CausationLink>,
    submitting_principal_ref: Option<OpaqueRef>,
    requesting_actor_ref: Option<OpaqueRef>,
    purpose_ref: Option<OpaqueRef>,
    authorization_ref: Option<OpaqueRef>,
    identity_context_ref: Option<OpaqueRef>,
    signed_statement_ref: Option<OpaqueRef>,
    proof_context_ref: Option<OpaqueRef>,
    origin_ref: Option<OpaqueRef>,
}
impl CausalContext {
    /// Constructs attribution with required bounded values.
    pub fn new(trace_id: TraceId, correlation_id: CorrelationId) -> Self {
        Self {
            trace_id,
            correlation_id,
            causation: None,
            submitting_principal_ref: None,
            requesting_actor_ref: None,
            purpose_ref: None,
            authorization_ref: None,
            identity_context_ref: None,
            signed_statement_ref: None,
            proof_context_ref: None,
            origin_ref: None,
        }
    }
    /// Returns the recorded trace id.
    pub fn trace_id(&self) -> &TraceId {
        &self.trace_id
    }
    /// Overrides correlation for an explicitly forked child attribution.
    pub fn with_correlation_id(mut self, value: CorrelationId) -> Self {
        self.correlation_id = value;
        self
    }
    /// Returns the recorded correlation id.
    pub fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }
    /// Returns the recorded causation.
    pub fn causation(&self) -> Option<&CausationLink> {
        self.causation.as_ref()
    }
    /// Sets the bounded causation.
    pub fn with_causation(mut self, value: CausationLink) -> Self {
        self.causation = Some(value);
        self
    }
    /// Returns the recorded submitting principal ref.
    pub fn submitting_principal_ref(&self) -> Option<&OpaqueRef> {
        self.submitting_principal_ref.as_ref()
    }
    /// Sets the bounded submitting principal ref.
    pub fn with_submitting_principal_ref(mut self, value: OpaqueRef) -> Self {
        self.submitting_principal_ref = Some(value);
        self
    }
    /// Returns the recorded requesting actor ref.
    pub fn requesting_actor_ref(&self) -> Option<&OpaqueRef> {
        self.requesting_actor_ref.as_ref()
    }
    /// Sets the bounded requesting actor ref.
    pub fn with_requesting_actor_ref(mut self, value: OpaqueRef) -> Self {
        self.requesting_actor_ref = Some(value);
        self
    }
    /// Returns the recorded purpose ref.
    pub fn purpose_ref(&self) -> Option<&OpaqueRef> {
        self.purpose_ref.as_ref()
    }
    /// Sets the bounded purpose ref.
    pub fn with_purpose_ref(mut self, value: OpaqueRef) -> Self {
        self.purpose_ref = Some(value);
        self
    }
    /// Returns the recorded authorization ref.
    pub fn authorization_ref(&self) -> Option<&OpaqueRef> {
        self.authorization_ref.as_ref()
    }
    /// Sets the bounded authorization ref.
    pub fn with_authorization_ref(mut self, value: OpaqueRef) -> Self {
        self.authorization_ref = Some(value);
        self
    }
    /// Returns the recorded identity context ref.
    pub fn identity_context_ref(&self) -> Option<&OpaqueRef> {
        self.identity_context_ref.as_ref()
    }
    /// Sets the bounded identity context ref.
    pub fn with_identity_context_ref(mut self, value: OpaqueRef) -> Self {
        self.identity_context_ref = Some(value);
        self
    }
    /// Returns the recorded signed statement ref.
    pub fn signed_statement_ref(&self) -> Option<&OpaqueRef> {
        self.signed_statement_ref.as_ref()
    }
    /// Sets the bounded signed statement ref.
    pub fn with_signed_statement_ref(mut self, value: OpaqueRef) -> Self {
        self.signed_statement_ref = Some(value);
        self
    }
    /// Returns the recorded proof context ref.
    pub fn proof_context_ref(&self) -> Option<&OpaqueRef> {
        self.proof_context_ref.as_ref()
    }
    /// Sets the bounded proof context ref.
    pub fn with_proof_context_ref(mut self, value: OpaqueRef) -> Self {
        self.proof_context_ref = Some(value);
        self
    }
    /// Returns the recorded origin ref.
    pub fn origin_ref(&self) -> Option<&OpaqueRef> {
        self.origin_ref.as_ref()
    }
    /// Sets the bounded origin ref.
    pub fn with_origin_ref(mut self, value: OpaqueRef) -> Self {
        self.origin_ref = Some(value);
        self
    }
}
/// Immutable structural attribution, with validated component values.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ControlMutationContext {
    caller_ref: OpaqueRef,
    host_session_ref: Option<OpaqueRef>,
    request_id: Option<OpaqueRef>,
    reason_code: Option<BoundedCode>,
}
impl ControlMutationContext {
    /// Constructs attribution with required bounded values.
    pub fn new(caller_ref: OpaqueRef) -> Self {
        Self { caller_ref, host_session_ref: None, request_id: None, reason_code: None }
    }
    /// Returns the recorded caller ref.
    pub fn caller_ref(&self) -> &OpaqueRef {
        &self.caller_ref
    }
    /// Returns the recorded host session ref.
    pub fn host_session_ref(&self) -> Option<&OpaqueRef> {
        self.host_session_ref.as_ref()
    }
    /// Sets the bounded host session ref.
    pub fn with_host_session_ref(mut self, value: OpaqueRef) -> Self {
        self.host_session_ref = Some(value);
        self
    }
    /// Returns the recorded request id.
    pub fn request_id(&self) -> Option<&OpaqueRef> {
        self.request_id.as_ref()
    }
    /// Sets the bounded request id.
    pub fn with_request_id(mut self, value: OpaqueRef) -> Self {
        self.request_id = Some(value);
        self
    }
    /// Returns the recorded reason code.
    pub fn reason_code(&self) -> Option<&BoundedCode> {
        self.reason_code.as_ref()
    }
    /// Sets the bounded reason code.
    pub fn with_reason_code(mut self, value: BoundedCode) -> Self {
        self.reason_code = Some(value);
        self
    }
}
/// Bounded child attribution overrides (ADR-014); absence inherits the parent.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default)]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
pub struct CausalOverride {
    /// Optional explicit correlation fork.
    pub correlation_id: Option<CorrelationId>,
    /// Optional requester override.
    pub requesting_actor_ref: Option<OpaqueRef>,
    /// Optional origin override.
    pub origin_ref: Option<OpaqueRef>,
}
/// Structurally invalid causal ancestry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CausationLinkError {
    /// No parent or external source.
    Empty,
    /// Attempt requires run; run requires task.
    InconsistentHierarchy,
}
impl std::fmt::Display for CausationLinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid causation: {self:?}")
    }
}
impl std::error::Error for CausationLinkError {}
/// A validated structural parent or external origin.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "CausationWire"))]
pub struct CausationLink {
    parent_task_id: Option<TaskId>,
    parent_run_id: Option<RunId>,
    parent_attempt_id: Option<AttemptId>,
    external_ref: Option<OpaqueRef>,
}
impl CausationLink {
    /// Rejects empty ancestry and inconsistent task/run/attempt hierarchy.
    pub fn new(
        parent_task_id: Option<TaskId>,
        parent_run_id: Option<RunId>,
        parent_attempt_id: Option<AttemptId>,
        external_ref: Option<OpaqueRef>,
    ) -> Result<Self, CausationLinkError> {
        if parent_task_id.is_none()
            && parent_run_id.is_none()
            && parent_attempt_id.is_none()
            && external_ref.is_none()
        {
            return Err(CausationLinkError::Empty);
        }
        if (parent_attempt_id.is_some() && parent_run_id.is_none())
            || (parent_run_id.is_some() && parent_task_id.is_none())
        {
            return Err(CausationLinkError::InconsistentHierarchy);
        }
        Ok(Self { parent_task_id, parent_run_id, parent_attempt_id, external_ref })
    }
    /// Parent task identity.
    pub fn parent_task_id(&self) -> Option<TaskId> {
        self.parent_task_id
    }
    /// Parent run identity.
    pub fn parent_run_id(&self) -> Option<RunId> {
        self.parent_run_id
    }
    /// Parent attempt identity.
    pub fn parent_attempt_id(&self) -> Option<AttemptId> {
        self.parent_attempt_id
    }
    /// External origin, if supplied.
    pub fn external_ref(&self) -> Option<&OpaqueRef> {
        self.external_ref.as_ref()
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct CausationWire {
    parent_task_id: Option<TaskId>,
    parent_run_id: Option<RunId>,
    parent_attempt_id: Option<AttemptId>,
    external_ref: Option<OpaqueRef>,
}
#[cfg(feature = "serde")]
impl TryFrom<CausationWire> for CausationLink {
    type Error = CausationLinkError;
    fn try_from(w: CausationWire) -> Result<Self, Self::Error> {
        Self::new(w.parent_task_id, w.parent_run_id, w.parent_attempt_id, w.external_ref)
    }
}
