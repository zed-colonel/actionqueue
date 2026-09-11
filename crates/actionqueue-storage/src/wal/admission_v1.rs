//! Frozen kind 256/schema 1 payloads. No public admission plan is serialized.
use actionqueue_core::{
    admission::{AdmissionDigest, EnsureTaskRequest},
    bounded::{BoundedCode, ContentHash, HashAlgorithm, OpaqueRef},
    causal::{CausalContext, CausationLink, ControlMutationContext},
    ids::{AdmissionKey, AttemptId, CorrelationId, RunId, TaskId, TraceId},
    run::RunInstance,
};
use serde::{Deserialize, Serialize};

use super::{codec::DecodeError, domain_v1::RunV1, task_v1::TaskSpecV1};
use crate::mutation::admission::AdmissionRecord;
fn invalid(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Decode(e.to_string())
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdmissionCommittedV1 {
    record: AdmissionRecordV1,
    runs: Vec<RunV1>,
}
impl AdmissionCommittedV1 {
    pub(super) fn new(record: &AdmissionRecord, runs: &[RunInstance]) -> Self {
        Self { record: record.clone().into(), runs: runs.iter().map(RunV1::from).collect() }
    }
    pub(super) fn into_event(self) -> Result<super::event::WalEventType, DecodeError> {
        Ok(super::event::WalEventType::AdmissionCommitted {
            record: self.record.try_into()?,
            runs: self.runs.into_iter().map(TryInto::try_into).collect::<Result<_, _>>()?,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdmissionRecordV1 {
    key: String,
    task_spec: TaskSpecV1,
    dependencies: Vec<TaskId>,
    causal: CausalV1,
    control: Option<ControlV1>,
    canonical_version: u32,
    hash_algorithm: u8,
    hash: [u8; 32],
    timestamp: u64,
    sequence: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LinkV1 {
    task: Option<TaskId>,
    run: Option<RunId>,
    attempt: Option<AttemptId>,
    external: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CausalV1 {
    trace: String,
    correlation: String,
    causation: Option<LinkV1>,
    submitting_principal_ref: Option<String>,
    requesting_actor_ref: Option<String>,
    purpose_ref: Option<String>,
    authorization_ref: Option<String>,
    identity_context_ref: Option<String>,
    signed_statement_ref: Option<String>,
    proof_context_ref: Option<String>,
    origin_ref: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlV1 {
    caller: String,
    session: Option<String>,
    request: Option<String>,
    reason: Option<String>,
}
impl From<AdmissionRecord> for AdmissionRecordV1 {
    fn from(r: AdmissionRecord) -> Self {
        let q = r.request();
        let c = q.causal_context();
        Self {
            key: r.key().as_str().into(),
            task_spec: q.task_spec().into(),
            dependencies: q.dependencies().to_vec(),
            causal: CausalV1 {
                trace: c.trace_id().as_str().into(),
                correlation: c.correlation_id().as_str().into(),
                causation: c.causation().map(|l| LinkV1 {
                    task: l.parent_task_id(),
                    run: l.parent_run_id(),
                    attempt: l.parent_attempt_id(),
                    external: l.external_ref().map(|v| v.expose().into()),
                }),
                submitting_principal_ref: c.submitting_principal_ref().map(|v| v.expose().into()),
                requesting_actor_ref: c.requesting_actor_ref().map(|v| v.expose().into()),
                purpose_ref: c.purpose_ref().map(|v| v.expose().into()),
                authorization_ref: c.authorization_ref().map(|v| v.expose().into()),
                identity_context_ref: c.identity_context_ref().map(|v| v.expose().into()),
                signed_statement_ref: c.signed_statement_ref().map(|v| v.expose().into()),
                proof_context_ref: c.proof_context_ref().map(|v| v.expose().into()),
                origin_ref: c.origin_ref().map(|v| v.expose().into()),
            },
            control: q.control_context().map(|c| ControlV1 {
                caller: c.caller_ref().expose().into(),
                session: c.host_session_ref().map(|v| v.expose().into()),
                request: c.request_id().map(|v| v.expose().into()),
                reason: c.reason_code().map(|v| v.as_str().into()),
            }),
            canonical_version: r.digest().canonical_version(),
            hash_algorithm: 1,
            hash: r.digest().hash().bytes().try_into().expect("SHA-256 size"),
            timestamp: r.timestamp(),
            sequence: r.sequence(),
        }
    }
}
impl TryFrom<AdmissionRecordV1> for AdmissionRecord {
    type Error = DecodeError;
    fn try_from(w: AdmissionRecordV1) -> Result<Self, Self::Error> {
        if w.hash_algorithm != 1 {
            return Err(invalid("unsupported admission hash algorithm"));
        }
        let digest = AdmissionDigest::versioned(
            w.canonical_version,
            ContentHash::new(HashAlgorithm::Sha256, w.hash.to_vec()).map_err(invalid)?,
        )
        .map_err(invalid)?;
        let c = w.causal;
        let mut causal = CausalContext::new(
            TraceId::new(c.trace).map_err(invalid)?,
            CorrelationId::new(c.correlation).map_err(invalid)?,
        );
        if let Some(l) = c.causation {
            causal = causal.with_causation(
                CausationLink::new(
                    l.task,
                    l.run,
                    l.attempt,
                    l.external.map(OpaqueRef::new).transpose().map_err(invalid)?,
                )
                .map_err(invalid)?,
            );
        }
        if let Some(v) = c.submitting_principal_ref {
            causal = causal.with_submitting_principal_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.requesting_actor_ref {
            causal = causal.with_requesting_actor_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.purpose_ref {
            causal = causal.with_purpose_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.authorization_ref {
            causal = causal.with_authorization_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.identity_context_ref {
            causal = causal.with_identity_context_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.signed_statement_ref {
            causal = causal.with_signed_statement_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.proof_context_ref {
            causal = causal.with_proof_context_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        if let Some(v) = c.origin_ref {
            causal = causal.with_origin_ref(OpaqueRef::new(v).map_err(invalid)?);
        }
        let control = w
            .control
            .map(|c| -> Result<_, DecodeError> {
                let mut value =
                    ControlMutationContext::new(OpaqueRef::new(c.caller).map_err(invalid)?);
                if let Some(v) = c.session {
                    value = value.with_host_session_ref(OpaqueRef::new(v).map_err(invalid)?);
                }
                if let Some(v) = c.request {
                    value = value.with_request_id(OpaqueRef::new(v).map_err(invalid)?);
                }
                if let Some(v) = c.reason {
                    value = value.with_reason_code(BoundedCode::new(v).map_err(invalid)?);
                }
                Ok(value)
            })
            .transpose()?;
        Self::new(
            EnsureTaskRequest::new(
                AdmissionKey::new(w.key).map_err(invalid)?,
                w.task_spec.try_into()?,
                w.dependencies,
                causal,
                control,
            )
            .map_err(invalid)?,
            digest,
            w.timestamp,
            w.sequence,
        )
        .map_err(invalid)
    }
}
