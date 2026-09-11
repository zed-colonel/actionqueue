//! Frozen kinds 304–307 and 320–321, schema 1. Explicit numeric resolution/policy tags.
use actionqueue_core::{bounded::*, continuation::*, ids::*, mutation::CancelTarget};
use serde::{Deserialize, Serialize};

use super::{
    codec::DecodeError,
    signal_v1::{ControlV1, PayloadV1},
};
use crate::mutation::wait::*;
fn invalid(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Decode(e.to_string())
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolutionV1 {
    run: RunId,
    wait: WaitId,
    sequence: u64,
    timestamp: u64,
    kind: u8,
    signal: Option<u64>,
    control: Option<ControlV1>,
}
impl From<WaitResolution> for ResolutionV1 {
    fn from(r: WaitResolution) -> Self {
        let (kind, signal, control) = match r.kind {
            WaitResolutionKind::Signal(s) => (0, Some(s.get()), None),
            WaitResolutionKind::Deadline => (1, None, None),
            WaitResolutionKind::Control(c) => (2, None, Some((&c).into())),
            WaitResolutionKind::Canceled(c) => (3, None, c.as_ref().map(Into::into)),
        };
        Self {
            run: r.run_id,
            wait: r.wait_id,
            sequence: r.sequence,
            timestamp: r.timestamp,
            kind,
            signal,
            control,
        }
    }
}
impl TryFrom<ResolutionV1> for WaitResolution {
    type Error = DecodeError;
    fn try_from(r: ResolutionV1) -> Result<Self, Self::Error> {
        let kind = match (r.kind, r.signal, r.control) {
            (0, Some(s), None) if s > 0 => WaitResolutionKind::Signal(SignalSequence::new(s)),
            (1, None, None) => WaitResolutionKind::Deadline,
            (2, None, Some(c)) => WaitResolutionKind::Control(c.try_into()?),
            (3, None, c) => WaitResolutionKind::Canceled(c.map(TryInto::try_into).transpose()?),
            _ => return Err(invalid("invalid resolution tag")),
        };
        if r.run.as_uuid().is_nil() || r.wait.is_nil() || r.sequence == 0 {
            return Err(invalid("invalid resolution identity"));
        }
        Ok(Self {
            run_id: r.run,
            wait_id: r.wait,
            sequence: r.sequence,
            timestamp: r.timestamp,
            kind,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeadlineV1 {
    at: u64,
    policy: u8,
    code: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointV1 {
    id: CheckpointId,
    attempt: AttemptId,
    data: PayloadV1,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WaitV1 {
    run: RunId,
    attempt: AttemptId,
    owner: String,
    grant: u64,
    sequence: u64,
    timestamp: u64,
    wait: WaitId,
    tenant: Option<TenantId>,
    namespace: String,
    kind: String,
    correlation: Option<String>,
    source: Option<String>,
    after: Option<u64>,
    deadline: Option<DeadlineV1>,
    checkpoint: Option<CheckpointV1>,
    resolution: Option<ResolutionV1>,
}
impl From<WaitRecord> for WaitV1 {
    fn from(r: WaitRecord) -> Self {
        let f = r.spec.filter();
        Self {
            run: r.run_id,
            attempt: r.attempt_id,
            owner: r.lease_owner,
            grant: r.lease_granted_at_sequence,
            sequence: r.sequence,
            timestamp: r.timestamp,
            wait: r.spec.wait_id(),
            tenant: f.tenant_id,
            namespace: f.namespace.as_str().into(),
            kind: f.kind.as_str().into(),
            correlation: f.correlation_id.as_ref().map(|s| s.as_str().into()),
            source: f.source_ref.as_ref().map(|s| s.expose().into()),
            after: match r.spec.eligible_from() {
                SignalEligibility::AnyRetained => None,
                SignalEligibility::After(s) => Some(s.get()),
            },
            deadline: r.spec.deadline().map(|d| {
                let (policy, code) = match &d.policy {
                    WaitTimeoutPolicy::ResumeWithTimeout => (0, None),
                    WaitTimeoutPolicy::FailRun { code } => (1, Some(code.as_str().into())),
                    WaitTimeoutPolicy::CancelRun => (2, None),
                };
                DeadlineV1 { at: d.at, policy, code }
            }),
            checkpoint: r.checkpoint.map(|c| CheckpointV1 {
                id: c.checkpoint_id,
                attempt: c.created_by_attempt,
                data: (&c.data).into(),
            }),
            resolution: r.resolution.map(Into::into),
        }
    }
}
impl TryFrom<WaitV1> for WaitRecord {
    type Error = DecodeError;
    fn try_from(r: WaitV1) -> Result<Self, Self::Error> {
        let deadline = r
            .deadline
            .map(|d| {
                let policy = match (d.policy, d.code) {
                    (0, None) => WaitTimeoutPolicy::ResumeWithTimeout,
                    (1, Some(c)) => {
                        WaitTimeoutPolicy::FailRun { code: BoundedCode::new(c).map_err(invalid)? }
                    }
                    (2, None) => WaitTimeoutPolicy::CancelRun,
                    _ => return Err(invalid("invalid timeout policy")),
                };
                Ok(WaitDeadline { at: d.at, policy })
            })
            .transpose()?;
        let spec = WaitSpec::new(
            r.wait,
            SignalFilter {
                tenant_id: r.tenant,
                namespace: SignalNamespace::new(r.namespace).map_err(invalid)?,
                kind: SignalKind::new(r.kind).map_err(invalid)?,
                correlation_id: r
                    .correlation
                    .map(CorrelationId::new)
                    .transpose()
                    .map_err(invalid)?,
                source_ref: r.source.map(OpaqueRef::new).transpose().map_err(invalid)?,
            },
            WaitMatchPolicy::FirstMatch,
            r.after
                .map(|s| SignalEligibility::After(SignalSequence::new(s)))
                .unwrap_or(SignalEligibility::AnyRetained),
            deadline,
        )
        .map_err(invalid)?;
        let checkpoint = r
            .checkpoint
            .map(|c| {
                Ok::<_, DecodeError>(CheckpointRef {
                    checkpoint_id: c.id,
                    created_by_attempt: c.attempt,
                    data: c.data.try_into()?,
                })
            })
            .transpose()?;
        if r.run.as_uuid().is_nil()
            || r.wait.is_nil()
            || r.attempt.as_uuid().is_nil()
            || r.owner.is_empty()
            || r.grant == 0
            || r.sequence <= r.grant
        {
            return Err(invalid("invalid wait identity or fence"));
        }
        Ok(Self {
            run_id: r.run,
            attempt_id: r.attempt,
            lease_owner: r.owner,
            lease_granted_at_sequence: r.grant,
            sequence: r.sequence,
            timestamp: r.timestamp,
            spec,
            checkpoint,
            resolution: r.resolution.map(TryInto::try_into).transpose()?,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelV1 {
    task: Option<TaskId>,
    run: Option<RunId>,
    tenant: Option<TenantId>,
    control: Option<ControlV1>,
    sequence: u64,
    timestamp: u64,
}
impl From<CancelRecord> for CancelV1 {
    fn from(r: CancelRecord) -> Self {
        let (task, run) = match r.target {
            CancelTarget::Task(t) => (Some(t), None),
            CancelTarget::Run(r) => (None, Some(r)),
        };
        Self {
            task,
            run,
            tenant: r.tenant_id,
            control: r.control_context.as_ref().map(Into::into),
            sequence: r.sequence,
            timestamp: r.timestamp,
        }
    }
}
impl TryFrom<CancelV1> for CancelRecord {
    type Error = DecodeError;
    fn try_from(r: CancelV1) -> Result<Self, Self::Error> {
        let target = match (r.task, r.run) {
            (Some(t), None) if !t.is_nil() => CancelTarget::Task(t),
            (None, Some(id)) if !id.as_uuid().is_nil() => CancelTarget::Run(id),
            _ => return Err(invalid("invalid cancellation target")),
        };
        Ok(Self {
            target,
            tenant_id: r.tenant,
            control_context: r.control.map(TryInto::try_into).transpose()?,
            sequence: r.sequence,
            timestamp: r.timestamp,
        })
    }
}
