//! Frozen kinds 288–291/schema 1. Public domain structs are never serialized directly.
use actionqueue_core::{bounded::*, causal::*, continuation::*, data_ref::*, ids::*};
use serde::{Deserialize, Serialize};

use super::codec::DecodeError;
use crate::mutation::signal::*;
fn invalid(e: impl std::fmt::Display) -> DecodeError {
    DecodeError::Decode(e.to_string())
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HashV1 {
    algorithm: u8,
    bytes: [u8; 32],
}
impl From<&ContentHash> for HashV1 {
    fn from(v: &ContentHash) -> Self {
        Self { algorithm: 1, bytes: v.bytes().try_into().expect("sha256") }
    }
}
impl TryFrom<HashV1> for ContentHash {
    type Error = DecodeError;
    fn try_from(v: HashV1) -> Result<Self, Self::Error> {
        if v.algorithm != 1 {
            return Err(invalid("unsupported signal hash algorithm"));
        }
        ContentHash::new(HashAlgorithm::Sha256, v.bytes.to_vec()).map_err(invalid)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LinkV1 {
    task: Option<TaskId>,
    run: Option<RunId>,
    attempt: Option<AttemptId>,
    external: Option<String>,
}
impl From<&CausationLink> for LinkV1 {
    fn from(v: &CausationLink) -> Self {
        Self {
            task: v.parent_task_id(),
            run: v.parent_run_id(),
            attempt: v.parent_attempt_id(),
            external: v.external_ref().map(|s| s.expose().into()),
        }
    }
}
impl TryFrom<LinkV1> for CausationLink {
    type Error = DecodeError;
    fn try_from(v: LinkV1) -> Result<Self, Self::Error> {
        Self::new(
            v.task,
            v.run,
            v.attempt,
            v.external.map(OpaqueRef::new).transpose().map_err(invalid)?,
        )
        .map_err(invalid)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlV1 {
    caller: String,
    session: Option<String>,
    request: Option<String>,
    reason: Option<String>,
}
impl From<&ControlMutationContext> for ControlV1 {
    fn from(c: &ControlMutationContext) -> Self {
        Self {
            caller: c.caller_ref().expose().into(),
            session: c.host_session_ref().map(|v| v.expose().into()),
            request: c.request_id().map(|v| v.expose().into()),
            reason: c.reason_code().map(|v| v.as_str().into()),
        }
    }
}
impl TryFrom<ControlV1> for ControlMutationContext {
    type Error = DecodeError;
    fn try_from(c: ControlV1) -> Result<Self, Self::Error> {
        let mut v = Self::new(OpaqueRef::new(c.caller).map_err(invalid)?);
        if let Some(s) = c.session {
            v = v.with_host_session_ref(OpaqueRef::new(s).map_err(invalid)?);
        }
        if let Some(s) = c.request {
            v = v.with_request_id(OpaqueRef::new(s).map_err(invalid)?);
        }
        if let Some(s) = c.reason {
            v = v.with_reason_code(BoundedCode::new(s).map_err(invalid)?);
        }
        Ok(v)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum PayloadV1 {
    Inline {
        content_type: Option<String>,
        bytes: Vec<u8>,
        hash: HashV1,
    },
    External {
        scheme: String,
        locator: String,
        hash: HashV1,
        size_bytes: Option<u64>,
        content_type: Option<String>,
    },
}
impl From<&DataRef> for PayloadV1 {
    fn from(p: &DataRef) -> Self {
        match p {
            DataRef::Inline(d) => Self::Inline {
                content_type: d.content_type().map(|v| v.as_str().into()),
                bytes: d.bytes().to_vec(),
                hash: d.hash().into(),
            },
            DataRef::External(d) => Self::External {
                scheme: d.scheme.as_str().into(),
                locator: d.locator.expose().into(),
                hash: (&d.hash).into(),
                size_bytes: d.size_bytes,
                content_type: d.content_type.as_ref().map(|v| v.as_str().into()),
            },
        }
    }
}
impl TryFrom<PayloadV1> for DataRef {
    type Error = DecodeError;
    fn try_from(p: PayloadV1) -> Result<Self, Self::Error> {
        Ok(match p {
            PayloadV1::Inline { content_type, bytes, hash } => Self::Inline(
                InlineData::new(
                    content_type.map(ContentType::new).transpose().map_err(invalid)?,
                    bytes,
                    hash.try_into()?,
                )
                .map_err(invalid)?,
            ),
            PayloadV1::External { scheme, locator, hash, size_bytes, content_type } => {
                Self::External(ExternalDataRef {
                    scheme: DataScheme::new(scheme).map_err(invalid)?,
                    locator: OpaqueRef::new(locator).map_err(invalid)?,
                    hash: hash.try_into()?,
                    size_bytes,
                    content_type: content_type
                        .map(ContentType::new)
                        .transpose()
                        .map_err(invalid)?,
                })
            }
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControlStampV1 {
    wal_sequence: u64,
    timestamp: u64,
    context: Option<ControlV1>,
}
impl From<SignalControlRecord> for ControlStampV1 {
    fn from(v: SignalControlRecord) -> Self {
        Self {
            wal_sequence: v.wal_sequence,
            timestamp: v.timestamp,
            context: v.control_context.as_ref().map(Into::into),
        }
    }
}
impl TryFrom<ControlStampV1> for SignalControlRecord {
    type Error = DecodeError;
    fn try_from(v: ControlStampV1) -> Result<Self, Self::Error> {
        Ok(Self {
            wal_sequence: v.wal_sequence,
            timestamp: v.timestamp,
            control_context: v.context.map(TryInto::try_into).transpose()?,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SignalRecordV1 {
    signal_id: String,
    tenant_id: Option<TenantId>,
    namespace: String,
    kind: String,
    correlation_id: Option<String>,
    causation: Option<LinkV1>,
    source_ref: Option<String>,
    payload: Option<PayloadV1>,
    payload_hash: Option<HashV1>,
    occurred_at: Option<u64>,
    received_at: u64,
    control_context: Option<ControlV1>,
    canonical_version: u32,
    hash_algorithm: u8,
    digest: [u8; 32],
    sequence: u64,
    wal_sequence: u64,
    retirement: Option<ControlStampV1>,
    pins: Vec<(String, ControlStampV1)>,
}
impl From<SignalRecord> for SignalRecordV1 {
    fn from(r: SignalRecord) -> Self {
        let s = r.envelope();
        Self {
            signal_id: s.signal_id.as_str().into(),
            tenant_id: s.tenant_id,
            namespace: s.namespace.as_str().into(),
            kind: s.kind.as_str().into(),
            correlation_id: s.correlation_id.as_ref().map(|v| v.as_str().into()),
            causation: s.causation.as_ref().map(Into::into),
            source_ref: s.source_ref.as_ref().map(|v| v.expose().into()),
            payload: s.payload.as_ref().map(Into::into),
            payload_hash: s.payload_hash.as_ref().map(Into::into),
            occurred_at: s.occurred_at,
            received_at: s.received_at,
            control_context: s.control_context.as_ref().map(Into::into),
            canonical_version: r.digest().canonical_version(),
            hash_algorithm: 1,
            digest: *r.digest().bytes(),
            sequence: r.sequence().get(),
            wal_sequence: r.wal_sequence(),
            retirement: r.retirement.clone().map(Into::into),
            pins: r.pins.into_iter().map(|(id, c)| (id.as_str().into(), c.into())).collect(),
        }
    }
}
impl TryFrom<SignalRecordV1> for SignalRecord {
    type Error = DecodeError;
    fn try_from(v: SignalRecordV1) -> Result<Self, Self::Error> {
        if v.hash_algorithm != 1 {
            return Err(invalid("unsupported signal digest algorithm"));
        }
        let digest = SignalDigest::versioned(v.canonical_version, v.digest).map_err(invalid)?;
        let envelope = SignalEnvelope {
            signal_id: SignalId::new(v.signal_id).map_err(invalid)?,
            tenant_id: v.tenant_id,
            namespace: SignalNamespace::new(v.namespace).map_err(invalid)?,
            kind: SignalKind::new(v.kind).map_err(invalid)?,
            correlation_id: v
                .correlation_id
                .map(CorrelationId::new)
                .transpose()
                .map_err(invalid)?,
            causation: v.causation.map(TryInto::try_into).transpose()?,
            source_ref: v.source_ref.map(OpaqueRef::new).transpose().map_err(invalid)?,
            payload: v.payload.map(TryInto::try_into).transpose()?,
            payload_hash: v.payload_hash.map(TryInto::try_into).transpose()?,
            occurred_at: v.occurred_at,
            received_at: v.received_at,
            control_context: v.control_context.map(TryInto::try_into).transpose()?,
        };
        // Persisted envelopes must already carry the effective hash, never silently normalize.
        if effective_payload_hash(envelope.payload.as_ref(), envelope.payload_hash.as_ref())
            .map_err(invalid)?
            != envelope.payload_hash
        {
            return Err(invalid("noncanonical signal payload hash"));
        }
        let mut r = Self::new(envelope, SignalSequence::new(v.sequence), v.wal_sequence)
            .map_err(invalid)?;
        if *r.digest() != digest {
            return Err(invalid("signal digest mismatch"));
        }
        if v.pins.len() > actionqueue_core::limits::MAX_SIGNAL_PINS
            || v.pins.windows(2).any(|w| w[0].0 >= w[1].0)
        {
            return Err(invalid("noncanonical signal pins"));
        }
        r.retirement = v.retirement.map(TryInto::try_into).transpose()?;
        for (id, c) in v.pins {
            r.pins.insert(SignalPinId::new(id).map_err(invalid)?, c.try_into()?);
        }
        Ok(r)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinV1 {
    tenant_id: Option<TenantId>,
    signal_id: String,
    pin_id: String,
    control: ControlStampV1,
}
impl From<SignalPinRecord> for PinV1 {
    fn from(v: SignalPinRecord) -> Self {
        Self {
            tenant_id: v.tenant_id,
            signal_id: v.signal_id.as_str().into(),
            pin_id: v.pin_id.as_str().into(),
            control: v.control.into(),
        }
    }
}
impl TryFrom<PinV1> for SignalPinRecord {
    type Error = DecodeError;
    fn try_from(v: PinV1) -> Result<Self, Self::Error> {
        Ok(Self {
            tenant_id: v.tenant_id,
            signal_id: SignalId::new(v.signal_id).map_err(invalid)?,
            pin_id: SignalPinId::new(v.pin_id).map_err(invalid)?,
            control: v.control.try_into()?,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetiredV1 {
    tenant_id: Option<TenantId>,
    sequences: Vec<u64>,
    control: ControlStampV1,
}
impl From<SignalsRetiredRecord> for RetiredV1 {
    fn from(v: SignalsRetiredRecord) -> Self {
        Self {
            tenant_id: v.tenant_id,
            sequences: v.sequences.iter().map(|s| s.get()).collect(),
            control: v.control.into(),
        }
    }
}
impl TryFrom<RetiredV1> for SignalsRetiredRecord {
    type Error = DecodeError;
    fn try_from(v: RetiredV1) -> Result<Self, Self::Error> {
        if v.sequences.is_empty()
            || v.sequences.len() > actionqueue_core::limits::MAX_SIGNAL_BATCH
            || v.sequences.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(invalid("invalid retirement batch"));
        }
        Ok(Self {
            tenant_id: v.tenant_id,
            sequences: v.sequences.into_iter().map(SignalSequence::new).collect(),
            control: v.control.try_into()?,
        })
    }
}
