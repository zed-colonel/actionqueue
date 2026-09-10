//! Immutable admission facts. Lifecycle controls never modify this record.
use actionqueue_core::{
    admission::{AdmissionDigest, AdmissionRejection, EnsureTaskOutcome, EnsureTaskRequest},
    ids::{AdmissionKey, TaskId, TenantId},
    mutation::AdmissionCommitCommand,
};

/// Original admission meaning and attribution, retained after terminal cleanup.
/// The original specification is retained so future controls cannot change its digest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    try_from = "crate::wal::admission_v1::AdmissionRecordV1",
    into = "crate::wal::admission_v1::AdmissionRecordV1"
)]
pub struct AdmissionRecord {
    request: EnsureTaskRequest,
    digest: AdmissionDigest,
    timestamp: u64,
    sequence: u64,
}
impl AdmissionRecord {
    /// Reconstructs immutable facts; full store validation occurs before commit or hydration.
    pub fn new(
        request: EnsureTaskRequest,
        digest: AdmissionDigest,
        timestamp: u64,
        sequence: u64,
    ) -> Result<Self, AdmissionRejection> {
        if request.digest()? != digest {
            return Err(AdmissionRejection::InvalidDigest);
        }
        Ok(Self { request, digest, timestamp, sequence })
    }
    pub(crate) fn from_command(c: &AdmissionCommitCommand) -> Result<Self, AdmissionRejection> {
        let p = c.plan();
        Self::new(
            EnsureTaskRequest::new(
                p.admission_key().clone(),
                p.task_spec().clone(),
                p.dependencies().to_vec(),
                p.causal_context().clone(),
                c.control_context().cloned(),
            )?,
            p.digest().clone(),
            c.timestamp(),
            c.expected_sequence(),
        )
    }
    /// Original request including the first successful control attribution.
    pub fn request(&self) -> &EnsureTaskRequest {
        &self.request
    }
    /// Original task identity.
    pub fn task_id(&self) -> TaskId {
        self.request.task_spec().id()
    }
    /// Tenant namespace; None is the single-tenant namespace.
    pub fn tenant_id(&self) -> Option<TenantId> {
        self.request.task_spec().tenant_id()
    }
    /// Original lookup key.
    pub fn key(&self) -> &AdmissionKey {
        self.request.admission_key()
    }
    /// Original digest.
    pub fn digest(&self) -> &AdmissionDigest {
        &self.digest
    }
    /// Original timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    /// Original admission sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub(crate) fn outcome(&self, created: bool) -> EnsureTaskOutcome {
        if created {
            EnsureTaskOutcome::Created {
                task_id: self.task_id(),
                admission_key: self.key().clone(),
                digest: self.digest.clone(),
                sequence: self.sequence,
            }
        } else {
            EnsureTaskOutcome::AlreadyExists {
                task_id: self.task_id(),
                admission_key: self.key().clone(),
                digest: self.digest.clone(),
                sequence: self.sequence,
            }
        }
    }
    pub(crate) fn resolve(
        &self,
        proposed: &AdmissionDigest,
    ) -> Result<EnsureTaskOutcome, AdmissionRejection> {
        if proposed == &self.digest {
            Ok(self.outcome(false))
        } else {
            Err(AdmissionRejection::Conflict {
                admission_key: self.key().clone(),
                existing_task_id: self.task_id(),
                existing_digest: self.digest.clone(),
                proposed_digest: proposed.clone(),
            })
        }
    }
}
