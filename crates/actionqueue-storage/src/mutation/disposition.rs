//! Storage-owned immutable compound attempt history and preparation.
use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::OpaqueRef,
    causal::CausationLink,
    continuation::*,
    disposition::{AttemptDisposition, ChildAdmission},
    ids::*,
    limits::*,
    mutation::*,
    run::{RunInstance, RunState},
};

use crate::{
    mutation::{admission::AdmissionRecord, signal::SignalRecord, wait::WaitRecord},
    recovery::reducer::ReplayReducer,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispositionRejection {
    Stale,
    Invalid,
    ChildrenNonterminal,
    InvalidChildWait,
    TooLarge,
    UnsupportedFeature,
    ImmediateDurabilityRequired,
}
impl std::fmt::Display for DispositionRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "disposition rejected: {self:?}")
    }
}
impl std::error::Error for DispositionRejection {}
use DispositionRejection as R;

/// Version-one persisted child plan, including append-free existing admissions.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispositionChild {
    pub admission: AdmissionRecord,
    pub runs: Vec<RunInstance>,
}
/// Complete immutable lineage; effects share this single WAL sequence.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispositionRecord {
    pub sequence: u64,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub fence: LeaseFence,
    pub timestamp: u64,
    pub disposition: AttemptDisposition,
    pub children: Vec<DispositionChild>,
    pub signals: Vec<SignalRecord>,
    pub target_state: RunState,
    pub failure_attempt_count: u32,
}
impl DispositionRecord {
    pub(crate) fn wait_record(&self) -> Option<WaitRecord> {
        self.disposition.wait().map(|spec| WaitRecord {
            run_id: self.run_id,
            attempt_id: self.attempt_id,
            lease_owner: self.fence.owner().as_str().into(),
            lease_granted_at_sequence: self.fence.granted_at_sequence(),
            sequence: self.sequence,
            timestamp: self.timestamp,
            spec: spec.clone(),
            checkpoint: self.disposition.checkpoint().cloned(),
            resolution: None,
        })
    }
}
impl ReplayReducer {
    /// Checks accepted execution ownership before any child derivation or effect processing.
    pub fn validate_disposition_fence(&self, c: &AttemptDispositionCommitCommand) -> Result<(), R> {
        let run = self.get_run_instance(&c.run_id()).ok_or(R::Stale)?;
        let lease = self.get_lease_metadata(&c.run_id()).ok_or(R::Stale)?;
        let start = self.get_attempt_history(&c.run_id()).and_then(|h| h.last()).ok_or(R::Stale)?;
        if self.latest_sequence().checked_add(1) != Some(c.expected_sequence())
            || c.expected_state() != RunState::Running
            || run.state() != RunState::Running
            || run.current_attempt_id() != Some(c.attempt_id())
            || self.is_task_canceled(run.task_id())
            || lease.owner() != c.expected_lease().owner().as_str()
            || lease.granted_at_sequence() != c.expected_lease().granted_at_sequence()
            || c.timestamp() >= lease.expiry()
            || c.timestamp() < start.started_at()
            || start.attempt_id() != c.attempt_id()
            || start.finished_at().is_some()
            || start.accepted_start().is_none_or(|s| &s.fence != c.expected_lease())
        {
            return Err(R::Stale);
        }
        Ok(())
    }
    /// Constructs trusted child attribution. Exact retries retain their original producer.
    pub fn disposition_child_request(
        &self,
        run: RunId,
        attempt: AttemptId,
        child: &ChildAdmission,
    ) -> Result<EnsureTaskRequest, R> {
        let parent = self.get_run_instance(&run).ok_or(R::Invalid)?.task_id();
        let spec = self.get_task(&parent).ok_or(R::Invalid)?;
        let original = self.task_admission(parent).ok_or(R::Invalid)?;
        let mut causal = original.request().causal_context().clone();
        let overrides = child.causal_override();
        if let Some(v) = &overrides.correlation_id {
            causal = causal.with_correlation_id(v.clone());
        }
        if let Some(v) = &overrides.requesting_actor_ref {
            causal = causal.with_requesting_actor_ref(v.clone());
        }
        if let Some(v) = &overrides.origin_ref {
            causal = causal.with_origin_ref(v.clone());
        }
        let key = actionqueue_core::admission::canonical::scoped_child_key(
            spec.tenant_id(),
            parent,
            run,
            child.admission_key(),
        );
        let old = self.admission(spec.tenant_id(), &key);
        let link = if let Some(old) = old {
            let link = old.request().causal_context().causation().ok_or(R::Invalid)?;
            if link.parent_task_id() != Some(parent) || link.parent_run_id() != Some(run) {
                return Err(R::Invalid);
            }
            let producer = self
                .get_attempt_history(&run)
                .into_iter()
                .flatten()
                .find(|a| Some(a.attempt_id()) == link.parent_attempt_id())
                .and_then(|a| a.disposition.as_ref())
                .ok_or(R::Invalid)?;
            let original = producer
                .disposition
                .child_admissions()
                .iter()
                .find(|c| c.admission_key() == child.admission_key())
                .ok_or(R::Invalid)?;
            if original.causal_override() != child.causal_override() {
                return Err(R::Invalid);
            }
            link.clone()
        } else {
            CausationLink::new(Some(parent), Some(run), Some(attempt), None)
                .map_err(|_| R::Invalid)?
        };
        causal = causal.with_causation(link);
        if child.task_spec().tenant_id() != spec.tenant_id()
            || child.task_spec().parent_task_id().is_some_and(|p| p != parent)
        {
            return Err(R::Invalid);
        }
        EnsureTaskRequest::new(
            key,
            child.task_spec().clone().with_parent(parent),
            child.dependencies().to_vec(),
            causal,
            None,
        )
        .map_err(|_| R::Invalid)
    }
    pub(crate) fn prepare_disposition(
        &self,
        c: &AttemptDispositionCommitCommand,
        admission_limits: AdmissionLimits,
        continuation_limits: ContinuationLimits,
        signal_limits: SignalLimits,
    ) -> Result<DispositionRecord, R> {
        self.validate_disposition_fence(c)?;
        let run = self.get_run_instance(&c.run_id()).unwrap();
        let task = self.get_task(&run.task_id()).unwrap();
        let d = c.disposition();
        if matches!(d.outcome(), actionqueue_core::disposition::DispositionOutcome::Complete)
            && !self.required_children_terminal(run.task_id())
        {
            return Err(R::ChildrenNonterminal);
        }
        let accounting = d
            .outcome()
            .accounting(run.failure_attempt_count(), task.constraints().max_attempts())
            .map_err(|_| R::Invalid)?;
        if d.output().is_some_and(|v| matches!(v, actionqueue_core::data_ref::DataRef::Inline(i) if i.bytes().len() > continuation_limits.output_bytes.min(MAX_INLINE_DATA_BYTES)))
            || d.checkpoint().is_some_and(|v| matches!(&v.data, actionqueue_core::data_ref::DataRef::Inline(i) if i.bytes().len() > continuation_limits.checkpoint_bytes.min(MAX_INLINE_DATA_BYTES))) { return Err(R::TooLarge); }
        if let Some(v) = d.output() {
            v.validate().map_err(|_| R::Invalid)?;
        }
        if let Some(cp) = d.checkpoint() {
            if cp.checkpoint_id.is_nil()
                || cp.created_by_attempt != c.attempt_id()
                || cp.data.validate().is_err()
                || self.checkpoint(cp.checkpoint_id).is_some()
            {
                return Err(R::Invalid);
            }
        }
        if c.children().len() != d.child_admissions().len() {
            return Err(R::Invalid);
        }
        let mut children = Vec::new();
        let mut scratch = self.clone();
        let mut ids = std::collections::HashSet::new();
        let mut keys = std::collections::HashSet::new();
        for (child, plan) in d.child_admissions().iter().zip(c.children()) {
            let request = self.disposition_child_request(c.run_id(), c.attempt_id(), child)?;
            let proposed = AdmissionRecord::from_command(&AdmissionCommitCommand::new(
                c.expected_sequence(),
                plan.clone(),
                None,
                c.timestamp(),
            ))
            .map_err(|_| R::Invalid)?;
            if proposed.request() != &request
                || !ids.insert(proposed.task_id())
                || !keys.insert(proposed.key().clone())
            {
                return Err(R::Invalid);
            }
            if let Some(old) = self.admission(proposed.tenant_id(), proposed.key()) {
                old.resolve(proposed.digest()).map_err(|_| R::Invalid)?;
                children.push(DispositionChild { admission: old.clone(), runs: Vec::new() });
            } else {
                admission_limits
                    .validate_spec(request.task_spec(), request.dependencies().len())
                    .map_err(|_| R::TooLarge)?;
                if scratch.get_task(&proposed.task_id()).is_some() {
                    return Err(R::Invalid);
                }
                // Stage sibling identities and edges for whole-batch cycle and forward-reference checks.
                scratch
                    .apply_task_created(request.task_spec(), c.timestamp())
                    .map_err(|_| R::Invalid)?;
                scratch.apply_dependency_declared(proposed.task_id(), request.dependencies());
                children.push(DispositionChild { admission: proposed, runs: plan.runs().to_vec() });
            }
        }
        for child in &children {
            if child.admission.sequence() != c.expected_sequence() {
                continue;
            }
            let id = child.admission.task_id();
            let staged = scratch.tasks.remove(&id).ok_or(R::Invalid)?;
            let check = scratch.validate_admission(&child.admission, &child.runs);
            scratch.tasks.insert(id, staged);
            check.map_err(|_| R::Invalid)?;
            for run in &child.runs {
                scratch.apply_run_created(run).map_err(|_| R::Invalid)?;
            }
            scratch.insert_admission(child.admission.clone());
        }
        let mut signals = Vec::new();
        for proposal in d.emitted_signals() {
            let envelope = SignalEnvelope {
                signal_id: proposal.signal_id.clone(),
                tenant_id: task.tenant_id(),
                namespace: proposal.namespace.clone(),
                kind: proposal.kind.clone(),
                correlation_id: Some(proposal.correlation_id.clone()),
                causation: Some(
                    CausationLink::new(
                        Some(run.task_id()),
                        Some(c.run_id()),
                        Some(c.attempt_id()),
                        None,
                    )
                    .map_err(|_| R::Invalid)?,
                ),
                source_ref: Some(
                    OpaqueRef::new(format!("actionqueue:task:{}", run.task_id()))
                        .map_err(|_| R::Invalid)?,
                ),
                payload: proposal.payload.clone(),
                payload_hash: proposal.payload_hash.clone(),
                occurred_at: proposal.occurred_at,
                received_at: c.timestamp(),
                control_context: None,
            };
            let digest = CanonicalSignalV1::new(&envelope).map_err(|_| R::Invalid)?.digest();
            if let Some(old) = scratch.signals().get_signal(task.tenant_id(), &proposal.signal_id) {
                old.resolve(&digest).map_err(|_| R::Invalid)?;
                signals.push(old.clone());
                continue;
            }
            scratch.validate_signal_references(&envelope).map_err(|_| R::Invalid)?;
            if envelope.payload.as_ref().is_some_and(|v| matches!(v, actionqueue_core::data_ref::DataRef::Inline(i) if i.bytes().len() > signal_limits.inline_bytes.min(MAX_INLINE_DATA_BYTES))) { return Err(R::TooLarge); }
            let stats = scratch.signals().statistics();
            let seq = scratch.signals().last_sequence().get().checked_add(1).ok_or(R::Invalid)?;
            let record =
                SignalRecord::new(envelope, SignalSequence::new(seq), c.expected_sequence())
                    .map_err(|_| R::Invalid)?;
            let bytes = record.encoded_bytes().map_err(|_| R::TooLarge)?;
            if bytes > signal_limits.record_bytes.min(MAX_SIGNAL_RECORD_BYTES)
                || stats.retained + stats.retired >= signal_limits.identities
                || stats.bytes.checked_add(bytes).is_none_or(|n| n > signal_limits.bytes)
            {
                return Err(R::TooLarge);
            }
            scratch.signals.insert(record.clone()).map_err(|_| R::Invalid)?;
            signals.push(record);
        }
        let record = DispositionRecord {
            sequence: c.expected_sequence(),
            run_id: c.run_id(),
            attempt_id: c.attempt_id(),
            fence: c.expected_lease().clone(),
            timestamp: c.timestamp(),
            disposition: d.clone(),
            children,
            signals,
            target_state: accounting.target_state,
            failure_attempt_count: accounting.failure_attempt_count,
        };
        if let Some(w) = record.wait_record() {
            scratch.validate_wait_establishment(&w).map_err(|_| R::InvalidChildWait)?;
        }
        Ok(record)
    }
}
