//! Derived active-filter/deadline/match indexes and atomic continuation projection.
use super::reducer::{ReplayReducer, ReplayReducerError};
use crate::{
    mutation::wait::*,
    wal::event::{WalEvent, WalEventType},
};
use actionqueue_core::{
    bounded::OpaqueRef, continuation::*, ids::*, mutation::*, run::RunState,
    task::constraints::ConcurrencyKeyWaitPolicy,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};
type Key =
    (Option<TenantId>, SignalNamespace, SignalKind, Option<CorrelationId>, Option<OpaqueRef>);
fn key(f: &SignalFilter) -> Key {
    (
        f.tenant_id,
        f.namespace.clone(),
        f.kind.clone(),
        f.correlation_id.clone(),
        f.source_ref.clone(),
    )
}
fn after(w: &WaitSpec) -> SignalSequence {
    match w.eligible_from() {
        SignalEligibility::AnyRetained => SignalSequence::new(0),
        SignalEligibility::After(s) => *s,
    }
}
#[derive(Debug, Clone, Default)]
pub struct WaitIndex {
    pub(crate) records: BTreeMap<WaitId, WaitRecord>,
    active: HashMap<RunId, WaitId>,
    matching: HashMap<Key, BTreeSet<WaitId>>,
    deadlines: BTreeSet<(u64, WaitId)>,
    candidates: BTreeSet<(SignalSequence, WaitId)>,
    pub(crate) pending: BTreeMap<RunId, WaitId>,
    historical: BTreeSet<SignalSequence>,
}
impl WaitIndex {
    pub fn get(&self, id: WaitId) -> Option<&WaitRecord> {
        self.records.get(&id)
    }
    pub fn records(&self) -> impl Iterator<Item = &WaitRecord> {
        self.records.values()
    }
    pub fn active(&self, run: RunId) -> Option<&WaitRecord> {
        self.active.get(&run).and_then(|id| self.get(*id))
    }
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.deadlines.first().map(|v| v.0)
    }
    pub fn pending_wait(&self, run: RunId) -> Option<WaitId> {
        self.pending.get(&run).copied()
    }
    /// Indexed, deterministic candidates with explicit remaining-work reporting at the service.
    pub fn matches(&self, limit: usize) -> Vec<(SignalSequence, WaitId)> {
        self.candidates.iter().take(limit).copied().collect()
    }
    pub fn due(&self, now: u64, limit: usize) -> Vec<WaitId> {
        self.deadlines
            .iter()
            .take_while(|(at, _)| *at <= now)
            .take(limit)
            .map(|(_, id)| *id)
            .collect()
    }
    pub fn signal_waiters(&self, e: &SignalEnvelope) -> BTreeSet<WaitId> {
        let mut ids = BTreeSet::new();
        for correlation in [None, e.correlation_id.clone()] {
            for source in [None, e.source_ref.clone()] {
                if let Some(bucket) = self.matching.get(&(
                    e.tenant_id,
                    e.namespace.clone(),
                    e.kind.clone(),
                    correlation.clone(),
                    source,
                )) {
                    ids.extend(bucket);
                }
            }
        }
        ids
    }
    pub(crate) fn insert(&mut self, r: WaitRecord) {
        let id = r.spec.wait_id();
        if r.resolution.is_none() {
            self.active.insert(r.run_id, id);
            self.matching.entry(key(r.spec.filter())).or_default().insert(id);
            if let Some(d) = r.spec.deadline() {
                self.deadlines.insert((d.at, id));
            }
        } else if let Some(WaitResolution { kind: WaitResolutionKind::Signal(s), .. }) =
            &r.resolution
        {
            self.historical.insert(*s);
        }
        self.records.insert(id, r);
    }
    fn close(&mut self, r: WaitResolution) {
        let w = self.records.get_mut(&r.wait_id).expect("validated wait");
        self.active.remove(&w.run_id);
        let k = key(w.spec.filter());
        if let Some(bucket) = self.matching.get_mut(&k) {
            bucket.remove(&r.wait_id);
            if bucket.is_empty() {
                self.matching.remove(&k);
            }
        }
        if let Some(d) = w.spec.deadline() {
            self.deadlines.remove(&(d.at, r.wait_id));
        }
        self.candidates.retain(|(_, id)| *id != r.wait_id);
        if let WaitResolutionKind::Signal(s) = r.kind {
            self.historical.insert(s);
        }
        w.resolution = Some(r);
    }
}
impl ReplayReducer {
    pub fn waits(&self) -> &WaitIndex {
        &self.waits
    }
    pub fn pending_resume(&self, run: RunId) -> Option<ResumeContext> {
        let w = self.waits.get(self.waits.pending_wait(run)?)?;
        let r = w.resolution.as_ref()?;
        let wake = match &r.kind {
            WaitResolutionKind::Signal(s) => WakeReason::Signal {
                wait_id: r.wait_id,
                signal_sequence: *s,
                envelope: Box::new(self.signals.by_sequence(*s)?.envelope().clone()),
            },
            WaitResolutionKind::Deadline => {
                WakeReason::Deadline { wait_id: r.wait_id, deadline_at: w.spec.deadline()?.at }
            }
            WaitResolutionKind::Control(c) => {
                WakeReason::ControlResolution { wait_id: r.wait_id, control_context: c.clone() }
            }
            WaitResolutionKind::Canceled(_) => return None,
        };
        Some(ResumeContext { checkpoint: w.checkpoint.clone(), wake, resumed_at: r.timestamp })
    }
    pub fn earliest_signal(&self, spec: &WaitSpec) -> Option<SignalSequence> {
        self.signals
            .retained_candidates(spec.filter(), after(spec), 1)
            .first()
            .map(|r| r.sequence())
    }
    pub(crate) fn refresh_wait_candidate(&mut self, id: WaitId) {
        self.waits.candidates.retain(|(_, w)| *w != id);
        if let Some(w) = self.waits.get(id) {
            if w.resolution.is_none() {
                if let Some(s) = self.earliest_signal(&w.spec) {
                    self.waits.candidates.insert((s, id));
                }
            }
        }
    }
    pub(crate) fn signal_arrived(&mut self, e: &SignalEnvelope) {
        for id in self.waits.signal_waiters(e) {
            self.refresh_wait_candidate(id);
        }
    }
    /// Continuation references are independent of removable manual pins.
    pub fn signal_is_protected(&self, s: SignalSequence) -> bool {
        let Some(r) = self.signals.by_sequence(s) else { return false };
        !r.pins().is_empty()
            || self.waits.historical.contains(&s)
            || self
                .waits
                .signal_waiters(r.envelope())
                .iter()
                .any(|id| s > after(&self.waits.records[id].spec))
    }
    pub fn signal_retirement_candidates(
        &self,
        tenant: Option<TenantId>,
        policy: actionqueue_core::limits::SignalRetentionPolicy,
        now: u64,
        limit: usize,
    ) -> Vec<SignalSequence> {
        self.signals
            .records()
            .filter(|r| {
                r.is_retained()
                    && r.envelope().tenant_id == tenant
                    && policy.permits(
                        actionqueue_core::limits::SignalRetentionCandidate {
                            received_at: r.envelope().received_at,
                            sequence: r.sequence().get(),
                            protected: self.signal_is_protected(r.sequence()),
                        },
                        self.signals.last_sequence().get(),
                        now,
                    )
            })
            .take(limit.min(actionqueue_core::limits::MAX_SIGNAL_BATCH))
            .map(|r| r.sequence())
            .collect()
    }
    fn wait_sequence(&self, seq: u64) -> Result<(), WaitRejection> {
        if self.latest_sequence.checked_add(1) != Some(seq) {
            Err(WaitRejection::StaleSequence)
        } else {
            Ok(())
        }
    }
    fn tenant_for_run(&self, id: RunId) -> Result<Option<TenantId>, WaitRejection> {
        let run = self.get_run_instance(&id).ok_or(WaitRejection::NotFound)?;
        Ok(self.get_task(&run.task_id()).ok_or(WaitRejection::NotFound)?.tenant_id())
    }
    pub(crate) fn validate_wait_establishment(&self, r: &WaitRecord) -> Result<(), WaitRejection> {
        use WaitRejection as E;
        if r.spec.wait_id().is_nil()
            || r.resolution.is_some()
            || self.waits.get(r.spec.wait_id()).is_some()
        {
            return Err(E::InvalidIdentity);
        }
        if self.waits.active(r.run_id).is_some() {
            return Err(E::ActiveWaitExists);
        }
        let run = self.get_run_instance(&r.run_id).ok_or(E::NotFound)?;
        if self.is_task_canceled(run.task_id()) {
            return Err(E::TaskCanceled);
        }
        if run.state() != RunState::Running || self.waits.pending_wait(r.run_id).is_some() {
            return Err(E::InvalidState);
        }
        if self.tenant_for_run(r.run_id)? != r.spec.filter().tenant_id {
            return Err(E::TenantMismatch);
        }
        if run.current_attempt_id() != Some(r.attempt_id) {
            return Err(E::StaleAttempt);
        }
        let l = self.get_lease_metadata(&r.run_id).ok_or(E::StaleLease)?;
        if l.owner() != r.lease_owner
            || l.granted_at_sequence() != r.lease_granted_at_sequence
            || r.timestamp >= l.expiry()
            || r.sequence <= r.lease_granted_at_sequence
        {
            return Err(E::StaleLease);
        }
        if let Some(c) = &r.checkpoint {
            if c.checkpoint_id.is_nil() || c.created_by_attempt != r.attempt_id {
                return Err(E::InvalidCheckpoint);
            }
        }
        if r.timestamp
            < self
                .get_attempt_history(&r.run_id)
                .and_then(|h| h.last())
                .ok_or(E::StaleAttempt)?
                .started_at()
        {
            return Err(E::StaleAttempt);
        }
        Ok(())
    }
    pub(crate) fn resolution_state(
        &self,
        w: &WaitRecord,
        r: &WaitResolution,
    ) -> Result<RunState, WaitRejection> {
        use WaitRejection as E;
        if r.run_id != w.run_id || r.wait_id != w.spec.wait_id() || r.sequence <= w.sequence {
            return Err(E::InvalidIdentity);
        }
        if w.resolution.is_some() {
            return Err(E::WaitAlreadyResolved);
        }
        if self.get_run_state(&r.run_id) != Some(&RunState::Awaiting)
            || self.waits.active(r.run_id).map(|w| w.spec.wait_id()) != Some(r.wait_id)
        {
            return Err(E::InvalidState);
        }
        match &r.kind {
            WaitResolutionKind::Signal(s) => {
                if self.earliest_signal(&w.spec) != Some(*s) {
                    return Err(E::InvalidSignal);
                }
                Ok(RunState::Ready)
            }
            WaitResolutionKind::Control(_) => Ok(RunState::Ready),
            WaitResolutionKind::Canceled(_) => Ok(RunState::Canceled),
            WaitResolutionKind::Deadline => {
                let d = w.spec.deadline().ok_or(E::NotDue)?;
                if r.timestamp < d.at {
                    return Err(E::NotDue);
                }
                Ok(match d.policy {
                    WaitTimeoutPolicy::ResumeWithTimeout => RunState::Ready,
                    WaitTimeoutPolicy::FailRun { .. } => RunState::Failed,
                    WaitTimeoutPolicy::CancelRun => RunState::Canceled,
                })
            }
        }
    }
    pub(crate) fn prepare_wait_command(
        &self,
        c: &MutationCommand,
        d: DurabilityPolicy,
    ) -> Result<Option<WaitPreparation>, WaitRejection> {
        use WaitRejection as E;
        let mut resolution = None;
        let mut cancel = None;
        match c {
            MutationCommand::WaitEstablish(c) => {
                if d != DurabilityPolicy::Immediate {
                    return Err(E::ImmediateDurabilityRequired);
                }
                let e = &c.expected;
                let r = WaitRecord {
                    run_id: e.run_id(),
                    attempt_id: e.attempt_id(),
                    lease_owner: e.expected_lease().owner().as_str().into(),
                    lease_granted_at_sequence: e.expected_lease().granted_at_sequence(),
                    sequence: e.expected_sequence(),
                    timestamp: c.timestamp,
                    spec: c.wait.clone(),
                    checkpoint: c.checkpoint.clone(),
                    resolution: None,
                };
                if e.expected_state() != RunState::Running {
                    return Err(E::InvalidState);
                }
                if let Some(old) = self.waits.get(r.spec.wait_id()) {
                    let mut original = old.clone();
                    original.resolution = None;
                    original.sequence = r.sequence;
                    original.timestamp = r.timestamp;
                    if original != r {
                        return Err(E::InvalidIdentity);
                    }
                    return Ok(Some(wait_noop(WaitOutcome::AlreadyEstablished {
                        wait_id: r.spec.wait_id(),
                        sequence: old.sequence,
                    })));
                }
                self.wait_sequence(r.sequence)?;
                self.validate_wait_establishment(&r)?;
                let applied = AppliedMutation::Wait(WaitOutcome::Established {
                    wait_id: r.spec.wait_id(),
                    sequence: r.sequence,
                });
                return Ok(Some(WaitPreparation::Event(
                    Box::new(WalEvent::new(
                        r.sequence,
                        WalEventType::WaitEstablished { record: r },
                    )),
                    applied,
                )));
            }
            MutationCommand::WaitSatisfy(c) => {
                resolution = Some(WaitResolution {
                    run_id: c.run_id(),
                    wait_id: c.wait_id(),
                    sequence: c.expected_sequence(),
                    timestamp: c.timestamp(),
                    kind: WaitResolutionKind::Signal(c.signal_sequence()),
                })
            }
            MutationCommand::WaitTimeout(c) => {
                resolution = Some(WaitResolution {
                    run_id: c.run_id(),
                    wait_id: c.wait_id(),
                    sequence: c.expected_sequence(),
                    timestamp: c.timestamp(),
                    kind: WaitResolutionKind::Deadline,
                })
            }
            MutationCommand::WaitResolve(c) => {
                if self.tenant_for_run(c.run_id)? != c.tenant_id {
                    return Err(E::TenantMismatch);
                }
                resolution = Some(WaitResolution {
                    run_id: c.run_id,
                    wait_id: c.wait_id,
                    sequence: c.expected_sequence,
                    timestamp: c.timestamp,
                    kind: WaitResolutionKind::Control(c.control_context.clone()),
                });
            }
            MutationCommand::WaitCancel(c) => {
                if self.tenant_for_run(c.run_id())? != c.tenant_id() {
                    return Err(E::TenantMismatch);
                }
                resolution = Some(WaitResolution {
                    run_id: c.run_id(),
                    wait_id: c.wait_id(),
                    sequence: c.expected_sequence(),
                    timestamp: c.timestamp(),
                    kind: WaitResolutionKind::Canceled(Some(c.control_context().clone())),
                });
            }
            MutationCommand::Cancel(c) => {
                cancel = Some(CancelRecord {
                    target: c.target,
                    tenant_id: c.tenant_id,
                    control_context: c.control_context.clone(),
                    sequence: c.expected_sequence,
                    timestamp: c.timestamp,
                })
            }
            // Legacy surfaces still pass through the same compound cancellation authority.
            MutationCommand::TaskCancel(c) => {
                let t = self.get_task(&c.task_id()).ok_or(E::NotFound)?;
                cancel = Some(CancelRecord {
                    target: CancelTarget::Task(c.task_id()),
                    tenant_id: t.tenant_id(),
                    control_context: None,
                    sequence: c.sequence(),
                    timestamp: c.timestamp(),
                });
            }
            MutationCommand::RunStateTransition(c) if c.new_state() == RunState::Canceled => {
                if self.get_run_state(&c.run_id()) != Some(&c.previous_state()) {
                    return Err(E::InvalidState);
                }
                cancel = Some(CancelRecord {
                    target: CancelTarget::Run(c.run_id()),
                    tenant_id: self.tenant_for_run(c.run_id())?,
                    control_context: None,
                    sequence: c.sequence(),
                    timestamp: c.timestamp(),
                });
            }
            _ => return Ok(None),
        }
        if d != DurabilityPolicy::Immediate {
            return Err(E::ImmediateDurabilityRequired);
        }
        if let Some(r) = resolution {
            let w = self.waits.get(r.wait_id).ok_or(E::NotFound)?;
            if r.run_id != w.run_id {
                return Err(E::InvalidIdentity);
            }
            if let Some(old) = &w.resolution {
                if old.kind != r.kind {
                    return Err(E::WaitAlreadyResolved);
                }
                return Ok(Some(wait_noop(WaitOutcome::AlreadyResolved {
                    wait_id: r.wait_id,
                    sequence: old.sequence,
                })));
            }
            self.wait_sequence(r.sequence)?;
            self.resolution_state(w, &r)?;
            let applied = AppliedMutation::Wait(WaitOutcome::Resolved {
                wait_id: r.wait_id,
                sequence: r.sequence,
            });
            let seq = r.sequence;
            let event = match r.kind {
                WaitResolutionKind::Signal(_) | WaitResolutionKind::Control(_) => {
                    WalEventType::WaitSatisfied { record: r }
                }
                WaitResolutionKind::Deadline => WalEventType::WaitTimedOut { record: r },
                WaitResolutionKind::Canceled(_) => WalEventType::WaitCanceled { record: r },
            };
            return Ok(Some(WaitPreparation::Event(Box::new(WalEvent::new(seq, event)), applied)));
        }
        let r = cancel.expect("cancel command");
        self.validate_cancel(&r)?;
        if let Some(old) = self.cancellations.iter().find(|old| old.target == r.target) {
            return Ok(Some(WaitPreparation::Noop(MutationOutcome::new(
                old.sequence,
                AppliedMutation::NoOp,
            ))));
        }
        self.wait_sequence(r.sequence)?;
        let applied = match r.target {
            CancelTarget::Task(task_id) => AppliedMutation::TaskCancel { task_id },
            CancelTarget::Run(run_id) => AppliedMutation::RunStateTransition {
                run_id,
                previous_state: *self.get_run_state(&run_id).ok_or(E::NotFound)?,
                new_state: RunState::Canceled,
            },
        };
        let event = match r.target {
            CancelTarget::Task(_) => WalEventType::TaskCancellationCommitted { record: r.clone() },
            CancelTarget::Run(_) => WalEventType::RunCancellationCommitted { record: r.clone() },
        };
        Ok(Some(WaitPreparation::Event(Box::new(WalEvent::new(r.sequence, event)), applied)))
    }
    fn validate_cancel(&self, r: &CancelRecord) -> Result<(), WaitRejection> {
        let tenant = match r.target {
            CancelTarget::Run(id) => self.tenant_for_run(id)?,
            CancelTarget::Task(id) => {
                self.get_task(&id).ok_or(WaitRejection::NotFound)?.tenant_id()
            }
        };
        if tenant != r.tenant_id {
            return Err(WaitRejection::TenantMismatch);
        }
        Ok(())
    }
    pub(crate) fn apply_wait_established(
        &mut self,
        r: &WaitRecord,
    ) -> Result<(), ReplayReducerError> {
        self.validate_wait_establishment(r).map_err(ReplayReducerError::Wait)?;
        let mut p = self.clone();
        p.apply_attempt_finished(
            &r.run_id,
            &r.attempt_id,
            AttemptOutcome::awaiting(),
            r.timestamp,
        )?;
        p.leases.remove(&r.run_id);
        p.lease_metadata.remove(&r.run_id);
        p.apply_run_state_changed(&r.run_id, &RunState::Running, &RunState::Awaiting, r.timestamp)?;
        let task = p.get_task(&p.get_run_instance(&r.run_id).unwrap().task_id()).unwrap();
        if task.constraints().concurrency_key_wait_policy()
            == ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting
        {
            p.key_reservations.remove(&r.run_id);
        }
        p.waits.insert(r.clone());
        p.refresh_wait_candidate(r.spec.wait_id());
        *self = p;
        Ok(())
    }
    pub(crate) fn apply_wait_resolution(
        &mut self,
        r: &WaitResolution,
    ) -> Result<(), ReplayReducerError> {
        let w = self.waits.get(r.wait_id).ok_or(ReplayReducerError::CorruptedData)?;
        let state = self.resolution_state(w, r).map_err(ReplayReducerError::Wait)?;
        let mut p = self.clone();
        p.waits.close(r.clone());
        if state == RunState::Ready {
            p.waits.pending.insert(r.run_id, r.wait_id);
        }
        p.apply_run_state_changed(&r.run_id, &RunState::Awaiting, &state, r.timestamp)?;
        *self = p;
        Ok(())
    }
    pub(crate) fn apply_cancellation(
        &mut self,
        r: &CancelRecord,
    ) -> Result<(), ReplayReducerError> {
        self.validate_cancel(r).map_err(ReplayReducerError::Wait)?;
        let mut p = self.clone();
        let runs = match r.target {
            CancelTarget::Run(id) => vec![id],
            CancelTarget::Task(id) => {
                if !p.is_task_canceled(id) {
                    p.apply_task_canceled(&id, r.timestamp)?;
                }
                p.runs_for_task(id).map(|r| r.id()).collect()
            }
        };
        for id in runs {
            if p.get_run_state(&id).is_some_and(|s| s.is_terminal()) {
                continue;
            }
            if let Some(w) = p.waits.active(id) {
                let res = WaitResolution {
                    run_id: id,
                    wait_id: w.spec.wait_id(),
                    sequence: r.sequence,
                    timestamp: r.timestamp,
                    kind: WaitResolutionKind::Canceled(r.control_context.clone()),
                };
                p.apply_wait_resolution(&res)?;
            } else {
                p.waits.pending.remove(&id);
                p.apply_run_canceled(&id, r.timestamp)?;
            }
        }
        p.cancellations.push(r.clone());
        *self = p;
        Ok(())
    }
    /// Durable claims include leased/running runs and retained retry/suspension/wait owners.
    pub fn key_reservations(&self) -> impl Iterator<Item = (RunId, &str)> {
        self.key_reservations.iter().map(|(id, key)| (*id, key.as_str()))
    }
}
fn wait_noop(o: WaitOutcome) -> WaitPreparation {
    WaitPreparation::Noop(MutationOutcome::new(o.sequence(), AppliedMutation::Wait(o)))
}
impl ReplayReducer {
    pub(crate) fn hydrate_waits(
        &mut self,
        records: &[WaitRecord],
        pending: &[(RunId, WaitId)],
        keys: &[(RunId, String)],
        cancels: &[CancelRecord],
    ) -> Result<(), WaitRejection> {
        use WaitRejection as E;
        let mut index = WaitIndex::default();
        for r in records {
            if r.sequence == 0
                || r.sequence > self.latest_sequence
                || r.lease_granted_at_sequence == 0
                || r.lease_granted_at_sequence >= r.sequence
                || r.spec.wait_id().is_nil()
                || index.get(r.spec.wait_id()).is_some()
            {
                return Err(E::InvalidIdentity);
            }
            if self.tenant_for_run(r.run_id)? != r.spec.filter().tenant_id {
                return Err(E::TenantMismatch);
            }
            let attempt = self
                .get_attempt_history(&r.run_id)
                .and_then(|h| h.iter().find(|a| a.attempt_id() == r.attempt_id))
                .ok_or(E::StaleAttempt)?;
            if attempt.result() != Some(AttemptResultKind::Awaiting)
                || attempt.finished_at() != Some(r.timestamp)
            {
                return Err(E::StaleAttempt);
            }
            if r.checkpoint
                .as_ref()
                .is_some_and(|c| c.created_by_attempt != r.attempt_id || c.checkpoint_id.is_nil())
            {
                return Err(E::InvalidCheckpoint);
            }
            if let Some(res) = &r.resolution {
                if res.run_id != r.run_id
                    || res.wait_id != r.spec.wait_id()
                    || res.sequence <= r.sequence
                    || res.sequence > self.latest_sequence
                {
                    return Err(E::InvalidIdentity);
                }
                match &res.kind {
                    WaitResolutionKind::Signal(s) => {
                        let selected = self.signals.records().find(|s| {
                            s.wal_sequence() < res.sequence
                                && s.retirement().is_none_or(|c| c.wal_sequence > res.sequence)
                                && matches_signal(&r.spec, s.envelope(), s.sequence())
                        });
                        if selected.map(|r| r.sequence()) != Some(*s)
                            || self.signals.by_sequence(*s).is_none_or(|r| !r.is_retained())
                        {
                            return Err(E::InvalidSignal);
                        }
                    }
                    WaitResolutionKind::Deadline => {
                        if r.spec.deadline().is_none_or(|d| res.timestamp < d.at) {
                            return Err(E::NotDue);
                        }
                    }
                    _ => {}
                }
            } else {
                if index.active(r.run_id).is_some()
                    || self.get_run_state(&r.run_id) != Some(&RunState::Awaiting)
                    || self.is_task_canceled(
                        self.get_run_instance(&r.run_id).ok_or(E::NotFound)?.task_id(),
                    )
                {
                    return Err(E::InvalidState);
                }
                if self.get_lease(&r.run_id).is_some()
                    || self.get_run_instance(&r.run_id).unwrap().current_attempt_id().is_some()
                {
                    return Err(E::StaleLease);
                }
                // An unresolved wait could not have permitted retirement of any eligible fact.
                if self.signals.records().any(|s| {
                    matches_signal(&r.spec, s.envelope(), s.sequence())
                        && s.retirement().is_some_and(|c| c.wal_sequence > r.sequence)
                }) {
                    return Err(E::InvalidSignal);
                }
            }
            index.insert(r.clone());
        }
        for run in self.run_instances() {
            if run.state() == RunState::Awaiting && index.active(run.id()).is_none() {
                return Err(E::InvalidState);
            }
        }
        for (run, id) in pending {
            if index.pending.insert(*run, *id).is_some() {
                return Err(E::InvalidIdentity);
            }
            let w = index.get(*id).ok_or(E::NotFound)?;
            let res = w.resolution.as_ref().ok_or(E::InvalidState)?;
            let resumes = match res.kind {
                WaitResolutionKind::Signal(_) | WaitResolutionKind::Control(_) => true,
                WaitResolutionKind::Deadline => w
                    .spec
                    .deadline()
                    .is_some_and(|d| d.policy == WaitTimeoutPolicy::ResumeWithTimeout),
                _ => false,
            };
            if !resumes || w.run_id != *run || self.get_run_state(run) != Some(&RunState::Ready) {
                return Err(E::InvalidState);
            }
        }
        // No accepted-start/consumption exists in AQ-06: every successful wake remains pending
        // unless a subsequent cancellation durably removed it.
        for w in index.records() {
            if let Some(res) = &w.resolution {
                let resumes = matches!(
                    res.kind,
                    WaitResolutionKind::Signal(_) | WaitResolutionKind::Control(_)
                ) || matches!(res.kind, WaitResolutionKind::Deadline)
                    && w.spec
                        .deadline()
                        .is_some_and(|d| d.policy == WaitTimeoutPolicy::ResumeWithTimeout);
                if resumes
                    && self.get_run_state(&w.run_id) != Some(&RunState::Canceled)
                    && index.pending_wait(w.run_id) != Some(w.spec.wait_id())
                {
                    return Err(E::InvalidState);
                }
            }
        }
        let mut claims = BTreeMap::new();
        let mut used = BTreeSet::new();
        for (run, key) in keys {
            let ri = self.get_run_instance(run).ok_or(E::NotFound)?;
            if ri.state().is_terminal()
                || claims.insert(*run, key.clone()).is_some()
                || !used.insert(key.clone())
                || self.get_task(&ri.task_id()).ok_or(E::NotFound)?.constraints().concurrency_key()
                    != Some(key.as_str())
            {
                return Err(E::ConflictingKeyOwnership);
            }
        }
        for run in self.run_instances() {
            let c = self.get_task(&run.task_id()).ok_or(E::NotFound)?.constraints();
            let must_hold = run.state() == RunState::Running
                || (run.state() == RunState::Awaiting || index.pending_wait(run.id()).is_some())
                    && c.concurrency_key_wait_policy()
                        == ConcurrencyKeyWaitPolicy::HoldWhileAwaiting;
            if must_hold && c.concurrency_key().is_some() && !claims.contains_key(&run.id()) {
                return Err(E::ConflictingKeyOwnership);
            }
        }
        for c in cancels {
            self.validate_cancel(c)?;
            if c.sequence == 0 || c.sequence > self.latest_sequence {
                return Err(E::InvalidIdentity);
            }
        }
        self.waits = index;
        self.key_reservations = claims;
        self.cancellations = cancels.to_vec();
        let ids: Vec<_> = self.waits.active.values().copied().collect();
        for id in ids {
            self.refresh_wait_candidate(id);
        }
        Ok(())
    }
}
fn matches_signal(w: &WaitSpec, e: &SignalEnvelope, s: SignalSequence) -> bool {
    let f = w.filter();
    s > after(w)
        && f.tenant_id == e.tenant_id
        && f.namespace == e.namespace
        && f.kind == e.kind
        && f.correlation_id.as_ref().is_none_or(|c| Some(c) == e.correlation_id.as_ref())
        && f.source_ref.as_ref().is_none_or(|v| Some(v) == e.source_ref.as_ref())
}
