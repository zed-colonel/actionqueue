//! Durable assignment of immutable wake input at the accepted-start boundary.
use actionqueue_core::{continuation::*, ids::*, mutation::*, run::RunState};

use super::reducer::{ReplayReducer, ReplayReducerError};
use crate::{
    mutation::wait::WaitPreparation,
    wal::event::{WalEvent, WalEventType},
};

/// Schema-2 accepted start. No checkpoint bytes or signal envelopes are copied here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcceptedStart {
    pub sequence: u64,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub timestamp: u64,
    pub fence: LeaseFence,
    pub assignment: Option<ResumeAssignment>,
}
/// Schema-2 closure with structural recovery attribution.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AttemptClosure {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub timestamp: u64,
    pub result: AttemptResultKind,
    pub error: Option<String>,
    pub output: Option<Vec<u8>>,
    pub origin: AttemptFinishOrigin,
}
/// Administrative wake retains only a checkpoint reference to existing history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AdministrativeWake {
    pub run_id: RunId,
    pub context_id: ResumeContextId,
    pub checkpoint_id: Option<CheckpointId>,
    pub timestamp: u64,
}
impl ReplayReducer {
    pub fn pending_resume(&self, run: RunId) -> Option<ResumeContext> {
        self.next_resume_assignment(run).map(|a| self.resume_context(run, a.context_id))
    }
    pub fn attempt_resume(&self, run: RunId, attempt: AttemptId) -> Option<ResumeContext> {
        self.get_attempt_history(&run)?
            .iter()
            .find(|a| a.attempt_id() == attempt)?
            .accepted_start
            .as_ref()?
            .assignment
            .map(|a| self.resume_context(run, a.context_id))
    }
    pub fn next_resume_assignment(&self, run: RunId) -> Option<ResumeAssignment> {
        let r = self.get_run_instance(&run)?;
        if r.state().is_terminal()
            || r.state() == RunState::Awaiting
            || r.state() == RunState::Suspended
            || r.current_attempt_id().is_some()
        {
            return None;
        }
        if let Some(id) = self.waits.pending_wait(run) {
            let w = self.waits.get(id).expect("pending wait exists");
            return Some(ResumeAssignment {
                context_id: ResumeContextId(
                    w.resolution.as_ref().expect("pending wake exists").sequence,
                ),
                previous_attempt_id: None,
                delivery: ResumeDelivery::Initial,
            });
        }
        if let Some(id) = self.administrative_pending.get(&run) {
            return Some(ResumeAssignment {
                context_id: *id,
                previous_attempt_id: None,
                delivery: ResumeDelivery::Initial,
            });
        }
        let last = self.get_attempt_history(&run)?.last()?;
        if !matches!(last.result(), Some(AttemptResultKind::Failure | AttemptResultKind::Timeout)) {
            return None;
        }
        let old = last.accepted_start.as_ref()?.assignment?;
        Some(ResumeAssignment {
            context_id: old.context_id,
            previous_attempt_id: Some(last.attempt_id()),
            delivery: if last.finish_origin == AttemptFinishOrigin::Recovery {
                ResumeDelivery::Recovery
            } else {
                ResumeDelivery::Retry
            },
        })
    }
    fn resume_context(&self, run: RunId, id: ResumeContextId) -> ResumeContext {
        if let Some(w) = self.administrative_wakes.get(&id) {
            assert_eq!(w.run_id, run, "corrupt wake ownership");
            return ResumeContext {
                context_id: id,
                checkpoint: w
                    .checkpoint_id
                    .map(|c| self.checkpoint(c).expect("retained checkpoint").checkpoint.clone()),
                wake: WakeReason::AdministrativeResume { control_context: None },
                resumed_at: w.timestamp,
            };
        }
        let w = self
            .waits
            .records()
            .find(|w| w.run_id == run && w.resolution.as_ref().is_some_and(|r| r.sequence == id.0))
            .expect("validated immutable wake source");
        self.wait_resume_context(w)
    }
    pub(crate) fn validate_accepted_start(
        &self,
        s: &AcceptedStart,
    ) -> Result<(), ReplayReducerError> {
        let bad = ReplayReducerError::CorruptedData;
        let run = self.get_run_instance(&s.run_id).ok_or_else(|| bad.clone())?;
        if s.sequence == 0
            || s.attempt_id.as_uuid().is_nil()
            || run.state() != RunState::Running
            || run.current_attempt_id().is_some()
            || self.is_task_canceled(run.task_id())
            || self.attempt_history.values().flatten().any(|a| a.attempt_id() == s.attempt_id)
            || self.next_resume_assignment(s.run_id) != s.assignment
        {
            return Err(bad);
        }
        let lease = self.get_lease_metadata(&s.run_id).ok_or_else(|| bad.clone())?;
        if lease.owner() != s.fence.owner().as_str()
            || lease.granted_at_sequence() != s.fence.granted_at_sequence()
            || lease.granted_at_sequence() >= s.sequence
            || s.timestamp < lease.acquired_at()
            || s.timestamp >= lease.expiry()
        {
            return Err(bad);
        }
        Ok(())
    }
    pub(crate) fn prepare_start(
        &self,
        c: &AttemptStartCommand,
        d: DurabilityPolicy,
    ) -> Result<WaitPreparation, WaitRejection> {
        if d != DurabilityPolicy::Immediate {
            return Err(WaitRejection::ImmediateDurabilityRequired);
        }
        if let Some(old) =
            self.attempt_history.values().flatten().find(|a| a.attempt_id() == c.attempt_id())
        {
            let s = old.accepted_start.as_ref().ok_or(WaitRejection::StaleAttempt)?;
            if s.run_id != c.run_id()
                || &s.fence != c.fence()
                || s.timestamp != c.timestamp()
                || s.assignment.map(|a| a.context_id) != c.resume()
            {
                return Err(WaitRejection::StaleAttempt);
            }
            return Ok(WaitPreparation::Noop(MutationOutcome::new(
                s.sequence,
                AppliedMutation::AlreadyStarted {
                    run_id: s.run_id,
                    attempt_id: s.attempt_id,
                    assignment: s.assignment,
                },
            )));
        }
        if self.latest_sequence.checked_add(1) != Some(c.sequence()) {
            return Err(WaitRejection::StaleSequence);
        }
        let assignment = self.next_resume_assignment(c.run_id());
        if assignment.map(|a| a.context_id) != c.resume() {
            return Err(WaitRejection::InvalidState);
        }
        let s = AcceptedStart {
            sequence: c.sequence(),
            run_id: c.run_id(),
            attempt_id: c.attempt_id(),
            timestamp: c.timestamp(),
            fence: c.fence().clone(),
            assignment,
        };
        self.validate_accepted_start(&s).map_err(|_| WaitRejection::StaleAttempt)?;
        Ok(WaitPreparation::Event(
            Box::new(WalEvent::new(s.sequence, WalEventType::AcceptedAttemptStarted { record: s })),
            AppliedMutation::AttemptStart {
                run_id: c.run_id(),
                attempt_id: c.attempt_id(),
                assignment,
            },
        ))
    }
}
impl ReplayReducer {
    pub(crate) fn record_administrative_wake(&mut self, run: RunId, sequence: u64, timestamp: u64) {
        let checkpoint_id = self.get_attempt_history(&run).and_then(|h| h.last()).and_then(|a| {
            a.disposition
                .as_ref()
                .and_then(|d| d.disposition.checkpoint())
                .map(|c| c.checkpoint_id)
                .or_else(|| {
                    self.attempt_resume(run, a.attempt_id())
                        .and_then(|c| c.checkpoint.map(|c| c.checkpoint_id))
                })
        });
        let id = ResumeContextId(sequence);
        self.administrative_wakes.insert(
            id,
            AdministrativeWake { run_id: run, context_id: id, checkpoint_id, timestamp },
        );
        self.administrative_pending.insert(run, id);
    }
    /// Whether the current Running dispatch has an accepted start, even if already finished.
    pub fn dispatch_has_started(&self, run: RunId) -> bool {
        let seq = self.dispatch_sequences.get(&run).copied().unwrap_or(0);
        self.get_attempt_history(&run)
            .and_then(|h| h.last())
            .is_some_and(|a| a.accepted_start.as_ref().is_none_or(|s| s.sequence > seq))
    }
    pub(crate) fn hydrate_resume(
        &mut self,
        wakes: &[AdministrativeWake],
        pending: &[(RunId, ResumeContextId)],
    ) -> Result<(), ReplayReducerError> {
        let bad = ReplayReducerError::CorruptedData;
        for w in wakes {
            if w.context_id.0 == 0
                || w.context_id.0 > self.latest_sequence
                || self.get_run_instance(&w.run_id).is_none()
                || self.administrative_wakes.contains_key(&w.context_id)
                || w.checkpoint_id.is_some_and(|id| {
                    self.checkpoint(id)
                        .is_none_or(|c| c.run_id != w.run_id || c.sequence >= w.context_id.0)
                })
            {
                return Err(bad);
            }
            self.administrative_wakes.insert(w.context_id, w.clone());
        }
        for (run, id) in pending {
            if self.administrative_wakes.get(id).is_none_or(|w| w.run_id != *run)
                || self.administrative_pending.insert(*run, *id).is_some()
                || self.waits.pending_wait(*run).is_some()
                || !self.get_run_state(run).is_some_and(|s| {
                    matches!(
                        s,
                        RunState::Ready
                            | RunState::Leased
                            | RunState::Running
                            | RunState::RetryWait
                    )
                })
            {
                return Err(bad);
            }
        }
        let mut ids = std::collections::HashSet::new();
        let mut sequences = std::collections::HashSet::new();
        let mut initial = std::collections::HashSet::new();
        for (run, history) in &self.attempt_history {
            let mut previous: Option<&super::reducer::AttemptHistoryEntry> = None;
            for a in history {
                if a.attempt_id().as_uuid().is_nil() || !ids.insert(a.attempt_id()) {
                    return Err(bad);
                }
                if a.finish_origin == AttemptFinishOrigin::Recovery
                    && (a.result() != Some(AttemptResultKind::Failure) || a.finished_at().is_none())
                {
                    return Err(bad);
                }
                if let Some(s) = &a.accepted_start {
                    if s.run_id != *run
                        || s.attempt_id != a.attempt_id()
                        || s.timestamp != a.started_at()
                        || s.sequence == 0
                        || s.sequence > self.latest_sequence
                        || !sequences.insert(s.sequence)
                        || s.fence.owner().as_str().is_empty()
                        || s.fence.granted_at_sequence() == 0
                        || s.fence.granted_at_sequence() >= s.sequence
                        || previous
                            .and_then(|a| a.accepted_start.as_ref())
                            .is_some_and(|p| p.sequence >= s.sequence)
                    {
                        return Err(bad);
                    }
                    if let Some(input) = s.assignment {
                        let wake = self.waits.records().find(|w| {
                            w.run_id == *run
                                && w.resolution
                                    .as_ref()
                                    .is_some_and(|r| r.sequence == input.context_id.0)
                        });
                        if let Some(w) = wake {
                            let r = w.resolution.as_ref().unwrap();
                            if matches!(
                                r.kind,
                                crate::mutation::wait::WaitResolutionKind::Canceled(_)
                            ) || matches!(
                                r.kind,
                                crate::mutation::wait::WaitResolutionKind::Deadline
                            ) && w
                                .spec
                                .deadline()
                                .is_none_or(|d| d.policy != WaitTimeoutPolicy::ResumeWithTimeout)
                            {
                                return Err(bad);
                            }
                        } else if self
                            .administrative_wakes
                            .get(&input.context_id)
                            .is_none_or(|w| w.run_id != *run)
                        {
                            return Err(bad);
                        }
                        if input.context_id.0 >= s.sequence {
                            return Err(bad);
                        }
                        match input.delivery {
                            ResumeDelivery::Initial => {
                                if input.previous_attempt_id.is_some()
                                    || !initial.insert(input.context_id)
                                {
                                    return Err(bad);
                                }
                            }
                            ResumeDelivery::Retry | ResumeDelivery::Recovery => {
                                let p = previous.ok_or_else(|| bad.clone())?;
                                if input.previous_attempt_id != Some(p.attempt_id())
                                    || p.accepted_start
                                        .as_ref()
                                        .and_then(|s| s.assignment)
                                        .map(|a| a.context_id)
                                        != Some(input.context_id)
                                    || !matches!(
                                        p.result(),
                                        Some(
                                            AttemptResultKind::Failure | AttemptResultKind::Timeout
                                        )
                                    )
                                    || (input.delivery == ResumeDelivery::Recovery)
                                        != (p.finish_origin == AttemptFinishOrigin::Recovery)
                                {
                                    return Err(bad);
                                }
                            }
                        }
                    }
                }
                previous = Some(a);
            }
        }
        for (run, id) in &self.waits.pending {
            let w = self.waits.get(*id).ok_or_else(|| bad.clone())?;
            if initial.contains(&ResumeContextId(
                w.resolution.as_ref().ok_or_else(|| bad.clone())?.sequence,
            )) || self.get_run_instance(run).is_none_or(|r| r.current_attempt_id().is_some())
            {
                return Err(bad);
            }
        }
        for (run, id) in &self.administrative_pending {
            if initial.contains(id)
                || self.get_run_instance(run).is_none_or(|r| r.current_attempt_id().is_some())
            {
                return Err(bad);
            }
        }
        for w in wakes {
            if !initial.contains(&w.context_id)
                && self.administrative_pending.get(&w.run_id) != Some(&w.context_id)
                && !self.get_run_state(&w.run_id).is_some_and(|s| s.is_terminal())
            {
                return Err(bad);
            }
        }
        Ok(())
    }
}
