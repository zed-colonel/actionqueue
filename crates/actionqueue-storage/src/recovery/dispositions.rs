//! Atomic application and replay validation of complete dispositions.
use super::reducer::{ReplayReducer, ReplayReducerError};
use crate::mutation::disposition::DispositionRecord;
use actionqueue_core::{
    admission::AdmissionPlan, disposition::DispositionOutcome, limits::*, mutation::*,
    run::RunState,
};
impl ReplayReducer {
    pub(crate) fn apply_disposition(
        &mut self,
        r: &DispositionRecord,
    ) -> Result<(), ReplayReducerError> {
        let bad = || ReplayReducerError::CorruptedData;
        let children = r
            .children
            .iter()
            .map(|c| {
                AdmissionPlan::new(
                    c.admission.request().clone(),
                    c.runs.clone(),
                    c.admission.digest().clone(),
                )
                .map_err(|_| bad())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let c = AttemptDispositionCommitCommand::new(
            AttemptCommitExpectation::new(
                r.sequence,
                r.run_id,
                r.attempt_id,
                RunState::Running,
                r.fence.clone(),
            ),
            r.disposition.clone(),
            r.timestamp,
        )
        .with_children(children);
        let prepared = self
            .prepare_disposition(
                &c,
                AdmissionLimits::default(),
                ContinuationLimits {
                    output_bytes: MAX_INLINE_DATA_BYTES,
                    checkpoint_bytes: MAX_INLINE_DATA_BYTES,
                    disposition_bytes: MAX_ADMISSION_RECORD_BYTES,
                },
                SignalLimits {
                    identities: usize::MAX,
                    bytes: usize::MAX,
                    record_bytes: MAX_SIGNAL_RECORD_BYTES,
                    inline_bytes: MAX_INLINE_DATA_BYTES,
                    ..Default::default()
                },
            )
            .map_err(|_| bad())?;
        if &prepared != r {
            return Err(bad());
        }
        let mut p = self.clone();
        for child in &r.children {
            if child.admission.sequence() != r.sequence {
                continue;
            }
            p.apply_task_created(
                child.admission.request().task_spec(),
                child.admission.timestamp(),
            )?;
            for run in &child.runs {
                p.apply_run_created(run)?;
            }
            if !child.admission.request().dependencies().is_empty() {
                p.apply_dependency_declared(
                    child.admission.task_id(),
                    child.admission.request().dependencies(),
                );
                p.dependency_declared_at.insert(child.admission.task_id(), r.timestamp);
            }
            p.insert_admission(child.admission.clone());
        }
        for signal in &r.signals {
            if p.signals.by_sequence(signal.sequence()).is_none() {
                p.signals.insert(signal.clone()).map_err(ReplayReducerError::Signal)?;
                p.signal_arrived(signal.envelope());
            }
        }
        if let Some(w) = r.wait_record() {
            p.apply_wait_established(&w)?;
        } else {
            let outcome = match r.disposition.outcome() {
                DispositionOutcome::Complete => AttemptOutcome::success(),
                DispositionOutcome::Suspended { .. } => AttemptOutcome::suspended(),
                DispositionOutcome::Timeout { error } => {
                    AttemptOutcome::timeout(error.message.as_str())
                }
                DispositionOutcome::RetryableFailure { error }
                | DispositionOutcome::TerminalFailure { error } => {
                    AttemptOutcome::failure(error.message.as_str())
                }
                DispositionOutcome::Awaiting => return Err(bad()),
            };
            p.apply_attempt_finished(&r.run_id, &r.attempt_id, outcome, r.timestamp)?;
            p.leases.remove(&r.run_id);
            p.lease_metadata.remove(&r.run_id);
            p.apply_run_state_changed(&r.run_id, &RunState::Running, &r.target_state, r.timestamp)?;
            if let Some(c) = r.disposition.checkpoint() {
                p.index_produced_checkpoint(r.run_id, r.attempt_id, r.sequence, c)?;
            }
        }
        if p.get_run_instance(&r.run_id).unwrap().failure_attempt_count() != r.failure_attempt_count
        {
            return Err(bad());
        }
        let task = p.get_run_instance(&r.run_id).unwrap().task_id();
        for c in r.disposition.consumption() {
            p.apply_budget_consumed(task, c.dimension, c.amount, r.timestamp);
        }
        p.attempt_history.get_mut(&r.run_id).unwrap().last_mut().unwrap().disposition =
            Some(Box::new(r.clone()));
        *self = p;
        Ok(())
    }
    /// Validate persisted accounting and links independently during snapshot hydration.
    pub(crate) fn validate_disposition_history(&self) -> Result<(), ReplayReducerError> {
        for run in self.run_instances() {
            let mut failures = 0u32;
            for a in self.get_attempt_history(&run.id()).into_iter().flatten() {
                if matches!(
                    a.result(),
                    Some(AttemptResultKind::Failure | AttemptResultKind::Timeout)
                ) {
                    failures = failures.checked_add(1).ok_or(ReplayReducerError::CorruptedData)?;
                }
                if let Some(r) = &a.disposition {
                    let previous = failures
                        - u32::from(matches!(
                            a.result(),
                            Some(AttemptResultKind::Failure | AttemptResultKind::Timeout)
                        ));
                    let accounting = r
                        .disposition
                        .outcome()
                        .accounting(
                            previous,
                            self.get_task(&run.task_id()).unwrap().constraints().max_attempts(),
                        )
                        .map_err(|_| ReplayReducerError::CorruptedData)?;
                    let expected_result = match r.disposition.outcome() {
                        DispositionOutcome::Complete => AttemptResultKind::Success,
                        DispositionOutcome::Awaiting => AttemptResultKind::Awaiting,
                        DispositionOutcome::Suspended { .. } => AttemptResultKind::Suspended,
                        DispositionOutcome::Timeout { .. } => AttemptResultKind::Timeout,
                        _ => AttemptResultKind::Failure,
                    };
                    if accounting.target_state != r.target_state
                        || accounting.failure_attempt_count != r.failure_attempt_count
                        || a.result() != Some(expected_result)
                        || r.children.len() != r.disposition.child_admissions().len()
                        || r.signals.len() != r.disposition.emitted_signals().len()
                        || r.disposition.output().is_some_and(|d| d.validate().is_err())
                        || crate::wal::codec::encode(&crate::wal::event::WalEvent::new(
                            r.sequence,
                            crate::wal::event::WalEventType::AttemptDispositionCommitted {
                                record: (**r).clone(),
                            },
                        ))
                        .is_err()
                    {
                        return Err(ReplayReducerError::CorruptedData);
                    }
                    for (child, proposal) in r.children.iter().zip(r.disposition.child_admissions())
                    {
                        if self
                            .disposition_child_request(r.run_id, r.attempt_id, proposal)
                            .ok()
                            .as_ref()
                            != Some(child.admission.request())
                        {
                            return Err(ReplayReducerError::CorruptedData);
                        }
                    }
                    if let Some(w) = r.wait_record() {
                        let mut actual = self
                            .waits
                            .get(w.spec.wait_id())
                            .ok_or(ReplayReducerError::CorruptedData)?
                            .clone();
                        actual.resolution = None;
                        if actual != w {
                            return Err(ReplayReducerError::CorruptedData);
                        }
                    }
                    if r.run_id != run.id()
                        || r.attempt_id != a.attempt_id()
                        || a.finished_at() != Some(r.timestamp)
                        || r.failure_attempt_count != failures
                        || r.sequence > self.latest_sequence()
                        || a.accepted_start()
                            .is_none_or(|s| s.fence != r.fence || s.sequence >= r.sequence)
                    {
                        return Err(ReplayReducerError::CorruptedData);
                    }
                    for c in &r.children {
                        if self.task_admission(c.admission.task_id()) != Some(&c.admission) {
                            return Err(ReplayReducerError::CorruptedData);
                        }
                    }
                    for s in &r.signals {
                        if self
                            .signals
                            .by_sequence(s.sequence())
                            .is_none_or(|v| v.digest() != s.digest())
                        {
                            return Err(ReplayReducerError::CorruptedData);
                        }
                    }
                    if let Some(cp) = r.disposition.checkpoint() {
                        if self
                            .checkpoint(cp.checkpoint_id)
                            .is_none_or(|v| &v.checkpoint != cp || v.attempt_id != r.attempt_id)
                        {
                            return Err(ReplayReducerError::CorruptedData);
                        }
                    }
                }
            }
            if failures != run.failure_attempt_count() {
                return Err(ReplayReducerError::CorruptedData);
            }
        }
        Ok(())
    }
}
