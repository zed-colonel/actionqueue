#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_core::disposition::*;
use actionqueue_storage::mutation::disposition::DispositionRejection;

fn expected(a: &s::Authority, r: RunId) -> AttemptCommitExpectation {
    command(a, r, spec(WaitId::new(), None)).expected
}
fn proposed(a: &s::Authority, r: RunId, d: AttemptDisposition) -> AttemptDispositionCommitCommand {
    let e = expected(a, r);
    let plans = d
        .child_admissions()
        .iter()
        .map(|child| {
            let q = a.projection().disposition_child_request(r, e.attempt_id(), child).unwrap();
            let digest = q.digest().unwrap();
            actionqueue_engine::admission::plan_admission(q, digest, 20).unwrap()
        })
        .collect();
    AttemptDispositionCommitCommand::new(e, d, 20).with_children(plans)
}
fn compound(a: &s::Authority, r: RunId) -> AttemptDisposition {
    let child = admission_support::request(2).task_spec().clone();
    let envelope = s::envelope(1, 20);
    AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            checkpoint: Some(checkpoint(a, r, b"checkpoint")),
            wait: Some(spec(WaitId::new(), None)),
            child_admissions: vec![ChildAdmission::new(
                AdmissionKey::new("child/2").unwrap(),
                child,
                vec![],
                Default::default(),
            )
            .unwrap()],
            emitted_signals: vec![SignalProposal {
                signal_id: envelope.signal_id,
                namespace: envelope.namespace,
                kind: envelope.kind,
                correlation_id: envelope.correlation_id.unwrap(),
                payload: envelope.payload,
                payload_hash: envelope.payload_hash,
                occurred_at: None,
            }],
            consumption: vec![actionqueue_core::budget::BudgetConsumption::new(
                actionqueue_core::budget::BudgetDimension::Token,
                7,
            )],
            ..Default::default()
        },
    )
    .unwrap()
}
fn submit_disposition(
    a: &mut s::Authority,
    c: AttemptDispositionCommitCommand,
) -> Result<
    MutationOutcome,
    MutationAuthorityError<actionqueue_storage::recovery::reducer::ReplayReducerError>,
> {
    a.submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate)
}
#[test]
fn all_effects_share_one_sequence_and_survive_wal_snapshot_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let before = seq(&a);
    let d = compound(&a, r);
    let cp = d.checkpoint().unwrap().clone();
    let c = proposed(&a, r, d);
    let _ = submit_disposition(&mut a, c).unwrap();
    assert_eq!(seq(&a), before + 1);
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Awaiting));
    assert!(a.projection().get_lease(&r).is_none());
    let record = a.projection().get_attempt_history(&r).unwrap()[0].disposition.as_ref().unwrap();
    assert_eq!(record.children[0].admission.sequence(), before);
    assert_eq!(record.signals[0].wal_sequence(), before);
    assert_eq!(record.disposition.consumption()[0].amount, 7);
    assert_eq!(a.projection().checkpoint(cp.checkpoint_id).unwrap().sequence, before);
    assert_eq!(a.projection().get_run_instance(&r).unwrap().failure_attempt_count(), 0);
    parity(&a);
    assert_eq!(reconcile(&mut a, 30).unwrap(), 1);
    assert_eq!(reconcile(&mut a, 30).unwrap(), 0);
    lease(&mut a, r, 31);
    let id = start(&mut a, r, 31);
    assert_eq!(a.projection().attempt_resume(r, id).unwrap().checkpoint, Some(cp));
    let child_id =
        a.projection().get_attempt_history(&r).unwrap()[0].disposition.as_ref().unwrap().children
            [0]
        .admission
        .task_id();
    let c = MutationCommand::Cancel(CancelCommand {
        expected_sequence: seq(&a),
        target: CancelTarget::Task(child_id),
        tenant_id: None,
        control_context: None,
        timestamp: 31,
    });
    let _ = apply(&mut a, c);
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(
        &mut a,
        e,
        AttemptDisposition::complete(Some(DataRef::from_bytes(b"done".to_vec()).unwrap())),
        32,
    )
    .unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Completed));
    parity(&a);
}
#[test]
fn storage_rejects_every_stale_fence_without_subordinate_effects() {
    for case in 0..7 {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let d = compound(&a, r);
        let original = proposed(&a, r, d.clone());
        let e = expected(&a, r);
        let e = AttemptCommitExpectation::new(
            if case == 0 { e.expected_sequence() - 1 } else { e.expected_sequence() },
            r,
            if case == 1 { AttemptId::new() } else { e.attempt_id() },
            if case == 2 { RunState::Leased } else { RunState::Running },
            if case == 3 {
                LeaseFence::new("other".into(), e.expected_lease().granted_at_sequence())
            } else if case == 4 {
                LeaseFence::new("worker".into(), 1)
            } else {
                e.expected_lease().clone()
            },
        );
        if case == 6 {
            cancel(&mut a, r);
        }
        let c = AttemptDispositionCommitCommand::new(e, d, if case == 5 { 1000 } else { 20 })
            .with_children(original.children().to_vec());
        let before = a.projection().projection_digest().unwrap();
        let sequence = seq(&a);
        assert!(matches!(
            submit_disposition(&mut a, c),
            Err(MutationAuthorityError::Disposition(DispositionRejection::Stale))
        ));
        assert_eq!(seq(&a), sequence);
        assert_eq!(a.projection().projection_digest().unwrap(), before);
    }
}
#[test]
fn invalid_checkpoint_child_signal_and_quota_reject_all_effects_then_terminal_fallback() {
    for case in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let base = compound(&a, r);
        let mut parts = DispositionParts {
            checkpoint: base.checkpoint().cloned(),
            wait: base.wait().cloned(),
            child_admissions: base.child_admissions().to_vec(),
            emitted_signals: base.emitted_signals().to_vec(),
            consumption: base.consumption().to_vec(),
            ..Default::default()
        };
        if case == 0 {
            parts.checkpoint.as_mut().unwrap().created_by_attempt = AttemptId::new();
        }
        if case == 1 {
            let child = parts.child_admissions[0].clone();
            parts.child_admissions[0] = ChildAdmission::new(
                child.admission_key().clone(),
                child.task_spec().clone(),
                vec![TaskId::new()],
                Default::default(),
            )
            .unwrap();
        }
        if case == 2 {
            let mut other = parts.emitted_signals[0].clone();
            other.kind = SignalKind::new("conflict").unwrap();
            parts.emitted_signals.push(other);
        }
        if case == 3 {
            a.set_signal_limits(actionqueue_core::limits::SignalLimits {
                identities: 0,
                ..Default::default()
            });
        }
        if case == 4 {
            a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
                checkpoint_bytes: 0,
                ..Default::default()
            });
        }
        let d = AttemptDisposition::new(DispositionOutcome::Awaiting, parts).unwrap();
        let c = proposed(&a, r, d.clone());
        let before = a.projection().projection_digest().unwrap();
        let sequence = seq(&a);
        assert!(matches!(
            submit_disposition(&mut a, c),
            Err(MutationAuthorityError::Disposition(_))
        ));
        assert_eq!(a.projection().projection_digest().unwrap(), before);
        assert_eq!(seq(&a), sequence);
        let e = expected(&a, r);
        actionqueue_runtime::disposition::commit(&mut a, e, d, 20).unwrap();
        assert_eq!(seq(&a), sequence + 1);
        assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Failed));
        assert_eq!(a.projection().task_count(), 1);
        assert_eq!(a.projection().signals().statistics().retained, 0);
        assert_eq!(a.projection().waits().records().count(), 0);
        parity(&a);
    }
}
#[test]
fn suspended_checkpoint_replaces_assigned_checkpoint_and_remains_immutable() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let (r, old) = wake(&mut a);
    lease(&mut a, r, 31);
    let id = start(&mut a, r, 31);
    let cp = checkpoint(&a, r, b"replacement");
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(
        &mut a,
        e,
        AttemptDisposition::suspended(Some(cp.clone()), None),
        32,
    )
    .unwrap();
    parity(&a);
    transition(&mut a, r, RunState::Ready, 33);
    assert_eq!(a.projection().pending_resume(r).unwrap().checkpoint, Some(cp.clone()));
    assert_eq!(a.projection().attempt_resume(r, id).unwrap(), old);
    parity(&a);
    lease(&mut a, r, 34);
    start(&mut a, r, 34);
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(
        &mut a,
        e,
        AttemptDisposition::suspended(None, None),
        35,
    )
    .unwrap();
    transition(&mut a, r, RunState::Ready, 36);
    assert_eq!(a.projection().pending_resume(r).unwrap().checkpoint, Some(cp));
    parity(&a);
}
#[test]
fn yields_do_not_increase_failure_backoff_and_interruption_counts_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    for now in [20, 40, 60] {
        let e = expected(&a, r);
        let d = AttemptDisposition::awaiting(
            spec(
                WaitId::new(),
                Some(WaitDeadline { at: now, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
            ),
            None,
        );
        actionqueue_runtime::disposition::commit(&mut a, e, d, now).unwrap();
        reconcile(&mut a, now).unwrap();
        lease(&mut a, r, now + 1);
        start(&mut a, r, now + 1);
    }
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(
        &mut a,
        e,
        AttemptDisposition::retryable_failure(BoundedError::new("failure").unwrap()),
        70,
    )
    .unwrap();
    let run = a.projection().get_run_instance(&r).unwrap();
    assert_eq!(run.attempt_count(), 4);
    assert_eq!(run.failure_attempt_count(), 1);
    let backoff = actionqueue_executor_local::ExponentialBackoff::new(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_secs(1000),
    )
    .unwrap();
    assert_eq!(
        actionqueue_engine::scheduler::retry_promotion::promote_retry_wait_to_ready(
            std::slice::from_ref(run),
            80,
            &backoff
        )
        .unwrap()
        .promoted()
        .len(),
        1
    );
    parity(&a);
    transition(&mut a, r, RunState::Ready, 80);
    lease(&mut a, r, 81);
    start(&mut a, r, 81);
    recover_execution(&mut a, 90).unwrap();
    assert_eq!(a.projection().get_run_instance(&r).unwrap().failure_attempt_count(), 2);
    recover_execution(&mut a, 91).unwrap();
    assert_eq!(a.projection().get_run_instance(&r).unwrap().failure_attempt_count(), 2);
    parity(&a);
}

#[derive(Clone)]
struct YieldingHandler {
    deadline: bool,
}
impl actionqueue_executor_local::ExecutorHandler for YieldingHandler {
    fn execute(&self, ctx: actionqueue_executor_local::ExecutorContext) -> AttemptDisposition {
        if let Some(resume) = ctx.input.resume_context {
            assert!(ctx.input.causal_context.is_some());
            assert!(resume.checkpoint.is_some());
            assert_eq!(matches!(resume.wake, WakeReason::Deadline { .. }), self.deadline);
            return AttemptDisposition::complete(Some(DataRef::External(
                actionqueue_core::data_ref::ExternalDataRef {
                    scheme: DataScheme::new("blob").unwrap(),
                    locator: OpaqueRef::new("opaque/result").unwrap(),
                    hash: ContentHash::new(HashAlgorithm::Sha256, vec![7; 32]).unwrap(),
                    size_bytes: Some(9),
                    content_type: None,
                },
            )));
        }
        let envelope = s::envelope(1, 1000);
        let signals = if self.deadline {
            vec![]
        } else {
            vec![SignalProposal {
                signal_id: envelope.signal_id,
                namespace: envelope.namespace,
                kind: envelope.kind,
                correlation_id: envelope.correlation_id.unwrap(),
                payload: None,
                payload_hash: None,
                occurred_at: None,
            }]
        };
        AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(spec(
                    WaitId::new(),
                    self.deadline.then_some(WaitDeadline {
                        at: 1000,
                        policy: WaitTimeoutPolicy::ResumeWithTimeout,
                    }),
                )),
                checkpoint: Some(CheckpointRef {
                    checkpoint_id: CheckpointId::new(),
                    created_by_attempt: ctx.input.attempt_id,
                    data: DataRef::from_bytes(vec![1]).unwrap(),
                }),
                emitted_signals: signals,
                ..Default::default()
            },
        )
        .unwrap()
    }
}
#[tokio::test]
async fn normal_handler_awaits_then_receives_signal_or_deadline_and_completes_with_external_output()
{
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
    for deadline in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut boot = ActionQueueEngine::new(
            RuntimeConfig { data_dir: dir.path().into(), ..Default::default() },
            YieldingHandler { deadline },
        )
        .bootstrap_with_clock(MockClock::new(1000))
        .unwrap()
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
        let q = admission_support::request(1);
        let mut task = q.task_spec().clone();
        task.set_constraints(TaskConstraints::new(1, None, None).unwrap()).unwrap();
        let id = task.id();
        boot.submit_task(task).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), boot.run_until_idle())
            .await
            .unwrap()
            .unwrap();
        let run = boot.projection().runs_for_task(id).next().unwrap();
        assert_eq!(run.state(), RunState::Completed);
        assert_eq!(run.attempt_count(), 2);
        assert_eq!(run.failure_attempt_count(), 0);
        let output = boot.projection().get_attempt_history(&run.id()).unwrap()[1]
            .output_ref()
            .unwrap()
            .clone();
        assert!(matches!(output, DataRef::External(_)));
        let digest = boot.projection().projection_digest().unwrap();
        boot.shutdown().unwrap();
        let a = s::reopen(dir.path());
        assert_eq!(a.projection().projection_digest().unwrap(), digest);
        parity(&a);
    }
}

#[test]
#[ignore = "subprocess crash helper"]
fn disposition_crash_child() {
    let path = std::path::PathBuf::from(std::env::var("AQ_DISPOSITION_CRASH_ROOT").unwrap());
    let point = std::env::var("AQ_DISPOSITION_CRASH_POINT").unwrap();
    let mut a = s::open(&path);
    let r = running(&mut a, 1, None, false);
    let c = proposed(&a, r, compound(&a, r));
    actionqueue_storage::store::fault::pause_once(&point);
    let _ = submit_disposition(&mut a, c);
    panic!("missed crash boundary");
}
#[test]
fn killed_compound_commit_recovers_every_effect_or_none_at_each_boundary() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    for point in [
        "wal_before_append",
        "wal_partial_frame",
        "wal_before_sync",
        "authority_before_publish",
        "authority_after_publish",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "disposition_crash_child", "--ignored", "--nocapture"])
            .env("AQ_DISPOSITION_CRASH_ROOT", &path)
            .env("AQ_DISPOSITION_CRASH_POINT", point)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if line.unwrap().contains("AQ_CRASH_BOUNDARY") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        let ready = rx.recv_timeout(std::time::Duration::from_secs(15));
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        ready.unwrap();
        let a = s::reopen(&path);
        let run = a
            .projection()
            .runs_for_task(admission_support::request(1).task_spec().id())
            .next()
            .unwrap();
        let committed = run.state() == RunState::Awaiting;
        if matches!(point, "wal_before_append" | "wal_partial_frame") {
            assert!(!committed);
        } else if point != "wal_before_sync" {
            assert!(committed);
        }
        assert_eq!(a.projection().task_count(), if committed { 2 } else { 1 });
        assert_eq!(a.projection().signals().statistics().retained, usize::from(committed));
        assert_eq!(a.projection().waits().records().count(), usize::from(committed));
        let attempt = &a.projection().get_attempt_history(&run.id()).unwrap()[0];
        assert_eq!(attempt.disposition.is_some(), committed);
        if committed {
            assert_eq!(
                attempt.disposition.as_ref().unwrap().disposition.consumption()[0].amount,
                7
            );
            assert_eq!(
                a.projection().checkpoints_by_producer(run.id(), attempt.attempt_id()).count(),
                1
            );
        }
        parity(&a);
    }
}
#[test]
fn append_sync_and_publication_failures_fence_without_fallback() {
    for point in [
        "wal_before_append",
        "wal_partial_frame",
        "wal_before_sync",
        "authority_before_publish",
        "authority_after_publish",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let e = expected(&a, r);
        let d = compound(&a, r);
        actionqueue_storage::store::fault::fail_once(point);
        assert!(actionqueue_runtime::disposition::commit(&mut a, e.clone(), d.clone(), 20).is_err());
        assert!(a.recovery_required());
        assert!(matches!(
            actionqueue_runtime::disposition::commit(&mut a, e, d, 20),
            Err(MutationAuthorityError::RecoveryRequired)
        ));
        drop(a);
        let a = s::reopen(dir.path());
        let history = a.projection().get_attempt_history(&r).unwrap();
        assert_eq!(history.len(), 1);
        assert_ne!(history[0].result(), Some(AttemptResultKind::Failure));
        parity(&a);
    }
}
#[test]
fn child_retry_preserves_first_producer_and_conflicting_intent_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let d = compound(&a, r);
    let child = d.child_admissions()[0].clone();
    let c = proposed(&a, r, d);
    let _ = submit_disposition(&mut a, c).unwrap();
    let old = a.projection().task_admission(child.task_spec().id()).unwrap().clone();
    reconcile(&mut a, 30).unwrap();
    lease(&mut a, r, 31);
    start(&mut a, r, 31);
    let d = AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(spec(
                WaitId::new(),
                Some(WaitDeadline { at: 32, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
            )),
            child_admissions: vec![child.clone()],
            ..Default::default()
        },
    )
    .unwrap();
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(&mut a, e, d, 32).unwrap();
    assert_eq!(a.projection().task_admission(child.task_spec().id()).unwrap(), &old);
    assert_eq!(a.projection().task_count(), 2);
    parity(&a);
    reconcile(&mut a, 33).unwrap();
    lease(&mut a, r, 34);
    start(&mut a, r, 34);
    let mut spec_changed = child.task_spec().clone();
    spec_changed.set_constraints(TaskConstraints::new(5, None, None).unwrap()).unwrap();
    let child = ChildAdmission::new(
        child.admission_key().clone(),
        spec_changed,
        vec![],
        Default::default(),
    )
    .unwrap();
    let d = AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(spec(WaitId::new(), None)),
            child_admissions: vec![child],
            ..Default::default()
        },
    )
    .unwrap();
    let e = expected(&a, r);
    actionqueue_runtime::disposition::commit(&mut a, e, d, 35).unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Failed));
    assert_eq!(a.projection().task_admission(old.task_id()).unwrap(), &old);
    parity(&a);
}
#[test]
fn sibling_forward_references_are_validated_as_one_graph() {
    for cycle in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let a_spec = admission_support::request(2).task_spec().clone();
        let b_spec = admission_support::request(3).task_spec().clone();
        let a_id = a_spec.id();
        let b_id = b_spec.id();
        let children = vec![
            ChildAdmission::new(
                AdmissionKey::new("a").unwrap(),
                a_spec,
                vec![b_id],
                Default::default(),
            )
            .unwrap(),
            ChildAdmission::new(
                AdmissionKey::new("b").unwrap(),
                b_spec,
                if cycle { vec![a_id] } else { vec![] },
                Default::default(),
            )
            .unwrap(),
        ];
        let d = AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(spec(WaitId::new(), None)),
                child_admissions: children,
                ..Default::default()
            },
        )
        .unwrap();
        let c = proposed(&a, r, d);
        let sequence = seq(&a);
        let result = submit_disposition(&mut a, c);
        assert_eq!(result.is_err(), cycle, "{result:?}");
        assert_eq!(a.projection().task_count(), if cycle { 1 } else { 3 });
        assert_eq!(seq(&a), sequence + u64::from(!cycle));
        parity(&a);
    }
}
#[test]
fn exact_encoded_record_limit_and_cumulative_signal_quota_are_enforced() {
    #[derive(Default)]
    struct MemoryWriter;
    impl actionqueue_storage::wal::writer::WalWriter for MemoryWriter {
        fn append(
            &mut self,
            _: &actionqueue_storage::wal::event::WalEvent,
        ) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
            Ok(())
        }
        fn flush(&mut self) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
            Ok(())
        }
        fn close(self) -> Result<(), actionqueue_storage::wal::writer::WalWriterError> {
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let c = proposed(&a, r, compound(&a, r));
    let original = a.projection().clone();
    let sequence = seq(&a);
    let _ = submit_disposition(&mut a, c.clone()).unwrap();
    let record =
        (**a.projection().get_attempt_history(&r).unwrap()[0].disposition.as_ref().unwrap())
            .clone();
    let size =
        actionqueue_storage::wal::codec::encode(&actionqueue_storage::wal::event::WalEvent::new(
            sequence,
            actionqueue_storage::wal::event::WalEventType::AttemptDispositionCommitted { record },
        ))
        .unwrap()
        .len();
    for limit in [size, size - 1] {
        let mut probe = actionqueue_storage::mutation::StorageMutationAuthority::new(
            MemoryWriter,
            original.clone(),
        )
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
        probe.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
            disposition_bytes: limit,
            ..Default::default()
        });
        let result = probe.submit_command(
            MutationCommand::AttemptDispositionCommit(c.clone()),
            DurabilityPolicy::Immediate,
        );
        assert_eq!(result.is_ok(), limit == size);
        assert_eq!(probe.projection().latest_sequence(), sequence - u64::from(limit < size));
    }
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let base = compound(&a, r);
    let mut signals = base.emitted_signals().to_vec();
    let mut second = signals[0].clone();
    second.signal_id = SignalId::new("second").unwrap();
    signals.push(second);
    let d = AttemptDisposition::new(
        DispositionOutcome::Complete,
        DispositionParts { emitted_signals: signals, ..Default::default() },
    )
    .unwrap();
    a.set_signal_limits(actionqueue_core::limits::SignalLimits {
        identities: 1,
        ..Default::default()
    });
    let c = proposed(&a, r, d);
    let before = seq(&a);
    assert!(submit_disposition(&mut a, c).is_err());
    assert_eq!(seq(&a), before);
    assert_eq!(a.projection().signals().statistics().retained, 0);
}

#[derive(Clone)]
struct ExpiryClock(std::sync::Arc<std::sync::atomic::AtomicU64>);
impl actionqueue_core::time::clock::Clock for ExpiryClock {
    fn now(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
struct ExpiryHandler(tokio::sync::mpsc::UnboundedSender<()>);
impl actionqueue_executor_local::handler::ExecutorHandler for ExpiryHandler {
    fn execute(
        &self,
        _: actionqueue_executor_local::handler::ExecutorContext,
    ) -> AttemptDisposition {
        let disposition =
            AttemptDisposition::complete(Some(DataRef::from_bytes(vec![42]).unwrap()));
        self.0.send(()).unwrap();
        disposition
    }
}

// F-009: a delayed scheduler must retire expired ownership even when its handler has
// returned. Both retry allowance and a shared concurrency key must remain usable.
#[tokio::test]
async fn delayed_tick_recovers_expired_execution_and_advances_queued_task() {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
    };

    use actionqueue_runtime::{
        config::{BackoffStrategyConfig, RuntimeConfig},
        engine::ActionQueueEngine,
    };
    for max_attempts in [1, 2] {
        for delayed_time in [1003, 1009] {
            let dir = tempfile::tempdir().unwrap();
            let clock = ExpiryClock(Arc::new(AtomicU64::new(1000)));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let mut boot = ActionQueueEngine::new(
                RuntimeConfig {
                    data_dir: dir.path().into(),
                    dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
                    lease_timeout_secs: 3,
                    backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
                    ..Default::default()
                },
                ExpiryHandler(tx),
            )
            .bootstrap_with_clock(clock.clone())
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
            let mut first = admission_support::request(1).task_spec().clone();
            first
                .set_constraints(
                    TaskConstraints::new(max_attempts, None, Some("expiry-key".into())).unwrap(),
                )
                .unwrap();
            let first_id = first.id();
            boot.submit_task(first).unwrap();
            assert_eq!(boot.tick().await.unwrap().dispatched, 1);
            tokio::time::timeout(Duration::from_secs(10), rx.recv()).await.unwrap().unwrap();
            let mut second = admission_support::request(2).task_spec().clone();
            second
                .set_constraints(TaskConstraints::new(1, None, Some("expiry-key".into())).unwrap())
                .unwrap();
            let second_id = second.id();
            boot.submit_task(second).unwrap();
            clock.0.store(delayed_time, Ordering::SeqCst);
            let _ = boot.tick().await.unwrap();
            let _ = tokio::time::timeout(Duration::from_secs(10), boot.run_until_idle())
                .await
                .unwrap()
                .unwrap();
            let run = boot.projection().runs_for_task(first_id).next().unwrap();
            assert_eq!(
                run.state(),
                if max_attempts == 1 { RunState::Failed } else { RunState::Completed }
            );
            assert_eq!(run.attempt_count(), max_attempts);
            assert_eq!(run.failure_attempt_count(), 1);
            assert!(boot.projection().get_lease(&run.id()).is_none());
            let history = boot.projection().get_attempt_history(&run.id()).unwrap();
            assert_eq!(history[0].result(), Some(AttemptResultKind::Failure));
            assert_eq!(history[0].finished_at(), Some(delayed_time));
            assert_eq!(
                history[0].finish_origin(),
                actionqueue_core::continuation::AttemptFinishOrigin::Recovery
            );
            assert!(history[0].output_ref().is_none());
            assert!(history[0].disposition.is_none(), "stale effects must never commit");
            let second = boot.projection().runs_for_task(second_id).next().unwrap();
            assert_eq!(second.state(), RunState::Completed);
            assert_eq!(second.attempt_count(), 1);
            assert_eq!(second.failure_attempt_count(), 0);
            assert_eq!(boot.projection().key_reservations().count(), 0);
            let digest = boot.projection().projection_digest().unwrap();
            boot.shutdown().unwrap();
            let a = s::reopen(dir.path());
            assert_eq!(a.projection().projection_digest().unwrap(), digest);
            parity(&a);
        }
    }
}

#[test]
fn active_wait_quota_rejects_entire_compound_disposition() {
    for tenant_limit in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let first = running(&mut a, 10, None, false);
        establish_wait(&mut a, first, spec(WaitId::new(), None));
        let r = running(&mut a, 1, None, false);
        a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
            active_waits: if tenant_limit { 10 } else { 1 },
            active_waits_per_tenant: if tenant_limit { 1 } else { 10 },
            ..Default::default()
        });
        let d = compound(&a, r);
        let cp = d.checkpoint().unwrap().checkpoint_id;
        let c = proposed(&a, r, d);
        let before = a.projection().projection_digest().unwrap();
        assert!(matches!(
            submit_disposition(&mut a, c),
            Err(MutationAuthorityError::Disposition(DispositionRejection::WaitCapacity))
        ));
        assert_eq!(before, a.projection().projection_digest().unwrap());
        assert!(a.projection().checkpoint(cp).is_none());
        assert!(a.projection().get_task(&admission_support::id(2)).is_none());
        assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Running));
        parity(&a);
        cancel(&mut a, first);
        let c = proposed(&a, r, compound(&a, r));
        let _ = submit_disposition(&mut a, c).unwrap();
        a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
            active_waits: 0,
            active_waits_per_tenant: 0,
            ..Default::default()
        });
        parity(&a);
        drop(a);
        let a = s::reopen(dir.path());
        assert_eq!(a.projection().waits().active_count(), 1);
        assert_eq!(a.projection().waits().active_count_for_tenant(None), 1);
    }
}

struct CapacityHandler {
    reject: RunId,
    queued: RunId,
    tenant: Option<TenantId>,
}
impl actionqueue_executor_local::ExecutorHandler for CapacityHandler {
    fn execute(&self, ctx: actionqueue_executor_local::ExecutorContext) -> AttemptDisposition {
        if ctx.input.run_id == self.queued {
            return AttemptDisposition::complete(None);
        }
        let mut filter = s::filter();
        filter.tenant_id = self.tenant;
        let wait = WaitSpec::new(
            WaitId::new(),
            filter,
            WaitMatchPolicy::FirstMatch,
            SignalEligibility::After(SignalSequence::new(0)),
            None,
        )
        .unwrap();
        if ctx.input.run_id != self.reject {
            return AttemptDisposition::awaiting(wait, None);
        }
        let mut child = admission_support::request(99).task_spec().clone();
        if let Some(tenant) = self.tenant {
            child = child.with_tenant(tenant);
        }
        // Every subordinate effect must be discarded when capacity rejects the wait.
        AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(wait),
                checkpoint: Some(CheckpointRef {
                    checkpoint_id: CheckpointId::new(),
                    created_by_attempt: ctx.input.attempt_id,
                    data: DataRef::from_bytes(vec![42]).unwrap(),
                }),
                child_admissions: vec![ChildAdmission::new(
                    AdmissionKey::new("capacity-child").unwrap(),
                    child,
                    vec![],
                    Default::default(),
                )
                .unwrap()],
                emitted_signals: vec![SignalProposal {
                    signal_id: SignalId::new("capacity-signal").unwrap(),
                    namespace: SignalNamespace::new("unrelated").unwrap(),
                    kind: SignalKind::new("complete").unwrap(),
                    correlation_id: CorrelationId::new("capacity").unwrap(),
                    payload: None,
                    payload_hash: None,
                    occurred_at: None,
                }],
                consumption: vec![actionqueue_core::budget::BudgetConsumption::new(
                    actionqueue_core::budget::BudgetDimension::Token,
                    7,
                )],
                ..Default::default()
            },
        )
        .unwrap()
    }
}

async fn local_wait_capacity_case(occupied: bool, tenant_limit: bool, tenant: Option<TenantId>) {
    use std::{sync::atomic::Ordering, time::Duration};

    use actionqueue_core::limits::ContinuationLimits;
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};

    let dir = tempfile::tempdir().unwrap();
    let mut a = if tenant.is_some() {
        #[cfg(feature = "platform")]
        {
            s::open_platform(dir.path())
        }
        #[cfg(not(feature = "platform"))]
        {
            unreachable!()
        }
    } else {
        s::open(dir.path())
    };
    #[cfg(feature = "platform")]
    if let Some(tenant) = tenant {
        commit!(
            &mut a,
            MutationCommand::TenantCreate(TenantCreateCommand::new(
                seq(&a),
                actionqueue_core::platform::TenantRegistration::new(tenant, "capacity-tenant"),
                1
            ))
        );
    }
    // Separate admission times force the accepted wait, rejected wait and queued
    // completion to execute in that order, through the sole local worker slot.
    for n in if occupied { 1..=3 } else { 2..=3 } {
        let q = admission_support::request(n);
        let mut task = TaskSpec::new(
            q.task_spec().id(),
            q.task_spec().task_payload().clone(),
            actionqueue_core::task::run_policy::RunPolicy::Once,
            q.task_spec().constraints().clone(),
            q.task_spec().metadata().clone(),
        )
        .unwrap();
        let mut constraints =
            TaskConstraints::new(3, None, (n != 1).then(|| "capacity-key".into())).unwrap();
        constraints.set_concurrency_key_wait_policy(ConcurrencyKeyWaitPolicy::HoldWhileAwaiting);
        task.set_constraints(constraints).unwrap();
        if let Some(tenant) = tenant {
            task = task.with_tenant(tenant);
        }
        let _ = admission_support::ensure(&mut a, admission_support::with_spec(&q, task), 10 + n)
            .unwrap();
    }
    let reject = a.projection().runs_for_task(admission_support::id(2)).next().unwrap().id();
    let queued = a.projection().runs_for_task(admission_support::id(3)).next().unwrap().id();
    drop(a);
    let clock = ExpiryClock(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1000)));
    let config = RuntimeConfig {
        data_dir: dir.path().into(),
        dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
        lease_timeout_secs: 3,
        continuation_limits: ContinuationLimits {
            active_waits: if tenant_limit { 10 } else { usize::from(occupied) },
            active_waits_per_tenant: if tenant_limit { usize::from(occupied) } else { 10 },
            disposition_bytes: RuntimeConfig::minimum_disposition_bytes(),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut boot =
        ActionQueueEngine::new(config.clone(), CapacityHandler { reject, queued, tenant })
            .bootstrap_with_clock(clock.clone())
            .unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(10), boot.run_until_idle())
        .await
        .expect("completed workers must release their slots")
        .expect("wait capacity must close a local attempt durably");
    assert_eq!(summary.total_dispatched, if occupied { 3 } else { 2 });
    assert_eq!(boot.projection().get_run_state(&reject), Some(&RunState::Failed));
    assert_eq!(boot.projection().get_run_state(&queued), Some(&RunState::Completed));
    let run = boot.projection().get_run_instance(&reject).unwrap();
    assert_eq!(run.attempt_count(), 1);
    assert_eq!(run.failure_attempt_count(), 1);
    assert!(boot.projection().get_lease_metadata(&reject).is_none());
    assert_eq!(boot.projection().key_reservations().count(), 0);
    let attempt = &boot.projection().get_attempt_history(&reject).unwrap()[0];
    assert_eq!(attempt.result(), Some(AttemptResultKind::Failure));
    assert_eq!(attempt.finished_at(), Some(1000));
    assert_eq!(attempt.error(), Some(actionqueue_runtime::config::WAIT_CAPACITY));
    assert_eq!(attempt.finish_origin(), AttemptFinishOrigin::Executor);
    let failure = attempt.disposition.as_ref().unwrap();
    assert!(matches!(failure.disposition.outcome(), DispositionOutcome::TerminalFailure { error }
        if error.code.as_str() == actionqueue_runtime::config::WAIT_CAPACITY));
    assert!(failure.disposition.checkpoint().is_none());
    assert!(failure.disposition.wait().is_none());
    assert!(failure.disposition.child_admissions().is_empty());
    assert!(failure.disposition.emitted_signals().is_empty());
    assert!(failure.disposition.consumption().is_empty());
    assert!(failure.children.is_empty());
    assert!(failure.signals.is_empty());
    assert_eq!(boot.projection().checkpoints_by_producer(reject, attempt.attempt_id()).count(), 0);
    assert_eq!(boot.projection().task_count(), if occupied { 3 } else { 2 });
    assert_eq!(boot.projection().signals().statistics().retained, 0);
    assert_eq!(boot.projection().waits().records().count(), usize::from(occupied));
    assert_eq!(boot.projection().waits().active_count_for_tenant(tenant), usize::from(occupied));
    let digest = boot.projection().projection_digest().unwrap();
    for now in 1001..=1010 {
        clock.0.store(now, Ordering::SeqCst);
        assert_eq!(boot.tick().await.unwrap().dispatched, 0);
        assert_eq!(
            boot.projection().projection_digest().unwrap(),
            digest,
            "no abandoned heartbeat"
        );
    }
    boot.shutdown().unwrap();
    let a = s::reopen(dir.path());
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    parity(&a); // Independent WAL-only and snapshot recovery preserve the closure.
    drop(a);
    let mut boot = ActionQueueEngine::new(config, CapacityHandler { reject, queued, tenant })
        .bootstrap_with_clock(clock)
        .unwrap();
    assert_eq!(boot.run_until_idle().await.unwrap().total_dispatched, 0);
    assert_eq!(boot.projection().projection_digest().unwrap(), digest);
    boot.shutdown().unwrap();
}

// F-022: zero capacity and normal saturation must not strand a finished worker,
// retain its concurrency key, or leak any effect of the rejected proposal.
#[tokio::test]
async fn local_wait_capacity_closes_worker_and_advances_queue() {
    for occupied in [false, true] {
        for tenant_limit in [false, true] {
            local_wait_capacity_case(occupied, tenant_limit, None).await;
        }
    }
}

#[cfg(feature = "platform")]
#[tokio::test]
async fn local_wait_capacity_closes_tenant_worker_and_advances_queue() {
    for occupied in [false, true] {
        for tenant_limit in [false, true] {
            local_wait_capacity_case(occupied, tenant_limit, Some(TenantId::new())).await;
        }
    }
}
