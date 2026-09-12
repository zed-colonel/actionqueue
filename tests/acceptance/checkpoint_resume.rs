#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
#[test]
fn immutable_checkpoints_assignment_retry_and_supersession() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    let cp = context.checkpoint.clone().unwrap();
    parity(&a);
    lease(&mut a, r, 31);
    assert_eq!(a.projection().pending_resume(r), Some(context.clone()));
    parity(&a);
    let c = start_command(&a, r, 32);
    let id = c.attempt_id();
    let _ = apply(&mut a, MutationCommand::AttemptStart(c.clone()));
    let digest = a.projection().projection_digest().unwrap();
    let ack = apply(&mut a, MutationCommand::AttemptStart(c));
    assert!(matches!(ack.applied(), AppliedMutation::AlreadyStarted { .. }));
    assert_eq!(digest, a.projection().projection_digest().unwrap());
    assert!(a.projection().pending_resume(r).is_none());
    let input = observe(a.projection(), r, id);
    assert_eq!(input.resume_context, Some(context.clone()));
    let task =
        a.projection().get_task(&a.projection().get_run_instance(&r).unwrap().task_id()).unwrap();
    assert_eq!(input.payload, task.task_payload().bytes());
    assert_eq!(
        input.causal_context.as_ref(),
        Some(a.projection().task_admission(task.id()).unwrap().request().causal_context())
    );
    parity(&a);
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq(&a),
            r,
            id,
            AttemptOutcome::failure("transient"),
            33
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1031, 33))
    );
    transition(&mut a, r, RunState::RetryWait, 33);
    parity(&a);
    transition(&mut a, r, RunState::Ready, 34);
    lease(&mut a, r, 35);
    let second = start(&mut a, r, 36);
    let assignment = a
        .projection()
        .get_attempt_history(&r)
        .unwrap()
        .last()
        .unwrap()
        .accepted_start()
        .unwrap()
        .assignment
        .unwrap();
    assert_eq!(
        assignment,
        ResumeAssignment {
            context_id: context.context_id,
            previous_attempt_id: Some(id),
            delivery: ResumeDelivery::Retry
        }
    );
    assert_eq!(observe(a.projection(), r, second).resume_context, Some(context.clone()));
    let mut c = command(&a, r, spec(WaitId::new(), None));
    c.timestamp = 37;
    c.checkpoint = Some(CheckpointRef { created_by_attempt: second, ..cp.clone() });
    let before = a.projection().projection_digest().unwrap();
    assert!(establish(&mut a, c.clone()).is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    c.checkpoint = None;
    establish(&mut a, c).unwrap();
    reconcile(&mut a, 38).unwrap();
    assert!(a.projection().pending_resume(r).unwrap().checkpoint.is_none());
    assert_eq!(a.projection().checkpoint(cp.checkpoint_id).unwrap().checkpoint, cp);
    assert_eq!(a.projection().attempt_resume(r, id), Some(context));
    parity(&a);
    cancel(&mut a, r);
    assert!(a.projection().pending_resume(r).is_none());
    parity(&a);
}
#[test]
fn rejected_starts_and_creation_limits_are_append_free() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    lease(&mut a, r, 31);
    let good = start_command(&a, r, 32);
    let before = a.projection().projection_digest().unwrap();
    for (fence, input, id, at) in [
        (
            LeaseFence::new("wrong".into(), good.fence().granted_at_sequence()),
            Some(context.context_id),
            AttemptId::new(),
            32,
        ),
        (good.fence().clone(), None, AttemptId::new(), 32),
        (
            good.fence().clone(),
            Some(context.context_id),
            "00000000-0000-0000-0000-000000000000".parse().unwrap(),
            32,
        ),
        (good.fence().clone(), Some(context.context_id), AttemptId::new(), 1031),
    ] {
        let c = AttemptStartCommand::new(seq(&a), r, id, at, fence, input);
        assert!(a
            .submit_command(MutationCommand::AttemptStart(c), DurabilityPolicy::Immediate)
            .is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
    assert!(a
        .submit_command(MutationCommand::AttemptStart(good.clone()), DurabilityPolicy::Deferred)
        .is_err());
    let _ = apply(&mut a, MutationCommand::AttemptStart(good));
    let mut c = command(&a, r, spec(WaitId::new(), None));
    c.timestamp = 33;
    c.checkpoint = Some(checkpoint(&a, r, &vec![9; 32769]));
    assert!(establish(&mut a, c.clone()).is_err());
    a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
        checkpoint_bytes: 65536,
        ..Default::default()
    });
    establish(&mut a, c.clone()).unwrap();
    a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
        checkpoint_bytes: 0,
        disposition_bytes: 0,
        ..Default::default()
    });
    assert!(matches!(establish(&mut a, c).unwrap(), WaitOutcome::AlreadyEstablished { .. }));
    parity(&a);
}
#[test]
fn deadline_and_control_wakes_have_exact_checkpoint_and_reason() {
    for control in [false, true] {
        let dir = resume_dir();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let id = WaitId::new();
        let mut c = command(
            &a,
            r,
            spec(id, Some(WaitDeadline { at: 25, policy: WaitTimeoutPolicy::ResumeWithTimeout })),
        );
        c.checkpoint = Some(checkpoint(&a, r, b"state"));
        establish(&mut a, c).unwrap();
        if control {
            let ctx = ControlMutationContext::new(OpaqueRef::new("operator/ref").unwrap());
            commit!(
                &mut a,
                MutationCommand::WaitResolve(WaitResolveCommand {
                    expected_sequence: seq(&a),
                    run_id: r,
                    wait_id: id,
                    tenant_id: None,
                    control_context: ctx,
                    timestamp: 30
                })
            );
        } else {
            reconcile(&mut a, 30).unwrap();
        }
        let expected = a.projection().pending_resume(r).unwrap();
        lease(&mut a, r, 31);
        let id = start(&mut a, r, 32);
        assert_eq!(observe(a.projection(), r, id).resume_context, Some(expected));
        commit!(
            &mut a,
            MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                seq(&a),
                r,
                id,
                AttemptOutcome::success(),
                33
            ))
        );
        transition(&mut a, r, RunState::Completed, 34);
        assert!(a.projection().pending_resume(r).is_none());
        parity(&a);
    }
}
#[tokio::test]
async fn runtime_delivers_original_causal_payload_and_wake_once_aq_dd_006() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let config = actionqueue_runtime::config::RuntimeConfig {
        data_dir: dir.path().into(),
        ..Default::default()
    };
    let mut boot =
        actionqueue_runtime::engine::ActionQueueEngine::new(config, Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(40))
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
    let inputs = seen.lock().unwrap();
    let resumed: Vec<_> = inputs.iter().filter(|i| i.run_id == r).collect();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].resume_context, Some(context));
    assert!(resumed[0].causal_context.is_some());
}
#[test]
fn timeout_and_administrative_resume_keep_checkpoint_lineage() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    lease(&mut a, r, 31);
    let first = start(&mut a, r, 32);
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq(&a),
            r,
            first,
            AttemptOutcome::timeout("timeout"),
            33
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1031, 33))
    );
    transition(&mut a, r, RunState::RetryWait, 34);
    transition(&mut a, r, RunState::Ready, 35);
    lease(&mut a, r, 36);
    let second = start(&mut a, r, 37);
    assert_eq!(a.projection().attempt_resume(r, second), Some(context.clone()));
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq(&a),
            r,
            second,
            AttemptOutcome::suspended_with_output(b"not a checkpoint".to_vec()),
            38
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1036, 38))
    );
    // Only recovery may finish this legacy prefix: there is no active attempt
    // left for an administrative suspension to close.
    let before = seq(&a);
    assert!(a
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                before,
                r,
                RunState::Running,
                RunState::Suspended,
                39
            )),
            DurabilityPolicy::Immediate
        )
        .is_err());
    assert_eq!(seq(&a), before);
    recover_execution(&mut a, 39).unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Suspended));
    assert!(a.projection().pending_resume(r).is_none());
    parity(&a);
    commit!(&mut a, MutationCommand::RunResume(RunResumeCommand::new(seq(&a), r, 40)));
    let next = a.projection().pending_resume(r).unwrap();
    assert_ne!(next.context_id, context.context_id);
    assert_eq!(next.checkpoint, context.checkpoint);
    assert!(matches!(next.wake, WakeReason::AdministrativeResume { .. }));
    parity(&a);
    lease(&mut a, r, 41);
    let third = start(&mut a, r, 42);
    assert_eq!(observe(a.projection(), r, third).resume_context, Some(next));
    parity(&a);
    cancel(&mut a, r);
    assert!(a.projection().pending_resume(r).is_none());
    parity(&a);
}
#[derive(Clone)]
struct ExternalFailure(Recording);
impl actionqueue_executor_local::handler::ExecutorHandler for ExternalFailure {
    fn execute(
        &self,
        c: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_executor_local::handler::AttemptDisposition {
        use actionqueue_executor_local::handler::AttemptDisposition;
        if let Some(resume) = &c.input.resume_context {
            let data = &resume.checkpoint.as_ref().unwrap().data;
            assert!(data.verify_bytes(b"wrong backend bytes").is_err());
            let n = self.0 .0.lock().unwrap().len();
            self.0 .0.lock().unwrap().push(c.input);
            if n == 0 {
                return actionqueue_core::disposition::AttemptDisposition::retryable_failure(
                    actionqueue_core::bounded::BoundedError::new("checkpoint unavailable").unwrap(),
                );
            }
        }
        AttemptDisposition::complete(None)
    }
}
#[tokio::test]
async fn external_resolution_failure_uses_normal_handler_retry() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let mut c = command(&a, r, spec(WaitId::new(), None));
    let mut cp = checkpoint(&a, r, b"state");
    cp.data = DataRef::External(actionqueue_core::data_ref::ExternalDataRef {
        scheme: DataScheme::new("blob").unwrap(),
        locator: OpaqueRef::new("SECRET-LOCATOR").unwrap(),
        hash: ContentHash::new(HashAlgorithm::Sha256, sha2::Sha256::digest(b"state").to_vec())
            .unwrap(),
        size_bytes: Some(5),
        content_type: None,
    });
    c.checkpoint = Some(cp);
    establish(&mut a, c).unwrap();
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    reconcile(&mut a, 30).unwrap();
    let context = a.projection().pending_resume(r).unwrap();
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let config = actionqueue_runtime::config::RuntimeConfig {
        data_dir: dir.path().into(),
        backoff_strategy: actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
            interval: std::time::Duration::ZERO,
        },
        ..Default::default()
    };
    let mut boot = actionqueue_runtime::engine::ActionQueueEngine::new(
        config,
        ExternalFailure(Recording(seen.clone())),
    )
    .bootstrap_with_clock(MockClock::new(40))
    .unwrap()
    .with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    });
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for i in seen.iter() {
        assert_eq!(i.resume_context, Some(context.clone()));
        assert!(!format!("{:?}", i.resume_context).contains("SECRET-LOCATOR"));
    }
    assert_ne!(seen[0].attempt_id, seen[1].attempt_id);
}
#[cfg(feature = "budget")]
#[tokio::test]
async fn exhausted_budget_retains_input_until_replenished() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    let task = a.projection().get_run_instance(&r).unwrap().task_id();
    commit!(
        &mut a,
        MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
            seq(&a),
            task,
            actionqueue_core::budget::BudgetDimension::Token,
            1,
            31
        ))
    );
    commit!(
        &mut a,
        MutationCommand::BudgetConsume(BudgetConsumeCommand::new(
            seq(&a),
            task,
            actionqueue_core::budget::BudgetDimension::Token,
            1,
            32
        ))
    );
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let config = actionqueue_runtime::config::RuntimeConfig {
        data_dir: dir.path().into(),
        ..Default::default()
    };
    let mut boot =
        actionqueue_runtime::engine::ActionQueueEngine::new(config, Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(40))
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
    let _ = boot.tick().await.unwrap();
    assert_eq!(boot.projection().pending_resume(r), Some(context.clone()));
    assert!(seen.lock().unwrap().is_empty());
    boot.replenish_budget(task, actionqueue_core::budget::BudgetDimension::Token, 10).unwrap();
    let _ = boot.run_until_idle().await.unwrap();
    let inputs = seen.lock().unwrap();
    assert_eq!(
        inputs
            .iter()
            .filter(|i| i.run_id == r && i.resume_context == Some(context.clone()))
            .count(),
        1
    );
}
#[tokio::test]
async fn occupied_concurrency_key_retains_pending_input() {
    use actionqueue_runtime::{
        config::BackoffStrategyConfig,
        dispatch::{DispatchConfig, DispatchLoop},
    };
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, Some("shared"), false);
    let mut c = command(&a, r, spec(WaitId::new(), None));
    c.checkpoint = Some(checkpoint(&a, r, b"pending"));
    establish(&mut a, c).unwrap();
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    reconcile(&mut a, 30).unwrap();
    let context = a.projection().pending_resume(r).unwrap();
    let blocker = running(&mut a, 2, Some("shared"), true);
    let wait = WaitSpec::new(
        WaitId::new(),
        s::filter(),
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(1)),
        None,
    )
    .unwrap();
    establish_wait(&mut a, blocker, wait);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let config = || {
        DispatchConfig::new(
            BackoffStrategyConfig::Fixed { interval: std::time::Duration::ZERO },
            1,
            100,
            None,
            None,
        )
    };
    let mut d =
        DispatchLoop::new(a, Recording(seen.clone()), MockClock::new(40), config()).unwrap();
    let _ = d.tick().await.unwrap();
    assert_eq!(d.projection().pending_resume(r), Some(context.clone()));
    assert!(seen.lock().unwrap().is_empty());
    let mut a = d.into_authority();
    cancel(&mut a, blocker);
    parity(&a);
    let mut d =
        DispatchLoop::new(a, Recording(seen.clone()), MockClock::new(41), config()).unwrap();
    let _ = d.run_until_idle().await.unwrap();
    assert_eq!(
        seen.lock()
            .unwrap()
            .iter()
            .filter(|i| i.run_id == r && i.resume_context == Some(context.clone()))
            .count(),
        1
    );
}
#[test]
fn pending_input_cannot_be_discarded_by_ordinary_disposition_transitions() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, input) = wake(&mut a);
    lease(&mut a, r, 31);
    for to in [RunState::Completed, RunState::Failed, RunState::Suspended] {
        let before = a.projection().projection_digest().unwrap();
        assert!(a
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq(&a),
                    r,
                    RunState::Running,
                    to,
                    32
                )),
                DurabilityPolicy::Immediate
            )
            .is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
        assert_eq!(a.projection().pending_resume(r), Some(input.clone()));
    }
}
#[test]
fn recovery_before_start_preserves_held_key_and_does_not_spend_an_attempt() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, Some("held"), true);
    establish_wait(&mut a, r, spec(WaitId::new(), None));
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    reconcile(&mut a, 30).unwrap();
    let input = a.projection().pending_resume(r).unwrap();
    lease(&mut a, r, 31);
    let count = a.projection().get_run_instance(&r).unwrap().attempt_count();
    recover_execution(&mut a, 32).unwrap();
    assert_eq!(a.projection().get_run_instance(&r).unwrap().attempt_count(), count);
    assert_eq!(a.projection().pending_resume(r), Some(input));
    parity(&a);
}
#[test]
fn corrupted_checkpoint_or_assignment_cannot_publish_a_snapshot() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, _) = wake(&mut a);
    lease(&mut a, r, 31);
    let id = start(&mut a, r, 32);
    parity(&a);
    let snapshot = build_snapshot_from_projection(a.projection(), 0).unwrap();
    let session = a.store_session().unwrap();
    let before = std::fs::read(session.snapshot_path()).unwrap();
    for case in 0..6 {
        let mut bad = snapshot.clone();
        match case {
            0 => bad.waits.clear(),
            1 => bad.waits[0].checkpoint.as_mut().unwrap().created_by_attempt = AttemptId::new(),
            2 => {
                bad.waits[0].checkpoint.as_mut().unwrap().data = DataRef::Inline(
                    InlineData::new(
                        None,
                        b"tampered".to_vec(),
                        ContentHash::new(HashAlgorithm::Sha256, vec![0; 32]).unwrap(),
                    )
                    .unwrap(),
                )
            }
            _ => {
                let a = bad
                    .runs
                    .iter_mut()
                    .flat_map(|r| r.attempts.iter_mut())
                    .find(|a| a.attempt_id == id)
                    .unwrap();
                match case {
                    3 => a.accepted_start = None,
                    4 => {
                        a.accepted_start.as_mut().unwrap().assignment.as_mut().unwrap().context_id =
                            ResumeContextId(u64::MAX)
                    }
                    _ => {
                        a.accepted_start
                            .as_mut()
                            .unwrap()
                            .assignment
                            .as_mut()
                            .unwrap()
                            .previous_attempt_id = Some(AttemptId::new())
                    }
                }
            }
        }
        let mut writer = SnapshotFsWriter::new(session).unwrap();
        assert!(writer.write(&bad).is_err());
        assert_eq!(before, std::fs::read(session.snapshot_path()).unwrap());
    }
}

#[derive(Clone)]
struct OversizedResult {
    run: RunId,
    bytes: usize,
    rejected_bytes: usize,
    failures: usize,
    suspended: bool,
    seen: Recording,
}
impl actionqueue_executor_local::handler::ExecutorHandler for OversizedResult {
    fn execute(
        &self,
        c: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_executor_local::handler::AttemptDisposition {
        use actionqueue_executor_local::handler::AttemptDisposition;
        if c.input.run_id == self.run {
            let mut seen = self.seen.0.lock().unwrap();
            let attempt_id = c.input.attempt_id;
            seen.push(c.input);
            if seen.len() <= self.failures {
                let output = Some(vec![b'x'; self.rejected_bytes]);
                return if self.suspended {
                    AttemptDisposition::suspended(
                        Some(CheckpointRef {
                            checkpoint_id: CheckpointId::new(),
                            created_by_attempt: attempt_id,
                            data: DataRef::from_bytes(output.unwrap()).unwrap(),
                        }),
                        None,
                    )
                } else {
                    actionqueue_core::disposition::AttemptDisposition::complete(
                        (output)
                            .map(|v| actionqueue_core::data_ref::DataRef::from_bytes(v).unwrap()),
                    )
                };
            }
        }
        // The exact boundary remains accepted, including a zero-byte limit.
        actionqueue_core::disposition::AttemptDisposition::complete(
            (Some(vec![0; self.bytes]))
                .map(|v| actionqueue_core::data_ref::DataRef::from_bytes(v).unwrap()),
        )
    }
}

async fn output_limit_case(
    limit: usize,
    resumed: bool,
    failures: usize,
    suspended: bool,
    record_limit: Option<usize>,
) {
    use actionqueue_core::limits::ContinuationLimits;
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let mut context = None;
    let run = if resumed {
        let run = running(&mut a, 1, Some("output-limit"), false);
        let mut c = command(&a, run, spec(WaitId::new(), None));
        c.checkpoint = Some(checkpoint(&a, run, b"preserved"));
        establish(&mut a, c).unwrap();
        let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
        reconcile(&mut a, 30).unwrap();
        context = a.projection().pending_resume(run);
        run
    } else {
        let request = admission_support::request(1);
        let mut task = request.task_spec().clone();
        task.set_constraints(TaskConstraints::new(3, None, Some("output-limit".into())).unwrap())
            .unwrap();
        let _ = admission_support::ensure(&mut a, admission_support::with_spec(&request, task), 10)
            .unwrap();
        a.projection().runs_for_task(request.task_spec().id()).next().unwrap().id()
    };
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let config = RuntimeConfig {
        data_dir: dir.path().into(),
        dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
        continuation_limits: ContinuationLimits {
            output_bytes: limit,
            checkpoint_bytes: limit,
            disposition_bytes: record_limit
                .unwrap_or(actionqueue_core::limits::MAX_ADMISSION_RECORD_BYTES),
            ..Default::default()
        },
        backoff_strategy: actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
            interval: std::time::Duration::ZERO,
        },
        ..Default::default()
    };
    let bytes = if record_limit.is_some() { 0 } else { limit };
    let rejected_bytes = if record_limit.is_some() { 1024 } else { limit + 1 };
    let handler = OversizedResult {
        run,
        bytes,
        rejected_bytes,
        failures,
        suspended,
        seen: Recording(seen.clone()),
    };
    let mut boot = ActionQueueEngine::new(config, handler)
        .bootstrap_with_clock(MockClock::new(40))
        .unwrap()
        .with_host(actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        });
    let _ = boot.run_until_idle().await.expect("oversized result must close normally");
    let expected = RunState::Failed;
    assert_eq!(boot.projection().get_run_state(&run), Some(&expected));
    assert!(boot.projection().get_lease_metadata(&run).is_none());
    assert!(boot.projection().pending_resume(run).is_none());
    {
        let inputs = seen.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        for (n, input) in inputs.iter().enumerate() {
            assert_eq!(input.resume_context, context);
            let history = boot.projection().get_attempt_history(&run).unwrap();
            let attempt = history.iter().find(|h| h.attempt_id() == input.attempt_id).unwrap();
            assert!(attempt.finished_at().is_some());
            if n < failures {
                assert_eq!(attempt.result(), Some(AttemptResultKind::Failure));
                assert_eq!(attempt.error(), Some("invalid handler disposition"));
                assert!(attempt.output().is_none());
            }
            if let Some(context) = &context {
                let assignment = attempt.accepted_start().unwrap().assignment.unwrap();
                assert_eq!(assignment.context_id, context.context_id);
                assert_eq!(
                    assignment.delivery,
                    if n == 0 { ResumeDelivery::Initial } else { ResumeDelivery::Retry }
                );
                assert_eq!(
                    assignment.previous_attempt_id,
                    n.checked_sub(1).map(|i| inputs[i].attempt_id)
                );
            }
        }
    }
    // Reuse the same concurrency key and the sole worker slot after rejection.
    let mut next = admission_support::request(2).task_spec().clone();
    next.set_constraints(TaskConstraints::new(1, None, Some("output-limit".into())).unwrap())
        .unwrap();
    let next_id = next.id();
    boot.submit_task(next).unwrap();
    let _ = boot.run_until_idle().await.expect("queue must continue after rejection");
    let next_run = boot.projection().runs_for_task(next_id).next().unwrap();
    assert_eq!(next_run.state(), RunState::Completed);
    assert_eq!(
        boot.projection().get_attempt_history(&next_run.id()).unwrap()[0]
            .output()
            .map_or(0, <[u8]>::len),
        bytes
    );
    let digest = boot.projection().projection_digest().unwrap();
    boot.shutdown().unwrap();
    let a = s::open(dir.path());
    assert_eq!(a.projection().projection_digest().unwrap(), digest);
    parity(&a);
}

#[tokio::test]
async fn oversized_success_terminally_closes_attempt_and_preserves_resume_lineage() {
    for limit in [actionqueue_core::limits::MAX_INLINE_DATA_BYTES - 1, 8, 0] {
        for resumed in [false, true] {
            for failures in [1, usize::MAX] {
                output_limit_case(limit, resumed, failures, false, None).await;
            }
        }
    }
}

#[tokio::test]
async fn oversized_suspension_closes_as_failure_instead_of_stranding_run() {
    output_limit_case(8, true, usize::MAX, true, None).await;
}

#[tokio::test]
async fn total_record_limit_closes_fresh_and_resumed_results_and_releases_ownership() {
    use actionqueue_runtime::config::RuntimeConfig;
    let minimum = RuntimeConfig::minimum_disposition_bytes();
    for record_limit in [minimum, minimum + 1] {
        for resumed in [false, true] {
            for failures in [1, usize::MAX] {
                for suspended in [false, true] {
                    // The output quota admits all 1,024 bytes. Only the framed
                    // record quota rejects the worker result.
                    output_limit_case(1024, resumed, failures, suspended, Some(record_limit)).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn unsafe_record_limits_reject_before_bootstrap_and_minimum_recovers_active_attempts() {
    use actionqueue_core::limits::ContinuationLimits;
    use actionqueue_runtime::{
        config::{ConfigError, RuntimeConfig},
        engine::{ActionQueueEngine, BootstrapError},
    };
    let minimum = RuntimeConfig::minimum_disposition_bytes();
    for resumed in [false, true] {
        let dir = resume_dir();
        let mut a = s::open(dir.path());
        let run = running(&mut a, 1, Some("record-recovery"), false);
        let context = if resumed {
            let mut c = command(&a, run, spec(WaitId::new(), None));
            c.checkpoint = Some(checkpoint(&a, run, b"preserved"));
            establish(&mut a, c).unwrap();
            let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
            reconcile(&mut a, 30).unwrap();
            let context = a.projection().pending_resume(run);
            lease(&mut a, run, 31);
            start(&mut a, run, 32);
            context
        } else {
            None
        };
        let interrupted =
            a.projection().get_run_instance(&run).unwrap().current_attempt_id().unwrap();
        let digest = a.projection().projection_digest().unwrap();
        drop(a); // Restart with a durably started attempt and retained lease.
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut config = RuntimeConfig {
            data_dir: dir.path().into(),
            dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            continuation_limits: ContinuationLimits {
                output_bytes: 0,
                disposition_bytes: minimum,
                ..Default::default()
            },
            backoff_strategy: actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
                interval: std::time::Duration::ZERO,
            },
            ..Default::default()
        };
        for invalid in [0, 91, 99, minimum - 1] {
            config.continuation_limits.disposition_bytes = invalid;
            let error = ActionQueueEngine::new(config.clone(), Recording(seen.clone()))
                .bootstrap_with_clock(MockClock::new(40))
                .err()
                .expect("unsafe quota must reject");
            assert!(
                matches!(error, BootstrapError::Config(ConfigError::DispositionLimitTooLow { minimum: m }) if m == minimum)
            );
            let a = s::open(dir.path());
            assert_eq!(a.projection().projection_digest().unwrap(), digest);
            assert_eq!(
                a.projection().get_run_instance(&run).unwrap().current_attempt_id(),
                Some(interrupted)
            );
            assert!(seen.lock().unwrap().is_empty());
        }
        config.continuation_limits.disposition_bytes = minimum;
        let mut boot = ActionQueueEngine::new(config, Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(40))
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
        let closed = boot
            .projection()
            .get_attempt_history(&run)
            .unwrap()
            .iter()
            .find(|a| a.attempt_id() == interrupted)
            .unwrap();
        assert_eq!(closed.result(), Some(AttemptResultKind::Failure));
        assert_eq!(closed.finish_origin(), AttemptFinishOrigin::Recovery);
        assert_eq!(closed.error(), Some("executor interrupted before durable disposition"));
        assert!(boot.projection().get_lease_metadata(&run).is_none());
        let _ = boot.run_until_idle().await.unwrap();
        assert_eq!(boot.projection().get_run_state(&run), Some(&RunState::Completed));
        {
            let seen = seen.lock().unwrap();
            let inputs: Vec<_> = seen.iter().filter(|i| i.run_id == run).collect();
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].resume_context, context);
            let attempt = boot.projection().get_attempt_history(&run).unwrap().last().unwrap();
            if context.is_some() {
                let assignment = attempt.accepted_start().unwrap().assignment.unwrap();
                assert_eq!(assignment.delivery, ResumeDelivery::Recovery);
                assert_eq!(assignment.previous_attempt_id, Some(interrupted));
            }
        }
        let mut next = admission_support::request(2).task_spec().clone();
        next.set_constraints(
            TaskConstraints::new(1, None, Some("record-recovery".into())).unwrap(),
        )
        .unwrap();
        let next_id = next.id();
        boot.submit_task(next).unwrap();
        let _ = boot.run_until_idle().await.unwrap();
        assert_eq!(
            boot.projection().runs_for_task(next_id).next().unwrap().state(),
            RunState::Completed
        );
        let digest = boot.projection().projection_digest().unwrap();
        boot.shutdown().unwrap();
        let a = s::open(dir.path());
        assert_eq!(a.projection().projection_digest().unwrap(), digest);
        parity(&a);
    }
}
