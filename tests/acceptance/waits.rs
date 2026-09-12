include!("wait_support.rs");

#[test]
fn all_no_lost_wakeup_orderings_and_every_restart_prefix() {
    // signal before execution, during execution, or after yield; duplicates on either side.
    for signal_step in 0..3 {
        for duplicate in [false, true] {
            for crash_mask in 0..16 {
                let dir = tempfile::tempdir().unwrap();
                let mut a = s::open(dir.path());
                if signal_step == 0 {
                    let _ = s::submit(&mut a, s::envelope(1, 9)).unwrap();
                    if duplicate {
                        let _ = s::submit(&mut a, s::envelope(1, 99)).unwrap();
                    }
                }
                let r = running(&mut a, 1, None, false);
                if crash_mask & 1 != 0 {
                    drop(a);
                    a = s::reopen(dir.path());
                }
                if signal_step == 1 {
                    let _ = s::submit(&mut a, s::envelope(1, 15)).unwrap();
                }
                if crash_mask & 2 != 0 {
                    drop(a);
                    a = s::reopen(dir.path());
                }
                let w = WaitId::new();
                establish_wait(&mut a, r, spec(w, None));
                assert_invariants(&a);
                if crash_mask & 4 != 0 {
                    drop(a);
                    a = s::reopen(dir.path());
                }
                if signal_step == 2 {
                    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
                }
                if duplicate {
                    let _ = s::submit(&mut a, s::envelope(1, 99)).unwrap();
                }
                if crash_mask & 8 != 0 {
                    drop(a);
                    a = s::reopen(dir.path());
                }
                assert_eq!(reconcile(&mut a, 30).unwrap(), 1);
                let sequence = seq(&a);
                assert_eq!(reconcile(&mut a, 300).unwrap(), 0);
                assert_eq!(seq(&a), sequence);
                assert_invariants(&a);
                assert!(
                    matches!(a.projection().pending_resume(r).unwrap().wake,WakeReason::Signal{signal_sequence,..} if signal_sequence.get()==1)
                );
                let digest = a.projection().projection_digest().unwrap();
                drop(a);
                let a = s::reopen(dir.path());
                assert_eq!(digest, a.projection().projection_digest().unwrap());
            }
        }
    }
}
#[test]
fn exact_optional_filter_table_and_exclusive_cursors() {
    for corr in [None, Some("job/1"), Some("other")] {
        for source in [None, Some("origin"), Some("other")] {
            for cursor in [0, 1, 2] {
                let dir = tempfile::tempdir().unwrap();
                let mut a = s::open(dir.path());
                let r = running(&mut a, 1, None, false);
                let mut e = s::envelope(1, 15);
                e.source_ref = Some(OpaqueRef::new("origin").unwrap());
                let _ = s::submit(&mut a, e.clone()).unwrap();
                e.signal_id = SignalId::new("second").unwrap();
                let _ = s::submit(&mut a, e).unwrap();
                let mut f = s::filter();
                f.correlation_id = corr.map(|s| CorrelationId::new(s).unwrap());
                f.source_ref = source.map(|s| OpaqueRef::new(s).unwrap());
                let w = WaitSpec::new(
                    WaitId::new(),
                    f,
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::After(SignalSequence::new(cursor)),
                    None,
                )
                .unwrap();
                establish_wait(&mut a, r, w);
                let expected = corr != Some("other") && source != Some("other") && cursor < 2;
                assert_eq!(reconcile(&mut a, 30).unwrap(), usize::from(expected));
                if expected {
                    assert!(
                        matches!(a.projection().pending_resume(r).unwrap().wake,WakeReason::Signal{signal_sequence,..} if signal_sequence.get()==cursor+1)
                    );
                }
                assert_invariants(&a);
            }
        }
    }
}
#[test]
fn resolution_races_preserve_first_commit_and_identical_retries_append_nothing() {
    for first in 0..4 {
        for second in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let mut a = s::open(dir.path());
            let r = running(&mut a, 1, None, false);
            let w = WaitId::new();
            establish_wait(
                &mut a,
                r,
                spec(
                    w,
                    Some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
                ),
            );
            let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
            let ctx = ControlMutationContext::new(OpaqueRef::new("operator").unwrap());
            let proposal = |i, sequence| match i {
                0 => MutationCommand::WaitSatisfy(WaitSatisfyCommand::new(
                    sequence,
                    r,
                    w,
                    SignalSequence::new(1),
                    30,
                )),
                1 => MutationCommand::WaitTimeout(WaitTimeoutCommand::new(sequence, r, w, 30)),
                2 => MutationCommand::WaitCancel(WaitCancelCommand::new(
                    sequence,
                    r,
                    w,
                    ctx.clone(),
                    30,
                )),
                _ => MutationCommand::WaitResolve(WaitResolveCommand {
                    expected_sequence: sequence,
                    run_id: r,
                    wait_id: w,
                    tenant_id: None,
                    control_context: ctx.clone(),
                    timestamp: 30,
                }),
            };
            let first_outcome = commit!(&mut a, proposal(first, seq(&a)));
            let before = a.projection().projection_digest().unwrap();
            let retry =
                a.submit_command(fixture_control(proposal(second, 0)), DurabilityPolicy::Immediate);
            if first == second {
                assert!(
                    matches!(retry.unwrap().applied(),AppliedMutation::Wait(WaitOutcome::AlreadyResolved{sequence,..}) if *sequence==first_outcome)
                );
            } else {
                assert!(matches!(
                    retry,
                    Err(MutationAuthorityError::Wait(WaitRejection::WaitAlreadyResolved))
                ));
            }
            assert_eq!(before, a.projection().projection_digest().unwrap());
            assert_invariants(&a);
            drop(a);
            let a = s::reopen(dir.path());
            assert_eq!(before, a.projection().projection_digest().unwrap());
        }
    }
}
#[test]
fn deadlines_all_policies_boundaries_rollback_and_signal_priority_on_recovery() {
    for policy in [
        WaitTimeoutPolicy::ResumeWithTimeout,
        WaitTimeoutPolicy::FailRun { code: BoundedCode::new("elapsed").unwrap() },
        WaitTimeoutPolicy::CancelRun,
    ] {
        for now in [29, 30, 31] {
            let dir = tempfile::tempdir().unwrap();
            let mut a = s::open(dir.path());
            let r = running(&mut a, 1, None, false);
            let w = WaitId::new();
            establish_wait(
                &mut a,
                r,
                spec(w, Some(WaitDeadline { at: 30, policy: policy.clone() })),
            );
            let result = timeout(&mut a, r, w, now);
            if now < 30 {
                assert!(matches!(result, Err(MutationAuthorityError::Wait(WaitRejection::NotDue))));
            } else {
                let _ = result.unwrap();
            }
            let state = if now < 30 {
                RunState::Awaiting
            } else {
                match policy {
                    WaitTimeoutPolicy::ResumeWithTimeout => RunState::Ready,
                    WaitTimeoutPolicy::FailRun { .. } => RunState::Failed,
                    WaitTimeoutPolicy::CancelRun => RunState::Canceled,
                }
            };
            assert_eq!(a.projection().get_run_state(&r), Some(&state));
            assert_eq!(reconcile(&mut a, 0).unwrap(), 0);
            assert_invariants(&a);
            assert_eq!(
                a.projection().get_attempt_history(&r).unwrap()[0].result(),
                Some(AttemptResultKind::Awaiting)
            );
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let w = WaitId::new();
    establish_wait(
        &mut a,
        r,
        spec(w, Some(WaitDeadline { at: 1, policy: WaitTimeoutPolicy::CancelRun })),
    );
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    drop(a);
    let mut a = s::reopen(dir.path());
    reconcile(&mut a, 100).unwrap();
    assert!(a.projection().pending_resume(r).is_some());
}
#[test]
fn fanout_drains_more_than_one_page_in_deterministic_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    for n in 1..=MATCH_BATCH + 3 {
        let r = running(&mut a, n as u64, None, false);
        establish_wait(&mut a, r, spec(WaitId::new(), None));
    }
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    let expected = a.projection().waits().matches(MATCH_BATCH + 3);
    let batch = reconcile_batch(&mut a, 30, MATCH_BATCH).unwrap();
    assert_eq!(batch.resolved, MATCH_BATCH);
    assert!(batch.remaining);
    drop(a);
    let mut a = s::reopen(dir.path());
    assert_eq!(reconcile(&mut a, 30).unwrap(), 3);
    let mut history: Vec<_> = a
        .projection()
        .waits()
        .records()
        .map(|w| (w.resolution.as_ref().unwrap().sequence, w.spec.wait_id()))
        .collect();
    history.sort();
    assert_eq!(
        history.iter().map(|(_, w)| *w).collect::<Vec<_>>(),
        expected.iter().map(|(_, w)| *w).collect::<Vec<_>>()
    );
    assert_invariants(&a);
}
#[test]
fn establishment_is_atomic_fenced_and_retryable_with_held_key_and_checkpoint() {
    for hold in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, Some("exclusive"), hold);
        let w = WaitId::new();
        let mut c = command(&a, r, spec(w, None));
        c.checkpoint = Some(CheckpointRef {
            checkpoint_id: CheckpointId::new(),
            created_by_attempt: c.expected.attempt_id(),
            data: actionqueue_core::data_ref::DataRef::Inline(
                actionqueue_core::data_ref::InlineData::new(
                    None,
                    vec![1, 2, 3],
                    ContentHash::new(
                        HashAlgorithm::Sha256,
                        sha2::Sha256::digest([1, 2, 3]).to_vec(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            ),
        });
        let mut stale = c.clone();
        stale.expected = AttemptCommitExpectation::new(
            seq(&a),
            r,
            c.expected.attempt_id(),
            RunState::Running,
            LeaseFence::new(
                LeaseOwner::new("worker"),
                c.expected.expected_lease().granted_at_sequence() - 1,
            ),
        );
        assert!(matches!(
            establish(&mut a, stale),
            Err(MutationAuthorityError::Wait(WaitRejection::StaleLease))
        ));
        assert!(a
            .submit_command(MutationCommand::WaitEstablish(c.clone()), DurabilityPolicy::Deferred)
            .is_err());
        establish(&mut a, c.clone()).unwrap();
        let n = seq(&a);
        assert!(matches!(
            establish(&mut a, c.clone()).unwrap(),
            WaitOutcome::AlreadyEstablished { .. }
        ));
        assert_eq!(n, seq(&a));
        assert_invariants(&a);
        assert_eq!(a.projection().key_reservations().any(|(id, _)| id == r), hold);
        let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
        reconcile(&mut a, 30).unwrap();
        assert_eq!(a.projection().pending_resume(r).unwrap().checkpoint, c.checkpoint);
        drop(a);
        let mut a = s::reopen(dir.path());
        assert_eq!(a.projection().key_reservations().any(|(id, _)| id == r), hold);
        let history = a.projection().waits().get(w).unwrap().resolution.clone();
        cancel(&mut a, r);
        assert_eq!(history, a.projection().waits().get(w).unwrap().resolution);
        assert!(!a.projection().key_reservations().any(|(id, _)| id == r));
        assert_invariants(&a);
    }
}
#[test]
fn cancellation_before_establishment_and_mixed_task_runs_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let c = command(&a, r, spec(WaitId::new(), None));
    cancel(&mut a, r);
    assert!(establish(&mut a, c).is_err());
    let r = running(&mut a, 2, None, false);
    establish_wait(&mut a, r, spec(WaitId::new(), None));
    let task = a.projection().get_run_instance(&r).unwrap().task_id();
    commit!(&mut a, MutationCommand::TaskCancel(TaskCancelCommand::new(seq(&a), task, 30)));
    assert!(a.projection().runs_for_task(task).all(|r| r.state().is_terminal()));
    assert_invariants(&a);
    assert_eq!(reconcile(&mut a, 999).unwrap(), 0);
}
#[test]
fn continuation_retention_is_independent_of_manual_unpin() {
    use actionqueue_runtime::signals::{pin_signal, retire_signals, unpin_signal};
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let w = WaitId::new();
    establish_wait(&mut a, r, spec(w, None));
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    a.set_signal_retention_policy(actionqueue_core::limits::SignalRetentionPolicy {
        minimum_age_secs: 0,
        minimum_sequence_window: 0,
    });
    let pin = SignalPinId::new("manual").unwrap();
    pin_signal(&mut a, s::id(1), pin.clone(), Default::default(), &MockClock::new(26)).unwrap();
    unpin_signal(&mut a, s::id(1), pin, Default::default(), &MockClock::new(27)).unwrap();
    for resolved in [false, true] {
        if resolved {
            reconcile(&mut a, 30).unwrap();
            cancel(&mut a, r);
        }
        assert!(a.projection().signal_is_protected(SignalSequence::new(1)));
        assert!(retire_signals(
            &mut a,
            vec![SignalSequence::new(1)],
            Default::default(),
            &MockClock::new(100)
        )
        .is_err());
    }
}
#[test]
fn every_new_mutation_fault_boundary_is_fenced_and_replayable() {
    let _guard = FAULTS.lock().unwrap();
    for mutation in 0..6 {
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
            let w = WaitId::new();
            let c = command(
                &a,
                r,
                spec(
                    w,
                    Some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
                ),
            );
            if mutation > 0 {
                establish(&mut a, c.clone()).unwrap();
                let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
            }
            let cmd = match mutation {
                0 => MutationCommand::WaitEstablish(c),
                1 => MutationCommand::WaitSatisfy(WaitSatisfyCommand::new(
                    seq(&a),
                    r,
                    w,
                    SignalSequence::new(1),
                    30,
                )),
                2 => MutationCommand::WaitTimeout(WaitTimeoutCommand::new(seq(&a), r, w, 30)),
                3 => MutationCommand::WaitCancel(WaitCancelCommand::new(
                    seq(&a),
                    r,
                    w,
                    ControlMutationContext::new(OpaqueRef::new("host").unwrap()),
                    30,
                )),
                n => MutationCommand::Cancel(CancelCommand {
                    expected_sequence: seq(&a),
                    target: if n == 4 {
                        CancelTarget::Run(r)
                    } else {
                        CancelTarget::Task(a.projection().get_run_instance(&r).unwrap().task_id())
                    },
                    tenant_id: None,
                    control_context: None,
                    timestamp: 30,
                }),
            };
            let before = a.projection().projection_digest().unwrap();
            actionqueue_storage::store::fault::fail_once(point);
            assert!(a
                .submit_command(fixture_control(cmd.clone()), DurabilityPolicy::Immediate)
                .is_err());
            assert!(a.recovery_required());
            if point != "authority_after_publish" {
                assert_eq!(before, a.projection().projection_digest().unwrap());
            }
            assert!(matches!(
                a.submit_command(cmd, DurabilityPolicy::Immediate),
                Err(MutationAuthorityError::RecoveryRequired)
            ));
            drop(a);
            let mut a = s::reopen(dir.path());
            assert_invariants(&a);
            if mutation == 0 && a.projection().waits().get(w).is_none() {
                recover_execution(&mut a, 31).unwrap();
                assert_ne!(a.projection().get_run_state(&r), Some(&RunState::Running));
                assert!(a.projection().get_lease(&r).is_none());
            }
            reconcile(&mut a, 31).unwrap();
            assert_invariants(&a);
            let seq = seq(&a);
            assert_eq!(reconcile(&mut a, 31).unwrap(), 0);
            assert_eq!(seq, crate::seq(&a));
        }
    }
}
#[test]
fn snapshot_tail_backup_and_malformed_continuation_references() {
    use actionqueue_storage::{
        snapshot::writer::{SnapshotFsWriter, SnapshotWriter, SnapshotWriterError},
        store::backup::{backup_store, restore_store},
    };
    let _ = std::mem::size_of::<SnapshotWriterError>();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut a = s::open(&path);
    let r = running(&mut a, 1, Some("key"), true);
    let w = WaitId::new();
    establish_wait(&mut a, r, spec(w, None));
    let snapshot = build_snapshot_from_projection(a.projection(), 0).unwrap();
    let session = a.store_session().unwrap().clone();
    std::fs::create_dir_all(path.join("snapshots")).unwrap();
    let mut writer = SnapshotFsWriter::new(&session).unwrap();
    writer.write(&snapshot).unwrap();
    writer.close().unwrap();
    let snapshot_bytes = std::fs::read(session.snapshot_path()).unwrap();
    for mutation in 0..3 {
        let mut bad = snapshot.clone();
        match mutation {
            0 => bad.waits.clear(),
            1 => bad.waits[0].lease_granted_at_sequence = 0,
            _ => bad.waits[0].attempt_id = AttemptId::new(),
        }
        let mut writer = SnapshotFsWriter::new(&session).unwrap();
        assert!(writer.write(&bad).is_err());
        assert_eq!(std::fs::read(session.snapshot_path()).unwrap(), snapshot_bytes);
    }
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    reconcile(&mut a, 30).unwrap();
    let digest = a.projection().projection_digest().unwrap();
    drop(session);
    drop(a);
    let a = s::reopen(&path);
    assert_eq!(digest, a.projection().projection_digest().unwrap());
    drop(a);
    let backup = dir.path().join("backup");
    let restored = dir.path().join("restored");
    assert_eq!(backup_store(&path, &backup).unwrap().projection_digest, digest);
    assert_eq!(restore_store(&backup, &restored).unwrap().projection_digest, digest);
    let session = open_store(&restored, OpenOptions::ReadOnly).unwrap();
    assert_eq!(
        recover_read_only(&session, RepairPolicy::Strict)
            .unwrap()
            .projection
            .projection_digest()
            .unwrap(),
        digest
    );
}
#[derive(Clone)]
struct SharedClock(std::sync::Arc<std::sync::atomic::AtomicU64>);
impl actionqueue_core::time::clock::Clock for SharedClock {
    fn now(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
struct Handler {
    forbidden: RunId,
    sleep: bool,
}
impl actionqueue_executor_local::ExecutorHandler for Handler {
    fn execute(
        &self,
        ctx: actionqueue_executor_local::ExecutorContext,
    ) -> actionqueue_executor_local::AttemptDisposition {
        if ctx.input.run_id == self.forbidden {
            assert!(ctx.input.resume_context.is_some(), "resumed handler requires durable input");
        }
        if self.sleep {
            std::thread::sleep(std::time::Duration::from_millis(600));
        }
        actionqueue_executor_local::AttemptDisposition::complete(None)
    }
}
fn dispatch(
    a: s::Authority,
    r: RunId,
    clock: SharedClock,
    sleep: bool,
) -> actionqueue_runtime::dispatch::DispatchLoop<
    actionqueue_storage::wal::fs_writer::WalFsWriter,
    Handler,
    SharedClock,
> {
    actionqueue_runtime::dispatch::DispatchLoop::new(
        a,
        Handler { forbidden: r, sleep },
        clock,
        actionqueue_runtime::dispatch::DispatchConfig::new(
            actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
                interval: std::time::Duration::from_secs(1),
            },
            1,
            100,
            None,
            None,
        ),
    )
    .unwrap()
}

fn admit_once(a: &mut s::Authority, n: u64, parent: Option<TaskId>, deps: Vec<TaskId>) -> RunId {
    let q = admission_support::request(n);
    let t = q.task_spec();
    let mut task = TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        actionqueue_core::task::run_policy::RunPolicy::Once,
        TaskConstraints::default(),
        t.metadata().clone(),
    )
    .unwrap();
    if let Some(parent) = parent {
        task = task.with_parent(parent);
    }
    let q = admission_support::with_dependencies(&admission_support::with_spec(&q, task), deps);
    let _ = admission_support::ensure(a, q, 10).unwrap();
    a.projection().runs_for_task(t.id()).next().unwrap().id()
}

fn await_scheduled(a: &mut s::Authority, run: RunId, wait: WaitSpec) {
    transition(a, run, RunState::Ready, 11);
    transition(a, run, RunState::Leased, 12);
    commit!(
        a,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(seq(a), run, "worker", 1000, 12))
    );
    transition(a, run, RunState::Running, 13);
    commit!(
        a,
        MutationCommand::AttemptStart(AttemptStartCommand::new(
            seq(a),
            run,
            AttemptId::new(),
            13,
            a.projection()
                .get_lease_metadata(&run)
                .map(|l| actionqueue_core::mutation::LeaseFence::new(
                    l.owner().into(),
                    l.granted_at_sequence()
                ))
                .unwrap_or_else(|| actionqueue_core::mutation::LeaseFence::new(
                    "missing".into(),
                    0
                )),
            a.projection().pending_resume(run).map(|c| c.context_id)
        ))
    );
    establish_wait(a, run, wait);
}

fn cancel_task_command(sequence: u64, task: TaskId) -> CancelCommand {
    CancelCommand {
        expected_sequence: sequence,
        target: CancelTarget::Task(task),
        tenant_id: None,
        control_context: None,
        timestamp: 25,
    }
}

#[tokio::test]
async fn embedded_parent_cancellation_precedes_descendant_matching_and_recovers_every_prefix() {
    // Live API, restart after parent commit, and restart after one descendant commit.
    for prefix in 0..=2 {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let parent = admit_once(&mut a, 1, None, vec![]);
        let child = admit_once(&mut a, 2, Some(admission_support::id(1)), vec![]);
        let grandchild = admit_once(&mut a, 3, Some(admission_support::id(2)), vec![]);
        let sibling = admit_once(&mut a, 4, Some(admission_support::id(1)), vec![]);
        let child_wait = WaitId::new();
        let grandchild_wait = WaitId::new();
        await_scheduled(&mut a, child, spec(child_wait, None));
        await_scheduled(&mut a, grandchild, spec(grandchild_wait, None));
        let clock = SharedClock(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(25)));
        let mut d = if prefix == 0 {
            let mut d = dispatch(a, child, clock.clone(), false);
            d.cancel(cancel_task_command(
                d.projection().latest_sequence() + 1,
                admission_support::id(1),
            ))
            .unwrap();
            // No tick in between: admission itself may trigger matching.
            d.admit_signal(s::request(1), Default::default()).unwrap();
            d
        } else {
            for n in 1..=prefix {
                commit!(
                    &mut a,
                    MutationCommand::Cancel(cancel_task_command(seq(&a), admission_support::id(n)))
                );
            }
            let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
            drop(a);
            dispatch(s::reopen(dir.path()), child, clock.clone(), false)
        };
        for run in [parent, child, grandchild, sibling] {
            assert_eq!(
                d.projection().get_run_state(&run),
                Some(&RunState::Canceled),
                "prefix {prefix}"
            );
            assert!(d.projection().waits().active(run).is_none());
            assert!(d.projection().pending_resume(run).is_none());
        }
        for wait in [child_wait, grandchild_wait] {
            assert!(matches!(
                d.projection().waits().get(wait).unwrap().resolution.as_ref().unwrap().kind,
                actionqueue_storage::mutation::wait::WaitResolutionKind::Canceled(_)
            ));
        }
        let before = d.projection().projection_digest().unwrap();
        assert_eq!(d.tick().await.unwrap().dispatched, 0);
        assert_eq!(d.reconcile_waits().unwrap(), 0);
        assert_eq!(d.projection().projection_digest().unwrap(), before);
        drop(d);
        let d = dispatch(s::reopen(dir.path()), child, clock, false);
        assert_eq!(d.projection().projection_digest().unwrap(), before);
    }
}

#[tokio::test]
async fn terminal_deadlines_cancel_dependencies_and_hierarchy_live_and_on_bootstrap() {
    for policy in [
        WaitTimeoutPolicy::CancelRun,
        WaitTimeoutPolicy::FailRun { code: BoundedCode::new("expired").unwrap() },
    ] {
        // Tick, explicit service, signal-triggered service, overdue bootstrap,
        // crash after timeout, and crash partway through its cancellation cascade.
        for path in 0..6 {
            let dir = tempfile::tempdir().unwrap();
            let mut a = s::open(dir.path());
            let prerequisite = admit_once(&mut a, 1, None, vec![]);
            let dependent = admit_once(&mut a, 2, None, vec![admission_support::id(1)]);
            let transitive = admit_once(&mut a, 3, None, vec![admission_support::id(2)]);
            let child = admit_once(&mut a, 4, Some(admission_support::id(2)), vec![]);
            let wait = WaitId::new();
            await_scheduled(
                &mut a,
                prerequisite,
                spec(wait, Some(WaitDeadline { at: 30, policy: policy.clone() })),
            );
            let child_wait = WaitId::new();
            await_scheduled(
                &mut a,
                child,
                spec(
                    child_wait,
                    Some(WaitDeadline { at: 31, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
                ),
            );
            let clock = SharedClock(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(25)));
            let mut d = if path >= 3 {
                if path >= 4 {
                    let _ = timeout(&mut a, prerequisite, wait, 40).unwrap();
                }
                if path == 5 {
                    commit!(
                        &mut a,
                        MutationCommand::Cancel(cancel_task_command(
                            seq(&a),
                            admission_support::id(2)
                        ))
                    );
                }
                drop(a);
                clock.0.store(40, std::sync::atomic::Ordering::SeqCst);
                dispatch(s::reopen(dir.path()), prerequisite, clock.clone(), false)
            } else {
                let mut d = dispatch(a, prerequisite, clock.clone(), false);
                clock.0.store(40, std::sync::atomic::Ordering::SeqCst);
                match path {
                    0 => {
                        assert_eq!(d.tick().await.unwrap().dispatched, 0);
                    }
                    1 => {
                        assert_eq!(d.reconcile_waits().unwrap(), 1);
                    }
                    _ => {
                        // An unrelated signal still runs the deadline service.
                        let mut e = s::envelope(1, 40);
                        e.kind = SignalKind::new("unrelated").unwrap();
                        d.admit_signal(s::request_from(&e).unwrap(), Default::default()).unwrap();
                    }
                }
                d
            };
            let expected = if matches!(policy, WaitTimeoutPolicy::CancelRun) {
                RunState::Canceled
            } else {
                RunState::Failed
            };
            assert_eq!(d.projection().get_run_state(&prerequisite), Some(&expected));
            for run in [dependent, transitive, child] {
                assert_eq!(
                    d.projection().get_run_state(&run),
                    Some(&RunState::Canceled),
                    "path {path}, policy {policy:?}"
                );
                assert!(d.projection().pending_resume(run).is_none());
            }
            assert!(matches!(
                d.projection().waits().get(child_wait).unwrap().resolution.as_ref().unwrap().kind,
                actionqueue_storage::mutation::wait::WaitResolutionKind::Canceled(_)
            ));
            // Terminal bookkeeping must also prevent new children of the timed-out task.
            let late_child = admission_support::spec(5).with_parent(admission_support::id(1));
            assert!(d.submit_task(late_child).is_err());
            let before = d.projection().projection_digest().unwrap();
            assert_eq!(d.tick().await.unwrap().dispatched, 0);
            assert_eq!(d.reconcile_waits().unwrap(), 0);
            assert_eq!(d.projection().projection_digest().unwrap(), before);
            drop(d);
            let d = dispatch(s::reopen(dir.path()), prerequisite, clock, false);
            assert_eq!(d.projection().projection_digest().unwrap(), before);
        }
    }
}

#[tokio::test]
async fn deadlines_continue_while_paused_or_draining_and_dispatch_after_reopen() {
    for paused in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, Some("key"), true);
        let w = WaitId::new();
        establish_wait(
            &mut a,
            r,
            spec(w, Some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout })),
        );
        if paused {
            commit!(&mut a, MutationCommand::EnginePause(EnginePauseCommand::new(seq(&a), 25)));
        }
        let clock = SharedClock(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(25)));
        let mut d = dispatch(a, r, clock.clone(), false);
        assert_eq!(d.next_wait_deadline(), Some(30));
        if !paused {
            d.start_drain();
        }
        clock.0.store(30, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(d.tick().await.unwrap().dispatched, 0);
        assert!(d.projection().pending_resume(r).is_some());
        let a = d.into_authority();
        let mut d = dispatch(a, r, clock.clone(), false);
        let _ = d.tick().await.unwrap();
        if paused {
            assert!(d.projection().pending_resume(r).is_some());
        } else {
            assert!(d
                .projection()
                .get_attempt_history(&r)
                .unwrap()
                .iter()
                .any(|a| a.accepted_start().is_some_and(|s| s.assignment.is_some())));
        }
    }
}
#[tokio::test]
async fn worker_result_wait_does_not_starve_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    establish_wait(
        &mut a,
        r,
        spec(
            WaitId::new(),
            Some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
        ),
    );
    let clock = SharedClock(std::sync::Arc::new(std::sync::atomic::AtomicU64::new(25)));
    let mut d = dispatch(a, r, clock.clone(), true);
    let c = clock.clone();
    let advance = async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        c.0.store(30, std::sync::atomic::Ordering::SeqCst);
    };
    let run = async {
        tokio::time::timeout(std::time::Duration::from_millis(250), d.run_until_idle()).await
    };
    let (_, result) = tokio::join!(advance, run);
    assert!(result.is_err());
    assert!(
        d.projection().pending_resume(r).is_some()
            || d.projection()
                .get_attempt_history(&r)
                .unwrap()
                .iter()
                .any(|a| a.accepted_start().is_some_and(|s| s.assignment.is_some()))
    );
    d.start_drain();
    let _ = d.drain_until_idle(std::time::Duration::from_secs(2)).await.unwrap();
}
#[test]
fn rejects_invalid_identity_attempt_scope_and_non_earliest_signal_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let id = WaitId::new();
    let base = command(&a, r, spec(id, None));
    let mut wrong = base.clone();
    wrong.expected = AttemptCommitExpectation::new(
        seq(&a),
        r,
        AttemptId::new(),
        RunState::Running,
        base.expected.expected_lease().clone(),
    );
    let before = seq(&a);
    assert!(establish(&mut a, wrong).is_err());
    assert_eq!(before, seq(&a));
    let mut f = s::filter();
    f.tenant_id = Some(TenantId::new());
    let mut wrong = base.clone();
    wrong.wait = WaitSpec::new(
        id,
        f,
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        None,
    )
    .unwrap();
    assert!(matches!(
        establish(&mut a, wrong),
        Err(MutationAuthorityError::Wait(WaitRejection::TenantMismatch))
    ));
    let nil: WaitId = "00000000-0000-0000-0000-000000000000".parse().unwrap();
    let mut wrong = base.clone();
    wrong.wait = spec(nil, None);
    assert!(establish(&mut a, wrong).is_err());
    assert_eq!(before, seq(&a));
    establish(&mut a, base).unwrap();
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    let _ = s::submit(&mut a, s::envelope(2, 26)).unwrap();
    assert!(matches!(
        satisfy(&mut a, r, id, 2),
        Err(MutationAuthorityError::Wait(WaitRejection::InvalidSignal))
    ));
    let _ = satisfy(&mut a, r, id, 1).unwrap();
    let before = seq(&a);
    let state = *a.projection().get_run_state(&r).unwrap();
    let context = a.projection().pending_resume(r);
    let _ = a
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                before,
                r,
                state,
                RunState::Leased,
                31,
            )),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    assert_eq!(context, a.projection().pending_resume(r));
}
#[tokio::test]
async fn daemon_run_and_task_cancellation_resolve_waits_through_compound_controls() {
    use tower::ServiceExt;
    for task_control in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = open_store(dir.path(), OpenOptions::Initialize { features: vec![] })
            .unwrap()
            .into_authority()
            .unwrap()
            .with_host(actionqueue_core::control::HostControlContext {
                actor_id: None,
                scope: actionqueue_core::control::ControlScope::SingleTenant,
                attribution: actionqueue_core::causal::ControlMutationContext::new(
                    actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                ),
            });
        let r = running(&mut a, 1, None, false);
        let w = WaitId::new();
        establish_wait(&mut a, r, spec(w, None));
        let task = a.projection().get_run_instance(&r).unwrap().task_id();
        drop(a);
        let state = actionqueue_daemon::bootstrap::bootstrap_with_authenticator(
            actionqueue_daemon::config::DaemonConfig {
                data_dir: dir.path().to_path_buf(),
                enable_control: true,
                ..Default::default()
            },
            Some(std::sync::Arc::new(|_, _| {
                Ok(actionqueue_core::control::HostControlContext {
                    actor_id: None,
                    scope: actionqueue_core::control::ControlScope::SingleTenant,
                    attribution: ControlMutationContext::new(
                        OpaqueRef::new("wait-test-host").unwrap(),
                    ),
                })
            })),
        )
        .unwrap();
        let path = if task_control {
            format!("/api/v2/tasks/{task}:cancel")
        } else {
            format!("/api/v2/runs/{r}:cancel")
        };
        let response = state
            .http_router()
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        state.shutdown().await;
        let a = s::reopen(dir.path());
        assert!(matches!(
            a.projection().waits().get(w).unwrap().resolution.as_ref().unwrap().kind,
            actionqueue_storage::mutation::wait::WaitResolutionKind::Canceled(_)
        ));
        assert_invariants(&a);
    }
}
#[test]
fn reacquisition_by_same_owner_changes_fence_and_heartbeat_preserves_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let original = a.projection().get_lease_metadata(&r).unwrap().granted_at_sequence();
    commit!(
        &mut a,
        MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(seq(&a), r, "worker", 1100, 14))
    );
    assert_eq!(a.projection().get_lease_metadata(&r).unwrap().granted_at_sequence(), original);
    let attempt = a.projection().get_run_instance(&r).unwrap().current_attempt_id().unwrap();
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            seq(&a),
            r,
            attempt,
            AttemptOutcome::failure("retry"),
            15
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1100, 15))
    );
    transition(&mut a, r, RunState::RetryWait, 15);
    transition(&mut a, r, RunState::Ready, 16);
    transition(&mut a, r, RunState::Leased, 17);
    commit!(
        &mut a,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(seq(&a), r, "worker", 1200, 17))
    );
    transition(&mut a, r, RunState::Running, 18);
    let attempt = AttemptId::new();
    commit!(
        &mut a,
        MutationCommand::AttemptStart(AttemptStartCommand::new(
            seq(&a),
            r,
            attempt,
            18,
            a.projection()
                .get_lease_metadata(&r)
                .map(|l| actionqueue_core::mutation::LeaseFence::new(
                    l.owner().into(),
                    l.granted_at_sequence()
                ))
                .unwrap_or_else(|| actionqueue_core::mutation::LeaseFence::new(
                    "missing".into(),
                    0
                )),
            a.projection().pending_resume(r).map(|c| c.context_id)
        ))
    );
    let mut c = command(&a, r, spec(WaitId::new(), None));
    c.expected = AttemptCommitExpectation::new(
        seq(&a),
        r,
        attempt,
        RunState::Running,
        LeaseFence::new(LeaseOwner::new("worker"), original),
    );
    assert!(matches!(
        establish(&mut a, c),
        Err(MutationAuthorityError::Wait(WaitRejection::StaleLease))
    ));
    let c = command(&a, r, spec(WaitId::new(), None));
    establish(&mut a, c).unwrap();
    assert_invariants(&a);
}
#[cfg(feature = "platform")]
#[test]
fn scoped_waits_never_observe_other_tenants_or_unscoped_signals() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open_platform(dir.path());
    let tenant = TenantId::new();
    let other = TenantId::new();
    for t in [tenant, other] {
        commit!(
            &mut a,
            MutationCommand::TenantCreate(TenantCreateCommand::new(
                seq(&a),
                actionqueue_core::platform::TenantRegistration::new(t, "tenant"),
                1
            ))
        );
    }
    let r = running_scoped(&mut a, 1, None, false, Some(tenant));
    let id = WaitId::new();
    let mut f = s::filter();
    f.tenant_id = Some(tenant);
    let w = WaitSpec::new(
        id,
        f,
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::After(SignalSequence::new(0)),
        None,
    )
    .unwrap();
    establish_wait(&mut a, r, w);
    assert!(s::submit(&mut a, s::envelope(1, 25)).is_err());
    let mut e = s::envelope(1, 26);
    e.tenant_id = Some(other);
    let _ = s::submit(&mut a, e).unwrap();
    assert_eq!(reconcile(&mut a, 30).unwrap(), 0);
    let control = WaitResolveCommand {
        expected_sequence: seq(&a),
        run_id: r,
        wait_id: id,
        tenant_id: Some(other),
        control_context: ControlMutationContext::new(OpaqueRef::new("host").unwrap()),
        timestamp: 30,
    };
    assert!(matches!(
        a.submit_command(MutationCommand::WaitResolve(control), DurabilityPolicy::Immediate),
        Err(MutationAuthorityError::Control(_))
    ));
    let mut e = s::envelope(1, 27);
    e.tenant_id = Some(tenant);
    let _ = s::submit(&mut a, e).unwrap();
    assert_eq!(reconcile(&mut a, 30).unwrap(), 1);
    assert!(
        matches!(a.projection().pending_resume(r).unwrap().wake,WakeReason::Signal{signal_sequence,..} if signal_sequence.get()==2)
    );
}
#[test]
fn wait_policy_changes_conflict_under_the_same_admission_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let q = admission_support::request(1);
    admission_support::ensure(&mut a, q.clone(), 10).unwrap();
    let t = q.task_spec();
    let mut constraints = t.constraints().clone();
    constraints.set_concurrency_key_wait_policy(ConcurrencyKeyWaitPolicy::HoldWhileAwaiting);
    let spec = TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        t.run_policy().clone(),
        constraints,
        t.metadata().clone(),
    )
    .unwrap();
    let changed = admission_support::with_spec(&q, spec);
    assert_ne!(q.digest().unwrap(), changed.digest().unwrap());
    assert!(admission_support::ensure(&mut a, changed, 11).is_err());
}
