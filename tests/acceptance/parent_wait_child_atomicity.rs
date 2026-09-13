#![allow(dead_code, unused_imports)]
include!("child_support.rs");
#[test]
fn child_termination_wakes_once_and_redelivers_original_evidence_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    assert_eq!(reconcile(&mut a, 21).unwrap(), 0);
    parity(&a);
    finish_child(&mut a, id, true, 30);
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    assert_eq!(reconcile(&mut a, 31).unwrap(), 1);
    assert_eq!(reconcile(&mut a, 31).unwrap(), 0);
    let original = a.projection().pending_resume(r).unwrap();
    assert_eq!(
        original.wake,
        WakeReason::Children {
            wait_id: original.wait_id().unwrap(),
            outcomes: vec![ChildOutcome { task_id: id, status: TaskTerminalStatus::Succeeded }]
        }
    );
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    lease(&mut a, r, 32);
    let first = start(&mut a, r, 32);
    assert_eq!(a.projection().attempt_resume(r, first), Some(original.clone()));
    put(&mut a, r, AttemptDisposition::retryable_failure(BoundedError::new("retry").unwrap()), 33);
    transition(&mut a, r, RunState::Ready, 34);
    lease(&mut a, r, 34);
    let retry = start(&mut a, r, 34);
    assert_eq!(a.projection().attempt_resume(r, retry), Some(original.clone()));
    parity(&a);
    drop(a);
    let mut a = s::reopen(dir.path());
    recover_execution(&mut a, 35).unwrap();
    transition(&mut a, r, RunState::Ready, 36);
    lease(&mut a, r, 36);
    let recovered = start(&mut a, r, 36);
    assert_eq!(a.projection().attempt_resume(r, recovered), Some(original));
    put(&mut a, r, AttemptDisposition::complete(None), 37);
    parity(&a);
}
#[test]
fn failure_witness_is_sorted_and_original_wake_survives_later_child_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let y = child(3, vec![], ChildLifecyclePolicy::Required, p);
    let xid = x.task_spec().id();
    let yid = y.task_spec().id();
    let d = child_disposition(
        &a,
        r,
        vec![x, y],
        vec![yid, xid],
        ChildWaitPolicy::AllSucceededOrAnyFailed,
    );
    put(&mut a, r, d, 20);
    finish_child(&mut a, yid, false, 30);
    reconcile(&mut a, 31).unwrap();
    let wake = a.projection().pending_resume(r).unwrap();
    assert_eq!(
        wake.wake,
        WakeReason::Children {
            wait_id: wake.wait_id().unwrap(),
            outcomes: vec![ChildOutcome { task_id: yid, status: TaskTerminalStatus::Failed }]
        }
    );
    finish_child(&mut a, xid, false, 32);
    control_task(&mut a, yid, 33);
    assert_eq!(a.projection().pending_resume(r), Some(wake));
    parity(&a);
}
#[test]
fn forged_evidence_is_rejected_and_child_fact_wins_over_due_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let w = WaitId::new();
    let d = AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(
                WaitSpec::children(
                    w,
                    vec![id],
                    ChildWaitPolicy::AllTerminal,
                    Some(WaitDeadline { at: 21, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
                )
                .unwrap(),
            ),
            child_admissions: vec![x],
            ..Default::default()
        },
    )
    .unwrap();
    put(&mut a, r, d, 20);
    let before = a.projection().projection_digest().unwrap();
    let c = WaitSatisfyCommand::children(
        seq(&a),
        r,
        w,
        vec![ChildOutcome { task_id: id, status: TaskTerminalStatus::Succeeded }],
        21,
    );
    assert!(a
        .submit_command(MutationCommand::WaitSatisfy(c), DurabilityPolicy::Immediate)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    finish_child(&mut a, id, true, 30);
    reconcile(&mut a, 31).unwrap();
    assert!(matches!(a.projection().pending_resume(r).unwrap().wake, WakeReason::Children { .. }));
    assert!(timeout(&mut a, r, w, 32).is_err());
    parity(&a);
}
#[test]
fn cancellation_recovery_stops_at_detached_relationship_and_cancels_active_waits() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let y = child(3, vec![], ChildLifecyclePolicy::Detached, p);
    let xid = x.task_spec().id();
    let yid = y.task_spec().id();
    let d = child_disposition(&a, r, vec![x, y], vec![xid, yid], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    for id in [xid, yid] {
        let run = a.projection().runs_for_task(id).next().unwrap().id();
        execute_run(&mut a, run, AttemptDisposition::awaiting(spec(WaitId::new(), None), None), 21);
    }
    control_task(&mut a, p, 22);
    drop(a);
    let mut a = s::reopen(dir.path());
    reconcile(&mut a, 23).unwrap();
    assert!(a.projection().is_task_canceled(xid));
    assert!(!a.projection().is_task_canceled(yid));
    assert_eq!(a.projection().waits().active_count(), 1);
    control_task(&mut a, yid, 24);
    reconcile(&mut a, 25).unwrap();
    assert_eq!(a.projection().waits().active_count(), 0);
    parity(&a);
}
#[test]
fn snapshot_rejects_tampered_child_ownership_and_wake_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    finish_child(&mut a, id, true, 30);
    reconcile(&mut a, 31).unwrap();
    let mut snapshot = build_snapshot_from_projection(a.projection(), 0).unwrap();
    snapshot.waits[0].resolution.as_mut().unwrap().kind =
        actionqueue_storage::mutation::wait::WaitResolutionKind::Children(vec![ChildOutcome {
            task_id: id,
            status: TaskTerminalStatus::Failed,
        }]);
    let mut writer = SnapshotFsWriter::new(a.store_session().unwrap()).unwrap();
    assert!(writer.write(&snapshot).is_err());
}
#[test]
fn repeat_child_waits_for_every_run_and_already_terminal_children_wake_new_wait() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let base = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = base.task_spec().id();
    let mut spec = base.task_spec().clone();
    spec.set_run_policy(RunPolicy::repeat(2, 1).unwrap()).unwrap();
    let x = ChildAdmission::new(base.admission_key().clone(), spec, vec![], Default::default())
        .unwrap();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    let runs = a.projection().run_ids_for_task(id);
    execute_run(&mut a, runs[0], AttemptDisposition::complete(None), 30);
    assert_eq!(reconcile(&mut a, 31).unwrap(), 0);
    execute_run(
        &mut a,
        runs[1],
        AttemptDisposition::terminal_failure(BoundedError::new("failed").unwrap()),
        32,
    );
    assert_eq!(reconcile(&mut a, 33).unwrap(), 1);
    assert_eq!(a.projection().task_terminal_status(id), Some(TaskTerminalStatus::Succeeded));
    lease(&mut a, r, 34);
    start(&mut a, r, 34);
    let d = child_disposition(&a, r, vec![], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 35);
    assert_eq!(reconcile(&mut a, 36).unwrap(), 1);
    parity(&a);
}
#[cfg(feature = "workflow")]
#[test]
fn drained_cron_window_is_not_terminal_until_occurrences_exhaust_or_task_cancels() {
    use actionqueue_core::task::run_policy::CronPolicy;
    for cap in [None, Some(5), Some(6)] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let p = parent(&a, r);
        let base = child(2, vec![], ChildLifecyclePolicy::Required, p);
        let id = base.task_spec().id();
        let mut spec = base.task_spec().clone();
        let cron = CronPolicy::new("* * * * * * *").unwrap();
        let cron = if let Some(n) = cap { cron.with_max_occurrences(n).unwrap() } else { cron };
        spec.set_run_policy(RunPolicy::Cron(cron)).unwrap();
        let x = ChildAdmission::new(base.admission_key().clone(), spec, vec![], Default::default())
            .unwrap();
        let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
        put(&mut a, r, d, 20);
        finish_child(&mut a, id, true, 40);
        assert_eq!(reconcile(&mut a, 41).unwrap(), usize::from(cap == Some(5)));
        if cap != Some(5) {
            assert_eq!(a.projection().task_terminal_status(id), None);
            control_task(&mut a, id, 42);
            assert_eq!(reconcile(&mut a, 43).unwrap(), 1);
        }
        parity(&a);
    }
}
#[test]
fn cancellation_reaches_required_grandchildren_of_explicitly_canceled_detached_child() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Detached, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    let cr = a.projection().runs_for_task(id).next().unwrap().id();
    transition(&mut a, cr, RunState::Ready, 21);
    lease(&mut a, cr, 21);
    start(&mut a, cr, 21);
    let grand = child(3, vec![], ChildLifecyclePolicy::Required, id);
    let gid = grand.task_spec().id();
    let d = child_disposition(&a, cr, vec![grand], vec![gid], ChildWaitPolicy::AllTerminal);
    put(&mut a, cr, d, 22);
    control_task(&mut a, p, 23);
    reconcile(&mut a, 24).unwrap();
    assert!(!a.projection().is_task_canceled(id));
    assert!(!a.projection().is_task_canceled(gid));
    control_task(&mut a, id, 25);
    reconcile(&mut a, 26).unwrap();
    assert!(a.projection().is_task_canceled(gid));
    assert_eq!(a.projection().waits().active_count(), 0);
    parity(&a);
}
#[test]
fn control_and_cancellation_are_durable_winners_against_later_child_completion() {
    for cancel_parent in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let p = parent(&a, r);
        let x = child(2, vec![], ChildLifecyclePolicy::Detached, p);
        let id = x.task_spec().id();
        let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
        let w = d.wait().unwrap().wait_id();
        put(&mut a, r, d, 20);
        if cancel_parent {
            cancel(&mut a, r);
        } else {
            commit!(
                &mut a,
                MutationCommand::WaitResolve(WaitResolveCommand {
                    expected_sequence: seq(&a),
                    run_id: r,
                    wait_id: w,
                    tenant_id: None,
                    control_context: ControlMutationContext::new(
                        OpaqueRef::new("operator").unwrap()
                    ),
                    timestamp: 21
                })
            );
        }
        let winner = a.projection().waits().get(w).unwrap().resolution.clone();
        finish_child(&mut a, id, true, 30);
        assert_eq!(reconcile(&mut a, 31).unwrap(), 0);
        assert_eq!(winner, a.projection().waits().get(w).unwrap().resolution);
        parity(&a);
    }
}

fn crash_disposition(a: &s::Authority, r: RunId) -> AttemptDisposition {
    let x = child(2, vec![], ChildLifecyclePolicy::Required, parent(a, r));
    let id = x.task_spec().id();
    child_disposition(a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal)
}
#[test]
#[ignore = "subprocess crash helper"]
fn child_wait_crash_process() {
    let path = std::path::PathBuf::from(std::env::var("AQ_CHILD_WAIT_CRASH_ROOT").unwrap());
    let point = std::env::var("AQ_CHILD_WAIT_CRASH_POINT").unwrap();
    let mut a = s::open(&path);
    let r = running(&mut a, 1, None, false);
    let d = crash_disposition(&a, r);
    let c = proposal(&a, r, d, 20);
    actionqueue_storage::store::fault::pause_once(&point);
    let _ =
        a.submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate);
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
            .args(["--exact", "child_wait_crash_process", "--ignored", "--nocapture"])
            .env("AQ_CHILD_WAIT_CRASH_ROOT", &path)
            .env("AQ_CHILD_WAIT_CRASH_POINT", point)
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
        assert_eq!(a.projection().signals().statistics().retained, 0);
        assert_eq!(a.projection().waits().records().count(), usize::from(committed));
        let attempt = &a.projection().get_attempt_history(&run.id()).unwrap()[0];
        assert_eq!(attempt.disposition.is_some(), committed);
        if committed {
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
        let e = command(&a, r, spec(WaitId::new(), None)).expected;
        let d = crash_disposition(&a, r);
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
fn child_deadline_failure_is_a_terminal_fact() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    let cr = a.projection().runs_for_task(id).next().unwrap().id();
    execute_run(
        &mut a,
        cr,
        AttemptDisposition::awaiting(
            spec(
                WaitId::new(),
                Some(WaitDeadline {
                    at: 30,
                    policy: WaitTimeoutPolicy::FailRun {
                        code: BoundedCode::new("deadline").unwrap(),
                    },
                }),
            ),
            None,
        ),
        21,
    );
    assert_eq!(reconcile(&mut a, 31).unwrap(), 2);
    assert_eq!(a.projection().task_terminal_status(id), Some(TaskTerminalStatus::Failed));
    parity(&a);
}
#[cfg(feature = "workflow")]
#[test]
fn exhausted_zero_run_cron_cancellation_is_a_terminal_fact() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let zero = child(3, vec![], ChildLifecyclePolicy::Detached, p).task_spec().clone();
    let zid = zero.id();
    let zero = TaskSpec::new(
        zid,
        zero.task_payload().clone(),
        actionqueue_core::task::run_policy::RunPolicy::cron("0 0 0 1 1 * 1970").unwrap(),
        zero.constraints().clone(),
        zero.metadata().clone(),
    )
    .unwrap()
    .with_parent_policy(p, ChildLifecyclePolicy::Detached);
    let q = actionqueue_core::admission::EnsureTaskRequest::for_task(zero, vec![]).unwrap();
    admission_support::ensure(&mut a, q, 32).unwrap();
    assert_eq!(a.projection().runs_for_task(zid).count(), 0);
    assert_eq!(a.projection().task_terminal_status(zid), Some(TaskTerminalStatus::Failed));
    control_task(&mut a, zid, 33);
    assert_eq!(a.projection().task_terminal_status(zid), Some(TaskTerminalStatus::Canceled));
    parity(&a);
}
#[test]
#[ignore = "subprocess continuation crash helper"]
fn continuation_crash_process() {
    let path = std::path::PathBuf::from(std::env::var("AQ_CHILD_PHASE_ROOT").unwrap());
    let phase = std::env::var("AQ_CHILD_PHASE").unwrap();
    let point = std::env::var("AQ_CHILD_POINT").unwrap();
    let mut a = s::open(&path);
    let r = running(&mut a, 1, None, false);
    let d = crash_disposition(&a, r);
    put(&mut a, r, d, 20);
    let child = admission_support::id(2);
    if phase == "termination" {
        let run = a.projection().runs_for_task(child).next().unwrap().id();
        transition(&mut a, run, RunState::Ready, 30);
        lease(&mut a, run, 30);
        start(&mut a, run, 30);
        actionqueue_storage::store::fault::pause_once(&point);
        put(&mut a, run, AttemptDisposition::complete(None), 30);
    }
    finish_child(&mut a, child, true, 30);
    if phase == "resolution" {
        actionqueue_storage::store::fault::pause_once(&point);
        reconcile(&mut a, 31).unwrap();
    }
    reconcile(&mut a, 31).unwrap();
    lease(&mut a, r, 32);
    actionqueue_storage::store::fault::pause_once(&point);
    start(&mut a, r, 32);
    panic!("missed boundary");
}
#[test]
fn killed_child_termination_resolution_and_resume_acceptance_recover_original_wake() {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
    };
    for phase in ["termination", "resolution", "acceptance"] {
        for point in ["wal_partial_frame", "authority_before_publish"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let mut process = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "continuation_crash_process", "--ignored", "--nocapture"])
                .env("AQ_CHILD_PHASE_ROOT", &path)
                .env("AQ_CHILD_PHASE", phase)
                .env("AQ_CHILD_POINT", point)
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let output = process.stdout.take().unwrap();
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
            process.kill().unwrap();
            process.wait().unwrap();
            reader.join().unwrap();
            ready.unwrap();
            let mut a = s::reopen(&path);
            recover_execution(&mut a, 40).unwrap();
            reconcile(&mut a, 41).unwrap();
            parity(&a);
            let r = a.projection().runs_for_task(admission_support::id(1)).next().unwrap().id();
            if phase != "termination" {
                let w = a.projection().waits().records().find(|w| w.run_id == r).unwrap();
                assert!(matches!(
                    w.resolution.as_ref().unwrap().kind,
                    actionqueue_storage::mutation::wait::WaitResolutionKind::Children(_)
                ));
            }
        }
    }
}

#[test]
fn resolved_child_wait_keeps_historical_evidence_after_later_dependency_declaration() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Detached, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    finish_child(&mut a, id, true, 30);
    reconcile(&mut a, 31).unwrap();
    let wake = a.projection().pending_resume(r).unwrap();
    commit!(
        &mut a,
        MutationCommand::DependencyDeclare(DependencyDeclareCommand::new(seq(&a), id, vec![p], 32))
    );
    assert_eq!(a.projection().pending_resume(r), Some(wake));
    parity(&a);
}
