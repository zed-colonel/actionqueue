#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_runtime::inspection::{DisclosurePolicy, Inspector, Query};
fn inspect_host() -> actionqueue_core::control::HostControlContext {
    actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("inspector").unwrap()),
    }
}
fn encoded_trace(p: &ReplayReducer) -> serde_json::Value {
    let h = inspect_host();
    let i = Inspector::new(p, &h, false, Default::default(), false, 40).unwrap();
    serde_json::to_value(i.trace(&Query::default()).unwrap()).unwrap()
}
#[test]
fn aq_dd_007_011_013_018_resume_history_redaction_and_replay_inspection() {
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let (r, context) = wake(&mut a);
    let cp = context.checkpoint.as_ref().unwrap().checkpoint_id;
    lease(&mut a, r, 31);
    let attempt = start(&mut a, r, 32);
    let h = inspect_host();
    let i = Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    let view = i.get_attempt(r, attempt).unwrap();
    assert_eq!(view.resume.unwrap().checkpoint_id, Some(cp));
    assert_eq!(view.assignment.unwrap().context_id, context.context_id);
    let trace = encoded_trace(a.projection());
    let text = serde_json::to_string(&trace).unwrap();
    for secret in ["private continuation", "payload", "locator"] {
        if secret == "payload" {
            continue;
        }
        assert!(!text.contains(secret));
    }
    for field in ["winner", "score", "saturation", "acceptance", "binding_constraint"] {
        assert!(!text.contains(&format!("\"{field}\"")));
    }
    assert!(trace["edges"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["kind"] == "consumed_checkpoint"));
    parity(&a);
    let recovered =
        recover_read_only(a.store_session().unwrap(), RepairPolicy::Strict).unwrap().projection;
    assert_eq!(trace, encoded_trace(&recovered));
    // A host must independently permit disclosure; a flag alone fails closed.
    assert!(
        Inspector::new(a.projection(), &h, false, DisclosurePolicy::default(), true, 40).is_err()
    );
    let disclosed = Inspector::new(
        a.projection(),
        &h,
        false,
        DisclosurePolicy { allow_references: true },
        true,
        40,
    )
    .unwrap();
    assert!(serde_json::to_string(
        &disclosed.get_task(a.projection().get_run_instance(&r).unwrap().task_id()).unwrap()
    )
    .unwrap()
    .contains("disclosed"));
}
#[test]
fn physical_retry_edges_keep_original_wake_identity() {
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
            AttemptOutcome::failure("ERROR_CANARY"),
            33
        ))
    );
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1031, 33))
    );
    transition(&mut a, r, RunState::RetryWait, 33);
    transition(&mut a, r, RunState::Ready, 34);
    lease(&mut a, r, 35);
    let second = start(&mut a, r, 36);
    let h = inspect_host();
    let i = Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    let v = i.get_attempt(r, second).unwrap();
    let assignment = v.assignment.unwrap();
    assert_eq!(assignment.previous_attempt_id, Some(first));
    assert_eq!(assignment.context_id, context.context_id);
    let trace = encoded_trace(a.projection());
    assert!(!trace.to_string().contains("ERROR_CANARY"));
    assert!(trace["edges"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["kind"] == "previous_attempt"));
}

#[test]
fn committed_signal_matching_failure_preserves_identity_and_recovery() {
    use actionqueue_storage::wal::{
        event::{WalEvent, WalEventType},
        writer::{WalWriter, WalWriterError},
    };
    struct FailMatch(actionqueue_storage::wal::fs_writer::WalFsWriter);
    impl WalWriter for FailMatch {
        fn append(&mut self, e: &WalEvent) -> Result<(), WalWriterError> {
            if matches!(e.event(), WalEventType::WaitSatisfied { .. }) {
                return Err(WalWriterError::IoError("PRIVATE_FAILURE".into()));
            }
            self.0.append(e)
        }
        fn flush(&mut self) -> Result<(), WalWriterError> {
            self.0.flush()
        }
        fn close(self) -> Result<(), WalWriterError> {
            self.0.close()
        }
        fn store_session(&self) -> Option<&actionqueue_storage::store::StoreSession> {
            self.0.store_session()
        }
        fn fence(&mut self) {
            self.0.fence();
        }
        fn recovery_required(&self) -> bool {
            self.0.recovery_required()
        }
    }
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let wait = WaitId::new();
    establish_wait(&mut a, r, spec(wait, None));
    let (writer, projection) = a.into_parts();
    let mut a =
        actionqueue_storage::mutation::StorageMutationAuthority::new(FailMatch(writer), projection);
    let h = inspect_host();
    let error = actionqueue_runtime::control::execute_control(
        &mut a,
        &h,
        actionqueue_runtime::control::ControlOperation::AdmitSignal(s::request(1)),
        &MockClock::new(30),
    )
    .unwrap_err();
    assert_eq!(error.code(), "signal_committed_recovery_required");
    assert!(!error.to_string().contains("PRIVATE_FAILURE"));
    let actionqueue_runtime::control::ServiceError::Signal(
        actionqueue_runtime::signals::SignalAdmissionError::Matching { outcome, .. },
    ) = error
    else {
        panic!("typed committed result required")
    };
    assert_eq!(outcome.sequence(), SignalSequence::new(1));
    assert!(a.recovery_required());
    assert!(a.projection().signals().get_signal(None, &s::id(1)).is_some());
    drop(a);
    let mut a = s::reopen(dir.path());
    reconcile(&mut a, 31).unwrap();
    let duplicate = actionqueue_runtime::control::execute_control(
        &mut a,
        &h,
        actionqueue_runtime::control::ControlOperation::AdmitSignal(s::request(1)),
        &MockClock::new(40),
    )
    .unwrap();
    assert!(matches!(
        duplicate,
        actionqueue_runtime::control::ControlOutcome::Signal(
            AdmitSignalOutcome::AlreadyExists { .. }
        )
    ));
    assert!(a.projection().waits().get(wait).unwrap().resolution.is_some());
}

#[test]
fn daemon_maintenance_resolves_deadlines_without_actor_requirement() {
    use std::sync::{Arc, RwLock};

    use actionqueue_daemon::{
        bootstrap::{ReadyStatus, RouterConfig},
        http::{RouterObservability, RouterStateInner},
    };
    use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let wait = WaitId::new();
    establish_wait(
        &mut a,
        r,
        spec(wait, Some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout })),
    );
    let (writer, projection) = a.into_parts();
    let telemetry = WalAppendTelemetry::new();
    let a = Arc::new(Mutex::new(actionqueue_storage::mutation::StorageMutationAuthority::new(
        InstrumentedWalWriter::new(writer, telemetry.clone()),
        projection.clone(),
    )));
    let state = Arc::new(RouterStateInner::with_control_authority(
        RouterConfig { control_enabled: true, metrics_enabled: true },
        Arc::new(RwLock::new(projection)),
        RouterObservability {
            metrics: Arc::new(
                actionqueue_daemon::metrics::registry::MetricsRegistry::new(None).unwrap(),
            ),
            wal_append_telemetry: telemetry,
            clock: Arc::new(MockClock::new(40)),
            recovery_observations:
                actionqueue_storage::recovery::bootstrap::RecoveryObservations::zero(),
        },
        a.clone(),
        ReadyStatus::ready(),
    ));
    actionqueue_daemon::http::maintenance::tick(&state).unwrap();
    let a = a.lock().unwrap();
    assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Ready));
    assert!(a.projection().pending_resume(r).is_some());
    let i = Inspector::new(a.projection(), &inspect_host(), false, Default::default(), false, 40)
        .unwrap()
        .get_wait(wait)
        .unwrap();
    assert!(matches!(
        i.resolution.unwrap().reason,
        actionqueue_runtime::views::ResolutionReason::Deadline
    ));
}

#[test]
fn control_target_history_is_exact_at_equal_times_and_rebuilt_from_retained_wal() {
    use actionqueue_runtime::control::{execute_control, ControlOperation};
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let one = running(&mut a, 1, None, false);
    let two = running(&mut a, 2, None, false);
    let w1 = WaitId::new();
    let w2 = WaitId::new();
    establish_wait(&mut a, one, spec(w1, None));
    establish_wait(&mut a, two, spec(w2, None));
    let h = inspect_host();
    execute_control(
        &mut a,
        &h,
        ControlOperation::ResolveWait { run_id: one, wait_id: w1 },
        &MockClock::new(30),
    )
    .unwrap();
    let first_seq = a.projection().latest_sequence();
    execute_control(
        &mut a,
        &h,
        ControlOperation::CancelWait { run_id: two, wait_id: w2 },
        &MockClock::new(30),
    )
    .unwrap();
    let second_seq = a.projection().latest_sequence();
    let i = Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    let first = i.run_controls(one, &Query::default()).unwrap();
    let second = i.run_controls(two, &Query::default()).unwrap();
    assert!(first.available && second.available);
    assert_eq!(first.entries.items.iter().map(|c| c.sequence).collect::<Vec<_>>(), vec![first_seq]);
    assert_eq!(
        second.entries.items.iter().map(|c| c.sequence).collect::<Vec<_>>(),
        vec![second_seq]
    );
    parity(&a);
    let recovered =
        recover_read_only(a.store_session().unwrap(), RepairPolicy::Strict).unwrap().projection;
    let recovered = Inspector::new(&recovered, &h, false, Default::default(), false, 40).unwrap();
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(recovered.run_controls(one, &Query::default()).unwrap()).unwrap()
    );
    assert_eq!(
        serde_json::to_value(second).unwrap(),
        serde_json::to_value(recovered.run_controls(two, &Query::default()).unwrap()).unwrap()
    );
}

#[test]
fn offline_cli_wait_signal_checkpoint_and_resume_share_embedded_views() {
    use actionqueue_cli::cmd::{api, CommandOutput};
    let dir = resume_dir();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let w = WaitId::new();
    let mut c = command(&a, r, spec(w, None));
    c.checkpoint = Some(checkpoint(&a, r, b"CLI_CHECKPOINT_SECRET"));
    let cp = c.checkpoint.as_ref().unwrap().checkpoint_id;
    establish(&mut a, c).unwrap();
    let h = inspect_host();
    let expected_wait = serde_json::to_value(
        Inspector::new(a.projection(), &h, false, Default::default(), false, 40)
            .unwrap()
            .get_wait(w)
            .unwrap(),
    )
    .unwrap();
    drop(a);
    let invoke = |words: &[&str]| {
        let mut args: Vec<String> = words.iter().map(|s| s.to_string()).collect();
        args.extend([
            "--offline".into(),
            "--data-dir".into(),
            dir.path().to_str().unwrap().into(),
            "--json".into(),
        ]);
        match api::run(args).unwrap() {
            CommandOutput::Json(v) => v,
            _ => panic!("JSON requested"),
        }
    };
    assert_eq!(invoke(&["wait", "inspect", &w.to_string()]), expected_wait);
    let request_file = tempfile::NamedTempFile::new().unwrap();
    let file = request_file.path();
    // The test directory is controller-owned scratch, outside the repository.
    std::fs::write(file, serde_json::to_vec(&s::request(1)).unwrap()).unwrap();
    let admitted = invoke(&["signal", "admit", "--file", file.to_str().unwrap()]);
    let duplicate = invoke(&["signal", "admit", "--file", file.to_str().unwrap()]);
    assert_ne!(admitted, duplicate);
    let run = invoke(&["run", "continuation", &r.to_string()]);
    let cp_view = invoke(&["checkpoint", "inspect", &cp.to_string()]);
    assert!(!cp_view.to_string().contains("CLI_CHECKPOINT_SECRET"));
    let a = s::reopen(dir.path());
    let inspector =
        Inspector::new(a.projection(), &h, false, Default::default(), false, 40).unwrap();
    // Run dispatch gates depend on the supplied clock; this run is already Ready.
    assert_eq!(run, serde_json::to_value(inspector.get_run(r).unwrap()).unwrap());
    assert_eq!(cp_view, serde_json::to_value(inspector.get_checkpoint(cp).unwrap()).unwrap());
}
