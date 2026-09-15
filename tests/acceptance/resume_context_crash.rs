#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
#[path = "../conformance/harness/process.rs"]
mod process;
fn pause() -> ! {
    use std::io::Write;
    println!("AQ_CRASH_BOUNDARY resumed execution");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}
struct CrashRecording(std::path::PathBuf);
impl actionqueue_executor_local::handler::ExecutorHandler for CrashRecording {
    fn execute(
        &self,
        c: actionqueue_executor_local::handler::ExecutorContext,
    ) -> actionqueue_executor_local::handler::AttemptDisposition {
        assert!(c.input.resume_context.as_ref().unwrap().checkpoint.is_some());
        assert!(c.input.causal_context.is_some());
        assert_eq!(c.input.payload, [0, 1, 255]);
        std::fs::write(self.0.join("observed"), c.input.attempt_id.to_string()).unwrap();
        pause()
    }
}
#[test]
#[ignore = "subprocess kill helper"]
fn resume_crash_child() {
    let path = std::path::PathBuf::from(std::env::var("AQ_RESUME_ROOT").unwrap());
    let stage: usize = std::env::var("AQ_RESUME_STAGE").unwrap().parse().unwrap();
    let mut a = s::open(&path);
    let (r, _) = wake(&mut a);
    a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
        output_bytes: 0,
        disposition_bytes: std::env::var("AQ_RESUME_LIMIT").unwrap().parse().unwrap(),
        ..Default::default()
    });
    parity(&a);
    transition(&mut a, r, RunState::Leased, 31);
    commit!(
        &mut a,
        MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(seq(&a), r, "worker", 1000, 31))
    );
    if stage == 3 {
        pause();
    }
    transition(&mut a, r, RunState::Running, 32);
    if stage == 4 {
        pause();
    }
    let point = match stage {
        0 => Some("wal_before_append"),
        1 => Some("wal_partial_frame"),
        2 => Some("authority_before_publish"),
        _ => None,
    };
    if let Some(p) = point {
        actionqueue_storage::store::fault::pause_once(p);
    }
    let id = start(&mut a, r, 33);
    if stage == 9 {
        pause();
    }
    if stage == 5 {
        let p = a.projection();
        let run = p.get_run_instance(&r).unwrap();
        let task = p.get_task(&run.task_id()).unwrap();
        let _ = actionqueue_executor_local::attempt_runner::AttemptRunner::new(CrashRecording(
            path.clone(),
        ))
        .run_attempt(actionqueue_executor_local::types::ExecutorRequest {
            lease_fence: actionqueue_core::mutation::LeaseFence::new("test".into(), 1),
            failure_attempt_count: 0,
            run_id: r,
            attempt_id: id,
            payload: task.payload().to_vec(),
            constraints: task.constraints().clone(),
            attempt_number: run.attempt_count(),
            resume_context: p.attempt_resume(r, id),
            causal_context: p
                .task_admission(task.id())
                .map(|a| a.request().causal_context().clone()),

            children: None,
            cancellation_context: None,
        });
        unreachable!();
    }
    if stage == 6 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
        recover_execution(&mut a, 34).unwrap();
    }
    commit!(
        &mut a,
        MutationCommand::AttemptFinish(
            AttemptFinishCommand::new(seq(&a), r, id, AttemptOutcome::failure("interrupted"), 34)
                .with_recovery_origin()
        )
    );
    if stage == 7 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
    }
    commit!(
        &mut a,
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(seq(&a), r, "worker", 1000, 35))
    );
    if stage == 8 {
        actionqueue_storage::store::fault::pause_once("authority_before_publish");
    }
    transition(&mut a, r, RunState::RetryWait, 36);
    panic!("missed crash boundary");
}
#[test]
fn exactly_once_assignment_and_recovery_redelivery_at_every_prefix_aq_dd_011_012() {
    let limits = [
        actionqueue_core::limits::ContinuationLimits::default().disposition_bytes,
        actionqueue_runtime::config::RuntimeConfig::minimum_disposition_bytes(),
    ];
    for (stage, limit) in (0..10).flat_map(|stage| limits.map(|limit| (stage, limit))) {
        let dir = resume_dir();
        let path = dir.path().join("store");
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "resume_crash_child", "--ignored", "--nocapture"])
            .env("AQ_RESUME_ROOT", &path)
            .env("AQ_RESUME_STAGE", stage.to_string())
            .env("AQ_RESUME_LIMIT", limit.to_string());
        process::kill_at_prefix(cmd, "AQ_CRASH_BOUNDARY ");
        let mut a = s::reopen(&path);
        a.set_continuation_limits(actionqueue_core::limits::ContinuationLimits {
            output_bytes: 0,
            disposition_bytes: limit,
            ..Default::default()
        });
        let r = a.projection().waits().records().next().unwrap().run_id;
        parity(&a);
        let original =
            a.projection().waits().records().next().unwrap().resolution.as_ref().unwrap().sequence;
        recover_execution(&mut a, 40).unwrap();
        parity(&a);
        let before = seq(&a);
        recover_execution(&mut a, 40).unwrap();
        assert_eq!(before, seq(&a));
        assert_eq!(a.projection().pending_resume(r).unwrap().context_id, ResumeContextId(original));
        if a.projection().get_run_state(&r) == Some(&RunState::RetryWait) {
            transition(&mut a, r, RunState::Ready, 41);
        }
        lease(&mut a, r, 42);
        let id = start(&mut a, r, 43);
        let input = observe(a.projection(), r, id);
        if stage == 5 {
            assert_ne!(std::fs::read_to_string(path.join("observed")).unwrap(), id.to_string());
        }
        assert_eq!(input.resume_context.as_ref().unwrap().context_id, ResumeContextId(original));
        assert!(input.causal_context.is_some());
        let assignments: Vec<_> = a
            .projection()
            .get_attempt_history(&r)
            .unwrap()
            .iter()
            .filter_map(|a| a.accepted_start().and_then(|s| s.assignment))
            .collect();
        assert_eq!(
            assignments.iter().filter(|a| a.delivery == ResumeDelivery::Initial).count(),
            1,
            "stage {stage}"
        );
        let was_started = matches!(stage, 2 | 5 | 6 | 7 | 8 | 9);
        assert_eq!(assignments.len(), if was_started { 2 } else { 1 }, "stage {stage}");
        if was_started {
            assert_eq!(assignments.last().unwrap().delivery, ResumeDelivery::Recovery);
            assert!(assignments.last().unwrap().previous_attempt_id.is_some());
        }
        parity(&a);
        commit!(
            &mut a,
            MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                seq(&a),
                r,
                id,
                AttemptOutcome::success(),
                44
            ))
        );
        // Durable success before state transition must never reopen the input.
        drop(a);
        let mut a = s::reopen(&path);
        recover_execution(&mut a, 45).unwrap();
        assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Completed));
        assert!(a.projection().pending_resume(r).is_none());
        parity(&a);
    }
}
