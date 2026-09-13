#[path = "../conformance/harness/engine.rs"]
mod engine;
#[path = "../conformance/harness/package.rs"]
mod package;
#[path = "../conformance/harness/process.rs"]
mod process;
use std::{fs, process::Command};

use actionqueue_core::run::RunState;
use engine::{Embedded, Step};
use serde::Deserialize;
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cut {
    id: String,
    point: String,
    kind: Option<u16>,
    occurrence: usize,
    steps: Vec<Step>,
    outcome: String,
}
fn cuts() -> Vec<Cut> {
    serde_json::from_slice(
        &fs::read(package::root().join("crash-matrix/continuation.json")).unwrap(),
    )
    .unwrap()
}
#[test]
#[ignore = "invoked by bounded crash controller"]
fn continuation_worker() {
    let id = std::env::var("AQ_MATRIX_ID").unwrap();
    let cut = cuts().into_iter().find(|c| c.id == id).unwrap();
    let path = std::path::PathBuf::from(std::env::var("AQ_STORE").unwrap());
    let mut d = Embedded::new(&path);
    actionqueue_storage::store::fault::pause_on(&cut.point, cut.kind, cut.occurrence);
    for step in &cut.steps {
        d.execute(step);
    }
    panic!("missed boundary {}", cut.id);
}
#[test]
fn eleven_commit_boundaries_and_torn_frames_recover_only_complete_facts() {
    package::validate(&package::root()).unwrap();
    let matrix = cuts();
    assert!(matrix.len() >= 14);
    for cut in matrix {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "continuation_worker", "--ignored", "--nocapture"])
            .env("AQ_MATRIX_ID", &cut.id)
            .env("AQ_STORE", &path);
        process::kill_at(cmd, &format!("AQ_CRASH_BOUNDARY {} kind={:?}", cut.point, cut.kind));
        let mut d = Embedded::reopen(&path);
        let p = d.a().projection();
        let tasks = p.task_count();
        match cut.outcome.as_str() {
            "absent" => assert_eq!(tasks, 1, "{}", cut.id),
            "optional" => assert!([1, 3].contains(&tasks), "{}", cut.id),
            "compound" => assert_eq!(tasks, 3, "{}", cut.id),
            "admitted" => {
                assert_eq!(tasks, 1);
                assert_eq!(p.run_instances().next().unwrap().state(), RunState::Scheduled);
            }
            "signal" => {
                assert_eq!(p.signals().statistics().retained, 1);
                assert_eq!(p.waits().active_count(), 1);
            }
            "wait" => assert_eq!(p.waits().active_count(), 1),
            "wake" => {
                assert_eq!(p.waits().active_count(), 0);
                assert_eq!(p.waits().records().filter(|w| w.resolution.is_some()).count(), 1);
            }
            "accepted" => {
                let run = p.run_instances().next().unwrap();
                assert_eq!(run.attempt_count(), 2);
                assert!(p.attempt_resume(run.id(), run.current_attempt_id().unwrap()).is_some());
            }
            other => panic!("unknown expectation {other}"),
        }
        if ["absent", "optional", "compound"].contains(&cut.outcome.as_str()) {
            let committed = tasks == 3;
            let run = p
                .runs_for_task("00000000-0000-0000-0000-000000000001".parse().unwrap())
                .next()
                .unwrap();
            assert_eq!(run.state() == RunState::Awaiting, committed);
            assert_eq!(p.waits().active_count(), usize::from(committed));
            let attempt = &p.get_attempt_history(&run.id()).unwrap()[0];
            assert_eq!(attempt.disposition.is_some(), committed);
            assert_eq!(
                p.checkpoints_by_producer(run.id(), attempt.attempt_id()).count(),
                usize::from(committed)
            );
        }
        let wal = fs::read(d.a().store_session().unwrap().wal_path()).unwrap();
        d.verify();
        d.execute(&Step::Snapshot);
        assert_eq!(
            wal,
            fs::read(d.a().store_session().unwrap().wal_path()).unwrap(),
            "snapshot must retain complete WAL history"
        );
        d.verify();
        // A retained match is reconciled before the caller can dispatch anything.
        d.execute(&Step::Reconcile { at: 100 });
        for run in d.a().projection().run_instances() {
            if run.state().is_terminal() {
                assert!(d.a().projection().waits().active(run.id()).is_none());
            }
        }
        let before = d.evidence();
        d.execute(&Step::Reconcile { at: 100 });
        assert_eq!(before, d.evidence());
    }
}
