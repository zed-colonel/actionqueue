//! Executable developmental assertions over persisted workloads and exact recovery.
#[path = "harness/engine.rs"]
mod engine;
#[path = "harness/package.rs"]
mod package;
#[path = "harness/process.rs"]
mod process;
use std::{fs, process::Command};

use actionqueue_runtime::inspection::{DisclosurePolicy, Inspector, Query};
use engine::{Embedded, Step};
use serde_json::{json, Value};
const CASES: [u32; 6] = [1, 13, 14, 15, 16, 18];
fn steps(case: u32) -> Vec<Step> {
    let config: Value = serde_json::from_slice(
        &fs::read(package::root().join("developmental/workloads.json")).unwrap(),
    )
    .unwrap();
    serde_json::from_value(config[format!("AQ-DD-{case:03}")]["steps"].clone()).unwrap()
}
fn check(case: u32, d: &Embedded) {
    let p = d.a().projection();
    let h = engine::host();
    let i = Inspector::new(p, &h, false, DisclosurePolicy { allow_references: true }, true, 100)
        .unwrap();
    let redacted = Inspector::new(p, &h, false, Default::default(), false, 100).unwrap();
    let trace = serde_json::to_value(redacted.trace(&Query::default()).unwrap()).unwrap();
    for name in [
        "winner",
        "score",
        "gain",
        "saturation",
        "binding_constraint",
        "candidate_acceptance",
        "replication",
        "statistical_independence",
    ] {
        assert!(!trace.to_string().contains(&format!("\"{name}\"")));
    }
    match case {
        1 | 16 => {
            let q = p.task_admission(p.tasks().next().unwrap().id()).unwrap().request();
            let original: Value = serde_json::from_str(include_str!(
                "../../conformance/aq-cont-1/developmental-admission.json"
            ))
            .unwrap();
            assert_eq!(
                q.causal_context().origin_ref().unwrap().expose(),
                original["origin_ref"].as_str().unwrap()
            );
            let task = serde_json::to_value(i.get_task(q.task_spec().id()).unwrap()).unwrap();
            assert_eq!(task["admission"]["causal"]["origin_ref"]["value"], original["origin_ref"]);
            assert!(!trace.to_string().contains(original["origin_ref"].as_str().unwrap()));
            assert_eq!(task["payload"]["size_bytes"], q.task_spec().task_payload().bytes().len());
            assert!(task["payload"]["content_hash"].is_object());
        }
        13 => {
            let run = p.run_instances().next().unwrap();
            assert_eq!(run.state().label(), "completed");
            let digest = p.projection_digest().unwrap();
            // Evaluation belongs entirely to the caller; rejection does not rewrite execution history.
            let output = p.get_attempt_history(&run.id()).unwrap()[0].output_ref().unwrap();
            let accepted_by_external_verifier = matches!(output,actionqueue_core::data_ref::DataRef::Inline(data) if data.bytes()==b"4");
            assert!(!accepted_by_external_verifier);
            assert_eq!(p.projection_digest().unwrap(), digest);
            assert_eq!(run.failure_attempt_count(), 0);
        }
        14 => {
            let run = p.run_instances().next().unwrap();
            assert_eq!(run.state().label(), "failed");
            assert_eq!(run.failure_attempt_count(), 1);
            let attempts =
                serde_json::to_value(redacted.list_attempts(run.id(), &Query::default()).unwrap())
                    .unwrap();
            assert_eq!(attempts["items"].as_array().unwrap().len(), 1);
            assert!(!attempts.to_string().contains("external tool failure"));
        }
        15 => {
            let states: std::collections::BTreeSet<_> =
                p.run_instances().map(|r| r.state().label()).collect();
            assert_eq!(states, ["completed", "failed", "canceled"].into_iter().collect());
            for run in p.run_instances() {
                assert!(p.get_run_history(&run.id()).unwrap().last().unwrap().to().is_terminal());
            }
        }
        18 => {
            let q = p.task_admission(p.tasks().next().unwrap().id()).unwrap().request();
            let origin = q.causal_context().origin_ref().unwrap().expose();
            let correlation = q.causal_context().correlation_id().as_str();
            for query in [
                Query { origin_ref: Some(origin.into()), limit: Some(1), ..Default::default() },
                Query {
                    correlation_id: Some(correlation.into()),
                    limit: Some(1),
                    ..Default::default()
                },
            ] {
                let first = i.list_tasks(&query).unwrap();
                assert_eq!(first.items.len(), 1);
                let cursor = first.next_cursor.clone().expect("second page");
                let second =
                    i.list_tasks(&Query { cursor: Some(cursor), ..query.clone() }).unwrap();
                assert_eq!(second.items.len(), 1);
                assert_ne!(first.items[0].id, second.items[0].id);
            }
            assert!(i
                .list_tasks(&Query {
                    origin_ref: Some(format!("{origin}/near-miss")),
                    ..Default::default()
                })
                .unwrap()
                .items
                .is_empty());
            let wrong = actionqueue_core::control::HostControlContext {
                scope: actionqueue_core::control::ControlScope::Tenant(
                    actionqueue_core::ids::TenantId::new(),
                ),
                ..h
            };
            assert!(Inspector::new(p, &wrong, false, Default::default(), false, 100)
                .and_then(|i| i.list_tasks(&Query::default()))
                .is_err());
            assert_eq!(
                trace["notice"],
                "Opaque references are attribution only. Queue outcomes describe execution; \
                 application-level judgments belong to the caller."
            );
        }
        _ => panic!("unimplemented developmental case"),
    }
}
#[test]
#[ignore = "invoked by bounded developmental crash controller"]
fn developmental_worker() {
    let case = std::env::var("AQ_DD_CASE").unwrap().parse().unwrap();
    let path = std::path::PathBuf::from(std::env::var("AQ_STORE").unwrap());
    let mut d = Embedded::new(&path);
    for step in steps(case) {
        d.execute(&step);
    }
    check(case, &d);
    fs::write(path.with_extension("evidence.json"), serde_json::to_vec(&d.evidence()).unwrap())
        .unwrap();
    process::halt(&format!("AQ_DD_COMMITTED {case}"));
}
fn run(case: u32) {
    assert!(CASES.contains(&case));
    package::validate(&package::root()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ordinary");
    let mut d = Embedded::new(&path);
    for step in steps(case) {
        d.execute(&step);
    }
    check(case, &d);
    d.verify();
    d.execute(&Step::Snapshot);
    let before = d.evidence();
    drop(d);
    let d = Embedded::reopen(&path);
    assert_eq!(before, d.evidence());
    check(case, &d);
    d.verify();
    let path = dir.path().join("crashed");
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "developmental_worker", "--ignored", "--nocapture"])
        .env("AQ_DD_CASE", case.to_string())
        .env("AQ_STORE", &path);
    process::kill_at(cmd, &format!("AQ_DD_COMMITTED {case}"));
    let d = Embedded::reopen(&path);
    let before: Value =
        serde_json::from_slice(&fs::read(path.with_extension("evidence.json")).unwrap()).unwrap();
    assert_eq!(before, d.evidence());
    check(case, &d);
    d.verify();
    println!(
        "{}",
        json!({"case":format!("AQ-DD-{case:03}"),"variants":["ordinary","replay","crash"],"result":"passed"})
    );
}
#[test]
fn aq_dd_001() {
    run(1);
}
#[test]
fn aq_dd_013() {
    run(13);
}
#[test]
fn aq_dd_014() {
    run(14);
}
#[test]
fn aq_dd_015() {
    run(15);
}
#[test]
fn aq_dd_016() {
    run(16);
}
#[test]
fn aq_dd_018() {
    run(18);
}
