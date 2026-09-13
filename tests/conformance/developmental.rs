//! Executable developmental assertions over persisted workloads and exact recovery.
#[path = "contract_boundaries.rs"]
mod boundaries;
#[path = "harness/engine.rs"]
mod engine;
#[path = "harness/neutrality.rs"]
mod neutrality;
#[path = "harness/package.rs"]
mod package;
#[path = "harness/process.rs"]
mod process;
use std::{fs, process::Command};

use actionqueue_runtime::inspection::{DisclosurePolicy, Inspector, Query};
use engine::{Embedded, Step};
use serde_json::{json, Value};
const CASES: std::ops::RangeInclusive<u32> = 1..=18;
fn steps(case: u32) -> Vec<Step> {
    let config: Value = serde_json::from_slice(
        &fs::read(package::root().join(format!("developmental/case-{case:03}-v1.json"))).unwrap(),
    )
    .unwrap();
    serde_json::from_value(config["steps"].clone()).unwrap()
}
fn check(case: u32, d: &mut Embedded) {
    match case {
        3 => d.developmental_denials(),
        9 => {
            d.execute(&Step::RetryAdmission { task: 1, conflict: false });
        }
        10 => d.developmental_conflicts(),
        11 => d.developmental_retry(),
        _ => {}
    }
    if [15, 17].contains(&case) {
        let text = d.metrics_text();
        assert!(text.contains("actionqueue_admission_total"));
        for forbidden in [
            "campaign-arm",
            "distribution-fingerprint",
            "winner",
            "saturation",
            "candidate_acceptance",
        ] {
            assert!(!text.contains(forbidden));
        }
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            if let Some((_, labels)) = line.split_once('{') {
                for pair in labels.split('}').next().unwrap().split(',') {
                    let key = pair.split('=').next().unwrap();
                    assert!(
                        ["state", "result", "outcome", "namespace", "kind", "reason", "le"]
                            .contains(&key),
                        "{key}"
                    );
                }
            }
        }
    }
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
        1 => {
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
        2 => {
            neutrality::check();
            let tasks: Vec<_> = p.tasks().collect();
            assert_eq!(tasks.len(), 3);
            let first = i.get_task(tasks[0].id()).unwrap();
            for t in &tasks[1..] {
                let other = i.get_task(t.id()).unwrap();
                assert_eq!(first.priority, other.priority);
                assert_eq!(first.constraints, other.constraints);
                assert_eq!(first.run_policy, other.run_policy);
                assert_eq!(first.budgets, other.budgets);
            }
            let schedules: Vec<_> =
                p.run_instances().map(|r| (r.scheduled_at(), r.created_at(), r.state())).collect();
            assert!(schedules.iter().all(|r| r == &schedules[0]));
        }
        3 | 10 => {} // Mutation rejection assertions above also run on recovered stores.
        4 => {
            boundaries::assert_synthetic_metadata_rejected();
            let q = p.task_admission(p.tasks().next().unwrap().id()).unwrap().request();
            let mut body = serde_json::to_value(q).unwrap();
            body["metadata"] = json!({"arm":"a","score":1,"scheduler_hint":"fast"});
            assert!(serde_json::from_value::<actionqueue_core::admission::EnsureTaskRequest>(body)
                .is_err());
        }
        5 => {
            let r = p.get_run_instance(&d.runs[&1]).unwrap();
            assert_eq!(p.task_count(), 3);
            assert_eq!(r.state().label(), "awaiting");
            assert_eq!(r.failure_attempt_count(), 0);
            assert!(p.get_lease_metadata(&r.id()).is_none());
            let w = p.waits().active(r.id()).unwrap();
            assert!(w.checkpoint.is_some());
            assert_eq!(
                p.get_attempt_history(&r.id()).unwrap()[0]
                    .disposition
                    .as_ref()
                    .unwrap()
                    .children
                    .len(),
                2
            );
            for t in p.tasks().filter(|t| t.id() != r.task_id()) {
                assert_eq!(t.parent_task_id(), Some(r.task_id()));
                let c = p.task_admission(t.id()).unwrap().request().causal_context();
                assert_eq!(
                    c.correlation_id(),
                    p.task_admission(r.task_id())
                        .unwrap()
                        .request()
                        .causal_context()
                        .correlation_id()
                );
            }
        }
        6 => {
            assert_eq!(p.signals().statistics().retained, 1);
            assert_eq!(p.waits().active_count(), 0);
            for r in p.run_instances() {
                assert_eq!(r.state().label(), "completed");
                assert_eq!(r.attempt_count(), 2);
                assert_eq!(r.failure_attempt_count(), 0);
                let h = p.get_attempt_history(&r.id()).unwrap();
                let context = p.attempt_resume(r.id(), h[1].attempt_id()).unwrap();
                let w = p.waits().records().find(|w| w.run_id == r.id()).unwrap();
                assert_eq!(context.checkpoint, w.checkpoint);
                assert_eq!(
                    context.checkpoint.as_ref().unwrap().created_by_attempt,
                    h[0].attempt_id()
                );
                assert!(
                    matches!(context.wake,actionqueue_core::continuation::WakeReason::Signal{signal_sequence,..} if signal_sequence.get()==1)
                );
                assert!(h[1].accepted_start().unwrap().assignment.is_some());
            }
        }
        7 => {
            use actionqueue_runtime::views::SchedulingField;
            let differences = i.trace(&Query::default()).unwrap().different_fields;
            for field in
                [SchedulingField::Priority, SchedulingField::Constraints, SchedulingField::Budget]
            {
                assert!(
                    differences
                        .iter()
                        .any(|v| std::mem::discriminant(v) == std::mem::discriminant(&field)),
                    "{differences:?}"
                );
            }
            let task = |n| i.get_task(engine::reference_request(n).task_spec().id()).unwrap();
            assert_eq!(task(2).priority, 9);
            assert_eq!(task(3).constraints.timeout_secs(), Some(90));
            assert_eq!(task(4).constraints.concurrency_key(), Some("explicit-lock"));
            assert_eq!(
                task(5).constraints.required_executor_traits(),
                Some(
                    &actionqueue_core::actor::ExecutorTraits::new(vec!["cpu".to_string()]).unwrap()
                )
            );
            assert_eq!(task(6).budgets[0].limit, 99);
        }
        8 => {
            assert_eq!(p.task_count(), 3);
            assert!(p.run_instances().all(|r| r.state().label() == "canceled"));
            assert_eq!(p.waits().active_count(), 0);
            let controls = i
                .task_controls(engine::reference_request(1).task_spec().id(), &Query::default())
                .unwrap();
            assert!(!controls.entries.items.is_empty());
            assert!(serde_json::to_string(&controls).unwrap().contains("conformance-host"));
        }
        9 => {
            assert_eq!(p.task_count(), 1);
        }
        11 => {
            assert_eq!(p.run_instances().next().unwrap().attempt_count(), 3);
        }
        12 => {
            assert_eq!(p.task_count(), 2);
            assert_eq!(p.waits().active_count(), 1);
            assert_eq!(p.signals().statistics().retained, 1);
            assert_eq!(p.signals().records().next().unwrap().envelope().kind.as_str(), "unmatched");
        }
        16 => {
            let r = p.run_instances().next().unwrap();
            let output = p.get_attempt_history(&r.id()).unwrap()[0].output_ref().unwrap();
            output.verify_bytes(b"SYNTHETIC_CUSTOMER_ERP_CANARY_7dbe").unwrap();
            let actionqueue_core::data_ref::DataRef::External(reference) = output else {
                panic!("sensitive data must remain external")
            };
            assert_eq!(reference.locator.expose(), "private://ERP_REFERENCE_CANARY/fixture");
            assert_eq!(reference.scheme.as_str(), "protected-fixture");
            let wal = fs::read(d.a().store_session().unwrap().wal_path()).unwrap();
            assert!(!wal
                .windows(b"SYNTHETIC_CUSTOMER_ERP_CANARY_7dbe".len())
                .any(|w| w == b"SYNTHETIC_CUSTOMER_ERP_CANARY_7dbe"));
            assert!(!trace.to_string().contains("ERP_REFERENCE_CANARY"));
            let attempt = redacted
                .get_attempt(r.id(), p.get_attempt_history(&r.id()).unwrap()[0].attempt_id())
                .unwrap();
            assert!(serde_json::to_value(attempt).unwrap()["output"].is_object());
        }
        17 => {
            assert_eq!(p.task_count(), 150);
            let refs: std::collections::BTreeSet<_> = p
                .tasks()
                .map(|t| {
                    p.task_admission(t.id()).unwrap().request().causal_context().correlation_id()
                })
                .collect();
            assert_eq!(refs.len(), 150);
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
    if case != 11 {
        check(case, &mut d);
    }
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
    check(case, &mut d);
    d.verify();
    d.verify_backup_corruption();
    d.execute(&Step::Snapshot);
    let before = d.evidence();
    drop(d);
    let mut d = Embedded::reopen(&path);
    assert_eq!(before, d.evidence());
    check(case, &mut d);
    d.verify();
    d.verify_backup_corruption();
    let path = dir.path().join("crashed");
    let mut cmd = Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", "developmental_worker", "--ignored", "--nocapture"])
        .env("AQ_DD_CASE", case.to_string())
        .env("AQ_STORE", &path);
    process::kill_at(cmd, &format!("AQ_DD_COMMITTED {case}"));
    let mut d = Embedded::reopen(&path);
    let before: Value =
        serde_json::from_slice(&fs::read(path.with_extension("evidence.json")).unwrap()).unwrap();
    assert_eq!(before, d.evidence());
    check(case, &mut d);
    d.verify();
    d.verify_backup_corruption();
    let (manifest, _) = package::validate(&package::root()).unwrap();
    let hash = package::hash(
        &fs::read(package::root().join(format!("developmental/case-{case:03}-v1.json"))).unwrap(),
    );
    let results:Vec<_>=["ordinary","replay","crash"].iter().map(|variant|json!({"package_revision":manifest.package_revision,"case_id":format!("AQ-DD-{case:03}"),"fixture_id":format!("AQ-CF-DD-{case:03}"),"fixture_hash":hash,"feature_profile":manifest.full_feature_set,"variant":variant,"driver":"embedded","crash_point":if *variant=="crash" {Some("after_workload_commit")}else{None},"assertion_result":"passed"})).collect();
    let report = std::env::var_os("AQ_EVIDENCE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("aq-dd-{case:03}-report.json"));
    fs::write(report, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
    for result in results {
        println!("{result}");
    }
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

#[test]
fn aq_dd_002() {
    run(2);
}

#[test]
fn aq_dd_003() {
    run(3);
}

#[test]
fn aq_dd_004() {
    run(4);
}

#[test]
fn aq_dd_005() {
    run(5);
}

#[test]
fn aq_dd_006() {
    run(6);
}

#[test]
fn aq_dd_007() {
    run(7);
}

#[test]
fn aq_dd_008() {
    run(8);
}

#[test]
fn aq_dd_009() {
    run(9);
}

#[test]
fn aq_dd_010() {
    run(10);
}

#[test]
fn aq_dd_011() {
    run(11);
}

#[test]
fn aq_dd_012() {
    run(12);
}

#[test]
fn aq_dd_017() {
    run(17);
}
