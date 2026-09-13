//! The same published workloads through local dispatch, real TCP, CLI and an adapter process.
mod harness {
    pub mod engine;
    pub mod package;
    pub mod process;
    pub mod public;
}
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use harness::{
    engine::Embedded,
    package, process,
    public::{observe, Driver},
};
use serde_json::{json, Value};
fn fixtures() -> Vec<Value> {
    let (m, _) = package::validate(&package::root()).unwrap();
    m.fixtures
        .iter()
        .filter(|f| f.path.starts_with("public/"))
        .map(|f| {
            let mut v: Value =
                serde_json::from_slice(&fs::read(package::root().join(&f.path)).unwrap()).unwrap();
            v["fixture_hash"] = json!(f.sha256);
            v
        })
        .collect()
}
fn adapter() -> PathBuf {
    if let Some(path) = std::env::var_os("AQ_ADAPTER") {
        return path.into();
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/examples/aq_adapter");
    assert!(
        path.is_file(),
        "build the published reference adapter: cargo build --example aq_adapter --features \
         workflow,budget,actor,platform"
    );
    path
}
fn adapter_run(store: &Path, mode: &str, phase: &str) -> Value {
    let input = json!({"schema_version":1,"store":store,"mode":mode,"phase":phase,"hold":false})
        .to_string();
    let output = process::output(Command::new(adapter()), Some(input.as_bytes()));
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
#[ignore = "bounded public-driver crash controller"]
fn public_worker() {
    let path = PathBuf::from(std::env::var("AQ_PUBLIC_STORE").unwrap());
    let mode = std::env::var("AQ_PUBLIC_MODE").unwrap();
    let kind = std::env::var("AQ_PUBLIC_DRIVER").unwrap();
    let mut d = Driver::new(&path, &mode, &kind);
    d.prepare();
    fs::write(
        path.with_extension("evidence.json"),
        serde_json::to_vec(&observe(&d.projection())).unwrap(),
    )
    .unwrap();
    process::halt("AQ_PUBLIC_PREPARED");
}
#[test]
fn public_runtime_daemon_cli_and_adapter_all_persisted_workloads() {
    package::validate(&package::root()).unwrap();
    let selected = std::env::var("AQ_PUBLIC_DRIVER").ok();
    let mut results = vec![];
    for kind in ["embedded", "daemon", "cli", "adapter"] {
        if selected.as_ref().is_some_and(|s| s != kind) {
            continue;
        }
        for fixture in fixtures() {
            if std::env::var("AQ_PUBLIC_FIXTURE").ok().is_some_and(|id| fixture["id"] != id) {
                continue;
            }
            let mode = fixture["mode"].as_str().unwrap();
            let hash = fixture["fixture_hash"].clone();
            for variant in ["ordinary", "replay", "crash"] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("store");
                if variant == "crash" {
                    if kind == "adapter" {
                        // The adapter owns the writer while acknowledging its durable prepare cut.
                        let input = dir.path().join("request.json");
                        fs::write(&input,json!({"schema_version":1,"store":path,"mode":mode,"phase":"prepare","hold":true}).to_string()).unwrap();
                        let mut cmd = Command::new(adapter());
                        cmd.arg("--request-file").arg(input);
                        process::kill_at(cmd, "AQ_PUBLIC_PREPARED");
                    } else {
                        let mut cmd = Command::new(std::env::current_exe().unwrap());
                        cmd.args(["--exact", "public_worker", "--ignored", "--nocapture"])
                            .env("AQ_PUBLIC_STORE", &path)
                            .env("AQ_PUBLIC_MODE", mode)
                            .env("AQ_PUBLIC_DRIVER", kind);
                        process::kill_at(cmd, "AQ_PUBLIC_PREPARED");
                    }
                    let d = Embedded::reopen(&path);
                    let before: Value = serde_json::from_slice(
                        &fs::read(path.with_extension("evidence.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(observe(d.a().projection()), before);
                    d.verify();
                } else if kind == "adapter" {
                    let before = adapter_run(&path, mode, "prepare");
                    let d = Embedded::reopen(&path);
                    assert_eq!(observe(d.a().projection()), before);
                } else {
                    let mut d = Driver::new(&path, mode, kind);
                    d.prepare();
                    d.close();
                }
                if variant == "replay" {
                    let mut d = Embedded::reopen(&path);
                    d.verify();
                    d.execute(&harness::engine::Step::Snapshot);
                    d.verify_backup_corruption();
                }
                if kind == "adapter" {
                    let after = adapter_run(&path, mode, "finish");
                    let d = Embedded::reopen(&path);
                    assert_eq!(observe(d.a().projection()), after);
                } else {
                    let mut d = Driver::new(&path, mode, kind);
                    d.finish();
                    d.close();
                }
                let mut d = Embedded::reopen(&path);
                d.verify();
                assert_eq!(harness::public::summary(d.a().projection()), fixture["expected"]);
                d.verify_backup_corruption();
                results.push(json!({"fixture_id":fixture["id"],"fixture_hash":hash,"driver":kind,"variant":variant,"feature_profile":["workflow","budget","actor","platform"],"crash_point":if variant=="crash"{Some("after_public_prepare")}else{None},"assertion_result":"passed"}));
            }
        }
    }
    assert!(!results.is_empty());
    let path = std::env::var_os("AQ_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join("public-report.json");
    fs::write(path, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
}
