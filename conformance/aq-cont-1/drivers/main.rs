//! Standalone package runner. Exit 2 means capabilities/coverage are incomplete.
#[path = "../../../tests/conformance/harness/engine.rs"]
mod engine;
#[path = "../../../tests/conformance/harness/package.rs"]
mod package;
#[path = "../../../tests/conformance/harness/process.rs"]
mod process;
#[path = "../../../tests/conformance/harness/report.rs"]
mod report;
mod suites;
use std::{fs, path::PathBuf, process::Command};

use engine::{read_scenario, Embedded};
use serde_json::{json, Value};
fn features() -> Vec<&'static str> {
    [
        ("workflow", cfg!(feature = "workflow")),
        ("budget", cfg!(feature = "budget")),
        ("actor", cfg!(feature = "actor")),
        ("platform", cfg!(feature = "platform")),
    ]
    .into_iter()
    .filter_map(|(n, b)| b.then_some(n))
    .collect()
}
fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help") {
        println!(
            "aq_conformance [--fixture ID] [--driver embedded|daemon|cli|adapter] [--adapter \
             PATH] [--full] [--report PATH]\nRuns ordinary, replay and acknowledged process-crash \
             cuts. Full fails on missing coverage or features."
        );
        return;
    }
    let mut options = std::collections::BTreeSet::new();
    let mut words = args.iter();
    while let Some(flag) = words.next() {
        if !options.insert(flag) {
            eprintln!("duplicate option: {flag}");
            std::process::exit(2);
        }
        match flag.as_str() {
            "--full" => {}
            "--fixture" | "--driver" | "--report" | "--worker" | "--cut" | "--adapter" => {
                if words.next().is_none_or(|v| v.starts_with("--")) {
                    eprintln!("missing value for {flag}");
                    std::process::exit(2);
                }
            }
            _ => {
                eprintln!("unknown option: {flag}");
                std::process::exit(2);
            }
        }
    }
    let value = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .map(|i| args.get(i + 1).expect("option requires value").clone())
    };
    let root = package::root();
    let (manifest, coverage) =
        package::validate(&root).unwrap_or_else(|e| panic!("package integrity: {e}"));
    if let Some(path) = value("--worker") {
        let fixture =
            manifest.fixtures.iter().find(|f| f.id == value("--fixture").unwrap()).unwrap();
        let scenario = read_scenario(&root, &fixture.path);
        let cut: usize = value("--cut").unwrap().parse().unwrap();
        let mut d = Embedded::new(&PathBuf::from(&path));
        for step in scenario.steps.iter().take(cut) {
            d.execute(step);
        }
        fs::write(
            PathBuf::from(&path).with_extension("evidence.json"),
            serde_json::to_vec(&d.evidence()).unwrap(),
        )
        .unwrap();
        process::halt(&format!("AQ_SCENARIO_CUT {} {cut}", fixture.id));
    }
    let full = args.iter().any(|a| a == "--full");
    let selected = value("--fixture");
    let driver = value("--driver").unwrap_or("embedded".into());
    let mut missing = Vec::new();
    if !["embedded", "daemon", "cli", "adapter"].contains(&driver.as_str()) {
        missing.push(format!("unsupported driver: {driver}"));
    }
    if full {
        for feature in &manifest.full_feature_set {
            if !features().contains(&feature.as_str()) {
                missing.push(format!("missing feature: {feature}"));
            }
        }
        for case in &coverage.cases {
            if case.fixtures.is_empty() || case.drivers.is_empty() {
                missing.push(format!("unexecuted case: {}", case.id));
            }
        }
        if manifest.status != "executable" {
            missing.push(format!("package status: {}", manifest.status));
        }
        if value("--driver").is_some() {
            missing.push("full profile cannot select a driver; all drivers are required".into());
        }
        if selected.is_some() {
            missing.push("full profile cannot select a subset".into());
        }
    }
    let fixtures: Vec<_> = manifest
        .fixtures
        .iter()
        .filter(|f| {
            f.path.starts_with("fixtures/") && selected.as_ref().is_none_or(|id| id == &f.id)
        })
        .collect();
    let public_selected = selected.as_ref().is_some_and(|id| id.starts_with("AQ-CF-PUBLIC-"));
    if fixtures.is_empty() && driver == "embedded" && !public_selected {
        missing.push("no executable fixture selected".into());
    }
    let mut results = Vec::new();
    if missing.is_empty() && driver == "embedded" {
        for f in fixtures {
            let scenario = read_scenario(&root, &f.path);
            if scenario.required_features.iter().any(|n| !features().contains(&n.as_str())) {
                missing.push(format!("fixture lacks features: {}", f.id));
                continue;
            }
            let expected: Vec<Value> = serde_json::from_slice(
                &fs::read(root.join(&scenario.expected_observations)).unwrap(),
            )
            .unwrap();
            assert_eq!(expected.len(), scenario.steps.len());
            let result = std::panic::catch_unwind(|| {
                let dir = tempfile::tempdir().unwrap();
                let mut d = Embedded::new(&dir.path().join("store"));
                for (i, step) in scenario.steps.iter().enumerate() {
                    assert_eq!(d.execute(step), expected[i]);
                    if scenario.recovery_cuts.contains(&(i + 1)) {
                        d.verify();
                        d.apply_step(&engine::Step::Snapshot);
                        d.verify();
                        d.verify_backup_corruption();
                    }
                }
            });
            for variant in ["ordinary", "replay", "backup", "corruption"] {
                results.push(json!({"fixture_id":f.id,"fixture_hash":f.sha256,"feature_profile":features(),"driver":driver,"variant":variant,"crash_point":null,"assertion_result":if result.is_ok(){"passed"}else{"failed"}}));
            }
            for cut in &scenario.recovery_cuts {
                let result = std::panic::catch_unwind(|| {
                    let dir = tempfile::tempdir().unwrap();
                    let path = dir.path().join("store");
                    let mut cmd = Command::new(std::env::current_exe().unwrap());
                    cmd.args([
                        "--worker",
                        path.to_str().unwrap(),
                        "--fixture",
                        &f.id,
                        "--cut",
                        &cut.to_string(),
                    ]);
                    process::kill_at(cmd, &format!("AQ_SCENARIO_CUT {} {cut}", f.id));
                    let mut d = Embedded::reopen(&path);
                    let before: Value = serde_json::from_slice(
                        &fs::read(path.with_extension("evidence.json")).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(d.evidence(), before);
                    d.verify();
                    for (i, step) in scenario.steps.iter().enumerate().skip(*cut) {
                        assert_eq!(d.execute(step), expected[i]);
                    }
                    d.verify();
                });
                results.push(json!({"fixture_id":f.id,"fixture_hash":f.sha256,"feature_profile":features(),"driver":driver,"variant":"crash","crash_point":cut,"assertion_result":if result.is_ok(){"passed"}else{"failed"}}));
            }
        }
    }
    if missing.is_empty() && (full || driver != "embedded" || public_selected) {
        let evidence =
            tempfile::Builder::new().prefix("aq-full-evidence-").tempdir().unwrap().keep();
        eprintln!("Fresh conformance evidence: {}", evidence.display());
        results.extend(suites::execute(
            &manifest,
            &evidence,
            if full { None } else { Some((driver.as_str(), selected.as_deref())) },
            full,
            value("--adapter").as_deref(),
        ));
    }
    missing.extend(report::invalid_hashes(&manifest, &results));
    if results.is_empty() {
        missing.push("no passing executable results".into());
    }
    if full {
        missing.extend(report::missing_evidence(&coverage, &results));
        missing.extend(report::missing_storage_evidence(&root, &manifest, &results));
    }
    let passed = missing.is_empty() && results.iter().all(|v| v["assertion_result"] == "passed");
    let report = json!({"schema_version":1,"package_revision":manifest.package_revision,"contract_revision":manifest.contract_revision,"developmental_profile_revision":manifest.developmental_profile_revision,"profile":if full{"full"}else{"subset"},"passed":passed,"missing":missing,"results":results});
    let bytes = serde_json::to_vec_pretty(&report).unwrap();
    if let Some(path) = value("--report") {
        fs::write(path, &bytes).unwrap();
    }
    println!("{}", String::from_utf8(bytes).unwrap());
    if !passed {
        std::process::exit(if missing.is_empty() { 1 } else { 2 });
    }
}
