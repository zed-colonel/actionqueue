mod harness {
    pub mod engine;
    pub mod package;
    pub mod process;
}
use std::{fs, process::Command};

use harness::{engine::*, package, process};

#[test]
#[ignore = "invoked by the bounded crash controller"]
fn scenario_worker() {
    let root = package::root();
    let scenario = read_scenario(&root, &std::env::var("AQ_SCENARIO").unwrap());
    let path = std::path::PathBuf::from(std::env::var("AQ_STORE").unwrap());
    let cut: usize = std::env::var("AQ_CUT").unwrap().parse().unwrap();
    let mut driver = Embedded::new(&path);
    for step in scenario.steps.iter().take(cut) {
        driver.execute(step);
    }
    let evidence = driver.evidence();
    fs::write(path.parent().unwrap().join("evidence.json"), serde_json::to_vec(&evidence).unwrap())
        .unwrap();
    process::halt(&format!("AQ_SCENARIO_CUT {} {cut}", scenario.id));
}
#[test]
fn immutable_scenarios_ordinary_wal_snapshot_tail_and_process_restart() {
    let (manifest, _) = package::validate(&package::root()).unwrap();
    for fixture in manifest.fixtures.iter().filter(|f| f.path.starts_with("fixtures/")) {
        let scenario = read_scenario(&package::root(), &fixture.path);
        let expected: Vec<serde_json::Value> = serde_json::from_slice(
            &fs::read(package::root().join(&scenario.expected_observations)).unwrap(),
        )
        .unwrap();
        assert_eq!(expected.len(), scenario.steps.len());
        let dir = tempfile::tempdir().unwrap();
        let mut driver = Embedded::new(&dir.path().join("ordinary"));
        for (index, step) in scenario.steps.iter().enumerate() {
            assert_eq!(driver.execute(step), expected[index], "{} step {}", fixture.id, index + 1);
            if scenario.recovery_cuts.contains(&(index + 1)) {
                driver.verify();
                driver.apply_step(&Step::Snapshot);
                driver.verify();
                driver.verify_backup_corruption();
            }
        }
        drop(driver);
        for cut in &scenario.recovery_cuts {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let mut cmd = Command::new(std::env::current_exe().unwrap());
            cmd.args(["--exact", "scenario_worker", "--ignored", "--nocapture"])
                .env("AQ_SCENARIO", &fixture.path)
                .env("AQ_CUT", cut.to_string())
                .env("AQ_STORE", &path);
            process::kill_at(cmd, &format!("AQ_SCENARIO_CUT {} {cut}", scenario.id));
            let mut driver = Embedded::reopen(&path);
            let evidence: serde_json::Value =
                serde_json::from_slice(&fs::read(dir.path().join("evidence.json")).unwrap())
                    .unwrap();
            assert_eq!(driver.evidence(), evidence, "{} crash cut {cut}", fixture.id);
            driver.verify();
            driver.verify_backup_corruption();
            for (index, step) in scenario.steps.iter().enumerate().skip(*cut) {
                assert_eq!(driver.execute(step), expected[index]);
            }
            driver.verify();
        }
    }
}
