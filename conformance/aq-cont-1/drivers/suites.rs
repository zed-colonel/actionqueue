//! Build proof binaries once, then run them directly and consume only fresh reports.
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use serde_json::{json, Value};

use super::package;
fn run(mut cmd: Command, log: &Path, minimum: usize) -> bool {
    let file = fs::File::create(log).unwrap();
    let output = cmd
        .stdout(Stdio::from(file.try_clone().unwrap()))
        .stderr(Stdio::from(file))
        .status()
        .unwrap();
    let text = fs::read_to_string(log).unwrap();
    let passed: usize = text
        .lines()
        .filter_map(|l| l.strip_prefix("test result: ok. "))
        .filter_map(|s| s.split_whitespace().next()?.parse::<usize>().ok())
        .sum();
    output.success() && passed >= minimum
}
pub fn execute(
    manifest: &package::Manifest,
    dir: &Path,
    selected: Option<(&str, Option<&str>)>,
    full: bool,
    adapter: Option<&str>,
) -> Vec<Value> {
    fs::create_dir_all(dir).unwrap();
    let mut results = vec![];
    let mut build = Command::new("cargo");
    build.args([
        "build",
        "--workspace",
        "--examples",
        "--features",
        "workflow,budget,actor,platform",
    ]);
    assert!(run(build, &dir.join("consumer-build.log"), 0), "consumer build failed");
    let specs: Vec<_> = manifest
        .fixtures
        .iter()
        .filter(|f| full && f.path.starts_with("proofs/"))
        .map(|f| {
            let spec: Value =
                serde_json::from_slice(&fs::read(package::root().join(&f.path)).unwrap()).unwrap();
            (f, spec)
        })
        .collect();
    let mut build = Command::new("cargo");
    build.args([
        "test",
        "--features",
        "workflow,budget,actor,platform",
        "--no-run",
        "--message-format=json",
        "--test",
        "conformance_public",
    ]);
    for (_, spec) in &specs {
        build.args(["--test", spec["target"].as_str().unwrap()]);
    }
    let log = dir.join("proof-build.log");
    assert!(run(build, &log, 0), "proof build failed");
    let binaries: BTreeMap<String, PathBuf> = fs::read_to_string(log)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|v| v["reason"] == "compiler-artifact" && v["profile"]["test"] == true)
        .filter_map(|v| {
            Some((v["target"]["name"].as_str()?.into(), PathBuf::from(v["executable"].as_str()?)))
        })
        .collect();
    for (f, spec) in specs {
        let target = spec["target"].as_str().unwrap();
        eprintln!("Conformance proof {}: {target}", f.id);
        let mut command = Command::new(&binaries[target]);
        command.arg("--test-threads=1").env("AQ_EVIDENCE_DIR", dir);
        let passed = run(
            command,
            &dir.join(format!("{target}.log")),
            spec["minimum_passed"].as_u64().unwrap() as usize,
        );
        results.push(json!({"fixture_id":f.id,"fixture_hash":f.sha256,"driver":"embedded","variant":"suite","feature_profile":manifest.full_feature_set,"assertion_result":if passed{"passed"}else{"failed"},"test_target":target}));
    }
    if full {
        for case in 1..=18 {
            let path = dir.join(format!("aq-dd-{case:03}-report.json"));
            if let Ok(bytes) = fs::read(path) {
                let records: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
                results.extend(records);
            }
        }
    }
    let mut c = Command::new(&binaries["conformance_public"]);
    c.arg("--test-threads=1").env("AQ_EVIDENCE_DIR", dir);
    if let Some(path) = adapter {
        c.env("AQ_ADAPTER", path);
    }
    if let Some((driver, fixture)) = selected {
        c.env("AQ_PUBLIC_DRIVER", driver);
        if let Some(fixture) = fixture {
            c.env("AQ_PUBLIC_FIXTURE", fixture);
        } else {
            c.env_remove("AQ_PUBLIC_FIXTURE");
        }
    } else {
        c.env_remove("AQ_PUBLIC_DRIVER").env_remove("AQ_PUBLIC_FIXTURE");
    }
    let passed = run(c, &dir.join("conformance_public.log"), 1);
    if passed {
        let records: Vec<Value> =
            serde_json::from_slice(&fs::read(dir.join("public-report.json")).unwrap()).unwrap();
        results.extend(records);
    } else {
        eprintln!("Public-driver suite failed: {}", dir.join("conformance_public.log").display());
    }
    results
}
