#[path = "harness/package.rs"]
mod package;
#[test]
fn inventory_is_closed_and_hashes_are_immutable() {
    package::validate(&package::root()).unwrap();
}
#[test]
fn rejects_path_traversal_absolute_paths_and_symlink_escape() {
    let root = tempfile::tempdir().unwrap();
    for path in ["../manifest.yaml", "/etc/passwd", "a/../b", "a/./b", "a//b", "a\\b", ""] {
        assert!(package::safe_path(root.path(), path).is_err());
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", root.path().join("link")).unwrap();
        assert!(package::safe_path(root.path(), "link").is_err());
    }
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let target = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_tree(&p, &target);
        } else {
            std::fs::copy(p, target).unwrap();
        }
    }
}
#[test]
fn malformed_inventory_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("conformance/aq-cont-1");
    copy_tree(&package::root(), &root);
    copy_tree(
        &package::root().parent().unwrap().parent().unwrap().join("docs/contracts"),
        &dir.path().join("docs/contracts"),
    );
    copy_tree(
        &package::root().parent().unwrap().parent().unwrap().join("tests"),
        &dir.path().join("tests"),
    );
    package::validate(&root).unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.yaml")).unwrap()).unwrap();
    for mutation in 0..6 {
        let mut m = original.clone();
        match mutation {
            0 => {
                let f = m["fixtures"][0].clone();
                m["fixtures"].as_array_mut().unwrap().push(f);
            }
            1 => {
                m["fixtures"][0].as_object_mut().unwrap().remove("sha256");
            }
            2 => m["fixtures"][0]["path"] = "../escape.json".into(),
            3 => m["fixtures"][0]["driver"] = "unknown".into(),
            4 => m["fixtures"][0]["sha256"] = "0".repeat(64).into(),
            _ => {
                std::fs::write(root.join("fixtures/unregistered.json"), b"{}").unwrap();
            }
        }
        std::fs::write(root.join("manifest.yaml"), serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(package::validate(&root).is_err(), "mutation {mutation} passed");
    }
}

#[path = "harness/report.rs"]
mod report;
#[test]
fn passing_subset_cannot_satisfy_missing_variants_drivers_or_features() {
    use serde_json::json;
    let coverage:package::Coverage=serde_json::from_value(json!({"schema_version":1,"cases":[{"id":"AQ-DD-001","fixtures":["fixture"],"drivers":["embedded"],"required_features":["platform"],"variants":["ordinary","replay","crash"],"assertions":["preservation"],"supplemental_tests":[]}]})).unwrap();
    let ordinary = json!({"fixture_id":"fixture","driver":"embedded","variant":"ordinary","assertion_result":"passed","feature_profile":["platform"]});
    assert_eq!(report::missing_evidence(&coverage, std::slice::from_ref(&ordinary)).len(), 2);
    let records: Vec<_> = ["ordinary", "replay", "crash"]
        .iter()
        .map(|variant| {
            let mut r = ordinary.clone();
            r["variant"] = json!(variant);
            r
        })
        .collect();
    assert!(report::missing_evidence(&coverage, &records).is_empty());
    for field in ["driver", "assertion_result", "feature_profile"] {
        let mut records = records.clone();
        records[2][field] = json!("wrong");
        assert_eq!(report::missing_evidence(&coverage, &records).len(), 1);
    }
}

#[test]
fn evidence_hashes_and_public_driver_variants_are_required() {
    use serde_json::json;
    let (manifest, mut coverage) = package::validate(&package::root()).unwrap();
    let fixture = &manifest.fixtures[0];
    let good = json!({"fixture_id":fixture.id,"fixture_hash":fixture.sha256,"driver":"embedded","variant":"ordinary","assertion_result":"passed"});
    assert!(report::invalid_hashes(&manifest, std::slice::from_ref(&good)).is_empty());
    let mut bad = good.clone();
    bad["fixture_hash"] = json!("0".repeat(64));
    assert_eq!(report::invalid_hashes(&manifest, &[bad]).len(), 1);
    coverage.cases.clear();
    coverage.public_fixtures = vec![fixture.id.clone()];
    coverage.public_drivers = vec!["embedded".into(), "daemon".into()];
    assert_eq!(report::missing_evidence(&coverage, &[good]).len(), 5);
}

#[test]
fn every_storage_variant_and_crash_cut_is_required() {
    use serde_json::{json, Value};
    let root = package::root();
    let (manifest, _) = package::validate(&root).unwrap();
    let mut records = Vec::new();
    for fixture in manifest.fixtures.iter().filter(|f| f.path.starts_with("fixtures/")) {
        let scenario: Value =
            serde_json::from_slice(&std::fs::read(root.join(&fixture.path)).unwrap()).unwrap();
        for variant in ["ordinary", "replay", "backup", "corruption", "crash"] {
            let cuts = if variant == "crash" {
                scenario["recovery_cuts"].as_array().unwrap().clone()
            } else {
                vec![Value::Null]
            };
            for cut in cuts {
                records.push(json!({"fixture_id":fixture.id,"fixture_hash":fixture.sha256,"driver":fixture.driver,"variant":variant,"crash_point":cut,"feature_profile":scenario["required_features"],"assertion_result":"passed"}));
            }
        }
    }
    assert_eq!(records.len(), 56);
    assert!(report::missing_storage_evidence(&root, &manifest, &records).is_empty());
    assert_eq!(report::missing_storage_evidence(&root, &manifest, &[]).len(), records.len());
    for index in 0..records.len() {
        let mut subset = records.clone();
        subset.remove(index);
        assert_eq!(report::missing_storage_evidence(&root, &manifest, &subset).len(), 1);
        for field in ["fixture_hash", "driver", "crash_point", "assertion_result"] {
            let mut corrupted = records.clone();
            corrupted[index][field] = json!("wrong");
            assert_eq!(report::missing_storage_evidence(&root, &manifest, &corrupted).len(), 1);
        }
    }
}

#[test]
fn full_driver_selection_cannot_certify_a_partial_report() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("report.json");
    let output = std::process::Command::new("cargo")
        .args([
            "run",
            "--quiet",
            "--example",
            "aq_conformance",
            "--",
            "--full",
            "--driver",
            "daemon",
            "--report",
        ])
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2), "{}", String::from_utf8_lossy(&output.stderr));
    let report: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(report["passed"], false);
    assert!(report["missing"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v.as_str().unwrap().contains("full profile cannot select a driver")));
    assert_eq!(
        report["missing"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v.as_str().unwrap().starts_with("storage scenario:"))
            .count(),
        56
    );
}
