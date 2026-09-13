//! Coverage is earned by individual passing records, never by a process exit alone.
use serde_json::Value;

use super::package::Coverage;
pub fn missing_evidence(coverage: &Coverage, results: &[Value]) -> Vec<String> {
    let mut missing = Vec::new();
    for case in &coverage.cases {
        if case.fixtures.is_empty() || case.drivers.is_empty() {
            missing.push(format!("{}: no executable mapping", case.id));
            continue;
        }
        for fixture in &case.fixtures {
            for driver in &case.drivers {
                for variant in &case.variants {
                    let passed = results.iter().any(|r| {
                        r["fixture_id"] == *fixture
                            && r["driver"] == *driver
                            && r["variant"] == *variant
                            && r["assertion_result"] == "passed"
                            && case.required_features.iter().all(|feature| {
                                r["feature_profile"]
                                    .as_array()
                                    .is_some_and(|fs| fs.iter().any(|f| f == feature))
                            })
                    });
                    if !passed {
                        missing.push(format!(
                            "{}: {fixture}/{driver}/{variant} has no passing evidence",
                            case.id
                        ));
                    }
                }
            }
        }
    }
    for fixture in &coverage.public_fixtures {
        for driver in &coverage.public_drivers {
            for variant in ["ordinary", "replay", "crash"] {
                if !results.iter().any(|r| {
                    r["fixture_id"] == *fixture
                        && r["driver"] == *driver
                        && r["variant"] == variant
                        && r["assertion_result"] == "passed"
                }) {
                    missing.push(format!("public workload: {fixture}/{driver}/{variant}"));
                }
            }
        }
    }
    missing
}
/// Evidence with the wrong revision's input hash must never satisfy a case.
pub fn invalid_hashes(manifest: &super::package::Manifest, results: &[Value]) -> Vec<String> {
    results
        .iter()
        .filter_map(|r| {
            let f = manifest.fixtures.iter().find(|f| r["fixture_id"] == f.id);
            if f.is_none_or(|f| r["fixture_hash"] != f.sha256) {
                Some(format!("unknown fixture or incorrect evidence hash: {}", r["fixture_id"]))
            } else {
                None
            }
        })
        .collect()
}
