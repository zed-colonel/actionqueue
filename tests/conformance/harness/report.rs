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
    missing
}
