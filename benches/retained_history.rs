//! Retained-history checkpoint acceptance: structural counts and paired startup gates.
#[path = "support/retained_history.rs"]
mod retained_history;

fn main() {
    let mut sizes = Vec::new();
    let mut repetitions = 1;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            // Cargo supplies this to custom harnesses.
            "--bench" => {}
            "--fixture-cache" => retained_history::set_fixture_cache(
                args.next().expect("fixture cache requires a path").into(),
            ),
            "--repetitions" => {
                repetitions =
                    args.next().expect("--repetitions requires a count").parse::<usize>().unwrap();
                assert!(repetitions > 0);
            }
            "--retained-runs" => {
                let n = args.next().expect("--retained-runs requires a count");
                sizes
                    .push(n.parse::<usize>().expect("retained runs must be a nonnegative integer"));
            }
            "--help" | "-h" => {
                println!("cargo bench --locked --bench retained_history -- --retained-runs N [--retained-runs N ...] [--repetitions N] [--fixture-cache DIR]");
                return;
            }
            _ => panic!("unknown argument {arg}; use --retained-runs N"),
        }
    }
    assert!(!sizes.is_empty(), "supply --retained-runs N (including 100000)");
    let results: Vec<_> =
        sizes.into_iter().flat_map(|n| retained_history::run_repeated(n, repetitions)).collect();
    fn median(mut values: Vec<f64>) -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }
    let mut gates = Vec::new();
    for n in [10_000, 100_000] {
        let samples: Vec<_> = results.iter().filter(|r| r["retained_terminal_runs"] == n).collect();
        if !samples.is_empty() {
            let ratio = median(
                samples.iter().map(|s| s["paired_startup_ratio"].as_f64().unwrap()).collect(),
            );
            gates.push(serde_json::json!({"retained_runs":n,"median_paired_ratio":ratio,"maximum_ratio":0.5,"passed":ratio<=0.5}));
        }
    }
    let hydration_gate = {
        let at =
            |n| results.iter().filter(|r| r["retained_terminal_runs"] == n).collect::<Vec<_>>();
        let small = at(10_000);
        let big = at(100_000);
        if small.is_empty() || big.is_empty() {
            None
        } else {
            let h = median(big.iter().map(|r| r["hydration_seconds"].as_f64().unwrap()).collect())
                / median(small.iter().map(|r| r["hydration_seconds"].as_f64().unwrap()).collect());
            let size = big[0]["snapshot_bytes"].as_f64().unwrap()
                / small[0]["snapshot_bytes"].as_f64().unwrap();
            Some(
                serde_json::json!({"hydration_ratio":h,"serialized_size_ratio":size,"passed":h<=size}),
            )
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 3,
            "startup_gates":gates,"hydration_gate":hydration_gate,
            "structural_expectations": "AQ-P3",
            "repetitions": repetitions,
            "results": results,
        }))
        .unwrap()
    );
    assert!(gates.iter().all(|g| g["passed"] == true), "paired startup gate failed");
    assert!(
        hydration_gate.as_ref().is_none_or(|g| g["passed"] == true),
        "linear hydration gate failed"
    );
}
