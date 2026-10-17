//! Storage-boundary retained-history measurements. Timings are informational.
#[path = "support/retained_history.rs"]
mod retained_history;

fn main() {
    let mut sizes = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            // Cargo supplies this to custom harnesses.
            "--bench" => {}
            "--retained-runs" => {
                let n = args.next().expect("--retained-runs requires a count");
                sizes
                    .push(n.parse::<usize>().expect("retained runs must be a nonnegative integer"));
            }
            "--help" | "-h" => {
                println!("cargo bench --locked --bench retained_history -- --retained-runs N [--retained-runs N ...]");
                return;
            }
            _ => panic!("unknown argument {arg}; use --retained-runs N"),
        }
    }
    assert!(
        !sizes.is_empty(),
        "supply --retained-runs N (100000 is supported for later revisions)"
    );
    let results: Vec<_> = sizes.into_iter().map(retained_history::run).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "schema_version": 1,
            "structural_expectations": "0.2.0",
            "results": results,
        }))
        .unwrap()
    );
}
