#[path = "../../benches/support/retained_history.rs"]
mod retained_history;

#[test]
fn retained_history_has_shared_structural_costs_and_recovery_parity() {
    // Empty and nonempty retained prefixes use the same 100-operation batches as the bench.
    for n in [0, 7] {
        let result = retained_history::run(n);
        assert_eq!(result["retained_terminal_runs"], n);
        assert_eq!(result["tail_events"], 200);
        assert_eq!(result["final_sequence"], 1 + 2 * (n + retained_history::BATCH));
    }
}

#[test]
fn repeated_samples_start_from_identical_durable_prefixes_and_independent_files() {
    let results = retained_history::run_repeated(7, 2);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["prefix_wal_sha256"], results[1]["prefix_wal_sha256"]);
    for result in results {
        assert_eq!(result["retained_terminal_runs"], 7);
        assert_eq!(result["prefix_events"], 15);
        assert_eq!(result["final_sequence"], 215);
        assert_eq!(result["final_runs"], 107);
    }
}
