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
