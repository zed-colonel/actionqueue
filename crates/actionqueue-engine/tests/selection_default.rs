//! Integration tests for default selector determinism.

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_engine::index::ready::ReadyIndex;
use actionqueue_engine::selection::default_selector::{
    ready_inputs_from_index, select_ready_runs, ReadyRunSelectionInput, SelectionResult,
};

#[test]
fn selects_mixed_priorities_desc_then_fifo_tie_break() {
    let ready = ReadyIndex::from_runs(vec![
        RunInstance::new_ready(TaskId::new(), 900, 1300, 1).expect("valid ready run"),
        RunInstance::new_ready(TaskId::new(), 900, 1100, 5).expect("valid ready run"),
        RunInstance::new_ready(TaskId::new(), 900, 1200, 5).expect("valid ready run"),
        RunInstance::new_ready(TaskId::new(), 900, 1000, 8).expect("valid ready run"),
    ]);

    let inputs = ready_inputs_from_index(&ready);
    let result = select_ready_runs(&inputs);

    let ordered: Vec<(i32, u64)> =
        result.selected().iter().map(|run| (run.effective_priority(), run.created_at())).collect();

    assert_eq!(ordered, vec![(8, 1000), (5, 1100), (5, 1200), (1, 1300)]);
    assert!(result.remaining().is_empty());
}

#[test]
fn uses_selector_input_priority_snapshot_not_mutated_run_value() {
    let run = RunInstance::new_ready(TaskId::new(), 900, 1000, 99).expect("valid ready run");
    let run_id = run.id();

    let low_snapshot = ReadyRunSelectionInput::new(run.clone(), 1);
    let high_snapshot = ReadyRunSelectionInput::new(
        RunInstance::new_ready(TaskId::new(), 900, 1200, -7).expect("valid ready run"),
        10,
    );

    let result = select_ready_runs(&[low_snapshot, high_snapshot]);

    assert_eq!(result.selected().len(), 2);
    // lower snapshot priority run should be second despite having a high run field priority
    assert_eq!(result.selected()[1].id(), run_id);
}

/// Verifies that selector produces identical output regardless of input order (determinism).
#[test]
fn selector_is_deterministic_with_shuffled_input_order() {
    let run_a = RunInstance::new_ready(TaskId::new(), 900, 1000, 5).expect("valid ready run");
    let run_b = RunInstance::new_ready(TaskId::new(), 900, 1100, 3).expect("valid ready run");
    let run_c = RunInstance::new_ready(TaskId::new(), 900, 1200, 5).expect("valid ready run");
    let run_d = RunInstance::new_ready(TaskId::new(), 900, 1300, 3).expect("valid ready run");

    // Collect all runs to create multiple input orderings
    let all_runs = vec![run_a.clone(), run_b.clone(), run_c.clone(), run_d.clone()];

    // Generate all permutations of the input orderings
    let mut results = Vec::new();

    // Helper to generate permutations and run selector
    fn permute_and_test(
        current: &mut Vec<ReadyRunSelectionInput>,
        remaining: &[ReadyRunSelectionInput],
        results: &mut Vec<SelectionResult>,
    ) {
        if remaining.is_empty() {
            let result = select_ready_runs(current);
            results.push(result);
            return;
        }

        for i in 0..remaining.len() {
            current.push(remaining[i].clone());
            let mut next_remaining = remaining.to_vec();
            next_remaining.remove(i);
            permute_and_test(current, &next_remaining, results);
            current.pop();
        }
    }

    let inputs: Vec<ReadyRunSelectionInput> =
        all_runs.iter().map(|run| ReadyRunSelectionInput::from_ready_run(run.clone())).collect();

    permute_and_test(&mut Vec::new(), &inputs, &mut results);

    // All results must be identical (deterministic)
    let first_result = results.first().unwrap();
    for result in &results {
        let first_ordered: Vec<(i32, u64)> = first_result
            .selected()
            .iter()
            .map(|r| (r.effective_priority(), r.created_at()))
            .collect();
        let result_ordered: Vec<(i32, u64)> =
            result.selected().iter().map(|r| (r.effective_priority(), r.created_at())).collect();
        assert_eq!(
            first_ordered, result_ordered,
            "All input orderings must produce same selection order"
        );
    }
}

/// Verifies that runs with identical priority maintain FIFO order deterministically.
#[test]
fn identical_priority_maintains_fifo_deterministically() {
    let now = 1000;
    let task_id = TaskId::new();

    // Create runs with identical priority but different created_at times
    let run_1 = ReadyRunSelectionInput::new(
        RunInstance::new_ready(task_id, now - 100, now + 10, 5).expect("valid ready run"),
        5,
    );
    let run_2 = ReadyRunSelectionInput::new(
        RunInstance::new_ready(task_id, now - 90, now + 20, 5).expect("valid ready run"),
        5,
    );
    let run_3 = ReadyRunSelectionInput::new(
        RunInstance::new_ready(task_id, now - 80, now + 30, 5).expect("valid ready run"),
        5,
    );

    // Intentionally shuffle input order
    let inputs = vec![run_3.clone(), run_1.clone(), run_2.clone()];

    let result = select_ready_runs(&inputs);

    // Despite shuffled input, output must be FIFO (earliest created_at first)
    let ordered: Vec<u64> = result.selected().iter().map(|r| r.created_at()).collect();
    assert_eq!(ordered, vec![now + 10, now + 20, now + 30], "FIFO order must be deterministic");

    // All should be selected (no remaining)
    assert!(result.remaining().is_empty());
}

/// Verifies that selection result is stable across multiple runs with same input.
#[test]
fn selector_result_is_stable_across_multiple_runs() {
    let run_a = RunInstance::new_ready(TaskId::new(), 900, 1000, 8).expect("valid ready run");
    let run_b = RunInstance::new_ready(TaskId::new(), 900, 1100, 5).expect("valid ready run");
    let run_c = RunInstance::new_ready(TaskId::new(), 900, 1200, 5).expect("valid ready run");
    let run_d = RunInstance::new_ready(TaskId::new(), 900, 1300, 1).expect("valid ready run");

    let inputs = vec![
        ReadyRunSelectionInput::from_ready_run(run_a.clone()),
        ReadyRunSelectionInput::from_ready_run(run_b.clone()),
        ReadyRunSelectionInput::from_ready_run(run_c.clone()),
        ReadyRunSelectionInput::from_ready_run(run_d.clone()),
    ];

    // Run selector multiple times with same input
    let results: Vec<SelectionResult> = (0..5).map(|_| select_ready_runs(&inputs)).collect();

    // All results must be identical (stable)
    let first = &results[0];
    for result in &results[1..] {
        assert_eq!(first.selected().len(), result.selected().len());
        assert_eq!(first.remaining().len(), result.remaining().len());

        for (i, (first_run, result_run)) in
            first.selected().iter().zip(result.selected()).enumerate()
        {
            assert_eq!(
                first_run.id(),
                result_run.id(),
                "Selected run at index {i} must be identical"
            );
            assert_eq!(
                first_run.effective_priority(),
                result_run.effective_priority(),
                "Priority must match at index {i}"
            );
            assert_eq!(
                first_run.created_at(),
                result_run.created_at(),
                "created_at must match at index {i}"
            );
        }
    }
}

/// Verifies priority-then-FIFO ordering is applied correctly with complex mix.
#[test]
fn complex_mixed_priorities_deterministic_order() {
    // Create runs with various priorities and creation times
    let runs = vec![
        // Priority 10: created at 300 (should be selected 2nd among p10)
        RunInstance::new_ready(TaskId::new(), 0, 300, 10).expect("valid ready run"),
        // Priority 5: created at 100 (should be selected 4th)
        RunInstance::new_ready(TaskId::new(), 0, 100, 5).expect("valid ready run"),
        // Priority 10: created at 200 (should be selected 1st among p10)
        RunInstance::new_ready(TaskId::new(), 0, 200, 10).expect("valid ready run"),
        // Priority 5: created at 250 (should be selected 5th)
        RunInstance::new_ready(TaskId::new(), 0, 250, 5).expect("valid ready run"),
        // Priority 10: created at 100 (should be selected 3rd among p10)
        RunInstance::new_ready(TaskId::new(), 0, 100, 10).expect("valid ready run"),
        // Priority 1: created at 400 (should be selected last)
        RunInstance::new_ready(TaskId::new(), 0, 400, 1).expect("valid ready run"),
    ];

    let inputs: Vec<ReadyRunSelectionInput> =
        runs.iter().map(|r| ReadyRunSelectionInput::from_ready_run(r.clone())).collect();

    // Intentionally shuffle input to test determinism
    let mut shuffled_inputs = inputs.clone();
    shuffled_inputs.swap(0, 3);
    shuffled_inputs.swap(1, 4);

    let result = select_ready_runs(&shuffled_inputs);

    // Expected order: priority 10 (FIFO: 100, 200, 300), priority 5 (FIFO: 100, 250), priority 1 (400)
    let expected_order: Vec<(i32, u64)> = vec![
        (10, 100), // p10, FIFO 1st
        (10, 200), // p10, FIFO 2nd
        (10, 300), // p10, FIFO 3rd
        (5, 100),  // p5, FIFO 1st
        (5, 250),  // p5, FIFO 2nd
        (1, 400),  // p1, FIFO 1st (only one)
    ];

    let actual_order: Vec<(i32, u64)> =
        result.selected().iter().map(|r| (r.effective_priority(), r.created_at())).collect();

    assert_eq!(
        actual_order, expected_order,
        "Complex mixed priorities must be deterministically ordered"
    );
    assert!(result.remaining().is_empty());
}

/// Verifies exact tie-breaking by RunId when priority and created_at are identical.
/// This test creates permutations of runs that are identical in priority and created_at
/// but have distinct RunIds, validating that the selector produces identical, deterministic
/// output regardless of input order.
#[test]
fn exact_tie_breaks_deterministically_by_run_id() {
    let now = 1000;
    let priority = 5;

    // Create runs with identical priority and created_at but distinct TaskIds (hence distinct RunIds)
    let run_a = ReadyRunSelectionInput::new(
        RunInstance::new_ready(TaskId::new(), now - 100, now, priority).expect("valid ready run"),
        priority,
    );
    let run_b = ReadyRunSelectionInput::new(
        RunInstance::new_ready(TaskId::new(), now - 100, now, priority).expect("valid ready run"),
        priority,
    );
    let run_c = ReadyRunSelectionInput::new(
        RunInstance::new_ready(TaskId::new(), now - 100, now, priority).expect("valid ready run"),
        priority,
    );

    // Captured RunIds for later comparison
    let run_ids = vec![run_a.run().id(), run_b.run().id(), run_c.run().id()];

    // Generate all permutations of input orderings and verify deterministic output
    let all_runs = vec![run_a.clone(), run_b.clone(), run_c.clone()];
    let mut results = Vec::new();

    // Helper to generate permutations
    fn permute_and_test(
        current: &mut Vec<ReadyRunSelectionInput>,
        remaining: &[ReadyRunSelectionInput],
        results: &mut Vec<SelectionResult>,
    ) {
        if remaining.is_empty() {
            let result = select_ready_runs(current);
            results.push(result);
            return;
        }

        for i in 0..remaining.len() {
            current.push(remaining[i].clone());
            let mut next_remaining = remaining.to_vec();
            next_remaining.remove(i);
            permute_and_test(current, &next_remaining, results);
            current.pop();
        }
    }

    let inputs: Vec<ReadyRunSelectionInput> = all_runs.to_vec();
    permute_and_test(&mut Vec::new(), &inputs, &mut results);

    // All results must be identical
    let first_result = results.first().unwrap();
    for result in &results {
        assert_eq!(
            first_result.selected().len(),
            result.selected().len(),
            "All permutations must select same number of runs"
        );

        // Verify runs are in RunId order (deterministic tie-breaking)
        let first_ids: Vec<_> = first_result.selected().iter().map(|r| r.id()).collect();
        let result_ids: Vec<_> = result.selected().iter().map(|r| r.id()).collect();

        // The expected order should be sorted by RunId for exact ties
        let mut expected_ids = run_ids.clone();
        expected_ids.sort();
        assert_eq!(first_ids, expected_ids, "Exactly tied runs must be ordered by RunId");
        assert_eq!(result_ids, expected_ids, "All permutations must produce same RunId order");
    }

    // All runs selected, none remaining (all results are identical so check any result)
    assert!(results[0].remaining().is_empty());
}
