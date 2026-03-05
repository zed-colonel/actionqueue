//! Property-based tests for run accounting invariants.

use std::collections::HashSet;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_engine::derive::derive_runs;
use actionqueue_engine::time::clock::MockClock;
use proptest::prelude::*;

fn config() -> ProptestConfig {
    ProptestConfig::with_cases(256)
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn once_policy_produces_exactly_one_scheduled_run(_seed in 0u64..u64::MAX) {
        let clock = MockClock::new(1000);
        let task_id = TaskId::new();

        let result = derive_runs(&clock, task_id, &RunPolicy::Once, 0, 1000)
            .expect("Once derivation must succeed");

        prop_assert_eq!(result.derived().len(), 1);
        prop_assert_eq!(result.already_derived(), 1);
        prop_assert_eq!(result.derived()[0].state(), RunState::Scheduled);
    }

    #[test]
    fn repeat_policy_produces_exactly_n_scheduled_runs_with_unique_ids(
        n in 1u32..100,
        interval in 1u64..10000,
    ) {
        let clock = MockClock::new(1000);
        let task_id = TaskId::new();
        let policy = RunPolicy::repeat(n, interval).expect("policy must be valid");

        let result = derive_runs(&clock, task_id, &policy, 0, 1000)
            .expect("Repeat derivation must succeed");

        // Exactly n runs produced
        prop_assert_eq!(result.derived().len(), n as usize);
        prop_assert_eq!(result.already_derived(), n);

        // All runs in Scheduled state
        for run in result.derived() {
            prop_assert_eq!(run.state(), RunState::Scheduled);
        }

        // No duplicate RunIds
        let ids: HashSet<_> = result.derived().iter().map(|r| r.id()).collect();
        prop_assert_eq!(ids.len(), n as usize);
    }
}
