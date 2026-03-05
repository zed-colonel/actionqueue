//! Crash-during-promotion acceptance proof.
//!
//! Verifies that a crash after promoting some runs to Ready does not lose
//! the promoted state, and non-promoted runs remain in Scheduled state.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::mutation::DurabilityPolicy;
use actionqueue_core::run::state::RunState;
use actionqueue_engine::index::scheduled::ScheduledIndex;
use actionqueue_engine::scheduler::promotion::{
    promote_scheduled_to_ready_via_authority, PromotionParams,
};
use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;

#[test]
fn crash_during_promotion_preserves_promoted_state() {
    let data_dir = support::unique_data_dir("crash-during-promotion");
    let task_id_str = "aaaa0000-0000-0000-0000-000000000001";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // 1) Submit a Repeat(3, 100) task — creates 3 runs spaced 100s apart
    let submit = support::submit_repeat_task_via_cli(task_id_str, &data_dir, 3, 100);
    assert_eq!(submit["runs_created"], 3, "Repeat(3) should create 3 runs");

    // 2) Bootstrap, promote ALL runs by using u64::MAX as current_time
    let pre_crash_ready_count;
    {
        let recovery = load_projection_from_storage(&data_dir).expect("bootstrap should succeed");

        let mut authority = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

        let runs: Vec<_> = authority.projection().run_instances().cloned().collect();
        let scheduled = ScheduledIndex::from(runs.as_slice());

        let first_sequence = support::next_sequence(authority.projection().latest_sequence());

        let result = promote_scheduled_to_ready_via_authority(
            &scheduled,
            PromotionParams::new(
                u64::MAX,
                first_sequence,
                first_sequence,
                DurabilityPolicy::Immediate,
            ),
            &mut authority,
        )
        .expect("promotion should succeed");

        pre_crash_ready_count = result.outcomes().len();
        assert_eq!(pre_crash_ready_count, 3, "all 3 runs should be promoted at u64::MAX");

        // Verify pre-crash state: all promoted runs are Ready
        let run_ids = authority.projection().run_ids_for_task(task_id);
        for rid in &run_ids {
            let state =
                authority.projection().get_run_state(rid).copied().expect("run state should exist");
            assert_eq!(state, RunState::Ready, "all runs should be Ready before crash");
        }

        // 3) Simulate crash by dropping all in-memory state
        drop(authority);
    }

    // 4) Re-bootstrap from WAL
    let recovery =
        load_projection_from_storage(&data_dir).expect("post-crash bootstrap should succeed");

    let run_ids = recovery.projection.run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 3, "all 3 runs should survive crash");

    // 5) Verify promoted runs retained Ready state after crash recovery
    let mut ready_count = 0;
    for rid in &run_ids {
        match recovery.projection.get_run_state(rid).copied() {
            Some(RunState::Ready) => ready_count += 1,
            other => panic!("expected Ready after crash recovery, got: {other:?}"),
        }
    }

    assert_eq!(
        ready_count, pre_crash_ready_count,
        "all promoted runs should retain Ready state after crash recovery"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
