//! P6-011 Concurrent mutation boundary acceptance proofs.
//!
//! Verifies that WAL sequence monotonicity is enforced when concurrent or
//! overlapping mutation attempts occur, and that the mutation authority
//! prevents sequence collisions. Three scenarios:
//!
//! 1. A stale/duplicate sequence is rejected by the authority with
//!    `NonMonotonicSequence`.
//! 2. Sequential mutations through the full run lifecycle preserve strict
//!    monotonicity on each transition.
//! 3. The `WalFsWriter` rejects non-monotonic sequence appends at the WAL
//!    layer, independent of the authority.

mod support;

use std::path::PathBuf;
use std::str::FromStr;

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::mutation::{
    DurabilityPolicy, MutationAuthority, MutationCommand, RunStateTransitionCommand,
};
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::mutation::authority::{
    MutationAuthorityError, MutationValidationError, StorageMutationAuthority,
};
use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::writer::{WalWriter, WalWriterError};

/// Helper to compute next sequence from authority's projection.
fn next_seq<W, P>(authority: &StorageMutationAuthority<W, P>) -> u64
where
    W: WalWriter,
    P: actionqueue_storage::mutation::authority::MutationProjection,
{
    authority.projection().latest_sequence().checked_add(1).expect("sequence should not overflow")
}

/// Creates a deterministic test WAL directory isolated per scenario label.
fn wal_test_dir(label: &str) -> PathBuf {
    let dir = PathBuf::from("target").join("tmp").join(format!(
        "{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("WAL test dir should be creatable");
    dir
}

/// Creates a deterministic TaskSpec for test event construction.
fn test_task_spec(task_id: TaskId) -> TaskSpec {
    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(
            b"p6-011-concurrent-mutation-payload".to_vec(),
            "application/octet-stream",
        ),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

// ---------------------------------------------------------------------------
// Test 1: A stale/duplicate sequence (equal to latest rather than latest+1)
// is rejected by the mutation authority with NonMonotonicSequence.
// ---------------------------------------------------------------------------
#[test]
fn cm_a_sequence_collision_rejected_by_authority() {
    let data_dir = support::unique_data_dir("concurrent-mutation-a");
    let task_id_str = "a6a60011-0001-0001-0001-000000000001";

    // Step 1: Submit a Once task via CLI helper.
    support::submit_once_task_via_cli(task_id_str, &data_dir);

    // Step 2: Bootstrap the projection from storage.
    let recovery =
        load_projection_from_storage(&data_dir).expect("storage bootstrap should succeed");

    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Locate the sole run for the task.
    let run_ids = recovery.projection.run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "task should have exactly one run");
    let run_id = run_ids[0];

    // Step 3: Create a StorageMutationAuthority from the recovery.
    let mut authority = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

    // Record the projection state before the stale command.
    let pre_sequence = authority.projection().latest_sequence();
    let pre_run_state = authority.projection().get_run_state(&run_id).copied();

    // Step 4: Submit a RunStateTransition with sequence = latest_sequence()
    // (stale: should be latest_sequence() + 1).
    let stale_sequence = authority.projection().latest_sequence();
    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            stale_sequence,
            run_id,
            RunState::Scheduled,
            RunState::Ready,
            stale_sequence,
        )),
        DurabilityPolicy::Immediate,
    );

    // Step 5: Assert rejection with NonMonotonicSequence.
    assert!(result.is_err(), "stale sequence must be rejected by authority");
    match result.unwrap_err() {
        MutationAuthorityError::Validation(MutationValidationError::NonMonotonicSequence {
            expected,
            provided,
        }) => {
            assert_eq!(
                provided, stale_sequence,
                "provided sequence in error must match the stale value"
            );
            assert_eq!(
                expected,
                stale_sequence + 1,
                "expected sequence in error must be latest + 1"
            );
        }
        other => {
            panic!("expected NonMonotonicSequence validation error, got: {other:?}");
        }
    }

    // Step 6: Verify projection state is unchanged.
    assert_eq!(
        authority.projection().latest_sequence(),
        pre_sequence,
        "projection sequence must be unchanged after rejected stale command"
    );
    assert_eq!(
        authority.projection().get_run_state(&run_id).copied(),
        pre_run_state,
        "run state must be unchanged after rejected stale command"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Sequential mutations through the run lifecycle preserve strict
// monotonicity. Each transition must produce a strictly greater sequence.
// ---------------------------------------------------------------------------
#[test]
fn cm_b_sequential_mutations_preserve_monotonicity() {
    let data_dir = support::unique_data_dir("concurrent-mutation-b");
    let task_id_str = "b6b60011-0002-0002-0002-000000000002";

    // Step 1: Submit a Once task.
    support::submit_once_task_via_cli(task_id_str, &data_dir);

    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Step 2: Bootstrap and create authority.
    let recovery =
        load_projection_from_storage(&data_dir).expect("storage bootstrap should succeed");

    let run_ids = recovery.projection.run_ids_for_task(task_id);
    assert_eq!(run_ids.len(), 1, "task should have exactly one run");
    let run_id = run_ids[0];

    let mut authority = StorageMutationAuthority::new(recovery.wal_writer, recovery.projection);

    // Record sequence after bootstrap.
    let mut previous_sequence = authority.projection().latest_sequence();
    assert!(previous_sequence > 0, "initial sequence must be > 0 after task submission");

    // Transition 1: Scheduled -> Ready.
    let seq1 = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                seq1,
                run_id,
                RunState::Scheduled,
                RunState::Ready,
                seq1,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("Scheduled -> Ready should succeed");

    let after_ready = authority.projection().latest_sequence();
    assert!(
        after_ready > previous_sequence,
        "sequence must strictly increase after Scheduled -> Ready: {previous_sequence} -> \
         {after_ready}"
    );
    previous_sequence = after_ready;

    // Transition 2: Ready -> Leased.
    let seq2 = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                seq2,
                run_id,
                RunState::Ready,
                RunState::Leased,
                seq2,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("Ready -> Leased should succeed");

    let after_leased = authority.projection().latest_sequence();
    assert!(
        after_leased > previous_sequence,
        "sequence must strictly increase after Ready -> Leased: {previous_sequence} -> \
         {after_leased}"
    );
    previous_sequence = after_leased;

    // Transition 3: Leased -> Running.
    let seq3 = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                seq3,
                run_id,
                RunState::Leased,
                RunState::Running,
                seq3,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("Leased -> Running should succeed");

    let after_running = authority.projection().latest_sequence();
    assert!(
        after_running > previous_sequence,
        "sequence must strictly increase after Leased -> Running: {previous_sequence} -> \
         {after_running}"
    );

    // Verify the full monotonic chain: seq1 < seq2 < seq3.
    assert!(
        seq1 < seq2 && seq2 < seq3,
        "sequence numbers must form a strict monotonic chain: {seq1} < {seq2} < {seq3}"
    );

    // Verify latest_sequence() grew monotonically throughout.
    assert_eq!(after_running, seq3, "latest_sequence must equal the last applied command sequence");
}

// ---------------------------------------------------------------------------
// Test 3: The WalFsWriter itself rejects non-monotonic sequence appends,
// independent of the authority layer.
// ---------------------------------------------------------------------------
#[test]
fn cm_c_wal_writer_rejects_non_monotonic_append() {
    let dir = wal_test_dir("p6-011-cm-wal-writer");
    let wal_path = dir.join("actionqueue.wal");

    let task_id = TaskId::new();
    let task_spec = test_task_spec(task_id);

    // Step 1: Create a WAL file with WalFsWriter.
    let mut writer = WalFsWriter::new(wal_path.clone())
        .expect("WAL writer creation should succeed for new file");

    // Step 2: Append an event with sequence=1.
    let event1 = WalEvent::new(
        1,
        WalEventType::TaskCreated { task_spec: task_spec.clone(), timestamp: 1000 },
    );
    writer.append(&event1).expect("append with sequence=1 should succeed");
    assert_eq!(writer.current_sequence(), 1, "current_sequence must be 1 after first append");

    // Step 3: Try to append another event with sequence=1 (duplicate).
    let run_id = RunId::new();
    let run_instance = RunInstance::new_scheduled_with_id(run_id, task_id, 100, 100)
        .expect("scheduled run should be valid");

    let event_dup =
        WalEvent::new(1, WalEventType::RunCreated { run_instance: run_instance.clone() });
    let dup_result = writer.append(&event_dup);

    // Step 4: Assert the duplicate is rejected with SequenceViolation.
    assert!(
        matches!(dup_result, Err(WalWriterError::SequenceViolation { expected: 2, provided: 1 })),
        "duplicate sequence=1 must be rejected with SequenceViolation, got: {dup_result:?}"
    );

    // Step 5: Try to append an event with sequence=0 (backwards).
    let event_backward = WalEvent::new(0, WalEventType::EnginePaused { timestamp: 2000 });
    let backward_result = writer.append(&event_backward);

    // Step 6: Assert rejection for backward sequence.
    assert!(
        matches!(
            backward_result,
            Err(WalWriterError::SequenceViolation { expected: 2, provided: 0 })
        ),
        "backward sequence=0 must be rejected with SequenceViolation, got: {backward_result:?}"
    );

    // Step 7: Append with sequence=2 and verify success.
    let event2 = WalEvent::new(2, WalEventType::RunCreated { run_instance });
    writer.append(&event2).expect("append with sequence=2 should succeed after rejected appends");
    assert_eq!(writer.current_sequence(), 2, "current_sequence must be 2 after valid append");

    // Verify the writer is still usable: sequence=3 should also succeed.
    let event3 = WalEvent::new(
        3,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 3000,
        },
    );
    writer.append(&event3).expect("append with sequence=3 should succeed");
    assert_eq!(writer.current_sequence(), 3, "current_sequence must be 3 after third append");

    writer.flush().expect("flush should succeed");
    writer.close().expect("close should succeed");
}
