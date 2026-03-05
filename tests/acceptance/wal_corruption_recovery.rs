//! P6-003 WAL corruption recovery acceptance proof.
//!
//! Verifies that WAL corruption recovery works correctly with
//! `RepairPolicy::TruncatePartial`. The test writes valid events, appends
//! corrupt trailing bytes, bootstraps with repair, and asserts that all valid
//! events survive intact and new appends succeed post-recovery.

mod support;

use std::io::Write;
use std::path::PathBuf;

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::wal::codec;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_reader::WalFsReader;
use actionqueue_storage::wal::fs_writer::{WalFsWriter, WalFsWriterInitError};
use actionqueue_storage::wal::reader::WalReader;
use actionqueue_storage::wal::repair::RepairPolicy;
use actionqueue_storage::wal::writer::WalWriter;

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
            b"p6-003-wal-corruption-payload".to_vec(),
            "application/octet-stream",
        ),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

/// Builds a deterministic sequence of N WAL events exercising multiple event
/// types (TaskCreated, RunCreated, RunStateChanged) for thorough recovery
/// verification.
fn build_valid_events(n: usize) -> Vec<WalEvent> {
    assert!(n >= 4, "scenario requires at least 4 events for meaningful coverage");

    let task_id = TaskId::new();
    let run_id = RunId::new();
    let task_spec = test_task_spec(task_id);

    let run_instance = RunInstance::new_scheduled_with_id(run_id, task_id, 100, 100)
        .expect("scheduled run should be valid");

    let mut events = Vec::with_capacity(n);

    // Event 1: TaskCreated
    events.push(WalEvent::new(1, WalEventType::TaskCreated { task_spec, timestamp: 1000 }));

    // Event 2: RunCreated
    events.push(WalEvent::new(2, WalEventType::RunCreated { run_instance }));

    // Event 3: RunStateChanged Scheduled -> Ready
    events.push(WalEvent::new(
        3,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Scheduled,
            new_state: RunState::Ready,
            timestamp: 2000,
        },
    ));

    // Event 4: RunStateChanged Ready -> Leased
    events.push(WalEvent::new(
        4,
        WalEventType::RunStateChanged {
            run_id,
            previous_state: RunState::Ready,
            new_state: RunState::Leased,
            timestamp: 3000,
        },
    ));

    // Fill remaining slots with EnginePaused/EngineResumed pairs if n > 4.
    for seq in 5..=n {
        let event_type = if seq % 2 == 1 {
            WalEventType::EnginePaused { timestamp: (seq as u64) * 1000 }
        } else {
            WalEventType::EngineResumed { timestamp: (seq as u64) * 1000 }
        };
        events.push(WalEvent::new(seq as u64, event_type));
    }

    events
}

/// Writes events to a WAL file using `WalFsWriter` and flushes to disk.
fn write_events_to_wal(wal_path: &std::path::Path, events: &[WalEvent]) {
    let mut writer = WalFsWriter::new(wal_path.to_path_buf())
        .expect("WAL writer creation should succeed for valid empty file");
    for event in events {
        writer.append(event).expect("WAL append should succeed for valid event");
    }
    writer.flush().expect("WAL flush should succeed");
    writer.close().expect("WAL close should succeed");
}

/// Appends raw corrupt bytes to the end of a WAL file, simulating a crash
/// mid-write that left an incomplete record.
fn append_corrupt_trailing_bytes(wal_path: &std::path::Path, variant: CorruptionVariant) {
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(wal_path)
        .expect("WAL file should be openable for corruption injection");

    match variant {
        CorruptionVariant::IncompleteHeader => {
            // Write only 6 bytes of what should be a 12-byte header.
            file.write_all(&codec::VERSION.to_le_bytes()).expect("corruption write should succeed");
            file.write_all(&[0xAB, 0xCD]).expect("corruption write should succeed");
        }
        CorruptionVariant::IncompletePayload => {
            // Write a complete header declaring 80 bytes of payload, then only 15.
            file.write_all(&codec::VERSION.to_le_bytes()).expect("corruption write should succeed");
            file.write_all(&80u32.to_le_bytes()).expect("corruption write should succeed");
            file.write_all(&0u32.to_le_bytes()).expect("corruption write should succeed"); // CRC (dummy)
            file.write_all(&[0xDE; 15]).expect("corruption write should succeed");
        }
        CorruptionVariant::GarbageBytes => {
            // Write arbitrary non-framed garbage bytes.
            file.write_all(&[0xFF; 7]).expect("corruption write should succeed");
        }
    }

    file.sync_all().expect("corruption flush should succeed");
}

/// Reads all events from a WAL file using `WalFsReader` and returns them.
fn read_all_events(wal_path: &std::path::Path) -> Vec<WalEvent> {
    let mut reader =
        WalFsReader::new(wal_path.to_path_buf()).expect("WAL reader should open successfully");
    let mut events = Vec::new();
    loop {
        match reader.read_next() {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break,
            Err(e) => panic!("unexpected WAL read error during verification: {e}"),
        }
    }
    events
}

/// Corruption patterns exercised in the acceptance suite.
#[derive(Debug, Clone, Copy)]
enum CorruptionVariant {
    /// Partial header: fewer than HEADER_LEN bytes at the tail.
    IncompleteHeader,
    /// Complete header but payload shorter than declared length.
    IncompletePayload,
    /// Raw garbage that does not even start with a valid version.
    GarbageBytes,
}

// ---------------------------------------------------------------------------
// Test: TruncatePartial repairs incomplete-header corruption and recovers all
// valid events.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_truncate_partial_repairs_incomplete_header() {
    let dir = wal_test_dir("p6-003-cr-incomplete-header");
    let wal_path = dir.join("actionqueue.wal");
    let valid_events = build_valid_events(6);

    // Step 1: Write valid events.
    write_events_to_wal(&wal_path, &valid_events);

    // Step 2: Append corrupt trailing bytes (incomplete header).
    append_corrupt_trailing_bytes(&wal_path, CorruptionVariant::IncompleteHeader);

    // Verify: Strict mode rejects the corruption.
    let strict_result = WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::Strict);
    assert!(
        strict_result.is_err(),
        "strict policy must reject WAL with trailing incomplete header"
    );

    // Step 3: Bootstrap with TruncatePartial.
    let repaired_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial should repair incomplete header corruption");

    // Step 4: Verify all valid events are recovered.
    let recovered = read_all_events(&wal_path);
    assert_eq!(
        recovered.len(),
        valid_events.len(),
        "all {n} valid events must survive TruncatePartial repair (incomplete header)",
        n = valid_events.len()
    );
    for (i, (recovered_event, original_event)) in
        recovered.iter().zip(valid_events.iter()).enumerate()
    {
        assert_eq!(
            recovered_event, original_event,
            "event at index {i} must be identical after repair"
        );
    }

    // Verify current sequence matches the last valid event.
    assert_eq!(
        repaired_writer.current_sequence(),
        valid_events.last().unwrap().sequence(),
        "writer current_sequence must equal last valid event sequence after repair"
    );

    // Step 5: Verify new appends succeed after repair.
    let mut writer = repaired_writer;
    let next_seq = valid_events.last().unwrap().sequence() + 1;
    let new_event = WalEvent::new(next_seq, WalEventType::EnginePaused { timestamp: 99_000 });
    writer.append(&new_event).expect("append after TruncatePartial repair should succeed");
    writer.flush().expect("flush after repair append should succeed");
    writer.close().expect("close after repair append should succeed");

    // Verify the new event is readable.
    let all_events = read_all_events(&wal_path);
    assert_eq!(
        all_events.len(),
        valid_events.len() + 1,
        "WAL should contain original events plus the new post-repair append"
    );
    assert_eq!(all_events.last().unwrap().sequence(), next_seq);

    // Strict re-open must now succeed (file is clean after repair + append).
    let strict_reopen = WalFsWriter::new(wal_path.clone());
    assert!(
        strict_reopen.is_ok(),
        "strict re-open must succeed after TruncatePartial repair and clean append"
    );
    assert_eq!(strict_reopen.unwrap().current_sequence(), next_seq);
}

// ---------------------------------------------------------------------------
// Test: TruncatePartial repairs incomplete-payload corruption and recovers all
// valid events.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_truncate_partial_repairs_incomplete_payload() {
    let dir = wal_test_dir("p6-003-cr-incomplete-payload");
    let wal_path = dir.join("actionqueue.wal");
    let valid_events = build_valid_events(8);

    // Step 1: Write valid events.
    write_events_to_wal(&wal_path, &valid_events);

    // Step 2: Append corrupt trailing bytes (incomplete payload).
    append_corrupt_trailing_bytes(&wal_path, CorruptionVariant::IncompletePayload);

    // Verify: Strict mode rejects.
    let strict_result = WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::Strict);
    assert!(
        strict_result.is_err(),
        "strict policy must reject WAL with trailing incomplete payload"
    );

    // Step 3: Bootstrap with TruncatePartial.
    let repaired_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial should repair incomplete payload corruption");

    // Step 4: Verify all valid events are recovered.
    let recovered = read_all_events(&wal_path);
    assert_eq!(
        recovered.len(),
        valid_events.len(),
        "all {n} valid events must survive TruncatePartial repair (incomplete payload)",
        n = valid_events.len()
    );
    for (i, (recovered_event, original_event)) in
        recovered.iter().zip(valid_events.iter()).enumerate()
    {
        assert_eq!(
            recovered_event, original_event,
            "event at index {i} must be identical after repair"
        );
    }

    assert_eq!(
        repaired_writer.current_sequence(),
        valid_events.last().unwrap().sequence(),
        "writer current_sequence must equal last valid event sequence after repair"
    );

    // Step 5: Verify new appends succeed after repair.
    let mut writer = repaired_writer;
    let next_seq = valid_events.last().unwrap().sequence() + 1;
    let new_event = WalEvent::new(next_seq, WalEventType::EngineResumed { timestamp: 100_000 });
    writer.append(&new_event).expect("append after TruncatePartial repair should succeed");
    writer.flush().expect("flush after repair append should succeed");
    writer.close().expect("close after repair append should succeed");

    let all_events = read_all_events(&wal_path);
    assert_eq!(all_events.len(), valid_events.len() + 1);
    assert_eq!(all_events.last().unwrap().sequence(), next_seq);

    let strict_reopen = WalFsWriter::new(wal_path.clone());
    assert!(strict_reopen.is_ok());
    assert_eq!(strict_reopen.unwrap().current_sequence(), next_seq);
}

// ---------------------------------------------------------------------------
// Test: TruncatePartial repairs garbage-byte corruption and recovers all
// valid events.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_truncate_partial_repairs_garbage_bytes() {
    let dir = wal_test_dir("p6-003-cr-garbage-bytes");
    let wal_path = dir.join("actionqueue.wal");
    let valid_events = build_valid_events(5);

    // Step 1: Write valid events.
    write_events_to_wal(&wal_path, &valid_events);

    // Step 2: Append garbage bytes (not even a valid version prefix).
    append_corrupt_trailing_bytes(&wal_path, CorruptionVariant::GarbageBytes);

    // Verify: Strict mode rejects.
    let strict_result = WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::Strict);
    assert!(strict_result.is_err(), "strict policy must reject WAL with trailing garbage bytes");

    // Step 3: Bootstrap with TruncatePartial.
    let repaired_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial should repair garbage-byte corruption");

    // Step 4: Verify all valid events are recovered.
    let recovered = read_all_events(&wal_path);
    assert_eq!(
        recovered.len(),
        valid_events.len(),
        "all {n} valid events must survive TruncatePartial repair (garbage bytes)",
        n = valid_events.len()
    );
    for (i, (recovered_event, original_event)) in
        recovered.iter().zip(valid_events.iter()).enumerate()
    {
        assert_eq!(
            recovered_event, original_event,
            "event at index {i} must be identical after repair"
        );
    }

    assert_eq!(repaired_writer.current_sequence(), valid_events.last().unwrap().sequence(),);

    // Step 5: Verify new appends succeed after repair.
    let mut writer = repaired_writer;
    let next_seq = valid_events.last().unwrap().sequence() + 1;
    let new_event = WalEvent::new(next_seq, WalEventType::EnginePaused { timestamp: 55_000 });
    writer.append(&new_event).expect("append after TruncatePartial repair should succeed");
    writer.flush().expect("flush after repair should succeed");
    writer.close().expect("close after repair should succeed");

    let all_events = read_all_events(&wal_path);
    assert_eq!(all_events.len(), valid_events.len() + 1);
    assert_eq!(all_events.last().unwrap().sequence(), next_seq);
}

// ---------------------------------------------------------------------------
// Test: TruncatePartial on a clean WAL (no corruption) is a no-op -- all
// events survive and the writer opens identically to Strict mode.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_truncate_partial_no_op_on_clean_wal() {
    let dir = wal_test_dir("p6-003-cr-clean-no-op");
    let wal_path = dir.join("actionqueue.wal");
    let valid_events = build_valid_events(4);

    write_events_to_wal(&wal_path, &valid_events);

    // Strict opens cleanly.
    let strict_writer =
        WalFsWriter::new(wal_path.clone()).expect("strict open on clean WAL should succeed");
    let strict_seq = strict_writer.current_sequence();
    drop(strict_writer);

    // TruncatePartial also opens cleanly with same sequence.
    let repair_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial on clean WAL should succeed");
    assert_eq!(
        repair_writer.current_sequence(),
        strict_seq,
        "TruncatePartial on clean WAL must yield same sequence as Strict"
    );
    drop(repair_writer);

    // All events survive.
    let recovered = read_all_events(&wal_path);
    assert_eq!(recovered.len(), valid_events.len());
    for (i, (r, o)) in recovered.iter().zip(valid_events.iter()).enumerate() {
        assert_eq!(r, o, "event at index {i} must match after no-op repair");
    }
}

// ---------------------------------------------------------------------------
// Test: Strict mode correctly rejects corruption that TruncatePartial repairs.
// This provides the contrast proof that the corruption is real, not a test
// artifact.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_strict_rejects_all_corruption_variants() {
    for (label_suffix, variant) in [
        ("incomplete-header", CorruptionVariant::IncompleteHeader),
        ("incomplete-payload", CorruptionVariant::IncompletePayload),
        ("garbage-bytes", CorruptionVariant::GarbageBytes),
    ] {
        let dir = wal_test_dir(&format!("p6-003-strict-reject-{label_suffix}"));
        let wal_path = dir.join("actionqueue.wal");
        let valid_events = build_valid_events(4);

        write_events_to_wal(&wal_path, &valid_events);
        append_corrupt_trailing_bytes(&wal_path, variant);

        let result = WalFsWriter::new(wal_path);
        assert!(
            matches!(result, Err(WalFsWriterInitError::Corruption(_))),
            "strict mode must reject corruption variant {label_suffix}"
        );
    }
}

// ---------------------------------------------------------------------------
// Test: Recovery via the full bootstrap path (load_projection_from_storage)
// after injecting corruption into the WAL file, then re-bootstrapping with a
// fresh (non-corrupt) WAL after TruncatePartial repair.
//
// This exercises the end-to-end path: write events via mutation authority,
// inject corruption, repair, then verify the projection is correct.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_recovery_full_bootstrap_after_truncate_partial_repair() {
    let dir = wal_test_dir("p6-003-full-bootstrap-repair");
    let wal_dir = dir.join("wal");
    let snapshot_dir = dir.join("snapshots");
    std::fs::create_dir_all(&wal_dir).expect("wal dir should be creatable");
    std::fs::create_dir_all(&snapshot_dir).expect("snapshot dir should be creatable");

    let wal_path = wal_dir.join("actionqueue.wal");

    let task_id = TaskId::new();
    let run_id = RunId::new();
    let task_spec = test_task_spec(task_id);
    let run_instance = RunInstance::new_scheduled_with_id(run_id, task_id, 100, 100)
        .expect("scheduled run should be valid");

    // Write 4 valid events directly to the WAL file.
    let events = vec![
        WalEvent::new(1, WalEventType::TaskCreated { task_spec, timestamp: 1000 }),
        WalEvent::new(2, WalEventType::RunCreated { run_instance }),
        WalEvent::new(
            3,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Scheduled,
                new_state: RunState::Ready,
                timestamp: 2000,
            },
        ),
        WalEvent::new(
            4,
            WalEventType::RunStateChanged {
                run_id,
                previous_state: RunState::Ready,
                new_state: RunState::Leased,
                timestamp: 3000,
            },
        ),
    ];

    write_events_to_wal(&wal_path, &events);

    // Inject corruption.
    append_corrupt_trailing_bytes(&wal_path, CorruptionVariant::IncompletePayload);

    // Standard bootstrap (which uses Strict) must fail.
    let bootstrap_result =
        actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&dir);
    assert!(
        bootstrap_result.is_err(),
        "strict bootstrap must fail when WAL has trailing corruption"
    );

    // Repair the WAL with TruncatePartial.
    let repaired_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial repair should succeed");
    assert_eq!(repaired_writer.current_sequence(), 4);
    drop(repaired_writer);

    // Now standard bootstrap (Strict) should succeed on the repaired WAL.
    let bootstrap = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&dir)
        .expect("bootstrap should succeed after TruncatePartial repair");

    assert_eq!(bootstrap.projection.task_count(), 1, "task must survive repair");
    assert_eq!(bootstrap.projection.run_count(), 1, "run must survive repair");
    assert_eq!(
        bootstrap.projection.latest_sequence(),
        4,
        "projection sequence must match last valid event"
    );

    // Verify the run is in the expected state (Leased after Scheduled->Ready->Leased).
    let recovered_state = bootstrap
        .projection
        .get_run_state(&run_id)
        .expect("run must be present in projection after recovery");
    assert_eq!(*recovered_state, RunState::Leased, "run must be in Leased state after recovery");

    assert_eq!(
        bootstrap.recovery_observations.wal_replay_events_applied, 4,
        "all 4 valid WAL events must be replayed"
    );
}

// ---------------------------------------------------------------------------
// Test: T-2 — A single bitflip in the payload is detected by CRC-32 and the
// corrupt trailing event is discarded by TruncatePartial, while all prior
// events are recovered intact.
// ---------------------------------------------------------------------------
#[test]
fn wal_corruption_bitflip_in_payload_detected_by_crc() {
    let dir = wal_test_dir("t2-bitflip-crc-detect");
    let wal_path = dir.join("actionqueue.wal");
    let valid_events = build_valid_events(6);

    // Step 1: Write valid events.
    write_events_to_wal(&wal_path, &valid_events);

    // Step 2: Flip a single bit in the payload of the last event.
    // The WAL record format is: version(4B) + length(4B) + crc32(4B) + payload(NB).
    // We need to locate the last event's payload and flip one bit.
    {
        let file_bytes = std::fs::read(&wal_path).expect("WAL file should be readable");
        let file_len = file_bytes.len();

        // The last byte of the file is the last byte of the last event's payload.
        // Flip bit 0 of that byte.
        let mut corrupted = file_bytes;
        assert!(file_len > 0, "WAL file should not be empty");
        corrupted[file_len - 1] ^= 0x01;

        std::fs::write(&wal_path, corrupted).expect("corrupted WAL should be writable");
    }

    // Step 3: Strict mode must reject the corruption (CRC mismatch).
    let strict_result = WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::Strict);
    assert!(
        strict_result.is_err(),
        "strict policy must reject WAL with bitflip-corrupted payload (CRC mismatch)"
    );

    // Step 4: Bootstrap with TruncatePartial — the corrupt last event is discarded.
    let repaired_writer =
        WalFsWriter::new_with_repair(wal_path.clone(), RepairPolicy::TruncatePartial)
            .expect("TruncatePartial should repair bitflip corruption by discarding last event");

    // Step 5: Verify all events except the last are recovered.
    let recovered = read_all_events(&wal_path);
    assert_eq!(
        recovered.len(),
        valid_events.len() - 1,
        "bitflip in last event payload should cause that event to be discarded; {expected} of \
         {total} events should survive",
        expected = valid_events.len() - 1,
        total = valid_events.len()
    );
    for (i, (recovered_event, original_event)) in
        recovered.iter().zip(valid_events.iter()).enumerate()
    {
        assert_eq!(
            recovered_event, original_event,
            "event at index {i} must be identical after bitflip repair"
        );
    }

    // Writer sequence should match the last surviving event.
    assert_eq!(
        repaired_writer.current_sequence(),
        valid_events[valid_events.len() - 2].sequence(),
        "writer current_sequence must equal last surviving event sequence after bitflip repair"
    );

    // Step 6: Verify new appends succeed after repair.
    let mut writer = repaired_writer;
    let next_seq = valid_events[valid_events.len() - 2].sequence() + 1;
    let new_event = WalEvent::new(next_seq, WalEventType::EnginePaused { timestamp: 77_000 });
    writer.append(&new_event).expect("append after bitflip repair should succeed");
    writer.flush().expect("flush after bitflip repair should succeed");
    writer.close().expect("close after bitflip repair should succeed");

    // Verify the new event is readable and the file is now clean.
    let all_events = read_all_events(&wal_path);
    assert_eq!(
        all_events.len(),
        valid_events.len(),
        "WAL should contain surviving events plus the new post-repair append"
    );
    assert_eq!(all_events.last().unwrap().sequence(), next_seq);

    // Strict re-open must succeed on the repaired + appended file.
    let strict_reopen = WalFsWriter::new(wal_path.clone());
    assert!(
        strict_reopen.is_ok(),
        "strict re-open must succeed after bitflip repair and clean append"
    );
    assert_eq!(strict_reopen.unwrap().current_sequence(), next_seq);
}
