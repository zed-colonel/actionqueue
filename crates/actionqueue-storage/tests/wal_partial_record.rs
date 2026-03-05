//! Strict WAL tail corruption matrix tests for D-02.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::recovery::replay::ReplayDriver;
use actionqueue_storage::wal::codec;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_reader::WalFsReader;
use actionqueue_storage::wal::fs_writer::{WalFsWriter, WalFsWriterInitError};
use actionqueue_storage::wal::reader::{WalReader, WalReaderError};
use actionqueue_storage::wal::tail_validation::{WalCorruption, WalCorruptionReasonCode};
use actionqueue_storage::wal::writer::WalWriter;

/// Counter used to create unique WAL test paths under parallel execution.
static TEST_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn temp_wal_path() -> PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = dir.join(format!("actionqueue_wal_strict_tail_test_{count}.tmp"));
    let _ = fs::remove_file(&path);
    path
}

fn test_task_spec(id: u128) -> TaskSpec {
    TaskSpec::new(
        TaskId::from_uuid(uuid::Uuid::from_u128(id)),
        TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("test task spec should be valid")
}

fn task_created(seq: u64, task_id: u128) -> WalEvent {
    WalEvent::new(
        seq,
        WalEventType::TaskCreated { task_spec: test_task_spec(task_id), timestamp: 0 },
    )
}

fn run_created(seq: u64, task_id: u128, run_id: u128) -> WalEvent {
    let run = actionqueue_core::run::run_instance::RunInstance::new_scheduled_with_id(
        RunId::from_uuid(uuid::Uuid::from_u128(run_id)),
        TaskId::from_uuid(uuid::Uuid::from_u128(task_id)),
        1000,
        1000,
    )
    .expect("test run should be valid");
    WalEvent::new(seq, WalEventType::RunCreated { run_instance: run })
}

fn run_state_changed(seq: u64, run_id: u128, previous: RunState, new: RunState) -> WalEvent {
    WalEvent::new(
        seq,
        WalEventType::RunStateChanged {
            run_id: RunId::from_uuid(uuid::Uuid::from_u128(run_id)),
            previous_state: previous,
            new_state: new,
            timestamp: 2000,
        },
    )
}

fn append_bytes(path: &Path, bytes: &[u8]) {
    let mut file =
        fs::OpenOptions::new().append(true).create(true).open(path).expect("open append");
    file.write_all(bytes).expect("append bytes");
    file.sync_all().expect("sync bytes");
}

fn assert_writer_corruption(
    path: &Path,
    expected_offset: u64,
    expected_reason: WalCorruptionReasonCode,
) {
    let result = WalFsWriter::new(path.to_path_buf());
    assert!(matches!(
        result,
        Err(WalFsWriterInitError::Corruption(WalCorruption { offset, reason }))
            if offset == expected_offset && reason == expected_reason
    ));
}

fn assert_reader_corruption(
    path: &Path,
    expected_offset: u64,
    expected_reason: WalCorruptionReasonCode,
) {
    let mut reader = WalFsReader::new(path.to_path_buf()).expect("reader open");
    loop {
        match reader.read_next() {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("expected corruption but reached clean EOF"),
            Err(error) => {
                assert!(matches!(
                    error,
                    WalReaderError::Corruption(WalCorruption { offset, reason })
                        if offset == expected_offset && reason == expected_reason
                ));
                break;
            }
        }
    }
}

fn assert_replay_corruption(
    path: &Path,
    expected_offset: u64,
    expected_reason: WalCorruptionReasonCode,
) {
    let reader = WalFsReader::new(path.to_path_buf()).expect("reader open for replay");
    let mut driver = ReplayDriver::new(reader, ReplayReducer::new());
    let result = driver.run();
    assert!(matches!(
        result,
        Err(WalReaderError::Corruption(WalCorruption { offset, reason }))
            if offset == expected_offset && reason == expected_reason
    ));
}

#[test]
fn d02_t_p1_clean_wal_bootstrap_succeeds_and_sequence_continues() {
    let path = temp_wal_path();

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&task_created(1, 1000)).expect("append seq 1");
        writer.append(&task_created(2, 2000)).expect("append seq 2");
        writer.flush().expect("flush");
    }

    let mut reopened = WalFsWriter::new(path.clone()).expect("writer reopen");
    reopened.append(&task_created(3, 3000)).expect("append seq 3 after bootstrap");

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_p2_clean_reader_and_replay_flows_stay_green() {
    let path = temp_wal_path();

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&task_created(1, 1000)).expect("append task");
        writer.append(&run_created(2, 1000, 2000)).expect("append run");
        writer
            .append(&run_state_changed(3, 2000, RunState::Scheduled, RunState::Ready))
            .expect("append transition");
    }

    let mut reader = WalFsReader::new(path.clone()).expect("reader open");
    assert!(reader.read_next().expect("read 1").is_some());
    assert!(reader.read_next().expect("read 2").is_some());
    assert!(reader.read_next().expect("read 3").is_some());
    assert!(reader.read_next().expect("read eof").is_none());

    let replay_reader = WalFsReader::new(path.clone()).expect("reader replay open");
    let mut driver = ReplayDriver::new(replay_reader, ReplayReducer::new());
    assert!(driver.run().is_ok(), "clean replay should succeed");

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_n1_partial_trailing_header_fails_writer_reader_and_replay() {
    let path = temp_wal_path();
    let first = task_created(1, 1000);
    let first_len = codec::encode(&first).expect("encode should succeed").len() as u64;

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&first).expect("append first");
    }

    append_bytes(&path, &codec::VERSION.to_le_bytes()[..2]);

    assert_writer_corruption(&path, first_len, WalCorruptionReasonCode::IncompleteHeader);
    assert_reader_corruption(&path, first_len, WalCorruptionReasonCode::IncompleteHeader);
    assert_replay_corruption(&path, first_len, WalCorruptionReasonCode::IncompleteHeader);

    // Strict seek policy must fail even when target event exists before corruption.
    let mut reader = WalFsReader::new(path.clone()).expect("seek reader open");
    let seek = reader.seek_to_sequence(1);
    assert!(matches!(
        seek,
        Err(WalReaderError::Corruption(WalCorruption {
            offset,
            reason: WalCorruptionReasonCode::IncompleteHeader
        })) if offset == first_len
    ));

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_n2_partial_trailing_payload_fails_writer_reader_and_replay() {
    let path = temp_wal_path();
    let first = task_created(1, 1000);
    let first_len = codec::encode(&first).expect("encode should succeed").len() as u64;

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&first).expect("append first");
    }

    let mut partial = Vec::new();
    partial.extend_from_slice(&codec::VERSION.to_le_bytes());
    partial.extend_from_slice(&10u32.to_le_bytes());
    partial.extend_from_slice(&0u32.to_le_bytes()); // CRC (dummy)
    partial.extend_from_slice(&[0u8; 3]); // only 3 of 10 payload bytes
    append_bytes(&path, &partial);

    assert_writer_corruption(&path, first_len, WalCorruptionReasonCode::IncompletePayload);
    assert_reader_corruption(&path, first_len, WalCorruptionReasonCode::IncompletePayload);
    assert_replay_corruption(&path, first_len, WalCorruptionReasonCode::IncompletePayload);

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_n3_decode_invalid_tail_fails_writer_reader_and_replay() {
    let path = temp_wal_path();
    let first = task_created(1, 1000);
    let first_len = codec::encode(&first).expect("encode should succeed").len() as u64;

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&first).expect("append first");
    }

    // With CRC-32 validation, wrong CRC is detected first as CrcMismatch.
    let payload = b"nope";
    let mut invalid = Vec::new();
    invalid.extend_from_slice(&codec::VERSION.to_le_bytes());
    invalid.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    invalid.extend_from_slice(&0xDEADBEEFu32.to_le_bytes()); // wrong CRC
    invalid.extend_from_slice(payload);
    append_bytes(&path, &invalid);

    assert_writer_corruption(&path, first_len, WalCorruptionReasonCode::CrcMismatch);
    assert_reader_corruption(&path, first_len, WalCorruptionReasonCode::CrcMismatch);
    assert_replay_corruption(&path, first_len, WalCorruptionReasonCode::CrcMismatch);

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_n4_corruption_offset_is_record_start_boundary() {
    let path = temp_wal_path();
    let first = task_created(1, 1000);
    let second = task_created(2, 2000);
    let expected_offset = (codec::encode(&first).expect("encode should succeed").len()
        + codec::encode(&second).expect("encode should succeed").len())
        as u64;

    {
        let mut writer = WalFsWriter::new(path.clone()).expect("writer open");
        writer.append(&first).expect("append first");
        writer.append(&second).expect("append second");
    }

    // Start a third record but do not complete its payload.
    let mut partial = Vec::new();
    partial.extend_from_slice(&codec::VERSION.to_le_bytes());
    partial.extend_from_slice(&9u32.to_le_bytes());
    partial.extend_from_slice(&0u32.to_le_bytes()); // CRC (dummy)
    partial.extend_from_slice(&[1u8, 2u8]); // only 2 of 9 payload bytes
    append_bytes(&path, &partial);

    assert_writer_corruption(&path, expected_offset, WalCorruptionReasonCode::IncompletePayload);
    assert_reader_corruption(&path, expected_offset, WalCorruptionReasonCode::IncompletePayload);
    assert_replay_corruption(&path, expected_offset, WalCorruptionReasonCode::IncompletePayload);

    let _ = fs::remove_file(path);
}

#[test]
fn d02_t_n5_reducer_errors_remain_distinct_from_wal_corruption_errors() {
    // Reducer semantic error path (valid WAL bytes, invalid lifecycle semantics).
    let reducer_path = temp_wal_path();
    {
        let mut writer = WalFsWriter::new(reducer_path.clone()).expect("writer open reducer case");
        writer.append(&task_created(1, 1000)).expect("append task");
        writer.append(&run_created(2, 1000, 2000)).expect("append run");
        writer
            .append(&run_state_changed(3, 2000, RunState::Ready, RunState::Scheduled))
            .expect("append semantically invalid transition event");
    }

    let reducer_reader = WalFsReader::new(reducer_path.clone()).expect("reader open reducer case");
    let mut reducer_driver = ReplayDriver::new(reducer_reader, ReplayReducer::new());
    assert!(matches!(reducer_driver.run(), Err(WalReaderError::ReducerError(_))));

    // WAL corruption error path.
    let corruption_path = temp_wal_path();
    let first = task_created(1, 3000);
    let first_len = codec::encode(&first).expect("encode should succeed").len() as u64;
    {
        let mut writer =
            WalFsWriter::new(corruption_path.clone()).expect("writer open corruption case");
        writer.append(&first).expect("append first");
    }
    append_bytes(&corruption_path, &codec::VERSION.to_le_bytes()[..1]);

    let corruption_reader =
        WalFsReader::new(corruption_path.clone()).expect("reader open corruption case");
    let mut corruption_driver = ReplayDriver::new(corruption_reader, ReplayReducer::new());
    assert!(matches!(
        corruption_driver.run(),
        Err(WalReaderError::Corruption(WalCorruption {
            offset,
            reason: WalCorruptionReasonCode::IncompleteHeader
        })) if offset == first_len
    ));

    let _ = fs::remove_file(reducer_path);
    let _ = fs::remove_file(corruption_path);
}
