//! Tests for the streaming WAL reader (S-5 rewrite verification).
//!
//! Verifies that the two-phase streaming reader correctly handles
//! large numbers of events, truncated records, and memory behavior.

use std::fs;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::wal::codec;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_reader::WalFsReader;
use actionqueue_storage::wal::reader::WalReader;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn temp_wal_path() -> std::path::PathBuf {
    let dir = std::env::temp_dir();
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path =
        dir.join(format!("actionqueue_wal_streaming_test_{}_{}.tmp", std::process::id(), count));
    let _ = fs::remove_file(&path);
    path
}

fn create_test_event(seq: u64) -> WalEvent {
    WalEvent::new(
        seq,
        WalEventType::TaskCreated {
            task_spec: TaskSpec::new(
                TaskId::new(),
                TaskPayload::with_content_type(vec![1, 2, 3], "application/octet-stream"),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .expect("test task spec should be valid"),
            timestamp: seq * 1000,
        },
    )
}

#[test]
fn reads_many_events_correctly() {
    let path = temp_wal_path();
    let event_count = 1000;

    // Write 1000 events
    {
        let mut file = fs::File::create(&path).expect("create file");
        for seq in 1..=event_count {
            let event = create_test_event(seq);
            let bytes = codec::encode(&event).expect("encode should succeed");
            file.write_all(&bytes).expect("write should succeed");
        }
        file.flush().expect("flush should succeed");
    }

    // Read them all back
    let mut reader = WalFsReader::new(path.clone()).expect("reader should open");
    let mut read_count = 0u64;

    while let Some(event) = reader.read_next().expect("read should succeed") {
        read_count += 1;
        assert_eq!(event.sequence(), read_count, "event sequence mismatch at event {read_count}");
    }

    assert_eq!(read_count, event_count, "should read exactly {event_count} events");
    assert!(reader.is_end());

    let _ = fs::remove_file(path);
}

#[test]
fn handles_truncated_header_at_end() {
    let path = temp_wal_path();

    // Write one valid event then a partial header (5 bytes instead of 12)
    {
        let event = create_test_event(1);
        let bytes = codec::encode(&event).expect("encode should succeed");
        let mut file = fs::File::create(&path).expect("create file");
        file.write_all(&bytes).expect("write event");
        file.write_all(&[0u8; 5]).expect("write partial header");
        file.flush().expect("flush");
    }

    let mut reader = WalFsReader::new(path.clone()).expect("reader should open");

    // First event should read successfully
    let event = reader.read_next().expect("first read should succeed");
    assert!(event.is_some());
    assert_eq!(event.unwrap().sequence(), 1);

    // Second read should return corruption (incomplete header)
    let result = reader.read_next();
    assert!(result.is_err(), "partial header should be corruption");
    assert!(reader.is_end());

    let _ = fs::remove_file(path);
}

#[test]
fn handles_truncated_payload_at_end() {
    let path = temp_wal_path();

    // Write one valid event then a record with valid header but truncated payload
    {
        let event = create_test_event(1);
        let bytes = codec::encode(&event).expect("encode should succeed");
        let mut file = fs::File::create(&path).expect("create file");
        file.write_all(&bytes).expect("write event");

        // Write a complete header declaring 50 bytes of payload, but only 10 bytes of payload
        file.write_all(&codec::VERSION.to_le_bytes()).expect("write version");
        file.write_all(&50u32.to_le_bytes()).expect("write length");
        file.write_all(&0u32.to_le_bytes()).expect("write CRC");
        file.write_all(&[0xABu8; 10]).expect("write partial payload");
        file.flush().expect("flush");
    }

    let mut reader = WalFsReader::new(path.clone()).expect("reader should open");

    // First event should succeed
    let event = reader.read_next().expect("first read should succeed");
    assert!(event.is_some());
    assert_eq!(event.unwrap().sequence(), 1);

    // Second read should return corruption (incomplete payload)
    let result = reader.read_next();
    assert!(result.is_err(), "truncated payload should be corruption");
    assert!(reader.is_end());

    let _ = fs::remove_file(path);
}

#[test]
fn reports_correct_corruption_offset_for_partial_record() {
    let path = temp_wal_path();

    // Write two valid events then trailing garbage
    let event1 = create_test_event(1);
    let event2 = create_test_event(2);
    let bytes1 = codec::encode(&event1).expect("encode 1");
    let bytes2 = codec::encode(&event2).expect("encode 2");
    let expected_corruption_offset = (bytes1.len() + bytes2.len()) as u64;

    {
        let mut file = fs::File::create(&path).expect("create file");
        file.write_all(&bytes1).expect("write 1");
        file.write_all(&bytes2).expect("write 2");
        // Write partial header at known offset
        file.write_all(&[0xFFu8; 3]).expect("write garbage");
        file.flush().expect("flush");
    }

    let mut reader = WalFsReader::new(path.clone()).expect("reader should open");

    // Read both valid events
    let e1 = reader.read_next().expect("read 1").expect("should have event 1");
    assert_eq!(e1.sequence(), 1);
    let e2 = reader.read_next().expect("read 2").expect("should have event 2");
    assert_eq!(e2.sequence(), 2);

    // Third read should report corruption at the correct offset
    match reader.read_next() {
        Err(actionqueue_storage::wal::reader::WalReaderError::Corruption(details)) => {
            assert_eq!(
                details.offset, expected_corruption_offset,
                "corruption offset should point to start of trailing garbage"
            );
        }
        other => panic!("expected corruption error, got: {other:?}"),
    }

    let _ = fs::remove_file(path);
}

#[test]
fn empty_wal_returns_none_immediately() {
    let path = temp_wal_path();
    fs::write(&path, []).expect("create empty file");

    let mut reader = WalFsReader::new(path.clone()).expect("reader should open");
    let result = reader.read_next().expect("read should succeed");
    assert!(result.is_none(), "empty WAL should return None");
    assert!(reader.is_end());

    let _ = fs::remove_file(path);
}
