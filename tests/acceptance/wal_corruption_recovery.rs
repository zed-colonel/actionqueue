//! Target corruption must never be mistaken for a repairable partial append.
use actionqueue_storage::{
    store::*,
    wal::{
        codec,
        event::{WalEvent, WalEventType},
        fs_writer::WalFsWriter,
        repair::RepairPolicy,
    },
};
#[test]
fn reserved_kinds_schemas_sequences_and_oversized_lengths_refuse_repair() {
    let dir = tempfile::tempdir().unwrap();
    let session = open_store(dir.path(), OpenOptions::Initialize { features: vec![] }).unwrap();
    let id = session.manifest().store_id;
    drop(session);
    let path = dir.path().join("wal/actionqueue.wal");
    let initial = std::fs::read(&path).unwrap();
    let frame = codec::encode_for_store(
        &WalEvent::new(2, WalEventType::EnginePaused { timestamp: 10 }),
        id,
    )
    .unwrap();
    for (offset, value) in [
        (12, 272u32),
        (12, 273),
        (12, 288),
        (12, 304),
        (12, 305),
        (12, 306),
        (12, 307),
        (12, 320),
        (12, 321),
        (12, 65535),
        (14, 2),
        (32, 7),
        (40, 16 * 1024 * 1024 + 1),
    ] {
        let mut bad = frame.clone();
        let width = if offset == 12 || offset == 14 { 2 } else { 4 };
        bad[offset..offset + width].copy_from_slice(&value.to_le_bytes()[..width]);
        let crc = crc32(&bad[..48]);
        bad[48..52].copy_from_slice(&crc.to_le_bytes());
        // Header validation must win even when the declared payload is short.
        bad.truncate(codec::HEADER_LEN);
        let mut bytes = initial.clone();
        bytes.extend_from_slice(&bad);
        std::fs::write(&path, &bytes).unwrap();
        let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
        assert!(
            WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).is_err(),
            "offset {offset}, value {value}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
}
// Independent small CRC reference avoids depending on storage's implementation.
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 == 1 { 0xedb88320 } else { 0 };
        }
    }
    !crc
}
#[test]
fn semantically_invalid_prefix_prevents_tail_repair() {
    let dir = tempfile::tempdir().unwrap();
    let session = open_store(dir.path(), OpenOptions::Initialize { features: vec![] }).unwrap();
    let id = session.manifest().store_id;
    drop(session);
    let path = dir.path().join("wal/actionqueue.wal");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(
        &codec::encode_for_store(
            &WalEvent::new(2, WalEventType::EngineResumed { timestamp: 10 }),
            id,
        )
        .unwrap(),
    );
    bytes.extend_from_slice(&codec::MAGIC[..5]);
    std::fs::write(&path, &bytes).unwrap();
    let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
    assert!(WalFsWriter::new_with_repair(session, RepairPolicy::TruncatePartial).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}
