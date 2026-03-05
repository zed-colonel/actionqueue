//! Integration tests for ID roundtrip parsing, UUID conversion, and uniqueness guarantees.

use std::str::FromStr;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};

#[test]
fn task_id_roundtrip_through_string() {
    let id = TaskId::new();
    let as_string = id.to_string();
    let parsed = TaskId::from_str(&as_string).unwrap();
    assert_eq!(id, parsed);
}

#[test]
fn run_id_roundtrip_through_string() {
    let id = RunId::new();
    let as_string = id.to_string();
    let parsed = RunId::from_str(&as_string).unwrap();
    assert_eq!(id, parsed);
}

#[test]
fn attempt_id_roundtrip_through_string() {
    let id = AttemptId::new();
    let as_string = id.to_string();
    let parsed = AttemptId::from_str(&as_string).unwrap();
    assert_eq!(id, parsed);
}

#[test]
fn task_id_from_uuid_roundtrip() {
    let uuid = uuid::Uuid::new_v4();
    let id = TaskId::from_uuid(uuid);
    assert_eq!(id.as_uuid(), &uuid);
}

#[test]
fn run_id_from_uuid_roundtrip() {
    let uuid = uuid::Uuid::new_v4();
    let id = RunId::from_uuid(uuid);
    assert_eq!(id.as_uuid(), &uuid);
}

#[test]
fn attempt_id_from_uuid_roundtrip() {
    let uuid = uuid::Uuid::new_v4();
    let id = AttemptId::from_uuid(uuid);
    assert_eq!(id.as_uuid(), &uuid);
}

#[test]
fn task_ids_are_unique() {
    let id1 = TaskId::new();
    let id2 = TaskId::new();
    assert_ne!(id1, id2);
}

#[test]
fn run_ids_are_unique() {
    let id1 = RunId::new();
    let id2 = RunId::new();
    assert_ne!(id1, id2);
}

#[test]
fn attempt_ids_are_unique() {
    let id1 = AttemptId::new();
    let id2 = AttemptId::new();
    assert_ne!(id1, id2);
}
