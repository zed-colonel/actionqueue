//! Frozen structural subscription filter encoding shared by WAL and snapshots.
//! Tags 0, 1, 2 retain their original order. Removed tag 3 is never reassigned.
use actionqueue_core::{
    budget::BudgetDimension, ids::TaskId, run::state::RunState, subscription::EventFilter,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum StructuralFilter {
    TaskCompleted { task_id: TaskId },
    RunStateChanged { task_id: TaskId, state: RunState },
    BudgetThreshold { task_id: TaskId, dimension: BudgetDimension, threshold_pct: u8 },
}
impl From<&EventFilter> for StructuralFilter {
    fn from(value: &EventFilter) -> Self {
        match *value {
            EventFilter::TaskCompleted { task_id } => Self::TaskCompleted { task_id },
            EventFilter::RunStateChanged { task_id, state } => {
                Self::RunStateChanged { task_id, state }
            }
            EventFilter::BudgetThreshold { task_id, dimension, threshold_pct } => {
                Self::BudgetThreshold { task_id, dimension, threshold_pct }
            }
        }
    }
}
impl From<StructuralFilter> for EventFilter {
    fn from(value: StructuralFilter) -> Self {
        match value {
            StructuralFilter::TaskCompleted { task_id } => Self::TaskCompleted { task_id },
            StructuralFilter::RunStateChanged { task_id, state } => {
                Self::RunStateChanged { task_id, state }
            }
            StructuralFilter::BudgetThreshold { task_id, dimension, threshold_pct } => {
                Self::BudgetThreshold { task_id, dimension, threshold_pct }
            }
        }
    }
}
pub(crate) fn serialize<S: serde::Serializer>(
    value: &EventFilter,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&StructuralFilter::from(value), serializer)
}
pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<EventFilter, D::Error> {
    Ok(<StructuralFilter as serde::Deserialize>::deserialize(deserializer)?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_bytes_and_removed_tag_are_frozen() {
        let task_id = "11111111-1111-1111-1111-111111111111".parse().unwrap();
        for (tag, filter, suffix) in [
            (0, EventFilter::TaskCompleted { task_id }, vec![]),
            (1, EventFilter::RunStateChanged { task_id, state: RunState::Scheduled }, vec![0]),
            (
                2,
                EventFilter::BudgetThreshold {
                    task_id,
                    dimension: BudgetDimension::Token,
                    threshold_pct: 80,
                },
                vec![0, 80],
            ),
        ] {
            let dto = StructuralFilter::from(&filter);
            let mut fixture = vec![tag, 16];
            fixture.extend([0x11; 16]);
            fixture.extend(suffix);
            assert_eq!(postcard::to_allocvec(&dto).unwrap(), fixture);
            assert_eq!(postcard::to_allocvec(&filter).unwrap(), fixture);
            assert_eq!(serde_json::to_vec(&dto).unwrap(), serde_json::to_vec(&filter).unwrap());
            assert_eq!(
                EventFilter::from(postcard::from_bytes::<StructuralFilter>(&fixture).unwrap()),
                filter
            );
        }
        // Original tag 3 followed by a postcard string; it must fail closed.
        assert!(postcard::from_bytes::<StructuralFilter>(&[3, 1, b'x']).is_err());
        assert!(serde_json::from_str::<StructuralFilter>(r#"{"Custom":{"key":"x"}}"#).is_err());
    }
}

#[cfg(all(test, feature = "budget"))]
mod recovery_tests {
    use actionqueue_core::{
        mutation::*,
        subscription::SubscriptionId,
        task::{
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };

    use super::*;
    use crate::{
        recovery::bootstrap::recover_read_only,
        snapshot::{
            build::build_snapshot_from_projection,
            writer::{SnapshotFsWriter, SnapshotWriter},
        },
        store::{capabilities, open_store, OpenOptions},
        wal::{fs_writer::WalFsWriter, repair::RepairPolicy},
    };

    #[test]
    fn removed_filter_fails_closed_without_repair_or_snapshot_fallback() {
        for snapshot_case in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let session = open_store(
                dir.path(),
                OpenOptions::Initialize {
                    features: capabilities().into_iter().filter(|f| f != "platform").collect(),
                },
            )
            .unwrap();
            let mut a = session.into_authority().unwrap().with_host(
                actionqueue_core::control::HostControlContext {
                    actor_id: None,
                    scope: actionqueue_core::control::ControlScope::SingleTenant,
                    attribution: actionqueue_core::causal::ControlMutationContext::new(
                        actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
                    ),
                },
            );
            let task = TaskId::new();
            let spec = TaskSpec::new(
                task,
                TaskPayload::new(vec![]),
                RunPolicy::Once,
                Default::default(),
                Default::default(),
            )
            .unwrap();
            let request =
                actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap();
            let digest = request.digest().unwrap();
            let plan = actionqueue_core::admission::AdmissionPlan::new(
                request,
                vec![actionqueue_core::run::RunInstance::new_scheduled(task, 1, 1).unwrap()],
                digest,
            )
            .unwrap();
            let _ = a
                .submit_command(
                    MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(2, plan, None, 1)),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
            let _ = a
                .submit_command(
                    MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
                        3,
                        SubscriptionId::new(),
                        task,
                        EventFilter::TaskCompleted { task_id: task },
                        2,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
            let session = a.store_session().unwrap();
            let wal_path = session.wal_path();
            let snapshot_path = session.snapshot_path();
            if snapshot_case {
                let snapshot = build_snapshot_from_projection(a.projection(), 3).unwrap();
                let mut writer = SnapshotFsWriter::new(session).unwrap();
                writer.write(&snapshot).unwrap();
                writer.close().unwrap();
            }
            drop(a);
            if snapshot_case {
                let bytes = std::fs::read(&snapshot_path).unwrap();
                let mut envelope: serde_json::Value = serde_json::from_slice(&bytes[20..]).unwrap();
                envelope["projection"]["subscriptions"][0]["filter"] =
                    serde_json::json!({"Custom": {"key": "x"}});
                let payload = serde_json::to_vec(&envelope).unwrap();
                let mut changed = bytes[..12].to_vec();
                changed.extend((payload.len() as u32).to_le_bytes());
                changed.extend(crc32fast::hash(&payload).to_le_bytes());
                changed.extend(payload);
                std::fs::write(&snapshot_path, changed).unwrap();
            } else {
                let mut bytes = std::fs::read(&wal_path).unwrap();
                let mut offset = 0;
                for _ in 0..2 {
                    offset += 52
                        + u32::from_le_bytes(bytes[offset + 40..offset + 44].try_into().unwrap())
                            as usize;
                }
                // Schema 2 attribution prefixes the original binary inner frame.
                assert_eq!(
                    u16::from_le_bytes(bytes[offset + 12..offset + 14].try_into().unwrap()),
                    352
                );
                let attribution_len =
                    u32::from_le_bytes(bytes[offset + 52..offset + 56].try_into().unwrap())
                        as usize;
                let inner = offset + 56 + attribution_len;
                let mut payload = bytes[inner + 52..inner + 52 + 34].to_vec(); // two UUIDs
                assert_eq!(bytes[inner + 52 + 34], 0); // original TaskCompleted tag
                payload.extend([3, 1, b'x', 2]); // removed filter and timestamp
                bytes.truncate(inner + 52);
                bytes.extend(payload);
                // Repair both checksums so recovery must reject the removed
                // semantic variant rather than incidental framing corruption.
                for frame in [inner, offset] {
                    let payload_len = bytes.len() - frame - 52;
                    let payload_crc = crc32fast::hash(&bytes[frame + 52..]);
                    bytes[frame + 40..frame + 44]
                        .copy_from_slice(&(payload_len as u32).to_le_bytes());
                    bytes[frame + 44..frame + 48].copy_from_slice(&payload_crc.to_le_bytes());
                    let header_crc = crc32fast::hash(&bytes[frame..frame + 48]);
                    bytes[frame + 48..frame + 52].copy_from_slice(&header_crc.to_le_bytes());
                }
                std::fs::write(&wal_path, bytes).unwrap();
            }
            let before_wal = std::fs::read(&wal_path).unwrap();
            let before_snapshot = std::fs::read(&snapshot_path).ok();
            for policy in [RepairPolicy::Strict, RepairPolicy::TruncatePartial] {
                let session = open_store(dir.path(), OpenOptions::ReadWrite).unwrap();
                let error = recover_read_only(&session, policy).unwrap_err().to_string();
                assert!(error.contains("variant") || error.contains("DecodeFailure"), "{error}");
                if !snapshot_case {
                    assert!(WalFsWriter::new_with_repair(session, policy).is_err());
                }
                assert_eq!(std::fs::read(&wal_path).unwrap(), before_wal);
                assert_eq!(std::fs::read(&snapshot_path).ok(), before_snapshot);
            }
        }
    }
}
