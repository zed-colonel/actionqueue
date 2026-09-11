//! Frozen v1 record payloads. Kind IDs never depend on enum discriminants.
use super::task_v1::TaskSpecV1;
use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{ActorId, AttemptId, LedgerEntryId, RunId, TaskId, TenantId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::platform::{Capability, Role};
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};

use super::codec::{DecodeError, EncodeError};
use super::event::WalEventType;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreInitializedV1 {
    manifest_digest: [u8; 32],
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCreatedV1 {
    task_spec: TaskSpecV1,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCreatedV1 {
    run_instance: super::domain_v1::RunV1,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStateChangedV1 {
    run_id: RunId,
    previous_state: RunState,
    new_state: RunState,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptStartedV1 {
    run_id: RunId,
    attempt_id: AttemptId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptFinishedV1 {
    run_id: RunId,
    attempt_id: AttemptId,
    result: AttemptResultKind,
    error: Option<String>,
    output: Option<Vec<u8>>,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskCanceledV1 {
    task_id: TaskId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunCanceledV1 {
    run_id: RunId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseAcquiredV1 {
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseHeartbeatV1 {
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseExpiredV1 {
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseReleasedV1 {
    run_id: RunId,
    owner: String,
    expiry: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EnginePausedV1 {
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineResumedV1 {
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DependencyDeclaredV1 {
    task_id: TaskId,
    depends_on: Vec<TaskId>,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunSuspendedV1 {
    run_id: RunId,
    reason: Option<String>,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RunResumedV1 {
    run_id: RunId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetAllocatedV1 {
    task_id: TaskId,
    dimension: BudgetDimension,
    limit: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetConsumedV1 {
    task_id: TaskId,
    dimension: BudgetDimension,
    amount: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetExhaustedV1 {
    task_id: TaskId,
    dimension: BudgetDimension,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BudgetReplenishedV1 {
    task_id: TaskId,
    dimension: BudgetDimension,
    new_limit: u64,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionCreatedV1 {
    subscription_id: SubscriptionId,
    task_id: TaskId,
    filter: EventFilter,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionTriggeredV1 {
    subscription_id: SubscriptionId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionCanceledV1 {
    subscription_id: SubscriptionId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorRegisteredV1 {
    actor_id: ActorId,
    identity: String,
    executor_traits: Vec<String>,
    department: Option<String>,
    heartbeat_interval_secs: u64,
    tenant_id: Option<TenantId>,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorDeregisteredV1 {
    actor_id: ActorId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorHeartbeatV1 {
    actor_id: ActorId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantCreatedV1 {
    tenant_id: TenantId,
    name: String,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RoleAssignedV1 {
    actor_id: ActorId,
    role: Role,
    tenant_id: TenantId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityGrantedV1 {
    actor_id: ActorId,
    capability: Capability,
    tenant_id: TenantId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityRevokedV1 {
    actor_id: ActorId,
    capability: Capability,
    tenant_id: TenantId,
    timestamp: u64,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerEntryAppendedV1 {
    entry_id: LedgerEntryId,
    tenant_id: TenantId,
    ledger_key: String,
    actor_id: Option<ActorId>,
    payload: Vec<u8>,
    timestamp: u64,
}
pub fn kind(event: &WalEventType) -> u16 {
    match event {
        WalEventType::SignalAdmitted { .. } => 288,
        WalEventType::SignalPinned { .. } => 289,
        WalEventType::SignalUnpinned { .. } => 290,
        WalEventType::SignalsRetired { .. } => 291,
        WalEventType::AdmissionCommitted { .. } => 256,
        WalEventType::StoreInitialized { .. } => 1,
        WalEventType::TaskCreated { .. } => 16,
        WalEventType::RunCreated { .. } => 17,
        WalEventType::RunStateChanged { .. } => 18,
        WalEventType::AttemptStarted { .. } => 19,
        WalEventType::AttemptFinished { .. } => 20,
        WalEventType::TaskCanceled { .. } => 21,
        WalEventType::RunCanceled { .. } => 22,
        WalEventType::LeaseAcquired { .. } => 23,
        WalEventType::LeaseHeartbeat { .. } => 24,
        WalEventType::LeaseExpired { .. } => 25,
        WalEventType::LeaseReleased { .. } => 26,
        WalEventType::EnginePaused { .. } => 27,
        WalEventType::EngineResumed { .. } => 28,
        WalEventType::DependencyDeclared { .. } => 29,
        WalEventType::RunSuspended { .. } => 30,
        WalEventType::RunResumed { .. } => 31,
        WalEventType::BudgetAllocated { .. } => 32,
        WalEventType::BudgetConsumed { .. } => 33,
        WalEventType::BudgetExhausted { .. } => 34,
        WalEventType::BudgetReplenished { .. } => 35,
        WalEventType::SubscriptionCreated { .. } => 36,
        WalEventType::SubscriptionTriggered { .. } => 37,
        WalEventType::SubscriptionCanceled { .. } => 38,
        WalEventType::ActorRegistered { .. } => 39,
        WalEventType::ActorDeregistered { .. } => 40,
        WalEventType::ActorHeartbeat { .. } => 41,
        WalEventType::TenantCreated { .. } => 42,
        WalEventType::RoleAssigned { .. } => 43,
        WalEventType::CapabilityGranted { .. } => 44,
        WalEventType::CapabilityRevoked { .. } => 45,
        WalEventType::LedgerEntryAppended { .. } => 46,
    }
}
pub fn check_kind(kind: u16) -> Result<(), DecodeError> {
    match kind {
        1 | 16 | 17 | 18 | 19 | 20 | 21 | 22 | 23 | 24 | 25 | 26 | 27 | 28 | 29 | 30 | 31 | 32
        | 33 | 34 | 35 | 36 | 37 | 38 | 39 | 40 | 41 | 42 | 43 | 44 | 45 | 46 | 256 | 288 | 289
        | 290 | 291 => Ok(()),
        _ => Err(DecodeError::UnsupportedRecordKind(kind)),
    }
}
pub fn encode_payload(event: &WalEventType) -> Result<Vec<u8>, EncodeError> {
    match event {
        WalEventType::SignalAdmitted { record } => {
            bounded(&super::signal_v1::SignalRecordV1::from(record.clone()))
        }
        WalEventType::SignalPinned { record } | WalEventType::SignalUnpinned { record } => {
            bounded(&super::signal_v1::PinV1::from(record.clone()))
        }
        WalEventType::SignalsRetired { record } => {
            bounded(&super::signal_v1::RetiredV1::from(record.clone()))
        }
        WalEventType::AdmissionCommitted { record, runs } => {
            bounded(&super::admission_v1::AdmissionCommittedV1::new(record, runs))
        }
        WalEventType::StoreInitialized { manifest_digest } => {
            bounded(&StoreInitializedV1 { manifest_digest: manifest_digest.clone() })
        }
        WalEventType::TaskCreated { task_spec, timestamp } => bounded(&TaskCreatedV1 {
            task_spec: TaskSpecV1::from(task_spec),
            timestamp: timestamp.clone(),
        }),
        WalEventType::RunCreated { run_instance } => {
            bounded(&RunCreatedV1 { run_instance: super::domain_v1::RunV1::from(run_instance) })
        }
        WalEventType::RunStateChanged { run_id, previous_state, new_state, timestamp } => {
            bounded(&RunStateChangedV1 {
                run_id: run_id.clone(),
                previous_state: previous_state.clone(),
                new_state: new_state.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::AttemptStarted { run_id, attempt_id, timestamp } => {
            bounded(&AttemptStartedV1 {
                run_id: run_id.clone(),
                attempt_id: attempt_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::AttemptFinished { run_id, attempt_id, result, error, output, timestamp } => {
            bounded(&AttemptFinishedV1 {
                run_id: run_id.clone(),
                attempt_id: attempt_id.clone(),
                result: result.clone(),
                error: error.clone(),
                output: output.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::TaskCanceled { task_id, timestamp } => {
            bounded(&TaskCanceledV1 { task_id: task_id.clone(), timestamp: timestamp.clone() })
        }
        WalEventType::RunCanceled { run_id, timestamp } => {
            bounded(&RunCanceledV1 { run_id: run_id.clone(), timestamp: timestamp.clone() })
        }
        WalEventType::LeaseAcquired { run_id, owner, expiry, timestamp } => {
            bounded(&LeaseAcquiredV1 {
                run_id: run_id.clone(),
                owner: owner.clone(),
                expiry: expiry.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::LeaseHeartbeat { run_id, owner, expiry, timestamp } => {
            bounded(&LeaseHeartbeatV1 {
                run_id: run_id.clone(),
                owner: owner.clone(),
                expiry: expiry.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::LeaseExpired { run_id, owner, expiry, timestamp } => {
            bounded(&LeaseExpiredV1 {
                run_id: run_id.clone(),
                owner: owner.clone(),
                expiry: expiry.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::LeaseReleased { run_id, owner, expiry, timestamp } => {
            bounded(&LeaseReleasedV1 {
                run_id: run_id.clone(),
                owner: owner.clone(),
                expiry: expiry.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::EnginePaused { timestamp } => {
            bounded(&EnginePausedV1 { timestamp: timestamp.clone() })
        }
        WalEventType::EngineResumed { timestamp } => {
            bounded(&EngineResumedV1 { timestamp: timestamp.clone() })
        }
        WalEventType::DependencyDeclared { task_id, depends_on, timestamp } => {
            bounded(&DependencyDeclaredV1 {
                task_id: task_id.clone(),
                depends_on: depends_on.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::RunSuspended { run_id, reason, timestamp } => bounded(&RunSuspendedV1 {
            run_id: run_id.clone(),
            reason: reason.clone(),
            timestamp: timestamp.clone(),
        }),
        WalEventType::RunResumed { run_id, timestamp } => {
            bounded(&RunResumedV1 { run_id: run_id.clone(), timestamp: timestamp.clone() })
        }
        WalEventType::BudgetAllocated { task_id, dimension, limit, timestamp } => {
            bounded(&BudgetAllocatedV1 {
                task_id: task_id.clone(),
                dimension: dimension.clone(),
                limit: limit.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::BudgetConsumed { task_id, dimension, amount, timestamp } => {
            bounded(&BudgetConsumedV1 {
                task_id: task_id.clone(),
                dimension: dimension.clone(),
                amount: amount.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::BudgetExhausted { task_id, dimension, timestamp } => {
            bounded(&BudgetExhaustedV1 {
                task_id: task_id.clone(),
                dimension: dimension.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::BudgetReplenished { task_id, dimension, new_limit, timestamp } => {
            bounded(&BudgetReplenishedV1 {
                task_id: task_id.clone(),
                dimension: dimension.clone(),
                new_limit: new_limit.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::SubscriptionCreated { subscription_id, task_id, filter, timestamp } => {
            bounded(&SubscriptionCreatedV1 {
                subscription_id: subscription_id.clone(),
                task_id: task_id.clone(),
                filter: filter.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::SubscriptionTriggered { subscription_id, timestamp } => {
            bounded(&SubscriptionTriggeredV1 {
                subscription_id: subscription_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::SubscriptionCanceled { subscription_id, timestamp } => {
            bounded(&SubscriptionCanceledV1 {
                subscription_id: subscription_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::ActorRegistered {
            actor_id,
            identity,
            executor_traits,
            department,
            heartbeat_interval_secs,
            tenant_id,
            timestamp,
        } => bounded(&ActorRegisteredV1 {
            actor_id: actor_id.clone(),
            identity: identity.clone(),
            executor_traits: executor_traits.clone(),
            department: department.clone(),
            heartbeat_interval_secs: heartbeat_interval_secs.clone(),
            tenant_id: tenant_id.clone(),
            timestamp: timestamp.clone(),
        }),
        WalEventType::ActorDeregistered { actor_id, timestamp } => bounded(&ActorDeregisteredV1 {
            actor_id: actor_id.clone(),
            timestamp: timestamp.clone(),
        }),
        WalEventType::ActorHeartbeat { actor_id, timestamp } => {
            bounded(&ActorHeartbeatV1 { actor_id: actor_id.clone(), timestamp: timestamp.clone() })
        }
        WalEventType::TenantCreated { tenant_id, name, timestamp } => bounded(&TenantCreatedV1 {
            tenant_id: tenant_id.clone(),
            name: name.clone(),
            timestamp: timestamp.clone(),
        }),
        WalEventType::RoleAssigned { actor_id, role, tenant_id, timestamp } => {
            bounded(&RoleAssignedV1 {
                actor_id: actor_id.clone(),
                role: role.clone(),
                tenant_id: tenant_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::CapabilityGranted { actor_id, capability, tenant_id, timestamp } => {
            bounded(&CapabilityGrantedV1 {
                actor_id: actor_id.clone(),
                capability: capability.clone(),
                tenant_id: tenant_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::CapabilityRevoked { actor_id, capability, tenant_id, timestamp } => {
            bounded(&CapabilityRevokedV1 {
                actor_id: actor_id.clone(),
                capability: capability.clone(),
                tenant_id: tenant_id.clone(),
                timestamp: timestamp.clone(),
            })
        }
        WalEventType::LedgerEntryAppended {
            entry_id,
            tenant_id,
            ledger_key,
            actor_id,
            payload,
            timestamp,
        } => bounded(&LedgerEntryAppendedV1 {
            entry_id: entry_id.clone(),
            tenant_id: tenant_id.clone(),
            ledger_key: ledger_key.clone(),
            actor_id: actor_id.clone(),
            payload: payload.clone(),
            timestamp: timestamp.clone(),
        }),
    }
}
pub fn decode_payload(kind: u16, payload: &[u8]) -> Result<WalEventType, DecodeError> {
    match kind {
        288 => {
            if payload.len() + 52 > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES {
                return Err(DecodeError::Decode("signal frame too large".into()));
            }
            let (v, rest) = postcard::take_from_bytes::<super::signal_v1::SignalRecordV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing signal bytes".into()));
            }
            Ok(WalEventType::SignalAdmitted { record: v.try_into()? })
        }
        289 => {
            if payload.len() + 52 > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES {
                return Err(DecodeError::Decode("signal frame too large".into()));
            }
            let (v, rest) = postcard::take_from_bytes::<super::signal_v1::PinV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing signal bytes".into()));
            }
            Ok(WalEventType::SignalPinned { record: v.try_into()? })
        }
        290 => {
            if payload.len() + 52 > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES {
                return Err(DecodeError::Decode("signal frame too large".into()));
            }
            let (v, rest) = postcard::take_from_bytes::<super::signal_v1::PinV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing signal bytes".into()));
            }
            Ok(WalEventType::SignalUnpinned { record: v.try_into()? })
        }
        291 => {
            if payload.len() + 52 > actionqueue_core::limits::MAX_SIGNAL_RECORD_BYTES {
                return Err(DecodeError::Decode("signal frame too large".into()));
            }
            let (v, rest) = postcard::take_from_bytes::<super::signal_v1::RetiredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing signal bytes".into()));
            }
            Ok(WalEventType::SignalsRetired { record: v.try_into()? })
        }
        256 => {
            let (v, rest) =
                postcard::take_from_bytes::<super::admission_v1::AdmissionCommittedV1>(payload)
                    .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            v.into_event()
        }
        1 => {
            let (v, rest) = postcard::take_from_bytes::<StoreInitializedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::StoreInitialized { manifest_digest: v.manifest_digest })
        }
        16 => {
            let (v, rest) = postcard::take_from_bytes::<TaskCreatedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::TaskCreated {
                task_spec: v.task_spec.try_into()?,
                timestamp: v.timestamp,
            })
        }
        17 => {
            let (v, rest) = postcard::take_from_bytes::<RunCreatedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RunCreated { run_instance: v.run_instance.try_into()? })
        }
        18 => {
            let (v, rest) = postcard::take_from_bytes::<RunStateChangedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RunStateChanged {
                run_id: v.run_id,
                previous_state: v.previous_state,
                new_state: v.new_state,
                timestamp: v.timestamp,
            })
        }
        19 => {
            let (v, rest) = postcard::take_from_bytes::<AttemptStartedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::AttemptStarted {
                run_id: v.run_id,
                attempt_id: v.attempt_id,
                timestamp: v.timestamp,
            })
        }
        20 => {
            let (v, rest) = postcard::take_from_bytes::<AttemptFinishedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::AttemptFinished {
                run_id: v.run_id,
                attempt_id: v.attempt_id,
                result: v.result,
                error: v.error,
                output: v.output,
                timestamp: v.timestamp,
            })
        }
        21 => {
            let (v, rest) = postcard::take_from_bytes::<TaskCanceledV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::TaskCanceled { task_id: v.task_id, timestamp: v.timestamp })
        }
        22 => {
            let (v, rest) = postcard::take_from_bytes::<RunCanceledV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RunCanceled { run_id: v.run_id, timestamp: v.timestamp })
        }
        23 => {
            let (v, rest) = postcard::take_from_bytes::<LeaseAcquiredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::LeaseAcquired {
                run_id: v.run_id,
                owner: v.owner,
                expiry: v.expiry,
                timestamp: v.timestamp,
            })
        }
        24 => {
            let (v, rest) = postcard::take_from_bytes::<LeaseHeartbeatV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::LeaseHeartbeat {
                run_id: v.run_id,
                owner: v.owner,
                expiry: v.expiry,
                timestamp: v.timestamp,
            })
        }
        25 => {
            let (v, rest) = postcard::take_from_bytes::<LeaseExpiredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::LeaseExpired {
                run_id: v.run_id,
                owner: v.owner,
                expiry: v.expiry,
                timestamp: v.timestamp,
            })
        }
        26 => {
            let (v, rest) = postcard::take_from_bytes::<LeaseReleasedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::LeaseReleased {
                run_id: v.run_id,
                owner: v.owner,
                expiry: v.expiry,
                timestamp: v.timestamp,
            })
        }
        27 => {
            let (v, rest) = postcard::take_from_bytes::<EnginePausedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::EnginePaused { timestamp: v.timestamp })
        }
        28 => {
            let (v, rest) = postcard::take_from_bytes::<EngineResumedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::EngineResumed { timestamp: v.timestamp })
        }
        29 => {
            let (v, rest) = postcard::take_from_bytes::<DependencyDeclaredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::DependencyDeclared {
                task_id: v.task_id,
                depends_on: v.depends_on,
                timestamp: v.timestamp,
            })
        }
        30 => {
            let (v, rest) = postcard::take_from_bytes::<RunSuspendedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RunSuspended {
                run_id: v.run_id,
                reason: v.reason,
                timestamp: v.timestamp,
            })
        }
        31 => {
            let (v, rest) = postcard::take_from_bytes::<RunResumedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RunResumed { run_id: v.run_id, timestamp: v.timestamp })
        }
        32 => {
            let (v, rest) = postcard::take_from_bytes::<BudgetAllocatedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::BudgetAllocated {
                task_id: v.task_id,
                dimension: v.dimension,
                limit: v.limit,
                timestamp: v.timestamp,
            })
        }
        33 => {
            let (v, rest) = postcard::take_from_bytes::<BudgetConsumedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::BudgetConsumed {
                task_id: v.task_id,
                dimension: v.dimension,
                amount: v.amount,
                timestamp: v.timestamp,
            })
        }
        34 => {
            let (v, rest) = postcard::take_from_bytes::<BudgetExhaustedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::BudgetExhausted {
                task_id: v.task_id,
                dimension: v.dimension,
                timestamp: v.timestamp,
            })
        }
        35 => {
            let (v, rest) = postcard::take_from_bytes::<BudgetReplenishedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::BudgetReplenished {
                task_id: v.task_id,
                dimension: v.dimension,
                new_limit: v.new_limit,
                timestamp: v.timestamp,
            })
        }
        36 => {
            let (v, rest) = postcard::take_from_bytes::<SubscriptionCreatedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::SubscriptionCreated {
                subscription_id: v.subscription_id,
                task_id: v.task_id,
                filter: v.filter,
                timestamp: v.timestamp,
            })
        }
        37 => {
            let (v, rest) = postcard::take_from_bytes::<SubscriptionTriggeredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::SubscriptionTriggered {
                subscription_id: v.subscription_id,
                timestamp: v.timestamp,
            })
        }
        38 => {
            let (v, rest) = postcard::take_from_bytes::<SubscriptionCanceledV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::SubscriptionCanceled {
                subscription_id: v.subscription_id,
                timestamp: v.timestamp,
            })
        }
        39 => {
            let (v, rest) = postcard::take_from_bytes::<ActorRegisteredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::ActorRegistered {
                actor_id: v.actor_id,
                identity: v.identity,
                executor_traits: v.executor_traits,
                department: v.department,
                heartbeat_interval_secs: v.heartbeat_interval_secs,
                tenant_id: v.tenant_id,
                timestamp: v.timestamp,
            })
        }
        40 => {
            let (v, rest) = postcard::take_from_bytes::<ActorDeregisteredV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::ActorDeregistered { actor_id: v.actor_id, timestamp: v.timestamp })
        }
        41 => {
            let (v, rest) = postcard::take_from_bytes::<ActorHeartbeatV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::ActorHeartbeat { actor_id: v.actor_id, timestamp: v.timestamp })
        }
        42 => {
            let (v, rest) = postcard::take_from_bytes::<TenantCreatedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::TenantCreated {
                tenant_id: v.tenant_id,
                name: v.name,
                timestamp: v.timestamp,
            })
        }
        43 => {
            let (v, rest) = postcard::take_from_bytes::<RoleAssignedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::RoleAssigned {
                actor_id: v.actor_id,
                role: v.role,
                tenant_id: v.tenant_id,
                timestamp: v.timestamp,
            })
        }
        44 => {
            let (v, rest) = postcard::take_from_bytes::<CapabilityGrantedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::CapabilityGranted {
                actor_id: v.actor_id,
                capability: v.capability,
                tenant_id: v.tenant_id,
                timestamp: v.timestamp,
            })
        }
        45 => {
            let (v, rest) = postcard::take_from_bytes::<CapabilityRevokedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::CapabilityRevoked {
                actor_id: v.actor_id,
                capability: v.capability,
                tenant_id: v.tenant_id,
                timestamp: v.timestamp,
            })
        }
        46 => {
            let (v, rest) = postcard::take_from_bytes::<LedgerEntryAppendedV1>(payload)
                .map_err(|e| DecodeError::Decode(e.to_string()))?;
            if !rest.is_empty() {
                return Err(DecodeError::Decode("trailing payload bytes".into()));
            }
            Ok(WalEventType::LedgerEntryAppended {
                entry_id: v.entry_id,
                tenant_id: v.tenant_id,
                ledger_key: v.ledger_key,
                actor_id: v.actor_id,
                payload: v.payload,
                timestamp: v.timestamp,
            })
        }
        _ => Err(DecodeError::UnsupportedRecordKind(kind)),
    }
}
/// Kind 256 is admission. Reserved IDs: compound attempt start/disposition 272/273;
/// signal kinds 288–291 are active; wait establish/satisfy/timeout/cancel 304..=307;
/// attributed task/run control 320/321. All are unsupported until their owners land.
pub const RESERVED_KINDS: &[u16] = &[272, 273, 304, 305, 306, 307, 320, 321];

fn bounded<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, EncodeError> {
    let size = postcard::serialize_with_flavor::<_, postcard::ser_flavors::Size, usize>(
        value,
        postcard::ser_flavors::Size::default(),
    )
    .map_err(|e| EncodeError::Serialization(e.to_string()))?;
    if size > super::codec::MAX_PAYLOAD_SIZE {
        return Err(EncodeError::PayloadTooLarge(size));
    }
    postcard::to_allocvec(value).map_err(|e| EncodeError::Serialization(e.to_string()))
}
