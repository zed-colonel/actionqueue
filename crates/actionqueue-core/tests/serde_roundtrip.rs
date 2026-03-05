#![cfg(feature = "serde")]

//! Integration tests for serde roundtrip stability across IDs and core domain models.

use actionqueue_core::budget::{BudgetAllocation, BudgetConsumption, BudgetDimension};
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::AttemptResultKind;
use actionqueue_core::run::{RunInstance, RunState};
use actionqueue_core::subscription::{EventFilter, SubscriptionId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

#[test]
fn ids_serialize_and_deserialize_predictably() {
    let task_id = TaskId::new();
    let run_id = RunId::new();
    let attempt_id = AttemptId::new();

    let task_json = serde_json::to_string(&task_id).expect("serialize TaskId");
    let run_json = serde_json::to_string(&run_id).expect("serialize RunId");
    let attempt_json = serde_json::to_string(&attempt_id).expect("serialize AttemptId");

    let task_back: TaskId = serde_json::from_str(&task_json).expect("deserialize TaskId");
    let run_back: RunId = serde_json::from_str(&run_json).expect("deserialize RunId");
    let attempt_back: AttemptId =
        serde_json::from_str(&attempt_json).expect("deserialize AttemptId");

    assert_eq!(task_id, task_back);
    assert_eq!(run_id, run_back);
    assert_eq!(attempt_id, attempt_back);
}

#[test]
fn task_spec_serde_roundtrip_is_stable() {
    let constraints = TaskConstraints::new(3, Some(30), Some("customer:123".to_string()))
        .expect("constraints should be valid");
    let metadata = TaskMetadata::new(
        vec!["demo".to_string(), "phase1".to_string()],
        7,
        Some("test task".to_string()),
    );
    let spec = TaskSpec::new(
        TaskId::new(),
        TaskPayload::with_content_type(b"hello".to_vec(), "text/plain"),
        RunPolicy::repeat(6, 1800).expect("repeat policy should be valid"),
        constraints,
        metadata,
    )
    .expect("task spec should be valid");

    let json = serde_json::to_string(&spec).expect("serialize TaskSpec");
    let back: TaskSpec = serde_json::from_str(&json).expect("deserialize TaskSpec");

    assert_eq!(spec, back);
}

#[test]
fn run_policy_deserialization_rejects_zero_repeat_count() {
    let err = serde_json::from_str::<RunPolicy>(r#"{"Repeat":{"count":0,"interval_secs":60}}"#)
        .expect_err("zero repeat count must be rejected during deserialization");

    assert!(
        err.to_string().contains("invalid repeat count"),
        "deserialization error should mention invalid repeat count: {err}"
    );
}

#[test]
fn task_spec_deserialization_rejects_zero_repeat_interval() {
    let spec = TaskSpec::new(
        TaskId::new(),
        TaskPayload::with_content_type(b"hello".to_vec(), "text/plain"),
        RunPolicy::repeat(6, 1800).expect("repeat policy should be valid"),
        TaskConstraints::new(3, Some(30), Some("customer:123".to_string()))
            .expect("constraints should be valid"),
        TaskMetadata::new(
            vec!["demo".to_string(), "phase1".to_string()],
            7,
            Some("test task".to_string()),
        ),
    )
    .expect("task spec should be valid");

    let mut value = serde_json::to_value(&spec).expect("serialize TaskSpec to Value");
    value["run_policy"] = serde_json::json!({"Repeat": {"count": 6, "interval_secs": 0}});

    let err = serde_json::from_value::<TaskSpec>(value)
        .expect_err("zero repeat interval must be rejected during deserialization");

    assert!(
        err.to_string().contains("invalid repeat interval_secs"),
        "deserialization error should mention invalid repeat interval: {err}"
    );
}

#[test]
fn run_instance_deserialization_rejects_ready_with_scheduled_after_created() {
    // Create a valid Ready run, then tamper with the JSON to violate the constraint
    let run = RunInstance::new_ready_with_id(RunId::new(), TaskId::new(), 1_000, 1_000, 0)
        .expect("valid ready run");

    let mut value = serde_json::to_value(&run).expect("serialize RunInstance to Value");
    // Set scheduled_at > created_at to violate the Ready-state constraint
    value["scheduled_at"] = serde_json::json!(2_000);
    value["created_at"] = serde_json::json!(1_000);

    let err = serde_json::from_value::<RunInstance>(value)
        .expect_err("Ready run with scheduled_at > created_at must be rejected");

    assert!(
        err.to_string().contains("scheduled_at"),
        "deserialization error should mention scheduled_at constraint: {err}"
    );
}

#[test]
fn task_constraints_deserialization_rejects_empty_capabilities() {
    let json = r#"{
        "max_attempts": 1,
        "timeout_secs": null,
        "concurrency_key": null,
        "required_capabilities": []
    }"#;

    let err = serde_json::from_str::<TaskConstraints>(json)
        .expect_err("empty required_capabilities must be rejected during deserialization");

    assert!(
        err.to_string().contains("empty"),
        "deserialization error should mention empty capabilities: {err}"
    );
}

#[test]
fn run_state_suspended_serde_roundtrip() {
    let state = RunState::Suspended;
    let json = serde_json::to_string(&state).expect("serialize RunState::Suspended");
    let back: RunState = serde_json::from_str(&json).expect("deserialize RunState::Suspended");
    assert_eq!(state, back);
    assert!(!back.is_terminal());
}

#[test]
fn attempt_result_kind_suspended_serde_roundtrip() {
    let kind = AttemptResultKind::Suspended;
    let json = serde_json::to_string(&kind).expect("serialize AttemptResultKind::Suspended");
    let back: AttemptResultKind =
        serde_json::from_str(&json).expect("deserialize AttemptResultKind::Suspended");
    assert_eq!(kind, back);
}

#[test]
fn budget_dimension_serde_roundtrip() {
    for dim in [BudgetDimension::Token, BudgetDimension::CostCents, BudgetDimension::TimeSecs] {
        let json = serde_json::to_string(&dim).expect("serialize BudgetDimension");
        let back: BudgetDimension =
            serde_json::from_str(&json).expect("deserialize BudgetDimension");
        assert_eq!(dim, back);
    }
}

#[test]
fn budget_consumption_serde_roundtrip() {
    let c = BudgetConsumption::new(BudgetDimension::Token, 500);
    let json = serde_json::to_string(&c).expect("serialize BudgetConsumption");
    let back: BudgetConsumption =
        serde_json::from_str(&json).expect("deserialize BudgetConsumption");
    assert_eq!(c, back);
}

#[test]
fn budget_allocation_serde_roundtrip() {
    let alloc = BudgetAllocation::new(BudgetDimension::CostCents, 1000).expect("valid allocation");
    let json = serde_json::to_string(&alloc).expect("serialize BudgetAllocation");
    let back: BudgetAllocation = serde_json::from_str(&json).expect("deserialize BudgetAllocation");
    assert_eq!(alloc, back);
}

#[test]
fn budget_allocation_deserialization_rejects_zero_limit() {
    let json = r#"{"dimension":"Token","limit":0}"#;
    let err = serde_json::from_str::<BudgetAllocation>(json)
        .expect_err("zero limit must be rejected during deserialization");
    assert!(
        err.to_string().contains("budget limit must be greater than zero"),
        "deserialization error should mention zero limit: {err}"
    );
}

#[test]
fn subscription_id_serde_roundtrip() {
    let id = SubscriptionId::new();
    let json = serde_json::to_string(&id).expect("serialize SubscriptionId");
    let back: SubscriptionId = serde_json::from_str(&json).expect("deserialize SubscriptionId");
    assert_eq!(id, back);
}

#[test]
fn event_filter_task_completed_serde_roundtrip() {
    let filter = EventFilter::TaskCompleted { task_id: TaskId::new() };
    let json = serde_json::to_string(&filter).expect("serialize EventFilter::TaskCompleted");
    let back: EventFilter = serde_json::from_str(&json).expect("deserialize EventFilter");
    assert_eq!(filter, back);
}

#[test]
fn event_filter_run_state_changed_serde_roundtrip() {
    let filter =
        EventFilter::RunStateChanged { task_id: TaskId::new(), state: RunState::Suspended };
    let json = serde_json::to_string(&filter).expect("serialize EventFilter::RunStateChanged");
    let back: EventFilter = serde_json::from_str(&json).expect("deserialize EventFilter");
    assert_eq!(filter, back);
}

#[test]
fn event_filter_budget_threshold_serde_roundtrip() {
    let filter = EventFilter::BudgetThreshold {
        task_id: TaskId::new(),
        dimension: BudgetDimension::Token,
        threshold_pct: 80,
    };
    let json = serde_json::to_string(&filter).expect("serialize EventFilter::BudgetThreshold");
    let back: EventFilter = serde_json::from_str(&json).expect("deserialize EventFilter");
    assert_eq!(filter, back);
}

#[test]
fn event_filter_custom_serde_roundtrip() {
    let filter = EventFilter::Custom { key: "caelum.thread.completed".to_string() };
    let json = serde_json::to_string(&filter).expect("serialize EventFilter::Custom");
    let back: EventFilter = serde_json::from_str(&json).expect("deserialize EventFilter");
    assert_eq!(filter, back);
}

#[test]
fn run_instance_serde_roundtrip_preserves_terminal_state_behavior() {
    let mut run =
        RunInstance::new_scheduled(TaskId::new(), 1_700_000_000, 1_699_999_000).expect("valid run");
    run.transition_to(RunState::Ready).expect("scheduled -> ready");
    run.transition_to(RunState::Leased).expect("ready -> leased");
    run.transition_to(RunState::Running).expect("leased -> running");
    run.start_attempt(AttemptId::new()).expect("start attempt");
    run.finish_attempt(run.current_attempt_id().expect("attempt id present"))
        .expect("finish attempt");
    run.transition_to(RunState::Completed).expect("running -> completed");

    let json = serde_json::to_string(&run).expect("serialize RunInstance");
    let back: RunInstance = serde_json::from_str(&json).expect("deserialize RunInstance");

    assert_eq!(run, back);
    assert!(back.is_terminal());
}
