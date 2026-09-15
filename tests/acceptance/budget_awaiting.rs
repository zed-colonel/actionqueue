//! Budget eligibility is independent of durable wait satisfaction and input delivery.
#![allow(dead_code, unused_imports)]
include!("resume_support.rs");
use actionqueue_core::budget::{BudgetConsumption, BudgetDimension};
use actionqueue_core::disposition::{AttemptDisposition, DispositionOutcome, DispositionParts};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};

const DIMS: [BudgetDimension; 3] =
    [BudgetDimension::Token, BudgetDimension::CostCents, BudgetDimension::TimeSecs];
fn once_task(hold: bool, key: Option<&str>) -> TaskSpec {
    let t = admission_support::spec(1);
    let mut constraints = TaskConstraints::new(3, None, key.map(str::to_owned)).unwrap();
    if hold {
        constraints.set_concurrency_key_wait_policy(ConcurrencyKeyWaitPolicy::HoldWhileAwaiting);
    }
    TaskSpec::new(
        t.id(),
        t.task_payload().clone(),
        actionqueue_core::task::run_policy::RunPolicy::Once,
        constraints,
        t.metadata().clone(),
    )
    .unwrap()
}
fn budget_running(a: &mut s::Authority, key: Option<&str>, hold: bool) -> RunId {
    let q = admission_support::with_spec(&admission_support::request(1), once_task(hold, key));
    let _ = admission_support::ensure(a, q, 10).unwrap();
    let r = a.projection().runs_for_task(admission_support::id(1)).next().unwrap().id();
    transition(a, r, RunState::Ready, 11);
    lease(a, r, 12);
    start(a, r, 13);
    r
}
fn allocation(a: &mut s::Authority, task: TaskId, dimension: BudgetDimension) {
    commit!(
        a,
        MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(seq(a), task, dimension, 1, 14))
    );
}
fn yielding(a: &mut s::Authority, r: RunId, dims: &[BudgetDimension], deadline: bool) {
    let wait = spec(
        WaitId::new(),
        deadline.then_some(WaitDeadline { at: 30, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
    );
    let expected = command(a, r, wait.clone()).expected;
    let d = AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(wait),
            consumption: dims.iter().map(|d| BudgetConsumption::new(*d, 1)).collect(),
            ..Default::default()
        },
    )
    .unwrap();
    actionqueue_runtime::disposition::commit(a, expected, d, 20).unwrap();
}
fn config(dir: &std::path::Path) -> RuntimeConfig {
    RuntimeConfig { data_dir: dir.into(), ..Default::default() }
}

#[tokio::test]
async fn wake_budget_recovery_matrix() {
    // Every dimension and multiple dimensions, both key policies, all wake timings.
    for dims in [vec![DIMS[0]], vec![DIMS[1]], vec![DIMS[2]], DIMS.to_vec()] {
        for hold in [false, true] {
            for wake_kind in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let mut a = s::open(dir.path());
                let r = budget_running(&mut a, Some("key"), hold);
                let task = a.projection().get_run_instance(&r).unwrap().task_id();
                for &dim in &dims {
                    allocation(&mut a, task, dim);
                }
                if wake_kind == 0 {
                    let _ = s::submit(&mut a, s::envelope(1, 15)).unwrap();
                    parity(&a);
                }
                yielding(&mut a, r, &dims, wake_kind == 2);
                assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Awaiting));
                assert!(a.projection().get_lease(&r).is_none());
                assert_eq!(a.projection().get_run_instance(&r).unwrap().failure_attempt_count(), 0);
                assert_eq!(a.projection().key_reservations().any(|(id, _)| id == r), hold);
                parity(&a); // WAL and snapshot recovery immediately after yield.
                if wake_kind == 1 {
                    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
                    parity(&a);
                }
                if wake_kind != 2 {
                    let before = seq(&a);
                    let _ = s::submit(&mut a, s::envelope(1, if wake_kind == 0 { 15 } else { 25 }))
                        .unwrap();
                    assert_eq!(seq(&a), before);
                }
                reconcile(&mut a, 30).unwrap();
                let context = a.projection().pending_resume(r).unwrap();
                assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Ready));
                parity(&a);
                drop(a);
                let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
                let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
                    .bootstrap_with_clock(MockClock::new(40))
                    .unwrap()
                    .with_host(s::host_support::host(
                        actionqueue_core::control::ControlScope::SingleTenant,
                    ));
                let digest = boot.projection().projection_digest().unwrap();
                for _ in 0..4 {
                    assert_eq!(boot.tick().await.unwrap().dispatched, 0);
                    assert_eq!(boot.projection().pending_resume(r), Some(context.clone()));
                    assert!(boot.projection().get_lease(&r).is_none());
                    assert_eq!(boot.projection().get_run_instance(&r).unwrap().attempt_count(), 1);
                    for &dim in &dims {
                        assert_eq!(boot.projection().get_budget(&task, dim).unwrap().consumed, 1);
                    }
                }
                assert_eq!(digest, boot.projection().projection_digest().unwrap());
                for (i, &dim) in dims.iter().enumerate() {
                    boot.replenish_budget(task, dim, 10).unwrap();
                    if i + 1 < dims.len() {
                        assert_eq!(boot.tick().await.unwrap().dispatched, 0);
                    }
                }
                drop(boot);
                let a = s::open(dir.path());
                parity(&a);
                drop(a);
                let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
                    .bootstrap_with_clock(MockClock::new(50))
                    .unwrap()
                    .with_host(s::host_support::host(
                        actionqueue_core::control::ControlScope::SingleTenant,
                    ));
                let _ = boot.run_until_idle().await.unwrap();
                assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
                let inputs = seen.lock().unwrap();
                assert_eq!(inputs.len(), 1);
                assert_eq!(inputs[0].resume_context, Some(context));
            }
        }
    }
}

#[tokio::test]
async fn replenish_awaiting_never_wakes_and_cancel_cleans_both_states() {
    for satisfy_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = budget_running(&mut a, None, false);
        let task = a.projection().get_run_instance(&r).unwrap().task_id();
        allocation(&mut a, task, DIMS[2]);
        yielding(&mut a, r, &[DIMS[2]], false);
        drop(a);
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(10000))
            .unwrap()
            .with_host(s::host_support::host(
                actionqueue_core::control::ControlScope::SingleTenant,
            ));
        assert!(boot.resume_run(r).is_err());
        for _ in 0..3 {
            assert_eq!(boot.tick().await.unwrap().dispatched, 0);
        }
        assert_eq!(boot.projection().get_budget(&task, DIMS[2]).unwrap().consumed, 1);
        if !satisfy_first {
            boot.replenish_budget(task, DIMS[2], 10).unwrap();
            assert_eq!(boot.tick().await.unwrap().dispatched, 0);
            assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Awaiting));
        }
        drop(boot);
        let mut a = s::open(dir.path());
        parity(&a);
        if satisfy_first {
            let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
            reconcile(&mut a, 10000).unwrap();
        }
        cancel(&mut a, r);
        assert!(a.projection().pending_resume(r).is_none());
        assert!(a.projection().waits().active(r).is_none());
        parity(&a);
        drop(a);
        let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(10001))
            .unwrap()
            .with_host(s::host_support::host(
                actionqueue_core::control::ControlScope::SingleTenant,
            ));
        boot.replenish_budget(task, DIMS[2], 100).unwrap();
        let _ = boot.run_until_idle().await.unwrap();
        assert!(seen.lock().unwrap().is_empty());
        assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Canceled));
    }
}

struct YieldOnce(Recording);
impl actionqueue_executor_local::handler::ExecutorHandler for YieldOnce {
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> AttemptDisposition {
        if ctx.input.resume_context.is_none() {
            AttemptDisposition::awaiting(spec(WaitId::new(), None), None)
                .with_consumption(DIMS.iter().map(|d| BudgetConsumption::new(*d, 1)).collect())
                .unwrap()
        } else {
            self.0.execute(ctx)
        }
    }
}
#[tokio::test]
async fn real_handler_yield_exhausts_and_preserves_input() {
    let dir = tempfile::tempdir().unwrap();
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let mut boot = ActionQueueEngine::new(config(dir.path()), YieldOnce(Recording(seen.clone())))
        .bootstrap_with_clock(MockClock::new(20))
        .unwrap()
        .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    let task = once_task(false, None);
    let task_id = task.id();
    boot.submit_task(task).unwrap();
    for dim in DIMS {
        boot.allocate_budget(task_id, dim, 1).unwrap();
    }
    let _ = boot.run_until_idle().await.unwrap();
    let r = boot.projection().runs_for_task(task_id).next().unwrap().id();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Awaiting));
    for dim in DIMS {
        assert!(boot.is_budget_exhausted(task_id, dim));
    }
    drop(boot);
    let mut a = s::open(dir.path());
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    parity(&a);
    drop(a);
    let mut boot = ActionQueueEngine::new(config(dir.path()), YieldOnce(Recording(seen.clone())))
        .bootstrap_with_clock(MockClock::new(30))
        .unwrap()
        .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    assert_eq!(boot.tick().await.unwrap().dispatched, 0);
    let context = boot.projection().pending_resume(r).unwrap();
    for dim in DIMS {
        boot.replenish_budget(task_id, dim, 2).unwrap();
    }
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(seen.lock().unwrap()[0].resume_context, Some(context));
}

#[tokio::test]
async fn structural_triggers_cannot_wake_awaiting_or_suspended() {
    use actionqueue_core::subscription::EventFilter;
    for suspended in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = budget_running(&mut a, None, false);
        if suspended {
            let e = command(&a, r, spec(WaitId::new(), None)).expected;
            actionqueue_runtime::disposition::commit(
                &mut a,
                e,
                AttemptDisposition::suspended(None, None),
                20,
            )
            .unwrap();
        } else {
            yielding(&mut a, r, &[], false);
        }
        drop(a);
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
            .bootstrap_with_clock(MockClock::new(30))
            .unwrap()
            .with_host(s::host_support::host(
                actionqueue_core::control::ControlScope::SingleTenant,
            ));
        let source = TaskId::new();
        boot.submit_task(
            TaskSpec::new(
                source,
                TaskPayload::new(vec![]),
                actionqueue_core::task::run_policy::RunPolicy::Once,
                TaskConstraints::default(),
                Default::default(),
            )
            .unwrap(),
        )
        .unwrap();
        let sub = boot
            .create_subscription(
                admission_support::id(1),
                EventFilter::TaskCompleted { task_id: source },
            )
            .unwrap();
        let _ = boot.run_until_idle().await.unwrap();
        assert!(boot
            .projection()
            .subscriptions()
            .find(|(id, _)| **id == sub)
            .unwrap()
            .1
            .triggered_at
            .is_some());
        assert_eq!(
            boot.projection().get_run_state(&r),
            Some(&if suspended { RunState::Suspended } else { RunState::Awaiting })
        );
        assert!(boot.projection().pending_resume(r).is_none());
        assert_eq!(boot.projection().get_run_instance(&r).unwrap().attempt_count(), 1);
        assert!(seen.lock().unwrap().iter().all(|i| i.run_id != r));
        drop(boot);
        let a = s::open(dir.path());
        parity(&a);
    }
}

struct FailOnce(Recording);
impl actionqueue_executor_local::handler::ExecutorHandler for FailOnce {
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> AttemptDisposition {
        self.0 .0.lock().unwrap().push(ctx.input);
        if self.0 .0.lock().unwrap().len() == 1 {
            AttemptDisposition::retryable_failure(BoundedError::new("retry").unwrap())
                .with_consumption(vec![BudgetConsumption::new(DIMS[0], 1)])
                .unwrap()
        } else {
            AttemptDisposition::complete(None)
        }
    }
}
#[tokio::test]
async fn failed_resumed_attempt_is_charged_once_and_keeps_its_input() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = budget_running(&mut a, None, false);
    yielding(&mut a, r, &[], false);
    let _ = s::submit(&mut a, s::envelope(1, 25)).unwrap();
    reconcile(&mut a, 30).unwrap();
    let context = a.projection().pending_resume(r).unwrap();
    allocation(&mut a, admission_support::id(1), DIMS[0]);
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let mut cfg = config(dir.path());
    cfg.backoff_strategy = actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
        interval: std::time::Duration::ZERO,
    };
    let mut boot = ActionQueueEngine::new(cfg, FailOnce(Recording(seen.clone())))
        .bootstrap_with_clock(MockClock::new(40))
        .unwrap()
        .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    let _ = boot.run_until_idle().await.unwrap();
    let run = boot.projection().get_run_instance(&r).unwrap();
    assert_eq!(run.attempt_count(), 2);
    assert_eq!(run.failure_attempt_count(), 1);
    for _ in 0..4 {
        assert_eq!(boot.tick().await.unwrap().dispatched, 0);
    }
    assert_eq!(
        boot.projection().get_budget(&admission_support::id(1), DIMS[0]).unwrap().consumed,
        1
    );
    boot.replenish_budget(admission_support::id(1), DIMS[0], 10).unwrap();
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(seen.iter().all(|i| i.resume_context == Some(context.clone())));
}

mod support;

struct AdmitChildAndExhaust {
    parent: TaskId,
    child: TaskId,
    recording: Recording,
}
impl actionqueue_executor_local::handler::ExecutorHandler for AdmitChildAndExhaust {
    fn execute(
        &self,
        ctx: actionqueue_executor_local::handler::ExecutorContext,
    ) -> AttemptDisposition {
        if ctx.input.payload == b"child" || ctx.input.resume_context.is_some() {
            return self.recording.execute(ctx);
        }
        let child = actionqueue_core::disposition::ChildAdmission::new(
            AdmissionKey::new("child").unwrap(),
            TaskSpec::new(
                self.child,
                TaskPayload::new(b"child".to_vec()),
                actionqueue_core::task::run_policy::RunPolicy::Once,
                TaskConstraints::default(),
                Default::default(),
            )
            .unwrap()
            .with_parent(self.parent),
            vec![],
            Default::default(),
        )
        .unwrap();
        AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(
                    WaitSpec::children(
                        WaitId::new(),
                        vec![self.child],
                        ChildWaitPolicy::AllTerminal,
                        None,
                    )
                    .unwrap(),
                ),
                child_admissions: vec![child],
                consumption: vec![BudgetConsumption::new(DIMS[0], 1)],
                ..Default::default()
            },
        )
        .unwrap()
    }
}

#[tokio::test]
async fn atomic_child_wake_survives_budget_block_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let task = once_task(false, None);
    let parent = task.id();
    let child = TaskId::new();
    let mut boot = ActionQueueEngine::new(
        config(dir.path()),
        AdmitChildAndExhaust { parent, child, recording: Recording(seen.clone()) },
    )
    .bootstrap_with_clock(MockClock::new(20))
    .unwrap()
    .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    boot.submit_task(task).unwrap();
    boot.allocate_budget(parent, DIMS[0], 1).unwrap();
    let _ = boot.run_until_idle().await.unwrap();
    let r = boot.projection().runs_for_task(parent).next().unwrap().id();
    let context = boot.projection().pending_resume(r).unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Ready));
    assert_eq!(
        context.wake,
        WakeReason::Children {
            wait_id: context.wait_id().unwrap(),
            outcomes: vec![ChildOutcome { task_id: child, status: TaskTerminalStatus::Succeeded }],
        }
    );
    assert!(boot.is_budget_exhausted(parent, DIMS[0]));
    assert_eq!(seen.lock().unwrap().len(), 1); // Only the child completed.
    drop(boot);
    let a = s::open(dir.path());
    parity(&a); // Child admission, consumption and wake agree in WAL and snapshot recovery.
    drop(a);
    let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
        .bootstrap_with_clock(MockClock::new(30))
        .unwrap()
        .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    for _ in 0..3 {
        assert_eq!(boot.tick().await.unwrap().dispatched, 0);
        assert_eq!(boot.projection().pending_resume(r), Some(context.clone()));
        assert!(boot.projection().get_lease(&r).is_none());
        assert_eq!(boot.projection().get_run_instance(&r).unwrap().attempt_count(), 1);
        assert_eq!(boot.projection().get_budget(&parent, DIMS[0]).unwrap().consumed, 1);
    }
    boot.replenish_budget(parent, DIMS[0], 2).unwrap();
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
    let inputs = seen.lock().unwrap();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[1].run_id, r);
    assert_eq!(inputs[1].resume_context, Some(context));
}

#[tokio::test]
async fn inspection_distinguishes_budget_block_from_satisfied_wait() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = budget_running(&mut a, None, false);
    allocation(&mut a, admission_support::id(1), DIMS[0]);
    yielding(&mut a, r, &[DIMS[0]], true);
    reconcile(&mut a, 30).unwrap();
    let context = a.projection().pending_resume(r).unwrap();
    parity(&a);
    drop(a);
    let mut router = support::bootstrap_http_router(dir.path(), true);
    let value = support::run_get(&mut router, r).await;
    assert_eq!(value["state"], "Ready");
    assert_eq!(value["block_reason"], "budget");
    assert_eq!(value["last_wait_id"], context.wait_id().unwrap().to_string());
    assert_eq!(value["pending_resume"]["context_id"], context.context_id.0);
    assert!(value["lease"].is_null());
}

#[tokio::test]
async fn explicit_suspension_resume_still_obeys_budget_gate() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = budget_running(&mut a, None, false);
    let task = admission_support::id(1);
    allocation(&mut a, task, DIMS[0]);
    let expected = command(&a, r, spec(WaitId::new(), None)).expected;
    actionqueue_runtime::disposition::commit(
        &mut a,
        expected,
        AttemptDisposition::suspended(None, None)
            .with_consumption(vec![BudgetConsumption::new(DIMS[0], 1)])
            .unwrap(),
        20,
    )
    .unwrap();
    parity(&a);
    drop(a);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let mut boot = ActionQueueEngine::new(config(dir.path()), Recording(seen.clone()))
        .bootstrap_with_clock(MockClock::new(30))
        .unwrap()
        .with_host(s::host_support::host(actionqueue_core::control::ControlScope::SingleTenant));
    boot.resume_run(r).unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Ready));
    let context = boot.projection().pending_resume(r);
    for _ in 0..3 {
        assert_eq!(boot.tick().await.unwrap().dispatched, 0);
        assert_eq!(boot.projection().pending_resume(r), context);
    }
    boot.replenish_budget(task, DIMS[0], 10).unwrap();
    let _ = boot.run_until_idle().await.unwrap();
    assert_eq!(boot.projection().get_run_state(&r), Some(&RunState::Completed));
    assert_eq!(seen.lock().unwrap().len(), 1);
}
