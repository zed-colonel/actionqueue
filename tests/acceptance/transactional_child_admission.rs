#![allow(dead_code, unused_imports)]
include!("child_support.rs");
#[test]
fn invalid_final_child_and_dependency_cycles_reject_every_effect() {
    for case in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let p = parent(&a, r);
        let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
        let xid = x.task_spec().id();
        let yid = admission_support::request(3).task_spec().id();
        let deps = match case {
            0 => vec![TaskId::new()],
            1 => vec![p],
            2 => vec![xid],
            _ => vec![],
        };
        let y = child(3, deps, ChildLifecyclePolicy::Required, p);
        let x = if case == 2 { child(2, vec![yid], ChildLifecyclePolicy::Required, p) } else { x };
        let targets = if case == 3 { vec![TaskId::new()] } else { vec![xid, yid] };
        let d = child_disposition(&a, r, vec![x, y], targets, ChildWaitPolicy::AllTerminal);
        reject_unchanged(&mut a, r, d);
        assert_eq!(a.projection().task_count(), 1);
        assert_eq!(a.projection().waits().active_count(), 0);
        parity(&a);
    }
}
#[test]
fn sibling_forward_references_commit_as_one_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let y = child(3, vec![], ChildLifecyclePolicy::Required, p);
    let yid = y.task_spec().id();
    let x = child(2, vec![yid], ChildLifecyclePolicy::Required, p);
    let xid = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x, y], vec![xid, yid], ChildWaitPolicy::AllTerminal);
    let before = seq(&a);
    put(&mut a, r, d, 20);
    assert_eq!(seq(&a), before + 1);
    assert_eq!(a.projection().task_admission(xid).unwrap().sequence(), before);
    assert_eq!(a.projection().waits().active(r).unwrap().sequence, before);
    parity(&a);
}
#[test]
fn exact_retry_preserves_original_producer_and_conflicting_policy_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x.clone()], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    let original = a.projection().task_admission(id).unwrap().clone();
    finish_child(&mut a, id, true, 30);
    reconcile(&mut a, 31).unwrap();
    lease(&mut a, r, 32);
    start(&mut a, r, 32);
    a.set_admission_limits(actionqueue_core::limits::AdmissionLimits {
        payload_bytes: 0,
        ..Default::default()
    });
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 33);
    assert_eq!(a.projection().task_admission(id), Some(&original));
    reconcile(&mut a, 34).unwrap();
    lease(&mut a, r, 35);
    start(&mut a, r, 35);
    let changed = child(2, vec![], ChildLifecyclePolicy::Detached, p);
    let d = child_disposition(&a, r, vec![changed], vec![id], ChildWaitPolicy::AllTerminal);
    let before = a.projection().projection_digest().unwrap();
    let c = proposal(&a, r, d, 36);
    assert!(a
        .submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    parity(&a);
}
#[test]
fn scoped_keys_separate_parent_runs_and_ignore_attempts() {
    use actionqueue_core::admission::canonical::scoped_child_key;
    let local = AdmissionKey::new("batch/0").unwrap();
    let p = TaskId::new();
    let r = RunId::new();
    assert_eq!(scoped_child_key(None, p, r, &local), scoped_child_key(None, p, r, &local));
    assert_ne!(
        scoped_child_key(None, p, r, &local),
        scoped_child_key(None, p, RunId::new(), &local)
    );
    assert_ne!(
        scoped_child_key(None, p, r, &local),
        scoped_child_key(None, TaskId::new(), r, &local)
    );
    assert_ne!(
        scoped_child_key(None, p, r, &local),
        scoped_child_key(Some(TenantId::new()), p, r, &local)
    );
}
#[test]
fn required_completion_is_rejected_but_detached_completion_is_allowed() {
    for policy in [ChildLifecyclePolicy::Required, ChildLifecyclePolicy::Detached] {
        let dir = tempfile::tempdir().unwrap();
        let mut a = s::open(dir.path());
        let r = running(&mut a, 1, None, false);
        let p = parent(&a, r);
        let x = child(2, vec![], policy, p);
        let id = x.task_spec().id();
        let d = AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(spec(
                    WaitId::new(),
                    Some(WaitDeadline { at: 21, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
                )),
                child_admissions: vec![x],
                ..Default::default()
            },
        )
        .unwrap();
        put(&mut a, r, d, 20);
        reconcile(&mut a, 21).unwrap();
        lease(&mut a, r, 22);
        start(&mut a, r, 22);
        let c = proposal(&a, r, AttemptDisposition::complete(None), 23);
        let before = a.projection().projection_digest().unwrap();
        let result = a.submit_command(
            MutationCommand::AttemptDispositionCommit(c),
            DurabilityPolicy::Immediate,
        );
        if policy == ChildLifecyclePolicy::Required {
            assert!(matches!(
                result,
                Err(MutationAuthorityError::Disposition(DispositionRejection::ChildrenNonterminal))
            ));
            assert_eq!(before, a.projection().projection_digest().unwrap());
            finish_child(&mut a, id, false, 24);
            put(&mut a, r, AttemptDisposition::complete(None), 25);
        } else {
            let _ = result.unwrap();
        }
        assert_eq!(a.projection().get_run_state(&r), Some(&RunState::Completed));
        parity(&a);
    }
}
#[test]
fn child_target_bounds_are_checked_before_deduplication_and_overrides_are_closed() {
    let id = TaskId::new();
    assert!(WaitSpec::children(WaitId::new(), vec![id; 65], ChildWaitPolicy::AllTerminal, None)
        .is_err());
    assert!(WaitSpec::children(WaitId::new(), vec![], ChildWaitPolicy::AllTerminal, None).is_err());
    assert!(serde_json::from_str::<CausalOverride>(r#"{"trace_id":"forbidden"}"#).is_err());
}
#[test]
fn child_namespaces_are_independent_for_two_scheduled_parent_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let other = a.projection().runs_for_task(p).find(|v| v.id() != r).unwrap().id();
    let x = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let xid = x.task_spec().id();
    let d = child_disposition(&a, r, vec![x], vec![xid], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    transition(&mut a, other, RunState::Ready, 30);
    lease(&mut a, other, 30);
    start(&mut a, other, 30);
    let base = child(3, vec![], ChildLifecyclePolicy::Required, p);
    let yid = base.task_spec().id();
    let y = ChildAdmission::new(
        AdmissionKey::new("local/2").unwrap(),
        base.task_spec().clone(),
        vec![],
        Default::default(),
    )
    .unwrap();
    let d = child_disposition(&a, other, vec![y], vec![yid], ChildWaitPolicy::AllTerminal);
    put(&mut a, other, d, 31);
    assert_ne!(
        a.projection().task_admission(xid).unwrap().key(),
        a.projection().task_admission(yid).unwrap().key()
    );
    parity(&a);
}
#[test]
fn hierarchy_depth_and_unrelated_wait_targets_reject_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let root = parent(&a, r);
    let mut p = root;
    for n in 2..=9 {
        let spec = child(n, vec![], ChildLifecyclePolicy::Required, p).task_spec().clone();
        p = spec.id();
        admission_support::ensure(
            &mut a,
            actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
            20,
        )
        .unwrap();
    }
    let deep = a.projection().runs_for_task(p).next().unwrap().id();
    transition(&mut a, deep, RunState::Ready, 30);
    lease(&mut a, deep, 30);
    start(&mut a, deep, 30);
    let x = child(10, vec![], ChildLifecyclePolicy::Required, p);
    let id = x.task_spec().id();
    let d = child_disposition(&a, deep, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    let c = proposal(&a, deep, d, 31);
    let before = a.projection().projection_digest().unwrap();
    assert!(a
        .submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    let d = child_disposition(&a, r, vec![], vec![p], ChildWaitPolicy::AllTerminal);
    reject_unchanged(&mut a, r, d);
}
#[test]
fn low_level_completed_transition_cannot_bypass_required_child_gate() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let spec = child(2, vec![], ChildLifecyclePolicy::Required, p).task_spec().clone();
    admission_support::ensure(
        &mut a,
        actionqueue_core::admission::EnsureTaskRequest::for_task(spec, vec![]).unwrap(),
        20,
    )
    .unwrap();
    let id = a.projection().get_run_instance(&r).unwrap().current_attempt_id().unwrap();
    let finish = MutationCommand::AttemptFinish(AttemptFinishCommand::new(
        seq(&a),
        r,
        id,
        AttemptOutcome::success(),
        21,
    ));
    assert!(a.submit_command(finish, DurabilityPolicy::Immediate).is_err());
    let before = a.projection().projection_digest().unwrap();
    let c = RunStateTransitionCommand::new(seq(&a), r, RunState::Running, RunState::Completed, 22);
    assert!(a
        .submit_command(MutationCommand::RunStateTransition(c), DurabilityPolicy::Immediate)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
}
#[test]
fn causal_inheritance_preserves_attribution_and_limits_explicit_overrides() {
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open(dir.path());
    let r = running(&mut a, 1, None, false);
    let p = parent(&a, r);
    let original = a.projection().task_admission(p).unwrap().request().causal_context().clone();
    let base = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let id = base.task_spec().id();
    let overrides = CausalOverride {
        correlation_id: Some(CorrelationId::new("fork").unwrap()),
        requesting_actor_ref: Some(OpaqueRef::new("requester/2").unwrap()),
        origin_ref: Some(OpaqueRef::new("opaque/context/2").unwrap()),
    };
    let x = ChildAdmission::new(
        base.admission_key().clone(),
        base.task_spec().clone(),
        vec![],
        overrides.clone(),
    )
    .unwrap();
    let attempt = a.projection().get_run_instance(&r).unwrap().current_attempt_id().unwrap();
    let d = child_disposition(&a, r, vec![x], vec![id], ChildWaitPolicy::AllTerminal);
    put(&mut a, r, d, 20);
    let expected = original
        .with_correlation_id(overrides.correlation_id.unwrap())
        .with_requesting_actor_ref(overrides.requesting_actor_ref.unwrap())
        .with_origin_ref(overrides.origin_ref.unwrap())
        .with_causation(CausationLink::new(Some(p), Some(r), Some(attempt), None).unwrap());
    assert_eq!(a.projection().task_admission(id).unwrap().request().causal_context(), &expected);
    parity(&a);
}
#[test]
fn independent_v2_admission_scoped_key_wait_and_wake_vectors_match() {
    use actionqueue_core::admission::{canonical::*, EnsureTaskRequest};
    let v1: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/aq-cont-1/admission-v1-vector.json"))
            .unwrap();
    let v2: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/aq-cont-1/admission-v2-vector.json"))
            .unwrap();
    for policy in ["Required", "Detached"] {
        let mut request = v1["request"].clone();
        request["task_spec"]["child_lifecycle_policy"] = policy.into();
        let q: EnsureTaskRequest = serde_json::from_value(request).unwrap();
        let canonical = CanonicalAdmissionV2::new(&q).unwrap();
        let hex = |b: &[u8]| b.iter().map(|v| format!("{v:02x}")).collect::<String>();
        assert_eq!(hex(canonical.bytes()), v2[policy]["canonical_hex"]);
        assert_eq!(hex(q.digest().unwrap().hash().bytes()), v2[policy]["sha256"]);
    }
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../conformance/aq-cont-1/child-coordination-v1-vector.json"
    ))
    .unwrap();
    let key = scoped_child_key(
        None,
        v["parent"].as_str().unwrap().parse().unwrap(),
        v["run"].as_str().unwrap().parse().unwrap(),
        &AdmissionKey::new(v["local_key"].as_str().unwrap()).unwrap(),
    );
    assert_eq!(key.as_str(), v["scoped_key"]);
    let w: WaitSpec = serde_json::from_value(v["wait"].clone()).unwrap();
    assert_eq!(serde_json::to_value(&w).unwrap(), v["wait"]);
    let wake: WakeReason = serde_json::from_value(v["wake"].clone()).unwrap();
    assert_eq!(serde_json::to_value(wake).unwrap(), v["wake"]);
}
struct BatchedCoordinator {
    parent: TaskId,
}
impl actionqueue_executor_local::ExecutorHandler for BatchedCoordinator {
    fn execute(&self, ctx: actionqueue_executor_local::ExecutorContext) -> AttemptDisposition {
        if ctx.input.payload == b"leaf" {
            return AttemptDisposition::complete(None);
        }
        let batch=ctx.input.resume_context.as_ref().map_or(0,|resume| {
            assert!(matches!(&resume.wake,WakeReason::Children {outcomes,..} if outcomes.len()==2 && outcomes.iter().all(|o|o.status==TaskTerminalStatus::Succeeded)));
            match &resume.checkpoint.as_ref().unwrap().data {DataRef::Inline(data)=>data.bytes()[0],_=>panic!("checkpoint")}
        });
        if batch == 3 {
            return AttemptDisposition::complete(None);
        }
        let first = admission_support::id(100 + u64::from(batch) * 2);
        let second = admission_support::id(101 + u64::from(batch) * 2);
        let children = [(first, vec![]), (second, vec![first])]
            .into_iter()
            .map(|(id, deps)| {
                let task = TaskSpec::new(
                    id,
                    TaskPayload::new(b"leaf".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    Default::default(),
                )
                .unwrap();
                actionqueue_workflow::child_admission::child(
                    self.parent,
                    AdmissionKey::new(format!("batch/{batch}/{id}")).unwrap(),
                    task,
                    deps,
                    ChildLifecyclePolicy::Required,
                    CausalOverride {
                        origin_ref: Some(OpaqueRef::new(format!("opaque/arm/{id}")).unwrap()),
                        ..Default::default()
                    },
                )
                .unwrap()
            })
            .collect();
        let cp = CheckpointRef {
            checkpoint_id: CheckpointId::new(),
            created_by_attempt: ctx.input.attempt_id,
            data: DataRef::from_bytes(vec![batch + 1]).unwrap(),
        };
        actionqueue_workflow::child_admission::awaiting_children(
            WaitSpec::children(
                WaitId::new(),
                vec![first, second],
                ChildWaitPolicy::AllTerminal,
                None,
            )
            .unwrap(),
            Some(cp),
            children,
        )
        .unwrap()
    }
}
#[tokio::test]
async fn aq_dd_005_bounded_multibatch_fanout_uses_checkpoints_and_ordinary_dag_edges() {
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
    let dir = tempfile::tempdir().unwrap();
    let p = admission_support::id(1);
    let mut engine = ActionQueueEngine::new(
        RuntimeConfig { data_dir: dir.path().into(), ..Default::default() },
        BatchedCoordinator { parent: p },
    )
    .bootstrap_with_clock(MockClock::new(1000))
    .unwrap()
    .with_host(actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
        ),
    });
    let mut task = admission_support::request(1).task_spec().clone();
    task.set_run_policy(RunPolicy::Once).unwrap();
    let q = admission_support::with_spec(&admission_support::request(1), task);
    engine.ensure_task(q).unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(15), engine.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(engine.projection().task_count(), 7);
    assert_eq!(engine.projection().task_terminal_status(p), Some(TaskTerminalStatus::Succeeded));
    let run = engine.projection().runs_for_task(p).next().unwrap();
    assert_eq!(run.attempt_count(), 4);
    for batch in 0..3 {
        let first = admission_support::id(100 + batch * 2);
        let second = admission_support::id(101 + batch * 2);
        let first_run = engine.projection().runs_for_task(first).next().unwrap();
        let second_run = engine.projection().runs_for_task(second).next().unwrap();
        let complete = engine.projection().get_attempt_history(&first_run.id()).unwrap()[0]
            .disposition
            .as_ref()
            .unwrap()
            .sequence;
        let started = engine.projection().get_attempt_history(&second_run.id()).unwrap()[0]
            .accepted_start()
            .unwrap()
            .sequence;
        assert!(complete < started);
        let origin = engine.projection().task_admission(second).unwrap().request().causal_context();
        assert_eq!(
            origin.purpose_ref(),
            engine.projection().task_admission(p).unwrap().request().causal_context().purpose_ref()
        );
    }
    let digest = engine.projection().projection_digest().unwrap();
    engine.shutdown().unwrap();
    let a = s::reopen(dir.path());
    assert_eq!(digest, a.projection().projection_digest().unwrap());
    parity(&a);
}
#[cfg(feature = "platform")]
#[test]
fn cross_tenant_final_child_rejects_the_entire_batch() {
    use actionqueue_core::platform::TenantRegistration;
    let dir = tempfile::tempdir().unwrap();
    let mut a = s::open_platform(dir.path());
    let own = TenantId::new();
    commit!(
        &mut a,
        MutationCommand::TenantCreate(TenantCreateCommand::new(
            seq(&a),
            TenantRegistration::new(own, "own"),
            1
        ))
    );
    let r = running_scoped(&mut a, 1, None, false, Some(own));
    let p = parent(&a, r);
    let tenant = TenantId::new();
    commit!(
        &mut a,
        MutationCommand::TenantCreate(TenantCreateCommand::new(
            seq(&a),
            TenantRegistration::new(tenant, "other"),
            15
        ))
    );
    let base = child(2, vec![], ChildLifecyclePolicy::Required, p);
    let x = ChildAdmission::new(
        base.admission_key().clone(),
        base.task_spec().clone().with_tenant(own),
        vec![],
        Default::default(),
    )
    .unwrap();
    let mut y = child(3, vec![], ChildLifecyclePolicy::Required, p);
    let xid = x.task_spec().id();
    let yid = y.task_spec().id();
    y = ChildAdmission::new(
        y.admission_key().clone(),
        y.task_spec().clone().with_tenant(tenant),
        vec![],
        Default::default(),
    )
    .unwrap();
    let d = child_disposition(&a, r, vec![x, y], vec![xid, yid], ChildWaitPolicy::AllTerminal);
    let e = command(&a, r, spec(WaitId::new(), None)).expected;
    let plans = d
        .child_admissions()
        .iter()
        .map(|c| {
            let q = actionqueue_core::admission::EnsureTaskRequest::for_task(
                c.task_spec().clone(),
                vec![],
            )
            .unwrap();
            let digest = q.digest().unwrap();
            actionqueue_engine::admission::plan_admission(q, digest, 20).unwrap()
        })
        .collect();
    let c = AttemptDispositionCommitCommand::new(e.clone(), d.clone(), 20).with_children(plans);
    let before = a.projection().projection_digest().unwrap();
    assert!(a
        .submit_command(MutationCommand::AttemptDispositionCommit(c), DurabilityPolicy::Immediate)
        .is_err());
    assert_eq!(before, a.projection().projection_digest().unwrap());
    actionqueue_runtime::disposition::commit(&mut a, e, d, 20).unwrap();
    assert!(a.projection().get_task(&xid).is_none());
    assert!(a.projection().get_task(&yid).is_none());
    assert_eq!(a.projection().waits().active_count(), 0);
    parity(&a);
}
