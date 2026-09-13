// Persisted inputs and assertions for the additive developmental profile.
impl Embedded {
    pub fn developmental_setup(&mut self, case: u32) {
        match case {
            2 | 7 | 17 => {
                let count = if case == 17 {
                    150
                } else if case == 7 {
                    8
                } else {
                    3
                };
                let a = self.authority.as_mut().unwrap();
                for n in 1..=count {
                    let q = admission_support::request(n);
                    let mut t = q.task_spec().clone();
                    t.set_run_policy(RunPolicy::Once).unwrap();
                    if case == 7 {
                        let mut c = t.constraints().clone();
                        match n {
                            2 => {
                                t.set_metadata(actionqueue_core::task::metadata::TaskMetadata::new(
                                    vec![],
                                    9,
                                    None,
                                ))
                            }
                            3 => c.set_timeout_secs(Some(90)).unwrap(),
                            4 => c.set_concurrency_key(Some("explicit-lock".into())).unwrap(),
                            5 => c.set_required_executor_traits(Some(vec!["cpu".into()])).unwrap(),
                            _ => {}
                        }
                        t.set_constraints(c).unwrap();
                    }
                    let causal = if case == 2 && n == 1 {
                        q.causal_context().clone()
                    } else {
                        CausalContext::new(
                            q.causal_context().trace_id().clone(),
                            CorrelationId::new(format!("distribution-fingerprint/{n}")).unwrap(),
                        )
                        .with_origin_ref(
                            OpaqueRef::new(format!("protected-research/campaign-arm/{n}")).unwrap(),
                        )
                    };
                    let q = admission_support::with_causal(
                        &admission_support::with_spec(&q, t),
                        causal,
                    );
                    admission_support::ensure(a, q.clone(), 10).unwrap();
                    let run = a.projection().runs_for_task(q.task_spec().id()).next().unwrap().id();
                    self.runs.insert(n, run);
                    if case == 7 && n >= 7 {
                        transition(a, run, RunState::Ready, 11);
                        lease(a, run, 12);
                        start(a, run, 13);
                        let c = command(
                            a,
                            run,
                            spec(
                                WaitId::new(),
                                Some(WaitDeadline {
                                    at: n * 100,
                                    policy: WaitTimeoutPolicy::ResumeWithTimeout,
                                }),
                            ),
                        );
                        establish(a, c).unwrap();
                    }
                    #[cfg(feature = "budget")]
                    {
                        let _ = apply(
                            a,
                            MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
                                seq(a),
                                q.task_spec().id(),
                                actionqueue_core::budget::BudgetDimension::Token,
                                if case == 7 && n == 6 { 99 } else { 10 },
                                10,
                            )),
                        );
                    }
                }
            }
            8 => {
                self.execute(&Step::Start { task: 1 });
                self.execute(&Step::Fanout { task: 1, children: vec![2, 3] });
                let a = self.authority.as_mut().unwrap();
                let h = host();
                actionqueue_runtime::control::execute_control(
                    a,
                    &h,
                    actionqueue_runtime::control::ControlOperation::Cancel(CancelTarget::Task(
                        admission_support::id(1),
                    )),
                    &MockClock::new(31),
                )
                .unwrap();
                reconcile(a, 32).unwrap();
            }
            11 => {
                self.model_start(1);
                self.model_wait(1, 20, false);
                self.model_signal(21);
                self.model_dispatch(1, 22);
            }
            12 => {
                self.execute(&Step::Start { task: 1 });
                self.execute(&Step::Wait { task: 1, deadline: None });
                self.execute(&Step::Start { task: 2 });
                let a = self.authority.as_mut().unwrap();
                let mut e = s::envelope(1, 25);
                e.kind = SignalKind::new("unmatched").unwrap();
                let _ = s::submit(a, e).unwrap();
            }
            16 => {
                self.execute(&Step::Start { task: 1 });
                let a = self.authority.as_mut().unwrap();
                let r = self.runs[&1];
                let bytes = b"SYNTHETIC_CUSTOMER_ERP_CANARY_7dbe";
                let reference = DataRef::External(actionqueue_core::data_ref::ExternalDataRef {
                    scheme: DataScheme::new("protected-fixture").unwrap(),
                    locator: OpaqueRef::new("private://ERP_REFERENCE_CANARY/fixture").unwrap(),
                    hash: ContentHash::new(
                        HashAlgorithm::Sha256,
                        sha2::Sha256::digest(bytes).to_vec(),
                    )
                    .unwrap(),
                    size_bytes: Some(bytes.len() as u64),
                    content_type: None,
                });
                put(a, r, AttemptDisposition::complete(Some(reference)), 30);
            }
            _ => panic!("not a custom workload"),
        }
    }
    pub fn developmental_conflicts(&mut self) {
        let a = self.authority.as_mut().unwrap();
        let q = a.projection().task_admission(admission_support::id(1)).unwrap().request().clone();
        let original = a.projection().projection_digest().unwrap();
        let mut variants = vec![];
        for field in 0..9 {
            let mut t = q.task_spec().clone();
            let mut c = t.constraints().clone();
            match field {
                0 => t.set_payload(TaskPayload::new(b"changed".to_vec())),
                1 => t.set_run_policy(RunPolicy::repeat(2, 7).unwrap()).unwrap(),
                2 => c.set_max_attempts(7).unwrap(),
                3 => c.set_timeout_secs(Some(9)).unwrap(),
                4 => c.set_concurrency_key(Some("lock".into())).unwrap(),
                5 => c.set_concurrency_key_hold_policy(ConcurrencyKeyHoldPolicy::ReleaseOnRetry),
                6 => c.set_safety_level(actionqueue_core::task::safety::SafetyLevel::Transactional),
                7 => c.set_required_executor_traits(Some(vec!["cpu".into()])).unwrap(),
                _ => t.set_metadata(actionqueue_core::task::metadata::TaskMetadata::new(
                    vec![],
                    4,
                    None,
                )),
            }
            t.set_constraints(c).unwrap();
            variants.push(admission_support::with_spec(&q, t));
        }
        variants.push(admission_support::with_dependencies(&q, vec![admission_support::id(2)]));
        for c in [
            CausalContext::new(
                TraceId::new("changed").unwrap(),
                q.causal_context().correlation_id().clone(),
            ),
            CausalContext::new(
                q.causal_context().trace_id().clone(),
                CorrelationId::new("changed").unwrap(),
            ),
            q.causal_context().clone().with_origin_ref(OpaqueRef::new("changed").unwrap()),
            q.causal_context().clone().with_authorization_ref(OpaqueRef::new("changed").unwrap()),
        ] {
            variants.push(admission_support::with_causal(&q, c));
        }
        for v in variants {
            for _ in 0..2 {
                assert!(matches!(
                    admission_support::ensure(a, v.clone(), 99),
                    Err(actionqueue_runtime::admission::AdmissionError::Rejected(
                        actionqueue_core::admission::AdmissionRejection::Conflict { .. }
                    ))
                ));
                assert_eq!(a.projection().projection_digest().unwrap(), original);
            }
        }
    }
    pub fn developmental_denials(&mut self) {
        use actionqueue_core::control::{ControlScope, HostControlContext};
        use actionqueue_runtime::control::{execute_control, ControlOperation};
        let a = self.authority.as_mut().unwrap();
        let r = self.runs[&1];
        let w = a.projection().waits().active(r).unwrap().spec.wait_id();
        let original = a.projection().projection_digest().unwrap();
        let q = a.projection().task_admission(admission_support::id(1)).unwrap().request().clone();
        for actor_id in [None, Some(ActorId::new())] {
            let denied = HostControlContext {
                actor_id,
                scope: ControlScope::Tenant(TenantId::new()),
                attribution: ControlMutationContext::new(
                    OpaqueRef::new("protected-research/full-authority").unwrap(),
                ),
            };
            for op in [
                ControlOperation::AdmitTask(q.clone()),
                ControlOperation::AdmitSignal(s::request(2)),
                ControlOperation::Cancel(CancelTarget::Run(r)),
                ControlOperation::Cancel(CancelTarget::Task(q.task_spec().id())),
                ControlOperation::CancelWait { run_id: r, wait_id: w },
                ControlOperation::ResolveWait { run_id: r, wait_id: w },
            ] {
                assert!(execute_control(a, &denied, op, &MockClock::new(40)).is_err());
                assert_eq!(a.projection().projection_digest().unwrap(), original);
            }
        }
        // Removing authenticated host configuration denies even an exact duplicate.
        let (writer, p) = self.authority.take().unwrap().into_parts();
        let mut unbound = actionqueue_storage::mutation::StorageMutationAuthority::new(writer, p);
        assert!(actionqueue_runtime::admission::ensure_task(&mut unbound, q, &MockClock::new(40))
            .is_err());
        self.authority = Some(unbound.with_host(host()));
    }
    pub fn developmental_retry(&mut self) {
        let r = self.runs[&1];
        if self.a().projection().get_run_state(&r) == Some(&RunState::Running) {
            self.expire(2000);
            self.model_dispatch(1, 2001);
            self.model_complete(1, 2002);
        }
        let p = self.a().projection();
        let h = p.get_attempt_history(&r).unwrap();
        assert_eq!(h.len(), 3);
        assert_eq!(p.get_run_state(&r), Some(&RunState::Completed));
        assert_eq!(p.get_run_instance(&r).unwrap().failure_attempt_count(), 1);
        assert_eq!(h[1].finish_origin(), AttemptFinishOrigin::Recovery);
        let assignment = h[2].accepted_start().unwrap().assignment.unwrap();
        assert_eq!(assignment.previous_attempt_id, Some(h[1].attempt_id()));
        assert_eq!(assignment.delivery, ResumeDelivery::Recovery);
        assert_eq!(p.attempt_resume(r, h[1].attempt_id()), p.attempt_resume(r, h[2].attempt_id()));
    }
    pub fn metrics_text(&mut self) -> String {
        use std::sync::{Arc, RwLock};

        use actionqueue_daemon::{
            bootstrap::{ReadyStatus, RouterConfig},
            http::{RouterObservability, RouterStateInner},
        };
        use actionqueue_storage::wal::{InstrumentedWalWriter, WalAppendTelemetry};
        use http_body_util::BodyExt;
        use tower::ServiceExt;
        let expected = self.evidence();
        let (writer, p) = self.authority.take().unwrap().into_parts();
        let telemetry = WalAppendTelemetry::new();
        let a = Arc::new(Mutex::new(actionqueue_storage::mutation::StorageMutationAuthority::new(
            InstrumentedWalWriter::new(writer, telemetry.clone()),
            p.clone(),
        )));
        let state = Arc::new(
            RouterStateInner::with_control_authority(
                RouterConfig { control_enabled: false, metrics_enabled: true },
                Arc::new(RwLock::new(p)),
                RouterObservability {
                    metrics: Arc::new(
                        actionqueue_daemon::metrics::registry::MetricsRegistry::new(Some(
                            "127.0.0.1:0".parse().unwrap(),
                        ))
                        .unwrap(),
                    ),
                    wal_append_telemetry: telemetry,
                    clock: Arc::new(MockClock::new(100)),
                    recovery_observations:
                        actionqueue_storage::recovery::bootstrap::RecoveryObservations::zero(),
                },
                a,
                ReadyStatus::ready(),
            )
            .without_background_maintenance(),
        );
        let router = actionqueue_daemon::http::build_router(state);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let text = rt.block_on(async {
            let r = router
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/metrics")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), 200);
            String::from_utf8(r.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
        });
        self.authority = Some(s::reopen(&self.path));
        assert_eq!(self.evidence(), expected);
        text
    }
}
