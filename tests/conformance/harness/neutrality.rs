//! Paired public dispatch workloads vary only ordinary opaque attribution.
use std::sync::{Arc, Mutex};

use actionqueue_core::{
    actor::ExecutorTraits,
    admission::EnsureTaskRequest,
    bounded::OpaqueRef,
    budget::{BudgetConsumption, BudgetDimension},
    causal::CausalContext,
    disposition::AttemptDisposition,
    ids::CorrelationId,
    task::{metadata::TaskMetadata, run_policy::RunPolicy, task_spec::TaskPayload},
    time::clock::MockClock,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};

use super::engine;
struct Handler(Arc<Mutex<Vec<u8>>>);
impl ExecutorHandler for Handler {
    fn execute(&self, c: ExecutorContext) -> AttemptDisposition {
        self.0.lock().unwrap().push(c.input.payload[0]);
        let d = if c.input.payload == [1] {
            use actionqueue_core::{continuation::*, ids::*};
            AttemptDisposition::awaiting(
                WaitSpec::new(
                    WaitId::new(),
                    engine::reference_filter(),
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::After(SignalSequence::new(0)),
                    None,
                )
                .unwrap(),
                None,
            )
        } else {
            AttemptDisposition::complete(None)
        };
        d.with_consumption(vec![BudgetConsumption::new(BudgetDimension::Token, 2)]).unwrap()
    }
}
pub fn check() {
    let dir = tempfile::tempdir().unwrap();
    let mut observations = vec![];
    for protected in [false, true] {
        let seen = Arc::new(Mutex::new(vec![]));
        let config = RuntimeConfig {
            data_dir: dir.path().join(if protected { "protected" } else { "ordinary" }),
            local_executor_traits: Some(ExecutorTraits::new(vec!["cpu".into()]).unwrap()),
            dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        };
        let mut b = ActionQueueEngine::new(config, Handler(seen.clone()))
            .bootstrap_with_clock(MockClock::new(1000))
            .unwrap()
            .with_host(engine::host());
        for n in 1..=4 {
            let q = engine::reference_request(n);
            let mut t = q.task_spec().clone();
            t.set_payload(TaskPayload::new(vec![n as u8]));
            t.set_run_policy(RunPolicy::Once).unwrap();
            t.set_metadata(TaskMetadata::new(vec![], n as i32, None));
            let mut constraints = t.constraints().clone();
            constraints.set_timeout_secs(Some(30)).unwrap();
            constraints
                .set_required_executor_traits(Some(vec![if n == 4 { "gpu" } else { "cpu" }.into()]))
                .unwrap();
            t.set_constraints(constraints).unwrap();
            let causal = if protected {
                CausalContext::new(
                    q.causal_context().trace_id().clone(),
                    CorrelationId::new(format!("protected/{n}")).unwrap(),
                )
                .with_origin_ref(OpaqueRef::new(format!("protected-research/arm/{n}")).unwrap())
            } else {
                q.causal_context().clone()
            };
            let q =
                EnsureTaskRequest::new(q.admission_key().clone(), t, vec![], causal, None).unwrap();
            b.ensure_task(q.clone()).unwrap();
            b.allocate_budget(
                q.task_spec().id(),
                BudgetDimension::Token,
                if n == 1 { 2 } else { 10 },
            )
            .unwrap();
        }
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _ = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), b.run_until_idle())
                .await
                .unwrap()
                .unwrap()
        });
        b.admit_signal(engine::reference_signal(1)).unwrap();
        let _ = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), b.run_until_idle())
                .await
                .unwrap()
                .unwrap()
        });
        assert!(b.is_budget_exhausted(
            engine::reference_request(1).task_spec().id(),
            BudgetDimension::Token
        ));
        assert_eq!(*seen.lock().unwrap(), vec![3, 2, 1]); // Priority, exhausted budget, and trait mismatch all exercised.
        let p = b.projection();
        let tasks: Vec<_> = (1..=4)
            .map(|n| {
                let task = engine::reference_request(n).task_spec().id();
                let r = p.runs_for_task(task).next().unwrap();
                (
                    n,
                    r.state(),
                    r.attempt_count(),
                    r.failure_attempt_count(),
                    r.scheduled_at(),
                    b.budget_remaining(task, BudgetDimension::Token),
                )
            })
            .collect();
        observations.push(tasks);
        b.shutdown().unwrap();
    }
    assert_eq!(observations[0], observations[1]);
}
