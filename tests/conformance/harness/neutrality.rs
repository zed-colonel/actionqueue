//! Paired public workloads resume the same stores across every semantic boundary.
use std::{
    fs,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use actionqueue_core::{
    actor::ExecutorTraits,
    admission::EnsureTaskRequest,
    bounded::OpaqueRef,
    budget::{BudgetConsumption, BudgetDimension},
    causal::CausalContext,
    continuation::*,
    disposition::AttemptDisposition,
    ids::*,
    limits::SignalRetentionPolicy,
    task::{metadata::TaskMetadata, run_policy::RunPolicy, task_spec::TaskPayload},
    time::clock::Clock,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
    signals::SignalAdmissionError,
};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{engine, process};

#[derive(Clone, Deserialize)]
struct Fixture {
    initial_time: u64,
    deadline: u64,
    retention_time: u64,
    minimum_age_secs: u64,
    minimum_sequence_window: u64,
    expected_initial_dispatch: Vec<u8>,
    crash_cuts: Vec<String>,
}
fn fixture() -> Fixture {
    serde_json::from_str(include_str!(
        "../../../conformance/aq-cont-1/developmental/neutrality-v2.json"
    ))
    .unwrap()
}
#[derive(Clone)]
struct TestClock(Arc<AtomicU64>);
impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct Handler {
    seen: Arc<Mutex<Vec<u8>>>,
    deadline: u64,
}
impl ExecutorHandler for Handler {
    fn execute(&self, c: ExecutorContext) -> AttemptDisposition {
        let n = c.input.payload[0];
        self.seen.lock().unwrap().push(n);
        let d = if [1, 5].contains(&n) && c.input.resume_context.is_none() {
            let mut filter = engine::reference_filter();
            if n == 5 {
                filter.correlation_id = Some(CorrelationId::new("deadline-only").unwrap());
            }
            AttemptDisposition::awaiting(
                WaitSpec::new(
                    WaitId::new(),
                    filter,
                    WaitMatchPolicy::FirstMatch,
                    SignalEligibility::After(SignalSequence::new(0)),
                    (n == 5).then_some(WaitDeadline {
                        at: self.deadline,
                        policy: WaitTimeoutPolicy::ResumeWithTimeout,
                    }),
                )
                .unwrap(),
                None,
            )
        } else {
            if n == 5 {
                assert!(
                    matches!(c.input.resume_context.unwrap().wake, WakeReason::Deadline { deadline_at, .. } if deadline_at == self.deadline)
                );
            }
            AttemptDisposition::complete(None)
        };
        d.with_consumption(vec![BudgetConsumption::new(BudgetDimension::Token, 2)]).unwrap()
    }
}
type Runtime = BootstrappedEngine<Handler, TestClock>;
struct Workload {
    runtime: Runtime,
    clock: TestClock,
    seen: Arc<Mutex<Vec<u8>>>,
    protected: bool,
    fixture: Fixture,
}
impl Workload {
    fn open(path: &Path, protected: bool, now: u64) -> Self {
        let fixture = fixture();
        let seen = Arc::new(Mutex::new(vec![]));
        let clock = TestClock(Arc::new(AtomicU64::new(now)));
        let config = RuntimeConfig {
            data_dir: path.to_path_buf(),
            local_executor_traits: Some(ExecutorTraits::new(vec!["cpu".into()]).unwrap()),
            dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            signal_retention: SignalRetentionPolicy {
                minimum_age_secs: fixture.minimum_age_secs,
                minimum_sequence_window: fixture.minimum_sequence_window,
            },
            ..Default::default()
        };
        let runtime = ActionQueueEngine::new(
            config,
            Handler { seen: seen.clone(), deadline: fixture.deadline },
        )
        .bootstrap_with_clock(clock.clone())
        .unwrap()
        .with_host(engine::host());
        Self { runtime, clock, seen, protected, fixture }
    }
    fn advance(&self, now: u64) {
        self.clock.0.store(now, Ordering::SeqCst);
    }
    fn dispatch(&mut self) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let _ = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), self.runtime.run_until_idle())
                .await
                .unwrap()
                .unwrap()
        });
    }
    fn seed(&mut self) {
        for n in 1..=5 {
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
            let causal = if self.protected {
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
            self.runtime.ensure_task(q.clone()).unwrap();
            self.runtime
                .allocate_budget(
                    q.task_spec().id(),
                    BudgetDimension::Token,
                    if n == 1 { 2 } else { 10 },
                )
                .unwrap();
        }
        for n in 1..=3 {
            let q = AdmitSignalRequest::new(
                SignalId::new(format!("retention/{n}")).unwrap(),
                SignalNamespace::new("retention").unwrap(),
                SignalKind::new("receipt").unwrap(),
                Some(
                    CorrelationId::new(if self.protected {
                        "protected/retention"
                    } else {
                        "ordinary/retention"
                    })
                    .unwrap(),
                ),
                None,
                Some(
                    OpaqueRef::new(if self.protected {
                        "protected-research/arm"
                    } else {
                        "ordinary/source"
                    })
                    .unwrap(),
                ),
                None,
                None,
                None,
            )
            .unwrap();
            assert_eq!(self.runtime.admit_signal(q).unwrap().sequence().get(), n);
        }
        assert_eq!(
            self.runtime
                .pin_signal(
                    SignalId::new("retention/1").unwrap(),
                    SignalPinId::new("keep").unwrap(),
                    Default::default()
                )
                .unwrap(),
            1
        );
    }
    fn phase(&mut self, phase: usize) -> Value {
        let b = &self.fixture;
        match phase {
            0 => self.seed(),
            1 => {
                self.dispatch();
                assert_eq!(*self.seen.lock().unwrap(), self.fixture.expected_initial_dispatch);
                assert_eq!(self.runtime.projection().waits().active_count(), 2);
                self.advance(self.fixture.deadline - 1);
                self.dispatch();
                assert_eq!(self.runtime.projection().waits().active_count(), 2);
                assert_eq!(*self.seen.lock().unwrap(), self.fixture.expected_initial_dispatch);
                // Age and sequence horizons refuse premature retirement of unpinned receipts.
                assert!(self.candidates().is_empty());
                self.retirement_rejected(2);
            }
            2 => {
                self.advance(b.deadline);
                self.dispatch();
                assert_eq!(*self.seen.lock().unwrap(), vec![5]);
                self.runtime.admit_signal(engine::reference_signal(4)).unwrap();
                self.dispatch();
                assert_eq!(*self.seen.lock().unwrap(), vec![5]); // Exhausted task 1 cannot resume.
                assert_eq!(self.runtime.projection().waits().active_count(), 0);
                // Isolate the age boundary: sequence 1 is outside the window,
                // unpinned, and unrelated to a wait, but exactly minimum_age old.
                self.runtime
                    .unpin_signal(
                        SignalId::new("retention/1").unwrap(),
                        SignalPinId::new("keep").unwrap(),
                        Default::default(),
                    )
                    .unwrap();
                assert!(self.candidates().is_empty());
                self.retirement_rejected(1);
                self.runtime
                    .pin_signal(
                        SignalId::new("retention/1").unwrap(),
                        SignalPinId::new("keep").unwrap(),
                        Default::default(),
                    )
                    .unwrap();
                let p = self.runtime.projection();
                for (n, state, attempts, remaining) in [
                    (1, "ready", 1, Some(0)),
                    (2, "completed", 1, None),
                    (3, "completed", 1, None),
                    (4, "ready", 0, Some(10)),
                    (5, "completed", 2, None),
                ] {
                    let task = engine::reference_request(n).task_spec().id();
                    let r = p.runs_for_task(task).next().unwrap();
                    assert_eq!(
                        (r.state().label(), r.attempt_count(), r.failure_attempt_count()),
                        (state, attempts, 0)
                    );
                    assert_eq!(
                        self.runtime.budget_remaining(task, BudgetDimension::Token),
                        remaining
                    );
                    let budget = p.get_budget(&task, BudgetDimension::Token).unwrap();
                    assert_eq!(
                        budget.consumed,
                        match n {
                            4 => 0,
                            5 => 4,
                            _ => 2,
                        }
                    );
                    assert!(p.get_lease_metadata(&r.id()).is_none());
                }
            }
            3 => {
                self.advance(b.retention_time);
                assert!(self.candidates().is_empty());
                self.retirement_rejected(1);
                assert_eq!(
                    self.runtime
                        .unpin_signal(
                            SignalId::new("retention/1").unwrap(),
                            SignalPinId::new("keep").unwrap(),
                            Default::default()
                        )
                        .unwrap(),
                    1
                );
                assert_eq!(self.candidates(), vec![SignalSequence::new(1)]);
                assert_eq!(
                    self.runtime.retire_signals(self.candidates(), Default::default()).unwrap(),
                    1
                );
                assert!(self.candidates().is_empty());
                self.retirement_rejected(2);
                let s = self.runtime.signal_statistics();
                assert_eq!((s.retained, s.retired, s.pinned), (3, 1, 0));
                let r = self
                    .runtime
                    .projection()
                    .signals()
                    .get_signal(None, &SignalId::new("retention/1").unwrap())
                    .unwrap();
                assert_eq!(r.retirement().unwrap().timestamp, self.fixture.retention_time);
                self.dispatch();
                assert!(self.seen.lock().unwrap().is_empty());
            }
            _ => unreachable!(),
        }
        self.observation()
    }
    fn retirement_rejected(&mut self, sequence: u64) {
        let before = self.runtime.projection().projection_digest().unwrap();
        assert!(matches!(
            self.runtime.retire_signals(vec![SignalSequence::new(sequence)], Default::default()),
            Err(SignalAdmissionError::Rejected(SignalRejection::Protected))
        ));
        assert_eq!(self.runtime.projection().projection_digest().unwrap(), before);
    }
    fn candidates(&self) -> Vec<SignalSequence> {
        self.runtime.projection().signals().retirement_candidates(
            None,
            SignalRetentionPolicy {
                minimum_age_secs: self.fixture.minimum_age_secs,
                minimum_sequence_window: self.fixture.minimum_sequence_window,
            },
            self.clock.now(),
            100,
        )
    }
    fn observation(&self) -> Value {
        let p = self.runtime.projection();
        let tasks: Vec<_> = (1..=5).map(|n| {
            let task = engine::reference_request(n).task_spec().id();
            let runs: Vec<_> = p.runs_for_task(task).map(|r| {
                let wakes: Vec<_> = p.get_attempt_history(&r.id()).into_iter().flatten().filter_map(|a| p.attempt_resume(r.id(), a.attempt_id())).map(|c| match c.wake {
                    WakeReason::Deadline { deadline_at, .. } => json!({"deadline":deadline_at}),
                    WakeReason::Signal { signal_sequence, .. } => json!({"signal":signal_sequence.get()}),
                    _ => panic!("unexpected wake"),
                }).collect();
                json!({"state":r.state().label(),"attempts":r.attempt_count(),"failures":r.failure_attempt_count(),"scheduled":r.scheduled_at(),"lease":p.get_lease_metadata(&r.id()).is_some(),"deadline":p.waits().active(r.id()).and_then(|w| w.spec.deadline()).map(|d| d.at),"wakes":wakes})
            }).collect();
            json!({"task":n,"runs":runs,"budget":p.get_budget(&task, BudgetDimension::Token)})
        }).collect();
        let signals: Vec<_> = p.signals().records().map(|s| json!({"sequence":s.sequence().get(),"received":s.envelope().received_at,"retired":s.retirement().map(|r| r.timestamp),"pins":s.pins().len()})).collect();
        json!({"tasks":tasks,"signals":signals,"dispatch":*self.seen.lock().unwrap()})
    }
}
fn side(protected: bool) -> &'static str {
    if protected {
        "protected"
    } else {
        "ordinary"
    }
}
fn open_pair(path: &Path, phase: usize) -> Vec<Workload> {
    let f = fixture();
    let now = match phase {
        0 | 1 => f.initial_time,
        2 => f.deadline - 1,
        3 => f.deadline,
        4 => f.retention_time,
        _ => unreachable!(),
    };
    [false, true]
        .into_iter()
        .map(|protected| {
            let w = Workload::open(&path.join(side(protected)), protected, now);
            if phase > 0 {
                let expected =
                    fs::read_to_string(path.join(format!("{}.digest", side(protected)))).unwrap();
                assert_eq!(w.runtime.projection().projection_digest().unwrap().hex, expected);
            }
            w
        })
        .collect()
}
fn phase_pair(path: &Path, pair: &mut [Workload], phase: usize) -> Value {
    let mut observations = vec![];
    for w in pair {
        w.seen.lock().unwrap().clear();
        observations.push(w.phase(phase));
        fs::write(
            path.join(format!("{}.digest", side(w.protected))),
            w.runtime.projection().projection_digest().unwrap().hex,
        )
        .unwrap();
    }
    assert_eq!(observations[0], observations[1], "attribution changed phase {phase}");
    observations.remove(0)
}
/// Child keeps both stores live until the controller acknowledges and kills it.
pub fn worker() {
    let path = std::path::PathBuf::from(std::env::var_os("AQ_NEUTRALITY_STORE").unwrap());
    let phase: usize = std::env::var("AQ_NEUTRALITY_PHASE").unwrap().parse().unwrap();
    let mut pair = open_pair(&path, phase);
    let observation = phase_pair(&path, &mut pair, phase);
    fs::write(path.join(format!("phase-{phase}.json")), serde_json::to_vec(&observation).unwrap())
        .unwrap();
    process::halt(&format!("AQ_NEUTRALITY_COMMITTED {}", fixture().crash_cuts[phase]));
}
/// All variants act on the same paired stores throughout their four phases.
/// Cross-variant comparisons also require recovery to preserve dispatch and decisions.
pub fn check() {
    let dir = tempfile::tempdir().unwrap();
    let mut observations = vec![];
    for variant in ["ordinary", "replay", "crash"] {
        let path = dir.path().join(variant);
        fs::create_dir_all(&path).unwrap();
        let mut phases = vec![];
        if variant == "crash" {
            for phase in 0..3 {
                let mut cmd = Command::new(std::env::current_exe().unwrap());
                cmd.args(["--exact", "neutrality_worker", "--ignored", "--nocapture"])
                    .env("AQ_NEUTRALITY_STORE", &path)
                    .env("AQ_NEUTRALITY_PHASE", phase.to_string());
                process::kill_at(
                    cmd,
                    &format!("AQ_NEUTRALITY_COMMITTED {}", fixture().crash_cuts[phase]),
                );
                phases.push(
                    serde_json::from_slice(
                        &fs::read(path.join(format!("phase-{phase}.json"))).unwrap(),
                    )
                    .unwrap(),
                );
            }
            let mut pair = open_pair(&path, 3);
            phases.push(phase_pair(&path, &mut pair, 3));
        } else {
            let mut pair = open_pair(&path, 0);
            for phase in 0..4 {
                if variant == "replay" && phase > 0 {
                    for w in pair.drain(..) {
                        w.runtime.shutdown().unwrap();
                    }
                    pair = open_pair(&path, phase);
                }
                phases.push(phase_pair(&path, &mut pair, phase));
            }
        }
        // Retirement itself must survive another reopen in every variant.
        let pair = open_pair(&path, 4);
        assert!(pair.iter().all(|w| w.runtime.signal_statistics().retired == 1));
        observations.push(phases);
    }
    for variant in 1..3 {
        for phase in 0..4 {
            for key in ["dispatch", "signals", "tasks"] {
                assert_eq!(
                    observations[0][phase][key], observations[variant][phase][key],
                    "variant {variant} phase {phase} key {key}"
                );
            }
        }
    }
}
