//! Test-side workload controller. Uses the public mutation, control and inspection APIs.
// Existing acceptance helpers each isolate their host fixture module.
#![allow(dead_code, unused_imports, clippy::duplicate_mod)]
include!("../../acceptance/child_support.rs");
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use actionqueue_runtime::inspection::{Inspector, Query};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Start { task: u64 },
    Signal { signal: u64 },
    Wait { task: u64, deadline: Option<u64> },
    Reconcile { at: u64 },
    Cancel { task: u64 },
    Finish { task: u64, success: bool },
    FinishOutput { task: u64, bytes: Vec<u8> },
    Fanout { task: u64, children: Vec<u64> },
    RetryAdmission { task: u64, conflict: bool },
    Snapshot,
    Restart,
    Inspect,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub schema_version: u32,
    pub id: String,
    pub required_features: Vec<String>,
    pub steps: Vec<Step>,
    pub expected_observations: String,
    pub recovery_cuts: Vec<usize>,
}
pub fn host() -> actionqueue_core::control::HostControlContext {
    actionqueue_core::control::HostControlContext {
        actor_id: None,
        scope: actionqueue_core::control::ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("conformance-host").unwrap()),
    }
}
pub fn full_observation(p: &ReplayReducer) -> Value {
    let h = host();
    let i = Inspector::new(p, &h, false, Default::default(), false, 100).unwrap();
    let q = Query { limit: Some(200), ..Default::default() };
    let mut tasks: Vec<_> = p.tasks().map(|t| t.id()).collect();
    tasks.sort();
    let mut runs: Vec<_> = p.run_instances().map(|r| r.id()).collect();
    runs.sort();
    json!({
        "trace": i.trace(&q).unwrap(),
        "tasks": tasks.iter().map(|id| json!({"task":i.get_task(*id).unwrap(), "controls":i.task_controls(*id,&q).unwrap()})).collect::<Vec<_>>(),
        "runs": runs.iter().map(|id| json!({"run":i.get_run(*id).unwrap(), "history":i.run_history(*id,&q).unwrap(), "attempts":i.list_attempts(*id,&q).unwrap(), "controls":i.run_controls(*id,&q).unwrap()})).collect::<Vec<_>>()
    })
}
pub fn exact_replay(a: &s::Authority) {
    let expected = full_observation(a.projection());
    let session = a.store_session().unwrap();
    use actionqueue_storage::wal::{fs_reader::WalFsReader, reader::WalReader};
    let mut reader = WalFsReader::for_session(session).unwrap();
    let mut p = ReplayReducer::new();
    while let Some(e) = reader.read_next().unwrap() {
        p.apply(&e).unwrap();
    }
    assert_eq!(a.projection().projection_digest().unwrap(), p.projection_digest().unwrap());
    assert_eq!(expected, full_observation(&p));
    let p = recover_read_only(session, RepairPolicy::Strict).unwrap().projection;
    assert_eq!(a.projection().projection_digest().unwrap(), p.projection_digest().unwrap());
    assert_eq!(expected, full_observation(&p));
}
pub struct Embedded {
    pub authority: Option<s::Authority>,
    pub path: PathBuf,
    pub runs: BTreeMap<u64, RunId>,
}
impl Embedded {
    pub fn new(path: &Path) -> Self {
        Self { authority: Some(s::open(path)), path: path.into(), runs: BTreeMap::new() }
    }
    pub fn reopen(path: &Path) -> Self {
        let a = s::reopen(path);
        let mut runs = BTreeMap::new();
        for task in a.projection().tasks() {
            let n = u64::try_from(task.id().as_uuid().as_u128()).expect("fixture task alias");
            let mut task_runs = a.projection().runs_for_task(task.id());
            let run = task_runs.next().expect("admitted run");
            assert!(task_runs.next().is_none(), "scenarios have one run per task");
            assert!(runs.insert(n, run.id()).is_none(), "unique task aliases");
        }
        Self { authority: Some(a), path: path.into(), runs }
    }
    pub fn a(&self) -> &s::Authority {
        self.authority.as_ref().unwrap()
    }
    pub fn execute(&mut self, step: &Step) -> Value {
        self.apply_step(step);
        self.observe()
    }
    pub fn apply_step(&mut self, step: &Step) {
        let a = self.authority.as_mut().unwrap();
        match step {
            Step::Start { task } => {
                let q = admission_support::request(*task);
                let mut t = q.task_spec().clone();
                t.set_run_policy(RunPolicy::Once).unwrap();
                admission_support::ensure(a, admission_support::with_spec(&q, t), 10).unwrap();
                let r =
                    a.projection().runs_for_task(admission_support::id(*task)).next().unwrap().id();
                transition(a, r, RunState::Ready, 11);
                lease(a, r, 12);
                start(a, r, 13);
                self.runs.insert(*task, r);
            }
            Step::Signal { signal } => {
                s::admit(a, *signal, 25).unwrap();
            }
            Step::Wait { task, deadline } => {
                let r = self.runs[task];
                let mut c = command(
                    a,
                    r,
                    spec(
                        WaitId::new(),
                        deadline.map(|at| WaitDeadline {
                            at,
                            policy: WaitTimeoutPolicy::ResumeWithTimeout,
                        }),
                    ),
                );
                c.checkpoint = Some(checkpoint(a, r, b"opaque checkpoint"));
                establish(a, c).unwrap();
            }
            Step::Reconcile { at } => {
                reconcile(a, *at).unwrap();
            }
            Step::Cancel { task } => cancel(a, self.runs[task]),
            Step::Finish { task, success } => {
                let r = self.runs[task];
                if a.projection().get_run_state(&r) == Some(&RunState::Ready) {
                    lease(a, r, 40);
                    start(a, r, 40);
                }
                put(
                    a,
                    r,
                    if *success {
                        AttemptDisposition::complete(None)
                    } else {
                        AttemptDisposition::terminal_failure(
                            BoundedError::new("external tool failure").unwrap(),
                        )
                    },
                    45,
                );
            }
            Step::FinishOutput { task, bytes } => {
                let r = self.runs[task];
                put(
                    a,
                    r,
                    AttemptDisposition::complete(Some(DataRef::from_bytes(bytes.clone()).unwrap())),
                    45,
                );
            }
            Step::Fanout { task, children } => {
                let r = self.runs[task];
                let p = parent(a, r);
                let cs: Vec<_> = children
                    .iter()
                    .map(|n| child(*n, vec![], ChildLifecyclePolicy::Required, p))
                    .collect();
                let ids = cs.iter().map(|c| c.task_spec().id()).collect();
                let d = child_disposition(a, r, cs, ids, ChildWaitPolicy::AllTerminal);
                put(a, r, d, 20);
                for n in children {
                    self.runs.insert(
                        *n,
                        a.projection()
                            .runs_for_task(admission_support::id(*n))
                            .next()
                            .unwrap()
                            .id(),
                    );
                }
            }
            Step::RetryAdmission { task, conflict } => {
                let original = a
                    .projection()
                    .task_admission(admission_support::id(*task))
                    .unwrap()
                    .request()
                    .clone();
                let before = a.projection().projection_digest().unwrap();
                let mut request = original.clone();
                if *conflict {
                    let mut t = original.task_spec().clone();
                    t.set_payload(TaskPayload::new(b"changed".to_vec()));
                    request = admission_support::with_spec(&original, t);
                }
                let result = admission_support::ensure(a, request, 99);
                if *conflict {
                    assert!(matches!(
                        result,
                        Err(actionqueue_runtime::admission::AdmissionError::Rejected(
                            actionqueue_core::admission::AdmissionRejection::Conflict { .. }
                        ))
                    ));
                } else {
                    assert!(matches!(
                        result.unwrap(),
                        actionqueue_core::admission::EnsureTaskOutcome::AlreadyExists { .. }
                    ));
                }
                assert_eq!(before, a.projection().projection_digest().unwrap());
            }
            Step::Snapshot => {
                let snapshot = build_snapshot_from_projection(a.projection(), 0).unwrap();
                let mut w = SnapshotFsWriter::new(a.store_session().unwrap()).unwrap();
                w.write(&snapshot).unwrap();
                w.close().unwrap();
            }
            Step::Restart => {
                self.authority.take();
                self.authority = Some(s::reopen(&self.path));
            }
            Step::Inspect => {}
        }
    }
    pub fn observe(&self) -> Value {
        let a = self.a();
        let p = a.projection();
        let runs: BTreeMap<_,_>=self.runs.iter().map(|(n,r)| (n.to_string(),json!({"state":p.get_run_state(r).unwrap().label(),"attempts":p.get_run_instance(r).unwrap().attempt_count(),"failures":p.get_run_instance(r).unwrap().failure_attempt_count()}))).collect();
        json!({"runs":runs,"tasks":p.task_count(),"waits":p.waits().records().count(),"active_waits":p.waits().active_count(),"resolved_waits":p.waits().records().filter(|w|w.resolution.is_some()).count()})
    }
    pub fn verify(&self) {
        exact_replay(self.a());
    }
    pub fn evidence(&self) -> Value {
        json!({"digest":self.a().projection().projection_digest().unwrap(),"inspection":full_observation(self.a().projection()),"observation":self.observe()})
    }
}
pub fn read_scenario(root: &Path, path: &str) -> Scenario {
    let scenario: Scenario =
        serde_json::from_slice(&std::fs::read(root.join(path)).unwrap()).unwrap();
    assert_eq!(scenario.schema_version, 1);
    assert!(!scenario.steps.is_empty());
    assert!(scenario.recovery_cuts.iter().all(|n| *n > 0 && *n <= scenario.steps.len()));
    scenario
}
impl Embedded {
    pub fn expire(&mut self, at: u64) {
        actionqueue_runtime::waits::recover_execution(self.authority.as_mut().unwrap(), at)
            .unwrap();
    }
    pub fn assert_stale_disposition_rejected(&mut self, task: u64) {
        let r = self.runs[&task];
        let a = self.authority.as_mut().unwrap();
        let old = a.projection().get_attempt_history(&r).unwrap()[0].attempt_id();
        let before = a.projection().projection_digest().unwrap();
        let c = AttemptDispositionCommitCommand::new(
            AttemptCommitExpectation::new(
                seq(a),
                r,
                old,
                RunState::Running,
                LeaseFence::new("stale-worker".into(), 0),
            ),
            AttemptDisposition::complete(None),
            100,
        );
        assert!(a
            .submit_command(
                MutationCommand::AttemptDispositionCommit(c),
                DurabilityPolicy::Immediate
            )
            .is_err());
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
}

pub fn reference_request(n: u64) -> actionqueue_core::admission::EnsureTaskRequest {
    admission_support::request(n)
}
pub fn reference_with_spec(
    q: &actionqueue_core::admission::EnsureTaskRequest,
    t: TaskSpec,
) -> actionqueue_core::admission::EnsureTaskRequest {
    admission_support::with_spec(q, t)
}
pub fn reference_signal(n: u64) -> AdmitSignalRequest {
    s::request(n)
}
pub fn reference_filter() -> SignalFilter {
    s::filter()
}

impl Embedded {
    pub fn assert_rejected_fanout(&mut self, children: Vec<u64>) {
        let r = self.runs[&1];
        let a = self.authority.as_mut().unwrap();
        let before = a.projection().projection_digest().unwrap();
        let p = parent(a, r);
        let cs: Vec<_> =
            children.iter().map(|n| child(*n, vec![], ChildLifecyclePolicy::Required, p)).collect();
        let ids = cs.iter().map(|c| c.task_spec().id()).collect();
        let d = child_disposition(a, r, cs, ids, ChildWaitPolicy::AllTerminal);
        let c = proposal(a, r, d, 20);
        assert!(matches!(
            a.submit_command(
                MutationCommand::AttemptDispositionCommit(c),
                DurabilityPolicy::Immediate
            ),
            Err(MutationAuthorityError::Disposition(DispositionRejection::TooLarge))
        ));
        assert_eq!(before, a.projection().projection_digest().unwrap());
    }
}
