//! Public-runtime and actual TCP/CLI workload drivers. No mutation commands are used.
#![allow(dead_code)]
use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use actionqueue_core::{
    admission::*,
    continuation::*,
    data_ref::DataRef,
    disposition::*,
    ids::*,
    run::RunState,
    task::{run_policy::RunPolicy, task_spec::TaskPayload},
    time::clock::Clock,
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use serde_json::{json, Value};

use super::engine;
#[derive(Clone)]
pub struct TestClock(Arc<AtomicU64>);
impl Clock for TestClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
#[derive(Clone)]
struct Handler {
    mode: String,
}
fn request(n: u64) -> EnsureTaskRequest {
    let q = engine::reference_request(n);
    let mut t = q.task_spec().clone();
    t.set_payload(TaskPayload::new(vec![n as u8]));
    t.set_run_policy(RunPolicy::Once).unwrap();
    engine::reference_with_spec(&q, t)
}
fn disposition(
    mode: &str,
    attempt: AttemptId,
    payload: &[u8],
    resume: Option<ResumeContext>,
) -> AttemptDisposition {
    if payload != [1] || mode == "admission" {
        return AttemptDisposition::complete(None);
    }
    if let Some(c) = resume {
        assert!(c.checkpoint.is_some());
        return AttemptDisposition::complete(None);
    }
    let cp = Some(CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        created_by_attempt: attempt,
        data: DataRef::from_bytes(b"public continuation".to_vec()).unwrap(),
    });
    if mode == "fanout" {
        let parent = request(1).task_spec().id();
        let children: Vec<_> = (2..=3)
            .map(|n| {
                ChildAdmission::new(
                    AdmissionKey::new(format!("public-child/{n}")).unwrap(),
                    request(n).task_spec().clone().with_parent_policy(
                        parent,
                        actionqueue_core::task::task_spec::ChildLifecyclePolicy::Required,
                    ),
                    vec![],
                    Default::default(),
                )
                .unwrap()
            })
            .collect();
        let ids = children.iter().map(|c| c.task_spec().id()).collect();
        return AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(
                    WaitSpec::children(WaitId::new(), ids, ChildWaitPolicy::AllTerminal, None)
                        .unwrap(),
                ),
                checkpoint: cp,
                child_admissions: children,
                ..Default::default()
            },
        )
        .unwrap();
    }
    AttemptDisposition::awaiting(
        WaitSpec::new(
            WaitId::new(),
            engine::reference_filter(),
            WaitMatchPolicy::FirstMatch,
            SignalEligibility::After(SignalSequence::new(0)),
            (mode == "deadline")
                .then_some(WaitDeadline { at: 1010, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
        )
        .unwrap(),
        cp,
    )
}
impl ExecutorHandler for Handler {
    fn execute(&self, c: ExecutorContext) -> AttemptDisposition {
        disposition(&self.mode, c.input.attempt_id, &c.input.payload, c.input.resume_context)
    }
}
fn actor() -> ActorId {
    "00000000-0000-0000-0000-000000000099".parse().unwrap()
}
fn host() -> actionqueue_core::control::HostControlContext {
    actionqueue_core::control::HostControlContext { actor_id: Some(actor()), ..engine::host() }
}
const TOKEN: &str = "AQ_CONFORMANCE_SYNTHETIC_TEST_TOKEN_0123456789";
struct Server {
    address: std::net::SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    authority: actionqueue_daemon::http::ControlMutationAuthority,
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
    }
}
impl Server {
    fn new(path: &Path, clock: TestClock) -> Self {
        use actionqueue_daemon::{
            bootstrap::{ReadyStatus, RouterConfig},
            http::{RouterObservability, RouterStateInner},
        };
        use actionqueue_storage::{
            store::{capabilities, open_store, OpenOptions},
            wal::{InstrumentedWalWriter, WalAppendTelemetry},
        };
        let session = open_store(
            path,
            if path.join("manifest.json").exists() {
                OpenOptions::ReadWrite
            } else {
                OpenOptions::Initialize {
                    features: capabilities().into_iter().filter(|f| f != "platform").collect(),
                }
            },
        )
        .unwrap();
        let (writer, p) = session.into_authority().unwrap().into_parts();
        let telemetry = WalAppendTelemetry::new();
        let a = Arc::new(Mutex::new(actionqueue_storage::mutation::StorageMutationAuthority::new(
            InstrumentedWalWriter::new(writer, telemetry.clone()),
            p.clone(),
        )));
        let state = Arc::new(
            RouterStateInner::with_control_authority(
                RouterConfig { control_enabled: true, metrics_enabled: true },
                Arc::new(RwLock::new(p)),
                RouterObservability {
                    metrics: Arc::new(
                        actionqueue_daemon::metrics::registry::MetricsRegistry::new(Some(
                            "127.0.0.1:0".parse().unwrap(),
                        ))
                        .unwrap(),
                    ),
                    wal_append_telemetry: telemetry,
                    clock: Arc::new(clock),
                    recovery_observations:
                        actionqueue_storage::recovery::bootstrap::RecoveryObservations::zero(),
                },
                a.clone(),
                ReadyStatus::ready(),
            )
            .without_background_maintenance()
            .with_host_authenticator(Arc::new(|headers, _| {
                if headers
                    .get("authorization")
                    .is_some_and(|v| v == format!("Bearer {TOKEN}").as_str())
                {
                    Ok(host())
                } else {
                    Err(actionqueue_daemon::http::auth::AuthenticationError)
                }
            })),
        );
        let router = actionqueue_daemon::http::build_router(state.clone());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                axum::serve(tokio::net::TcpListener::from_std(listener).unwrap(), router)
                    .with_graceful_shutdown(async {
                        let _ = rx.await;
                    })
                    .await
                    .unwrap();
            });
        });
        Self { address, stop: Some(tx), thread: Some(thread), authority: a }
    }
    fn send(&self, method: &str, path: &str, body: Value, authenticated: bool) -> (u16, Value) {
        use http_body_util::BodyExt;
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let stream = tokio::net::TcpStream::connect(self.address).await.unwrap();
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                        .await
                        .unwrap();
                tokio::spawn(async move {
                    connection.await.unwrap();
                });
                let mut req = axum::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .header("host", self.address.to_string())
                    .header("content-type", "application/json");
                if authenticated {
                    req = req.header("authorization", format!("Bearer {TOKEN}"));
                }
                let response = sender
                    .send_request(
                        req.body(axum::body::Body::from(if body.is_null() {
                            String::new()
                        } else {
                            body.to_string()
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status().as_u16();
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
            })
            .await
            .unwrap()
        })
    }
    fn ok(&self, method: &str, path: &str, body: Value) -> Value {
        let (status, v) = self.send(method, path, body, true);
        assert!((200..300).contains(&status), "{method} {path}: {status} {v}");
        v
    }
}
enum Backend {
    Local(Box<BootstrappedEngine<Handler, TestClock>>),
    Remote(Server),
}
pub struct Driver {
    backend: Option<Backend>,
    clock: TestClock,
    path: PathBuf,
    mode: String,
    cli: bool,
}
impl Driver {
    pub fn new(path: &Path, mode: &str, kind: &str) -> Self {
        let clock = TestClock(Arc::new(AtomicU64::new(1000)));
        let backend = if kind == "embedded" || kind == "adapter" {
            Backend::Local(Box::new(
                ActionQueueEngine::new(
                    RuntimeConfig {
                        data_dir: path.into(),
                        dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
                        ..Default::default()
                    },
                    Handler { mode: mode.into() },
                )
                .bootstrap_with_clock(clock.clone())
                .unwrap()
                .with_host(engine::host()),
            ))
        } else {
            Backend::Remote(Server::new(path, clock.clone()))
        };
        let d = Self {
            backend: Some(backend),
            clock,
            path: path.into(),
            mode: mode.into(),
            cli: kind == "cli",
        };
        if let Some(Backend::Remote(s)) = &d.backend {
            assert_eq!(s.send("GET", "/ready", Value::Null, false).0, 200);
            assert_eq!(s.send("GET", "/api/v2/tasks", Value::Null, false).0, 401);
            assert_eq!(
                s.send(
                    "POST",
                    "/api/v2/admissions:ensure",
                    json!({"origin_ref":"protected-research"}),
                    false
                )
                .0,
                401
            );
            let exists = s.authority.lock().unwrap().projection().get_actor(&actor()).is_some();
            if !exists {
                s.ok("POST","/api/v2/actors/register",json!({"protocol_version":actionqueue_actor::protocol::PROTOCOL_VERSION,"contract_revision":actionqueue_actor::protocol::CONTRACT_REVISION,"actor_id":actor(),"identity":"conformance-executor","executor_traits":["cpu"],"heartbeat_interval_secs":300}));
            }
        }
        d
    }
    fn cli(&self, args: &[String], body: Option<Value>) -> (bool, Value) {
        let Some(Backend::Remote(s)) = &self.backend else { panic!("CLI requires daemon") };
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        std::fs::write(&token, TOKEN).unwrap();
        let executable = std::env::var_os("AQ_CLI").map(PathBuf::from).unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/actionqueue")
        });
        let mut command = std::process::Command::new(executable);
        command.args(args).args([
            "--daemon",
            &format!("http://{}", s.address),
            "--token-file",
            token.to_str().unwrap(),
            "--json",
        ]);
        if let Some(body) = body {
            let file = dir.path().join("body.json");
            std::fs::write(&file, body.to_string()).unwrap();
            command.args(["--file", file.to_str().unwrap()]);
        }
        let result = super::process::output(command, None);
        let v = serde_json::from_slice(&result.stdout).unwrap_or(Value::Null);
        (result.status.success(), v)
    }
    pub fn admission(&mut self, conflict: bool) -> bool {
        let mut q = request(1);
        if conflict {
            let mut t = q.task_spec().clone();
            t.set_payload(TaskPayload::new(vec![9]));
            q = engine::reference_with_spec(&q, t);
        }
        if self.cli {
            return self.cli(&["ensure-task".into()], Some(serde_json::to_value(q).unwrap())).0;
        }
        match self.backend.as_mut().unwrap() {
            Backend::Local(b) => {
                let result = b.ensure_task(q);
                if conflict {
                    assert_eq!(result.unwrap_err().code(), "conflict");
                    false
                } else {
                    result.unwrap();
                    true
                }
            }
            Backend::Remote(s) => {
                let status = s
                    .send(
                        "POST",
                        "/api/v2/admissions:ensure",
                        serde_json::to_value(q).unwrap(),
                        true,
                    )
                    .0;
                if conflict {
                    assert_eq!(status, 409);
                    false
                } else {
                    assert!((200..300).contains(&status));
                    true
                }
            }
        }
    }
    pub fn signal(&mut self) {
        let q = engine::reference_signal(1);
        if self.cli {
            assert!(
                self.cli(
                    &["signal".into(), "admit".into()],
                    Some(serde_json::to_value(q).unwrap())
                )
                .0
            );
            return;
        }
        match self.backend.as_mut().unwrap() {
            Backend::Local(b) => {
                b.admit_signal(q).unwrap();
            }
            Backend::Remote(s) => {
                s.ok("POST", "/api/v2/signals", serde_json::to_value(q).unwrap());
            }
        }
    }
    pub fn cancel(&mut self) {
        let id = request(1).task_spec().id();
        if self.cli {
            assert!(self.cli(&["task".into(), "cancel".into(), id.to_string()], None).0);
            return;
        }
        match self.backend.as_mut().unwrap() {
            Backend::Local(b) => {
                b.cancel_task(id).unwrap();
            }
            Backend::Remote(s) => {
                s.ok("POST", &format!("/api/v2/tasks/{id}:cancel"), Value::Null);
            }
        }
    }
    pub fn drive(&mut self) {
        match self.backend.as_mut().unwrap() {
            Backend::Local(b) => {
                let rt =
                    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                let _ = rt.block_on(async {
                    tokio::time::timeout(std::time::Duration::from_secs(10), b.run_until_idle())
                        .await
                        .unwrap()
                        .unwrap()
                });
            }
            Backend::Remote(s) => {
                for _ in 0..16 {
                    let v =
                        s.ok("GET", &format!("/api/v2/actors/{}/claimable", actor()), Value::Null);
                    let runs = v["runs"].as_array().unwrap();
                    if runs.is_empty() {
                        return;
                    }
                    for run in runs {
                        let attempt = AttemptId::new();
                        let w=s.ok("POST",&format!("/api/v2/actors/{}/claim",actor()),json!({"protocol_version":actionqueue_actor::protocol::PROTOCOL_VERSION,"contract_revision":actionqueue_actor::protocol::CONTRACT_REVISION,"run_id":run,"attempt_id":attempt}));
                        let payload: Vec<u8> =
                            serde_json::from_value(w["payload"].clone()).unwrap();
                        let resume: Option<ResumeContext> =
                            serde_json::from_value(w["resume_context"].clone()).unwrap();
                        let d = disposition(&self.mode, attempt, &payload, resume);
                        let result = actionqueue_actor::protocol::RemoteAttemptResult {
                            protocol_version: actionqueue_actor::protocol::PROTOCOL_VERSION,
                            contract_revision: actionqueue_actor::protocol::CONTRACT_REVISION
                                .into(),
                            run_id: serde_json::from_value(run.clone()).unwrap(),
                            attempt_id: attempt,
                            lease_fence: serde_json::from_value(w["lease_fence"].clone()).unwrap(),
                            disposition_digest:
                                actionqueue_core::disposition_digest::disposition_digest(&d),
                            disposition: d,
                        };
                        s.ok(
                            "POST",
                            &format!("/api/v2/actors/{}/result", actor()),
                            serde_json::to_value(result).unwrap(),
                        );
                    }
                }
                panic!("remote dispatch exceeded bounded workload");
            }
        }
    }
    pub fn projection(&self) -> actionqueue_storage::recovery::reducer::ReplayReducer {
        match self.backend.as_ref().unwrap() {
            Backend::Local(b) => b.projection().clone(),
            Backend::Remote(s) => s.authority.lock().unwrap().projection().clone(),
        }
    }
    pub fn prepare(&mut self) {
        assert!(self.admission(false));
        assert!(self.admission(false));
        assert!(!self.admission(true));
        if self.mode == "early" {
            self.signal();
            self.signal();
        } else if self.mode != "admission" {
            self.drive();
        }
        let p = self.projection();
        if ["late", "deadline", "cancel"].contains(&self.mode.as_str()) {
            assert_eq!(p.run_instances().next().unwrap().state(), RunState::Awaiting);
            assert_eq!(p.waits().active_count(), 1);
        }
        self.inspect_transport();
    }
    pub fn finish(&mut self) {
        match self.mode.as_str() {
            "late" => {
                self.signal();
                self.signal();
            }
            "cancel" => self.cancel(),
            "deadline" => {
                self.clock.0.store(1020, Ordering::SeqCst);
            }
            _ => {}
        }
        self.drive();
        let p = self.projection();
        assert_eq!(p.task_count(), if self.mode == "fanout" { 3 } else { 1 });
        let r = p.runs_for_task(request(1).task_spec().id()).next().unwrap();
        assert_eq!(
            r.state(),
            if self.mode == "cancel" { RunState::Canceled } else { RunState::Completed }
        );
        assert_eq!(r.failure_attempt_count(), 0);
        assert!(p.get_lease_metadata(&r.id()).is_none());
        assert_eq!(
            r.attempt_count(),
            if ["cancel", "admission"].contains(&self.mode.as_str()) { 1 } else { 2 }
        );
        if !["cancel", "admission"].contains(&self.mode.as_str()) {
            let history = p.get_attempt_history(&r.id()).unwrap();
            let c = p.attempt_resume(r.id(), history[1].attempt_id()).unwrap();
            let w = p.waits().records().find(|w| w.run_id == r.id()).unwrap();
            assert_eq!(c.checkpoint, w.checkpoint);
            assert_eq!(c.checkpoint.as_ref().unwrap().created_by_attempt, history[0].attempt_id());
            c.checkpoint.as_ref().unwrap().data.verify_bytes(b"public continuation").unwrap();
            match (&c.wake, self.mode.as_str()) {
                (WakeReason::Signal { signal_sequence, .. }, "early" | "late") => {
                    assert_eq!(signal_sequence.get(), 1)
                }
                (WakeReason::Deadline { deadline_at, .. }, "deadline") => {
                    assert_eq!(*deadline_at, 1010)
                }
                (WakeReason::Children { outcomes, .. }, "fanout") => assert_eq!(outcomes.len(), 2),
                _ => panic!("wrong public wake"),
            }
        }
        self.inspect_transport();
    }
    fn inspect_transport(&self) {
        let p = self.projection();
        let id = request(1).task_spec().id();
        if let Some(Backend::Remote(s)) = &self.backend {
            let v = s.ok("GET", &format!("/api/v2/tasks/{id}"), Value::Null);
            assert_eq!(v["id"], id.to_string());
            assert!(!v.to_string().contains("protected-research"));
            let run = p.runs_for_task(id).next().unwrap();
            let history =
                s.ok("GET", &format!("/api/v2/runs/{}/attempts?limit=1", run.id()), Value::Null);
            assert!(history["items"].as_array().unwrap().len() <= 1);
            if self.cli {
                let (ok, v) = self.cli(&["task".into(), "inspect".into(), id.to_string()], None);
                assert!(ok);
                assert_eq!(v["id"], id.to_string());
            }
        }
    }
    pub fn close(mut self) {
        match self.backend.take().unwrap() {
            Backend::Local(b) => b.shutdown().unwrap(),
            Backend::Remote(s) => drop(s),
        }
    }
}
pub fn observe(p: &actionqueue_storage::recovery::reducer::ReplayReducer) -> Value {
    json!({"digest":p.projection_digest().unwrap(),"inspection":engine::full_observation(p)})
}

pub fn summary(p: &actionqueue_storage::recovery::reducer::ReplayReducer) -> Value {
    let r = p.runs_for_task(request(1).task_spec().id()).next().unwrap();
    let wake = p
        .get_attempt_history(&r.id())
        .unwrap()
        .iter()
        .filter_map(|a| p.attempt_resume(r.id(), a.attempt_id()))
        .next_back()
        .map(|c| match c.wake {
            WakeReason::Signal { .. } => "signal",
            WakeReason::Deadline { .. } => "deadline",
            WakeReason::Children { .. } => "children",
            _ => "unexpected",
        });
    json!({"tasks":p.task_count(),"parent_state":r.state().label(),"parent_attempts":r.attempt_count(),"parent_failures":r.failure_attempt_count(),"active_waits":p.waits().active_count(),"wake":wake})
}
