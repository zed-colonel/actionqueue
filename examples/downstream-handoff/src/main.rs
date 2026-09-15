//! Application-owned outbox, independent of ActionQueue's transaction boundary.
use actionqueue_core::{
    admission::{EnsureTaskOutcome, EnsureTaskRequest},
    bounded::OpaqueRef,
    causal::{CausalContext, ControlMutationContext},
    continuation::*,
    control::{ControlScope, HostControlContext},
    data_ref::DataRef,
    ids::{AdmissionKey, CheckpointId, CorrelationId, SignalId, TaskId, TraceId, WaitId},
    run::RunState,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{AttemptDisposition, ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
    time::Duration,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Serialize, Deserialize)]
struct Application {
    intent: String,
    // This immutable context belongs to the application, never the queue schema.
    context: Option<String>,
    admission: EnsureTaskRequest,
    admission_pending: bool,
    external_outcome: Option<String>,
    signal: Option<AdmitSignalRequest>,
    signal_pending: bool,
    verification_accepted: bool,
}
impl Application {
    fn new(developmental: bool) -> Self {
        let task = TaskId::new();
        let key = format!("execution/{task}");
        let context = developmental.then(|| "immutable application context revision 1".to_owned());
        let mut causal =
            CausalContext::new(TraceId::new(&key).unwrap(), CorrelationId::new(&key).unwrap());
        if developmental {
            causal =
                causal.with_origin_ref(OpaqueRef::new("application-context/revision-1").unwrap());
        }
        let spec = TaskSpec::new(
            task,
            TaskPayload::new(b"perform external operation".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap();
        Self {
            intent: "perform external operation".into(),
            context,
            admission: EnsureTaskRequest::new(
                AdmissionKey::new(key).unwrap(),
                spec,
                vec![],
                causal,
                None,
            )
            .unwrap(),
            admission_pending: true,
            external_outcome: None,
            signal: None,
            signal_pending: false,
            verification_accepted: false,
        }
    }
    fn read(dir: &Path) -> Result<Self> {
        Ok(serde_json::from_slice(&fs::read(dir.join("application.json"))?)?)
    }
    // Single writer, local POSIX filesystem. A failed sync is ambiguous: stop and
    // reopen, retaining the same request. Never acknowledge on queue timeout.
    fn commit(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join("application.next");
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        fs::rename(tmp, dir.join("application.json"))?;
        File::open(dir)?.sync_all()?;
        Ok(())
    }
    fn record_outcome(&mut self, dir: &Path) -> Result<()> {
        if self.signal.is_none() {
            self.external_outcome =
                Some("external operation finished; verifier rejected result".into());
            self.signal = Some(AdmitSignalRequest::new(
                SignalId::new(format!("outcome/{}", self.admission.task_spec().id()))?,
                SignalNamespace::new("application")?,
                SignalKind::new("finished")?,
                Some(self.admission.causal_context().correlation_id().clone()),
                None,
                None,
                Some(DataRef::from_bytes(
                    self.external_outcome.as_ref().unwrap().as_bytes().to_vec(),
                )?),
                None,
                Some(1),
            )?);
            self.signal_pending = true;
            self.commit(dir)?; // Outcome and outbox entry have one durable commit.
        }
        Ok(())
    }
}
struct Handler;
impl ExecutorHandler for Handler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        if let Some(resume) = &ctx.input.resume_context {
            let checkpoint = resume.checkpoint.as_ref().expect("checkpoint survives restart");
            checkpoint.data.verify_bytes(b"external operation requested").unwrap();
            assert_ne!(checkpoint.created_by_attempt, ctx.input.attempt_id);
            assert!(matches!(resume.wake, WakeReason::Signal { .. }));
            return AttemptDisposition::complete(None);
        }
        let filter = SignalFilter {
            tenant_id: None,
            namespace: SignalNamespace::new("application").unwrap(),
            kind: SignalKind::new("finished").unwrap(),
            correlation_id: Some(
                ctx.input.causal_context.as_ref().unwrap().correlation_id().clone(),
            ),
            source_ref: None,
        };
        AttemptDisposition::awaiting(
            WaitSpec::new(
                WaitId::new(),
                filter,
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::AnyRetained,
                None,
            )
            .unwrap(),
            Some(CheckpointRef {
                checkpoint_id: CheckpointId::new(),
                created_by_attempt: ctx.input.attempt_id,
                data: DataRef::from_bytes(b"external operation requested".to_vec()).unwrap(),
            }),
        )
    }
}
fn open(dir: &Path) -> Result<BootstrappedEngine<Handler>> {
    Ok(ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: dir.join("queue"),
            store_features: vec![],
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        },
        Handler,
    )
    .bootstrap()?
    .with_host(HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("application-host")?),
    }))
}
async fn drive(
    queue: &mut BootstrappedEngine<Handler>,
    task: TaskId,
    expected: RunState,
) -> Result<()> {
    for _ in 0..2000 {
        let _ = queue.tick().await?;
        if queue.projection().runs_for_task(task).any(|r| r.state() == expected) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Err(format!("did not reach {expected:?}").into())
}
async fn scenario(dir: &Path, developmental: bool, early: bool) -> Result<serde_json::Value> {
    // Caller supplies a fresh, durable parent directory. No concurrent writers.
    let app = Application::new(developmental);
    app.commit(dir)?; // Intent + all canonical request fields precede queue I/O.
    let mut queue = open(dir)?;
    assert!(queue.ensure_task(app.admission.clone())?.is_created());
    drop(queue); // Simulate losing the admission response, without acknowledging the outbox.
    let mut app = Application::read(dir)?;
    assert!(app.admission_pending);
    let mut queue = open(dir)?;
    assert!(matches!(
        queue.ensure_task(app.admission.clone())?,
        EnsureTaskOutcome::AlreadyExists { .. }
    ));
    app.admission_pending = false;
    app.commit(dir)?;
    let mut changed = app.admission.task_spec().clone();
    changed.set_payload(TaskPayload::new(b"changed meaning".to_vec()));
    let changed = EnsureTaskRequest::new(
        app.admission.admission_key().clone(),
        changed,
        vec![],
        app.admission.causal_context().clone(),
        None,
    )?;
    assert_eq!(queue.ensure_task(changed).unwrap_err().code(), "conflict");
    let task = app.admission.task_spec().id();
    if !early {
        drive(&mut queue, task, RunState::Awaiting).await?;
        drop(queue);
        queue = open(dir)?; // Restart while awaiting; no application wait registry.
        assert!(queue.projection().runs_for_task(task).all(|r| r.state() == RunState::Awaiting));
    }
    app.record_outcome(dir)?;
    assert!(matches!(
        queue.admit_signal(app.signal.clone().unwrap())?,
        AdmitSignalOutcome::Admitted { .. }
    ));
    drop(queue); // Lost signal acknowledgement; application retains the entry.
    app = Application::read(dir)?;
    assert!(app.signal_pending);
    queue = open(dir)?;
    assert!(matches!(
        queue.admit_signal(app.signal.clone().unwrap())?,
        AdmitSignalOutcome::AlreadyExists { .. }
    ));
    app.signal_pending = false;
    app.commit(dir)?;
    drive(&mut queue, task, RunState::Completed).await?;
    assert!(matches!(
        queue.admit_signal(app.signal.clone().unwrap())?,
        AdmitSignalOutcome::AlreadyExists { .. }
    ));
    let original = app.signal.as_ref().unwrap().envelope(&Default::default(), 0);
    let changed_signal = AdmitSignalRequest::new(
        original.signal_id,
        original.namespace,
        original.kind,
        original.correlation_id,
        original.causation,
        original.source_ref,
        Some(DataRef::from_bytes(b"changed outcome".to_vec())?),
        None,
        original.occurred_at,
    )?;
    assert_eq!(queue.admit_signal(changed_signal).unwrap_err().code(), "conflict");
    let run = queue.projection().runs_for_task(task).next().unwrap().id();
    let inspector = queue.inspector()?;
    let attempts = inspector.list_attempts(run, &Default::default())?;
    assert_eq!(attempts.items.len(), 2);
    let first = &attempts.items[0];
    let second = &attempts.items[1];
    assert_eq!(second.previous_attempt_id, Some(first.attempt_id));
    assert_eq!(second.resume.as_ref().unwrap().checkpoint_id, first.checkpoints.first().copied());
    let trace = inspector.trace(&Default::default())?;
    assert!(!Application::read(dir)?.verification_accepted);
    Ok(serde_json::json!({"queue_completed":true,"application_verification_accepted":false,
        "attempts": attempts, "trace":trace}))
}
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let dir = args
        .get(1)
        .ok_or("usage: downstream-handoff EXISTING_EMPTY_DIR [developmental] [early]")?;
    if fs::read_dir(dir)?.next().is_some() {
        return Err("use an empty directory".into());
    }
    let result = scenario(
        Path::new(dir),
        args.iter().any(|x| x == "developmental"),
        args.iter().any(|x| x == "early"),
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn durable_outboxes_restart_duplicates_and_lineage_in_both_modes() {
        for developmental in [false, true] {
            for early in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let result = scenario(dir.path(), developmental, early).await.unwrap();
                assert_eq!(result["queue_completed"], true);
                assert_eq!(result["application_verification_accepted"], false);
                assert_eq!(result["attempts"]["items"].as_array().unwrap().len(), 2);
            }
        }
    }
}
