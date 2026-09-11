//! Separate-binary compatibility probe used by the AQ-03 conformance script.
use actionqueue_core::{
    continuation::*,
    ids::TaskId,
    ids::{AttemptId, RunId, SignalSequence, WaitId},
    mutation::*,
    run::RunState,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_storage::{
    recovery::bootstrap::recover_read_only,
    snapshot::{
        build::build_snapshot_from_projection,
        writer::{SnapshotFsWriter, SnapshotWriter},
    },
    store::*,
    wal::repair::RepairPolicy,
    wal::{
        codec,
        event::{WalEvent, WalEventType as E},
        fs_writer::WalFsWriter,
        writer::WalWriter,
    },
};
fn spec() -> TaskSpec {
    TaskSpec::new(
        id(),
        TaskPayload::new(vec![0, 1, 255]),
        RunPolicy::repeat(3, 7).unwrap(),
        TaskConstraints::default(),
        TaskMetadata::new(vec![], 17, None),
    )
    .unwrap()
}
fn id() -> TaskId {
    "11111111-1111-4111-8111-111111111111".parse().unwrap()
}
fn admission_event() -> Result<WalEvent, Box<dyn std::error::Error>> {
    let request = actionqueue_core::admission::EnsureTaskRequest::for_task(spec(), vec![])?;
    let record = actionqueue_storage::mutation::admission::AdmissionRecord::new(
        request.clone(),
        request.digest()?,
        42,
        2,
    )?;
    let runs = (0..3)
        .map(|n| {
            actionqueue_core::run::RunInstance::new_scheduled_with_id(
                format!("22222222-2222-4222-8222-{n:012}").parse().unwrap(),
                id(),
                42 + n * 7,
                42,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(WalEvent::new(2, E::AdmissionCommitted { record, runs }))
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).ok_or("missing mode")?;
    if mode == "wire" {
        for b in codec::encode(&admission_event()?)? {
            print!("{b:02x}");
        }
        println!();
        let (wait, resolution) = wait_records();
        for e in [
            E::WaitEstablished { record: wait },
            E::WaitSatisfied { record: resolution.clone() },
            E::WaitTimedOut {
                record: actionqueue_storage::mutation::wait::WaitResolution {
                    kind: actionqueue_storage::mutation::wait::WaitResolutionKind::Deadline,
                    ..resolution.clone()
                },
            },
            E::WaitCanceled {
                record: actionqueue_storage::mutation::wait::WaitResolution {
                    kind: actionqueue_storage::mutation::wait::WaitResolutionKind::Canceled(None),
                    ..resolution
                },
            },
        ] {
            for b in codec::encode(&WalEvent::new(9, e))? {
                print!("{b:02x}");
            }
            println!();
        }
        return Ok(());
    }
    let path = std::path::Path::new(args.get(2).ok_or("missing path")?);
    match mode.as_str() {
        "create" => {
            let session = open_store(path, OpenOptions::Initialize { features: capabilities() })?;
            let mut writer = WalFsWriter::new(session.clone())?;
            writer.append(&admission_event()?)?;
            writer.flush()?;
            let p = recover_read_only(&session, RepairPolicy::Strict)?.projection;
            let mut a = actionqueue_storage::mutation::StorageMutationAuthority::new(writer, p);
            let (w, _) = wait_records();
            for (from, to) in
                [(RunState::Scheduled, RunState::Ready), (RunState::Ready, RunState::Leased)]
            {
                let _ = a.submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        a.projection().latest_sequence() + 1,
                        w.run_id,
                        from,
                        to,
                        42,
                    )),
                    DurabilityPolicy::Immediate,
                )?;
            }
            let grant = a.projection().latest_sequence() + 1;
            let _ = a.submit_command(
                MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
                    grant, w.run_id, "probe", 100, 42,
                )),
                DurabilityPolicy::Immediate,
            )?;
            let _ = a.submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    a.projection().latest_sequence() + 1,
                    w.run_id,
                    RunState::Leased,
                    RunState::Running,
                    42,
                )),
                DurabilityPolicy::Immediate,
            )?;
            let _ = a.submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    a.projection().latest_sequence() + 1,
                    w.run_id,
                    w.attempt_id,
                    42,
                )),
                DurabilityPolicy::Immediate,
            )?;
            let _ = a.submit_command(
                MutationCommand::WaitEstablish(WaitEstablishCommand {
                    expected: AttemptCommitExpectation::new(
                        a.projection().latest_sequence() + 1,
                        w.run_id,
                        w.attempt_id,
                        RunState::Running,
                        LeaseFence::new(LeaseOwner::new("probe"), grant),
                    ),
                    wait: w.spec,
                    checkpoint: None,
                    timestamp: 43,
                }),
                DurabilityPolicy::Immediate,
            )?;
            let p = a.projection();
            let mut sw = SnapshotFsWriter::new(&session)?;
            sw.write(&build_snapshot_from_projection(&p, 42)?)?;
            sw.close()?;
        }
        "read" => {
            println!("{}", serde_json::to_string(&inspect_store(path)?)?);
        }
        "add-budget" => {
            let session = open_store(path, OpenOptions::ReadWrite)?;
            let mut writer = WalFsWriter::new(session)?;
            writer.append(&WalEvent::new(
                writer.current_sequence() + 1,
                E::BudgetAllocated {
                    task_id: id(),
                    dimension: actionqueue_core::budget::BudgetDimension::Token,
                    limit: 10,
                    timestamp: 43,
                },
            ))?;
            writer.flush()?;
        }
        _ => return Err("unknown mode".into()),
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn wait_records() -> (
    actionqueue_storage::mutation::wait::WaitRecord,
    actionqueue_storage::mutation::wait::WaitResolution,
) {
    use actionqueue_storage::mutation::wait::*;
    let run_id: RunId = "22222222-2222-4222-8222-000000000000".parse().unwrap();
    let attempt_id: AttemptId = "33333333-3333-4333-8333-333333333333".parse().unwrap();
    let wait_id: WaitId = "44444444-4444-4444-8444-444444444444".parse().unwrap();
    (
        WaitRecord {
            run_id,
            attempt_id,
            lease_owner: "probe".into(),
            lease_granted_at_sequence: 5,
            sequence: 8,
            timestamp: 43,
            spec: WaitSpec::new(
                wait_id,
                SignalFilter {
                    tenant_id: None,
                    namespace: SignalNamespace::new("callback").unwrap(),
                    kind: SignalKind::new("done").unwrap(),
                    correlation_id: None,
                    source_ref: None,
                },
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::After(SignalSequence::new(0)),
                Some(WaitDeadline { at: 100, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
            )
            .unwrap(),
            checkpoint: None,
            resolution: None,
        },
        WaitResolution {
            run_id,
            wait_id,
            sequence: 9,
            timestamp: 44,
            kind: WaitResolutionKind::Signal(SignalSequence::new(1)),
        },
    )
}
