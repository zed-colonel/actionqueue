//! Separate-binary compatibility probe used by the AQ-03 conformance script.
use actionqueue_core::{
    ids::TaskId,
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
