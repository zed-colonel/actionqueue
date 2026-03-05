//! Submit command execution path.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::mutation::{
    DurabilityPolicy, MutationAuthority, MutationCommand, RunCreateCommand, TaskCreateCommand,
};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use serde_json::json;

use crate::args::SubmitArgs;
use crate::cmd::{now_unix_seconds, resolve_data_dir, CliError, CommandOutput};

/// Executes submit command flow.
pub fn run(args: SubmitArgs) -> Result<CommandOutput, CliError> {
    let data_dir = resolve_data_dir(args.data_dir.as_deref());
    let task_id = TaskId::from_str(&args.task_id).map_err(|error| {
        CliError::validation(
            "invalid_task_id",
            format!("invalid task id '{}': {error}", args.task_id),
        )
    })?;

    let run_policy = parse_run_policy(&args.run_policy)?;
    let constraints = parse_constraints(args.constraints.as_deref())?;
    let metadata = parse_metadata(args.metadata.as_deref())?;
    let payload = load_payload(args.payload_path.as_deref())?;
    let now = now_unix_seconds()?;

    let task_payload = match args.content_type.clone() {
        Some(ct) => TaskPayload::with_content_type(payload, ct),
        None => TaskPayload::new(payload),
    };

    let task_spec = TaskSpec::new(task_id, task_payload, run_policy.clone(), constraints, metadata)
        .map_err(|error| {
            CliError::validation(
                "task_spec_invalid",
                format!("submit task spec failed validation: {error}"),
            )
        })?;

    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(
        &data_dir,
    )
    .map_err(|error| {
        CliError::runtime(
            "storage_bootstrap_failed",
            format!("unable to load storage projection: {error}"),
        )
    })?;

    let mut authority = actionqueue_storage::mutation::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    submit_task_create(&mut authority, task_spec.clone(), now)?;

    let clock = actionqueue_engine::time::clock::SystemClock;
    let derivation =
        actionqueue_engine::derive::derive_runs(&clock, task_id, task_spec.run_policy(), 0, now)
            .map_err(|error| {
                CliError::validation(
                    "run_derivation_failed",
                    format!("submit run derivation failed: {error}"),
                )
            })?;

    let runs_created = derivation.derived().len();
    for run in derivation.into_derived() {
        submit_run_create(&mut authority, run)?;
    }

    let latest_sequence = authority.projection().latest_sequence();
    if args.json {
        return Ok(CommandOutput::Json(json!({
            "command": "submit",
            "task_id": task_id.to_string(),
            "run_policy": format_run_policy(run_policy),
            "runs_created": runs_created,
            "latest_sequence": latest_sequence,
            "data_dir": data_dir.display().to_string(),
        })));
    }

    let lines = [
        "command=submit".to_string(),
        format!("task_id={task_id}"),
        format!("run_policy={}", format_run_policy(run_policy)),
        format!("runs_created={runs_created}"),
        format!("latest_sequence={latest_sequence}"),
        format!("data_dir={}", data_dir.display()),
    ];
    Ok(CommandOutput::Text(lines.join("\n")))
}

fn submit_task_create(
    authority: &mut actionqueue_storage::mutation::StorageMutationAuthority<
        actionqueue_storage::wal::InstrumentedWalWriter<
            actionqueue_storage::wal::fs_writer::WalFsWriter,
        >,
        actionqueue_storage::recovery::reducer::ReplayReducer,
    >,
    task_spec: TaskSpec,
    timestamp: u64,
) -> Result<(), CliError> {
    let sequence = next_sequence(authority)?;
    let command =
        MutationCommand::TaskCreate(TaskCreateCommand::new(sequence, task_spec, timestamp));
    authority
        .submit_command(command, DurabilityPolicy::Immediate)
        .map_err(map_authority_error)
        .map(|_| ())
}

fn submit_run_create(
    authority: &mut actionqueue_storage::mutation::StorageMutationAuthority<
        actionqueue_storage::wal::InstrumentedWalWriter<
            actionqueue_storage::wal::fs_writer::WalFsWriter,
        >,
        actionqueue_storage::recovery::reducer::ReplayReducer,
    >,
    run_instance: actionqueue_core::run::RunInstance,
) -> Result<(), CliError> {
    let sequence = next_sequence(authority)?;
    let command = MutationCommand::RunCreate(RunCreateCommand::new(sequence, run_instance));
    authority
        .submit_command(command, DurabilityPolicy::Immediate)
        .map_err(map_authority_error)
        .map(|_| ())
}

fn next_sequence(
    authority: &actionqueue_storage::mutation::StorageMutationAuthority<
        actionqueue_storage::wal::InstrumentedWalWriter<
            actionqueue_storage::wal::fs_writer::WalFsWriter,
        >,
        actionqueue_storage::recovery::reducer::ReplayReducer,
    >,
) -> Result<u64, CliError> {
    authority
        .projection()
        .latest_sequence()
        .checked_add(1)
        .ok_or_else(|| CliError::runtime("sequence_overflow", "next WAL sequence overflowed u64"))
}

fn map_authority_error(
    error: actionqueue_storage::mutation::MutationAuthorityError<
        actionqueue_storage::recovery::reducer::ReplayReducerError,
    >,
) -> CliError {
    match error {
        actionqueue_storage::mutation::MutationAuthorityError::Validation(validation) => {
            CliError::validation("mutation_validation_failed", validation.to_string())
        }
        actionqueue_storage::mutation::MutationAuthorityError::Append(append) => {
            CliError::runtime("wal_append_failed", append.to_string())
        }
        actionqueue_storage::mutation::MutationAuthorityError::PartialDurability {
            sequence,
            flush_error,
        } => CliError::runtime(
            "wal_partial_durability",
            format!("append succeeded at sequence {sequence} but flush failed: {flush_error}"),
        ),
        actionqueue_storage::mutation::MutationAuthorityError::Apply { sequence, source } => {
            CliError::runtime(
                "projection_apply_failed",
                format!(
                    "projection apply failed after durable append sequence {sequence}: {source}"
                ),
            )
        }
    }
}

fn parse_run_policy(raw: &str) -> Result<RunPolicy, CliError> {
    if raw.eq_ignore_ascii_case("once") {
        return Ok(RunPolicy::Once);
    }

    // Cron parsing: format is cron:EXPRESSION — split on first colon only.
    #[cfg(feature = "workflow")]
    {
        let parts: Vec<&str> = raw.splitn(2, ':').collect();
        if parts.len() == 2 && parts[0].eq_ignore_ascii_case("cron") {
            return RunPolicy::cron(parts[1])
                .map_err(|error| CliError::validation("invalid_run_policy", error.to_string()));
        }
    }

    // Repeat: repeat:N:SECONDS
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() == 3 && parts[0].eq_ignore_ascii_case("repeat") {
        let count = parts[1].parse::<u32>().map_err(|error| {
            CliError::validation(
                "invalid_run_policy",
                format!("invalid repeat count '{}': {error}", parts[1]),
            )
        })?;
        let interval_secs = parts[2].parse::<u64>().map_err(|error| {
            CliError::validation(
                "invalid_run_policy",
                format!("invalid repeat interval '{}': {error}", parts[2]),
            )
        })?;
        return RunPolicy::repeat(count, interval_secs)
            .map_err(|error| CliError::validation("invalid_run_policy", error.to_string()));
    }

    Err(CliError::validation(
        "invalid_run_policy",
        format!(
            "unsupported run policy '{raw}', expected 'once', 'repeat:N:SECONDS'{}",
            if cfg!(feature = "workflow") { ", or 'cron:EXPRESSION'" } else { "" }
        ),
    ))
}

fn format_run_policy(policy: RunPolicy) -> String {
    match policy {
        RunPolicy::Once => "once".to_string(),
        RunPolicy::Repeat(ref rp) => {
            format!("repeat:{}:{}", rp.count(), rp.interval_secs())
        }
        #[cfg(feature = "workflow")]
        RunPolicy::Cron(ref cp) => {
            format!("cron:{}", cp.expression())
        }
    }
}

fn parse_constraints(raw: Option<&str>) -> Result<TaskConstraints, CliError> {
    match raw {
        None => Ok(TaskConstraints::default()),
        Some(source) => {
            let json = read_inline_or_file(source)?;
            serde_json::from_str::<TaskConstraints>(&json).map_err(|error| {
                CliError::validation(
                    "invalid_constraints_json",
                    format!("failed to parse constraints JSON: {error}"),
                )
            })
        }
    }
}

fn parse_metadata(raw: Option<&str>) -> Result<TaskMetadata, CliError> {
    match raw {
        None => Ok(TaskMetadata::default()),
        Some(source) => {
            let json = read_inline_or_file(source)?;
            serde_json::from_str::<TaskMetadata>(&json).map_err(|error| {
                CliError::validation(
                    "invalid_metadata_json",
                    format!("failed to parse metadata JSON: {error}"),
                )
            })
        }
    }
}

fn load_payload(payload_path: Option<&Path>) -> Result<Vec<u8>, CliError> {
    match payload_path {
        None => Ok(Vec::new()),
        Some(path) => std::fs::read(path).map_err(|error| {
            CliError::validation(
                "payload_read_failed",
                format!("unable to read payload '{}': {error}", path.display()),
            )
        }),
    }
}

fn read_inline_or_file(raw: &str) -> Result<String, CliError> {
    if let Some(stripped) = raw.strip_prefix('@') {
        let path = PathBuf::from(stripped);
        return std::fs::read_to_string(&path).map_err(|error| {
            CliError::validation(
                "json_source_read_failed",
                format!("unable to read JSON source '{}': {error}", path.display()),
            )
        });
    }

    Ok(raw.to_string())
}
