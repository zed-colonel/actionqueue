//! Submit command execution path.

use std::path::{Path, PathBuf};
use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use serde_json::json;

use crate::args::SubmitArgs;
use crate::cmd::{resolve_data_dir, CliError, CommandOutput};

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

    let request = actionqueue_core::admission::EnsureTaskRequest::for_task(task_spec, vec![])
        .map_err(|e| CliError::validation("admission_rejected", e.to_string()))?;
    let outcome = actionqueue_runtime::admission::ensure_task(
        &mut authority,
        request,
        &actionqueue_engine::time::clock::SystemClock,
    )
    .map_err(|e| match e {
        actionqueue_runtime::admission::AdmissionError::Rejected(ref rejection) => {
            CliError::validation("admission_rejected", rejection.to_string())
        }
        e => CliError::runtime("admission_failed", e.to_string()),
    })?;
    let status = if outcome.is_created() { "created" } else { "already_exists" };
    let runs_created = if outcome.is_created() {
        authority.projection().run_ids_for_task(task_id).len()
    } else {
        0
    };
    let latest_sequence = authority.projection().latest_sequence();
    if args.json {
        return Ok(CommandOutput::Json(json!({
            "command": "submit",
            "admission_status": status,
            "admission_sequence": outcome.sequence(),
            "task_id": task_id.to_string(),
            "run_policy": format_run_policy(run_policy),
            "runs_created": runs_created,
            "latest_sequence": latest_sequence,
            "data_dir": data_dir.display().to_string(),
        })));
    }

    let lines = [
        "command=submit".to_string(),
        format!("admission_status={status}"),
        format!("admission_sequence={}", outcome.sequence()),
        format!("task_id={task_id}"),
        format!("run_policy={}", format_run_policy(run_policy)),
        format!("runs_created={runs_created}"),
        format!("latest_sequence={latest_sequence}"),
        format!("data_dir={}", data_dir.display()),
    ];
    Ok(CommandOutput::Text(lines.join("\n")))
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
        Some(path) => {
            use std::io::Read;
            let result = (|| -> std::io::Result<Vec<u8>> {
                let file = std::fs::File::open(path)?;
                let mut bytes = Vec::new();
                file.take(actionqueue_core::limits::MAX_INLINE_DATA_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)?;
                Ok(bytes)
            })();
            let bytes = result.map_err(|error| {
                CliError::validation(
                    "payload_read_failed",
                    format!("unable to read payload '{}': {error}", path.display()),
                )
            })?;
            if bytes.len() > actionqueue_core::limits::MAX_INLINE_DATA_BYTES {
                return Err(CliError::validation("admission_rejected", "payload exceeds 64 KiB"));
            }
            Ok(bytes)
        }
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
