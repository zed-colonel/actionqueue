//! CLI argument model and parsing.
//!
//! This module defines deterministic, side-effect-free argument parsing for the
//! ActionQueue CLI control-plane commands.

use std::path::PathBuf;

/// Root CLI command being invoked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Start daemon runtime bootstrap flow.
    Daemon(DaemonArgs),
    /// Submit a task through CLI control-plane semantics.
    Submit(SubmitArgs),
    /// Return aggregate runtime stats.
    Stats(StatsArgs),
}

/// Arguments for the `daemon` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonArgs {
    /// Optional data directory override.
    pub data_dir: Option<PathBuf>,
    /// Optional HTTP API bind address (`IP:PORT`).
    pub bind: Option<String>,
    /// Optional metrics bind address (`IP:PORT`).
    pub metrics_bind: Option<String>,
    /// Explicit control-endpoint enablement.
    pub enable_control: bool,
    /// Emit JSON success payload on stdout when true.
    pub json: bool,
}

/// Arguments for the `submit` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitArgs {
    /// Optional data directory override.
    pub data_dir: Option<PathBuf>,
    /// Task identifier string (UUID format).
    pub task_id: String,
    /// Optional path to payload bytes.
    pub payload_path: Option<PathBuf>,
    /// Optional payload content type.
    pub content_type: Option<String>,
    /// Run policy string (`once` or `repeat:N:SECONDS`).
    pub run_policy: String,
    /// Optional constraints JSON (inline or `@path`).
    pub constraints: Option<String>,
    /// Optional metadata JSON (inline or `@path`).
    pub metadata: Option<String>,
    /// Emit JSON success payload on stdout when true.
    pub json: bool,
}

/// Stats output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsOutputFormat {
    /// Human-readable text output.
    Text,
    /// Machine-readable JSON output.
    Json,
}

/// Arguments for the `stats` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsArgs {
    /// Optional data directory override.
    pub data_dir: Option<PathBuf>,
    /// Requested output format.
    pub format: StatsOutputFormat,
}

/// Parse command-line arguments into a typed command structure.
pub fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.is_empty() {
        return Err(String::from("No command provided. Use 'daemon', 'submit', or 'stats'."));
    }

    let command = &args[0];
    match command.as_str() {
        "daemon" => parse_daemon(&args[1..]),
        "submit" => parse_submit(&args[1..]),
        "stats" => parse_stats(&args[1..]),
        _ => Err(format!("Unknown command: {command}. Use 'daemon', 'submit', or 'stats'.")),
    }
}

fn require_value(iter: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    iter.next().cloned().ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_daemon(args: &[String]) -> Result<Command, String> {
    let mut data_dir: Option<PathBuf> = None;
    let mut bind: Option<String> = None;
    let mut metrics_bind: Option<String> = None;
    let mut enable_control = false;
    let mut json = false;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(require_value(&mut iter, "--data-dir")?)),
            "--bind" => bind = Some(require_value(&mut iter, "--bind")?),
            "--metrics-bind" => metrics_bind = Some(require_value(&mut iter, "--metrics-bind")?),
            "--enable-control" => enable_control = true,
            "--json" => json = true,
            "--help" | "-h" => return Err(USAGE_DAEMON.to_string()),
            unknown if unknown.starts_with('-') => {
                return Err(format!("Unknown option: {unknown}"))
            }
            unexpected => return Err(format!("Unexpected argument: {unexpected}")),
        }
    }

    Ok(Command::Daemon(DaemonArgs { data_dir, bind, metrics_bind, enable_control, json }))
}

fn parse_submit(args: &[String]) -> Result<Command, String> {
    let mut data_dir: Option<PathBuf> = None;
    let mut task_id: Option<String> = None;
    let mut payload_path: Option<PathBuf> = None;
    let mut content_type: Option<String> = None;
    let mut run_policy: Option<String> = None;
    let mut constraints: Option<String> = None;
    let mut metadata: Option<String> = None;
    let mut json = false;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(require_value(&mut iter, "--data-dir")?)),
            "--task-id" => task_id = Some(require_value(&mut iter, "--task-id")?),
            "--payload" => {
                payload_path = Some(PathBuf::from(require_value(&mut iter, "--payload")?))
            }
            "--content-type" => content_type = Some(require_value(&mut iter, "--content-type")?),
            "--run-policy" => run_policy = Some(require_value(&mut iter, "--run-policy")?),
            "--constraints" => constraints = Some(require_value(&mut iter, "--constraints")?),
            "--metadata" => metadata = Some(require_value(&mut iter, "--metadata")?),
            "--json" => json = true,
            "--help" | "-h" => return Err(USAGE_SUBMIT.to_string()),
            unknown if unknown.starts_with('-') => {
                return Err(format!("Unknown option: {unknown}"))
            }
            unexpected => return Err(format!("Unexpected argument: {unexpected}")),
        }
    }

    let task_id = task_id.ok_or_else(|| String::from("--task-id is required"))?;
    let run_policy = run_policy.ok_or_else(|| String::from("--run-policy is required"))?;

    Ok(Command::Submit(SubmitArgs {
        data_dir,
        task_id,
        payload_path,
        content_type,
        run_policy,
        constraints,
        metadata,
        json,
    }))
}

fn parse_stats(args: &[String]) -> Result<Command, String> {
    let mut data_dir: Option<PathBuf> = None;
    let mut format = StatsOutputFormat::Text;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(require_value(&mut iter, "--data-dir")?)),
            "--format" => {
                let raw = require_value(&mut iter, "--format")?;
                format = match raw.as_str() {
                    "text" => StatsOutputFormat::Text,
                    "json" => StatsOutputFormat::Json,
                    _ => {
                        return Err(format!("--format must be 'text' or 'json' (received '{raw}')"))
                    }
                };
            }
            "--json" => format = StatsOutputFormat::Json,
            "--help" | "-h" => return Err(USAGE_STATS.to_string()),
            unknown if unknown.starts_with('-') => {
                return Err(format!("Unknown option: {unknown}"))
            }
            unexpected => return Err(format!("Unexpected argument: {unexpected}")),
        }
    }

    Ok(Command::Stats(StatsArgs { data_dir, format }))
}

/// Usage string for the daemon command.
const USAGE_DAEMON: &str = r#"actionqueue-cli daemon [OPTIONS]

Start the ActionQueue daemon bootstrap path.

Options:
    --data-dir <PATH>       Path to the data directory (default: ~/.actionqueue/data)
    --bind <ADDRESS>        HTTP API bind address (default: 127.0.0.1:8787)
    --metrics-bind <ADDR>   Metrics endpoint bind address (default: 127.0.0.1:9090)
    --enable-control        Enable control endpoints (cancel, pause, resume)
    --json                  Emit machine-readable JSON on stdout
    --help, -h              Show this help message
"#;

/// Usage string for the submit command.
const USAGE_SUBMIT: &str = r#"actionqueue-cli submit [OPTIONS]

Submit a task for execution through CLI control-plane semantics.

Options:
    --data-dir <PATH>      Path to the data directory (default: ~/.actionqueue/data)
    --task-id <ID>         Task identifier (required)
    --payload <PATH>       Path to payload file (optional)
    --content-type <MIME>  Payload content type (optional)
    --run-policy <POLICY>  Run policy: "once" or "repeat:N:SECONDS" (required)
    --constraints <JSON>   Constraints as JSON string or @path (optional)
    --metadata <JSON>      Metadata as JSON string or @path (optional)
    --json                 Emit machine-readable JSON on stdout
    --help, -h             Show this help message
"#;

/// Usage string for the stats command.
const USAGE_STATS: &str = r#"actionqueue-cli stats [OPTIONS]

Show deterministic system statistics from authoritative storage projection.

Options:
    --data-dir <PATH>      Path to the data directory (default: ~/.actionqueue/data)
    --format <FORMAT>      Output format: "text" or "json" (default: text)
    --json                 Shortcut for --format json
    --help, -h             Show this help message
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_daemon_with_json_and_control() {
        let args = ["daemon".to_string(), "--enable-control".to_string(), "--json".to_string()];
        let parsed = parse_args(&args).expect("daemon args should parse");

        match parsed {
            Command::Daemon(daemon) => {
                assert!(daemon.enable_control);
                assert!(daemon.json);
                assert_eq!(daemon.data_dir, None);
            }
            _ => panic!("expected daemon command"),
        }
    }

    #[test]
    fn parse_submit_requires_task_and_policy() {
        let args = ["submit".to_string(), "--run-policy".to_string(), "once".to_string()];
        let error = parse_args(&args).expect_err("submit parse should fail without task-id");
        assert!(error.contains("--task-id is required"));
    }

    #[test]
    fn parse_submit_full_surface() {
        let args = [
            "submit".to_string(),
            "--data-dir".to_string(),
            "/tmp/actionqueue".to_string(),
            "--task-id".to_string(),
            "123e4567-e89b-12d3-a456-426614174000".to_string(),
            "--payload".to_string(),
            "/tmp/payload.bin".to_string(),
            "--content-type".to_string(),
            "application/octet-stream".to_string(),
            "--run-policy".to_string(),
            "repeat:3:60".to_string(),
            "--constraints".to_string(),
            "{}".to_string(),
            "--metadata".to_string(),
            "{}".to_string(),
            "--json".to_string(),
        ];

        let parsed = parse_args(&args).expect("submit args should parse");
        match parsed {
            Command::Submit(submit) => {
                assert_eq!(submit.data_dir, Some(PathBuf::from("/tmp/actionqueue")));
                assert_eq!(submit.task_id, "123e4567-e89b-12d3-a456-426614174000");
                assert_eq!(submit.payload_path, Some(PathBuf::from("/tmp/payload.bin")));
                assert_eq!(submit.content_type.as_deref(), Some("application/octet-stream"));
                assert_eq!(submit.run_policy, "repeat:3:60");
                assert_eq!(submit.constraints.as_deref(), Some("{}"));
                assert_eq!(submit.metadata.as_deref(), Some("{}"));
                assert!(submit.json);
            }
            _ => panic!("expected submit command"),
        }
    }

    #[test]
    fn parse_stats_json_shortcut() {
        let args = ["stats".to_string(), "--json".to_string()];
        let parsed = parse_args(&args).expect("stats args should parse");
        match parsed {
            Command::Stats(stats) => assert_eq!(stats.format, StatsOutputFormat::Json),
            _ => panic!("expected stats command"),
        }
    }

    #[test]
    fn parse_stats_rejects_invalid_format() {
        let args = ["stats".to_string(), "--format".to_string(), "yaml".to_string()];
        let error = parse_args(&args).expect_err("invalid stats format must fail");
        assert!(error.contains("--format must be 'text' or 'json'"));
    }
}
