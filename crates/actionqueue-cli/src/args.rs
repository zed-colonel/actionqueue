//! CLI argument model and parsing.
//!
//! This module defines deterministic, side-effect-free argument parsing for the
//! ActionQueue CLI control-plane commands.

use std::path::PathBuf;

/// Root CLI command being invoked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Canonical API operation with explicit daemon or offline mode.
    Api(Vec<String>),
    /// Offline target persistence operations.
    Storage(StorageArgs),
    /// Start daemon runtime bootstrap flow.
    Daemon(DaemonArgs),
}

/// Arguments for the `daemon` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonArgs {
    /// Trusted host bearer identity configuration file.
    pub auth_file: Option<PathBuf>,
    /// Optional data directory override.
    pub data_dir: Option<PathBuf>,
    /// Optional HTTP API bind address (`IP:PORT`).
    pub bind: Option<String>,
    /// Optional metrics bind address (`IP:PORT`).
    pub metrics_bind: Option<String>,
    /// Explicit control-endpoint enablement.
    pub enable_control: bool,
    /// `namespace:kind` pairs reported as distinct signal metric labels.
    pub signal_metric_labels: Vec<String>,
    /// Emit JSON success payload on stdout when true.
    pub json: bool,
}

/// Parse command-line arguments into a typed command structure.
pub fn parse_args(args: &[String]) -> Result<Command, String> {
    if args.is_empty() {
        return Err(String::from("No command provided. Use a canonical actionqueue command."));
    }

    let command = &args[0];
    match command.as_str() {
        "store" => parse_storage(&args[1..]),
        "backup" | "restore" => parse_storage(args),
        "ensure-task" | "admission" | "task" | "signal" | "wait" | "run" | "checkpoint"
        | "attempt" | "trace" | "inspect" => Ok(Command::Api(args.to_vec())),
        "daemon" => parse_daemon(&args[1..]),

        _ => Err(format!("Unknown command: {command}. Use a canonical actionqueue command.")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageArgs {
    pub operation: String,
    pub data_dir: PathBuf,
    pub input: Option<PathBuf>,
    pub output: Option<PathBuf>,
    pub json: bool,
}
fn parse_storage(args: &[String]) -> Result<Command, String> {
    let operation = args
        .first()
        .filter(|s| ["inspect", "backup", "restore"].contains(&s.as_str()))
        .ok_or("storage requires inspect, backup, or restore")?
        .clone();
    let (mut data_dir, mut input, mut output) = (None, None, None);
    let mut json = false;
    let mut iter = args[1..].iter();
    while let Some(flag) = iter.next() {
        match flag.as_str() {
            "--data-dir" if data_dir.is_none() => {
                data_dir = Some(PathBuf::from(require_value(&mut iter, flag)?))
            }
            "--input" if input.is_none() && operation == "restore" => {
                input = Some(PathBuf::from(require_value(&mut iter, flag)?))
            }
            "--output" if output.is_none() && operation == "backup" => {
                output = Some(PathBuf::from(require_value(&mut iter, flag)?))
            }
            "--json" if !json => json = true,
            _ => return Err(format!("unexpected storage argument: {flag}")),
        }
    }
    if operation == "backup" && output.is_none() {
        return Err("backup requires --output".into());
    }
    if operation == "restore" && input.is_none() {
        return Err("restore requires --input".into());
    }
    Ok(Command::Storage(StorageArgs {
        operation,
        data_dir: data_dir.ok_or("storage requires --data-dir")?,
        input,
        output,
        json,
    }))
}

fn require_value(iter: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    iter.next().cloned().ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_daemon(args: &[String]) -> Result<Command, String> {
    let mut data_dir: Option<PathBuf> = None;
    let mut bind: Option<String> = None;
    let mut metrics_bind: Option<String> = None;
    let mut auth_file = None;
    let mut enable_control = false;
    let mut signal_metric_labels = Vec::new();
    let mut json = false;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(require_value(&mut iter, "--data-dir")?)),
            "--signal-metric-label" => {
                signal_metric_labels.push(require_value(&mut iter, "--signal-metric-label")?)
            }
            "--bind" => bind = Some(require_value(&mut iter, "--bind")?),
            "--metrics-bind" => metrics_bind = Some(require_value(&mut iter, "--metrics-bind")?),
            "--auth-file" => {
                auth_file = Some(PathBuf::from(require_value(&mut iter, "--auth-file")?))
            }
            "--enable-control" => enable_control = true,
            "--json" => json = true,
            "--help" | "-h" => return Err(USAGE_DAEMON.to_string()),
            unknown if unknown.starts_with('-') => {
                return Err(format!("Unknown option: {unknown}"))
            }
            unexpected => return Err(format!("Unexpected argument: {unexpected}")),
        }
    }

    Ok(Command::Daemon(DaemonArgs {
        auth_file,
        data_dir,
        bind,
        metrics_bind,
        enable_control,
        signal_metric_labels,
        json,
    }))
}

/// Usage string for the daemon command.
const USAGE_DAEMON: &str = r#"actionqueue daemon [OPTIONS]

Start the ActionQueue daemon bootstrap path.

Options:
    --data-dir <PATH>       Path to the data directory (default: ~/.actionqueue/data)
    --bind <ADDRESS>        HTTP API bind address (default: 127.0.0.1:8787)
    --metrics-bind <ADDR>   Metrics endpoint bind address (default: 127.0.0.1:9090)
    --enable-control        Enable authenticated control endpoints (requires --auth-file)
    --auth-file <PATH>      Trusted host bearer identity configuration
    --signal-metric-label <NAMESPACE:KIND>
                            Report this signal namespace/kind as its own metric
                            label pair (repeatable, at most 64); all other pairs
                            share the overflow bucket
    --json                  Emit machine-readable JSON on stdout
    --help, -h              Show this help message
"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_commands_only() {
        for old in ["submit", "stats", "storage"] {
            assert!(parse_args(&[old.into()]).is_err());
        }
        for command in ["ensure-task", "trace", "inspect"] {
            assert!(matches!(parse_args(&[command.into()]), Ok(Command::Api(_))));
        }
    }
}
