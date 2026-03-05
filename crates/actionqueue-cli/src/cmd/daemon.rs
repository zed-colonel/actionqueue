//! Daemon command execution path.

use std::net::SocketAddr;

use serde_json::json;

use crate::args::DaemonArgs;
use crate::cmd::{resolve_data_dir, CliError, CommandOutput};

/// Executes daemon bootstrap command flow.
pub fn run(args: DaemonArgs) -> Result<CommandOutput, CliError> {
    let mut config = actionqueue_daemon::config::DaemonConfig::default();
    let data_dir = resolve_data_dir(args.data_dir.as_deref());
    config.data_dir = data_dir.clone();

    if let Some(bind) = args.bind.as_deref() {
        config.bind_address = parse_socket_addr(bind, "bind_address")?;
    }
    if let Some(metrics_bind) = args.metrics_bind.as_deref() {
        config.metrics_bind = Some(parse_socket_addr(metrics_bind, "metrics_bind")?);
    }
    config.enable_control = args.enable_control;

    config.validate().map_err(|error| {
        CliError::validation(
            "daemon_config_invalid",
            format!("daemon configuration rejected: {error}"),
        )
    })?;

    let state = actionqueue_daemon::bootstrap::bootstrap(config).map_err(|error| {
        CliError::runtime("daemon_bootstrap_failed", format!("daemon bootstrap failed: {error}"))
    })?;

    let ready = state.ready_status();
    let metrics_bind = state.config().metrics_bind.map(|addr| addr.to_string());

    if args.json {
        return Ok(CommandOutput::Json(json!({
            "command": "daemon",
            "data_dir": data_dir.display().to_string(),
            "bind_address": state.config().bind_address.to_string(),
            "metrics_bind": metrics_bind,
            "control_enabled": state.config().enable_control,
            "ready": ready.is_ready(),
            "ready_reason": ready.reason(),
        })));
    }

    let lines = [
        "command=daemon".to_string(),
        format!("data_dir={}", data_dir.display()),
        format!("bind_address={}", state.config().bind_address),
        format!("metrics_bind={}", metrics_bind.as_deref().unwrap_or("disabled")),
        format!("control_enabled={}", state.config().enable_control),
        format!("ready={}", ready.is_ready()),
        format!("ready_reason={}", ready.reason()),
    ];
    Ok(CommandOutput::Text(lines.join("\n")))
}

fn parse_socket_addr(raw: &str, field: &str) -> Result<SocketAddr, CliError> {
    raw.parse::<SocketAddr>().map_err(|error| {
        CliError::validation("invalid_socket_address", format!("invalid {field} '{raw}': {error}"))
    })
}
