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

    let hook = args
        .auth_file
        .as_ref()
        .map(|path| {
            let bytes = std::fs::read(path).map_err(|_| {
                CliError::validation("host_auth_invalid", "unable to read host authentication file")
            })?;
            actionqueue_daemon::http::auth::bearer_authenticator(&bytes).map_err(|_| {
                CliError::validation(
                    "host_auth_invalid",
                    "invalid host authentication configuration",
                )
            })
        })
        .transpose()?;
    if config.enable_control && hook.is_none() {
        return Err(CliError::validation(
            "host_auth_required",
            "--enable-control requires --auth-file",
        ));
    }
    let state = actionqueue_daemon::bootstrap::bootstrap_with_authenticator(config, hook).map_err(
        |error| {
            CliError::runtime(
                "daemon_bootstrap_failed",
                format!("daemon bootstrap failed: {error}"),
            )
        },
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| CliError::runtime("runtime_unavailable", "unable to start runtime"))?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(state.config().bind_address)
            .await
            .map_err(|_| CliError::runtime("bind_failed", "unable to bind listener"))?;
        let address = listener
            .local_addr()
            .map_err(|_| CliError::runtime("bind_failed", "listener unavailable"))?;
        println!(
            "{}",
            json!({"command":"daemon", "bind_address":address.to_string(), "ready":true})
        );
        // Bootstrap/store ownership is retained until graceful shutdown completes.
        let router = actionqueue_daemon::http::build_router(state.router_state().clone());
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                #[cfg(unix)]
                {
                    if let Ok(mut term) =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    {
                        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
                    } else {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                }
            })
            .await
            .map_err(|_| CliError::runtime("serve_failed", "HTTP server stopped unexpectedly"))?;
        drop(state);
        Ok(CommandOutput::Json(json!({"status":"stopped"})))
    })
}

fn parse_socket_addr(raw: &str, field: &str) -> Result<SocketAddr, CliError> {
    raw.parse::<SocketAddr>().map_err(|error| {
        CliError::validation("invalid_socket_address", format!("invalid {field} '{raw}': {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_bootstrap_requires_valid_trusted_host_configuration() {
        let root = std::env::temp_dir()
            .join(format!("aq-cli-auth-{}", actionqueue_core::ids::TaskId::new()));
        std::fs::create_dir_all(&root).unwrap();
        let mut args = DaemonArgs {
            auth_file: None,
            data_dir: Some(root.join("store")),
            bind: None,
            metrics_bind: None,
            enable_control: true,
            json: true,
        };
        assert!(run(args.clone()).is_err());
        let path = root.join("host.json");
        args.auth_file = Some(path.clone());
        assert!(run(args.clone()).is_err());
        std::fs::write(&path, b"invalid").unwrap();
        assert!(run(args.clone()).is_err());
        let h = actionqueue_core::causal::ControlMutationContext::new(
            actionqueue_core::bounded::OpaqueRef::new("trusted-cli-host").unwrap(),
        );
        std::fs::write(&path, serde_json::to_vec(&json!([{"token":"0123456789abcdef0123456789abcdef", "actor_id":null,"scope":"SingleTenant","attribution":h}])).unwrap()).unwrap();
        // Valid server startup and shutdown are covered by black-box process tests.
        assert!(actionqueue_daemon::http::auth::bearer_authenticator(
            &std::fs::read(path).unwrap()
        )
        .is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}
