//! Canonical API client and explicit offline adapter. Both render the daemon DTOs unchanged.
use axum::{
    body::Body,
    http::{Request, Uri},
};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::{CliError, CommandOutput};
fn invalid() -> CliError {
    CliError::validation("invalid_request", "invalid command arguments")
}
fn unavailable() -> CliError {
    CliError::connectivity("storage_unavailable", "service unavailable")
}
/// Encode an opaque value as exactly one path segment or query value.
fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
pub fn run(args: Vec<String>) -> Result<CommandOutput, CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| unavailable())?;
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(30), execute(args))
            .await
            .map_err(|_| unavailable())?
    })
}
async fn execute(args: Vec<String>) -> Result<CommandOutput, CliError> {
    let mut words = Vec::new();
    let mut options = std::collections::BTreeMap::new();
    let mut offline = false;
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--offline" if !offline => offline = true,
            "--json" if !json => json = true,
            "--daemon" | "--data-dir" | "--token-file" | "--file" | "--key" | "--run"
            | "--correlation" | "--origin-ref" | "--limit" | "--cursor" | "--edge-cursor" => {
                let value = iter.next().ok_or_else(invalid)?;
                if options.insert(arg.as_str(), value.as_str()).is_some() {
                    return Err(invalid());
                }
            }
            s if s.starts_with('-') => return Err(invalid()),
            _ => words.push(arg.as_str()),
        }
    }
    let opt = |key| options.get(key).copied().ok_or_else(invalid);
    let mut method = "GET";
    let mut body = Vec::new();
    let mut path = match words.as_slice() {
        ["ensure-task"] => {
            method = "POST";
            "/api/v2/admissions:ensure".into()
        }
        ["signal", "admit"] => {
            method = "POST";
            "/api/v2/signals".into()
        }
        ["admission", "inspect"] => format!("/api/v2/admissions?key={}", encode(opt("--key")?)),
        [kind @ ("task" | "run" | "signal" | "wait" | "checkpoint"), "inspect", id] => {
            format!("/api/v2/{kind}s/{}", encode(id))
        }
        ["wait", verb @ ("cancel" | "resolve"), id] => {
            method = "POST";
            let run: actionqueue_core::ids::RunId = opt("--run")?.parse().map_err(|_| invalid())?;
            body = serde_json::to_vec(&serde_json::json!({"run_id":run})).map_err(|_| invalid())?;
            format!("/api/v2/waits/{}:{verb}", encode(id))
        }
        [kind @ ("task" | "run"), "cancel", id] => {
            method = "POST";
            format!("/api/v2/{kind}s/{}:cancel", encode(id))
        }
        ["attempt", "inspect", id] => {
            format!("/api/v2/runs/{}/attempts/{}", encode(opt("--run")?), encode(id))
        }
        ["run", section @ ("history" | "attempts"), id] => {
            format!("/api/v2/runs/{}/{section}", encode(id))
        }
        ["run", "continuation", id] => format!("/api/v2/runs/{}/continuation", encode(id)),
        ["trace", id] => format!("/api/v2/traces/{}", encode(id)),
        ["trace"] => format!("/api/v2/inspect?correlation_id={}", encode(opt("--correlation")?)),
        ["inspect"] => format!("/api/v2/inspect?origin_ref={}", encode(opt("--origin-ref")?)),
        _ => return Err(invalid()),
    };
    if matches!(words.as_slice(), ["ensure-task"] | ["signal", "admit"]) {
        let file = std::fs::File::open(opt("--file")?).map_err(|_| invalid())?;
        use std::io::Read;
        file.take(2 * 1024 * 1024 + 1).read_to_end(&mut body).map_err(|_| invalid())?;
        if body.len() > 2 * 1024 * 1024 {
            return Err(invalid());
        }
        // Validate through the same typed request constructors before transport.
        if words[0] == "ensure-task" {
            let _: actionqueue_core::admission::EnsureTaskRequest =
                serde_json::from_slice(&body).map_err(|_| invalid())?;
        } else {
            let _: actionqueue_core::continuation::AdmitSignalRequest =
                serde_json::from_slice(&body).map_err(|_| invalid())?;
        }
    }
    let trace = matches!(words.as_slice(), ["trace", _] | ["trace"] | ["inspect"]);
    let paginated = trace || matches!(words.as_slice(), ["run", "history" | "attempts", _]);
    if (!paginated && (options.contains_key("--limit") || options.contains_key("--cursor")))
        || (!trace && options.contains_key("--edge-cursor"))
    {
        return Err(invalid());
    }
    for (flag, name) in
        [("--limit", "limit"), ("--cursor", "cursor"), ("--edge-cursor", "edge_cursor")]
    {
        if let Some(v) = options.get(flag) {
            path.push(if path.contains('?') { '&' } else { '?' });
            path.push_str(&format!("{name}={}", encode(v)));
        }
    }
    let response = if offline {
        if options.contains_key("--daemon") || options.contains_key("--token-file") {
            return Err(invalid());
        }
        let config = actionqueue_daemon::config::DaemonConfig {
            data_dir: opt("--data-dir")?.into(),
            enable_control: true,
            ..Default::default()
        };
        let host = actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("local-cli").expect("constant"),
            ),
        };
        let hook: actionqueue_daemon::http::auth::HostAuthenticator =
            std::sync::Arc::new(move |_, _| Ok(host.clone()));
        let recovered = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(
            &config.data_dir,
        )
        .map_err(|_| unavailable())?;
        let authority = actionqueue_storage::mutation::StorageMutationAuthority::new(
            recovered.wal_writer,
            recovered.projection.clone(),
        );
        let state = actionqueue_daemon::http::RouterStateInner::with_control_authority(
            actionqueue_daemon::bootstrap::RouterConfig {
                control_enabled: true,
                metrics_enabled: false,
            },
            std::sync::Arc::new(std::sync::RwLock::new(recovered.projection)),
            actionqueue_daemon::http::RouterObservability {
                metrics: std::sync::Arc::new(
                    actionqueue_daemon::metrics::registry::MetricsRegistry::new(None)
                        .map_err(|_| unavailable())?,
                ),
                wal_append_telemetry: recovered.wal_append_telemetry,
                clock: std::sync::Arc::new(actionqueue_daemon::time::clock::SystemClock),
                recovery_observations: recovered.recovery_observations,
            },
            std::sync::Arc::new(std::sync::Mutex::new(authority)),
            actionqueue_daemon::bootstrap::ReadyStatus::ready(),
        )
        .with_host_authenticator(hook)
        .without_background_maintenance();
        let router = actionqueue_daemon::http::build_router(std::sync::Arc::new(state));
        let request = Request::builder()
            .method(method)
            .uri(&path)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .map_err(|_| invalid())?;
        router.oneshot(request).await.map_err(|_| unavailable())?
    } else {
        if options.contains_key("--data-dir") {
            return Err(invalid());
        }
        let base: Uri = opt("--daemon")?.parse().map_err(|_| invalid())?;
        // This transport is deliberately local HTTP. TLS termination is a host concern.
        if base.scheme_str() != Some("http")
            || !matches!(base.host(), Some("127.0.0.1" | "localhost" | "[::1]"))
        {
            return Err(invalid());
        }
        let token = if let Some(file) = options.get("--token-file") {
            std::fs::read_to_string(file).map_err(|_| invalid())?
        } else {
            std::env::var("ACTIONQUEUE_TOKEN").map_err(|_| invalid())?
        };
        let token = token.trim();
        if token.len() < 32 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(invalid());
        }
        let host = base.host().ok_or_else(invalid)?;
        let socket_host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
        let stream = tokio::net::TcpStream::connect((socket_host, base.port_u16().unwrap_or(80)))
            .await
            .map_err(|_| unavailable())?;
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(|_| unavailable())?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", base.authority().ok_or_else(invalid)?.as_str())
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(body))
            .map_err(|_| invalid())?;
        let response = sender.send_request(request).await.map_err(|_| unavailable())?;
        response.map(Body::new)
    };
    let status = response.status();
    let bytes = http_body_util::Limited::new(response.into_body(), 2 * 1024 * 1024)
        .collect()
        .await
        .map_err(|_| unavailable())?
        .to_bytes();
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
    if !status.is_success() {
        let code = match value.get("error_code").and_then(|v| v.as_str()) {
            Some("conflict") => "conflict",
            Some("already_terminal") => "already_terminal",
            Some("stale_cursor") => "stale_cursor",
            Some("not_found") => "not_found",
            Some("forbidden") => "forbidden",
            Some("unauthenticated") => "unauthenticated",
            Some("storage_unavailable") => "storage_unavailable",
            Some("backpressure") => "backpressure",
            Some("recovery_required") => "recovery_required",
            Some("invalid_query") => "invalid_query",
            Some("invalid_json") => "invalid_json",
            Some("response_too_large") => "response_too_large",
            Some("body_too_large") => "body_too_large",
            Some("signal_committed_recovery_required") => "signal_committed_recovery_required",
            _ => "invalid_request",
        };
        let mut error = if status.as_u16() == 503 {
            CliError::connectivity(code, "service requires recovery or retry")
        } else if status.as_u16() == 409 {
            CliError::runtime(code, "conflicting request")
        } else {
            CliError::validation(code, "request rejected")
        };
        if code == "signal_committed_recovery_required" {
            let outcome =
                serde_json::from_value(value["committed"].clone()).map_err(|_| unavailable())?;
            error = error.with_committed_signal(outcome);
        }
        return Err(error);
    }
    if json {
        Ok(CommandOutput::Json(value))
    } else {
        Ok(CommandOutput::Text(serde_json::to_string_pretty(&value).map_err(|_| unavailable())?))
    }
}
