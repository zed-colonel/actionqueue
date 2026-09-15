//! Regressions for paginated CLI transport and literal IPv6 loopback.
use std::{
    process::Command,
    sync::{Arc, RwLock},
};

use actionqueue_core::{
    bounded::OpaqueRef,
    causal::ControlMutationContext,
    control::*,
    ids::*,
    mutation::AttemptResultKind,
    run::{run_instance::RunInstance, RunState},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_daemon::{
    bootstrap::{ReadyStatus, RouterConfig},
    http::*,
};
use actionqueue_runtime::inspection::{Inspector, Query};
use actionqueue_storage::{
    recovery::{bootstrap::RecoveryObservations, reducer::ReplayReducer},
    wal::{
        event::{WalEvent, WalEventType as E},
        WalAppendTelemetry,
    },
};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use tower::ServiceExt;
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(OpaqueRef::new("test").unwrap()),
    }
}
fn history() -> (ReplayReducer, RunId) {
    let mut p = ReplayReducer::new();
    let mut apply = |event| {
        let event = WalEvent::new(p.latest_sequence() + 1, event);
        p.apply(&event).unwrap_or_else(|e| panic!("{event:?}: {e:?}"));
    };
    let task = TaskId::new();
    let run = RunId::new();
    apply(E::TaskCreated {
        task_spec: TaskSpec::new(
            task,
            TaskPayload::new(vec![]),
            RunPolicy::Once,
            TaskConstraints::new(200, None, None).unwrap(),
            TaskMetadata::default(),
        )
        .unwrap(),
        timestamp: 1,
    });
    apply(E::RunCreated {
        run_instance: RunInstance::new_scheduled_with_id(run, task, 1, 1).unwrap(),
    });
    let mut state = RunState::Scheduled;
    for n in 0..110 {
        for next in [RunState::Ready, RunState::Leased, RunState::Running] {
            apply(E::RunStateChanged {
                run_id: run,
                previous_state: state,
                new_state: next,
                timestamp: n + 2,
            });
            state = next;
        }
        let attempt = AttemptId::new();
        apply(E::AttemptStarted { run_id: run, attempt_id: attempt, timestamp: n + 2 });
        apply(E::AttemptFinished {
            run_id: run,
            attempt_id: attempt,
            result: AttemptResultKind::Failure,
            error: Some("retry".into()),
            output: None,
            timestamp: n + 2,
        });
        apply(E::RunStateChanged {
            run_id: run,
            previous_state: state,
            new_state: RunState::RetryWait,
            timestamp: n + 2,
        });
        state = RunState::RetryWait;
    }
    (p, run)
}
fn router(p: ReplayReducer) -> axum::Router {
    build_router(Arc::new(
        RouterStateInner::new(
            RouterConfig { control_enabled: false, metrics_enabled: false },
            Arc::new(RwLock::new(p)),
            RouterObservability {
                metrics: Arc::new(
                    actionqueue_daemon::metrics::registry::MetricsRegistry::new(None).unwrap(),
                ),
                wal_append_telemetry: WalAppendTelemetry::new(),
                clock: Arc::new(actionqueue_daemon::time::clock::MockClock::new(200)),
                recovery_observations: RecoveryObservations::zero(),
            },
            ReadyStatus::ready(),
        )
        .with_host_authenticator(Arc::new(|_, _| Ok(host())))
        .without_background_maintenance(),
    ))
}
async fn cli(args: Vec<String>, address: std::net::SocketAddr) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_actionqueue"))
            .args(args)
            .args(["--daemon", &format!("http://{address}"), "--json"])
            .env("ACTIONQUEUE_TOKEN", "abcdefghijklmnopqrstuvwxyz0123456789")
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn complete_history_and_attempt_pages_match_embedded_http_and_cli() {
    let (p, run) = history();
    let h = host();
    let inspector = Inspector::new(&p, &h, false, Default::default(), false, 200).unwrap();
    let router = router(p.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_router = router.clone();
    let server = tokio::spawn(async move { axum::serve(listener, server_router).await.unwrap() });
    for (section, total) in [("history", 441), ("attempts", 110)] {
        let mut query = Query::default();
        let mut count = 0;
        loop {
            let expected = if section == "history" {
                serde_json::to_value(inspector.run_history(run, &query).unwrap()).unwrap()
            } else {
                serde_json::to_value(inspector.list_attempts(run, &query).unwrap()).unwrap()
            };
            let mut args = vec!["run".into(), section.into(), run.to_string()];
            let mut path = format!("/api/v2/runs/{run}/{section}");
            if let Some(cursor) = &query.cursor {
                args.extend(["--cursor".into(), cursor.clone()]);
                path.push_str(&format!("?cursor={cursor}"));
            }
            let http = router
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(http.status(), 200);
            let http: serde_json::Value =
                serde_json::from_slice(&http.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(http, expected);
            let output = cli(args, address).await;
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
            let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(actual, expected);
            count += actual["items"].as_array().unwrap().len();
            query.cursor = actual["next_cursor"].as_str().map(str::to_owned);
            if query.cursor.is_none() {
                break;
            }
        }
        assert_eq!(count, total);
    }
    server.abort();
}
#[tokio::test]
async fn ipv6_literal_connects_and_preserves_host_header() {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().fallback(move |headers: axum::http::HeaderMap| async move {
        assert_eq!(headers["host"], address.to_string());
        axum::Json(serde_json::json!({"ipv6":true}))
    });
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let output = cli(vec!["run".into(), "inspect".into(), RunId::new().to_string()], address).await;
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"ipv6":true})
    );
    server.abort();
}
#[test]
fn pagination_flags_on_nonpaged_operations_are_rejected_before_transport() {
    for command in [
        vec!["run", "inspect", "id"],
        vec!["run", "continuation", "id"],
        vec!["task", "inspect", "id"],
        vec!["run", "cancel", "id"],
        vec!["wait", "inspect", "id"],
    ] {
        for flag in ["--limit", "--cursor", "--edge-cursor"] {
            let output = Command::new(env!("CARGO_BIN_EXE_actionqueue"))
                .args(&command)
                .args([flag, "1"])
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(3));
            assert!(String::from_utf8_lossy(&output.stderr).contains("invalid_request"));
        }
    }
    for section in ["history", "attempts"] {
        let output = Command::new(env!("CARGO_BIN_EXE_actionqueue"))
            .args(["run", section, "id", "--edge-cursor", "1"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3));
    }
}
