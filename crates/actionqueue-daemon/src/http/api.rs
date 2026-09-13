//! V2 adapters for the shared runtime service. Blocking storage work stays off executor threads.
use actionqueue_core::{control::HostControlContext, ids::*, mutation::CancelTarget};
use actionqueue_runtime::{
    control::{ControlOperation, ControlOutcome, ServiceError},
    inspection::{InspectionError, Inspector, Query},
};
use axum::{
    extract::{rejection::JsonRejection, Path, Query as Params, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;

use super::RouterState;

fn error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(serde_json::json!({"error_code":code}))).into_response()
}
fn inspection_error(e: InspectionError) -> Response {
    let status = match e {
        InspectionError::Unauthorized => StatusCode::FORBIDDEN,
        InspectionError::NotFound => StatusCode::NOT_FOUND,
        InspectionError::InvalidQuery => StatusCode::BAD_REQUEST,
        InspectionError::StaleCursor => StatusCode::CONFLICT,
        InspectionError::TooLarge => StatusCode::SERVICE_UNAVAILABLE,
    };
    error(status, e.code())
}
pub(crate) fn service_response(result: Result<ControlOutcome, ServiceError>) -> Response {
    match result {
        Ok(ControlOutcome::Task(outcome)) => {
            (if outcome.is_created() { StatusCode::CREATED } else { StatusCode::OK }, Json(outcome))
                .into_response()
        }
        Ok(ControlOutcome::Signal(outcome)) => (
            if matches!(
                outcome,
                actionqueue_core::continuation::AdmitSignalOutcome::Admitted { .. }
            ) {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(outcome),
        )
            .into_response(),
        Ok(ControlOutcome::Mutation(_)) => {
            Json(serde_json::json!({"status":"applied"})).into_response()
        }
        Err(e) => {
            let code = e.code();
            let status = match code {
                "conflict" => StatusCode::CONFLICT,
                "invalid_request" => StatusCode::UNPROCESSABLE_ENTITY,
                "forbidden" => StatusCode::FORBIDDEN,
                "not_found" => StatusCode::NOT_FOUND,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            };
            if let ServiceError::Signal(
                actionqueue_runtime::signals::SignalAdmissionError::Matching { outcome, .. },
            ) = e
            {
                return (status, Json(serde_json::json!({"error_code":code,"committed":outcome,"recovery_required":true}))).into_response();
            }
            error(status, code)
        }
    }
}
async fn mutate(
    state: RouterState,
    host: HostControlContext,
    operation: ControlOperation,
) -> Response {
    if !state.router_config.control_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !state.operational_failed.load(std::sync::atomic::Ordering::Acquire) {
        if let ControlOperation::AdmitTask(q) = &operation {
            if super::admission_throttle::throttled(&state, &host, q) {
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    [(axum::http::header::RETRY_AFTER, "30")],
                    Json(serde_json::json!({"error_code":"admission_conflict_throttled"})),
                )
                    .into_response();
            }
        }
    }
    let permit = match state.authority_lane.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "backpressure"),
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        if state.operational_failed.load(std::sync::atomic::Ordering::Acquire) {
            return error(StatusCode::SERVICE_UNAVAILABLE, "recovery_required");
        }
        let Some(authority) = &state.control_authority else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable");
        };
        let Ok(mut a) = authority.lock() else {
            state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
            return error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable");
        };
        let result = actionqueue_runtime::control::execute_control(
            &mut a,
            &host,
            operation,
            state.clock.as_ref(),
        );
        // Publish every durable prefix, even if later matching failed.
        if super::sync_projection(&state, &a).is_err()
            || a.recovery_required()
            || matches!(
                result,
                Err(ServiceError::Signal(
                    actionqueue_runtime::signals::SignalAdmissionError::Matching { .. }
                ))
            )
        {
            state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
        }
        service_response(result)
    })
    .await
    .unwrap_or_else(|_| error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"))
}
fn bad_json(e: JsonRejection) -> Response {
    match e {
        JsonRejection::JsonDataError(_) => {
            error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_request")
        }
        e if e.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            error(StatusCode::PAYLOAD_TOO_LARGE, "body_too_large")
        }
        _ => error(StatusCode::BAD_REQUEST, "invalid_json"),
    }
}

async fn ensure(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    body: Result<Json<actionqueue_core::admission::EnsureTaskRequest>, JsonRejection>,
) -> Response {
    match body {
        Ok(Json(q)) => mutate(s, h, ControlOperation::AdmitTask(q)).await,
        Err(e) => bad_json(e),
    }
}
async fn signal_admit(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    body: Result<Json<actionqueue_core::continuation::AdmitSignalRequest>, JsonRejection>,
) -> Response {
    match body {
        Ok(Json(q)) => mutate(s, h, ControlOperation::AdmitSignal(q)).await,
        Err(e) => bad_json(e),
    }
}
async fn inspect(
    s: RouterState,
    h: HostControlContext,
    q: Query,
    operation: impl FnOnce(&Inspector<'_>) -> Result<serde_json::Value, InspectionError>
        + Send
        + 'static,
) -> Response {
    let permit = match s.authority_lane.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "backpressure"),
    };
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let p = match super::read_inspection_projection(&s) {
            Ok(p) => p,
            Err(e) => return *e,
        };
        let platform = s
            .store_session
            .as_ref()
            .is_some_and(|s| s.manifest().features.iter().any(|f| f == "platform"));
        let result = Inspector::new(
            &p,
            &h,
            platform,
            s.disclosure_policy,
            q.display_references,
            s.clock.now(),
        )
        .and_then(|i| operation(&i));
        match result {
            Ok(v) => Json(v).into_response(),
            Err(e) => inspection_error(e),
        }
    })
    .await
    .unwrap_or_else(|_| error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"))
}
fn value(v: impl serde::Serialize) -> Result<serde_json::Value, InspectionError> {
    let bytes = serde_json::to_vec(&v).map_err(|_| InspectionError::TooLarge)?;
    if bytes.len() > 2 * 1024 * 1024 {
        return Err(InspectionError::TooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|_| InspectionError::TooLarge)
}
macro_rules! get {
    ($name:ident, $id:ty, $method:ident) => {
        async fn $name(
            State(s): State<RouterState>,
            Extension(h): Extension<HostControlContext>,
            Path(id): Path<$id>,
            Params(q): Params<Query>,
        ) -> Response {
            inspect(s, h, q, move |i| value(i.$method(id)?)).await
        }
    };
}
get!(task, TaskId, get_task);
get!(run, RunId, get_run);
get!(wait, WaitId, get_wait);
get!(checkpoint, CheckpointId, get_checkpoint);
async fn signal(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<String>,
    Params(q): Params<Query>,
) -> Response {
    let Ok(id) = SignalId::new(id) else {
        return error(StatusCode::BAD_REQUEST, "invalid_id");
    };
    inspect(s, h, q, move |i| value(i.get_signal(&id)?)).await
}
async fn attempt(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path((run, attempt)): Path<(RunId, AttemptId)>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q, move |i| value(i.get_attempt(run, attempt)?)).await
}
async fn signal_waits(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<String>,
    Params(q): Params<Query>,
) -> Response {
    let Ok(id) = SignalId::new(id) else {
        return error(StatusCode::BAD_REQUEST, "invalid_id");
    };
    inspect(s, h, q.clone(), move |i| value(i.linked_waits(&id, &q)?)).await
}
async fn checkpoint_consumers(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<CheckpointId>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q.clone(), move |i| value(i.checkpoint_consumers(id, &q)?)).await
}
async fn attempts(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(run): Path<RunId>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q.clone(), move |i| value(i.list_attempts(run, &q)?)).await
}
async fn task_controls(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<TaskId>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q.clone(), move |i| value(i.task_controls(id, &q)?)).await
}
async fn run_controls(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<RunId>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q.clone(), move |i| value(i.run_controls(id, &q)?)).await
}
async fn history(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(run): Path<RunId>,
    Params(q): Params<Query>,
) -> Response {
    inspect(s, h, q.clone(), move |i| value(i.run_history(run, &q)?)).await
}
#[derive(Deserialize)]
struct AdmissionQuery {
    key: AdmissionKey,
}
async fn admission(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Params(q): Params<AdmissionQuery>,
) -> Response {
    inspect(s, h, Query::default(), move |i| value(i.get_admission(&q.key)?)).await
}
macro_rules! list {
    ($name:ident, $method:ident) => {
        async fn $name(
            State(s): State<RouterState>,
            Extension(h): Extension<HostControlContext>,
            Params(q): Params<Query>,
        ) -> Response {
            inspect(s, h, q.clone(), move |i| value(i.$method(&q)?)).await
        }
    };
}
list!(tasks, list_tasks);
list!(runs, list_runs);
list!(waits, list_waits);
list!(signals, list_signals);
list!(trace_query, trace);
async fn trace(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(id): Path<String>,
    Params(mut q): Params<Query>,
) -> Response {
    if q.trace_id.is_some() || q.correlation_id.is_some() || q.origin_ref.is_some() {
        return error(StatusCode::BAD_REQUEST, "invalid_query");
    }
    q.trace_id = Some(id);
    inspect(s, h, q.clone(), move |i| value(i.trace(&q)?)).await
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitControl {
    run_id: RunId,
}
async fn wait_control(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(action): Path<String>,
    body: Result<Json<WaitControl>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => return bad_json(e),
    };
    let Some((id, verb)) = action.rsplit_once(':') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(wait_id) = id.parse() else {
        return error(StatusCode::BAD_REQUEST, "invalid_id");
    };
    let op = match verb {
        "cancel" => ControlOperation::CancelWait { run_id: body.run_id, wait_id },
        "resolve" => ControlOperation::ResolveWait { run_id: body.run_id, wait_id },
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    mutate(s, h, op).await
}
async fn task_cancel(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(action): Path<String>,
) -> Response {
    let Some(id) = action.strip_suffix(":cancel") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(id) = id.parse() else {
        return error(StatusCode::BAD_REQUEST, "invalid_id");
    };
    mutate(s, h, ControlOperation::Cancel(CancelTarget::Task(id))).await
}
async fn run_cancel(
    State(s): State<RouterState>,
    Extension(h): Extension<HostControlContext>,
    Path(action): Path<String>,
) -> Response {
    let Some(id) = action.strip_suffix(":cancel") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(id) = id.parse() else {
        return error(StatusCode::BAD_REQUEST, "invalid_id");
    };
    mutate(s, h, ControlOperation::Cancel(CancelTarget::Run(id))).await
}
pub fn reads() -> axum::Router<RouterState> {
    use axum::routing::get;
    axum::Router::new()
        .route("/api/v2/admissions", get(admission))
        .route("/api/v2/tasks", get(tasks))
        .route("/api/v2/tasks/:id/controls", get(task_controls))
        .route("/api/v2/runs/:id/controls", get(run_controls))
        .route("/api/v2/tasks/:id", get(task))
        .route("/api/v2/runs", get(runs))
        .route("/api/v2/runs/:id", get(run))
        .route("/api/v2/runs/:id/continuation", get(run))
        .route("/api/v2/runs/:id/attempts", get(attempts))
        .route("/api/v2/runs/:id/history", get(history))
        .route("/api/v2/runs/:run/attempts/:attempt", get(attempt))
        .route("/api/v2/signals", get(signals))
        .route("/api/v2/signals/:id", get(signal))
        .route("/api/v2/waits", get(waits))
        .route("/api/v2/waits/:id", get(wait))
        .route("/api/v2/checkpoints/:id", get(checkpoint))
        .route("/api/v2/checkpoints/:id/consumers", get(checkpoint_consumers))
        .route("/api/v2/signals/:id/waits", get(signal_waits))
        .route("/api/v2/traces/:id", get(trace))
        .route("/api/v2/inspect", get(trace_query))
}
pub fn writes() -> axum::Router<RouterState> {
    use axum::routing::post;
    axum::Router::new()
        .route("/api/v2/admissions:ensure", post(ensure))
        .route("/api/v2/signals", post(signal_admit))
        .route("/api/v2/tasks/:id", post(task_cancel))
        .route("/api/v2/runs/:id", post(run_cancel))
        .route("/api/v2/waits/:id", post(wait_control))
}

/// Framework extractor failures also use redacted error bodies; rejected values are never echoed.
pub async fn sanitize_errors(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let response = next.run(request).await;
    if response.status().is_client_error()
        && response.headers().get("content-type").is_none_or(|v| v != "application/json")
    {
        let status = response.status();
        let code = match status.as_u16() {
            400 => "invalid_request",
            401 => "unauthenticated",
            403 => "forbidden",
            404 => "not_found",
            413 => "body_too_large",
            _ => "request_rejected",
        };
        error(status, code)
    } else {
        response
    }
}

/// The feature adapters also execute their synchronous WAL work off runtime threads.
pub async fn blocking_adapter(
    State(state): State<RouterState>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if state.operational_failed.load(std::sync::atomic::Ordering::Acquire) {
        return error(StatusCode::SERVICE_UNAVAILABLE, "recovery_required");
    }
    let permit = match state.authority_lane.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => return error(StatusCode::SERVICE_UNAVAILABLE, "backpressure"),
    };
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let response = runtime.block_on(next.run(request));
        if let Some(a) = &state.control_authority {
            match a.lock() {
                Ok(a) => {
                    if a.recovery_required() || super::sync_projection(&state, &a).is_err() {
                        state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
                Err(_) => {
                    state.operational_failed.store(true, std::sync::atomic::Ordering::Release)
                }
            }
        }
        response
    })
    .await
    .unwrap_or_else(|_| error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"))
}
