//! HTTP /api/v2/actors endpoints for remote actor registration protocol.
//!
//! These endpoints are only registered when the `actor` feature is enabled.
//! They submit actor mutation commands through the WAL-backed control authority.

use actionqueue_core::actor::{ActorRegistration, ExecutorTraits};
use actionqueue_core::ids::ActorId;
use actionqueue_core::mutation::{
    ActorDeregisterCommand, ActorHeartbeatCommand, ActorRegisterCommand, MutationCommand,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::Json;

use crate::http::RouterState;

/// Request body for actor registration.
#[derive(serde::Deserialize)]
pub struct RegisterActorRequest {
    pub protocol_version: u32,
    pub contract_revision: String,
    pub actor_id: ActorId,
    pub identity: String,
    pub executor_traits: Vec<String>,
    pub heartbeat_interval_secs: u64,
    #[serde(default)]
    pub department: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<actionqueue_core::ids::TenantId>,
}

/// Registers routes for the actor API.
pub fn register_routes(router: axum::Router<RouterState>) -> axum::Router<RouterState> {
    router
        .route("/api/v2/actors/register", post(register_actor))
        .route("/api/v2/actors/:actor_id/heartbeat", post(actor_heartbeat))
        .route("/api/v2/actors/:actor_id", delete(deregister_actor))
        .route("/api/v2/actors/:actor_id/claimable", get(claimable_runs))
        .route("/api/v2/actors/:actor_id/claim", post(claim_run))
        .route("/api/v2/actors/:actor_id/result", post(submit_result))
        .route("/api/v2/actors/:actor_id/renew", post(renew_lease))
}

async fn register_actor(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Json(body): Json<RegisterActorRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "control_authority_unavailable" })),
        )
            .into_response();
    };

    if !actionqueue_actor::protocol::supported(body.protocol_version, &body.contract_revision)
        || body.identity.is_empty()
        || body.heartbeat_interval_secs == 0
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let caps = match ExecutorTraits::new(body.executor_traits) {
        Ok(c) => c,
        Err(e) => return (
            StatusCode::BAD_REQUEST,
            Json(
                serde_json::json!({ "error": "invalid_executor_traits", "message": e.to_string() }),
            ),
        )
            .into_response(),
    };

    let mut reg =
        ActorRegistration::new(body.actor_id, body.identity, caps, body.heartbeat_interval_secs);
    if let Some(tenant_id) = body.tenant_id {
        reg = reg.with_tenant(tenant_id);
    }
    if let Some(dept_str) = body.department {
        if let Ok(dept) = actionqueue_core::ids::DepartmentId::new(dept_str) {
            reg = reg.with_department(dept);
        }
    }

    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "authority_poisoned" })),
            )
                .into_response()
        }
    };

    let seq = auth.projection().latest_sequence() + 1;
    let ts = state.clock.now();

    match crate::http::execute_host_mutation(
        &state,
        &mut auth,
        &host,
        MutationCommand::ActorRegister(ActorRegisterCommand::new(seq, reg, ts)),
    ) {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!({ "actor_id": body.actor_id })))
            .into_response(),
        Err(e) => super::api::service_response(Err(e)),
    }
}

async fn actor_heartbeat(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let seq = auth.projection().latest_sequence() + 1;
    let ts = state.clock.now();

    match crate::http::execute_host_mutation(
        &state,
        &mut auth,
        &host,
        MutationCommand::ActorHeartbeat(ActorHeartbeatCommand::new(seq, actor_id, ts)),
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => super::api::service_response(Err(e)),
    }
}

async fn deregister_actor(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let seq = auth.projection().latest_sequence() + 1;
    let ts = state.clock.now();

    match crate::http::execute_host_mutation(
        &state,
        &mut auth,
        &host,
        MutationCommand::ActorDeregister(ActorDeregisterCommand::new(seq, actor_id, ts)),
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => super::api::service_response(Err(e)),
    }
}

/// Actor-scoped remote operations share one shape: the authenticated principal
/// must own the path actor, and the remote scheduler settles before and after
/// the operation. The blocking adapter publishes the projection (with the
/// equal-revision tripwire) after every adapter route.
pub(super) fn actor_operation<T: serde::Serialize>(
    state: &RouterState,
    host: &actionqueue_core::control::HostControlContext,
    actor_id: ActorId,
    rejection: StatusCode,
    operation: impl FnOnce(
        &mut ControlAuthority,
        &actionqueue_core::control::HostControlContext,
        u64,
    ) -> Result<Option<T>, actionqueue_core::control::ControlError>,
) -> axum::response::Response {
    if host.actor_id != Some(actor_id) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(a) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Ok(mut a) = a.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if crate::http::maintenance::maintain_locked(state, &mut a).is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let value = match operation(&mut a, host, state.clock.now()) {
        Ok(value) => value,
        Err(error) => {
            let recovery_required = a.recovery_required();
            if recovery_required {
                state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
            }
            return operation_error_status(&error, rejection, recovery_required).into_response();
        }
    };
    if crate::http::maintenance::maintain_locked(state, &mut a).is_err() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    match value {
        Some(value) => Json(value).into_response(),
        None => StatusCode::OK.into_response(),
    }
}
fn operation_error_status(
    error: &actionqueue_core::control::ControlError,
    rejection: StatusCode,
    recovery_required: bool,
) -> StatusCode {
    use actionqueue_core::control::ControlError;
    if recovery_required {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match error {
        ControlError::Unauthorized | ControlError::Scope => StatusCode::FORBIDDEN,
        ControlError::NotFound => StatusCode::NOT_FOUND,
        ControlError::Mutation(_) => rejection,
    }
}

type ControlAuthority = actionqueue_storage::mutation::StorageMutationAuthority<
    actionqueue_storage::wal::InstrumentedWalWriter<
        actionqueue_storage::wal::fs_writer::WalFsWriter,
    >,
    actionqueue_storage::recovery::reducer::ReplayReducer,
>;

/// Lists eligible work in the authenticated actor's explicit namespace.
async fn claimable_runs(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
) -> axum::response::Response {
    actor_operation(&state, &host, actor_id, StatusCode::FORBIDDEN, |a, host, now| {
        actionqueue_runtime::remote::claimable(a, host, now)
            .map(|runs| Some(serde_json::json!({"runs":runs})))
    })
}
async fn claim_run(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
    Json(request): Json<actionqueue_actor::protocol::RemoteClaim>,
) -> axum::response::Response {
    let policy = state.remote_policy;
    actor_operation(&state, &host, actor_id, StatusCode::CONFLICT, |a, host, now| {
        actionqueue_runtime::remote::claim_with_policy(a, host, request, now, policy).map(Some)
    })
}
async fn submit_result(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
    Json(request): Json<actionqueue_actor::protocol::RemoteAttemptResult>,
) -> axum::response::Response {
    actor_operation(&state, &host, actor_id, StatusCode::CONFLICT, |a, host, now| {
        actionqueue_runtime::remote::submit_result(a, host, request, now).map(|()| None::<()>)
    })
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RenewRequest {
    run_id: actionqueue_core::ids::RunId,
    attempt_id: actionqueue_core::ids::AttemptId,
    lease_fence: actionqueue_core::mutation::LeaseFence,
    expiry: u64,
}
async fn renew_lease(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Path(actor_id): Path<ActorId>,
    Json(request): Json<RenewRequest>,
) -> axum::response::Response {
    actor_operation(&state, &host, actor_id, StatusCode::CONFLICT, |a, host, now| {
        actionqueue_runtime::remote::renew(
            a,
            host,
            request.run_id,
            request.attempt_id,
            request.lease_fence,
            now,
            request.expiry,
        )
        .map(|()| None::<()>)
    })
}
