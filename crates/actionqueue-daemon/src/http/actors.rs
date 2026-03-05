//! HTTP /api/v2/actors endpoints for remote actor registration protocol.
//!
//! These endpoints are only registered when the `actor` feature is enabled.
//! They submit actor mutation commands through the WAL-backed control authority.

use actionqueue_core::actor::{ActorCapabilities, ActorRegistration};
use actionqueue_core::ids::ActorId;
use actionqueue_core::mutation::{
    ActorDeregisterCommand, ActorHeartbeatCommand, ActorRegisterCommand, DurabilityPolicy,
    MutationAuthority, MutationCommand,
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
    pub actor_id: ActorId,
    pub identity: String,
    pub capabilities: Vec<String>,
    pub heartbeat_interval_secs: u64,
    #[serde(default)]
    pub department: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<actionqueue_core::ids::TenantId>,
}

/// Response body for actor registration.
#[derive(serde::Serialize)]
pub struct RegisterActorResponse {
    pub actor_id: ActorId,
}

/// Registers routes for the actor API.
pub fn register_routes(router: axum::Router<RouterState>) -> axum::Router<RouterState> {
    router
        .route("/api/v2/actors/register", post(register_actor))
        .route("/api/v2/actors/:actor_id/heartbeat", post(actor_heartbeat))
        .route("/api/v2/actors/:actor_id", delete(deregister_actor))
        .route("/api/v2/actors/:actor_id/claimable", get(claimable_runs))
}

async fn register_actor(
    State(state): State<RouterState>,
    Json(body): Json<RegisterActorRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "control_authority_unavailable" })),
        )
            .into_response();
    };

    let caps = match ActorCapabilities::new(body.capabilities) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid_capabilities", "message": e })),
            )
                .into_response()
        }
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
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match auth.submit_command(
        MutationCommand::ActorRegister(ActorRegisterCommand::new(seq, reg, ts)),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!({ "actor_id": body.actor_id })))
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "mutation_failed", "message": e.to_string() })),
        )
            .into_response(),
    }
}

async fn actor_heartbeat(
    State(state): State<RouterState>,
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
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match auth.submit_command(
        MutationCommand::ActorHeartbeat(ActorHeartbeatCommand::new(seq, actor_id, ts)),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn deregister_actor(
    State(state): State<RouterState>,
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
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match auth.submit_command(
        MutationCommand::ActorDeregister(ActorDeregisterCommand::new(seq, actor_id, ts)),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Returns claimable runs for an actor (stub — capability routing not yet implemented).
async fn claimable_runs(
    State(_state): State<RouterState>,
    Path(_actor_id): Path<ActorId>,
) -> impl IntoResponse {
    // Capability-filtered dispatch is implemented in the dispatch loop.
    // This HTTP endpoint is a placeholder for the actor claiming protocol.
    (StatusCode::OK, Json(serde_json::json!({ "runs": [] }))).into_response()
}
