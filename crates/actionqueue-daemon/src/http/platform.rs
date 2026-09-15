//! HTTP /api/v2/platform endpoints for multi-tenant isolation and RBAC.
//!
//! These endpoints are only registered when the `platform` feature is enabled.

use actionqueue_core::ids::{ActorId, LedgerEntryId, TenantId};
use actionqueue_core::mutation::{
    CapabilityGrantCommand, LedgerAppendCommand, MutationCommand, RoleAssignCommand,
    TenantCreateCommand,
};
use actionqueue_core::platform::{Capability, LedgerEntry, Role, TenantRegistration};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Json;

use crate::http::RouterState;

/// Registers routes for the platform API.
pub fn register_routes(router: axum::Router<RouterState>) -> axum::Router<RouterState> {
    router
        .route("/api/v2/tenants", post(create_tenant))
        .route("/api/v2/actors/:actor_id/roles", post(assign_role))
        .route("/api/v2/actors/:actor_id/capabilities", post(grant_capability))
        .route("/api/v2/ledger", post(append_ledger_entry))
}

#[derive(serde::Deserialize)]
struct CreateTenantRequest {
    tenant_id: TenantId,
    name: String,
}

async fn create_tenant(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Json(body): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    host_mutation(
        &state,
        &host,
        |seq, ts| {
            if body.name.is_empty() {
                return Err(StatusCode::BAD_REQUEST.into_response());
            }
            Ok(MutationCommand::TenantCreate(TenantCreateCommand::new(
                seq,
                TenantRegistration::new(body.tenant_id, body.name),
                ts,
            )))
        },
        (StatusCode::CREATED, Json(serde_json::json!({ "tenant_id": body.tenant_id }))),
    )
}

#[derive(serde::Deserialize)]
struct AssignRoleRequest {
    role: String,
    tenant_id: TenantId,
}

async fn assign_role(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    axum::extract::Path(actor_id): axum::extract::Path<ActorId>,
    Json(body): Json<AssignRoleRequest>,
) -> impl IntoResponse {
    host_mutation(
        &state,
        &host,
        |seq, ts| {
            if body.role.is_empty() {
                return Err(StatusCode::BAD_REQUEST.into_response());
            }
            Ok(MutationCommand::RoleAssign(RoleAssignCommand::new(
                seq,
                actor_id,
                parse_role(&body.role),
                body.tenant_id,
                ts,
            )))
        },
        StatusCode::OK,
    )
}

#[derive(serde::Deserialize)]
struct GrantCapabilityRequest {
    capability: Capability,
    tenant_id: TenantId,
}

async fn grant_capability(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    axum::extract::Path(actor_id): axum::extract::Path<ActorId>,
    Json(body): Json<GrantCapabilityRequest>,
) -> impl IntoResponse {
    host_mutation(
        &state,
        &host,
        |seq, ts| {
            Ok(MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
                seq,
                actor_id,
                body.capability,
                body.tenant_id,
                ts,
            )))
        },
        StatusCode::OK,
    )
}

#[derive(serde::Deserialize)]
struct AppendLedgerRequest {
    tenant_id: TenantId,
    ledger_key: String,

    payload_base64: String,
}

async fn append_ledger_entry(
    State(state): State<RouterState>,
    axum::Extension(host): axum::Extension<actionqueue_core::control::HostControlContext>,
    Json(body): Json<AppendLedgerRequest>,
) -> impl IntoResponse {
    let entry_id = LedgerEntryId::new();
    host_mutation(
        &state,
        &host,
        |seq, ts| {
            let payload = base64_decode(&body.payload_base64).map_err(|_| {
                (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid_base64" })))
                    .into_response()
            })?;
            if body.ledger_key.is_empty() {
                return Err(StatusCode::BAD_REQUEST.into_response());
            }
            let entry = LedgerEntry::new(entry_id, body.tenant_id, body.ledger_key, payload, ts);
            let entry = if let Some(aid) = host.actor_id { entry.with_actor(aid) } else { entry };
            Ok(MutationCommand::LedgerAppend(LedgerAppendCommand::new(seq, entry, ts)))
        },
        (StatusCode::CREATED, Json(serde_json::json!({ "entry_id": entry_id }))),
    )
}

/// Keep sequence allocation, host attribution and response mapping in one lane.
fn host_mutation(
    state: &RouterState,
    host: &actionqueue_core::control::HostControlContext,
    build: impl FnOnce(u64, u64) -> Result<MutationCommand, axum::response::Response>,
    success: impl IntoResponse,
) -> axum::response::Response {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Ok(mut authority) = authority.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let command = match build(authority.projection().latest_sequence() + 1, state.clock.now()) {
        Ok(command) => command,
        Err(response) => return response,
    };
    match super::execute_host_mutation(state, &mut authority, host, command) {
        Ok(_) => success.into_response(),
        Err(error) => super::api::service_response(Err(error)),
    }
}

fn parse_role(s: &str) -> Role {
    match s {
        "Operator" => Role::Operator,
        "Auditor" => Role::Auditor,
        "Gatekeeper" => Role::Gatekeeper,
        other => Role::Custom(other.to_string()),
    }
}

fn base64_decode(s: &str) -> Result<Vec<u8>, ()> {
    // Simple base64 decode using the standard library approach
    // We use a basic implementation since we don't have a base64 crate
    use std::collections::HashMap;
    let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut lookup = HashMap::new();
    for (i, c) in alphabet.chars().enumerate() {
        lookup.insert(c, i as u8);
    }
    let s = s.trim_end_matches('=');
    let mut out = Vec::new();
    let bytes: Vec<u8> = s.chars().filter_map(|c| lookup.get(&c).copied()).collect();
    for chunk in bytes.chunks(4) {
        if chunk.len() >= 2 {
            out.push((chunk[0] << 2) | (chunk[1] >> 4));
        }
        if chunk.len() >= 3 {
            out.push((chunk[1] << 4) | (chunk[2] >> 2));
        }
        if chunk.len() >= 4 {
            out.push((chunk[2] << 6) | chunk[3]);
        }
    }
    Ok(out)
}
