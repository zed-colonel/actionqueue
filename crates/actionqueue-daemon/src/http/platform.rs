//! HTTP /api/v2/platform endpoints for multi-tenant isolation and RBAC.
//!
//! These endpoints are only registered when the `platform` feature is enabled.

use actionqueue_core::ids::{ActorId, LedgerEntryId, TenantId};
use actionqueue_core::mutation::{
    CapabilityGrantCommand, DurabilityPolicy, LedgerAppendCommand, MutationAuthority,
    MutationCommand, RoleAssignCommand, TenantCreateCommand,
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
    Json(body): Json<CreateTenantRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let seq = auth.projection().latest_sequence() + 1;
    let ts = now_secs();
    match auth.submit_command(
        MutationCommand::TenantCreate(TenantCreateCommand::new(
            seq,
            TenantRegistration::new(body.tenant_id, body.name),
            ts,
        )),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => (StatusCode::CREATED, Json(serde_json::json!({ "tenant_id": body.tenant_id })))
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() })))
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct AssignRoleRequest {
    role: String,
    tenant_id: TenantId,
}

async fn assign_role(
    State(state): State<RouterState>,
    axum::extract::Path(actor_id): axum::extract::Path<ActorId>,
    Json(body): Json<AssignRoleRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let role = parse_role(&body.role);
    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let seq = auth.projection().latest_sequence() + 1;
    let ts = now_secs();
    match auth.submit_command(
        MutationCommand::RoleAssign(RoleAssignCommand::new(
            seq,
            actor_id,
            role,
            body.tenant_id,
            ts,
        )),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() })))
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct GrantCapabilityRequest {
    capability: String,
    tenant_id: TenantId,
}

async fn grant_capability(
    State(state): State<RouterState>,
    axum::extract::Path(actor_id): axum::extract::Path<ActorId>,
    Json(body): Json<GrantCapabilityRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let capability = parse_capability(&body.capability);
    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let seq = auth.projection().latest_sequence() + 1;
    let ts = now_secs();
    match auth.submit_command(
        MutationCommand::CapabilityGrant(CapabilityGrantCommand::new(
            seq,
            actor_id,
            capability,
            body.tenant_id,
            ts,
        )),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() })))
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct AppendLedgerRequest {
    tenant_id: TenantId,
    ledger_key: String,
    #[serde(default)]
    actor_id: Option<ActorId>,
    payload_base64: String,
}

async fn append_ledger_entry(
    State(state): State<RouterState>,
    Json(body): Json<AppendLedgerRequest>,
) -> impl IntoResponse {
    let Some(authority) = state.control_authority.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };

    let payload = match base64_decode(&body.payload_base64) {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid_base64" })),
            )
                .into_response()
        }
    };

    let entry_id = LedgerEntryId::new();
    let ts = now_secs();
    let entry = LedgerEntry::new(entry_id, body.tenant_id, body.ledger_key, payload, ts);
    let entry = if let Some(aid) = body.actor_id { entry.with_actor(aid) } else { entry };

    let mut auth = match authority.lock() {
        Ok(a) => a,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let seq = auth.projection().latest_sequence() + 1;
    match auth.submit_command(
        MutationCommand::LedgerAppend(LedgerAppendCommand::new(seq, entry, ts)),
        DurabilityPolicy::Immediate,
    ) {
        Ok(_) => {
            (StatusCode::CREATED, Json(serde_json::json!({ "entry_id": entry_id }))).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": e.to_string() })))
            .into_response(),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn parse_role(s: &str) -> Role {
    match s {
        "Operator" => Role::Operator,
        "Auditor" => Role::Auditor,
        "Gatekeeper" => Role::Gatekeeper,
        other => Role::Custom(other.to_string()),
    }
}

fn parse_capability(s: &str) -> Capability {
    match s {
        "CanSubmit" => Capability::CanSubmit,
        "CanExecute" => Capability::CanExecute,
        "CanReview" => Capability::CanReview,
        "CanApprove" => Capability::CanApprove,
        "CanCancel" => Capability::CanCancel,
        other => Capability::Custom(other.to_string()),
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
